"""
Robust Data Upload Module

Provides:
- Streaming file upload (memory-efficient)
- SHA-256 file deduplication
- Async processing via Celery
- Progress tracking
"""

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime

from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from loguru import logger

from src.core.config.settings import get_settings


router = APIRouter(prefix="/upload", tags=["upload"])

# Constants
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for streaming
MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB
UPLOAD_DIR = Path("data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


class UploadStatus(BaseModel):
    """Status of an upload job."""
    job_id: str
    status: str  # "pending", "processing", "completed", "failed"
    progress: float  # 0.0 to 1.0
    message: str
    file_hash: Optional[str] = None
    row_count: Optional[int] = None
    summary: Optional[Dict[str, Any]] = None
    schema: Optional[List[Dict[str, Any]]] = None
    preview: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None


# In-memory job tracker (would use Redis in production)
_upload_jobs: Dict[str, UploadStatus] = {}


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA-256 hash of a file using streaming reads."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def check_duplicate(file_hash: str) -> Optional[Path]:
    """Check if file with hash already exists in upload directory."""
    # Check existing parquet files for matching hash
    hash_file = UPLOAD_DIR / f"{file_hash}.hash"
    if hash_file.exists():
        # Read the path from hash file
        original_path = hash_file.read_text().strip()
        if Path(original_path).exists():
            return Path(original_path)
    return None


def save_hash_mapping(file_hash: str, parquet_path: Path) -> None:
    """Save hash -> parquet path mapping for deduplication."""
    hash_file = UPLOAD_DIR / f"{file_hash}.hash"
    hash_file.write_text(str(parquet_path))


async def stream_upload_to_temp(file: UploadFile) -> tuple[Path, int]:
    """
    Stream upload file to temporary location, returning path and size.
    Uses constant memory regardless of file size.
    """
    # Create temp file with appropriate extension
    ext = Path(file.filename).suffix if file.filename else ".tmp"
    temp_fd, temp_path = tempfile.mkstemp(suffix=ext, dir=UPLOAD_DIR)
    temp_path = Path(temp_path)
    
    total_size = 0
    try:
        with os.fdopen(temp_fd, "wb") as temp_file:
            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break
                temp_file.write(chunk)
                total_size += len(chunk)
                
                # Check size limit during upload (fail fast)
                if total_size > MAX_FILE_SIZE:
                    raise HTTPException(
                        status_code=413,
                        detail=f"File exceeds maximum size of {MAX_FILE_SIZE // (1024*1024)}MB"
                    )
        
        return temp_path, total_size
        
    except Exception as e:
        # Cleanup on error
        if temp_path.exists():
            temp_path.unlink()
        raise


def process_file_sync(job_id: str, temp_path: Path, file_hash: str, sheet_name: Optional[str] = None):
    """
    Process uploaded file synchronously (called by background task or Celery).
    Converts CSV/Excel to Parquet and saves to DuckDB.
    """
    try:
        _upload_jobs[job_id].status = "processing"
        _upload_jobs[job_id].message = "Parsing file..."
        
        import pandas as pd
        from src.core.database.duckdb_manager import get_duckdb_manager
        
        # Parse file based on extension
        ext = temp_path.suffix.lower()
        if ext == ".csv":
            df = pd.read_csv(temp_path)
        elif ext in [".xlsx", ".xls"]:
            if sheet_name:
                df = pd.read_excel(temp_path, sheet_name=sheet_name)
            else:
                df = pd.read_excel(temp_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")
        
        _upload_jobs[job_id].message = f"Processing {len(df)} rows..."
        _upload_jobs[job_id].progress = 0.5
        
        from src.core.utils.column_mapping import find_column, normalize_column_names
        
        # Standardize column names (Rename 'Total Spent' -> 'spend', etc.)
        normalize_column_names(df, inplace=True)
        
        from src.core.schema.columns import Columns

        # Clean metric columns in-place before saving
        for metric_col in Columns.metrics():
            col_name = find_column(df, metric_col)
            if col_name and df[col_name].dtype == 'object':
                try:
                    logger.info(f"Cleaning column {col_name}...")
                    # Remove currency symbols and commas
                    df[col_name] = df[col_name].astype(str).str.replace(r'[$,]', '', regex=True)
                    # Convert to numeric
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0)
                except Exception as e:
                    logger.warning(f"Failed to clean column {col_name}: {e}")
                    
        # Filter rows with invalid dates (same logic as DuckDBManager)
        # This ensures Upload Summary matches Database content exactly
        date_col = Columns.DATE if Columns.DATE in df.columns else find_column(df, Columns.DATE)
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            initial_len = len(df)
            df = df.dropna(subset=[date_col])
            dropped_count = initial_len - len(df)
            if dropped_count > 0:
                logger.warning(f"Dropped {dropped_count} rows with invalid dates (likely summary/total rows)")
                _upload_jobs[job_id].message += f" (Dropped {dropped_count} invalid rows)"

        # Add Job ID to persist in DB (SSOT)
        df[Columns.JOB_ID.value] = job_id
        
        # Save to DuckDB/Parquet (now with cleaned data)
        duckdb_mgr = get_duckdb_manager()
        row_count = duckdb_mgr.save_campaigns(df)
        
        # Sync to Knowledge Graph (Neo4j) - FIRE AND FORGET
        try:
             # We do NOT await this or block the job completion
             # In a real production system, this would be a separate Celery task
             # with its own status tracking. For now, we launch it but don't hold the user hostage.
             from threading import Thread
             from src.kg_rag.etl.ingestion import ingest_dataframe
             
             def run_kg_sync(df_copy, j_id):
                 try:
                     logger.info(f"Background KG Sync started for {j_id}...")
                     ingest_dataframe(df_copy)
                     logger.info(f"Background KG Sync complete for {j_id}")
                 except Exception as ex:
                     logger.error(f"Background KG Sync failed for {j_id}: {ex}")

             # Start background thread
             Thread(target=run_kg_sync, args=(df.copy(), job_id), daemon=True).start()
             
             _upload_jobs[job_id].message += " (KG Sync started in background)"
        except Exception as kg_e:
            logger.error(f"Failed to launch KG Sync for {job_id}: {kg_e}")
            # Continue without failing the job

        
        # Save hash mapping for deduplication
        parquet_path = Path("data/campaigns.parquet")
        save_hash_mapping(file_hash, parquet_path)
        
        from src.core.utils.column_mapping import find_column
        
        # Calculate summary metrics (SSOT: Ask the DB!)
        # We NO LONGER calculate using Pandas/Polars here.
        # This guarantees 100% consistency with the Analysis layer.
        db_summary = duckdb_mgr.get_job_summary(job_id)
        
        # Extract values
        total_spend = db_summary.get('total_spend', 0.0)
        total_clicks = db_summary.get('total_clicks', 0)
        total_impressions = db_summary.get('total_impressions', 0)
        total_conversions = db_summary.get('total_conversions', 0)
        
        # Calculate CTR safely (UI logic only)
        avg_ctr = 0.0
        if total_impressions > 0:
            avg_ctr = float(total_clicks) / float(total_impressions) * 100.0
                    
        metrics = {
            'total_spend': float(total_spend),
            'total_clicks': int(total_clicks),
            'total_impressions': int(total_impressions),
            'total_conversions': int(total_conversions),
            'avg_ctr': float(avg_ctr)
        }
        
        # Calculate schema info
        schema_info = []
        for col in df.columns:
            dtype = str(df[col].dtype)
            null_count = int(df[col].isnull().sum())
            schema_info.append({
                "column": col,
                "dtype": dtype,
                "null_count": null_count
            })
        _upload_jobs[job_id].schema = schema_info
        
        # Enhance preview (pandas to dict records)
        if not df.empty:
            df_preview = df.head(5).copy()
            # Handle date serialization for JSON
            for col in df_preview.columns:
                if pd.api.types.is_datetime64_any_dtype(df_preview[col]):
                    df_preview[col] = df_preview[col].astype(str)
            _upload_jobs[job_id].preview = df_preview.to_dict(orient='records')
        else:
            _upload_jobs[job_id].preview = []

        # Update job status
        _upload_jobs[job_id].status = "completed"
        _upload_jobs[job_id].progress = 1.0
        _upload_jobs[job_id].message = f"Successfully imported {row_count} rows"
        _upload_jobs[job_id].row_count = row_count
        _upload_jobs[job_id].file_hash = file_hash
        _upload_jobs[job_id].summary = metrics
        
        logger.info(f"Upload job {job_id} completed: {row_count} rows")
        
    except Exception as e:
        logger.error(f"Upload job {job_id} failed: {e}")
        _upload_jobs[job_id].status = "failed"
        _upload_jobs[job_id].error = str(e)
        _upload_jobs[job_id].message = f"Processing failed: {str(e)}"
        
    finally:
        # Cleanup temp file
        if temp_path.exists():
            temp_path.unlink()


@router.post("/stream")
async def stream_upload(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    sheet_name: Optional[str] = Form(None),
):
    """
    Streaming file upload with deduplication.
    
    Features:
    - Memory-efficient streaming (constant memory usage)
    - SHA-256 deduplication (skip re-upload of identical files)
    - Async processing via background tasks
    - Progress tracking via job_id
    
    Returns:
        job_id for tracking upload progress
    """
    # Validate file extension
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    
    ext = Path(file.filename).suffix.lower()
    if ext not in [".csv", ".xlsx", ".xls"]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file format '{ext}'. Allowed: .csv, .xlsx, .xls"
        )
    
    try:
        # Stream to temp file
        logger.info(f"Streaming upload: {file.filename}")
        temp_path, file_size = await stream_upload_to_temp(file)
        
        # Compute hash for deduplication
        file_hash = compute_file_hash(temp_path)
        logger.info(f"File hash: {file_hash[:16]}... ({file_size / 1024 / 1024:.1f}MB)")
        
        # Idempotent Duplicate Check
        # If file exists, we don't error, we just return success (Skipping processing)
        existing_path = check_duplicate(file_hash)
        if existing_path:
            # Clean up temp file
            temp_path.unlink()
            logger.info(f"Duplicate file {file_hash[:8]} detected - Treating as success (Idempotent)")
            
            # Mock a successful job response so frontend is happy
            return JSONResponse({
                "status": "completed", # Pretend it's done
                "job_id": f"cached_{file_hash[:8]}",
                "file_hash": file_hash,
                "message": "File already validated and processed (Skipped re-import)",
                "note": "Duplicate upload detected - utilizing existing data."
            })
        
        # Create job for tracking
        job_id = f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file_hash[:8]}"
        _upload_jobs[job_id] = UploadStatus(
            job_id=job_id,
            status="pending",
            progress=0.1,
            message="File uploaded, queuing for processing...",
            file_hash=file_hash
        )
        
        # Process in background
        background_tasks.add_task(
            process_file_sync,
            job_id,
            temp_path,
            file_hash,
            sheet_name
        )
        
        return JSONResponse({
            "status": "accepted",
            "job_id": job_id,
            "file_hash": file_hash,
            "file_size_mb": round(file_size / 1024 / 1024, 2),
            "message": "File queued for processing. Use /upload/status/{job_id} to track progress."
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


from src.interface.api.middleware.rate_limit import limiter


@router.get("/status/{job_id}")
@limiter.exempt
async def get_upload_status(request: Request, job_id: str):
    logger.info(f"Checking status for job {job_id}")
    if job_id not in _upload_jobs:
        logger.error(f"Job {job_id} not found in _upload_jobs keys: {list(_upload_jobs.keys())}")
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    
    try:
        from fastapi.encoders import jsonable_encoder
        status = _upload_jobs[job_id]
        # Force encoding check to catch serialization errors early
        jsonable_encoder(status)
        return status
    except Exception as e:
        logger.error(f"Serialization error for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Serialization error: {str(e)}")


@router.get("/jobs")
async def list_upload_jobs():
    """List all upload jobs (for debugging)."""
    return list(_upload_jobs.values())


@router.post("/preview-sheets")
async def preview_excel_sheets(
    request: Request,
    file: UploadFile = File(...)
):
    """
    Preview available sheets in an Excel file before uploading.
    Optimized to minimize file reads.
    """
    try:
        if not file.filename.endswith(('.xls', '.xlsx')):
            raise HTTPException(status_code=400, detail="File must be an Excel file (.xls or .xlsx)")
        
        contents = await file.read()
        import io
        import pandas as pd
        
        xl_file = pd.ExcelFile(io.BytesIO(contents))
        sheet_names = xl_file.sheet_names
        
        sheet_info = []
        for sheet_name in sheet_names:
            try:
                # Read just header to get column count
                df = pd.read_excel(xl_file, sheet_name=sheet_name, nrows=0) 
                # Read full to get row count (expensive but necessary for accurate count? 
                # Or just estimates? The original code read full df. Let's stick to original behavior but maybe optimize later)
                # Actually, reading full df just for count is slow for big files. 
                # But let's copy behavior for correctness first.
                df_full = pd.read_excel(xl_file, sheet_name=sheet_name)
                
                sheet_info.append({
                    'name': sheet_name,
                    'row_count': len(df_full),
                    'column_count': len(df_full.columns)
                })
            except Exception as e:
                logger.warning(f"Could not read sheet {sheet_name}: {e}")
                sheet_info.append({
                    'name': sheet_name,
                    'row_count': 0,
                    'column_count': 0,
                    'error': str(e)
                })
        
        return {
            'filename': file.filename,
            'sheets': sheet_info,
            'default_sheet': sheet_names[0] if sheet_names else None
        }
        
    except Exception as e:
        logger.error(f"Failed to preview Excel sheets: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to preview sheets: {str(e)}")
