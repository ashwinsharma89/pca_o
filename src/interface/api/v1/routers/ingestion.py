"""
Ingestion Router - Upload, filters, schema endpoints

Handles data ingestion operations:
- File upload (CSV/Excel)
- Filter options discovery
- Schema detection
"""

from fastapi import APIRouter, HTTPException, Depends, Request, status, UploadFile, File, Form
from typing import Dict, Any, Optional
import logging
import pandas as pd

from src.interface.api.middleware.auth import get_current_user
from src.interface.api.middleware.rate_limit import limiter
from ..dependencies import get_campaign_service
from src.engine.services.campaign_service import CampaignService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/campaigns", tags=["ingestion"])


@router.post("/upload/preview-sheets")
@limiter.limit("10/minute")
async def preview_excel_sheets(
    request: Request,
    file: UploadFile = File(...),
    current_user: Dict[str, Any] = Depends(get_current_user)
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
        
        xl_file = pd.ExcelFile(io.BytesIO(contents))
        sheet_names = xl_file.sheet_names
        
        sheet_info = []
        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(xl_file, sheet_name=sheet_name)
                sheet_info.append({
                    'name': sheet_name,
                    'row_count': len(df),
                    'column_count': len(df.columns)
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


@router.post("/upload", status_code=status.HTTP_201_CREATED)
@limiter.limit("20/minute")
async def upload_campaign_data(
    request: Request,
    file: UploadFile = File(...),
    sheet_name: Optional[str] = Form(None),
    current_user: Dict[str, Any] = Depends(get_current_user),
    campaign_service: CampaignService = Depends(get_campaign_service)
):
    """
    Upload campaign data from CSV/Excel.
    Uses DuckDB + Parquet for fast analytics.
    
    **File Constraints:**
    - Max size: 100MB
    - Allowed formats: CSV, XLSX, XLS
    """
    try:
        file_ext = file.filename.split('.')[-1].lower() if file.filename else ''
        if file_ext not in ['csv', 'xlsx', 'xls']:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file format '{file_ext}'. Allowed: csv, xlsx, xls"
            )
        
        contents = await file.read()
        result = campaign_service.upload_from_bytes(contents, file.filename, sheet_name)
        return result
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="The uploaded file is empty")
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema")
async def get_data_schema(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user),
    campaign_service: CampaignService = Depends(get_campaign_service)
):
    """
    Get schema metadata about available columns for dynamic UI.
    Returns which metrics and dimensions are available in the uploaded data.
    """
    try:
        return campaign_service.get_schema_info()
    except Exception as e:
        logger.error(f"Failed to get data schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/filters")
async def get_filter_options(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user),
    campaign_service: CampaignService = Depends(get_campaign_service)
):
    """
    Get all unique filter option values for dropdowns.
    Uses DuckDB to dynamically detect all filterable columns.
    """
    try:
        result = campaign_service.get_filter_options()
        logger.info(f"Returning {len(result)} filters with values: {list(result.keys())}")
        return result
    except Exception as e:
        logger.error(f"Failed to get filter options: {e}")
        raise HTTPException(status_code=500, detail=str(e))
