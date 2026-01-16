"""
Pacing Reports Router - Excel report generation from templates

Handles:
- Template listing and upload
- Report generation from templates
- Report download
"""

from fastapi import APIRouter, HTTPException, Depends, Request, UploadFile, File
from fastapi.responses import FileResponse
from typing import Dict, Any, List
import os
import logging
from datetime import datetime
from pathlib import Path

from src.interface.api.middleware.auth import get_current_user
from src.interface.api.middleware.rate_limit import limiter
from src.core.database.duckdb_manager import get_duckdb_manager
from src.engine.reports.intelligent_engine import IntelligentReportEngine

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pacing", tags=["pacing"])

# Directory paths
TEMPLATES_DIR = Path("reports/pacing/templates")
OUTPUTS_DIR = Path("reports/pacing/outputs")

# Ensure directories exist
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)


def get_template_name(filename: str) -> str:
    """Extract human-readable name from template filename."""
    name = filename.replace('template_', '').replace('.xlsx', '').replace('.xls', '')
    # Remove timestamp prefix if present
    parts = name.split('_')
    if len(parts) > 2 and parts[0].isdigit():
        name = '_'.join(parts[2:])
    return name.replace('_', ' ').title()


@router.get("/templates")
@limiter.limit("60/minute")
async def list_templates(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """List all available pacing report templates."""
    try:
        templates = []
        for i, filepath in enumerate(sorted(TEMPLATES_DIR.glob("*.xlsx"))):
            stat = filepath.stat()
            templates.append({
                "id": str(i + 1),
                "name": get_template_name(filepath.name),
                "filename": filepath.name,
                "created_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d"),
                "size_bytes": stat.st_size
            })
        return {"templates": templates}
    except Exception as e:
        logger.error(f"Failed to list templates: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reports")
@limiter.limit("60/minute")
async def list_reports(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """List all generated pacing reports."""
    try:
        reports = []
        for i, filepath in enumerate(sorted(OUTPUTS_DIR.glob("*.xlsx"), reverse=True)):
            stat = filepath.stat()
            # Extract template info from filename
            name_parts = filepath.stem.split('_')
            report_type = name_parts[2] if len(name_parts) > 2 else "daily"
            
            reports.append({
                "id": str(i + 1),
                "name": f"{report_type.title()} Pacing Report",
                "filename": filepath.name,
                "template_name": "Auto-generated",
                "created_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                "size_bytes": stat.st_size,
                "status": "completed"
            })
        return {"reports": reports[:50]}  # Limit to last 50
    except Exception as e:
        logger.error(f"Failed to list reports: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/generate")
@limiter.limit("10/minute")
async def generate_report(
    request: Request,
    body: Dict[str, Any],
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Generate a new pacing report from a template."""
    try:
        template_id = body.get("template_id")
        if not template_id:
            raise HTTPException(status_code=400, detail="template_id is required")
        
        # Find template
        templates = list(sorted(TEMPLATES_DIR.glob("*.xlsx")))
        idx = int(template_id) - 1
        if idx < 0 or idx >= len(templates):
            raise HTTPException(status_code=404, detail="Template not found")
        
        template_path = templates[idx]
        
        # 1. Fetch data from DuckDB (Single Source of Truth)
        duckdb_mgr = get_duckdb_manager()
        if not duckdb_mgr.has_data():
            raise HTTPException(status_code=400, detail="No data available in database. Please upload a dataset first.")
        
        # Use full dataset for reporting
        data = duckdb_mgr.get_campaigns(limit=1000000)
        
        # 2. Initialize Engine
        # The engine handles: Analysis, Mapping, Aggregation, and Population
        engine = IntelligentReportEngine(output_dir=str(OUTPUTS_DIR))
        
        # 3. Generate Output Filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        template_name = template_path.stem.replace('template_', '')
        output_filename = f"report_{template_name}_{timestamp}.xlsx"
        
        # 4. Generate Report using the hardened pipeline data
        result = engine.generate(
            template_path=str(template_path),
            data=data,
            output_name=output_filename
        )
        
        logger.info(f"Generated pacing report: {output_filename}")
        
        return {
            "success": True,
            "filename": output_filename,
            "message": f"Report generated successfully: {output_filename}",
            "stats": result.get("stats")
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reports/{report_id}/download")
async def download_report(
    report_id: str,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Download a generated report."""
    try:
        reports = list(sorted(OUTPUTS_DIR.glob("*.xlsx"), reverse=True))
        idx = int(report_id) - 1
        if idx < 0 or idx >= len(reports):
            raise HTTPException(status_code=404, detail="Report not found")
        
        report_path = reports[idx]
        return FileResponse(
            path=str(report_path),
            filename=report_path.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to download report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/templates/upload")
@limiter.limit("10/minute")
async def upload_template(
    request: Request,
    file: UploadFile = File(...),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Upload a new pacing report template."""
    try:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are allowed")
        
        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = file.filename.replace(' ', '_')
        output_filename = f"template_{timestamp}_{safe_name}"
        output_path = TEMPLATES_DIR / output_filename
        
        # Save file
        content = await file.read()
        with open(output_path, 'wb') as f:
            f.write(content)
        
        logger.info(f"Uploaded template: {output_filename}")
        
        return {
            "success": True,
            "filename": output_filename,
            "message": f"Template '{file.filename}' uploaded successfully"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to upload template: {e}")
        raise HTTPException(status_code=500, detail=str(e))
