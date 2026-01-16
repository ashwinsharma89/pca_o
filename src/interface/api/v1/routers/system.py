"""
System Management Router - Maintenance and Reset operations
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
import logging
import os
import shutil
from pathlib import Path

from src.interface.api.middleware.auth import get_current_user
from src.core.database.duckdb_manager import get_duckdb_manager
from src.core.utils.performance import SemanticCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/system", tags=["System"])

@router.post("/reset")
async def reset_system(current_user: Dict[str, Any] = Depends(get_current_user)):
    """
    Nuclear Reset: Wipes all campaign data, tracking tables, and semantic caches.
    Ensures a 100% clean state.
    """
    logger.warning(f"Nuclear Reset initiated by user: {current_user.get('username')}")
    
    results = {
        "database": "failed",
        "cache": "failed",
        "success": False
    }
    
    try:
        # 1. Clear DuckDB and Parquet
        duckdb_mgr = get_duckdb_manager()
        duckdb_mgr.clear_data()
        results["database"] = "cleared"
        
        # 2. Clear Semantic Cache
        SemanticCache.get_instance().clear()
        results["cache"] = "cleared"
        
        results["success"] = True
        logger.info("Nuclear Reset completed successfully")
        return {
            "success": True,
            "message": "System has been reset to a clean state. All data and caches cleared.",
            "details": results
        }
    except Exception as e:
        logger.error(f"Nuclear Reset failed: {e}")
        raise HTTPException(status_code=500, detail=f"Reset failed: {str(e)}")
