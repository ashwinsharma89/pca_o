"""
API endpoints for knowledge base refresh management
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from src.platform.knowledge.enhanced_auto_refresh import get_enhanced_refresher

router = APIRouter(prefix="/api/knowledge/refresh", tags=["knowledge-refresh"])

class SourceRegistration(BaseModel):
    """Model for source registration."""
    source_id: str
    source_type: str
    location: str
    priority: int = 1
    tags: Optional[List[str]] = None

class RefreshRequest(BaseModel):
    """Model for refresh request."""
    source_ids: Optional[List[str]] = None

class RollbackRequest(BaseModel):
    """Model for rollback request."""
    source_id: str
    target_version: Optional[int] = None

@router.post("/register")
async def register_source(registration: SourceRegistration) -> Dict[str, Any]:
    """
    Register a new knowledge source for monitoring.
    
    Args:
        registration: Source registration details
    
    Returns:
        Registration result
    """
    refresher = get_enhanced_refresher()
    
    success = refresher.register_source(
        source_id=registration.source_id,
        source_type=registration.source_type,
        location=registration.location,
        priority=registration.priority,
        tags=registration.tags
    )
    
    if success:
        return {
            "success": True,
            "message": f"Source {registration.source_id} registered successfully"
        }
    else:
        raise HTTPException(status_code=400, detail="Failed to register source")

@router.delete("/unregister/{source_id}")
async def unregister_source(source_id: str) -> Dict[str, Any]:
    """
    Unregister a knowledge source.
    
    Args:
        source_id: Source to unregister
    
    Returns:
        Unregistration result
    """
    refresher = get_enhanced_refresher()
    
    if source_id not in refresher.sources:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
    
    del refresher.sources[source_id]
    refresher._save_metadata()
    
    return {
        "success": True,
        "message": f"Source {source_id} unregistered successfully"
    }

@router.get("/check")
async def check_for_changes(source_ids: Optional[str] = None) -> Dict[str, Any]:
    """
    Check sources for changes.
    
    Args:
        source_ids: Comma-separated list of source IDs (optional)
    
    Returns:
        Change detection results
    """
    refresher = get_enhanced_refresher()
    
    source_list = source_ids.split(",") if source_ids else None
    changes = refresher.check_for_changes(source_list)
    
    return {
        "success": True,
        "sources_checked": len(changes),
        "changes": changes
    }

@router.post("/refresh")
async def refresh_sources(
    request: RefreshRequest,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """
    Trigger refresh for sources.
    
    Args:
        request: Refresh request with optional source IDs
        background_tasks: FastAPI background tasks
    
    Returns:
        Refresh initiation result
    """
    refresher = get_enhanced_refresher()
    
    # Run refresh in background
    background_tasks.add_task(refresher.refresh_all_changed)
    
    return {
        "success": True,
        "message": "Refresh initiated in background",
        "source_ids": request.source_ids
    }

@router.post("/rollback")
async def rollback_source(request: RollbackRequest) -> Dict[str, Any]:
    """
    Rollback a source to a previous version.
    
    Args:
        request: Rollback request
    
    Returns:
        Rollback result
    """
    refresher = get_enhanced_refresher()
    
    success = refresher.rollback_source(
        source_id=request.source_id,
        target_version=request.target_version
    )
    
    if success:
        return {
            "success": True,
            "message": f"Source {request.source_id} rolled back successfully"
        }
    else:
        raise HTTPException(status_code=400, detail="Rollback failed")

@router.get("/source/{source_id}")
async def get_source_info(source_id: str) -> Dict[str, Any]:
    """
    Get detailed information about a source.
    
    Args:
        source_id: Source ID
    
    Returns:
        Source information
    """
    refresher = get_enhanced_refresher()
    
    info = refresher.get_source_info(source_id)
    
    if info is None:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
    
    return info

@router.get("/sources")
async def list_sources() -> Dict[str, Any]:
    """
    List all registered sources.
    
    Returns:
        List of sources
    """
    refresher = get_enhanced_refresher()
    
    sources = [
        {
            "source_id": meta.source_id,
            "source_type": meta.source_type,
            "current_version": meta.current_version,
            "enabled": meta.enabled,
            "priority": meta.priority,
            "refresh_count": meta.refresh_count,
            "last_refreshed": meta.last_refreshed
        }
        for meta in refresher.sources.values()
    ]
    
    return {
        "success": True,
        "total_sources": len(sources),
        "sources": sources
    }

@router.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """
    Get refresh statistics.
    
    Returns:
        Statistics
    """
    refresher = get_enhanced_refresher()
    return refresher.get_stats()

@router.get("/report")
async def get_report() -> Dict[str, Any]:
    """
    Get detailed refresh report.
    
    Returns:
        Detailed report
    """
    refresher = get_enhanced_refresher()
    report = refresher.generate_report()
    
    return {
        "success": True,
        "report": report
    }

@router.post("/start")
async def start_auto_refresh() -> Dict[str, Any]:
    """
    Start automatic refresh monitoring.
    
    Returns:
        Start result
    """
    refresher = get_enhanced_refresher()
    refresher.start_auto_refresh()
    
    return {
        "success": True,
        "message": "Auto-refresh monitoring started"
    }

@router.post("/stop")
async def stop_auto_refresh() -> Dict[str, Any]:
    """
    Stop automatic refresh monitoring.
    
    Returns:
        Stop result
    """
    refresher = get_enhanced_refresher()
    refresher.stop_auto_refresh()
    
    return {
        "success": True,
        "message": "Auto-refresh monitoring stopped"
    }

@router.patch("/source/{source_id}/enable")
async def enable_source(source_id: str) -> Dict[str, Any]:
    """
    Enable a source for monitoring.
    
    Args:
        source_id: Source to enable
    
    Returns:
        Enable result
    """
    refresher = get_enhanced_refresher()
    
    if source_id not in refresher.sources:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
    
    refresher.sources[source_id].enabled = True
    refresher._save_metadata()
    
    return {
        "success": True,
        "message": f"Source {source_id} enabled"
    }

@router.patch("/source/{source_id}/disable")
async def disable_source(source_id: str) -> Dict[str, Any]:
    """
    Disable a source from monitoring.
    
    Args:
        source_id: Source to disable
    
    Returns:
        Disable result
    """
    refresher = get_enhanced_refresher()
    
    if source_id not in refresher.sources:
        raise HTTPException(status_code=404, detail=f"Source {source_id} not found")
    
    refresher.sources[source_id].enabled = False
    refresher._save_metadata()
    
    return {
        "success": True,
        "message": f"Source {source_id} disabled"
    }
