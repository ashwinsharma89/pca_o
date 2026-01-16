from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import List, Dict, Any, Optional
from datetime import datetime
from src.core.database.duckdb_manager import get_duckdb_manager
from src.interface.api.middleware.auth import get_current_user
from src.engine.analytics.feedback_loop import OutcomeTracker
from loguru import logger

router = APIRouter(prefix="/feedback", tags=["Intelligence Feedback"])

@router.post("/recommendation")
async def submit_recommendation_feedback(
    recommendation_id: str = Body(..., embed=True),
    action: str = Body(..., embed=True), # "Implemented", "Ignored", "Modified"
    notes: Optional[str] = Body(None, embed=True),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Log user feedback on a specific recommendation."""
    try:
        db = get_duckdb_manager()
        with db.connection() as conn:
            # Verify recommendation exists
            exists = conn.execute("SELECT 1 FROM recommendation_history WHERE id = ?", (recommendation_id,)).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="Recommendation not found")
                
            conn.execute("""
                INSERT INTO recommendation_feedback 
                (recommendation_id, user_action, implementation_date, feedback_notes, timestamp)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (recommendation_id, action, datetime.utcnow(), notes))
            
        return {"success": True, "message": "Feedback recorded"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to record feedback: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/history")
async def get_recommendation_history(
    limit: int = 50,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Retrieve historical recommendations with their feedback status."""
    try:
        db = get_duckdb_manager()
        with db.connection() as conn:
            query = """
                SELECT 
                    h.*,
                    f.user_action,
                    f.feedback_notes,
                    f.actual_impact_observed
                FROM recommendation_history h
                LEFT JOIN (
                    -- Get the latest feedback per recommendation
                    SELECT * FROM recommendation_feedback 
                    WHERE (recommendation_id, timestamp) IN (
                        SELECT recommendation_id, MAX(timestamp) 
                        FROM recommendation_feedback 
                        GROUP BY recommendation_id
                    )
                ) f ON h.id = f.recommendation_id
                ORDER BY h.timestamp DESC
                LIMIT ?
            """
            df = conn.execute(query, (limit,)).df()
            return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Failed to get history: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/impact")
async def get_recommendation_impact(
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get the estimated vs actual impact for all closed-loop recommendations."""
    try:
        results = OutcomeTracker.get_attribution_summary()
        return {
            "success": True, 
            "results": results,
            "summary": {
                "total_tracked": len(results),
                "positive_impact": len([r for r in results if r['status'] == "Positive Impact"]),
                "average_roi_lift": sum([r['roi_percent'] for r in results]) / len(results) if results else 0
            }
        }
    except Exception as e:
        logger.error(f"Failed to get impact summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

