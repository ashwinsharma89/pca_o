"""
Health Check API Endpoint

Provides comprehensive campaign analysis using agent chains.
Combines NL-to-SQL, MediaAnalyticsExpert, and PacingReportAgent
for a full diagnostic view.
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import pandas as pd
import logging

# Imports moved to inside functions for lazy loading
# from src.engine.agents.agent_chain import (
#     campaign_health_check,
#     deep_analysis,
#     quick_insights,
#     get_workflow_status,
#     clear_workflow_state
# )
# from src.engine.agents.shared_context import get_shared_context, reset_shared_context

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analyze", tags=["Analysis Workflows"])

# =============================================================================
# Request/Response Models
# =============================================================================

class HealthCheckRequest(BaseModel):
    """Request for campaign health check"""
    question: Optional[str] = Field(
        default=None,
        description="Specific question to answer (optional). Default: overall metrics by platform"
    )
    start_date: Optional[str] = Field(None, description="Filter start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="Filter end date (YYYY-MM-DD)")
    
    class Config:
        json_schema_extra = {
            "example": {
                "question": "How are our Google campaigns performing?"
            }
        }


class QuickInsightRequest(BaseModel):
    """Request for quick insight on a specific metric"""
    metric: str = Field(
        default="spend",
        description="Metric to analyze: spend, roas, ctr, cpa, conversions"
    )


class DeepAnalysisRequest(BaseModel):
    """Request for deep analysis with multiple questions"""
    questions: List[str] = Field(
        ...,
        description="List of questions to answer"
    )
    
    class Config:
        json_schema_extra = {
            "example": {
                "questions": [
                    "What is total spend by platform?",
                    "Which platform has best ROAS?",
                    "What are top 5 campaigns by conversions?"
                ]
            }
        }


class WorkflowStep(BaseModel):
    """A single step in the workflow"""
    step: str
    status: str
    error: Optional[str] = None
    reason: Optional[str] = None


class HealthCheckResponse(BaseModel):
    """Response from health check"""
    success: bool
    workflow: str
    started_at: str
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    steps: List[WorkflowStep]
    metrics: Optional[Dict[str, Any]] = None
    insights: Optional[Dict[str, Any]] = None
    patterns: Optional[Dict[str, Any]] = None
    business_context: Optional[Dict[str, Any]] = None
    pacing: Optional[Dict[str, Any]] = None
    recommendations: List[str] = []
    all_recommendations: List[str] = []
    errors: List[str] = []
    context_summary: Optional[Dict[str, Any]] = None


class QuickInsightResponse(BaseModel):
    """Response from quick insight"""
    success: bool
    workflow: str
    metric: str
    question: str
    answer: Optional[str] = None
    sql: Optional[str] = None
    error: Optional[str] = None


class WorkflowStatusResponse(BaseModel):
    """Current workflow status"""
    context: Dict[str, Any]
    insights: List[Dict[str, Any]]
    recommendations: List[Dict[str, Any]]
    anomalies: List[Dict[str, Any]]
    recent_queries: List[Dict[str, Any]]


# =============================================================================
# Helper to get campaign data
# =============================================================================

async def get_campaign_data(start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
    """
    Get campaign data from DuckDB using DuckDBManager.
    Defaults to 'Latest 30 Days' if no filters provided.
    """
    from src.core.database.duckdb_manager import get_duckdb_manager
    from datetime import datetime, timedelta
    
    duckdb_mgr = get_duckdb_manager()
    
    if not duckdb_mgr.has_data():
        logger.warning("No campaign data found in DuckDB.")
        return pd.DataFrame(columns=['Date', 'Campaign', 'Platform', 'Spend', 'Impressions', 'Clicks', 'Conversions'])
    
    try:
        filters = {}
        if start_date:
            filters['start_date'] = start_date
        if end_date:
            filters['end_date'] = end_date
            
        # If no dates provided, DuckDBManager.get_campaigns handles internal defaults or returns all
        # For Health Check consistency, we prioritize the full dataset unless specific dates requested
        df = duckdb_mgr.get_campaigns(filters=filters if filters else None)
        
        if df.empty:
            logger.warning("No data returned from DuckDB for the requested filters.")
            return pd.DataFrame(columns=['Date', 'Campaign', 'Platform', 'Spend', 'Impressions', 'Clicks', 'Conversions'])

        # Ensure Date column is standard Title Case for agent consumption
        if 'Date' not in df.columns and 'date' in df.columns:
            df = df.rename(columns={'date': 'Date'})
            
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load data: {e}")


# =============================================================================
# API Endpoints
# =============================================================================

@router.post("/health-check", response_model=HealthCheckResponse)
async def run_health_check(request: HealthCheckRequest):
    """
    Run comprehensive campaign health check.
    
    Chains multiple agents:
    1. NL-to-SQL Engine - Gets key metrics
    2. MediaAnalyticsExpert - Generates insights
    3. PacingReportAgent - Checks budget pacing
    
    Returns combined analysis with metrics, insights, and recommendations.
    """
    logger.info(f"Health check requested. Question: {request.question}")
    
    try:
        # Get campaign data with filters
        data = await get_campaign_data(
            start_date=request.start_date,
            end_date=request.end_date
        )
        logger.info(f"Loaded {len(data)} rows from database")
        
        # Run health check workflow
        from src.engine.agents.agent_chain import campaign_health_check
        result = await campaign_health_check(
            data=data,
            question=request.question
        )
        
        return HealthCheckResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/quick-insight", response_model=QuickInsightResponse)
async def run_quick_insight(request: QuickInsightRequest):
    """
    Get quick insight for a specific metric.
    
    Available metrics: spend, roas, ctr, cpa, conversions
    """
    logger.info(f"Quick insight requested for: {request.metric}")
    
    try:
        data = await get_campaign_data()
        
        from src.engine.agents.agent_chain import quick_insights
        result = await quick_insights(
            data=data,
            metric=request.metric
        )
        
        return QuickInsightResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Quick insight failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deep-analysis")
async def run_deep_analysis(request: DeepAnalysisRequest):
    """
    Run deep analysis answering multiple questions.
    
    Useful for comprehensive multi-question analysis.
    """
    logger.info(f"Deep analysis requested. {len(request.questions)} questions")
    
    try:
        data = await get_campaign_data()
        
        from src.engine.agents.agent_chain import deep_analysis
        result = await deep_analysis(
            data=data,
            questions=request.questions
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Deep analysis failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/workflow-status", response_model=WorkflowStatusResponse)
async def get_status():
    """
    Get current workflow status.
    
    Shows accumulated insights, recommendations, anomalies,
    and recent queries from the current session.
    """
    from src.engine.agents.agent_chain import get_workflow_status
    status = get_workflow_status()
    return WorkflowStatusResponse(**status)


@router.post("/reset")
async def reset_workflow():
    """
    Reset workflow state.
    
    Clears all cached data, insights, and recommendations.
    Use before starting a new analysis session.
    """
    from src.engine.agents.agent_chain import clear_workflow_state
    clear_workflow_state()
    return {"status": "reset", "message": "Workflow state cleared"}
