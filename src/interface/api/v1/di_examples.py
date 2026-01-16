"""
Example API endpoints using Dependency Injection.

Demonstrates how to use the DI container in FastAPI endpoints.
"""

from fastapi import APIRouter, Depends, HTTPException
from typing import Optional, Dict, Any
import pandas as pd
from pydantic import BaseModel

# Import DI dependencies
from src.interface.api.dependencies import (
    get_reasoning_agent_dep,
    get_validated_reasoning_agent_dep,
    get_retriever_dep,
    get_benchmark_engine_dep,
    get_di_container
)

# Import protocols for type hints
from src.core.interfaces.protocols import IReasoningAgent, IRetriever, IBenchmarkEngine
from src.core.di.containers import ApplicationContainer


# Create router
router = APIRouter(prefix="/api/v1/di-examples", tags=["DI Examples"])


# ============================================================================
# Request/Response Models
# ============================================================================

class CampaignAnalysisRequest(BaseModel):
    """Request model for campaign analysis."""
    platform: str
    objective: str
    data: Dict[str, Any]  # Simplified for example
    
    class Config:
        schema_extra = {
            "example": {
                "platform": "meta",
                "objective": "conversions",
                "data": {
                    "spend": [1000, 1200, 1100],
                    "conversions": [50, 60, 55],
                    "ctr": [0.05, 0.055, 0.052]
                }
            }
        }


# ============================================================================
# Example 1: Inject Single Dependency
# ============================================================================

@router.post("/analyze-simple")
async def analyze_simple(
    request: CampaignAnalysisRequest,
    agent: Optional[IReasoningAgent] = Depends(get_reasoning_agent_dep)
):
    """
    Simple analysis endpoint with injected reasoning agent.
    
    Benefits:
    - No need to instantiate agent
    - Easy to test with mocked agent
    - Automatic dependency resolution
    """
    if not agent:
        raise HTTPException(
            status_code=503,
            detail="Reasoning agent not available"
        )
    
    # Convert request data to DataFrame (simplified)
    df = pd.DataFrame(request.data)
    df['Platform'] = request.platform
    
    # Use injected agent
    result = agent.analyze(df)
    
    return {
        "status": "success",
        "platform": request.platform,
        "objective": request.objective,
        "insights_count": len(result.get('insights', {}).get('pattern_insights', [])),
        "recommendations_count": len(result.get('recommendations', [])),
        "benchmarks_applied": result.get('benchmarks_applied', False)
    }


# ============================================================================
# Example 2: Inject Multiple Dependencies
# ============================================================================

@router.post("/analyze-comprehensive")
async def analyze_comprehensive(
    request: CampaignAnalysisRequest,
    agent: Optional[IReasoningAgent] = Depends(get_reasoning_agent_dep),
    retriever: Optional[IRetriever] = Depends(get_retriever_dep),
    benchmarks: Optional[IBenchmarkEngine] = Depends(get_benchmark_engine_dep)
):
    """
    Comprehensive analysis with multiple injected dependencies.
    
    Demonstrates:
    - Multiple dependency injection
    - Graceful degradation when dependencies unavailable
    - Type-safe interfaces
    """
    if not agent:
        raise HTTPException(status_code=503, detail="Agent not available")
    
    # Convert to DataFrame
    df = pd.DataFrame(request.data)
    df['Platform'] = request.platform
    
    # Use agent
    analysis_result = agent.analyze(df)
    
    # Get optimization strategies if retriever available
    optimization_strategies = None
    if retriever:
        try:
            optimization_strategies = retriever.retrieve(
                query=f"optimization strategies for {request.platform} {request.objective}",
                top_k=3,
                filters={'platform': request.platform, 'objective': request.objective}
            )
        except Exception as e:
            from loguru import logger
            logger.debug(f"Retriever failed: {e}")
    
    # Get benchmarks if available
    benchmark_data = None
    if benchmarks:
        try:
            benchmark_data = benchmarks.get_benchmarks(
                platform=request.platform,
                objective=request.objective
            )
        except Exception as e:
            from loguru import logger
            logger.debug(f"Benchmarks failed: {e}")
    
    return {
        "status": "success",
        "analysis": {
            "insights": analysis_result.get('insights', {}),
            "recommendations": analysis_result.get('recommendations', []),
            "patterns": analysis_result.get('patterns', {})
        },
        "optimization_strategies": optimization_strategies,
        "benchmarks": benchmark_data,
        "components_used": {
            "reasoning_agent": True,
            "retriever": retriever is not None,
            "benchmarks": benchmarks is not None
        }
    }


# ============================================================================
# Example 3: Inject Container Directly
# ============================================================================

@router.post("/analyze-flexible")
async def analyze_flexible(
    request: CampaignAnalysisRequest,
    container: ApplicationContainer = Depends(get_di_container)
):
    """
    Flexible analysis using container directly.
    
    Use when you need:
    - Dynamic component selection
    - Access to multiple components
    - Runtime component switching
    """
    # Get components from container
    agent = container.agents.get('enhanced_reasoning_agent')
    validated_agent = container.agents.get('validated_reasoning_agent')
    
    if not agent and not validated_agent:
        raise HTTPException(status_code=503, detail="No agents available")
    
    # Choose agent based on request (example)
    selected_agent = validated_agent if validated_agent else agent
    
    # Convert to DataFrame
    df = pd.DataFrame(request.data)
    df['Platform'] = request.platform
    
    # Analyze
    result = selected_agent.analyze(df)
    
    return {
        "status": "success",
        "agent_used": "validated" if validated_agent else "standard",
        "result": result
    }


# ============================================================================
# Example 4: Health Check with DI Status
# ============================================================================

@router.get("/di-health")
async def di_health_check(
    container: ApplicationContainer = Depends(get_di_container)
):
    """
    Health check showing DI container status.
    
    Useful for:
    - Monitoring DI system
    - Debugging dependency issues
    - Verifying component availability
    """
    # Check component availability
    components = {
        "knowledge": {
            "retriever": container.knowledge.get('hybrid_retriever') is not None,
            "benchmarks": container.knowledge.get('benchmark_engine') is not None,
        },
        "agents": {
            "reasoning": container.agents.get('enhanced_reasoning_agent') is not None,
            "validated": container.agents.get('validated_reasoning_agent') is not None,
            "vision": container.agents.get('vision_agent') is not None,
        },
        "services": {
            "analytics": container.services.get('analytics_expert') is not None,
            "user": container.services.get('user_service') is not None,
        }
    }
    
    # Calculate overall health
    total_components = sum(
        sum(1 for _ in category.values())
        for category in components.values()
    )
    available_components = sum(
        sum(1 for available in category.values() if available)
        for category in components.values()
    )
    
    health_percentage = (available_components / total_components * 100) if total_components > 0 else 0
    
    return {
        "status": "healthy" if health_percentage >= 50 else "degraded",
        "health_percentage": round(health_percentage, 1),
        "components": components,
        "summary": {
            "total": total_components,
            "available": available_components,
            "unavailable": total_components - available_components
        }
    }


# ============================================================================
# Example 5: Benchmark Comparison Endpoint
# ============================================================================

@router.get("/benchmarks/{platform}/{objective}")
async def get_benchmarks(
    platform: str,
    objective: str,
    benchmarks: Optional[IBenchmarkEngine] = Depends(get_benchmark_engine_dep)
):
    """
    Get benchmark data for platform and objective.
    
    Demonstrates:
    - Simple dependency injection
    - Protocol-based typing
    - Error handling
    """
    if not benchmarks:
        raise HTTPException(
            status_code=503,
            detail="Benchmark engine not available"
        )
    
    try:
        benchmark_data = benchmarks.get_benchmarks(
            platform=platform,
            objective=objective
        )
        
        return {
            "status": "success",
            "platform": platform,
            "objective": objective,
            "benchmarks": benchmark_data
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get benchmarks: {str(e)}"
        )
