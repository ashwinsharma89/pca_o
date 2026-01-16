
from fastapi import APIRouter, Query
from typing import Optional, Dict, Any
import math

router = APIRouter()

@router.get("/regression/v3")
async def get_regression_v3_mock(
    target: str = Query(..., description="Target metric"),
    features: str = Query(..., description="Features"),
    models: str = Query("Ridge", description="Models"),
    **kwargs
):
    """
    MOCK Endpoint for Regression V3 to bypass DuckDB locks.
    Returns static data to verify Frontend UI.
    """
    
    # Mock Coefficients (Linear Model)
    # y = 0.5 * spend + 0.3 * impressions + 100
    coefs = {
        "spend": 0.5,
        "impressions": 0.02,
        "clicks": 1.5
    }
    
    # Feature Stats for Z-Score calculation (Simulator)
    feature_stats = {
        "spend": {"mean": 5000, "std": 1000, "min": 1000, "max": 10000},
        "impressions": {"mean": 100000, "std": 20000, "min": 50000, "max": 200000},
        "clicks": {"mean": 2000, "std": 500, "min": 500, "max": 5000}
    }
    
    # Mock Model Performance
    model_performance = {
        "Ridge": {
            "metrics": {"r2": 0.85, "mae": 50.0, "rmse": 75.0, "mape": 0.12, "smape": 0.11},
            "coefficients": coefs,
            "feature_importance": coefs,
            "prediction_interval": {"lower": [], "upper": []}
        }
    }
    
    return {
        "status": "success",
        "analysis_id": "mock_123",
        "target_metric": target,
        "models": model_performance,
        "best_model": "Ridge",
         # Data for charts (scatter plot)
        "data": [
            {"actual": 100 + i*10, "predicted": 105 + i*9.5, "residual": 5, "spend": 1000+i*100} 
            for i in range(20)
        ],
        "feature_stats": feature_stats,
        "shap_data": [] # Optional
    }
