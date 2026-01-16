"""
Lightweight Backend for Regression V3 Testing
No AI agents, no LangGraph, no RAG - just regression and data access
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Set environment variables
os.environ.setdefault("JWT_SECRET_KEY", "lightweight_backend_for_regression_testing")
os.environ.setdefault("SKIP_LANCEDB", "1")

app = FastAPI(
    title="Regression V3 Lightweight Backend",
    description="Minimal backend for testing regression module",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import only what we need
from src.core.database.duckdb_manager import get_duckdb_manager
from src.engine.analytics.regression import RegressionPipeline

@app.get("/")
def root():
    return {
        "status": "running",
        "message": "Lightweight Regression V3 Backend",
        "endpoints": {
            "regression_v3": "/api/v1/campaigns/regression/v3",
            "columns": "/api/v1/campaigns/columns",
            "health": "/health"
        }
    }

@app.get("/health")
def health():
    return {"status": "healthy", "service": "regression-v3-backend"}

@app.get("/api/v1/campaigns/columns")
def get_columns():
    """Get available columns from campaign data"""
    try:
        db = get_duckdb_manager()
        
        # Get columns from campaigns table
        with db.connection() as conn:
            result = conn.execute("SELECT * FROM campaigns LIMIT 0").df()
            columns = result.columns.tolist()
        
        return {
            "success": True,
            "columns": columns,
            "count": len(columns)
        }
    except Exception as e:
        return {
            "success": False,
            "columns": [
                "Total Spent", "Spend", "Impressions", "Clicks", "Conversions",
                "Revenue", "CTR", "CPC", "CPA", "ROAS", "Reach", "Frequency"
            ],
            "error": str(e)
        }

@app.get("/api/v1/campaigns/regression/v3")
def regression_v3(
    target: str = Query(..., description="Target variable"),
    features: str = Query(..., description="Comma-separated feature names"),
    models: str = Query("Ridge", description="Models to run"),
    encode_dimensions: Optional[str] = Query(None, description="Dimensions to one-hot encode"),
    quick_mode: bool = Query(True, description="Use quick mode for faster results")
):
    """
    Regression V3 endpoint with comprehensive diagnostics
    """
    try:
        # Load data
        db = get_duckdb_manager()
        df = db.get_campaigns()  # Use the proper method
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No campaign data found")
        
        # Parse parameters
        feature_list = [f.strip() for f in features.split(",")]
        model_list = [m.strip() for m in models.split(",")]
        dimension_list = [d.strip() for d in encode_dimensions.split(",")] if encode_dimensions else []
        
        # Validate columns exist
        missing_cols = []
        for col in [target] + feature_list:
            if col not in df.columns:
                missing_cols.append(col)
        
        if missing_cols:
            raise HTTPException(
                status_code=400,
                detail=f"Columns not found: {', '.join(missing_cols)}. Available: {', '.join(df.columns.tolist())}"
            )
        
        # Validate data quality
        # Check for null values
        null_counts = df[[target] + feature_list].isnull().sum()
        null_pct = (null_counts / len(df) * 100).round(1)
        
        issues = []
        if null_pct[target] > 50:
            issues.append(f"Target '{target}' has {null_pct[target]}% null values")
        
        for feat in feature_list:
            if null_pct[feat] > 50:
                issues.append(f"Feature '{feat}' has {null_pct[feat]}% null values")
        
        if issues:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "Data quality issues detected",
                    "issues": issues,
                    "suggestion": "Try different columns with less missing data",
                    "null_percentages": null_pct.to_dict()
                }
            )
        
        # Drop rows with nulls in target or features
        df_clean = df[[target] + feature_list].dropna()
        
        if len(df_clean) < 100:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient data after removing nulls: {len(df_clean)} rows (need at least 100)"
            )
        
        # Auto-detect categorical columns
        categorical_cols = df_clean[feature_list].select_dtypes(include=['object', 'string']).columns.tolist()
        
        # Add to dimension list if not already there
        for col in categorical_cols:
            if col not in dimension_list:
                dimension_list.append(col)
        
        # Check for variance (only numeric columns)
        numeric_cols = df_clean.select_dtypes(include=['number']).columns.tolist()
        if numeric_cols:
            variances = df_clean[numeric_cols].var()
            zero_var_cols = variances[variances == 0].index.tolist()
            
            if zero_var_cols:
                # Only raise if it's a feature we actually need (not just metadata)
                critical_zero_var = [c for c in zero_var_cols if c in feature_list]
                if critical_zero_var:
                     raise HTTPException(
                        status_code=400,
                        detail=f"Features have zero variance (all same value): {', '.join(critical_zero_var)}"
                    )
        
        # Run regression
        pipeline = RegressionPipeline(
            models_to_run=model_list,
            quick_mode=quick_mode
        )
        result = pipeline.run(
            df=df_clean,
            target=target,
            features=feature_list,
            encode_dimensions=dimension_list if dimension_list else None
        )
        
        # Build response
        response = {
            "success": True,
            "model": {
                "type": result.best_model_name,
                "reason": f"Best test R² among {len(model_list)} models",
                "confidence": "High" if result.metrics.r2_test > 0.7 else "Moderate" if result.metrics.r2_test > 0.5 else "Low"
            },
            "performance": result.metrics.to_dict()["performance"],
            "diagnostics": {
                "multicollinearity": result.vif_analysis,
                "correlation": result.correlation_analysis,
                "residuals": {
                    "distribution": {
                        "mean": result.residual_diagnostics.mean,
                        "std": result.residual_diagnostics.std,
                        "skewness": result.residual_diagnostics.skewness,
                        "kurtosis": result.residual_diagnostics.kurtosis
                    },
                    "normality_test": {
                        "shapiro_p_value": float(result.residual_diagnostics.shapiro_p_value),
                        "is_normal": bool(result.residual_diagnostics.shapiro_p_value > 0.05),
                        "interpretation": "Residuals are normally distributed" if result.residual_diagnostics.shapiro_p_value > 0.05 else "Residuals are not normally distributed"
                    }
                }
            },
            "feature_insights": result.coefficient_insights[:10],  # Top 10
            "explanations": result.shap_data if hasattr(result, 'shap_data') else None,
            "predictions": {
                "sample": result.predictions.to_dict("records")[:20],  # First 20
                "residual_stats": {
                    "mean": float(result.predictions["residual"].mean()),
                    "std": float(result.predictions["residual"].std()),
                    "min": float(result.predictions["residual"].min()),
                    "max": float(result.predictions["residual"].max())
                }
            },
            "executive_summary": result.executive_summary,
            "model_comparison": [
                {
                    "model": name,
                    "r2_test": model.metrics.r2_test if hasattr(model, 'metrics') else 0,
                    "mae": model.metrics.mae if hasattr(model, 'metrics') else 0
                }
                for name, model in result.all_models.items()
            ] if hasattr(result, 'all_models') else []
        }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "type": type(e).__name__,
                "traceback": traceback.format_exc()
            }
        )

if __name__ == "__main__":
    import uvicorn
    print("\n" + "="*60)
    print("🚀 Starting Lightweight Regression V3 Backend")
    print("="*60)
    print("📍 Server: http://localhost:8000")
    print("📊 Regression V3: http://localhost:8000/api/v1/campaigns/regression/v3")
    print("📋 Columns: http://localhost:8000/api/v1/campaigns/columns")
    print("="*60 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
