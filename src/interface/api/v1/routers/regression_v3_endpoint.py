"""
Enhanced Regression V3 Endpoint - Production ML with Comprehensive Metrics

Uses new modular regression architecture with:
- Comprehensive metrics (MAE, RMSE, MAPE, SMAPE, R²)
- Multicollinearity detection (VIF, correlation)
- Residual diagnostics
- Stakeholder-friendly explanations
- Prediction intervals

Author: Senior ML Expert
"""

# Add this to the imports section at the top of reports.py
from src.engine.analytics.regression import RegressionPipeline as RegressionPipelineV3
import pandas as pd

# Add this new endpoint after the dimension-importance endpoint
@router.get("/regression/v3")
@limiter.limit("10/minute")
async def get_regression_v3(
    request: Request,
    target: str = Query(..., description="Target metric (e.g., 'conversions', 'roas')"),
    features: str = Query(..., description="Comma-separated features (e.g., 'spend,impressions,clicks')"),
    models: str = Query("Ridge,Random Forest,XGBoost", description="Models to run"),
    encode_dimensions: Optional[str] = Query(None, description="Categorical dimensions to encode"),
    test_size: float = Query(0.2, description="Test set proportion"),
    quick_mode: bool = Query(True, description="Use quick hyperparameter search"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    platforms: Optional[str] = Query(None, description="Comma-separated platforms to filter"),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Enhanced Regression Analysis V3 - Production ML with Comprehensive Diagnostics
    
    New Features:
    - MAE, RMSE, MAPE, SMAPE (not just R²)
    - VIF analysis for multicollinearity
    - Residual distribution analysis
    - Prediction intervals
    - Stakeholder-friendly explanations
    - Only 3 models (Ridge, RF, XGBoost)
    
    Example:
        GET /campaigns/regression/v3?target=conversions&features=spend,impressions&encode_dimensions=funnel,platform
    """
    try:
        from loguru import logger
        
        duckdb_mgr = get_duckdb_manager()
        if not duckdb_mgr.has_data():
            return {"success": False, "error": "No data found. Please upload a dataset first."}
        
        # Build filters
        filter_params = {}
        if platforms:
            sample_df = duckdb_mgr.get_campaigns(limit=1)
            platform_col = find_column(sample_df, 'platform')
            if platform_col:
                filter_params[platform_col] = platforms
        
        # Load data
        df = duckdb_mgr.get_campaigns(filters=filter_params if filter_params else None)
        if df.empty:
            return {"success": False, "error": "No data found matching your filters."}
        
        # Apply date filters
        date_col = find_column(df, 'date')
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            if start_date:
                df = df[df[date_col] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df[date_col] <= pd.to_datetime(end_date)]
        
        # Parse inputs
        feature_list = [f.strip() for f in features.split(',') if f.strip()]
        model_list = [m.strip() for m in models.split(',') if m.strip()]
        dimension_list = [d.strip() for d in encode_dimensions.split(',') if encode_dimensions and d.strip()] or None
        
        # Validate columns
        available_features = [f for f in feature_list if f in df.columns]
        if not available_features:
            return {"success": False, "error": f"No valid features found. Available: {list(df.columns)}"}
        
        if target not in df.columns:
            return {"success": False, "error": f"Target '{target}' not found. Available: {list(df.columns)}"}
        
        logger.info(f"Regression V3: {len(df)} rows, target={target}, features={available_features}, models={model_list}")

        # Calculate feature stats for simulator (min/max/mean)
        feature_stats = {}
        for feat in available_features:
            if feat in df.columns and pd.api.types.is_numeric_dtype(df[feat]):
                feature_stats[feat] = {
                    "min": float(df[feat].min()),
                    "max": float(df[feat].max()),
                    "mean": float(df[feat].mean()),
                    "std": float(df[feat].std())
                }
        
        # Check minimum data requirement
        if len(df) < 100:
            return {
                "success": False,
                "error": f"Insufficient data: {len(df)} rows (minimum 100 required)"
            }
        
        # Run new pipeline
        pipeline = RegressionPipelineV3(
            models_to_run=model_list,
            quick_mode=quick_mode
        )
        
        result = pipeline.run(
            df=df,
            target=target,
            features=available_features,
            test_size=test_size,
            encode_dimensions=dimension_list
        )
        
        # Build comprehensive API response
        api_response = {
            "success": True,
            "model": {
                "type": result.best_model_name,
                "reason": f"Best test R² among {len(result.all_models)} models",
                "confidence": "High" if result.metrics.train_test_gap < 0.10 else "Moderate"
            },
            "performance": result.metrics.to_dict(),
            "diagnostics": {
                "multicollinearity": result.vif_analysis,
                "correlation": {
                    "high_correlations": result.correlation_analysis.get("high_correlations", []),
                    "summary": result.correlation_analysis.get("summary", {})
                },
                "residuals": result.residual_diagnostics.to_dict(),
                "feature_coverage": result.feature_coverage
            },
            "feature_insights": result.coefficient_insights,
            "predictions": {
                "sample": result.predictions.head(20).to_dict('records'),
                "residual_stats": {
                    "mean": float(result.predictions['residual'].mean()),
                    "std": float(result.predictions['residual'].std()),
                    "min": float(result.predictions['residual'].min()),
                    "max": float(result.predictions['residual'].max())
                }
            },
            "executive_summary": result.executive_summary,
            "coefficients": result.coefficients,
            "explanations": result.shap_data,
            "feature_stats": feature_stats,
            "model_comparison": [
                {
                    "model": name,
                    "training_time": model.training_time,
                    "hyperparameters": model.hyperparameters
                }
                for name, model in result.all_models.items()
            ]
        }
        
        return api_response
        
    except Exception as e:
        logger.error(f"Regression V3 failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
