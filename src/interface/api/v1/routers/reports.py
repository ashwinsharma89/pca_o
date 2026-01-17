"""
Reports Router - Chart data, regression analysis, analytics snapshots

Handles reporting operations:
- Chart data generation with dynamic schema handling
- Regression analysis with multiple model options
- Analytics snapshots for dashboards
"""

from fastapi import APIRouter, HTTPException, Depends, Request, Query
from typing import Dict, Any, Optional, List
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from loguru import logger as loguru_logger

from src.interface.api.middleware.auth import get_current_user
from src.interface.api.middleware.rate_limit import limiter
from src.core.database.connection import get_db
from src.core.database.duckdb_manager import get_duckdb_manager
from src.platform.models.campaign import ReportTemplate
from src.engine.reports.dynamic_aggregator import DynamicAggregator
from src.engine.analytics.regression import RegressionPipeline, RegressionResult, RecommendationEngineV2
from src.core.schema.columns import Columns
from src.core.utils.column_mapping import find_column

# Phase 4: Observability & Audit
from src.core.utils.opentelemetry_config import get_tracer
from src.core.utils.observability import metrics
from src.enterprise.audit import audit_logger, AuditEventType

tracer = get_tracer(__name__)



# New modular regression module (V3)
# from src.engine.analytics.regression import RegressionPipeline as RegressionPipelineV3



logger = logging.getLogger(__name__)

router = APIRouter(prefix="/campaigns", tags=["reports"])


@router.get("/chart-data")
async def get_chart_data(
    request: Request,
    x_axis: str = 'date',
    y_axis: str = 'spend',
    aggregation: str = "sum",
    group_by: Optional[str] = None,
    platforms: Optional[str] = Query(None, description="Comma-separated list of platforms"),
    channels: Optional[str] = Query(None, description="Comma-separated list of channels"),
    regions: Optional[str] = Query(None, description="Comma-separated list of regions"),
    devices: Optional[str] = Query(None, description="Comma-separated list of devices"),
    funnels: Optional[str] = Query(None, description="Comma-separated list of funnels"),
    year: Optional[int] = Query(None, description="Filter by year"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get aggregated data for charts with dynamic schema handling."""
    try:
        duckdb_mgr = get_duckdb_manager()
        if not duckdb_mgr.has_data():
            return {"data": []}
            
        with duckdb_mgr.connection() as conn:
            # Introspection: Get actual column names
            schema_df = conn.execute(f"DESCRIBE SELECT * FROM {duckdb_mgr.get_optimized_table()}").fetchdf()
            cols = {row['column_name'].lower(): row['column_name'] for _, row in schema_df.iterrows()}
            
            def resolve(key):
                aliases = {
                    'spend': ['total spent', 'spend', 'cost'],
                    'impressions': ['impressions'],
                    'clicks': ['clicks'],
                    'conversions': ['site visit', 'conversions', 'leads'],
                    'date': ['date', 'day'],
                    'platform': ['platform', 'source', 'account'],
                    'channel': ['channel', 'medium'],
                    'region': ['geographic_region', 'region', 'country'],
                    'device': ['device_type', 'device'],
                    'device_type': ['device_type', 'device'],
                    'funnel': ['funnel', 'funnel_stage'],
                    'funnel_stage': ['funnel', 'funnel_stage'],
                    'campaign': ['campaign', 'campaign_name'],
                    'name': ['campaign', 'campaign_name'],  # Frontend uses 'name' for campaign
                    'placement': ['placement', 'placement_name', 'placement_name_full'],
                    'ad_type': ['ad_type', 'creative_type', 'ad_format'],
                    'audience_segment': ['audience_segment', 'audience', 'targeting'],
                    'objective': ['objective', 'campaign_objective', 'goal'],
                    'bid_strategy': ['bid_strategy', 'bidding_strategy', 'bid_type']
                }
                for alias in aliases.get(key.lower(), []):
                    if alias.lower() in cols:
                        return f'"{cols[alias.lower()]}"'
                if key.lower() in cols:
                    return f'"{cols[key.lower()]}"'
                return f'"{key}"'
            
            db_x = resolve(x_axis)
            db_group = resolve(group_by) if group_by else None
            col_spend = resolve('spend')
            col_impressions = resolve('impressions')
            col_clicks = resolve('clicks')
            col_conversions = resolve('conversions')
            col_date = resolve('date')
            
            # Check date type for filtering
            date_col_name = col_date.strip('"').lower()
            date_type_row = schema_df[schema_df['column_name'].str.lower() == date_col_name]
            is_date_string = False
            if not date_type_row.empty:
                dtype = str(date_type_row.iloc[0]['column_type'])
                if 'VARCHAR' in dtype or 'STRING' in dtype:
                    is_date_string = True
            
            date_expr = f"CAST({col_date} AS DATE)" if is_date_string else col_date
            
            # Build WHERE clause
            where_clauses = ["1=1"]
            params = []
            
            if platforms:
                p_list = [p.strip() for p in platforms.split(',')]
                where_clauses.append(f'{resolve("platform")} IN ({", ".join(["?" for _ in p_list])})')
                params.extend(p_list)
            if channels:
                c_list = [c.strip() for c in channels.split(',')]
                where_clauses.append(f'{resolve("channel")} IN ({", ".join(["?" for _ in c_list])})')
                params.extend(c_list)
            if regions:
                r_list = [r.strip() for r in regions.split(',')]
                where_clauses.append(f'{resolve("region")} IN ({", ".join(["?" for _ in r_list])})')
                params.extend(r_list)
            if devices:
                d_list = [d.strip() for d in devices.split(',')]
                where_clauses.append(f'{resolve("device")} IN ({", ".join(["?" for _ in d_list])})')
                params.extend(d_list)
            if funnels:
                f_list = [f.strip() for f in funnels.split(',')]
                where_clauses.append(f'{resolve("funnel")} IN ({", ".join(["?" for _ in f_list])})')
                params.extend(f_list)
            if year:
                where_clauses.append(f"EXTRACT(YEAR FROM {date_expr}) = ?")
                params.append(int(year))
            if start_date:
                where_clauses.append(f"{date_expr} >= CAST(? AS DATE)")
                params.append(start_date)
            if end_date:
                where_clauses.append(f"{date_expr} <= CAST(? AS DATE)")
                params.append(end_date)
            
            where_sql = " AND ".join(where_clauses)
            
            # Build metric expressions
            y_metrics = [y.strip() for y in y_axis.split(',')]
            select_metrics = []
            
            for i, metric in enumerate(y_metrics):
                is_calculated = False
                y_lower = metric.lower()
                
                if y_lower in ['spend', 'cost']:
                    y_col_sql = f'COALESCE({col_spend}, 0)'
                elif y_lower == 'impressions':
                    y_col_sql = f'COALESCE({col_impressions}, 0)'
                elif y_lower == 'clicks':
                    y_col_sql = f'COALESCE({col_clicks}, 0)'
                elif y_lower == 'conversions':
                    y_col_sql = f'COALESCE({col_conversions}, 0)'
                elif y_lower == 'ctr':
                    y_col_sql = f'CASE WHEN SUM(COALESCE({col_impressions}, 0)) > 0 THEN (SUM(COALESCE({col_clicks}, 0)) / SUM(COALESCE({col_impressions}, 0))) * 100 ELSE 0 END'
                    is_calculated = True
                elif y_lower == 'cpc':
                    y_col_sql = f'CASE WHEN SUM(COALESCE({col_clicks}, 0)) > 0 THEN SUM(COALESCE({col_spend}, 0)) / SUM(COALESCE({col_clicks}, 0)) ELSE 0 END'
                    is_calculated = True
                elif y_lower == 'cpa':
                    y_col_sql = f'CASE WHEN SUM(COALESCE({col_conversions}, 0)) > 0 THEN SUM(COALESCE({col_spend}, 0)) / SUM(COALESCE({col_conversions}, 0)) ELSE 0 END'
                    is_calculated = True
                elif y_lower == 'roas':
                    y_col_sql = f'CASE WHEN SUM(COALESCE({col_spend}, 0)) > 0 THEN (SUM(COALESCE({col_conversions}, 0)) * 50) / SUM(COALESCE({col_spend}, 0)) ELSE 0 END'
                    is_calculated = True
                else:
                    y_col_sql = f'COALESCE({resolve(metric)}, 0)'
                
                agg_func = aggregation.upper() if aggregation.upper() in ["SUM", "AVG", "COUNT", "MAX", "MIN"] else "SUM"
                metric_sql = y_col_sql if is_calculated else f"{agg_func}({y_col_sql})"
                select_metrics.append(f"{metric_sql} as y_{i}")

            select_y_clause = ", ".join(select_metrics)
            
            group_cols = [db_x]
            if db_group:
                group_cols.append(db_group)
            group_sql = ", ".join(group_cols)
            
            query = f"""
                SELECT 
                    {db_x} as x,
                    {f"{db_group} as group_col," if db_group else ""}
                    {select_y_clause}
                FROM {duckdb_mgr.get_optimized_table()}
                WHERE {where_sql}
                GROUP BY {group_sql}
                ORDER BY x ASC
            """
            
            df = conn.execute(query, params).fetchdf()
            
            if df.empty:
                return {"data": []}
            
            # Rename columns to match the requested axis names
            rename_map = {'x': x_axis}
            if 'group_col' in df.columns and group_by:
                rename_map['group_col'] = group_by
            
            # Map y_0, y_1... back to metric names
            for i, metric in enumerate(y_metrics):
                rename_map[f'y_{i}'] = metric
                
            df = df.rename(columns=rename_map)
            
            if x_axis in df.columns:
                df[x_axis] = df[x_axis].astype(str)
            
            return {"data": df.fillna(0).to_dict(orient="records")}
            
    except Exception as e:
        logger.error(f"Chart data error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"data": [], "error": str(e)}


@router.get("/regression")
@limiter.limit("10/minute")
async def get_regression_analysis(
    request: Request,
    target: str = Query("conversions", description="Target metric"),
    features: str = Query("spend,impressions,clicks", description="Comma-separated features"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platforms: Optional[str] = Query(None),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Simple regression analysis endpoint (v1 compatibility)."""
    # Simply redirect to v2 logic
    return await get_regression_analysis_v2(
        request=request,
        target=target,
        features=features,
        start_date=start_date,
        end_date=end_date,
        platforms_filter=platforms, # Use platforms_filter for v2
        current_user=current_user
    )


@router.get("/analytics-snapshot")
@limiter.limit("20/minute")
async def get_analytics_snapshot(
    request: Request,
    platforms: Optional[str] = Query(None, description="Comma-separated list of platforms to filter"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get a batched snapshot of data for the Analytics Studio using DuckDB.
    
    Returns data in format expected by frontend:
    - success: bool
    - platforms: list of platform names
    - kpis: {total_spend, total_impressions, total_clicks, total_conversions}
    - main_chart: aggregated data by platform
    - quick_charts: {spend_by_channel, conversions_by_funnel, ctr_by_platform, clicks_by_objective}
    """
    try:
        duckdb_mgr = get_duckdb_manager()
        if not duckdb_mgr.has_data():
            return {"success": False, "error": "No data available", "platforms": [], "kpis": {}, "main_chart": [], "quick_charts": {}}

        filter_params = {}
        if platforms:
            sample_df = duckdb_mgr.get_campaigns(limit=1)
            platform_col = find_column(sample_df, 'platform')
            if platform_col:
                filter_params[platform_col] = [p.strip() for p in platforms.split(',')]

        df = duckdb_mgr.get_campaigns(filters=filter_params if filter_params else None)
        if df.empty:
            return {"success": False, "error": "No data found", "platforms": [], "kpis": {}, "main_chart": [], "quick_charts": {}}

        date_col = find_column(df, 'date')
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            if start_date:
                df = df[df[date_col] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df[date_col] <= pd.to_datetime(end_date)]

        # Find columns dynamically
        spend_col = find_column(df, 'spend')
        impr_col = find_column(df, 'impressions')
        clicks_col = find_column(df, 'clicks')
        conv_col = find_column(df, 'conversions')
        platform_col = find_column(df, 'platform')
        channel_col = find_column(df, 'channel')
        funnel_col = find_column(df, 'funnel')
        objective_col = find_column(df, 'objective')

        # Get unique platforms
        unique_platforms = df[platform_col].dropna().unique().tolist() if platform_col else []

        # Calculate KPIs
        total_spend = float(df[spend_col].sum()) if spend_col else 0
        total_impressions = int(df[impr_col].sum()) if impr_col else 0
        total_clicks = int(df[clicks_col].sum()) if clicks_col else 0
        total_conversions = int(df[conv_col].sum()) if conv_col else 0
        
        kpis = {
            "total_spend": total_spend,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "total_conversions": total_conversions,
            "ctr": round((total_clicks / total_impressions * 100) if total_impressions > 0 else 0, 2),
            "cpc": round(total_spend / total_clicks if total_clicks > 0 else 0, 2),
            "cpa": round(total_spend / total_conversions if total_conversions > 0 else 0, 2)
        }

        # Main chart: Spend by Platform
        main_chart = []
        if platform_col and spend_col:
            grouped = df.groupby(platform_col).agg({spend_col: 'sum'}).reset_index()
            grouped.columns = ['platform', 'spend']
            main_chart = grouped.fillna(0).to_dict(orient='records')

        # Quick Charts
        quick_charts = {
            "spend_by_channel": [],
            "conversions_by_funnel": [],
            "ctr_by_platform": [],
            "clicks_by_objective": []
        }

        # Spend by Channel
        if channel_col and spend_col:
            grouped = df.groupby(channel_col).agg({spend_col: 'sum'}).reset_index()
            grouped.columns = ['channel', 'spend']
            quick_charts["spend_by_channel"] = grouped.fillna(0).to_dict(orient='records')

        # Conversions by Funnel
        if funnel_col and conv_col:
            grouped = df.groupby(funnel_col).agg({conv_col: 'sum'}).reset_index()
            grouped.columns = ['funnel_stage', 'conversions']
            quick_charts["conversions_by_funnel"] = grouped.fillna(0).to_dict(orient='records')

        # CTR by Platform
        if platform_col and impr_col and clicks_col:
            grouped = df.groupby(platform_col).agg({impr_col: 'sum', clicks_col: 'sum'}).reset_index()
            grouped.columns = ['platform', 'impressions', 'clicks']
            grouped['ctr'] = grouped.apply(
                lambda row: round((row['clicks'] / row['impressions'] * 100) if row['impressions'] > 0 else 0, 2), 
                axis=1
            )
            quick_charts["ctr_by_platform"] = grouped[['platform', 'ctr']].fillna(0).to_dict(orient='records')

        # Clicks by Objective
        if objective_col and clicks_col:
            grouped = df.groupby(objective_col).agg({clicks_col: 'sum'}).reset_index()
            grouped.columns = ['objective', 'clicks']
            quick_charts["clicks_by_objective"] = grouped.fillna(0).to_dict(orient='records')

        return {
            "success": True,
            "platforms": unique_platforms,
            "kpis": kpis,
            "main_chart": main_chart,
            "quick_charts": quick_charts,
            "row_count": len(df)
        }

    except Exception as e:
        logger.error(f"Analytics snapshot error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"success": False, "error": str(e), "platforms": [], "kpis": {}, "main_chart": [], "quick_charts": {}}


# =============================================================================
# ENHANCED REGRESSION ENDPOINT V2
# =============================================================================

@router.get("/regression/v2")
@limiter.limit("10/minute")
async def get_regression_analysis_v2(
    request: Request,
    target: str = Query(..., description="Target metric (e.g., 'conversions', 'roas')"),
    features: str = Query(..., description="Comma-separated features (e.g., 'spend,impressions,clicks')"),
    models: str = Query("Ridge,Random Forest,XGBoost", description="Comma-separated models to run"),
    encode_dimensions: Optional[str] = Query(None, description="Categorical dimensions to one-hot encode (e.g., 'platform,objective')"),
    prefer_interpretability: bool = Query(True, description="Prefer interpretable model over max accuracy"),
    quick_mode: bool = Query(True, description="Use reduced hyperparameter search for faster results"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    platforms_filter: Optional[str] = Query(None, alias="platforms"),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Production-grade Modular Regression Analysis.
    
    Uses Senior ML Expert pipeline with:
    - Multi-model comparison (Ridge, RF, XGBoost)
    - SHAP explainability for tree models
    - Robust feature scaling and VIF analysis
    - Standardized schema integration
    """
    import time
    start_api = time.time()
    
    # manual span
    if tracer:
        with tracer.start_as_current_span("get_regression_v2") as span:
            span.set_attribute("regression.target", target)
            span.set_attribute("regression.features", features)
            return await _run_regression_v2_logic(target, features, models, quick_mode, start_date, end_date, platforms_filter, encode_dimensions, start_api, current_user)
    else:
        return await _run_regression_v2_logic(target, features, models, quick_mode, start_date, end_date, platforms_filter, encode_dimensions, start_api, current_user)

async def _run_regression_v2_logic(target, features, models, quick_mode, start_date, end_date, platforms_filter, encode_dimensions, start_api, current_user):
    try:
        # Audit: Log analysis start
        audit_logger.log_event(
            event_type=AuditEventType.ANALYSIS_CREATED,
            user=current_user.get("email", "unknown"),
            action="Executed Regression Analysis V2",
            resource=f"target:{target}",
            details={"features": features, "models": models, "quick_mode": quick_mode}
        )
        
        duckdb_mgr = get_duckdb_manager()

        if not duckdb_mgr.has_data():
            return {"success": False, "error": "No data found. Please upload a dataset first."}
        
        # 1. Load data from DuckDB (SSOT)
        df = duckdb_mgr.get_campaigns(limit=100000)
        if df.empty:
            return {"success": False, "error": "Database is empty."}
        
        # 2. Filter & Clean
        # Platforms filter
        if platforms_filter:
            p_list = [p.strip().lower() for p in platforms_filter.split(',') if p.strip()]
            platform_col = Columns.PLATFORM.value
            if platform_col in df.columns:
                df = df[df[platform_col].str.lower().isin(p_list)]
        
        # Date filter
        date_col = Columns.DATE.value
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            if start_date:
                df = df[df[date_col] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df[date_col] <= pd.to_datetime(end_date)]
        
        if len(df) < 50:
            return {"success": False, "error": f"Insufficient data after filtering ({len(df)} rows). Need at least 50."}
        
        # 3. Parse Inputs
        feature_list = [f.strip() for f in features.split(',') if f.strip() and f.strip() in df.columns]
        model_list = [m.strip() for m in models.split(',') if m.strip()]
        dimensions = [d.strip() for d in encode_dimensions.split(',') if d.strip() and d.strip() in df.columns] if encode_dimensions else []
        
        if target not in df.columns:
            return {"success": False, "error": f"Target column '{target}' not found in data."}
        
        if not feature_list:
            return {"success": False, "error": "No valid features provided. Check column names."}
        
        # 4. Run Pipeline
        pipeline = RegressionPipeline(
            models_to_run=model_list,
            quick_mode=quick_mode
        )
        
        # This will now include internal spans
        result: RegressionResult = pipeline.run(
            df=df,
            target=target,
            features=feature_list,
            encode_dimensions=dimensions
        )
        
        # 5. Build API Response (frontend-compatible)
        recommendations = RecommendationEngineV2.generate(result)
        
        response = {
            "success": True,
            "best_model": result.best_model_name,
            "metrics": {
                "r2": float(result.metrics.r2_test),
                "mae": float(result.metrics.mae),
                "rmse": float(result.metrics.rmse),
                "mape": float(result.metrics.mape) if result.metrics.mape else 0
            },
            "coefficients": {k: float(v) for k, v in result.coefficients.items()},
            "insights": {
                "executive_summary": result.executive_summary,
                "driver_insights": [f"{item['feature']} has {item['impact'].lower()} {'positive' if item['coefficient'] > 0 else 'negative'} impact (rank {i+1})" for i, item in enumerate(result.coefficient_insights[:3])]
            },
            "recommendations": recommendations,
            "vif": result.vif_analysis,
            "shap": result.shap_data,
            "predictions_sample": result.predictions.head(20).to_dict(orient="records"),
            "model_comparison": [
                {
                    "model": name,
                    "r2": float(model.metrics.r2_test) if hasattr(model, 'metrics') else 0,
                    "mae": float(model.metrics.mae) if hasattr(model, 'metrics') else 0,
                    "training_time": float(model.training_time),
                    "params": model.hyperparameters
                }
                for name, model in result.all_models.items()
            ]
        }
        
        latency = (time.time() - start_api) * 1000
        metrics.record_time("api_regression_v2_latency_ms", latency)
        metrics.increment("api_requests_total", labels={"endpoint": "/regression/v2", "status": "200"})

        return response
        
    except Exception as e:
        loguru_logger.error(f"Regression V2 failed: {e}")
        import traceback
        loguru_logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# DIMENSION IMPORTANCE ENDPOINT
# =============================================================================

@router.get("/dimension-importance")
@limiter.limit("20/minute")
async def get_dimension_importance(
    request: Request,
    target: str = Query(..., description="Target metric to analyze (e.g., 'conversions', 'roas')"),
    dimensions: Optional[str] = Query(None, description="Comma-separated dimensions (auto-detect if not provided)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    platforms: Optional[str] = Query(None, description="Comma-separated platforms to filter"),
    include_interactions: bool = Query(True, description="Include dimension interaction analysis"),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Analyze which dimensions (platform, objective, placement, etc.) 
    have the most impact on the target metric.
    
    Returns:
    - Dimension rankings with importance scores
    - Effect sizes and statistical significance
    - Top performing values per dimension
    - Dimension interactions
    - Actionable recommendations
    """
    try:
        from src.engine.analytics.dimension_importance import (
            DimensionImportanceFramework,
            PermutationDimensionImportance
        )
        
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
        
        # Validate target
        if target not in df.columns:
            return {"success": False, "error": f"Target '{target}' not found. Available: {list(df.columns)}"}
        
        # Parse dimensions
        dimension_list = None
        if dimensions:
            dimension_list = [d.strip() for d in dimensions.split(',') if d.strip()]
        
        logger.info(f"Dimension Importance: {len(df)} rows, target={target}, dimensions={dimension_list or 'auto'}")
        
        # Run analysis
        framework = DimensionImportanceFramework()
        report = framework.analyze(df, target, dimension_list)
        
        if not report.success:
            return {"success": False, "error": "Analysis failed", "recommendations": report.recommendations}
        
        # Build response
        response = {
            "success": True,
            "target_metric": target,
            "sample_size": len(df),
            "dimensions_analyzed": report.dimensions_analyzed,
            
            # Rankings
            "rankings": report.rankings.to_dict(orient='records') if not report.rankings.empty else [],
            
            # Detailed results
            "dimension_details": [
                {
                    "dimension": r.dimension,
                    "importance_score": r.importance_score,
                    "effect_size": r.effect_size,
                    "effect": r.effect_interpretation,
                    "p_value": r.p_value,
                    "n_unique": r.n_unique,
                    "top_values": r.top_values,
                    "recommendation": r.recommendation
                }
                for r in report.results
            ],
            
            # Recommendations
            "recommendations": report.recommendations
        }
        
        # Add interactions if requested
        if include_interactions and report.interactions:
            response["interactions"] = report.interactions
        
        return response
        
    except Exception as e:
        logger.error(f"Dimension importance failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# =============================================================================
# ENHANCED REGRESSION V3 ENDPOINT - Production ML
# =============================================================================

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
    
    New Features vs V2:
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
