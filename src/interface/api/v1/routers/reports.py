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
# from src.engine.analytics.regression import RegressionPipeline as RegressionPipelineV3, RegressionResult, RecommendationEngineV2
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
