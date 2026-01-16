"""
Analytics Router - Metrics, visualizations, dashboard stats

Handles analytics operations:
- Global metrics (portfolio summary)
- Visualizations (charts, grouped data)
- Dashboard stats (comparisons, sparklines)
"""

from fastapi import APIRouter, HTTPException, Depends, Request, Query, Response
from typing import Dict, Any, Optional, List
import logging
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from src.interface.api.middleware.auth import get_current_user
from src.interface.api.middleware.rate_limit import limiter
from src.core.utils.column_mapping import find_column, consolidate_metric_column, METRIC_COLUMN_ALIASES
from src.interface.api.v1.models import VisualizationsQuery
from ..response_models import GlobalMetricsResponse, VisualizationsResponse, DashboardStatsResponse, FilterOptionsResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/campaigns", tags=["analytics"])


@router.get("/metrics", response_model=GlobalMetricsResponse)
@limiter.limit("60/minute")
async def get_global_metrics(
    request: Request,
    response: Response,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get aggregated key metrics for all campaigns.
    Populates Portfolio Summary cards.
    """
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    try:
        from src.core.database.duckdb_manager import get_duckdb_manager
        duckdb_mgr = get_duckdb_manager()
        
        df = duckdb_mgr.get_campaigns_polars()
        
        if df.is_empty():
            return {
                "total_spend": 0,
                "total_impressions": 0,
                "total_clicks": 0,
                "total_conversions": 0,
                "avg_ctr": 0,
                "avg_cpc": 0,
                "avg_cpa": 0,
                "conversion_rate": 0
            }
            
        # Polars columns from DuckDB will match the Parquet file casing (normalized to snake_case)
        # But we implement fallback just in case
        cols = df.columns
        
        def get_col(name):
            # Check exact match (lowercase preference)
            if name.lower() in cols: return name.lower()
            if name.title() in cols: return name.title()
            # Check loose match
            for c in cols:
                if c.lower() == name.lower(): return c
            return None

        spend_c = get_col("spend")
        impr_c = get_col("impressions")
        clicks_c = get_col("clicks")
        conv_c = get_col("conversions")
        
        # Calculate sums (using defaults 0 if column missing)
        total_spend = df[spend_c].sum() if spend_c else 0.0
        total_impr = df[impr_c].sum() if impr_c else 0
        total_clicks = df[clicks_c].sum() if clicks_c else 0
        total_conv = df[conv_c].sum() if conv_c else 0
        
        avg_ctr = (total_clicks / total_impr * 100) if total_impr > 0 else 0
        avg_cpc = (total_spend / total_clicks) if total_clicks > 0 else 0
        avg_cpa = (total_spend / total_conv) if total_conv > 0 else 0
        conversion_rate = (total_conv / total_clicks * 100) if total_clicks > 0 else 0
        
        return {
            "total_spend": float(total_spend),
            "total_impressions": int(total_impr),
            "total_clicks": int(total_clicks),
            "total_conversions": int(total_conv),
            "avg_ctr": float(avg_ctr),
            "avg_cpc": float(avg_cpc),
            "avg_cpa": float(avg_cpa),
            "conversion_rate": float(conversion_rate)
        }
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))




@router.get("/filters", response_model=FilterOptionsResponse)
@limiter.limit("60/minute")
async def get_filter_options(
    request: Request,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get all available filter options (platforms, channels, etc.)
    """
    try:
        from src.core.database.duckdb_manager import get_duckdb_manager
        duckdb_mgr = get_duckdb_manager()
        
        # Get all data to determine available options
        # Note: In a production DB, we would use SELECT DISTINCT queries.
        # Since we load everything into memory/DuckDB, getting the DF is fine for now.
        df = duckdb_mgr.get_campaigns()
        
        if df.empty:
            return {
                "platforms": [], "channels": [], "funnels": [], 
                "objectives": [], "regions": [], "devices": [],
                "ad_types": [], "placements": []
            }
            
        # Helper to get unique sorted list
        def get_unique(col_name):
            col = find_column(df, col_name)
            if col and col in df.columns:
                return sorted([str(x) for x in df[col].dropna().unique() if x])
            return []

        return {
            "platforms": get_unique('platform'),
            "channels": get_unique('channel'),
            # Map 'funnel_stages' (frontend) to 'funnels' (model) if needed, 
            # but model has 'funnels'. Frontend 'funnel_stages' likely maps to 'funnel' column.
            "funnels": get_unique('funnel'),
            "objectives": get_unique('objective'),
            "regions": get_unique('region'),
            "devices": get_unique('device'),
            "ad_types": get_unique('ad_type'),
            "placements": get_unique('placement')
        }
    except Exception as e:
        logger.error(f"Failed to get filter options: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schema")
@limiter.limit("60/minute")
async def get_schema_info(
    request: Request,
    response: Response,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get schema information including available metrics and dimensions.
    Used by frontend to adapt UI based on available data.
    """
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    try:
        from src.core.database.duckdb_manager import get_duckdb_manager
        duckdb_mgr = get_duckdb_manager()
        
        # Get schema/columns from a lightweight query
        # We can just get one row or empty df to check columns if available,
        # but check_available_columns helper would be better.
        # For now, get 1 row.
        df = duckdb_mgr.get_campaigns(limit=1)
        
        has_data = duckdb_mgr.has_data()
        
        if df.empty and not has_data:
            return {
                "has_data": False,
                "metrics": {},
                "dimensions": {},
                "extra_metrics": [],
                "extra_dimensions": [],
                "all_columns": []
            }
            
        cols = df.columns.tolist()
        
        # Define standard columns to categorize
        std_metrics = ['spend', 'impressions', 'clicks', 'conversions', 'revenue', 'reach']
        std_dims = ['platform', 'channel', 'funnel', 'objective', 'region', 'device', 'ad_type', 'placement', 'campaign_name', 'ad_group_name', 'date']
        
        metrics_avail = {}
        for m in std_metrics:
            actual = find_column(df, m)
            metrics_avail[m] = bool(actual)
            
        # Infer derived metrics
        if metrics_avail.get('clicks') and metrics_avail.get('impressions'):
             metrics_avail['ctr'] = True
        
        if metrics_avail.get('spend') and metrics_avail.get('clicks'):
             metrics_avail['cpc'] = True
             
        if metrics_avail.get('spend') and metrics_avail.get('impressions'):
             metrics_avail['cpm'] = True

        if metrics_avail.get('spend') and metrics_avail.get('conversions'):
             metrics_avail['cpa'] = True
             
        if metrics_avail.get('revenue') and metrics_avail.get('spend'):
             metrics_avail['roas'] = True
            
        dims_avail = {}
        for d in std_dims:
            actual = find_column(df, d)
            dims_avail[d] = bool(actual)
            
        # Identify extras
        extra_metrics = []
        extra_dims = []
        
        known_cols = set([find_column(df, c) for c in std_metrics + std_dims if find_column(df, c)])
        
        import numpy as np
        for col in cols:
            if col in known_cols:
                continue
            
            # Simple heuristic for type
            if pd.api.types.is_numeric_dtype(df[col]):
                extra_metrics.append(col)
            else:
                extra_dims.append(col)
        
        return {
            "has_data": has_data,
            "metrics": metrics_avail,
            "dimensions": dims_avail,
            "extra_metrics": extra_metrics,
            "extra_dimensions": extra_dims,
            "all_columns": cols
        }
            
    except Exception as e:
        logger.error(f"Failed to get schema info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dimensions", response_model=VisualizationsResponse)
@limiter.limit("60/minute")
async def get_dimension_metrics(
    request: Request,
    params: VisualizationsQuery = Depends(),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get metrics broken down by dimensions (Region, Device, Audience, etc.).
    Powering the V2 Executive Overview charts.
    """
    logger.info(f"📊 /dimensions endpoint called - params={params}")
    try:
        from src.core.database.duckdb_manager import get_duckdb_manager

        
        duckdb_mgr = get_duckdb_manager()
        
        if not duckdb_mgr.has_data():
            return {"trend": [], "device": [], "platform": [], "channel": []}
        
        # Build filter parameters
        filter_params = {}
        sample_df = duckdb_mgr.get_campaigns(limit=1)
        
        if not sample_df.empty:
            mapping = {
                'platform': params.platforms,
                'funnel': params.funnel_stages,
                'channel': params.channels,
                'device': params.devices,
                'placement': params.placements,
                'region': params.regions,
                'ad_type': params.adTypes,
                'audience': params.audiences,
                'age': params.ages,
                'objective': params.objectives,
                'targeting': params.targetings
            }
            
            for key, val in mapping.items():
                if val:
                    actual_col = find_column(sample_df, key)
                    if actual_col:
                        filter_params[actual_col] = val
                    else:
                        fallback_map = {
                            'platform': 'Platform', 'funnel': 'Funnel', 'channel': 'Channel',
                            'device': 'Device_Type', 'placement': 'Placement',
                            'region': 'Geographic_Region', 'ad_type': 'Ad Type'
                        }
                        filter_params[fallback_map.get(key, key)] = val
        
        logger.info(f"DuckDB visualization filters: {filter_params}")
        df = duckdb_mgr.get_campaigns(filters=filter_params if filter_params else None)
        
        if df.empty:
            return {"trend": [], "device": [], "platform": [], "channel": []}
        
        # Apply date filtering
        date_col = find_column(df, 'date')
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            if params.start_date:
                try:
                    df = df[df[date_col] >= pd.to_datetime(params.start_date)]
                except Exception as e:
                    logger.warning(f"Could not parse start_date: {e}")
            if params.end_date:
                try:
                    df = df[df[date_col] <= pd.to_datetime(params.end_date)]
                except Exception as e:
                    logger.warning(f"Could not parse end_date: {e}")
            
            if df.empty:
                return {"trend": [], "device": [], "platform": [], "channel": []}
        
        # Find columns
        spend_col = find_column(df, 'spend')
        impr_col = find_column(df, 'impressions')
        clicks_col = find_column(df, 'clicks')
        conv_col = find_column(df, 'conversions')
        platform_col = find_column(df, 'platform')
        channel_col = find_column(df, 'channel')
        device_col = find_column(df, 'device')
        region_col = find_column(df, 'region')
        audience_col = find_column(df, 'audience')
        age_col = find_column(df, 'age')
        ad_type_col = find_column(df, 'ad_type')
        objective_col = find_column(df, 'objective')
        targeting_col = find_column(df, 'targeting')
        
        # Find revenue and reach columns once (not in loop)
        revenue_col = find_column(df, 'revenue')
        reach_col = find_column(df, 'reach')
        
        # Convert to Polars for fast aggregation
        pl_df = pl.from_pandas(df)
        
        def calc_metrics_polars(dimension_col: str, key_name: str = 'name') -> List[Dict]:
            """Aggregate metrics by dimension using Polars (5-10x faster than Pandas)."""
            if not dimension_col or dimension_col not in pl_df.columns:
                return []
            
            # Build aggregation expressions - cast to numeric to handle string columns
            agg_exprs = []
            if spend_col and spend_col in pl_df.columns:
                agg_exprs.append(pl.col(spend_col).cast(pl.Float64, strict=False).sum().alias("spend"))
            else:
                agg_exprs.append(pl.lit(0.0).alias("spend"))
                
            if impr_col and impr_col in pl_df.columns:
                agg_exprs.append(pl.col(impr_col).cast(pl.Int64, strict=False).sum().alias("impressions"))
            else:
                agg_exprs.append(pl.lit(0).alias("impressions"))
                
            if clicks_col and clicks_col in pl_df.columns:
                agg_exprs.append(pl.col(clicks_col).cast(pl.Int64, strict=False).sum().alias("clicks"))
            else:
                agg_exprs.append(pl.lit(0).alias("clicks"))
                
            if conv_col and conv_col in pl_df.columns:
                agg_exprs.append(pl.col(conv_col).cast(pl.Int64, strict=False).sum().alias("conversions"))
            else:
                agg_exprs.append(pl.lit(0).alias("conversions"))
                
            if revenue_col and revenue_col in pl_df.columns:
                agg_exprs.append(pl.col(revenue_col).cast(pl.Float64, strict=False).sum().alias("revenue"))
            else:
                agg_exprs.append(pl.lit(0.0).alias("revenue"))
                
            if reach_col and reach_col in pl_df.columns:
                agg_exprs.append(pl.col(reach_col).cast(pl.Int64, strict=False).sum().alias("reach"))
            else:
                agg_exprs.append(pl.lit(0).alias("reach"))
            
            # Execute aggregation
            result_df = pl_df.group_by(dimension_col).agg(agg_exprs)
            
            # Calculate derived metrics and convert to dicts
            result = []
            for row in result_df.iter_rows(named=True):
                spend = float(row.get("spend", 0) or 0)
                impressions = int(row.get("impressions", 0) or 0)
                clicks = int(row.get("clicks", 0) or 0)
                conversions = int(row.get("conversions", 0) or 0)
                revenue = float(row.get("revenue", 0) or 0)
                reach = int(row.get("reach", 0) or 0)
                
                ctr = (clicks / impressions * 100) if impressions > 0 else 0
                cpc = (spend / clicks) if clicks > 0 else 0
                cpa = (spend / conversions) if conversions > 0 else 0
                cpm = (spend / impressions * 1000) if impressions > 0 else 0
                roas = (revenue / spend) if spend > 0 else 0
                
                result.append({
                    key_name: str(row[dimension_col]) if row[dimension_col] is not None else "Unknown",
                    "spend": round(spend, 2), "impressions": impressions, "clicks": clicks,
                    "conversions": conversions, "revenue": round(revenue, 2), "reach": reach,
                    "ctr": round(ctr, 2), "cpc": round(cpc, 2), "cpa": round(cpa, 2),
                    "cpm": round(cpm, 2), "roas": round(roas, 2)
                })
            return result
        
        # Trend data using Polars
        trend_data = []
        if date_col and date_col in pl_df.columns:
            # Build aggregation for trend - cast to numeric
            trend_agg_exprs = []
            if spend_col and spend_col in pl_df.columns:
                trend_agg_exprs.append(pl.col(spend_col).cast(pl.Float64, strict=False).sum().alias("spend"))
            else:
                trend_agg_exprs.append(pl.lit(0.0).alias("spend"))
            if impr_col and impr_col in pl_df.columns:
                trend_agg_exprs.append(pl.col(impr_col).cast(pl.Int64, strict=False).sum().alias("impressions"))
            else:
                trend_agg_exprs.append(pl.lit(0).alias("impressions"))
            if clicks_col and clicks_col in pl_df.columns:
                trend_agg_exprs.append(pl.col(clicks_col).cast(pl.Int64, strict=False).sum().alias("clicks"))
            else:
                trend_agg_exprs.append(pl.lit(0).alias("clicks"))
            if conv_col and conv_col in pl_df.columns:
                trend_agg_exprs.append(pl.col(conv_col).cast(pl.Int64, strict=False).sum().alias("conversions"))
            else:
                trend_agg_exprs.append(pl.lit(0).alias("conversions"))
            if revenue_col and revenue_col in pl_df.columns:
                trend_agg_exprs.append(pl.col(revenue_col).cast(pl.Float64, strict=False).sum().alias("revenue"))
            else:
                trend_agg_exprs.append(pl.lit(0.0).alias("revenue"))
            if reach_col and reach_col in pl_df.columns:
                trend_agg_exprs.append(pl.col(reach_col).cast(pl.Int64, strict=False).sum().alias("reach"))
            else:
                trend_agg_exprs.append(pl.lit(0).alias("reach"))
            
            trend_df = pl_df.group_by(date_col).agg(trend_agg_exprs).sort(date_col)
            
            for row in trend_df.iter_rows(named=True):
                date_val = row[date_col]
                if date_val is None:
                    continue
                spend = float(row.get("spend", 0) or 0)
                impressions = int(row.get("impressions", 0) or 0)
                clicks = int(row.get("clicks", 0) or 0)
                conversions = int(row.get("conversions", 0) or 0)
                revenue = float(row.get("revenue", 0) or 0)
                reach = int(row.get("reach", 0) or 0)
                
                date_str = date_val.strftime("%Y-%m-%d") if hasattr(date_val, 'strftime') else str(date_val)
                
                trend_data.append({
                    "date": date_str,
                    "spend": round(spend, 2), "impressions": impressions, "clicks": clicks,
                    "conversions": conversions, "revenue": round(revenue, 2), "reach": reach,
                    "ctr": round((clicks / impressions * 100) if impressions > 0 else 0, 2),
                    "cpc": round((spend / clicks) if clicks > 0 else 0, 2),
                    "cpa": round((spend / conversions) if conversions > 0 else 0, 2)
                })
        
        return {
            "trend": trend_data,
            "device": calc_metrics_polars(device_col, 'device') if device_col else [],
            "platform": calc_metrics_polars(platform_col, 'platform') if platform_col else [],
            "channel": calc_metrics_polars(channel_col, 'channel') if channel_col else [],
            "region": calc_metrics_polars(region_col, 'region') if region_col else [],
            "audience": calc_metrics_polars(audience_col, 'audience') if audience_col else [],
            "age": calc_metrics_polars(age_col, 'age') if age_col else [],
            "ad_type": calc_metrics_polars(ad_type_col, 'ad_type') if ad_type_col else [],
            "objective": calc_metrics_polars(objective_col, 'objective') if objective_col else [],
            "targeting": calc_metrics_polars(targeting_col, 'targeting') if targeting_col else []
        }
        
    except Exception as e:
        logger.error(f"Failed to get global visualizations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard-stats", response_model=DashboardStatsResponse)
@limiter.limit("60/minute")
async def get_dashboard_stats(
    request: Request,
    params: VisualizationsQuery = Depends(),
    current_user: Dict[str, Any] = Depends(get_current_user),
    response: Response = None
):
    """
    Get aggregated dashboard stats including comparisons to previous period,
    sparkline data, and monthly performance tables.
    """
    if response:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        
    logger.info(f"📈 /dashboard-stats endpoint called - params={params}")
    try:
        from src.core.database.duckdb_manager import get_duckdb_manager
        
        duckdb_mgr = get_duckdb_manager()
        if not duckdb_mgr.has_data():
            return {"summary_groups": {}, "monthly_performance": [], "platform_performance": []}
        
        # Build filters
        filter_params = {}
        sample_df = duckdb_mgr.get_campaigns(limit=1)
        
        if not sample_df.empty:
            mapping = {
                'platform': params.platforms, 'funnel': params.funnel_stages,
                'channel': params.channels, 'device': params.devices,
                'placement': params.placements, 'region': params.regions,
                'ad_type': params.adTypes, 'audience': params.audiences,
                'age': params.ages, 'objective': params.objectives,
                'targeting': params.targetings
            }
            
            for key, val in mapping.items():
                if val:
                    actual_col = find_column(sample_df, key)
                    if actual_col:
                        filter_params[actual_col] = val
                    else:
                        fallback_map = {
                            'platform': 'Platform', 'funnel': 'Funnel', 'channel': 'Channel',
                            'device': 'Device_Type', 'placement': 'Placement',
                            'region': 'Geographic_Region', 'ad_type': 'Ad Type'
                        }
                        filter_params[fallback_map.get(key, key)] = val
        
        total_df = duckdb_mgr.get_campaigns(filters=filter_params if filter_params else None)
        if total_df.empty:
            return {"summary_groups": {}, "monthly_performance": [], "platform_performance": []}
        
        # Find metric columns directly (consolidate_metric_column returns column name, not series)
        spend_col = find_column(total_df, 'spend')
        impr_col = find_column(total_df, 'impressions')
        clicks_col = find_column(total_df, 'clicks')
        conv_col = find_column(total_df, 'conversions')
        reach_col = find_column(total_df, 'reach')
        revenue_col = find_column(total_df, 'revenue')
        
        date_col = find_column(total_df, 'date')
        platform_col = find_column(total_df, 'platform')
        
        if not date_col:
            return {"summary_groups": {}, "monthly_performance": [], "platform_performance": []}
        
        total_df[date_col] = pd.to_datetime(total_df[date_col], errors='coerce')
        total_df = total_df.dropna(subset=[date_col])
        
        # Determine date range
        if not params.start_date and not params.end_date:
            d1 = total_df[date_col].min()
            d2 = total_df[date_col].max()
        else:
            if not params.end_date:
                max_data_date = total_df[date_col].max()
                end_date_str = max_data_date.strftime("%Y-%m-%d")
            else:
                end_date_str = params.end_date
                
            if not params.start_date:
                start_date_str = (datetime.strptime(end_date_str, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
            else:
                start_date_str = params.start_date
            
            d1 = pd.to_datetime(start_date_str)
            d2 = pd.to_datetime(end_date_str)
        
        delta = d2 - d1
        curr_df = total_df[(total_df[date_col] >= d1) & (total_df[date_col] <= d2)]
        
        # Previous period (YoY)
        yoy_d1 = d1 - relativedelta(years=1)
        yoy_d2 = d2 - relativedelta(years=1)
        prev_df = total_df[(total_df[date_col] >= yoy_d1) & (total_df[date_col] <= yoy_d2)]
        
        if prev_df.empty:
            prev_d1 = d1 - delta
            prev_d2 = d1 - timedelta(days=1)
            prev_df = total_df[(total_df[date_col] >= prev_d1) & (total_df[date_col] <= prev_d2)]
        
        def get_summary(df):
            if df.empty:
                return {"spend": 0, "impressions": 0, "reach": 0, "clicks": 0, "conversions": 0,
                        "ctr": 0, "cpc": 0, "cpm": 0, "cpa": 0, "roas": 0, "revenue": 0}
            s = float(df[spend_col].sum()) if spend_col else 0
            i = int(df[impr_col].sum()) if impr_col else 0
            r = int(df[reach_col].sum()) if reach_col else 0
            c = int(df[clicks_col].sum()) if clicks_col else 0
            cv = int(df[conv_col].sum()) if conv_col else 0
            revenue_sum = float(df[revenue_col].sum()) if revenue_col else 0
            
            return {
                "spend": round(s, 2), "impressions": i, "reach": r, "clicks": c, "conversions": cv,
                "ctr": round((c / i * 100) if i > 0 else 0, 2),
                "cpc": round((s / c) if c > 0 else 0, 2),
                "cpm": round((s / i * 1000) if i > 0 else 0, 2),
                "cpa": round((s / cv) if cv > 0 else 0, 2),
                "roas": round((revenue_sum / s) if s > 0 else 0, 2),
                "revenue": round(revenue_sum, 2)
            }
        
        curr_summary = get_summary(curr_df)
        prev_summary = get_summary(prev_df)
        
        # ============ POLARS OPTIMIZATION ============
        # Convert to Polars for fast aggregation
        pl_total = pl.from_pandas(total_df)
        pl_curr = pl.from_pandas(curr_df) if not curr_df.empty else None
        
        # Build reusable aggregation expressions - cast to numeric to handle string columns
        def build_agg_exprs():
            exprs = []
            if spend_col and spend_col in pl_total.columns:
                exprs.append(pl.col(spend_col).cast(pl.Float64, strict=False).sum().alias("spend"))
            else:
                exprs.append(pl.lit(0.0).alias("spend"))
            if impr_col and impr_col in pl_total.columns:
                exprs.append(pl.col(impr_col).cast(pl.Int64, strict=False).sum().alias("impressions"))
            else:
                exprs.append(pl.lit(0).alias("impressions"))
            if reach_col and reach_col in pl_total.columns:
                exprs.append(pl.col(reach_col).cast(pl.Int64, strict=False).sum().alias("reach"))
            else:
                exprs.append(pl.lit(0).alias("reach"))
            if clicks_col and clicks_col in pl_total.columns:
                exprs.append(pl.col(clicks_col).cast(pl.Int64, strict=False).sum().alias("clicks"))
            else:
                exprs.append(pl.lit(0).alias("clicks"))
            if conv_col and conv_col in pl_total.columns:
                exprs.append(pl.col(conv_col).cast(pl.Int64, strict=False).sum().alias("conversions"))
            else:
                exprs.append(pl.lit(0).alias("conversions"))
            if revenue_col and revenue_col in pl_total.columns:
                exprs.append(pl.col(revenue_col).cast(pl.Float64, strict=False).sum().alias("revenue"))
            else:
                exprs.append(pl.lit(0.0).alias("revenue"))
            return exprs
        
        agg_exprs = build_agg_exprs()
        
        def row_to_summary(row):
            s = float(row.get("spend", 0) or 0)
            i = int(row.get("impressions", 0) or 0)
            r = int(row.get("reach", 0) or 0)
            c = int(row.get("clicks", 0) or 0)
            cv = int(row.get("conversions", 0) or 0)
            rev = float(row.get("revenue", 0) or 0)
            return {
                "spend": round(s, 2), "impressions": i, "reach": r, "clicks": c, "conversions": cv,
                "ctr": round((c / i * 100) if i > 0 else 0, 2),
                "cpc": round((s / c) if c > 0 else 0, 2),
                "cpm": round((s / i * 1000) if i > 0 else 0, 2),
                "cpa": round((s / cv) if cv > 0 else 0, 2),
                "roas": round((rev / s) if s > 0 else 0, 2),
                "revenue": round(rev, 2)
            }
        
        # Sparkline using Polars
        sparkline_data = []
        if pl_curr is not None and date_col in pl_curr.columns:
            spark_df = pl_curr.group_by(date_col).agg(agg_exprs).sort(date_col)
            for row in spark_df.iter_rows(named=True):
                date_val = row[date_col]
                if date_val is None:
                    continue
                sparkline_data.append({
                    "date": date_val.strftime("%Y-%m-%d") if hasattr(date_val, 'strftime') else str(date_val),
                    "spend": float(row.get("spend", 0) or 0),
                    "impressions": int(row.get("impressions", 0) or 0),
                    "clicks": int(row.get("clicks", 0) or 0),
                    "conversions": int(row.get("conversions", 0) or 0)
                })
        
        # Monthly performance using Polars
        monthly_perf = []
        channel_col = find_column(total_df, 'channel')
        
        # Add Month column to Polars df
        if date_col in pl_total.columns:
            pl_total = pl_total.with_columns(
                pl.col(date_col).dt.strftime("%Y-%m").alias("Month")
            )
        
        if channel_col and channel_col in pl_total.columns:
            monthly_df = pl_total.group_by(["Month", channel_col]).agg(agg_exprs)
            for row in monthly_df.iter_rows(named=True):
                monthly_perf.append({"month": row["Month"], "channel": str(row[channel_col]), **row_to_summary(row)})
        elif "Month" in pl_total.columns:
            monthly_df = pl_total.group_by("Month").agg(agg_exprs)
            for row in monthly_df.iter_rows(named=True):
                monthly_perf.append({"month": row["Month"], **row_to_summary(row)})
        monthly_perf.sort(key=lambda x: x['month'], reverse=True)
        
        # Platform performance using Polars
        platform_perf = []
        if platform_col and platform_col in pl_total.columns:
            if channel_col and channel_col in pl_total.columns:
                plat_df = pl_total.group_by(["Month", platform_col, channel_col]).agg(agg_exprs)
                for row in plat_df.iter_rows(named=True):
                    platform_perf.append({"month": row["Month"], "platform": str(row[platform_col]), "channel": str(row[channel_col]), **row_to_summary(row)})
            else:
                plat_df = pl_total.group_by(["Month", platform_col]).agg(agg_exprs)
                for row in plat_df.iter_rows(named=True):
                    platform_perf.append({"month": row["Month"], "platform": str(row[platform_col]), **row_to_summary(row)})
        platform_perf.sort(key=lambda x: (x.get('month', ''), -x['spend']), reverse=True)
        
        # Funnel performance using Polars
        funnel_perf = []
        funnel_col = find_column(total_df, 'funnel')
        if funnel_col and funnel_col in pl_total.columns:
            funnel_df = pl_total.group_by(funnel_col).agg(agg_exprs)
            for row in funnel_df.iter_rows(named=True):
                stage = row[funnel_col]
                if stage is None or stage == 'Unknown':
                    continue
                funnel_perf.append({"funnel": str(stage), **row_to_summary(row)})
        funnel_perf.sort(key=lambda x: -x['spend'])
        
        # Channel by funnel using Polars
        channel_by_funnel = []
        if channel_col and funnel_col and channel_col in pl_total.columns and funnel_col in pl_total.columns:
            cbf_df = pl_total.group_by([channel_col, funnel_col]).agg(agg_exprs)
            for row in cbf_df.iter_rows(named=True):
                ch = row[channel_col]
                fn = row[funnel_col]
                if ch is None or fn is None or ch == 'Unknown' or fn == 'Unknown':
                    continue
                channel_by_funnel.append({"channel": str(ch), "funnel": str(fn), **row_to_summary(row)})
        channel_by_funnel.sort(key=lambda x: -x['spend'])
        
        return {
            "summary_groups": {"current": curr_summary, "previous": prev_summary, "sparkline": sparkline_data},
            "monthly_performance": monthly_perf,
            "platform_performance": platform_perf,
            "funnel": funnel_perf,
            "channel_by_funnel": channel_by_funnel
        }
        
    except Exception as e:
        logger.error(f"Dashboard stats failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics-snapshot")
async def get_analytics_snapshot(
    response: Response,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: dict = Depends(get_current_user)
):
    """
    Composite endpoint for In-Depth Analysis page.
    Returns KPIs, available platforms, main chart data, and quick charts in one go.
    """
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    
    try:
        from src.core.database.duckdb_manager import get_duckdb_manager
        duckdb_mgr = get_duckdb_manager()
        
        # 1. Get Base Data (filters applied)
        filters = {}
        if start_date and end_date:
            filters["date_range"] = (start_date, end_date)
            
        # Use Polars for efficiency
        df = duckdb_mgr.get_campaigns_polars(filters=filters)
        
        if df.height == 0:
            return {
                "success": True,
                "platforms": [],
                "kpis": {
                    "total_spend": 0,
                    "total_impressions": 0,
                    "total_clicks": 0,
                    "total_conversions": 0
                },
                "main_chart": [],
                "quick_charts": {
                    "spend_by_channel": [],
                    "conversions_by_funnel": [],
                    "ctr_by_platform": [],
                    "clicks_by_objective": []
                }
            }
            
        from src.core.schema.columns import Columns

        # Helper for column casing
        cols = df.columns
        def get_col(name):
            if name.lower() in cols: return name.lower()
            if name.title() in cols: return name.title()
            for c in cols:
                if c.lower() == name.lower(): return c
            return name # Fallback
            
        spend_c = get_col(Columns.SPEND)
        impr_c = get_col(Columns.IMPRESSIONS)
        clicks_c = get_col(Columns.CLICKS)
        conv_c = get_col(Columns.CONVERSIONS)
        platform_c = get_col(Columns.PLATFORM)
        channel_c = get_col(Columns.CHANNEL)
        funnel_c = get_col(Columns.FUNNEL)
        obj_c = get_col(Columns.OBJECTIVE)
        
        # 2. Key Metrics
        kpis = {
            "total_spend": df[spend_c].sum() if spend_c in df.columns else 0,
            "total_impressions": df[impr_c].sum() if impr_c in df.columns else 0,
            "total_clicks": df[clicks_c].sum() if clicks_c in df.columns else 0,
            "total_conversions": df[conv_c].sum() if conv_c in df.columns else 0,
        }
        
        # 3. Platforms List
        platforms = []
        if platform_c in df.columns:
            platforms = df[platform_c].unique().to_list()
            platforms = [p for p in platforms if p] # Filter None
            
        # 4. Main Chart (Default: Spend by Platform)
        main_chart = []
        if platform_c in df.columns and spend_c in df.columns:
            agg = df.group_by(platform_c).agg(pl.col(spend_c).sum())
            main_chart = agg.to_dicts()
            
        # 5. Quick Charts
        # a. Spend by Channel
        chart_1 = []
        if channel_c in df.columns and spend_c in df.columns:
            chart_1 = df.group_by(channel_c).agg(pl.col(spend_c).sum()).to_dicts()
            
        # b. Conversions by Funnel (renaming for frontend expected keys if needed)
        chart_2 = []
        if funnel_c in df.columns and conv_c in df.columns:
            # Frontend expects 'funnel_stage' and 'conversions'
            chart_2 = df.group_by(funnel_c).agg(pl.col(conv_c).sum()).rename({funnel_c: 'funnel_stage'}).to_dicts()
            
        # c. CTR by Platform
        chart_3 = []
        if platform_c in df.columns and clicks_c in df.columns and impr_c in df.columns:
            agg = df.group_by(platform_c).agg([
                pl.col(clicks_c).sum().alias("c"),
                pl.col(impr_c).sum().alias("i")
            ])
            chart_3 = agg.with_columns(
                (pl.col("c") / pl.col("i") * 100).fill_nan(0).alias("ctr")
            ).select([platform_c, "ctr"]).to_dicts()
            
        # d. Clicks by Objective
        chart_4 = []
        if obj_c in df.columns and clicks_c in df.columns:
            chart_4 = df.group_by(obj_c).agg(pl.col(clicks_c).sum()).to_dicts()
            
        return {
            "success": True,
            "platforms": platforms,
            "kpis": kpis,
            "main_chart": main_chart,
            "quick_charts": {
                "spend_by_channel": chart_1,
                "conversions_by_funnel": chart_2,
                "ctr_by_platform": chart_3,
                "clicks_by_objective": chart_4
            }
        }
        
    except Exception as e:
        logger.error(f"Snapshot failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/chart-data")
@limiter.limit("120/minute")
async def get_chart_data(
    request: Request,
    dimension: str,
    metric: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    platforms: Optional[str] = None,
    channels: Optional[str] = None,
    funnel_stages: Optional[str] = None,
    objectives: Optional[str] = None,
    regions: Optional[str] = None,
    devices: Optional[str] = None,
    ad_types: Optional[str] = None,
    placements: Optional[str] = None,
    audiences: Optional[str] = None,
    ages: Optional[str] = None,
    targetings: Optional[str] = None,
    response: Response = None,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """
    Get aggregated data for charts in {name: ..., value: ...} format.
    Supports dynamic dimension and metric.
    """
    if response:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    try:
        from src.core.database.duckdb_manager import get_duckdb_manager
        duckdb_mgr = get_duckdb_manager()

        df = duckdb_mgr.get_campaigns(
            start_date=start_date,
            end_date=end_date,
            platforms=platforms,
            channels=channels,
            funnel_stages=funnel_stages,
            objectives=objectives,
            regions=regions,
            devices=devices,
            ad_types=ad_types,
            placements=placements,
            audiences=audiences,
            ages=ages,
            targetings=targetings
        )

        if df.empty:
            return []

        # Find columns
        dim_col = find_column(df, dimension)
        # Handle 'date' special case
        if dimension == 'date':
             dim_col = find_column(df, 'date')

        if not dim_col:
             # If dimension is not in DF, try to find a close match or return empty
             # Some dimensions might be missing from some DFs
             return []

        # Find metric cols needed
        spend_col = find_column(df, 'spend')
        imp_col = find_column(df, 'impressions')
        click_col = find_column(df, 'clicks')
        conv_col = find_column(df, 'conversions')
        rev_col = find_column(df, 'revenue')

        # Aggregate
        agg_map = {}
        if spend_col: agg_map[spend_col] = 'sum'
        if imp_col: agg_map[imp_col] = 'sum'
        if click_col: agg_map[click_col] = 'sum'
        if conv_col: agg_map[conv_col] = 'sum'
        if rev_col: agg_map[rev_col] = 'sum'

        if not agg_map:
             return []

        # Perform groupby
        grouped = df.groupby(dim_col).agg(agg_map).reset_index()

        results = []
        for _, row in grouped.iterrows():
            name = row[dim_col]
            if dimension == 'date':
                name = str(name).split(' ')[0]
            else:
                name = str(name)

            val = 0
            
            s = row.get(spend_col, 0) if spend_col else 0
            i = row.get(imp_col, 0) if imp_col else 0
            c = row.get(click_col, 0) if click_col else 0
            cv = row.get(conv_col, 0) if conv_col else 0
            rv = row.get(rev_col, 0) if rev_col else 0

            m = metric.lower()
            if m == 'spend': val = s
            elif m == 'impressions': val = i
            elif m == 'clicks': val = c
            elif m == 'conversions': val = cv
            elif m == 'revenue': val = rv
            elif m == 'ctr': val = (c / i * 100) if i > 0 else 0
            elif m == 'cpc': val = (s / c) if c > 0 else 0
            elif m == 'cpa': val = (s / cv) if cv > 0 else 0
            elif m == 'roas': val = (rv / s) if s > 0 else 0
            elif m == 'cpm': val = (s / i * 1000) if i > 0 else 0
            
            # Format value
            if m in ['ctr', 'cpc', 'cpa', 'roas', 'cpm', 'spend', 'revenue']:
                 val = round(val, 2)
            else:
                 val = int(val)

            results.append({"name": name, "value": val})

        # Sort
        if dimension == 'date':
            results.sort(key=lambda x: x['name'])
        else:
            results.sort(key=lambda x: x['value'], reverse=True)

        return results

    except Exception as e:
        logger.error(f"Chart data failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

