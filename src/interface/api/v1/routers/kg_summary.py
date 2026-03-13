"""
KG Summary Router - Performance Summary endpoint

Generates aggregated performance summaries from campaign data,
powering the /rag-summary frontend page.
"""

from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, List
import logging
from datetime import datetime

from src.interface.api.middleware.auth import get_current_user
from src.core.database.duckdb_manager import get_duckdb_manager
from src.core.utils.column_mapping import find_column

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/kg", tags=["kg-summary"])


def _build_breakdown(df, group_col: str, spend_col: str, conv_col: str, revenue_col: str) -> List[Dict[str, Any]]:
    """Aggregate metrics by a dimension column."""
    import pandas as pd

    if not group_col or group_col not in df.columns:
        return []

    grouped = df.groupby(group_col, dropna=True).agg({
        **(({spend_col: "sum"}) if spend_col else {}),
        **(({conv_col: "sum"}) if conv_col else {}),
        **(({revenue_col: "sum"}) if revenue_col else {}),
    }).reset_index()

    results = []
    for _, row in grouped.iterrows():
        name = str(row[group_col])
        if not name or name == "nan":
            continue
        spend = float(row.get(spend_col, 0) or 0) if spend_col else 0
        conversions = float(row.get(conv_col, 0) or 0) if conv_col else 0
        revenue = float(row.get(revenue_col, 0) or 0) if revenue_col else 0
        roas = round(revenue / spend, 2) if spend > 0 else 0
        cpa = round(spend / conversions, 2) if conversions > 0 else 0

        results.append({
            "name": name,
            "spend": round(spend, 2),
            "revenue": round(revenue, 2),
            "roas": roas,
            "cpa": cpa,
            "conversions": int(conversions),
        })

    results.sort(key=lambda x: x["spend"], reverse=True)
    return results


def _find_insights(breakdowns: Dict[str, List[Dict]], avg_roas: float, avg_cpa: float):
    """Identify top performers and underperformers across all dimensions."""
    worked = []
    didnt_work = []

    for dimension, items in breakdowns.items():
        for item in items:
            if item["spend"] < 100:  # skip very small segments
                continue

            # Check ROAS
            if avg_roas > 0 and item["roas"] > 0:
                roas_ratio = item["roas"] / avg_roas
                if roas_ratio >= 1.3:
                    worked.append({
                        "segment": item["name"],
                        "dimension": dimension,
                        "metric": "ROAS",
                        "value": item["roas"],
                        "vs_avg": f"+{round((roas_ratio - 1) * 100)}% vs avg",
                        "spend": item["spend"],
                        "reason": f"{item['name']} delivers {item['roas']}x ROAS on ${item['spend']:,.0f} spend — significantly above the {avg_roas:.1f}x average.",
                    })
                elif roas_ratio <= 0.6:
                    didnt_work.append({
                        "segment": item["name"],
                        "dimension": dimension,
                        "metric": "ROAS",
                        "value": item["roas"],
                        "vs_avg": f"{round((roas_ratio - 1) * 100)}% vs avg",
                        "spend": item["spend"],
                        "reason": f"{item['name']} returns only {item['roas']}x ROAS on ${item['spend']:,.0f} spend — well below the {avg_roas:.1f}x average.",
                    })

            # Check CPA
            if avg_cpa > 0 and item["cpa"] > 0:
                cpa_ratio = item["cpa"] / avg_cpa
                if cpa_ratio <= 0.7:
                    worked.append({
                        "segment": item["name"],
                        "dimension": dimension,
                        "metric": "CPA",
                        "value": item["cpa"],
                        "vs_avg": f"-{round((1 - cpa_ratio) * 100)}% vs avg",
                        "spend": item["spend"],
                        "reason": f"{item['name']} acquires customers at ${item['cpa']:.2f} — {round((1 - cpa_ratio) * 100)}% cheaper than the ${avg_cpa:.2f} average.",
                    })
                elif cpa_ratio >= 1.5:
                    didnt_work.append({
                        "segment": item["name"],
                        "dimension": dimension,
                        "metric": "CPA",
                        "value": item["cpa"],
                        "vs_avg": f"+{round((cpa_ratio - 1) * 100)}% vs avg",
                        "spend": item["spend"],
                        "reason": f"{item['name']} costs ${item['cpa']:.2f} per acquisition — {round((cpa_ratio - 1) * 100)}% more expensive than the ${avg_cpa:.2f} average.",
                    })

    # Sort by spend (most impactful first) and limit
    worked.sort(key=lambda x: x["spend"], reverse=True)
    didnt_work.sort(key=lambda x: x["spend"], reverse=True)
    return worked[:8], didnt_work[:8]


def _generate_optimizations(worked: List[Dict], didnt_work: List[Dict]) -> List[Dict]:
    """Generate optimization recommendations from insights."""
    optimizations = []

    for item in worked[:4]:
        optimizations.append({
            "action": "SCALE",
            "segment": item["segment"],
            "dimension": item["dimension"],
            "reason": f"Strong {item['metric']} performance ({item['vs_avg']}). Consider increasing budget allocation.",
        })

    for item in didnt_work[:4]:
        optimizations.append({
            "action": "CUT",
            "segment": item["segment"],
            "dimension": item["dimension"],
            "reason": f"Weak {item['metric']} performance ({item['vs_avg']}). Review targeting or reduce spend.",
        })

    # Fill with HOLD for borderline items if we have few recommendations
    if len(optimizations) < 3:
        optimizations.append({
            "action": "HOLD",
            "segment": "Overall Portfolio",
            "dimension": "general",
            "reason": "Most segments are performing near average. Continue monitoring for emerging trends.",
        })

    return optimizations


@router.get("/summary")
async def get_performance_summary(
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """
    Generate a comprehensive performance summary from campaign data.
    Returns breakdowns by platform, channel, funnel, device, and age,
    along with insights and optimization recommendations.
    """
    try:
        duckdb_mgr = get_duckdb_manager()

        if not duckdb_mgr.has_data():
            return {
                "success": False,
                "error": "No campaign data available. Please upload data first.",
            }

        df = duckdb_mgr.get_campaigns()

        if df.empty:
            return {
                "success": False,
                "error": "No campaign data available. Please upload data first.",
            }

        # Resolve column names
        spend_col = find_column(df, "spend")
        conv_col = find_column(df, "conversions")
        revenue_col = find_column(df, "revenue")
        platform_col = find_column(df, "platform")
        channel_col = find_column(df, "channel")
        funnel_col = find_column(df, "funnel")
        device_col = find_column(df, "device")
        age_col = find_column(df, "age")

        # Compute averages
        total_spend = float(df[spend_col].sum()) if spend_col else 0
        total_conv = float(df[conv_col].sum()) if conv_col else 0
        total_revenue = float(df[revenue_col].sum()) if revenue_col else 0

        avg_roas = round(total_revenue / total_spend, 2) if total_spend > 0 else 0
        avg_cpa = round(total_spend / total_conv, 2) if total_conv > 0 else 0

        # Build breakdowns
        breakdown_map = {
            "Platform": (platform_col, "platform_breakdown"),
            "Channel": (channel_col, "channel_breakdown"),
            "Funnel": (funnel_col, "funnel_breakdown"),
            "Device": (device_col, "device_breakdown"),
            "Age": (age_col, "age_breakdown"),
        }

        breakdowns = {}
        result_breakdowns = {}
        for dim_label, (col, key) in breakdown_map.items():
            bd = _build_breakdown(df, col, spend_col, conv_col, revenue_col)
            breakdowns[dim_label] = bd
            result_breakdowns[key] = bd

        # Generate insights
        what_worked, what_didnt_work = _find_insights(breakdowns, avg_roas, avg_cpa)

        # Generate optimizations
        optimizations = _generate_optimizations(what_worked, what_didnt_work)

        return {
            "generated_at": datetime.utcnow().isoformat(),
            **result_breakdowns,
            "what_worked": what_worked,
            "what_didnt_work": what_didnt_work,
            "optimizations": optimizations,
            "averages": {
                "roas": avg_roas,
                "cpa": avg_cpa,
            },
        }

    except Exception as e:
        logger.error(f"Performance summary generation failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Endpoints for the /rag page
# ============================================================================

@router.get("/health")
async def kg_health(
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """KG health status — reports DuckDB data availability."""
    try:
        duckdb_mgr = get_duckdb_manager()
        has_data = duckdb_mgr.has_data()
        row_count = 0
        full_df = None
        if has_data:
            full_df = duckdb_mgr.get_campaigns()
            row_count = len(full_df) if not full_df.empty else 0

        # Estimate relationship count from the graph structure:
        #   HAS_PERFORMANCE  : 1 per row (Metric → Campaign)
        #   BELONGS_TO       : 1 per unique campaign-platform pair
        #   HAS_TARGETING    : 1 per unique campaign
        rel_count = 0
        if has_data and full_df is not None and not full_df.empty:
            from src.core.utils.column_mapping import find_column
            platform_col = find_column(full_df, "platform")
            campaign_col = find_column(full_df, "campaign_name") or find_column(full_df, "campaign")
            has_perf = row_count                                              # every row
            belongs_to = int(full_df[[c for c in [campaign_col, platform_col] if c]].drop_duplicates().shape[0]) if campaign_col or platform_col else 0
            has_targeting = int(full_df[campaign_col].nunique()) if campaign_col else 0
            rel_count = has_perf + belongs_to + has_targeting

        return {
            "status": "healthy" if has_data else "no_data",
            "neo4j_connected": has_data,  # frontend expects this field name
            "neo4j_uri": "duckdb://local",
            "node_count": row_count,
            "relationship_count": rel_count,
        }
    except Exception as e:
        logger.error(f"KG health check failed: {e}")
        return {
            "status": "error",
            "neo4j_connected": False,
            "neo4j_uri": "duckdb://local",
            "node_count": 0,
            "relationship_count": 0,
        }


@router.get("/templates")
async def kg_templates(
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """Return available query intent templates."""
    return {
        "templates": [
            {
                "intent": "platform",
                "examples": ["Meta ads performance", "Google Ads spend", "Platform comparison"],
            },
            {
                "intent": "temporal",
                "examples": ["Daily spend trend", "Weekly conversions", "Monthly ROAS"],
            },
            {
                "intent": "ranking",
                "examples": ["Top 5 campaigns by spend", "Best performing ads", "Worst ROAS campaigns"],
            },
            {
                "intent": "cross_channel",
                "examples": ["Compare Search vs Social", "Channel breakdown", "Search vs Display"],
            },
            {
                "intent": "targeting",
                "examples": ["Device breakdown", "Age group performance", "Audience segments"],
            },
            {
                "intent": "optimization",
                "examples": ["Budget recommendations", "Underperforming campaigns", "Scale opportunities"],
            },
        ]
    }


from pydantic import BaseModel, Field
from typing import Optional


class KGQueryRequest(BaseModel):
    query: str = Field(..., min_length=1)
    limit: int = Field(20, ge=1, le=1000)
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    platform: Optional[str] = None


@router.post("/query")
async def kg_query(
    request: KGQueryRequest,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """
    Execute a natural language query against campaign data.
    Uses DuckDB for data retrieval with keyword-based intent detection.
    """
    import time
    import pandas as pd

    start = time.time()
    query_lower = request.query.lower()

    try:
        duckdb_mgr = get_duckdb_manager()
        if not duckdb_mgr.has_data():
            return {
                "success": False,
                "data": [],
                "metadata": {
                    "query": request.query,
                    "intent": "error",
                    "confidence": 0,
                    "routing": "none",
                },
                "error": "No campaign data available. Please upload data first.",
            }

        df = duckdb_mgr.get_campaigns()
        if df.empty:
            return {
                "success": False,
                "data": [],
                "metadata": {
                    "query": request.query,
                    "intent": "error",
                    "confidence": 0,
                    "routing": "none",
                },
                "error": "No data found.",
            }

        # Resolve columns
        spend_col = find_column(df, "spend")
        impr_col = find_column(df, "impressions")
        clicks_col = find_column(df, "clicks")
        conv_col = find_column(df, "conversions")
        revenue_col = find_column(df, "revenue")
        platform_col = find_column(df, "platform")
        channel_col = find_column(df, "channel")
        device_col = find_column(df, "device")
        date_col = find_column(df, "date")
        campaign_col = find_column(df, "campaign_name") or find_column(df, "campaign")
        age_col = find_column(df, "age")
        funnel_col = find_column(df, "funnel")

        import re

        # ── Lookup tables ────────────────────────────────────────────────────
        # ── Deterministic Intent & Entity Extraction ─────────────────────────
        from src.platform.query_engine.hybrid_retrieval import HybridSQLRetrieval, QueryIntent
        
        hybrid_retrieval = HybridSQLRetrieval()
        analysis = hybrid_retrieval.analyze_question(request.query)
        kb_examples = hybrid_retrieval.retrieve_local_examples(request.query, k=1)
        
        # 1. Get Analysis Results
        intent_obj = analysis['intent']
        intent = intent_obj.value
        entities = analysis['entities']
        temporal_context = analysis['temporal']
        
        # 1.1 Override/Refine Intent from Knowledge Base match if high confidence
        if kb_examples and kb_examples[0].relevance_score > 0.7:
             kb_match = kb_examples[0]
             logger.info(f"High-confidence KB match found ({kb_match.relevance_score:.2f}): {kb_match.question}")
             # The KB example's intent is already classified by HybridSQLRetrieval
        
        # 4. Map to SQL/Pandas Logic
        confidence = 0.95  # Deterministic has high confidence for matched patterns
        group_col = None
        cypher_dimension_label = None
        cypher_where_clause = ""
        
        # Platform/Channel/Dimension mapping from entities
        entity_filters = []
        if 'platform' in entities.filters:
            platforms = entities.filters['platform']
            if platforms:
                platforms_str = ", ".join([f"'{p}'" for p in platforms])
                entity_filters.append(f"toLower({platform_col}) IN [{platforms_str}]")
                cypher_dimension_label = "platform"
        
        if 'channel' in entities.filters:
            channels = entities.filters['channel']
            if channels:
                channels_str = ", ".join([f"'{c}'" for c in channels])
                entity_filters.append(f"toLower({channel_col}) IN [{channels_str}]")
                cypher_dimension_label = "channel"
        
        # Fallback to group_by if no filters
        if not entity_filters:
            if 'platform' in entities.group_by:
                cypher_dimension_label = "platform"
            if 'channel' in entities.group_by:
                cypher_dimension_label = "channel"

        # Map QueryIntent to KG Intent
        # Prioritize platform/cross_channel if specific entities are detected
        # unless "trend" or "over time" is explicitly in the query.
        is_explicit_trend = any(w in request.query.lower() for w in ["trend", "over time", "daily", "weekly", "monthly"])
        
        if intent == QueryIntent.RANKING.value:
            intent = "ranking"
            group_col = campaign_col
        elif intent == QueryIntent.COMPARISON.value:
            intent = "cross_channel"
            group_col = channel_col
        elif intent == QueryIntent.TREND.value and is_explicit_trend:
            intent = "temporal"
            group_col = date_col
        elif intent == QueryIntent.BREAKDOWN.value:
            intent = "targeting"
            if 'device' in entities.group_by: group_col = device_col
            elif 'age' in entities.group_by: group_col = age_col
            elif 'funnel' in entities.group_by: group_col = funnel_col
            elif 'campaign' in entities.group_by: group_col = campaign_col
            else: group_col = platform_col or channel_col
        else:
            # Default fallback for platform/channel specific performance
            if 'platform' in entities.filters or 'platform' in entities.group_by:
                intent = "platform"
                # If plural "platforms", group by it
                if "platforms" in request.query.lower() or not entities.filters.get('platform'):
                    group_col = platform_col
            elif 'channel' in entities.filters or 'channel' in entities.group_by:
                intent = "cross_channel"
                if "channels" in request.query.lower() or not entities.filters.get('channel'):
                    group_col = channel_col
            elif 'funnel' in entities.group_by or 'funnel' in entities.filters:
                intent = "targeting" # Funnel is mapped to targeting layer
                group_col = funnel_col
            else:
                 intent = "general"
        
        # 5. Build WHERE clause for Cypher display and Pandas filter
        if entity_filters:
            cypher_where_clause = "WHERE " + " AND ".join(entity_filters) + "\n"
            # Apply filters to dataframe
            for dim, vals in entities.filters.items():
                col_name = platform_col if dim == 'platform' else (channel_col if dim == 'channel' else None)
                if col_name and col_name in df.columns and vals:
                    # Convert to lower list for case-insensitive matching
                    vals_lower = [v.lower() for v in vals]
                    df = df[df[col_name].astype(str).str.lower().isin(vals_lower)]

        # 6. Apply Temporal Filtering
        if temporal_context.primary_period and date_col and date_col in df.columns:
            try:
                df[date_col] = pd.to_datetime(df[date_col])
                max_date = df[date_col].max()
                interval_type = temporal_context.primary_period.sql_interval
                
                if interval_type:
                    # n is the number from "n MONTH" or "n DAY"
                    n = int(interval_type.split()[0])
                    unit = interval_type.split()[1].lower()
                    
                    if 'month' in unit:
                        cutoff = max_date - pd.Timedelta(days=n * 30)
                    else:
                        cutoff = max_date - pd.Timedelta(days=n)
                        
                    df = df[df[date_col] >= cutoff]
                    filter_str = f"m.date >= date('{cutoff.date()}')"
                    if cypher_where_clause:
                        cypher_where_clause = cypher_where_clause.rstrip("\n") + f" AND {filter_str}\n"
                    else:
                        cypher_where_clause = f"WHERE {filter_str}\n"
            except Exception as e:
                logger.warning(f"Temporal filtering failed: {e}")

        # ── Build aggregation ────────────────────────────────────────────────
        metric_cols = {}
        for col in [spend_col, impr_col, clicks_col, conv_col, revenue_col]:
            if col:
                metric_cols[col] = "sum"

        ascending = "worst" in query_lower or "lowest" in query_lower or "lagging" in query_lower
        sort_metric = spend_col or (list(metric_cols.keys())[0] if metric_cols else None)
        if "impression" in query_lower:
            sort_metric = impr_col or sort_metric
        elif "click" in query_lower and "ctr" not in query_lower:
            sort_metric = clicks_col or sort_metric
        elif "conversion" in query_lower:
            sort_metric = conv_col or sort_metric
        elif "roas" in query_lower or "performance" in query_lower or "performing" in query_lower:
            sort_metric = "roas" # Sort by the derived column
        elif "cvr" in query_lower:
            sort_metric = "cvr"
        elif "cpc" in query_lower:
            sort_metric = "cpc"
        elif "cpa" in query_lower:
            sort_metric = "cpa"

        # ── Helper: build comma-separated metric RETURN clause ───────────────
        def _metric_returns(prefix="m"):
            items = [f"SUM({prefix}.{c}) AS {c}" for c in metric_cols]
            return ",\n       ".join(items)

        if group_col and group_col in df.columns and metric_cols:
            result_df = df.groupby(group_col, dropna=True).agg(metric_cols).reset_index()

            # Derived metrics
            if spend_col and clicks_col and spend_col in result_df and clicks_col in result_df:
                result_df["cpc"] = (result_df[spend_col] / result_df[clicks_col].replace(0, float("nan"))).round(2)
            if clicks_col and impr_col and clicks_col in result_df and impr_col in result_df:
                result_df["ctr"] = ((result_df[clicks_col] / result_df[impr_col].replace(0, float("nan"))) * 100).round(2)
            if revenue_col and spend_col and revenue_col in result_df and spend_col in result_df:
                result_df["roas"] = (result_df[revenue_col] / result_df[spend_col].replace(0, float("nan"))).round(2)
            if spend_col and conv_col and spend_col in result_df and conv_col in result_df:
                result_df["cpa"] = (result_df[spend_col] / result_df[conv_col].replace(0, float("nan"))).round(2)

            # Sort
            if intent == "temporal":
                result_df = result_df.sort_values(group_col, ascending=True)
            elif sort_metric and sort_metric in result_df.columns:
                result_df = result_df.sort_values(sort_metric, ascending=ascending)

            # Format date column
            if intent == "temporal":
                try:
                    fmt = '%Y-%m' if "month" in query_lower else '%Y-%m-%d'
                    result_df[group_col] = pd.to_datetime(result_df[group_col]).dt.strftime(fmt)
                except Exception:
                    pass

            # Friendly column rename for frontend table
            rename_label = "Period" if intent == "temporal" else (
                "Campaign" if intent == "ranking" else "Dimension"
            )
            result_df = result_df.rename(columns={group_col: rename_label})
            result_df = result_df.head(request.limit).fillna(0)
            data = result_df.to_dict(orient="records")

            # ── Cypher for display ───────────────────────────────────────────
            mr = _metric_returns()
            dim = cypher_dimension_label or group_col
            order_dir = "ASC" if intent == "temporal" else ("ASC" if ascending else "DESC")
            order_by_expr = "Period" if intent == "temporal" else sort_metric or dim

            if intent == "temporal":
                cypher_display = (
                    f"MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)\n"
                    f"{cypher_where_clause}"
                    f"RETURN m.date AS Period,\n       {mr}\n"
                    f"ORDER BY Period ASC\n"
                    f"LIMIT {request.limit}"
                )
            elif intent == "ranking":
                cypher_display = (
                    f"MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)\n"
                    f"{cypher_where_clause}"
                    f"RETURN c.name AS Campaign,\n       {mr}\n"
                    f"ORDER BY {sort_metric or 'spend'} {order_dir}\n"
                    f"LIMIT {request.limit}"
                )
            else:
                cypher_display = (
                    f"MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)\n"
                    f"{cypher_where_clause}"
                    f"RETURN m.{dim} AS {dim.title()},\n       {mr}\n"
                    f"ORDER BY {sort_metric or 'spend'} {order_dir}\n"
                    f"LIMIT {request.limit}"
                )
        else:
            # No grouping — return filtered totals (e.g. "Meta ads performance")
            summary_row = {col: float(df[col].sum()) for col in metric_cols if col in df.columns}
            if summary_row:
                # Add derived metrics
                s = summary_row.get(spend_col, 0)
                c_ = summary_row.get(clicks_col, 0)
                i_ = summary_row.get(impr_col, 0)
                cv = summary_row.get(conv_col, 0)
                rv = summary_row.get(revenue_col, 0)
                if c_ > 0:
                    summary_row["cpc"] = round(s / c_, 2)
                if i_ > 0:
                    summary_row["ctr"] = round(c_ / i_ * 100, 2)
                if i_ > 0:
                    summary_row["cpm"] = round(s / i_ * 1000, 2)
                if cv > 0:
                    summary_row["cpa"] = round(s / cv, 2)
                if s > 0:
                    summary_row["roas"] = round(rv / s, 2)
                
                # Label the row with the filter value so frontend can display it
                dim_val = list(entities.filters.values())[0] if entities.filters else (
                    list(entities.group_by)[0] if entities.group_by else "Total"
                )
                summary_row["Dimension"] = dim_val
            data = [summary_row] if summary_row else []

            mr = _metric_returns()
            dim_key = list(entities.filters.keys())[0] if entities.filters else (
                list(entities.group_by)[0] if entities.group_by else "platform"
            )
            filter_label = f"m.{dim_key} AS {dim_key.title()},\n       "
            cypher_display = (
                f"MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)\n"
                f"{cypher_where_clause}"
                f"RETURN {filter_label}{mr}"
            )

        # ── Global Summary Calculation (Fix for KPI Cards) ───────────────────
        # Calculate totals from the full filtered 'df' BEFORE grouping/truncation
        global_summary = {"count": len(df)}
        if spend_col and spend_col in df.columns:
            global_summary["total_spend"] = round(float(df[spend_col].sum()), 2)
        if impr_col and impr_col in df.columns:
            global_summary["total_impressions"] = int(df[impr_col].sum())
        if clicks_col and clicks_col in df.columns:
            global_summary["total_clicks"] = int(df[clicks_col].sum())
        if conv_col and conv_col in df.columns:
            global_summary["total_conversions"] = int(df[conv_col].sum())
        
        # Calculate derived global metrics
        if global_summary.get("total_clicks", 0) and global_summary.get("total_impressions", 0):
            global_summary["avg_ctr"] = round(global_summary["total_clicks"] / global_summary["total_impressions"] * 100, 2)
        if global_summary.get("total_spend", 0) and global_summary.get("total_clicks", 0):
            global_summary["avg_cpc"] = round(global_summary["total_spend"] / global_summary["total_clicks"], 2)
        if global_summary.get("total_spend", 0) and global_summary.get("total_conversions", 0):
            global_summary["avg_cpa"] = round(global_summary["total_spend"] / global_summary["total_conversions"], 2)

        # ── LLM Guidance Generation ──────────────────────────────────────────
        llm_guidance = None
        try:
             from src.platform.query_engine.insight_generator import KGInsightGenerator
             insight_gen = KGInsightGenerator()
             llm_guidance = insight_gen.generate_guidance(
                 query=request.query,
                 results=data,
                 summary=global_summary
             )
        except Exception as l_err:
             logger.warning(f"Failed to generate LLM guidance: {l_err}")

        elapsed = (time.time() - start) * 1000

        return {
            "success": True,
            "data": data,
            "summary": global_summary,
            "llm_guidance": llm_guidance,
            "metadata": {
                "query": request.query,
                "intent": intent,
                "confidence": confidence,
                "routing": "template",
                "cypher": cypher_display,
                "execution_time_ms": round(elapsed, 1),
            },
        }

    except Exception as e:
        logger.error(f"KG query failed: {e}", exc_info=True)
        return {
            "success": False,
            "data": [],
            "metadata": {
                "query": request.query,
                "intent": "error",
                "confidence": 0,
                "routing": "error",
            },
            "error": str(e),
        }
