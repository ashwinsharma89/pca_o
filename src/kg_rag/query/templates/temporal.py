"""
Temporal Query Templates

Cypher templates for time-series and trend analysis.
"""

from typing import Dict, Any


# Template: Daily spend trend
DAILY_SPEND_TREND = """
MATCH (m:Metric)
WHERE m.date >= date($date_from) AND m.date <= date($date_to)
WITH m.date AS date,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
RETURN date,
       impressions,
       clicks,
       spend,
       conversions,
       revenue,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY date
"""

# Template: Weekly aggregation
WEEKLY_TREND = """
MATCH (m:Metric)
WHERE m.date >= date($date_from) AND m.date <= date($date_to)
WITH date.truncate('week', m.date) AS week_start,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
RETURN week_start,
       impressions,
       clicks,
       spend,
       conversions,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY week_start
"""

# Template: Platform trend over time
PLATFORM_TREND = """
MATCH (m:Metric)
WHERE m.platform = $platform_id AND m.date >= date($date_from) AND m.date <= date($date_to)
WITH m.date AS date,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions
RETURN date,
       impressions,
       clicks,
       spend,
       conversions,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr
ORDER BY date
"""

# Template: Campaign trend
CAMPAIGN_TREND = """
MATCH (m:Metric)
WHERE m.campaign_id = $campaign_id AND m.date >= date($date_from) AND m.date <= date($date_to)
RETURN m.date AS date,
       m.impressions AS impressions,
       m.clicks AS clicks,
       m.spend AS spend,
       m.conversions AS conversions,
       m.revenue AS revenue,
       CASE WHEN m.impressions > 0 THEN m.clicks * 100.0 / m.impressions ELSE 0 END AS ctr,
       CASE WHEN m.spend > 0 THEN m.revenue / m.spend ELSE 0 END AS roas
ORDER BY date
"""

# Template: Day of week analysis
DAY_OF_WEEK_ANALYSIS = """
MATCH (m:Metric)
WHERE m.date >= date($date_from) AND m.date <= date($date_to)
WITH m.date.dayOfWeek AS day_of_week,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions
RETURN 
    CASE day_of_week 
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_name,
    day_of_week,
    impressions,
    clicks,
    spend,
    conversions,
    CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr
ORDER BY day_of_week
"""

# Template: Monthly aggregation
MONTHLY_TREND = """
MATCH (m:Metric)
WHERE m.date >= date($date_from) AND m.date <= date($date_to)
WITH date.truncate('month', m.date) AS month,
     SUM(m.spend) AS spend,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
RETURN month,
       spend,
       impressions,
       clicks,
       conversions,
       revenue,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY month
"""

# Keep old name for backwards compatibility
MONTH_COMPARISON = MONTHLY_TREND


def get_daily_spend_trend(
    date_from: str,
    date_to: str
) -> tuple[str, Dict[str, Any]]:
    """Get daily spend trend."""
    return DAILY_SPEND_TREND, {"date_from": date_from, "date_to": date_to}


def get_weekly_trend(
    date_from: str,
    date_to: str
) -> tuple[str, Dict[str, Any]]:
    """Get weekly trend."""
    return WEEKLY_TREND, {"date_from": date_from, "date_to": date_to}


def get_platform_trend(
    platform_id: str,
    date_from: str,
    date_to: str
) -> tuple[str, Dict[str, Any]]:
    """Get platform trend over time."""
    return PLATFORM_TREND, {
        "platform_id": platform_id,
        "date_from": date_from,
        "date_to": date_to
    }


def get_campaign_trend(
    campaign_id: str,
    date_from: str,
    date_to: str
) -> tuple[str, Dict[str, Any]]:
    """Get single campaign trend."""
    return CAMPAIGN_TREND, {
        "campaign_id": campaign_id,
        "date_from": date_from,
        "date_to": date_to
    }


def get_day_of_week_analysis(
    date_from: str,
    date_to: str
) -> tuple[str, Dict[str, Any]]:
    """Get day of week analysis."""
    return DAY_OF_WEEK_ANALYSIS, {"date_from": date_from, "date_to": date_to}



# Template: Period over Period comparison
PERIOD_COMPARISON = """
MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WHERE (m.date >= date($p1_start) AND m.date <= date($p1_end)) 
   OR (m.date >= date($p2_start) AND m.date <= date($p2_end))
WITH m,
     CASE 
       WHEN m.date >= date($p1_start) AND m.date <= date($p1_end) THEN 'Previous Period (' + $p1_start + ' to ' + $p1_end + ')'
       WHEN m.date >= date($p2_start) AND m.date <= date($p2_end) THEN 'Current Period (' + $p2_start + ' to ' + $p2_end + ')'
       ELSE 'Other'
     END AS period
WITH period,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
WHERE period <> 'Other'
RETURN period,
       impressions,
       clicks,
       spend,
       conversions,
       revenue,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY period DESC
"""

# Template: Year-over-Year Seasonal Comparison (Specific Month across years)
SEASONAL_COMPARISON = """
MATCH (m:Metric)
WHERE m.date.month = $month_num
WITH m.date.year AS year,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
RETURN 'July' + ' ' + toString(year) AS period,
       year,
       impressions,
       clicks,
       spend,
       conversions,
       revenue,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY year DESC
"""

def get_seasonal_comparison(month_num: int, month_name: str) -> tuple[str, Dict[str, Any]]:
    """Get seasonal comparison across years."""
    query = SEASONAL_COMPARISON.replace("'July'", f"'{month_name}'")
    return query, {"month_num": month_num}


def get_month_comparison(
    date_from: str,
    date_to: str
) -> tuple[str, Dict[str, Any]]:
    """Get month over month comparison."""
    return MONTH_COMPARISON, {"date_from": date_from, "date_to": date_to}


def get_period_comparison(
    p1_start: str,
    p1_end: str,
    p2_start: str,
    p2_end: str
) -> tuple[str, Dict[str, Any]]:
    """Get period over period comparison."""
    return PERIOD_COMPARISON, {
        "p1_start": p1_start,
        "p1_end": p1_end,
        "p2_start": p2_start,
        "p2_end": p2_end
    }
