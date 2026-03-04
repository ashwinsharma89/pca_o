"""
Targeting Query Templates

Cypher templates for targeting dimension analysis.
"""

from typing import Dict, Any, List, Optional


# Template: Device type breakdown
DEVICE_BREAKDOWN = """
MATCH (c:Campaign)-[:HAS_TARGETING]->(t:Targeting)
WHERE t.device_types IS NOT NULL
WITH c, t, size(t.device_types) as device_count
UNWIND t.device_types AS device
WITH device,
     count(c) AS campaigns,
     SUM(c.spend_total / device_count) AS spend,
     SUM(c.impressions_total / device_count) AS impressions,
     SUM(c.clicks_total / device_count) AS clicks,
     SUM(c.conversions_total / device_count) AS conversions,
     SUM(c.revenue_total / device_count) AS revenue
RETURN device,
       campaigns,
       spend,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END AS cpc,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY spend DESC
"""

# Template: Age range breakdown
AGE_BREAKDOWN = """
MATCH (c:Campaign)-[:HAS_TARGETING]->(t:Targeting)
WHERE t.age_range IS NOT NULL
WITH c, t, size(t.age_range) as age_count
UNWIND t.age_range AS age_range
WITH age_range,
     count(c) AS campaigns,
     SUM(c.spend_total / age_count) AS spend,
     SUM(c.impressions_total / age_count) AS impressions,
     SUM(c.clicks_total / age_count) AS clicks,
     SUM(c.conversions_total / age_count) AS conversions,
     SUM(c.revenue_total / age_count) AS revenue
RETURN age_range,
       campaigns,
       spend,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY spend DESC
"""

# Template: Geographic breakdown
GEO_BREAKDOWN = """
MATCH (m:Metric)
WHERE m.geo_countries IS NOT NULL
UNWIND m.geo_countries AS country
WITH country,
     count(DISTINCT m.campaign_id) AS campaigns,
     SUM(m.spend) AS spend,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
RETURN country,
       campaigns,
       spend,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY spend DESC
LIMIT $limit
"""

# Template: Targeting dimension performance
TARGETING_BY_DIMENSION = """
MATCH (m:Metric)
WHERE m[$dimension] IS NOT NULL
WITH m[$dimension] AS dimension_value,
     count(DISTINCT m.campaign_id) AS campaigns,
     SUM(m.spend) AS spend,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
RETURN dimension_value,
       campaigns,
       spend,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END AS cpc,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY spend DESC
LIMIT $limit
"""

# Template: Bid strategy breakdown
BID_STRATEGY_BREAKDOWN = """
MATCH (m:Metric)
WHERE m.bid_strategy IS NOT NULL
WITH m.bid_strategy AS bid_strategy,
     count(DISTINCT m.campaign_id) AS campaigns,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue,
     AVG(m.spend) AS avg_spend_per_metric
RETURN bid_strategy,
       campaigns,
       spend,
       conversions,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas,
       avg_spend_per_metric
ORDER BY spend DESC
"""

# Template: Funnel stage breakdown
FUNNEL_BREAKDOWN = """
MATCH (m:Metric)
WHERE m.funnel IS NOT NULL
WITH m.funnel AS funnel_stage,
     count(DISTINCT m.campaign_id) AS campaigns,
     SUM(m.spend) AS spend,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(coalesce(m.conversions, 0)) AS conversions
RETURN funnel_stage,
       campaigns,
       spend,
       impressions,
       clicks,
       conversions,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN clicks > 0 THEN conversions * 100.0 / clicks ELSE 0 END AS cvr
ORDER BY 
    CASE lower(funnel_stage)
        WHEN 'awareness' THEN 1
        WHEN 'consideration' THEN 2
        WHEN 'conversion' THEN 3
        WHEN 'retention' THEN 4
        ELSE 5
    END
"""

# Template: Interest/Affinity breakdown
INTERESTS_BREAKDOWN = """
MATCH (c:Campaign)-[:HAS_TARGETING]->(t:Targeting)
WHERE t.interests IS NOT NULL
WITH c, t, size(t.interests) as interest_count
UNWIND t.interests AS interest
WITH interest,
     count(c) AS campaigns,
     SUM(c.spend_total / interest_count) AS spend,
     SUM(c.conversions_total / interest_count) AS conversions,
     SUM(c.revenue_total / interest_count) AS revenue
WHERE spend > $min_spend
RETURN interest,
       campaigns,
       spend,
       conversions,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY roas DESC
LIMIT $limit
"""


def get_device_breakdown() -> tuple[str, Dict[str, Any]]:
    """Get device type breakdown."""
    return DEVICE_BREAKDOWN, {}


def get_age_breakdown() -> tuple[str, Dict[str, Any]]:
    """Get age range breakdown."""
    return AGE_BREAKDOWN, {}


def get_geo_breakdown(limit: int = 20) -> tuple[str, Dict[str, Any]]:
    """Get geographic breakdown."""
    return GEO_BREAKDOWN, {"limit": limit}


def get_targeting_by_dimension(
    dimension: str,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get targeting by specific dimension."""
    return TARGETING_BY_DIMENSION, {"dimension": dimension, "limit": limit}


def get_bid_strategy_breakdown() -> tuple[str, Dict[str, Any]]:
    """Get bid strategy breakdown."""
    return BID_STRATEGY_BREAKDOWN, {}


def get_funnel_breakdown() -> tuple[str, Dict[str, Any]]:
    """Get funnel stage breakdown."""
    return FUNNEL_BREAKDOWN, {}


def get_interests_breakdown(
    min_spend: float = 100.0,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get interests breakdown."""
    return INTERESTS_BREAKDOWN, {"min_spend": min_spend, "limit": limit}
