"""
Placement Query Templates

Cypher templates for placement/site performance analysis.
"""

from typing import Dict, Any


# Template: Placement performance overview
PLACEMENT_OVERVIEW = """
MATCH (m:Metric)
WHERE m.placement IS NOT NULL
WITH m.placement AS placement_type,
     count(DISTINCT m.id) AS metrics,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions
RETURN placement_type,
       metrics AS placements,
       impressions,
       clicks,
       spend,
       conversions,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN spend > 0 THEN conversions / spend ELSE 0 END AS conv_rate
ORDER BY spend DESC
"""

# Template: Top performing placements
TOP_PLACEMENTS = """
MATCH (m:Metric)
WHERE m.spend > $min_spend AND m.placement IS NOT NULL
WITH m.placement AS placement,
     m.ad_type AS type,
     m.channel AS category,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions
WITH placement, type, category, impressions, clicks, spend, conversions,
     CASE WHEN spend > 0 THEN conversions / spend ELSE 0 END AS efficiency
ORDER BY efficiency DESC
LIMIT $limit
RETURN placement,
       type,
       category,
       spend,
       conversions,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       efficiency
"""

# Template: Worst performing placements
WORST_PLACEMENTS = """
MATCH (m:Metric)
WHERE m.spend > $min_spend AND m.placement IS NOT NULL
WITH m.placement AS placement,
     m.ad_type AS type,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     CASE WHEN SUM(m.conversions) > 0 THEN SUM(m.spend) / SUM(m.conversions) ELSE 999999 END AS cpa
ORDER BY cpa DESC
LIMIT $limit
RETURN placement,
       type,
       spend,
       conversions,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       cpa
"""

# Template: Placement by category
PLACEMENT_BY_CATEGORY = """
MATCH (m:Metric)
WHERE m.channel IS NOT NULL AND m.placement IS NOT NULL
WITH m.channel AS category,
     count(DISTINCT m.placement) AS placements,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions
RETURN category,
       placements,
       impressions,
       spend,
       conversions,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa
ORDER BY spend DESC
LIMIT $limit
"""

# Template: Placement for campaign
CAMPAIGN_PLACEMENTS = """
MATCH (m:Metric)
WHERE m.campaign_id = $campaign_id AND m.placement IS NOT NULL
RETURN m.placement AS placement,
       m.ad_type AS type,
       m.channel AS category,
       SUM(m.impressions) AS impressions,
       SUM(m.clicks) AS clicks,
       SUM(m.spend) AS spend,
       SUM(coalesce(m.conversions, 0)) AS conversions,
       CASE WHEN SUM(m.impressions) > 0 THEN SUM(m.clicks) * 100.0 / SUM(m.impressions) ELSE 0 END AS ctr
ORDER BY spend DESC
LIMIT $limit
"""

# Template: Placement viewability analysis
VIEWABILITY_ANALYSIS = """
MATCH (p:Placement)
WHERE p.viewability_rate IS NOT NULL
WITH 
    CASE 
        WHEN p.viewability_rate >= 0.7 THEN 'High (70%+)'
        WHEN p.viewability_rate >= 0.5 THEN 'Medium (50-70%)'
        ELSE 'Low (<50%)'
    END AS viewability_tier,
    p.spend AS spend,
    p.impressions AS impressions,
    p.clicks AS clicks,
    p.conversions AS conversions
WITH viewability_tier,
     count(*) AS placements,
     SUM(spend) AS total_spend,
     SUM(impressions) AS total_impressions,
     SUM(conversions) AS total_conversions
RETURN viewability_tier,
       placements,
       total_spend,
       total_impressions,
       total_conversions,
       CASE WHEN total_impressions > 0 THEN total_conversions * 1000.0 / total_impressions ELSE 0 END AS conv_per_1k
ORDER BY 
    CASE viewability_tier
        WHEN 'High (70%+)' THEN 1
        WHEN 'Medium (50-70%)' THEN 2
        ELSE 3
    END
"""


def get_placement_overview() -> tuple[str, Dict[str, Any]]:
    """Get placement type overview."""
    return PLACEMENT_OVERVIEW, {}


def get_top_placements(
    min_spend: float = 100.0,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get top performing placements."""
    return TOP_PLACEMENTS, {"min_spend": min_spend, "limit": limit}


def get_worst_placements(
    min_spend: float = 100.0,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get worst performing placements."""
    return WORST_PLACEMENTS, {"min_spend": min_spend, "limit": limit}


def get_placement_by_category(limit: int = 20) -> tuple[str, Dict[str, Any]]:
    """Get placement by category."""
    return PLACEMENT_BY_CATEGORY, {"limit": limit}


def get_campaign_placements(
    campaign_id: str,
    limit: int = 50
) -> tuple[str, Dict[str, Any]]:
    """Get placements for a campaign."""
    return CAMPAIGN_PLACEMENTS, {"campaign_id": campaign_id, "limit": limit}


def get_viewability_analysis() -> tuple[str, Dict[str, Any]]:
    """Get viewability tier analysis."""
    return VIEWABILITY_ANALYSIS, {}
