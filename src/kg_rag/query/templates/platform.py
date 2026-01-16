"""
Platform Query Templates

Cypher templates for platform-specific performance queries.
"""

from typing import Dict, Any, Optional


# Template: Platform overview
PLATFORM_OVERVIEW = """
MATCH (c:Campaign)
WHERE c.platform_id = $platform_id
WITH c.platform_id AS platform,
     count(c) AS total_campaigns,
     count(CASE WHEN c.status = 'active' THEN 1 END) AS active_campaigns,
     SUM(c.impressions_total) AS impressions,
     SUM(c.clicks_total) AS clicks,
     SUM(c.spend_total) AS spend,
     SUM(c.conversions_total) AS conversions,
     SUM(c.revenue_total) AS revenue
RETURN platform,
       total_campaigns,
       active_campaigns,
       impressions,
       clicks,
       spend,
       conversions,
       revenue,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END AS cpc,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
"""

# Template: Platform campaigns list
PLATFORM_CAMPAIGNS = """
MATCH (c:Campaign)
WHERE c.platform_id = $platform_id
OPTIONAL MATCH (c)-[:HAS_TARGETING]->(t:Targeting)
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       c.objective AS objective,
       c.status AS status,
       c.spend_total AS spend,
       c.impressions_total AS impressions,
       c.conversions_total AS conversions,
       CASE WHEN c.impressions_total > 0 
            THEN c.clicks_total * 100.0 / c.impressions_total ELSE 0 END AS ctr,
       CASE WHEN c.spend_total > 0 
            THEN c.revenue_total / c.spend_total ELSE 0 END AS roas,
       t.device_types AS devices,
       t.age_range AS age_range
ORDER BY c.spend_total DESC
LIMIT $limit
"""

# Template: All platforms comparison
ALL_PLATFORMS_COMPARISON = """
MATCH (c:Campaign)
WITH c.platform_id AS platform,
     count(c) AS campaigns,
     SUM(c.spend_total) AS spend,
     SUM(c.impressions_total) AS impressions,
     SUM(c.clicks_total) AS clicks,
     SUM(c.conversions_total) AS conversions,
     SUM(c.revenue_total) AS revenue
RETURN platform,
       campaigns,
       spend,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END AS cpc,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY spend DESC
"""

# Template: Top performers on platform
PLATFORM_TOP_CAMPAIGNS = """
MATCH (c:Campaign)
WHERE c.platform_id = $platform_id AND c.spend_total > 0
WITH c,
     CASE WHEN c.spend_total > 0 THEN c.revenue_total / c.spend_total ELSE 0 END AS roas,
     CASE WHEN c.impressions_total > 0 THEN c.clicks_total * 100.0 / c.impressions_total ELSE 0 END AS ctr
ORDER BY roas DESC
LIMIT $limit
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       c.spend_total AS spend,
       c.revenue_total AS revenue,
       roas,
       ctr
"""

# Template: Bottom performers on platform
PLATFORM_BOTTOM_CAMPAIGNS = """
MATCH (c:Campaign)
WHERE c.platform_id = $platform_id AND c.spend_total > $min_spend
WITH c,
     CASE WHEN c.spend_total > 0 THEN c.revenue_total / c.spend_total ELSE 0 END AS roas,
     CASE WHEN c.conversions_total > 0 THEN c.spend_total / c.conversions_total ELSE 999999 END AS cpa
ORDER BY roas ASC
LIMIT $limit
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       c.spend_total AS spend,
       c.conversions_total AS conversions,
       roas,
       cpa
"""

# Template: Global top campaigns
GLOBAL_TOP_CAMPAIGNS = """
MATCH (c:Campaign)
WITH c,
     CASE WHEN c.spend_total > 0 THEN c.revenue_total / c.spend_total ELSE 0 END AS roas,
     CASE WHEN c.impressions_total > 0 THEN c.clicks_total * 100.0 / c.impressions_total ELSE 0 END AS ctr
ORDER BY c.spend_total DESC
LIMIT $limit
RETURN c.name AS campaign_name,
       c.platform_id AS platform,
       c.spend_total AS spend,
       c.revenue_total AS revenue,
       c.impressions_total AS impressions,
       c.clicks_total AS clicks,
       c.conversions_total AS conversions,
       roas,
       ctr
"""

def get_global_top_campaigns(limit: int = 10) -> tuple[str, Dict[str, Any]]:
    """Get global top campaigns by spend."""
    return GLOBAL_TOP_CAMPAIGNS, {"limit": limit}




def get_platform_overview(platform_id: str) -> tuple[str, Dict[str, Any]]:
    """Get platform overview query."""
    return PLATFORM_OVERVIEW, {"platform_id": platform_id}


def get_platform_campaigns(
    platform_id: str,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get campaigns on platform."""
    return PLATFORM_CAMPAIGNS, {"platform_id": platform_id, "limit": limit}


def get_all_platforms_comparison() -> tuple[str, Dict[str, Any]]:
    """Get all platforms comparison."""
    return ALL_PLATFORMS_COMPARISON, {}


def get_platform_top_campaigns(
    platform_id: str,
    limit: int = 10
) -> tuple[str, Dict[str, Any]]:
    """Get top performing campaigns on platform."""
    return PLATFORM_TOP_CAMPAIGNS, {"platform_id": platform_id, "limit": limit}


def get_platform_bottom_campaigns(
    platform_id: str,
    min_spend: float = 100.0,
    limit: int = 10
) -> tuple[str, Dict[str, Any]]:
    """Get worst performing campaigns on platform."""
    return PLATFORM_BOTTOM_CAMPAIGNS, {
        "platform_id": platform_id,
        "min_spend": min_spend,
        "limit": limit
    }
