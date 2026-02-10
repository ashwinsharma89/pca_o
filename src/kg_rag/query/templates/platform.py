"""
Platform Query Templates

Cypher templates for platform-specific performance queries.
"""

from typing import Dict, Any, Optional


# Template: Platform overview
PLATFORM_OVERVIEW = """
MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WHERE c.platform_id = $platform_id
WITH c.platform_id AS platform,
     count(DISTINCT c) AS total_campaigns,
     count(CASE WHEN c.status = 'active' THEN 1 END) AS active_campaigns,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
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
MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WHERE c.platform_id = $platform_id
WITH c, 
     SUM(m.spend) AS spend, 
     SUM(m.impressions) AS impressions, 
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue,
     SUM(m.clicks) AS clicks
OPTIONAL MATCH (c)-[:HAS_TARGETING]->(t:Targeting)
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       c.objective AS objective,
       c.status AS status,
       spend,
       impressions,
       conversions,
       CASE WHEN impressions > 0 
            THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN spend > 0 
            THEN revenue / spend ELSE 0 END AS roas,
       t.device_types AS devices,
       t.age_range AS age_range
ORDER BY spend DESC
LIMIT $limit
"""

# Template: All platforms comparison
ALL_PLATFORMS_COMPARISON = """
MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WITH c.platform_id AS platform,
     count(DISTINCT c) AS campaigns,
     SUM(m.spend) AS spend,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
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
MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WHERE c.platform_id = $platform_id
WITH c, 
     SUM(m.spend) AS spend, 
     SUM(coalesce(m.revenue, 0)) AS revenue,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(coalesce(m.conversions, 0)) AS conversions
WHERE spend > 0
WITH c, spend, revenue,
     CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas,
     CASE WHEN conversions > 0 THEN spend / conversions ELSE 999999 END AS cpa,
     CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr
{order_by_clause}
LIMIT $limit
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       spend,
       revenue,
       roas,
       cpa,
       ctr
"""

# Template: Bottom performers on platform
PLATFORM_BOTTOM_CAMPAIGNS = """
MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WHERE c.platform_id = $platform_id
WITH c, 
     SUM(m.spend) AS spend, 
     SUM(coalesce(m.revenue, 0)) AS revenue,
     SUM(coalesce(m.conversions, 0)) AS conversions
WHERE spend > $min_spend
WITH c, spend,
     CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas,
     CASE WHEN conversions > 0 THEN spend / conversions ELSE 999999 END AS cpa
ORDER BY roas ASC, cpa DESC
LIMIT $limit
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       spend,
       conversions,
       roas,
       cpa
"""

# Template: Global top campaigns
GLOBAL_TOP_CAMPAIGNS = """
MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WITH c, 
     SUM(m.spend) AS spend, 
     SUM(coalesce(m.revenue, 0)) AS revenue,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(coalesce(m.conversions, 0)) AS conversions
WITH c, spend, revenue,
     CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas,
     CASE WHEN conversions > 0 THEN spend / conversions ELSE 999999 END AS cpa,
     CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr
{order_by_clause}
LIMIT $limit
RETURN c.name AS campaign_name,
       c.platform_id AS platform,
       spend,
       revenue,
       roas,
       cpa,
       ctr
"""

# Template: Top performers on channel
CHANNEL_TOP_CAMPAIGNS = """
MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WHERE toLower(m.channel) = toLower($channel)
WITH c, m.channel AS channel_name,
     SUM(m.spend) AS spend, 
     SUM(coalesce(m.revenue, 0)) AS revenue,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(coalesce(m.conversions, 0)) AS conversions
WHERE spend > 0
WITH c, channel_name, spend, revenue,
     CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas,
     CASE WHEN conversions > 0 THEN spend / conversions ELSE 999999 END AS cpa,
     CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr
{order_by_clause}
LIMIT $limit
RETURN channel_name AS channel,
       c.name AS campaign_name,
       spend,
       revenue,
       roas,
       cpa,
       ctr
"""

def get_global_top_campaigns(limit: int = 10, sort_by: str = "performance") -> tuple[str, Dict[str, Any]]:
    """Get global top campaigns."""
    order_clause = "ORDER BY roas DESC, cpa ASC, ctr DESC"
    if sort_by == "spend":
        order_clause = "ORDER BY spend DESC"
    
    query = GLOBAL_TOP_CAMPAIGNS.format(order_by_clause=order_clause)
    return query, {"limit": limit}


def get_channel_top_campaigns(
    channel: str,
    limit: int = 10,
    sort_by: str = "performance"
) -> tuple[str, Dict[str, Any]]:
    """Get top performing campaigns on channel."""
    order_clause = "ORDER BY roas DESC, cpa ASC, ctr DESC"
    if sort_by == "spend":
        order_clause = "ORDER BY spend DESC"
        
    query = CHANNEL_TOP_CAMPAIGNS.format(order_by_clause=order_clause)
    return query, {"channel": channel, "limit": limit}


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
    limit: int = 10,
    sort_by: str = "performance"
) -> tuple[str, Dict[str, Any]]:
    """Get top performing campaigns on platform."""
    order_clause = "ORDER BY roas DESC, cpa ASC, ctr DESC"
    if sort_by == "spend":
        order_clause = "ORDER BY spend DESC"
        
    query = PLATFORM_TOP_CAMPAIGNS.format(order_by_clause=order_clause)
    return query, {"platform_id": platform_id, "limit": limit}


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
