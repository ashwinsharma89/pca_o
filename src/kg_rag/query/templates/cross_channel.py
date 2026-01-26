"""
Cross-Channel Query Templates

Cypher templates for comparing performance across channels.
"""

from typing import Dict, Any, List, Optional


# Template: Compare two channels by metric
COMPARE_CHANNELS = """
MATCH (m:Metric)
WHERE toLower(m.channel) = toLower($channel1) OR toLower(m.channel) = toLower($channel2)
WITH m.channel AS channel,
     count(DISTINCT m.campaign_id) AS campaigns,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
RETURN channel,
       campaigns,
       spend,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END AS cpc,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY spend DESC
"""

# Template: All channels breakdown
ALL_CHANNELS_BREAKDOWN = """
MATCH (m:Metric)
WITH m.channel AS channel,
     count(DISTINCT m.campaign_id) AS campaigns,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
RETURN channel,
       campaigns,
       spend,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END AS cpc,
       CASE WHEN conversions > 0 THEN spend / conversions ELSE 0 END AS cpa,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY spend DESC
"""

# Template: Channel by metric comparison
CHANNEL_METRIC_COMPARISON = """
MATCH (m:Metric)
WHERE m.date >= date($date_from) AND m.date <= date($date_to)
WITH m.channel AS channel,
     SUM(m.impressions) AS impressions,
     SUM(m.clicks) AS clicks,
     SUM(m.spend) AS spend,
     SUM(coalesce(m.conversions, 0)) AS conversions,
     SUM(coalesce(m.revenue, 0)) AS revenue
RETURN channel,
       impressions,
       clicks,
       spend,
       conversions,
       revenue,
       CASE WHEN impressions > 0 THEN clicks * 100.0 / impressions ELSE 0 END AS ctr,
       CASE WHEN clicks > 0 THEN spend / clicks ELSE 0 END AS cpc,
       CASE WHEN spend > 0 THEN revenue / spend ELSE 0 END AS roas
ORDER BY spend DESC
LIMIT $limit
"""


def get_compare_channels_query(
    channel1: str,
    channel2: str
) -> tuple[str, Dict[str, Any]]:
    """Get query for comparing two channels."""
    return COMPARE_CHANNELS, {
        "channel1": channel1,
        "channel2": channel2
    }


def get_all_channels_breakdown() -> tuple[str, Dict[str, Any]]:
    """Get query for all channels breakdown."""
    return ALL_CHANNELS_BREAKDOWN, {}


def get_channel_metric_comparison(
    date_from: str,
    date_to: str,
    order_by: str = "spend",
    limit: int = 10
) -> tuple[str, Dict[str, Any]]:
    """Get query for channel metric comparison with date range."""
    return CHANNEL_METRIC_COMPARISON, {
        "date_from": date_from,
        "date_to": date_to,
        "order_by": order_by,
        "limit": limit
    }
