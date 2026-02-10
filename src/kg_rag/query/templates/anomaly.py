"""
Anomaly Detection Query Templates

Cypher templates for identifying outliers and anomalies.
"""

from typing import Dict, Any


# Template: High CPC campaigns
HIGH_CPC_CAMPAIGNS = """
MATCH (m:Metric)
WHERE m.clicks > $min_clicks
WITH m.campaign_id AS campaign_id,
     SUM(m.spend) / SUM(m.clicks) AS cpc
WITH avg(cpc) AS avg_cpc, stdev(cpc) AS std_cpc, collect({campaign_id: campaign_id, cpc: cpc}) AS campaigns
UNWIND campaigns AS item
WITH item.campaign_id AS campaign_id, item.cpc AS cpc, avg_cpc, std_cpc
WHERE cpc > avg_cpc + ($threshold * std_cpc)
MATCH (c:Campaign {id: campaign_id})-[:HAS_PERFORMANCE]->(m2:Metric)
WITH c, cpc, avg_cpc, std_cpc, SUM(m2.spend) as spend, SUM(m2.clicks) as clicks
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       c.platform_id AS platform,
       spend,
       clicks,
       cpc,
       avg_cpc,
       (cpc - avg_cpc) / std_cpc AS z_score
ORDER BY z_score DESC
LIMIT $limit
"""

# Template: Low ROAS campaigns
LOW_ROAS_CAMPAIGNS = """
MATCH (m:Metric)
WHERE m.spend > $min_spend AND m.revenue IS NOT NULL
WITH m.campaign_id AS campaign_id,
     SUM(m.revenue) / SUM(m.spend) AS roas
WITH avg(roas) AS avg_roas, stdev(roas) AS std_roas, collect({campaign_id: campaign_id, roas: roas}) AS campaigns
UNWIND campaigns AS item
WITH item.campaign_id AS campaign_id, item.roas AS roas, avg_roas, std_roas
WHERE roas < avg_roas - ($threshold * std_roas) OR roas < $min_roas
MATCH (c:Campaign {id: campaign_id})-[:HAS_PERFORMANCE]->(m2:Metric)
WITH c, roas, avg_roas, std_roas, SUM(m2.spend) as spend, SUM(coalesce(m2.revenue, 0)) as revenue
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       c.platform_id AS platform,
       spend,
       revenue,
       roas,
       avg_roas,
       CASE WHEN std_roas > 0 THEN (avg_roas - roas) / std_roas ELSE 0 END AS z_score
ORDER BY roas ASC
LIMIT $limit
"""

# Template: Spend spikes (daily)
SPEND_SPIKES = """
MATCH (m:Metric)
WHERE m.date >= date($date_from) AND m.date <= date($date_to)
WITH m.campaign_id AS campaign_id, m.date AS date, m.spend AS daily_spend
WITH campaign_id, date, daily_spend,
     avg(daily_spend) OVER (PARTITION BY campaign_id ORDER BY date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) AS avg_7d
WHERE daily_spend > avg_7d * $multiplier
MATCH (c:Campaign {id: campaign_id})
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       date,
       daily_spend,
       avg_7d,
       daily_spend / avg_7d AS spike_ratio
ORDER BY spike_ratio DESC
LIMIT $limit
"""

# Template: Zero performance campaigns
ZERO_PERFORMANCE = """
MATCH (c:Campaign {status: 'active'})-[:HAS_PERFORMANCE]->(m:Metric)
WITH c, SUM(m.spend) as spend, SUM(m.clicks) as clicks, SUM(m.impressions) as impressions
WHERE spend > $min_spend AND (clicks = 0 OR impressions = 0)
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       c.platform_id AS platform,
       spend,
       impressions,
       clicks,
       'Zero clicks or impressions despite spend' AS issue
ORDER BY spend DESC
LIMIT $limit
"""

# Template: Poor CTR campaigns
POOR_CTR_CAMPAIGNS = """
MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
WITH c, SUM(m.impressions) as impressions, SUM(m.clicks) as clicks
WHERE impressions > $min_impressions
WITH c, impressions, clicks,
     CASE WHEN impressions > 0 
          THEN clicks * 100.0 / impressions ELSE 0 END AS ctr
WITH percentileDisc(ctr, 0.1) AS p10_ctr, collect({campaign: c, ctr: ctr, impressions: impressions, clicks: clicks}) AS campaigns
UNWIND campaigns AS item
WITH item.campaign AS c, item.ctr AS ctr, item.impressions AS impressions, item.clicks AS clicks, p10_ctr
WHERE ctr <= p10_ctr
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       c.platform_id AS platform,
       impressions,
       clicks,
       ctr,
       p10_ctr AS threshold_ctr,
       'Bottom 10% CTR' AS issue
ORDER BY ctr ASC
LIMIT $limit
"""

# Template: Conversion drops
CONVERSION_DROPS = """
MATCH (m:Metric)
WHERE m.date >= date($date_from) AND m.date <= date($date_to)
WITH m.campaign_id AS campaign_id, m.date AS date, coalesce(m.conversions, 0) AS conversions
ORDER BY campaign_id, date
WITH campaign_id, collect({date: date, conversions: conversions}) AS daily_data
WHERE size(daily_data) >= 7
WITH campaign_id, daily_data,
     reduce(acc = 0.0, x IN daily_data[-7..-1] | acc + x.conversions) / 7.0 AS recent_avg,
     reduce(acc = 0.0, x IN daily_data[0..7] | acc + x.conversions) / 7.0 AS initial_avg
WHERE initial_avg > 0 AND recent_avg / initial_avg < $drop_threshold
MATCH (c:Campaign {id: campaign_id})
RETURN c.id AS campaign_id,
       c.name AS campaign_name,
       initial_avg,
       recent_avg,
       (initial_avg - recent_avg) / initial_avg * 100 AS drop_percentage
ORDER BY drop_percentage DESC
LIMIT $limit
"""


def get_high_cpc_campaigns(
    min_clicks: int = 100,
    threshold: float = 2.0,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get campaigns with unusually high CPC."""
    return HIGH_CPC_CAMPAIGNS, {
        "min_clicks": min_clicks,
        "threshold": threshold,
        "limit": limit
    }


def get_low_roas_campaigns(
    min_spend: float = 100.0,
    min_roas: float = 0.5,
    threshold: float = 1.5,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get campaigns with low ROAS."""
    return LOW_ROAS_CAMPAIGNS, {
        "min_spend": min_spend,
        "min_roas": min_roas,
        "threshold": threshold,
        "limit": limit
    }


def get_spend_spikes(
    date_from: str,
    date_to: str,
    multiplier: float = 2.0,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get daily spend spikes."""
    return SPEND_SPIKES, {
        "date_from": date_from,
        "date_to": date_to,
        "multiplier": multiplier,
        "limit": limit
    }


def get_zero_performance(
    min_spend: float = 50.0,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get campaigns with zero performance."""
    return ZERO_PERFORMANCE, {"min_spend": min_spend, "limit": limit}


def get_poor_ctr_campaigns(
    min_impressions: int = 10000,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get campaigns with poor CTR."""
    return POOR_CTR_CAMPAIGNS, {"min_impressions": min_impressions, "limit": limit}


def get_conversion_drops(
    date_from: str,
    date_to: str,
    drop_threshold: float = 0.5,
    limit: int = 20
) -> tuple[str, Dict[str, Any]]:
    """Get campaigns with conversion drops."""
    return CONVERSION_DROPS, {
        "date_from": date_from,
        "date_to": date_to,
        "drop_threshold": drop_threshold,
        "limit": limit
    }
