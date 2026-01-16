"""
Query Templates for Common Marketing Analytics Questions

Provides pre-built SQL templates for common marketing queries to improve
query success rate and response time.
"""

from typing import Dict, List, Optional
import re


class QueryTemplate:
    """Represents a query template with patterns and SQL"""
    
    def __init__(self, name: str, patterns: List[str], sql: str, description: str):
        self.name = name
        self.patterns = patterns
        self.sql = sql
        self.description = description
    
    def matches(self, question: str) -> bool:
        """Check if question matches any pattern"""
        q_lower = question.lower()
        return any(pattern.lower() in q_lower for pattern in self.patterns)


# Marketing Analytics Query Templates
QUERY_TEMPLATES = {
    "funnel_analysis": QueryTemplate(
        name="Marketing Funnel Analysis",
        patterns=["funnel", "conversion funnel", "drop off", "drop-off", "funnel analysis", "marketing funnel", "awareness", "consideration"],
        sql="""
WITH stage_totals AS (
    SELECT
        Funnel AS funnel_stage,
        SUM("Total Spent") AS stage_spend,
        SUM("Impressions") AS stage_impressions,
        SUM("Clicks") AS stage_clicks,
        SUM("Site Visit") AS stage_conversions
    FROM all_campaigns
    WHERE Funnel IS NOT NULL AND Funnel != 'Unknown'
    GROUP BY Funnel
)
SELECT
    funnel_stage,
    stage_spend,
    stage_impressions,
    stage_clicks,
    stage_conversions,
    ROUND((stage_clicks / NULLIF(stage_impressions, 0)) * 100, 2) AS ctr,
    ROUND(stage_spend / NULLIF(stage_clicks, 0), 2) AS cpc,
    ROUND(stage_spend / NULLIF(stage_conversions, 0), 2) AS cpa,
    ROUND((stage_conversions / NULLIF(stage_clicks, 0)) * 100, 2) AS conversion_rate
FROM stage_totals
ORDER BY 
    CASE funnel_stage
        WHEN 'Awareness' THEN 1
        WHEN 'Consideration' THEN 2
        WHEN 'Conversion' THEN 3
        ELSE 4
    END
        """,
        description="Marketing funnel by stage (Awareness → Consideration → Conversion) with performance metrics"
    ),
    
    "top_campaigns": QueryTemplate(
        name="Top Performing Campaigns",
        patterns=["top campaigns", "best campaigns", "highest roas", "top performing", "best performing", "winners", "top 10"],
        sql="""
SELECT
    "Campaign_Name_Full",
    platform,
    channel,
    SUM("Total Spent") AS total_spend,
    SUM("Site Visit") AS total_conversions,
    AVG("ROAS") AS avg_roas,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa
FROM all_campaigns
WHERE "Site Visit" > 0
GROUP BY "Campaign_Name_Full", platform, channel
ORDER BY avg_roas DESC
LIMIT 10
        """,
        description="Top 10 campaigns by ROAS"
    ),
    
    "worst_campaigns": QueryTemplate(
        name="Underperforming Campaigns",
        patterns=["worst", "underperforming", "low roas", "poor performance", "losers", "wasting money"],
        sql="""
SELECT
    "Campaign_Name_Full",
    platform,
    channel,
    SUM("Total Spent") AS total_spend,
    SUM("Site Visit") AS total_conversions,
    AVG("ROAS") AS avg_roas,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa
FROM all_campaigns
WHERE "Total Spent" > 1000
GROUP BY "Campaign_Name_Full", platform, channel
ORDER BY avg_roas ASC
LIMIT 10
        """,
        description="Bottom 10 campaigns by ROAS (minimum $1000 spend)"
    ),
    
    "channel_comparison": QueryTemplate(
        name="Channel Performance Comparison",
        patterns=["channel comparison", "compare channels", "channel performance", "which channel"],
        sql="""
SELECT
    channel,
    COUNT(DISTINCT "Campaign_Name_Full") AS campaign_count,
    SUM("Total Spent") AS total_spend,
    SUM("Impressions") AS total_impressions,
    SUM("Clicks") AS total_clicks,
    SUM("Site Visit") AS total_conversions,
    ROUND((SUM("Clicks") / NULLIF(SUM("Impressions"), 0)) * 100, 2) AS ctr,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Clicks"), 0), 2) AS cpc,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa,
    AVG("ROAS") AS avg_roas
FROM all_campaigns
GROUP BY channel
ORDER BY total_spend DESC
        """,
        description="Compare performance metrics across all channels"
    ),
    
    "platform_comparison": QueryTemplate(
        name="Platform Performance Comparison",
        patterns=["platform comparison", "compare platforms", "platform performance", "which platform"],
        sql="""
SELECT
    platform,
    COUNT(DISTINCT "Campaign_Name_Full") AS campaign_count,
    SUM("Total Spent") AS total_spend,
    SUM("Impressions") AS total_impressions,
    SUM("Clicks") AS total_clicks,
    SUM("Site Visit") AS total_conversions,
    ROUND((SUM("Clicks") / NULLIF(SUM("Impressions"), 0)) * 100, 2) AS ctr,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Clicks"), 0), 2) AS cpc,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa,
    AVG("ROAS") AS avg_roas
FROM all_campaigns
GROUP BY platform
ORDER BY avg_roas DESC
        """,
        description="Compare performance metrics across all platforms"
    ),
    
    "monthly_trend": QueryTemplate(
        name="Monthly Trend Analysis",
        patterns=["monthly", "trend", "month", "over time", "by month"],
        sql="""
SELECT 
    DATE_TRUNC('month', CAST(date AS TIMESTAMP)) AS month,
    SUM("Total Spent") as total_spend,
    SUM("Site Visit") as total_conversions,
    SUM("Clicks") as total_clicks,
    SUM("Impressions") as total_impressions
FROM all_campaigns
WHERE date IS NOT NULL
GROUP BY DATE_TRUNC('month', CAST(date AS TIMESTAMP))
ORDER BY month DESC
LIMIT 12
        """,
        description="Analyze campaign performance trends by month"
    ),
    
    "high_spend_low_conversion": QueryTemplate(
        name="High Spend, Low Conversion Campaigns",
        patterns=["wasting money", "high spend low conversion", "inefficient", "money pit"],
        sql="""
SELECT
    "Campaign_Name_Full",
    platform,
    channel,
    SUM("Total Spent") AS total_spend,
    SUM("Site Visit") AS total_conversions,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa,
    ROUND((SUM("Site Visit") / NULLIF(SUM("Clicks"), 0)) * 100, 2) AS conversion_rate
FROM all_campaigns
GROUP BY "Campaign_Name_Full", platform, channel
HAVING SUM("Total Spent") > 5000 AND SUM("Site Visit") < 100
ORDER BY total_spend DESC
        """,
        description="Campaigns with high spend but low conversions (potential waste)"
    ),
    
    "summary": QueryTemplate(
        name="Overall Summary",
        patterns=["summary", "overview", "overall", "total", "all metrics", "show all"],
        sql="""
SELECT
    COUNT(DISTINCT "Campaign_Name_Full") AS total_campaigns,
    COUNT(DISTINCT platform) AS total_platforms,
    COUNT(DISTINCT channel) AS total_channels,
    SUM("Total Spent") AS total_spend,
    SUM("Impressions") AS total_impressions,
    SUM("Clicks") AS total_clicks,
    SUM("Site Visit") AS total_conversions,
    ROUND((SUM("Clicks") / NULLIF(SUM("Impressions"), 0)) * 100, 2) AS overall_ctr,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Clicks"), 0), 2) AS overall_cpc,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS overall_cpa,
    AVG("ROAS") AS avg_roas
FROM all_campaigns
        """,
        description="High-level summary of all campaign performance"
    ),
    
    "device_performance": QueryTemplate(
        name="Device Performance Analysis",
        patterns=["device performance", "mobile vs desktop", "mobile versus desktop", "device type", "mobile and desktop", "desktop and mobile", "mobile or desktop", "device comparison", "mobile desktop", "compare device"],
        sql="""
SELECT
    device_type,
    COUNT(DISTINCT "Campaign_Name_Full") AS campaign_count,
    SUM("Total Spent") AS total_spend,
    SUM("Impressions") AS total_impressions,
    SUM("Clicks") AS total_clicks,
    SUM("Site Visit") AS total_conversions,
    ROUND((SUM("Clicks") / NULLIF(SUM("Impressions"), 0)) * 100, 2) AS ctr,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Clicks"), 0), 2) AS cpc,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa,
    ROUND((SUM("Site Visit") / NULLIF(SUM("Clicks"), 0)) * 100, 2) AS conversion_rate
FROM all_campaigns
WHERE device_type IS NOT NULL AND device_type != 'Unknown'
GROUP BY device_type
ORDER BY total_spend DESC
        """,
        description="Performance breakdown by device type (Mobile, Desktop, Tablet)"
    ),
    
    "daily_performance": QueryTemplate(
        name="Daily Performance Trend",
        patterns=["daily", "by day", "day by day", "daily trend", "performance by date"],
        sql="""
SELECT
    date,
    SUM("Total Spent") AS daily_spend,
    SUM("Impressions") AS daily_impressions,
    SUM("Clicks") AS daily_clicks,
    SUM("Site Visit") AS daily_conversions,
    ROUND((SUM("Clicks") / NULLIF(SUM("Impressions"), 0)) * 100, 2) AS ctr,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa
FROM all_campaigns
GROUP BY date
ORDER BY date DESC
LIMIT 30
        """,
        description="Daily performance metrics for the last 30 days"
    ),
    
    "budget_pacing": QueryTemplate(
        name="Budget Pacing Analysis",
        patterns=["budget", "pacing", "spend rate", "burn rate", "budget utilization"],
        sql="""
WITH daily_spend AS (
    SELECT
        date,
        SUM("Total Spent") AS daily_total
    FROM all_campaigns
    GROUP BY date
),
spend_stats AS (
    SELECT
        AVG(daily_total) AS avg_daily_spend,
        SUM(daily_total) AS total_spend,
        COUNT(DISTINCT date) AS days_active
    FROM daily_spend
)
SELECT
    total_spend,
    days_active,
    avg_daily_spend,
    ROUND(avg_daily_spend * 30, 2) AS projected_monthly_spend,
    ROUND(total_spend / NULLIF(days_active, 0), 2) AS actual_daily_avg
FROM spend_stats
        """,
        description="Budget pacing and spend rate analysis"
    ),
    
    "roas_by_funnel": QueryTemplate(
        name="ROAS by Funnel Stage",
        patterns=["roas by funnel", "roas by stage", "funnel roas", "return by stage"],
        sql="""
SELECT
    Funnel AS funnel_stage,
    SUM("Total Spent") AS total_spend,
    SUM("Site Visit") AS total_conversions,
    AVG("ROAS") AS avg_roas,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa
FROM all_campaigns
WHERE Funnel IS NOT NULL AND Funnel != 'Unknown'
GROUP BY Funnel
ORDER BY 
    CASE Funnel
        WHEN 'Awareness' THEN 1
        WHEN 'Consideration' THEN 2
        WHEN 'Conversion' THEN 3
        ELSE 4
    END
        """,
        description="ROAS and efficiency metrics by funnel stage"
    ),
    
    "top_campaigns_by_conversions": QueryTemplate(
        name="Top Campaigns by Conversions",
        patterns=["most conversions", "highest conversions", "top converting", "best converters"],
        sql="""
SELECT
    "Campaign_Name_Full",
    platform,
    channel,
    SUM("Total Spent") AS total_spend,
    SUM("Site Visit") AS total_conversions,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa,
    ROUND((SUM("Site Visit") / NULLIF(SUM("Clicks"), 0)) * 100, 2) AS conversion_rate
FROM all_campaigns
WHERE "Site Visit" > 0
GROUP BY "Campaign_Name_Full", platform, channel
ORDER BY total_conversions DESC
LIMIT 10
        """,
        description="Top 10 campaigns by total conversions"
    ),
    
    "low_ctr_campaigns": QueryTemplate(
        name="Low CTR Campaigns",
        patterns=["low ctr", "poor click rate", "bad engagement", "low engagement"],
        sql="""
WITH campaign_ctr AS (
    SELECT
        "Campaign_Name_Full",
        platform,
        channel,
        SUM("Total Spent") AS total_spend,
        SUM("Impressions") AS total_impressions,
        SUM("Clicks") AS total_clicks,
        ROUND((SUM("Clicks") / NULLIF(SUM("Impressions"), 0)) * 100, 2) AS ctr
    FROM all_campaigns
    GROUP BY "Campaign_Name_Full", platform, channel
    HAVING SUM("Impressions") > 1000
)
SELECT
    "Campaign_Name_Full",
    platform,
    channel,
    total_spend,
    total_impressions,
    total_clicks,
    ctr
FROM campaign_ctr
WHERE ctr < 1.0
ORDER BY total_spend DESC
LIMIT 10
        """,
        description="Campaigns with low CTR (< 1%) and significant spend"
    ),
    
    "platform_channel_matrix": QueryTemplate(
        name="Platform-Channel Performance Matrix",
        patterns=["platform channel", "platform by channel", "channel by platform", "cross analysis"],
        sql="""
SELECT
    platform,
    channel,
    COUNT(DISTINCT "Campaign_Name_Full") AS campaigns,
    SUM("Total Spent") AS total_spend,
    SUM("Site Visit") AS total_conversions,
    ROUND((SUM("Clicks") / NULLIF(SUM("Impressions"), 0)) * 100, 2) AS ctr,
    ROUND(SUM("Total Spent") / NULLIF(SUM("Site Visit"), 0), 2) AS cpa,
    AVG("ROAS") AS avg_roas
FROM all_campaigns
GROUP BY platform, channel
ORDER BY total_spend DESC
        """,
        description="Performance matrix showing all platform-channel combinations"
    ),
}


def find_matching_template(question: str) -> Optional[QueryTemplate]:
    """
    Find the best matching template for a question.
    
    Args:
        question: Natural language question
        
    Returns:
        QueryTemplate if match found, None otherwise
    """
    for template in QUERY_TEMPLATES.values():
        if template.matches(question):
            return template
    return None


def get_suggested_questions() -> List[Dict[str, str]]:
    """
    Get a list of suggested questions users can ask.
    
    Returns:
        List of dicts with 'question' and 'description' keys
    """
    suggestions = [
        {"question": "Show funnel analysis", "description": "Marketing funnel by stage (Awareness → Consideration → Conversion)"},
        {"question": "Which platform has the best ROAS?", "description": "Platform performance comparison"},
        {"question": "Show me top performing campaigns", "description": "Best campaigns by ROAS"},
        {"question": "Where am I wasting money?", "description": "High spend, low conversion campaigns"},
        {"question": "Show monthly trends", "description": "Performance over time"},
        {"question": "Compare channels", "description": "Channel performance comparison"},
        {"question": "How is mobile vs desktop performing?", "description": "Device performance breakdown"},
        {"question": "Show daily performance", "description": "Daily metrics for last 30 days"},
        {"question": "What's my budget pacing?", "description": "Spend rate and projections"},
        {"question": "Which campaigns have low CTR?", "description": "Campaigns with poor engagement"},
    ]
    return suggestions
