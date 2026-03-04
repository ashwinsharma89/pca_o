"""
KG-RAG Node Type Definitions

Defines node type labels and properties for the Knowledge Graph.
KùzuDB DDL for node table creation is provided in KUZU_NODE_DDL.
"""

from enum import Enum


class NodeLabel(Enum):
    """Node type labels for the Knowledge Graph."""

    CHANNEL = "Channel"
    PLATFORM = "Platform"
    ACCOUNT = "Account"
    CAMPAIGN = "Campaign"
    TARGETING = "Targeting"
    METRIC = "Metric"
    ENTITY_GROUP = "EntityGroup"
    CREATIVE = "Creative"
    KEYWORD = "Keyword"
    PLACEMENT = "Placement"
    AUDIENCE = "Audience"


KUZU_NODE_DDL = [
    """CREATE NODE TABLE IF NOT EXISTS Channel(
        id STRING,
        name STRING,
        description STRING,
        PRIMARY KEY(id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Platform(
        id STRING,
        name STRING,
        channel_id STRING,
        api_source STRING,
        parent_company STRING,
        supports_keywords BOOLEAN,
        supports_placements BOOLEAN,
        supports_video_metrics BOOLEAN,
        supports_reach BOOLEAN,
        supports_revenue BOOLEAN,
        supports_b2b_targeting BOOLEAN,
        PRIMARY KEY(id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Account(
        id STRING,
        name STRING,
        platform_id STRING,
        currency STRING,
        timezone STRING,
        PRIMARY KEY(id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Campaign(
        id STRING,
        account_id STRING,
        platform_id STRING,
        name STRING,
        objective STRING,
        status STRING,
        budget DOUBLE,
        budget_type STRING,
        start_date STRING,
        end_date STRING,
        impressions_total INT64,
        clicks_total INT64,
        spend_total DOUBLE,
        conversions_total DOUBLE,
        revenue_total DOUBLE,
        updated_at TIMESTAMP,
        PRIMARY KEY(id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Targeting(
        campaign_id STRING,
        age_range STRING,
        gender STRING,
        languages STRING[],
        geo_countries STRING[],
        geo_regions STRING[],
        geo_cities STRING[],
        device_types STRING[],
        operating_systems STRING[],
        interests STRING[],
        job_titles STRING[],
        companies STRING[],
        funnel_stage STRING,
        bid_strategy STRING,
        bid_amount DOUBLE,
        ad_type STRING,
        completeness_score DOUBLE,
        updated_at TIMESTAMP,
        PRIMARY KEY(campaign_id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Metric(
        id STRING,
        campaign_id STRING,
        date DATE,
        impressions INT64,
        clicks INT64,
        spend DOUBLE,
        conversions DOUBLE,
        revenue DOUBLE,
        reach INT64,
        frequency DOUBLE,
        video_plays INT64,
        video_completes INT64,
        engagements INT64,
        channel STRING,
        platform STRING,
        funnel STRING,
        ad_type STRING,
        placement STRING,
        device_types STRING[],
        age_range STRING,
        geo_countries STRING[],
        gender_targeting STRING,
        audience_segment STRING,
        creative_format STRING,
        targeting_type STRING,
        campaign_objective STRING,
        bid_strategy STRING,
        updated_at TIMESTAMP,
        PRIMARY KEY(id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS EntityGroup(
        id STRING,
        campaign_id STRING,
        name STRING,
        entity_type STRING,
        daily_budget DOUBLE,
        bid_strategy STRING,
        status STRING,
        updated_at TIMESTAMP,
        PRIMARY KEY(id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Creative(
        id STRING,
        entity_group_id STRING,
        name STRING,
        creative_type STRING,
        headline STRING,
        description STRING,
        landing_url STRING,
        image_url STRING,
        video_url STRING,
        ad_strength STRING,
        PRIMARY KEY(id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Keyword(
        id STRING,
        entity_group_id STRING,
        text STRING,
        match_type STRING,
        quality_score INT64,
        bid_amount DOUBLE,
        status STRING,
        impressions INT64,
        clicks INT64,
        spend DOUBLE,
        conversions DOUBLE,
        updated_at TIMESTAMP,
        PRIMARY KEY(id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Placement(
        id STRING,
        entity_group_id STRING,
        campaign_id STRING,
        name STRING,
        type STRING,
        url STRING,
        category STRING,
        iab_category STRING,
        position STRING,
        viewability_rate DOUBLE,
        impressions INT64,
        clicks INT64,
        spend DOUBLE,
        conversions DOUBLE,
        updated_at TIMESTAMP,
        PRIMARY KEY(id)
    )""",
    """CREATE NODE TABLE IF NOT EXISTS Audience(
        id STRING,
        name STRING,
        type STRING,
        size INT64,
        source STRING,
        PRIMARY KEY(id)
    )""",
]
