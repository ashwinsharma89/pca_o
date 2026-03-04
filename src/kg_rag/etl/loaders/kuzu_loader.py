"""
KG-RAG KùzuDB Loader

Batch loads transformed data into KùzuDB using UNWIND for efficiency.
"""

import logging
from typing import Dict, Any, List, Optional

from src.kg_rag.client.connection import get_kuzu_connection, KuzuConnection
from src.kg_rag.config.settings import get_kg_rag_settings


logger = logging.getLogger(__name__)


class KuzuLoader:
    """
    Load transformed data into KùzuDB using batch UNWIND operations.

    Usage:
        loader = KuzuLoader()
        loader.load_campaigns(campaign_records)
        loader.load_metrics(metric_records)
    """

    def __init__(self, connection: Optional[KuzuConnection] = None):
        self._conn = connection or get_kuzu_connection()
        self._settings = get_kg_rag_settings()

    def load_campaigns(
        self,
        campaigns: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> Dict[str, int]:
        """Load Campaign nodes (upsert via MERGE)."""
        batch_size = batch_size or self._settings.etl_batch_size

        query = """
        UNWIND $batch AS row
        MERGE (c:Campaign {id: row.id})
        SET c.name = row.name,
            c.platform_id = row.platform_id,
            c.account_id = row.account_id,
            c.objective = row.objective,
            c.status = row.status,
            c.budget = row.budget,
            c.budget_type = row.budget_type,
            c.start_date = row.start_date,
            c.end_date = row.end_date,
            c.impressions_total = coalesce(row.impressions_total, 0),
            c.clicks_total = coalesce(row.clicks_total, 0),
            c.spend_total = coalesce(row.spend_total, 0.0),
            c.conversions_total = coalesce(row.conversions_total, 0.0),
            c.revenue_total = coalesce(row.revenue_total, 0.0),
            c.updated_at = now()
        """
        return self._load_batches(campaigns, query, batch_size, "Campaign")

    def load_campaign_platform_relationships(
        self,
        campaigns: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        """Create BELONGS_TO relationships between Campaigns and Platforms."""
        query = """
        UNWIND $batch AS row
        MATCH (c:Campaign {id: row.id})
        MATCH (p:Platform {id: row.platform_id})
        MERGE (c)-[:BELONGS_TO]->(p)
        """
        filtered = [c for c in campaigns if c.get("platform_id")]
        return self._conn.execute_batch(query, filtered)

    def load_targeting(
        self,
        targeting_records: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> Dict[str, int]:
        """Load Targeting nodes and create HAS_TARGETING relationships."""
        batch_size = batch_size or self._settings.etl_batch_size

        # Explicit property SET (KùzuDB does not support SET n += map)
        query = """
        UNWIND $batch AS row
        MERGE (t:Targeting {campaign_id: row.campaign_id})
        SET t.age_range = row.age_range,
            t.gender = row.gender,
            t.languages = coalesce(row.languages, []),
            t.geo_countries = coalesce(row.geo_countries, []),
            t.geo_regions = coalesce(row.geo_regions, []),
            t.geo_cities = coalesce(row.geo_cities, []),
            t.device_types = coalesce(row.device_types, []),
            t.operating_systems = coalesce(row.operating_systems, []),
            t.interests = coalesce(row.interests, []),
            t.job_titles = coalesce(row.job_titles, []),
            t.companies = coalesce(row.companies, []),
            t.funnel_stage = row.funnel_stage,
            t.bid_strategy = row.bid_strategy,
            t.bid_amount = row.bid_amount,
            t.ad_type = row.ad_type,
            t.completeness_score = coalesce(row.completeness_score, 0.0),
            t.updated_at = now()
        """
        result = self._load_batches(targeting_records, query, batch_size, "Targeting")

        rel_query = """
        UNWIND $batch AS row
        MATCH (c:Campaign {id: row.campaign_id})
        MATCH (t:Targeting {campaign_id: row.campaign_id})
        MERGE (c)-[:HAS_TARGETING]->(t)
        """
        self._conn.execute_batch(rel_query, targeting_records)
        return result

    def load_metrics(
        self,
        metrics: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> Dict[str, int]:
        """Load Metric nodes and create HAS_PERFORMANCE relationships."""
        batch_size = batch_size or self._settings.etl_batch_size

        query = """
        UNWIND $batch AS row
        MERGE (m:Metric {id: row.id})
        SET m.campaign_id = row.campaign_id,
            m.date = date(row.date),
            m.impressions = coalesce(row.impressions, 0),
            m.clicks = coalesce(row.clicks, 0),
            m.spend = coalesce(row.spend, 0.0),
            m.conversions = row.conversions,
            m.revenue = row.revenue,
            m.reach = row.reach,
            m.video_plays = row.video_plays,
            m.video_completes = row.video_completes,
            m.engagements = row.engagements,
            m.channel = row.channel,
            m.platform = row.platform,
            m.funnel = row.funnel,
            m.ad_type = row.ad_type,
            m.placement = row.placement,
            m.device_types = coalesce(row.device_types, []),
            m.age_range = row.age_range,
            m.geo_countries = coalesce(row.geo_countries, []),
            m.gender_targeting = row.gender_targeting,
            m.audience_segment = row.audience_segment,
            m.creative_format = row.creative_format,
            m.targeting_type = row.targeting_type,
            m.campaign_objective = row.campaign_objective,
            m.bid_strategy = row.bid_strategy,
            m.updated_at = now()
        """
        result = self._load_batches(metrics, query, batch_size, "Metric")

        rel_query = """
        UNWIND $batch AS row
        MATCH (c:Campaign {id: row.campaign_id})
        MATCH (m:Metric {id: row.id})
        MERGE (c)-[:HAS_PERFORMANCE]->(m)
        """
        self._conn.execute_batch(rel_query, metrics)
        return result

    def update_campaign_totals(self, campaign_id: str) -> Dict[str, Any]:
        """Aggregate metrics into Campaign totals for a single campaign."""
        query = """
        MATCH (c:Campaign {id: $campaign_id})-[:HAS_PERFORMANCE]->(m:Metric)
        WITH c,
             SUM(m.impressions) AS total_impressions,
             SUM(m.clicks) AS total_clicks,
             SUM(m.spend) AS total_spend,
             SUM(coalesce(m.conversions, 0)) AS total_conversions,
             SUM(coalesce(m.revenue, 0)) AS total_revenue
        SET c.impressions_total = total_impressions,
            c.clicks_total = total_clicks,
            c.spend_total = total_spend,
            c.conversions_total = total_conversions,
            c.revenue_total = total_revenue,
            c.updated_at = now()
        RETURN c.impressions_total, c.clicks_total, c.spend_total,
               c.conversions_total, c.revenue_total
        """
        result = self._conn.execute_query(query, {"campaign_id": campaign_id})
        return result[0] if result else {}

    def update_all_campaign_totals(self) -> int:
        """Aggregate metrics into Campaign totals for all campaigns."""
        query = """
        MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
        WITH c,
             SUM(m.impressions) AS total_impressions,
             SUM(m.clicks) AS total_clicks,
             SUM(m.spend) AS total_spend,
             SUM(coalesce(m.conversions, 0)) AS total_conversions,
             SUM(coalesce(m.revenue, 0)) AS total_revenue
        SET c.impressions_total = total_impressions,
            c.clicks_total = total_clicks,
            c.spend_total = total_spend,
            c.conversions_total = total_conversions,
            c.revenue_total = total_revenue,
            c.updated_at = now()
        RETURN count(c) AS updated
        """
        result = self._conn.execute_query(query)
        return result[0]["updated"] if result else 0

    def load_entity_groups(
        self,
        entity_groups: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> Dict[str, int]:
        """Load EntityGroup nodes."""
        batch_size = batch_size or self._settings.etl_batch_size

        query = """
        UNWIND $batch AS row
        MERGE (eg:EntityGroup {id: row.id})
        SET eg.campaign_id = row.campaign_id,
            eg.name = row.name,
            eg.entity_type = row.entity_type,
            eg.daily_budget = row.daily_budget,
            eg.bid_strategy = row.bid_strategy,
            eg.status = row.status,
            eg.updated_at = now()
        """
        result = self._load_batches(entity_groups, query, batch_size, "EntityGroup")

        rel_query = """
        UNWIND $batch AS row
        MATCH (c:Campaign {id: row.campaign_id})
        MATCH (eg:EntityGroup {id: row.id})
        MERGE (c)-[:CONTAINS]->(eg)
        """
        self._conn.execute_batch(rel_query, entity_groups)
        return result

    def load_placements(
        self,
        placements: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> Dict[str, int]:
        """Load Placement nodes."""
        batch_size = batch_size or self._settings.etl_batch_size

        query = """
        UNWIND $batch AS row
        MERGE (p:Placement {id: row.id})
        SET p.entity_group_id = row.entity_group_id,
            p.campaign_id = row.campaign_id,
            p.name = row.name,
            p.type = row.type,
            p.url = row.url,
            p.category = row.category,
            p.iab_category = row.iab_category,
            p.position = row.position,
            p.viewability_rate = row.viewability_rate,
            p.impressions = coalesce(row.impressions, 0),
            p.clicks = coalesce(row.clicks, 0),
            p.spend = coalesce(row.spend, 0.0),
            p.conversions = coalesce(row.conversions, 0.0),
            p.updated_at = now()
        """
        result = self._load_batches(placements, query, batch_size, "Placement")

        rel_query = """
        UNWIND $batch AS row
        MATCH (eg:EntityGroup {id: row.entity_group_id})
        MATCH (p:Placement {id: row.id})
        MERGE (eg)-[:HAS_PLACEMENT]->(p)
        """
        filtered = [p for p in placements if p.get("entity_group_id")]
        if filtered:
            self._conn.execute_batch(rel_query, filtered)
        return result

    def load_keywords(
        self,
        keywords: List[Dict[str, Any]],
        batch_size: Optional[int] = None
    ) -> Dict[str, int]:
        """Load Keyword nodes."""
        batch_size = batch_size or self._settings.etl_batch_size

        query = """
        UNWIND $batch AS row
        MERGE (k:Keyword {id: row.id})
        SET k.entity_group_id = row.entity_group_id,
            k.text = row.text,
            k.match_type = row.match_type,
            k.quality_score = row.quality_score,
            k.bid_amount = row.bid_amount,
            k.status = row.status,
            k.impressions = coalesce(row.impressions, 0),
            k.clicks = coalesce(row.clicks, 0),
            k.spend = coalesce(row.spend, 0.0),
            k.conversions = coalesce(row.conversions, 0.0),
            k.updated_at = now()
        """
        result = self._load_batches(keywords, query, batch_size, "Keyword")

        rel_query = """
        UNWIND $batch AS row
        MATCH (eg:EntityGroup {id: row.entity_group_id})
        MATCH (k:Keyword {id: row.id})
        MERGE (eg)-[:HAS_KEYWORD]->(k)
        """
        self._conn.execute_batch(rel_query, keywords)
        return result

    def _load_batches(
        self,
        records: List[Dict[str, Any]],
        query: str,
        batch_size: int,
        node_type: str
    ) -> Dict[str, int]:
        """Load records in batches."""
        total_created = 0
        total_properties = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            result = self._conn.execute_batch(query, batch)
            total_created += result.get("nodes_created", 0)
            total_properties += result.get("properties_set", 0)
            logger.debug(
                f"Loaded {node_type} batch {i // batch_size + 1}: {len(batch)} records"
            )

        logger.info(f"Loaded {len(records)} {node_type} records")
        return {
            "records": len(records),
            "nodes_created": total_created,
            "properties_set": total_properties,
        }

    def get_stats(self) -> Dict[str, int]:
        """Get current node counts."""
        query = """
        MATCH (c:Campaign) WITH count(c) AS campaigns
        MATCH (t:Targeting) WITH campaigns, count(t) AS targeting
        MATCH (m:Metric) WITH campaigns, targeting, count(m) AS metrics
        MATCH (eg:EntityGroup) WITH campaigns, targeting, metrics, count(eg) AS entity_groups
        MATCH (p:Placement) WITH campaigns, targeting, metrics, entity_groups, count(p) AS placements
        MATCH (k:Keyword) WITH campaigns, targeting, metrics, entity_groups, placements, count(k) AS keywords
        RETURN campaigns, targeting, metrics, entity_groups, placements, keywords
        """
        result = self._conn.execute_query(query)
        return result[0] if result else {}

