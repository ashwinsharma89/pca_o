"""
KG-RAG Schema — KùzuDB Indexes

KùzuDB automatically builds an index on each node table's PRIMARY KEY column.
Secondary indexes on non-PK properties are not yet supported via Cypher DDL;
queries on non-PK properties use sequential scans, which is acceptable for the
expected data volumes.

The index definitions are kept here for documentation and future use.
"""

from typing import List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class IndexDefinition:
    """Index definition (informational — not executed in KùzuDB)."""
    label: str
    properties: List[str]
    index_type: str = "RANGE"
    name: Optional[str] = None

    def get_name(self) -> str:
        if self.name:
            return self.name
        props_str = "_".join(self.properties)
        return f"idx_{self.label.lower()}_{props_str}"


# Documented for reference
SINGLE_PROPERTY_INDEXES: List[IndexDefinition] = [
    IndexDefinition("Campaign", ["platform_id"]),
    IndexDefinition("Campaign", ["status"]),
    IndexDefinition("Campaign", ["objective"]),
    IndexDefinition("Campaign", ["start_date"]),
    IndexDefinition("Campaign", ["end_date"]),
    IndexDefinition("Campaign", ["name"], "TEXT"),
    IndexDefinition("Metric", ["campaign_id"]),
    IndexDefinition("Metric", ["date"]),
    IndexDefinition("Targeting", ["campaign_id"]),
    IndexDefinition("Targeting", ["funnel_stage"]),
    IndexDefinition("Targeting", ["bid_strategy"]),
    IndexDefinition("EntityGroup", ["campaign_id"]),
    IndexDefinition("EntityGroup", ["entity_type"]),
    IndexDefinition("Creative", ["entity_group_id"]),
    IndexDefinition("Creative", ["creative_type"]),
    IndexDefinition("Keyword", ["entity_group_id"]),
    IndexDefinition("Keyword", ["match_type"]),
    IndexDefinition("Keyword", ["text"], "TEXT"),
    IndexDefinition("Placement", ["entity_group_id"]),
    IndexDefinition("Placement", ["campaign_id"]),
    IndexDefinition("Placement", ["type"]),
    IndexDefinition("Placement", ["category"]),
    IndexDefinition("Platform", ["channel_id"]),
]

COMPOSITE_INDEXES: List[IndexDefinition] = [
    IndexDefinition("Campaign", ["platform_id", "status"]),
    IndexDefinition("Metric", ["campaign_id", "date"]),
    IndexDefinition("Targeting", ["device_types", "age_range"]),
    IndexDefinition("Placement", ["campaign_id", "type"]),
]

FULLTEXT_INDEXES = [
    {"name": "ft_campaign_search", "labels": ["Campaign"], "properties": ["name"]},
    {"name": "ft_keyword_search", "labels": ["Keyword"], "properties": ["text"]},
    {"name": "ft_placement_search", "labels": ["Placement"], "properties": ["name", "url"]},
]

RELATIONSHIP_INDEXES = [
    ("HAS_PERFORMANCE", "date"),
    ("CONTAINS", "position"),
    ("OVERLAPS_WITH", "overlap_pct"),
    ("SIMILAR_TO", "score"),
]


def generate_index_cypher() -> List[str]:
    """No-op: KùzuDB secondary indexes not supported via Cypher DDL."""
    return []


def generate_drop_index_cypher() -> List[str]:
    """No-op."""
    return []


def generate_relationship_index_cypher() -> List[str]:
    """No-op."""
    return []
