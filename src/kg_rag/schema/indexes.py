"""
KG-RAG Schema Indexes

Defines indexes for query performance optimization.
"""

from typing import List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class IndexDefinition:
    """Index definition."""
    label: str
    properties: List[str]
    index_type: str = "RANGE"  # RANGE, TEXT, FULLTEXT
    name: Optional[str] = None
    
    def get_name(self) -> str:
        if self.name:
            return self.name
        props_str = "_".join(self.properties)
        return f"idx_{self.label.lower()}_{props_str}"


# Single-property indexes for common queries
SINGLE_PROPERTY_INDEXES: List[IndexDefinition] = [
    # Campaign indexes
    IndexDefinition("Campaign", ["platform_id"]),
    IndexDefinition("Campaign", ["status"]),
    IndexDefinition("Campaign", ["objective"]),
    IndexDefinition("Campaign", ["start_date"]),
    IndexDefinition("Campaign", ["end_date"]),
    IndexDefinition("Campaign", ["name"], "TEXT"),
    
    # Metric indexes
    IndexDefinition("Metric", ["campaign_id"]),
    IndexDefinition("Metric", ["date"]),
    
    # Targeting indexes
    IndexDefinition("Targeting", ["campaign_id"]),
    IndexDefinition("Targeting", ["funnel_stage"]),
    IndexDefinition("Targeting", ["bid_strategy"]),
    
    # EntityGroup indexes
    IndexDefinition("EntityGroup", ["campaign_id"]),
    IndexDefinition("EntityGroup", ["entity_type"]),
    
    # Creative indexes
    IndexDefinition("Creative", ["entity_group_id"]),
    IndexDefinition("Creative", ["creative_type"]),
    
    # Keyword indexes
    IndexDefinition("Keyword", ["entity_group_id"]),
    IndexDefinition("Keyword", ["match_type"]),
    IndexDefinition("Keyword", ["text"], "TEXT"),
    
    # Placement indexes
    IndexDefinition("Placement", ["entity_group_id"]),
    IndexDefinition("Placement", ["campaign_id"]),
    IndexDefinition("Placement", ["type"]),
    IndexDefinition("Placement", ["category"]),
    
    # Platform indexes
    IndexDefinition("Platform", ["channel_id"]),
]


# Composite indexes for common query patterns
COMPOSITE_INDEXES: List[IndexDefinition] = [
    # Campaign by platform and status
    IndexDefinition("Campaign", ["platform_id", "status"]),
    
    # Metrics by campaign and date range
    IndexDefinition("Metric", ["campaign_id", "date"]),
    
    # Targeting by multiple dimensions
    IndexDefinition("Targeting", ["device_types", "age_range"]),
    
    # Placement by campaign and type
    IndexDefinition("Placement", ["campaign_id", "type"]),
]


# Full-text search indexes
FULLTEXT_INDEXES = [
    {
        "name": "ft_campaign_search",
        "labels": ["Campaign"],
        "properties": ["name"],
    },
    {
        "name": "ft_keyword_search",
        "labels": ["Keyword"],
        "properties": ["text"],
    },
    {
        "name": "ft_placement_search",
        "labels": ["Placement"],
        "properties": ["name", "url"],
    },
]


def generate_index_cypher() -> List[str]:
    """
    Generate Cypher statements to create all indexes.
    
    Returns:
        List of CREATE INDEX statements
    """
    statements = []
    
    # Single and composite indexes
    for idx in SINGLE_PROPERTY_INDEXES + COMPOSITE_INDEXES:
        props_str = ", ".join([f"n.{p}" for p in idx.properties])
        
        if idx.index_type == "TEXT":
            cypher = f"""
CREATE TEXT INDEX {idx.get_name()} IF NOT EXISTS
FOR (n:{idx.label})
ON ({props_str})
            """.strip()
        else:
            cypher = f"""
CREATE INDEX {idx.get_name()} IF NOT EXISTS
FOR (n:{idx.label})
ON ({props_str})
            """.strip()
        
        statements.append(cypher)
    
    # Fulltext indexes
    for ft in FULLTEXT_INDEXES:
        labels_str = "|".join(ft["labels"])
        props_str = ", ".join([f"n.{p}" for p in ft["properties"]])
        cypher = f"""
CREATE FULLTEXT INDEX {ft['name']} IF NOT EXISTS
FOR (n:{labels_str})
ON EACH [{props_str}]
        """.strip()
        statements.append(cypher)
    
    return statements


def generate_drop_index_cypher() -> List[str]:
    """Generate Cypher to drop all indexes."""
    statements = []
    
    for idx in SINGLE_PROPERTY_INDEXES + COMPOSITE_INDEXES:
        statements.append(f"DROP INDEX {idx.get_name()} IF EXISTS")
    
    for ft in FULLTEXT_INDEXES:
        statements.append(f"DROP INDEX {ft['name']} IF EXISTS")
    
    return statements


# Relationship indexes (Neo4j 5.x)
RELATIONSHIP_INDEXES = [
    ("HAS_PERFORMANCE", "date"),
    ("CONTAINS", "position"),
    ("OVERLAPS_WITH", "overlap_pct"),
    ("SIMILAR_TO", "score"),
]


def generate_relationship_index_cypher() -> List[str]:
    """Generate relationship property indexes."""
    statements = []
    for rel_type, prop in RELATIONSHIP_INDEXES:
        name = f"idx_rel_{rel_type.lower()}_{prop}"
        cypher = f"""
CREATE INDEX {name} IF NOT EXISTS
FOR ()-[r:{rel_type}]-()
ON (r.{prop})
        """.strip()
        statements.append(cypher)
    return statements
