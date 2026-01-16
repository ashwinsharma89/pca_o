"""
KG-RAG Edge Definitions

Defines all relationship types between nodes in the Knowledge Graph.
"""

from dataclasses import dataclass
from typing import Optional, Any, Dict
from enum import Enum


class EdgeType(str, Enum):
    """Relationship types for Neo4j."""
    CATEGORIZES = "CATEGORIZES"       # Channel -> Platform
    HOSTS = "HOSTS"                   # Platform -> Account
    OWNS = "OWNS"                     # Account -> Campaign
    BELONGS_TO = "BELONGS_TO"         # Campaign -> Platform
    HAS_TARGETING = "HAS_TARGETING"   # Campaign -> Targeting
    HAS_PERFORMANCE = "HAS_PERFORMANCE"  # Campaign -> Metric
    CONTAINS = "CONTAINS"             # Campaign -> EntityGroup
    HAS_CREATIVE = "HAS_CREATIVE"     # EntityGroup -> Creative
    HAS_KEYWORD = "HAS_KEYWORD"       # EntityGroup -> Keyword
    HAS_PLACEMENT = "HAS_PLACEMENT"   # EntityGroup -> Placement
    OVERLAPS_WITH = "OVERLAPS_WITH"   # Audience <-> Audience
    SIMILAR_TO = "SIMILAR_TO"         # Creative <-> Creative


@dataclass
class EdgeDefinition:
    """Edge definition with properties."""
    type: EdgeType
    from_label: str
    to_label: str
    properties: Dict[str, str]  # property_name -> type
    description: str
    cardinality: str  # 1:1, 1:many, many:1, many:many


# All edge definitions
EDGE_DEFINITIONS = [
    EdgeDefinition(
        type=EdgeType.CATEGORIZES,
        from_label="Channel",
        to_label="Platform",
        properties={},
        description="Groups platforms by channel type",
        cardinality="1:many"
    ),
    EdgeDefinition(
        type=EdgeType.HOSTS,
        from_label="Platform",
        to_label="Account",
        properties={},
        description="Platform contains advertiser account",
        cardinality="1:many"
    ),
    EdgeDefinition(
        type=EdgeType.OWNS,
        from_label="Account",
        to_label="Campaign",
        properties={},
        description="Account owns campaign",
        cardinality="1:many"
    ),
    EdgeDefinition(
        type=EdgeType.BELONGS_TO,
        from_label="Campaign",
        to_label="Platform",
        properties={},
        description="Campaign runs on platform (back-reference)",
        cardinality="many:1"
    ),
    EdgeDefinition(
        type=EdgeType.HAS_TARGETING,
        from_label="Campaign",
        to_label="Targeting",
        properties={},
        description="Campaign targeting configuration",
        cardinality="1:1"
    ),
    EdgeDefinition(
        type=EdgeType.HAS_PERFORMANCE,
        from_label="Campaign",
        to_label="Metric",
        properties={"date": "DATE"},
        description="Daily performance metrics",
        cardinality="1:many"
    ),
    EdgeDefinition(
        type=EdgeType.CONTAINS,
        from_label="Campaign",
        to_label="EntityGroup",
        properties={"position": "INTEGER"},
        description="Ad groups within campaign",
        cardinality="1:many"
    ),
    EdgeDefinition(
        type=EdgeType.HAS_CREATIVE,
        from_label="EntityGroup",
        to_label="Creative",
        properties={},
        description="Ads within ad group",
        cardinality="1:many"
    ),
    EdgeDefinition(
        type=EdgeType.HAS_KEYWORD,
        from_label="EntityGroup",
        to_label="Keyword",
        properties={},
        description="Keywords (Search platforms only)",
        cardinality="1:many"
    ),
    EdgeDefinition(
        type=EdgeType.HAS_PLACEMENT,
        from_label="EntityGroup",
        to_label="Placement",
        properties={},
        description="Placement reporting (Display/Programmatic)",
        cardinality="1:many"
    ),
    EdgeDefinition(
        type=EdgeType.OVERLAPS_WITH,
        from_label="Audience",
        to_label="Audience",
        properties={"overlap_pct": "FLOAT"},
        description="Audience overlap analysis",
        cardinality="many:many"
    ),
    EdgeDefinition(
        type=EdgeType.SIMILAR_TO,
        from_label="Creative",
        to_label="Creative",
        properties={"score": "FLOAT"},
        description="Creative similarity scoring",
        cardinality="many:many"
    ),
]


def get_edge_definition(edge_type: EdgeType) -> Optional[EdgeDefinition]:
    """Get edge definition by type."""
    for edge in EDGE_DEFINITIONS:
        if edge.type == edge_type:
            return edge
    return None


def generate_cypher_create_edge(
    edge_type: EdgeType,
    from_id: str,
    to_id: str,
    properties: Optional[Dict[str, Any]] = None
) -> str:
    """
    Generate Cypher to create an edge.
    
    Example:
        MATCH (a:Campaign {id: $from_id}), (b:Metric {id: $to_id})
        CREATE (a)-[:HAS_PERFORMANCE {date: $date}]->(b)
    """
    edge_def = get_edge_definition(edge_type)
    if not edge_def:
        raise ValueError(f"Unknown edge type: {edge_type}")
    
    props_str = ""
    if properties:
        props_parts = [f"{k}: ${k}" for k in properties.keys()]
        props_str = " {" + ", ".join(props_parts) + "}"
    
    return f"""
    MATCH (a:{edge_def.from_label} {{id: $from_id}}), (b:{edge_def.to_label} {{id: $to_id}})
    CREATE (a)-[:{edge_type.value}{props_str}]->(b)
    """
