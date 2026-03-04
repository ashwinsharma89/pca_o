"""
KG-RAG Relationship Type Definitions

Defines relationship types for the Knowledge Graph.
KùzuDB DDL for relationship table creation is provided in KUZU_REL_DDL.
"""

from typing import List, Tuple
from enum import Enum


class EdgeType(Enum):
    """Relationship types for the Knowledge Graph."""

    CATEGORIZES = "CATEGORIZES"
    HOSTS = "HOSTS"
    OWNS = "OWNS"
    BELONGS_TO = "BELONGS_TO"
    HAS_TARGETING = "HAS_TARGETING"
    HAS_PERFORMANCE = "HAS_PERFORMANCE"
    CONTAINS = "CONTAINS"
    HAS_CREATIVE = "HAS_CREATIVE"
    HAS_KEYWORD = "HAS_KEYWORD"
    HAS_PLACEMENT = "HAS_PLACEMENT"
    OVERLAPS_WITH = "OVERLAPS_WITH"
    SIMILAR_TO = "SIMILAR_TO"


EDGE_DEFINITIONS: List[Tuple[str, str, str]] = [
    ("CATEGORIZES", "Channel", "Platform"),
    ("HOSTS", "Platform", "Account"),
    ("OWNS", "Account", "Campaign"),
    ("BELONGS_TO", "Campaign", "Platform"),
    ("HAS_TARGETING", "Campaign", "Targeting"),
    ("HAS_PERFORMANCE", "Campaign", "Metric"),
    ("CONTAINS", "Campaign", "EntityGroup"),
    ("HAS_CREATIVE", "EntityGroup", "Creative"),
    ("HAS_KEYWORD", "EntityGroup", "Keyword"),
    ("HAS_PLACEMENT", "EntityGroup", "Placement"),
    ("OVERLAPS_WITH", "Audience", "Audience"),
    ("SIMILAR_TO", "Creative", "Creative"),
]


KUZU_REL_DDL = [
    "CREATE REL TABLE IF NOT EXISTS CATEGORIZES(FROM Channel TO Platform)",
    "CREATE REL TABLE IF NOT EXISTS HOSTS(FROM Platform TO Account)",
    "CREATE REL TABLE IF NOT EXISTS OWNS(FROM Account TO Campaign)",
    "CREATE REL TABLE IF NOT EXISTS BELONGS_TO(FROM Campaign TO Platform)",
    "CREATE REL TABLE IF NOT EXISTS HAS_TARGETING(FROM Campaign TO Targeting)",
    "CREATE REL TABLE IF NOT EXISTS HAS_PERFORMANCE(FROM Campaign TO Metric)",
    "CREATE REL TABLE IF NOT EXISTS CONTAINS(FROM Campaign TO EntityGroup, position INT64)",
    "CREATE REL TABLE IF NOT EXISTS HAS_CREATIVE(FROM EntityGroup TO Creative)",
    "CREATE REL TABLE IF NOT EXISTS HAS_KEYWORD(FROM EntityGroup TO Keyword)",
    "CREATE REL TABLE IF NOT EXISTS HAS_PLACEMENT(FROM EntityGroup TO Placement)",
    "CREATE REL TABLE IF NOT EXISTS OVERLAPS_WITH(FROM Audience TO Audience, overlap_pct DOUBLE)",
    "CREATE REL TABLE IF NOT EXISTS SIMILAR_TO(FROM Creative TO Creative, score DOUBLE)",
]
