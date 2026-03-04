"""
KG-RAG Schema — KùzuDB Note

In KùzuDB, uniqueness constraints are defined inline in CREATE NODE TABLE DDL
via the PRIMARY KEY clause.  There are no separate constraint statements.

The UNIQUE_CONSTRAINTS and NODE_KEY_CONSTRAINTS lists are kept here for
documentation purposes only; the actual enforcement happens in nodes.py DDL.
"""

from typing import List, Tuple


# Documented for reference — enforced via PRIMARY KEY in KUZU_NODE_DDL
UNIQUE_CONSTRAINTS: List[Tuple[str, str]] = [
    ("Channel", "id"),
    ("Platform", "id"),
    ("Account", "id"),
    ("Campaign", "id"),
    ("Targeting", "campaign_id"),
    ("Metric", "id"),
    ("EntityGroup", "id"),
    ("Creative", "id"),
    ("Keyword", "id"),
    ("Placement", "id"),
    ("Audience", "id"),
]

NODE_KEY_CONSTRAINTS = [
    ("Metric", ["campaign_id", "date"]),
]


def generate_constraint_cypher() -> List[str]:
    """No-op: constraints are handled by PRIMARY KEY in CREATE NODE TABLE DDL."""
    return []


def generate_drop_constraint_cypher() -> List[str]:
    """No-op: drop the whole node table to remove its constraints."""
    return []


def generate_node_key_cypher() -> List[str]:
    """No-op: KùzuDB PRIMARY KEY already enforces uniqueness."""
    return []


def generate_existence_cypher() -> List[str]:
    """No-op: existence is enforced at the application layer."""
    return []
