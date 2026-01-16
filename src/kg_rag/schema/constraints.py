"""
KG-RAG Schema Constraints

Defines unique constraints for Neo4j nodes to ensure data integrity.
"""

from typing import List, Tuple


# Unique constraints: (Label, property)
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


def generate_constraint_cypher() -> List[str]:
    """
    Generate Cypher statements to create all constraints.
    
    Returns:
        List of Cypher CREATE CONSTRAINT statements
    """
    statements = []
    for label, prop in UNIQUE_CONSTRAINTS:
        constraint_name = f"constraint_{label.lower()}_{prop}"
        cypher = f"""
CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
FOR (n:{label})
REQUIRE n.{prop} IS UNIQUE
        """.strip()
        statements.append(cypher)
    return statements


def generate_drop_constraint_cypher() -> List[str]:
    """Generate Cypher to drop all constraints."""
    statements = []
    for label, prop in UNIQUE_CONSTRAINTS:
        constraint_name = f"constraint_{label.lower()}_{prop}"
        cypher = f"DROP CONSTRAINT {constraint_name} IF EXISTS"
        statements.append(cypher)
    return statements


# Node key constraints (composite)
NODE_KEY_CONSTRAINTS = [
    # Metric is unique by campaign_id + date
    ("Metric", ["campaign_id", "date"]),
]


def generate_node_key_cypher() -> List[str]:
    """Generate node key constraints."""
    statements = []
    for label, props in NODE_KEY_CONSTRAINTS:
        props_str = ", ".join([f"n.{p}" for p in props])
        constraint_name = f"nodekey_{label.lower()}_{'_'.join(props)}"
        cypher = f"""
CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
FOR (n:{label})
REQUIRE ({props_str}) IS NODE KEY
        """.strip()
        statements.append(cypher)
    return statements


# Existence constraints (required properties)
EXISTENCE_CONSTRAINTS = [
    ("Campaign", "name"),
    ("Campaign", "platform_id"),
    ("Metric", "campaign_id"),
    ("Metric", "date"),
]


def generate_existence_cypher() -> List[str]:
    """Generate existence constraints (Enterprise Edition only)."""
    statements = []
    for label, prop in EXISTENCE_CONSTRAINTS:
        constraint_name = f"exists_{label.lower()}_{prop}"
        cypher = f"""
CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
FOR (n:{label})
REQUIRE n.{prop} IS NOT NULL
        """.strip()
        statements.append(cypher)
    return statements
