#!/usr/bin/env python3
"""
Initialize Neo4j Schema for KG-RAG

This script:
1. Creates all constraints
2. Creates all indexes
3. Seeds Channel and Platform nodes

Usage:
    python scripts/kg_rag/init_neo4j.py [--reset]
"""

import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.kg_rag.client.connection import get_neo4j_connection
from src.kg_rag.schema.constraints import (
    generate_constraint_cypher,
    generate_node_key_cypher,
    generate_drop_constraint_cypher,
)
from src.kg_rag.schema.indexes import (
    generate_index_cypher,
    generate_relationship_index_cypher,
    generate_drop_index_cypher,
)
from src.kg_rag.schema.nodes import ChannelNode, PlatformNode


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def reset_schema(neo4j) -> None:
    """Drop all constraints and indexes."""
    logger.warning("Resetting schema - dropping all constraints and indexes...")
    
    # Drop constraints
    for cypher in generate_drop_constraint_cypher():
        try:
            neo4j.execute_write(cypher)
            logger.debug(f"Dropped: {cypher[:50]}...")
        except Exception as e:
            logger.debug(f"Skip drop (may not exist): {e}")
    
    # Drop indexes
    for cypher in generate_drop_index_cypher():
        try:
            neo4j.execute_write(cypher)
            logger.debug(f"Dropped: {cypher[:50]}...")
        except Exception as e:
            logger.debug(f"Skip drop (may not exist): {e}")
    
    logger.info("Schema reset complete")


def create_constraints(neo4j) -> None:
    """Create all unique and node key constraints."""
    logger.info("Creating constraints...")
    
    # Unique constraints
    for cypher in generate_constraint_cypher():
        try:
            neo4j.execute_write(cypher)
            logger.info(f"Created constraint: {cypher.split('FOR')[0].strip()}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.debug(f"Constraint already exists")
            else:
                logger.error(f"Failed to create constraint: {e}")
    
    # Node key constraints
    for cypher in generate_node_key_cypher():
        try:
            neo4j.execute_write(cypher)
            logger.info(f"Created node key: {cypher.split('FOR')[0].strip()}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.debug(f"Node key already exists")
            else:
                logger.warning(f"Node key constraint may require Enterprise Edition: {e}")


def create_indexes(neo4j) -> None:
    """Create all indexes."""
    logger.info("Creating indexes...")
    
    # Property indexes
    for cypher in generate_index_cypher():
        try:
            neo4j.execute_write(cypher)
            logger.info(f"Created index: {cypher.split('FOR')[0].strip()}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.debug(f"Index already exists")
            else:
                logger.error(f"Failed to create index: {e}")
    
    # Relationship indexes
    for cypher in generate_relationship_index_cypher():
        try:
            neo4j.execute_write(cypher)
            logger.info(f"Created relationship index")
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.debug(f"Relationship index already exists")
            else:
                logger.warning(f"Relationship index may require Enterprise Edition: {e}")


def seed_channels(neo4j) -> None:
    """Seed the 4 channel nodes."""
    logger.info("Seeding Channel nodes...")
    
    query = """
    UNWIND $batch AS row
    MERGE (c:Channel {id: row.id})
    SET c.name = row.name,
        c.description = row.description
    RETURN count(c) AS count
    """
    
    result = neo4j.execute_batch(query, ChannelNode.SEED_DATA)
    logger.info(f"Seeded {result.get('nodes_created', 0)} Channel nodes")


def seed_platforms(neo4j) -> None:
    """Seed the 20+ platform nodes."""
    logger.info("Seeding Platform nodes...")
    
    query = """
    UNWIND $batch AS row
    MERGE (p:Platform {id: row.id})
    SET p.name = row.name,
        p.channel_id = row.channel_id,
        p.api_source = row.api_source,
        p.parent_company = row.parent_company,
        p.supports_keywords = coalesce(row.supports_keywords, false),
        p.supports_placements = coalesce(row.supports_placements, false),
        p.supports_video_metrics = coalesce(row.supports_video_metrics, false),
        p.supports_reach = coalesce(row.supports_reach, false),
        p.supports_revenue = coalesce(row.supports_revenue, false),
        p.supports_b2b_targeting = coalesce(row.supports_b2b_targeting, false)
    RETURN count(p) AS count
    """
    
    result = neo4j.execute_batch(query, PlatformNode.SEED_DATA)
    logger.info(f"Seeded {result.get('nodes_created', 0)} Platform nodes")


def create_channel_platform_relationships(neo4j) -> None:
    """Create CATEGORIZES relationships between Channels and Platforms."""
    logger.info("Creating Channel -> Platform relationships...")
    
    query = """
    MATCH (c:Channel), (p:Platform)
    WHERE c.id = p.channel_id
    MERGE (c)-[:CATEGORIZES]->(p)
    RETURN count(*) AS count
    """
    
    result = neo4j.execute_write(query)
    logger.info(f"Created {result.get('relationships_created', 0)} CATEGORIZES relationships")


def verify_schema(neo4j) -> None:
    """Verify schema was created correctly."""
    logger.info("Verifying schema...")
    
    schema_info = neo4j.get_schema_info()
    
    print("\n" + "=" * 50)
    print("SCHEMA VERIFICATION")
    print("=" * 50)
    print(f"Labels: {', '.join(schema_info['labels'])}")
    print(f"Relationship Types: {', '.join(schema_info['relationship_types'])}")
    print(f"Indexes: {schema_info['index_count']}")
    print(f"Constraints: {schema_info['constraint_count']}")
    
    # Count nodes
    counts = neo4j.execute_query("""
        MATCH (c:Channel) WITH count(c) AS channels
        MATCH (p:Platform) WITH channels, count(p) AS platforms
        RETURN channels, platforms
    """)
    
    if counts:
        print(f"Channels: {counts[0]['channels']}")
        print(f"Platforms: {counts[0]['platforms']}")
    print("=" * 50 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Initialize Neo4j schema for KG-RAG")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset schema (drop and recreate)"
    )
    args = parser.parse_args()
    
    logger.info("Starting Neo4j schema initialization...")
    
    try:
        neo4j = get_neo4j_connection()
        
        # Health check
        health = neo4j.health_check()
        if not health["connected"]:
            logger.error(f"Cannot connect to Neo4j: {health.get('error')}")
            sys.exit(1)
        
        logger.info(f"Connected to Neo4j at {health['uri']}")
        
        # Reset if requested
        if args.reset:
            reset_schema(neo4j)
        
        # Create schema
        create_constraints(neo4j)
        create_indexes(neo4j)
        
        # Seed data
        seed_channels(neo4j)
        seed_platforms(neo4j)
        create_channel_platform_relationships(neo4j)
        
        # Verify
        verify_schema(neo4j)
        
        logger.info("Schema initialization complete!")
        
    except Exception as e:
        logger.error(f"Schema initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
