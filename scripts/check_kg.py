
import sys
import os
import logging
sys.path.append(os.getcwd())

from src.kg_rag.client.connection import get_neo4j_connection
from src.core.utils.logger import setup_logger

logger = setup_logger("check_kg")

def main():
    try:
        conn = get_neo4j_connection()
        logger.info("Connected to Neo4j")
        
        # Count nodes
        result = conn.execute_query("MATCH (n) RETURN count(n) as count")
        count = result[0]['count']
        logger.info(f"Total Nodes: {count}")
        
        # Count by label
        result = conn.execute_query("MATCH (n) RETURN labels(n) as labels, count(n) as count")
        for row in result:
            logger.info(f"Labels: {row['labels']}, Count: {row['count']}")
            
        # Count relationships
        result = conn.execute_query("MATCH ()-[r]->() RETURN type(r) as type, count(r) as count")
        for row in result:
            logger.info(f"Rel Type: {row['type']}, Count: {row['count']}")
            
        # Check specific connectivity
        result = conn.execute_query("MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric) RETURN count(*) as count")
        logger.info(f"Campaign-Metric Links (HAS_PERFORMANCE): {result[0]['count']}")
            
        conn.close()
        
    except Exception as e:
        logger.error(f"Check Failed: {e}")

if __name__ == "__main__":
    main()
