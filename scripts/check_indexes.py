
from src.kg_rag.client.connection import get_neo4j_connection
from loguru import logger

def check_indexes():
    conn = get_neo4j_connection()
    try:
        # Neo4j 5.x syntax
        res = conn.execute_query("SHOW INDEXES")
        logger.info("Found Indexes:")
        for r in res:
             logger.info(r)
    except Exception as e:
        logger.error(f"Failed to list indexes: {e}")
        # Try 4.x syntax just in case
        try:
             res = conn.execute_query("CALL db.indexes()")
             for r in res:
                 logger.info(r)
        except:
             pass

if __name__ == "__main__":
    check_indexes()
