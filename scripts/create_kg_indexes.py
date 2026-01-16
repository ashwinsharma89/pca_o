
from src.kg_rag.client.connection import get_neo4j_connection
from loguru import logger

def create_indexes():
    conn = get_neo4j_connection()
    
    constraints = [
        "CREATE CONSTRAINT campaign_id IF NOT EXISTS FOR (c:Campaign) REQUIRE c.id IS UNIQUE",
        "CREATE CONSTRAINT metric_id IF NOT EXISTS FOR (m:Metric) REQUIRE m.id IS UNIQUE",
        "CREATE CONSTRAINT platform_id IF NOT EXISTS FOR (p:Platform) REQUIRE p.id IS UNIQUE",
        # Targeting uses campaign_id as key
        "CREATE CONSTRAINT targeting_id IF NOT EXISTS FOR (t:Targeting) REQUIRE t.campaign_id IS UNIQUE",
        "CREATE CONSTRAINT entitygroup_id IF NOT EXISTS FOR (eg:EntityGroup) REQUIRE eg.id IS UNIQUE",
        "CREATE CONSTRAINT placement_id IF NOT EXISTS FOR (p:Placement) REQUIRE p.id IS UNIQUE",
        "CREATE CONSTRAINT keyword_id IF NOT EXISTS FOR (k:Keyword) REQUIRE k.id IS UNIQUE"
    ]
    
    for c in constraints:
        try:
            logger.info(f"Executing: {c}")
            conn.execute_query(c)
            logger.info("✅ Success")
        except Exception as e:
            logger.error(f"❌ Failed: {e}")

if __name__ == "__main__":
    create_indexes()
