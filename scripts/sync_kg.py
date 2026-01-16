import sys
import os
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())

from src.core.database.duckdb_manager import DuckDBManager
from src.kg_rag.etl.ingestion import ingest_dataframe
from loguru import logger

def main():
    logger.info("Starting manual KG sync...")
    
    # 1. Fetch data from DuckDB
    logger.info("Fetching all campaigns from DuckDB...")
    try:
        db_mgr = DuckDBManager()
        
        # Get all campaigns (no limit)
        # We ensure we get the dataframe
        df = db_mgr.get_campaigns(limit=None)
        
        if df.empty:
            logger.error("No data found in DuckDB! Cannot sync.")
            # Check if parquet exists
            parquet_path = Path("data/campaigns.parquet")
            if parquet_path.exists():
                import pandas as pd
                logger.info(f"Fallback: Reading directly from {parquet_path}")
                df = pd.read_parquet(parquet_path)
            else:
                return
            
        logger.info(f"Retrieved {len(df)} rows from Source.")
        
        # 2. Clear existing data (Optional but safer for full re-sync)
        logger.info("Clearing existing Knowledge Graph data (Batched)...")
        from src.kg_rag.client.connection import get_neo4j_connection
        conn = get_neo4j_connection()
        
        # Use APOC for batched deletion to avoid OOM
        try:
            conn.execute_query("""
                CALL apoc.periodic.iterate(
                    "MATCH (n) RETURN n",
                    "DETACH DELETE n",
                    {batchSize: 10000}
                )
            """)
        except Exception as delete_err:
             logger.warning(f"APOC delete failed ({delete_err}), falling back to simple delete loop...")
             # Fallback if APOC not installed
             conn.execute_query("MATCH (n) LIMIT 10000 DETACH DELETE n")  
        
        logger.info("Knowledge Graph cleared.")
        
        # 3. Ingest into Neo4j
        logger.info("Ingesting into Knowledge Graph...")
        summary = ingest_dataframe(df)
        
        logger.info("✅ Sync Complete!")
        logger.info(f"Summary: {summary}")
        
    except Exception as e:
        logger.error(f"❌ Sync failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
