
import asyncio
import sys
import os
import logging
import pandas as pd

# Add project root to path
sys.path.append(os.getcwd())

from src.core.database.duckdb_manager import get_duckdb_manager
from src.kg_rag.etl.ingestion import ingest_dataframe
from src.core.utils.logger import setup_logger

logger = setup_logger("populate_kg")

async def main():
    try:
        logger.info("Starting Knowledge Graph population...")
        
        # 1. Get Data from DuckDB
        db_manager = get_duckdb_manager()
        logger.info("Fetching campaign data from DuckDB...")
        
        # Fetch all campaigns
        df = db_manager.get_campaigns(limit=100000)
        
        if df.empty:
            logger.error("No data found in DuckDB. Cannot populate KG.")
            return

        logger.info(f"Retrieved {len(df)} records from DuckDB")
        logger.info(f"Sample Record:\n{df.iloc[0].to_dict()}")
        
        # 2. Ingest into Neo4j
        logger.info("Ingesting into Neo4j...")
        summary = ingest_dataframe(df) # Full Ingestion
        
        logger.info("KG Population Complete!")
        logger.info(f"Summary: {summary}")
        
    except Exception as e:
        logger.error(f"KG Population Failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
