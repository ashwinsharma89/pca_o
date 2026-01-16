
import asyncio
import sys
import os
import logging
from pprint import pprint

# Add project root to path
sys.path.append(os.getcwd())

from src.core.database.duckdb_manager import get_duckdb_manager
from src.core.utils import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

def verify_data():
    print("--- Verifying DuckDB Data ---")
    try:
        duckdb_mgr = get_duckdb_manager()
        df = duckdb_mgr.get_campaigns()
        print(f"Total Rows in DuckDB: {len(df)}")
        if not df.empty:
            print("Columns:", df.columns.tolist())
            print("Sample Data (first 1 row):")
            print(df.head(1).to_dict(orient='records'))
            
            # Check date range
            if 'date' in df.columns:
                 print(f"Date Range: {df['date'].min()} to {df['date'].max()}")
            elif 'Date' in df.columns:
                 print(f"Date Range: {df['Date'].min()} to {df['Date'].max()}")
        else:
            print("⚠️ DuckDB is EMPTY!")
            
    except Exception as e:
        print(f"❌ Error querying DuckDB: {e}")

if __name__ == "__main__":
    verify_data()
