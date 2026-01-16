
import sys
import os
import logging
import pandas as pd

# Add project root to path
sys.path.append(os.getcwd())

from src.core.database.duckdb_manager import get_duckdb_manager
from src.core.utils import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

def verify_aggregates():
    print("--- Verifying Data Aggregates Internal ---")
    try:
        duckdb_mgr = get_duckdb_manager()
        df = duckdb_mgr.get_campaigns()
        
        print(f"Total Rows: {len(df)}")
        
        if not df.empty:
            print(f"Total Spend: ${df['Spend'].sum():,.2f}")
            print(f"Total Impressions: {df['Impressions'].sum():,.0f}")
            print(f"Total Clicks: {df['Clicks'].sum():,.0f}")
            if 'Revenue' in df.columns:
                print(f"Total Revenue: ${df['Revenue'].sum():,.2f}")
            
            # Check for 0s
            if df['Spend'].sum() == 0:
                 print("⚠️ CRITICAL: Spend is 0. Column mapping failed?")
            else:
                 print("✅ Spend data looks valid.")
        else:
            print("⚠️ No data returned!")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    verify_aggregates()
