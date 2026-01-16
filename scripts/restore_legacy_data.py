
import pandas as pd
import sys
import os
import logging
import numpy as np

# Add project root to path
# os.getcwd() is not needed here if we append os.getcwd()
sys.path.append(os.getcwd())

from src.core.database.duckdb_manager import get_duckdb_manager
from src.core.utils import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

def restore_legacy_data():
    legacy_path = 'data/campaigns.parquet'
    if not os.path.exists(legacy_path):
        print(f"❌ Legacy file {legacy_path} not found.")
        return

    print(f"Reading {legacy_path}...")
    try:
        df = pd.read_parquet(legacy_path)
        print(f"Loaded {len(df)} rows.")
        
        # Column Mapping
        # New Schema: Date, Platform, Campaign, Spend, Impressions, Clicks, Conversions, Revenue
        
        # 1. Spend
        if 'Total Spent' in df.columns:
            df['Spend'] = df['Total Spent']
        elif 'Spend_USD' in df.columns:
             df['Spend'] = df['Spend_USD']
             
        # 2. Campaign
        if 'Campaign_Name_Full' in df.columns:
            df['Campaign'] = df['Campaign_Name_Full']
            
        # 3. Revenue
        # Combine Revenue_2024 and Revenue_2025 if present
        df['Revenue'] = 0.0
        if 'Revenue_2024' in df.columns:
            df['Revenue'] += df['Revenue_2024'].fillna(0)
        if 'Revenue_2025' in df.columns:
            df['Revenue'] += df['Revenue_2025'].fillna(0)
            
        # 4. Conversions
        # Map 'Site Visit' to Conversions if Conversions missing
        if 'Conversions' not in df.columns:
            if 'Site Visit' in df.columns:
                print("⚠️ Mapping 'Site Visit' to 'Conversions' (Legacy Schema Adaptation)")
                df['Conversions'] = df['Site Visit']
            else:
                df['Conversions'] = 0
                
        # 5. Ensure Date is datetime
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            
        # Drop legacy columns to clean up? Or keep them? 
        # DuckDBManager saves everything passed in df.
        # We'll keep them for detailed analysis.
        
        print(f"Saving {len(df)} rows to partitioned storage...")
        duckdb_mgr = get_duckdb_manager()
        duckdb_mgr.save_campaigns(df)
        
        print("✅ Restore Complete!")
        
        # Verify
        new_df = duckdb_mgr.get_campaigns()
        print(f"New Total Rows in DuckDB: {len(new_df)}")
        
    except Exception as e:
        print(f"❌ Error restoring data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    restore_legacy_data()
