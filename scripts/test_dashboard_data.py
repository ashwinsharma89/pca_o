import asyncio
import pandas as pd
from src.database.duckdb_manager import get_duckdb_manager
from datetime import datetime, timedelta

async def test_dashboard_stats():
    dm = get_duckdb_manager()
    print(f"Has data: {dm.has_data()}")
    
    df = dm.get_campaigns()
    print(f"Total rows: {len(df)}")
    
    if not df.empty:
        print(f"Columns: {df.columns.tolist()}")
        print(f"First 5 rows:\n{df.head()}")
        
        # Test date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=90) # Last 3 months
        
        d_col = None
        for c in ['Date', 'date']:
            if c in df.columns:
                d_col = c
                break
        
        if d_col:
            df[d_col] = pd.to_datetime(df[d_col])
            mask = (df[d_col] >= start_date) & (df[d_col] <= end_date)
            period_df = df[mask]
            print(f"Rows in last 90 days: {len(period_df)}")
        else:
            print("No date column found!")

if __name__ == "__main__":
    asyncio.run(test_dashboard_stats())
