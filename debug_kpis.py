
import polars as pl
from src.core.database.duckdb_manager import get_duckdb_manager

def debug_analysis_kpis():
    print("--- Debugging Analysis KPIs ---")
    mgr = get_duckdb_manager()
    
    # 1. Check Polars (Analysis path)
    print("Fetching campaigns via Polars...")
    try:
        df_pl = mgr.get_campaigns_polars()
        print(f"Polars Row Count: {df_pl.height}")
        print("Polars Columns:", df_pl.columns)
        
        # Check specific columns expected by analytics.py
        expected_cols = ["Spend", "Impressions", "Clicks", "Conversions"]
        for col in expected_cols:
            if col in df_pl.columns:
                val = df_pl[col].sum()
                print(f"Sum of '{col}': {val}")
            else:
                print(f"WARNING: Column '{col}' NOT found in Polars DF!")
                
        # Check for lowercase versions
        lower_cols = ["spend", "impressions", "clicks", "conversions"]
        for col in lower_cols:
            if col in df_pl.columns:
                val = df_pl[col].sum()
                print(f"Sum of lowercase '{col}': {val}")
                
    except Exception as e:
        print(f"Polars fetch failed: {e}")

    # 2. Check Pandas (Vis-2 path)
    print("\n--- Debugging Vis-2 KPIs ---")
    try:
        df_pd = mgr.get_campaigns()
        print(f"Pandas Row Count: {len(df_pd)}")
        print("Pandas Columns:", df_pd.columns.tolist())
        
        # Vis-2 converts to Polars locally
        pl_from_pd = pl.from_pandas(df_pd)
        print("Converted Pandas to Polars locally, summing:")
        
        # Vis-2 uses get_summary logic which looks for 'spend', 'Spend', etc. via find_column logic mapping
        # Let's see what plain sum gives
        from src.core.utils.column_mapping import find_column
        
        target_cols = ['spend', 'impressions', 'clicks', 'conversions']
        for target in target_cols:
            actual = find_column(df_pd, target)
            if actual:
                val = df_pd[actual].sum()
                print(f"Pandas Sum of '{actual}' (mapped from {target}): {val}")
            else:
                print(f"WARNING: Could not find column for '{target}' in Pandas DF")
                
    except Exception as e:
        print(f"Pandas fetch failed: {e}")

if __name__ == "__main__":
    import logging
    # Suppress loud logs
    logging.getLogger('src.core.database.duckdb_manager').setLevel(logging.WARNING)
    debug_analysis_kpis()
