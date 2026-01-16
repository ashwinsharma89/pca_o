
import pandas as pd
import polars as pl
import sys
import os

# Add src to path
sys.path.append(os.getcwd())

from src.engine.analytics.data_quality import generate_data_quality_report, DataQualityAnalyzer

def verify_data_quality():
    print("Verifying DataQualityAnalyzer Refactor...")

    # 1. Create Dummy Data
    data = {
        "Campaign": ["C1", "C2", "C1"],
        "Platform": ["Google", "Facebook", "Google"],
        "Spend": [100.0, 200.0, 150.0],
        "Impressions": [1000, 2000, 1500],
        "Clicks": [10, 20, 15],
        "Conversions": [1, 2, 1],
        "ROAS": [2.5, 3.0, 2.0]
    }

    # 2. Test with Pandas
    print("\n--- Testing with Pandas DataFrame ---")
    df_pd = pd.DataFrame(data)
    try:
        report_pd = generate_data_quality_report(df_pd, context={"campaign_name": "Pandas Test"})
        if "Overall Completeness" in report_pd and "100/100" in report_pd:
             print("✅ Pandas DataFrame processed successfully.")
        else:
             print("❌ Pandas DataFrame output unexpected.")
             print(report_pd)
    except Exception as e:
        print(f"❌ Pandas DataFrame failed: {e}")

    # 3. Test with Polars
    print("\n--- Testing with Polars DataFrame ---")
    df_pl = pl.DataFrame(data)
    try:
        # Note: function logic converts to Polars internally now if it detects keys/columns logic match
        # Wait, my refactor logic was:
        # if hasattr(df, 'to_pandas'): pass (keeps it as is if it has to_pandas? wait, Polars HAS to_pandas)
        # my logic in data_quality.py:
        # if hasattr(df, 'to_pandas'): pass 
        # elif hasattr(df, 'columns'): df = pl.from_pandas(df)
        
        # If it is Polars, it has `to_pandas`, so it passes.
        # Then `columns = df.columns` works for Polars.
        # `df['Platform']` works for Polars.
        # `unique()` works for Polars.
        # `to_list()` works for Polars.
        # `df.height` works for Polars (but NOT Pandas! Pandas uses len())
        
        # Let's check the code I wrote for `sample_size = df.height`.
        # If I passed a Pandas df, `hasattr(df, 'to_pandas')` is True. So it passes.
        # Then `sample_size = df.height` -> Pandas DataFrame DOES NOT have `.height`. It has `.shape[0]` or `len()`.
        # BAD LOGIC detected in thought process. Polars has `.height`. Pandas does not.
        # If I passed Pandas, I need to convert it to Polars OR use compatible property.
        
        # Let's run this script to CONFIRM it fails for Pandas, which validates the "verify" request.
        
        report_pl = generate_data_quality_report(df_pl, context={"campaign_name": "Polars Test"})
        if "Overall Completeness" in report_pl:
             print("✅ Polars DataFrame processed successfully.")
        else:
             print("❌ Polars DataFrame output unexpected.")
    except Exception as e:
        print(f"❌ Polars DataFrame failed: {e}")

if __name__ == "__main__":
    verify_data_quality()
