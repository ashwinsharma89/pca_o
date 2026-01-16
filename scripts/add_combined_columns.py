#!/usr/bin/env python3
"""
Script to add combined Revenue and Reach columns to campaigns data.
Combines Revenue_2024 + Revenue_2025 into Revenue column.
Combines Reach_2024 + Reach_2025 into Reach column.
"""

import pandas as pd
import sys

def add_combined_columns():
    """Add combined Revenue and Reach columns to the parquet file."""
    
    parquet_file = 'data/campaigns.parquet'
    
    try:
        # Read the existing parquet file
        print(f"📖 Reading {parquet_file}...")
        df = pd.read_parquet(parquet_file)
        print(f"   Loaded {len(df)} rows with {len(df.columns)} columns")
        
        # Check if year-specific columns exist
        has_revenue_2024 = 'Revenue_2024' in df.columns
        has_revenue_2025 = 'Revenue_2025' in df.columns
        has_reach_2024 = 'Reach_2024' in df.columns
        has_reach_2025 = 'Reach_2025' in df.columns
        
        print(f"\n📊 Column availability:")
        print(f"   Revenue_2024: {has_revenue_2024}")
        print(f"   Revenue_2025: {has_revenue_2025}")
        print(f"   Reach_2024: {has_reach_2024}")
        print(f"   Reach_2025: {has_reach_2025}")
        
        # Add combined Revenue column
        if has_revenue_2024 or has_revenue_2025:
            revenue_2024 = df['Revenue_2024'].fillna(0) if has_revenue_2024 else 0
            revenue_2025 = df['Revenue_2025'].fillna(0) if has_revenue_2025 else 0
            df['Revenue'] = revenue_2024 + revenue_2025
            
            total_revenue = df['Revenue'].sum()
            print(f"\n✅ Added 'Revenue' column")
            print(f"   Total Revenue: ${total_revenue:,.2f}")
            print(f"   - From 2024: ${df['Revenue_2024'].sum() if has_revenue_2024 else 0:,.2f}")
            print(f"   - From 2025: ${df['Revenue_2025'].sum() if has_revenue_2025 else 0:,.2f}")
        else:
            print(f"\n⚠️  No Revenue columns found to combine")
        
        # Add combined Reach column
        if has_reach_2024 or has_reach_2025:
            reach_2024 = df['Reach_2024'].fillna(0) if has_reach_2024 else 0
            reach_2025 = df['Reach_2025'].fillna(0) if has_reach_2025 else 0
            df['Reach'] = reach_2024 + reach_2025
            
            total_reach = df['Reach'].sum()
            print(f"\n✅ Added 'Reach' column")
            print(f"   Total Reach: {total_reach:,.0f}")
            print(f"   - From 2024: {df['Reach_2024'].sum() if has_reach_2024 else 0:,.0f}")
            print(f"   - From 2025: {df['Reach_2025'].sum() if has_reach_2025 else 0:,.0f}")
        else:
            print(f"\n⚠️  No Reach columns found to combine")
        
        # Save back to parquet
        print(f"\n💾 Saving updated data to {parquet_file}...")
        df.to_parquet(parquet_file, index=False, engine='pyarrow')
        
        print(f"\n✅ SUCCESS! Updated parquet file with combined columns")
        print(f"   Total columns now: {len(df.columns)}")
        print(f"   New columns added: Revenue, Reach")
        
        # Show sample of new columns
        print(f"\n📋 Sample data (first 3 rows):")
        sample_cols = ['Date', 'Platform', 'Revenue', 'Reach'] if 'Revenue' in df.columns and 'Reach' in df.columns else df.columns[:5]
        print(df[sample_cols].head(3).to_string(index=False))
        
        return True
        
    except FileNotFoundError:
        print(f"❌ ERROR: File not found: {parquet_file}")
        print(f"   Please ensure the campaigns data has been uploaded first.")
        return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = add_combined_columns()
    sys.exit(0 if success else 1)
