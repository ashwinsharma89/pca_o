"""Create sample parquet file from existing campaigns.parquet"""
import pandas as pd
from pathlib import Path

# Read existing campaigns data
campaigns_file = Path("data/campaigns.parquet")
if campaigns_file.exists():
    df = pd.read_parquet(campaigns_file)
    
    # Take first 50 rows as sample
    sample_df = df.head(50)
    
    # Save to samples directory
    output_file = Path("data/samples/parquet/campaigns_sample.parquet")
    sample_df.to_parquet(output_file, index=False, compression='snappy')
    
    print(f"✅ Created {output_file} with {len(sample_df)} rows")
    print(f"   Columns: {', '.join(sample_df.columns[:5])}...")
else:
    print("❌ data/campaigns.parquet not found")
