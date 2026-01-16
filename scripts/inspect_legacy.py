
import pandas as pd
import sys

try:
    df = pd.read_parquet('data/campaigns.parquet')
    print(f"Legacy File Rows: {len(df)}")
    print("Columns:", df.columns.tolist())
    print("Sample Data:")
    print(df.head(1).to_dict(orient='records'))
    
    if 'Spend_USD' in df.columns:
        print(f"Total Spend: {df['Spend_USD'].sum()}")
    elif 'Spend' in df.columns:
        print(f"Total Spend: {df['Spend'].sum()}")
        
except Exception as e:
    print(f"Error reading legacy file: {e}")
