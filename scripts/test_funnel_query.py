import os
import pandas as pd
from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine
from src.core.database.duckdb_repository import CAMPAIGNS_PARQUET

def test_funnel():
    # Set dummy key if not present
    if not os.getenv("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = "dummy"
    
    engine = NaturalLanguageQueryEngine(api_key=os.getenv("OPENAI_API_KEY"))
    
    if not CAMPAIGNS_PARQUET.exists():
        print(f"Error: {CAMPAIGNS_PARQUET} does not exist")
        return

    print(f"Loading data from {CAMPAIGNS_PARQUET}...")
    engine.load_parquet_data(str(CAMPAIGNS_PARQUET), table_name="all_campaigns")
    
    question = "show funnel analysis"
    print(f"Asking: {question}")
    
    try:
        result = engine.ask(question)
        print("\n=== Result ===")
        print(f"Success: {result.get('success')}")
        print(f"Error: {result.get('error')}")
        print(f"SQL: {result.get('sql_query')}")
        
        if result.get('success'):
            df = result.get('results')
            if isinstance(df, pd.DataFrame):
                print(f"Rows: {len(df)}")
                print(df.head())
            else:
                print(f"Data type: {type(df)}")
                print(df)
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    test_funnel()
