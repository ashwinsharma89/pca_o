
import pandas as pd
import logging
from src.core.database.duckdb_manager import get_duckdb_manager
from src.kg_rag.client.connection import get_neo4j_connection

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_duckdb_totals():
    print("\n--- Checking DuckDB Totals ---")
    try:
        mgr = get_duckdb_manager()
        df = mgr.get_campaigns()
        
        if df.empty:
            print("DuckDB is EMPTY.")
            return

        print(f"Total Rows: {len(df)}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Check breakdown by Platform
        if 'Platform' in df.columns:
            print("\nRow Counts by Platform:")
            print(df['Platform'].value_counts())
        elif 'platform' in df.columns:
            print("\nRow Counts by Platform:")
            print(df['platform'].value_counts())
        
        if 'Date' in df.columns:
            print(f"\nDate Range: {df['Date'].min()} to {df['Date'].max()}")
            print("Null Dates: ", df['Date'].isna().sum())
            print("\nSample Rows:")
            print(df[['Date', 'Platform', 'Spend']].head())
        elif 'date' in df.columns:
            print(f"\nDate Range: {df['date'].min()} to {df['date'].max()}")
            print("\nSample Rows:")
            print(df[['date', 'Platform', 'spend']].head())
            
        if 'Spend' in df.columns:
            total_spend = df['Spend'].sum()
            print(f"\nTotal Spend (raw sum): {total_spend}")
        elif 'spend' in df.columns:
            total_spend = df['spend'].sum()
            print(f"\nTotal Spend (raw sum): {total_spend}")
        else:
            print("No 'Spend' column found in DuckDB.")

    except Exception as e:
        print(f"Error checking DuckDB: {e}")

def check_neo4j_totals():
    print("\n--- Checking Neo4j Totals ---")
    try:
        conn = get_neo4j_connection()
        query = """
        MATCH (c:Campaign)
        RETURN count(c) as count, sum(c.spend) as total_spend
        """
        # Fix: Use execute_query instead of query
        results = conn.execute_query(query)
        if results:
            print(f"Total Campaigns: {results[0]['count']}")
            print(f"Total Spend: {results[0]['total_spend']}")
        else:
            print("No verification result from Neo4j.")
            
    except Exception as e:
        print(f"Error checking Neo4j: {e}")

if __name__ == "__main__":
    check_duckdb_totals()
    check_neo4j_totals()
