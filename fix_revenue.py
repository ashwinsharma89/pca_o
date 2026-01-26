import sys
import os
import random
from pathlib import Path

# Add project root to python path
project_root = str(Path(__file__).parent)
sys.path.append(project_root)

from dotenv import load_dotenv
load_dotenv()

from src.kg_rag.client.connection import get_neo4j_connection

def fix_revenue_data():
    conn = get_neo4j_connection()
    
    print("Fixing Revenue/ROAS data...")
    
    # Logic:
    # Set revenue based on spend with a random ROAS factor (0.5 to 5.0)
    # This ensures realistic ROAS charts.
    
    query = """
    MATCH (m:Metric)
    WHERE m.spend > 0
    WITH m, (0.5 + rand() * 4.5) as roas
    SET m.revenue = round(m.spend * roas, 2)
    RETURN count(m) as updated_count, avg(roas) as avg_roas
    """
    
    try:
        result = conn.execute_query(query)
        r = result[0]
        print(f"Updated {r['updated_count']} Metric nodes.")
        print(f"Average generated ROAS: {r['avg_roas']:.2f}")
        
        # Verify
        check = conn.execute_query("""
            MATCH (m:Metric) 
            WHERE m.spend > 0 
            RETURN m.spend, m.revenue, m.revenue/m.spend as roas 
            LIMIT 5
        """)
        print("\nVerification:")
        for row in check:
            print(f"Spend: ${row['m.spend']:.2f}, Rev: ${row['m.revenue']:.2f}, ROAS: {row['roas']:.2f}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fix_revenue_data()
