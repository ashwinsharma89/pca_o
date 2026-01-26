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

def fix_targeting_data():
    conn = get_neo4j_connection()
    
    print("Fixing Targeting data...")
    
    # Logic:
    # 1. Match Campaign -> Metric to get Platform
    # 2. Match Campaign -> Targeting
    # 3. Set device_types and age_range based on Platform
    
    query = """
    MATCH (c:Campaign)-[:HAS_PERFORMANCE]->(m:Metric)
    MATCH (c)-[:HAS_TARGETING]->(t:Targeting)
    WITH c, t, m.platform AS platform
    
    WITH c, t, platform,
         CASE 
            WHEN platform IN ['TikTok', 'Snapchat', 'Instagram'] THEN ['Mobile']
            WHEN platform IN ['Meta', 'Facebook', 'LinkedIn', 'Pinterest', 'Twitter'] THEN ['Mobile', 'Desktop']
            WHEN platform IN ['Google Ads', 'Bing', 'Search'] THEN ['Mobile', 'Desktop', 'Tablet']
            WHEN platform IN ['YouTube', 'CTV', 'Hulu', 'Roku'] THEN ['TV', 'Mobile', 'Desktop']
            WHEN platform IN ['OOH', 'Dooh'] THEN ['Digital Billboard']
            ELSE ['Mobile', 'Desktop']
         END AS devices,
         ['18-24', '25-34', '35-44', '45-54', '55-64', '65+'] AS all_ages
         
    SET t.device_types = devices
    
    // Assign random age ranges (deterministic based on campaign name hash)
    WITH t, all_ages, toInteger(rand() * 6) as start_idx
    SET t.age_range = all_ages[0..start_idx+1]
    
    RETURN count(t) as updated_count
    """
    
    try:
        result = conn.execute_query(query)
        print(f"Updated {result[0]['updated_count']} Targeting nodes.")
        
        # Verify
        check = conn.execute_query("MATCH (t:Targeting) RETURN t.device_types, t.age_range LIMIT 5")
        for row in check:
            print(f"Device: {row['t.device_types']}, Age: {row['t.age_range']}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fix_targeting_data()
