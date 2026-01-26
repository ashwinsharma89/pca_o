import sys
import os
from pathlib import Path

# Add project root to python path
project_root = str(Path(__file__).parent)
sys.path.append(project_root)

from dotenv import load_dotenv
load_dotenv()

from src.kg_rag.client.connection import get_neo4j_connection

def debug_db():
    try:
        conn = get_neo4j_connection()
        
        print("\n=== DEBUG: Channels ===")
        channels = conn.execute_query("MATCH (c:Channel) RETURN c.id, c.name")
        print(f"Found {len(channels)} channels:")
        for c in channels:
            print(f"- ID: {c.get('c.id')}, Name: {c.get('c.name')}")
            
        print("\n=== DEBUG: Platforms linked to Channels ===")
        rels = conn.execute_query("""
            MATCH (ch:Channel)-[:CATEGORIZES]->(p:Platform) 
            RETURN ch.name as channel, p.name as platform
        """)
        for r in rels:
            print(f"- {r.get('channel')} -> {r.get('platform')}")
            
        
        print("\n=== DEBUG: Platforms ===")
        platforms = conn.execute_query("MATCH (p:Platform) RETURN p.id, p.name")
        print(f"Found {len(platforms)} platforms: {[(p.get('p.id'), p.get('p.name')) for p in platforms]}")

        print("\n=== DEBUG: Metric Duplicates Check (Detailed) ===")
        duplicates = conn.execute_query("""
            MATCH (m:Metric)
            WITH m.campaign_id as cid, m.date as date, m.placement as placement, m.ad_type as ad_type, m.audience_segment as segment, count(*) as count
            WHERE count > 1
            RETURN cid, date, segment, count
            LIMIT 5
        """)
        if duplicates:
            print(f"FOUND EXACT DUPLICATES: {duplicates}")
        else:
            print("No exact duplicates found (unique by segment).")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_db()
