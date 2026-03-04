from src.kg_rag.client.connection import get_neo4j_connection
from src.kg_rag.config.settings import get_kg_rag_settings
import json

def debug_kg():
    try:
        settings = get_kg_rag_settings()
        
        conn = get_neo4j_connection()
        
        # 1. Check Node counts
        node_counts = conn.execute_query("MATCH (n) RETURN labels(n) as label, count(*) as count")
        print("Node Counts:")
        print(json.dumps(node_counts, indent=2))
        
        # 2. Check Relationships
        rel_counts = conn.execute_query("MATCH ()-[r]->() RETURN type(r) as type, count(*) as count")
        print("\nRelationship Counts:")
        print(json.dumps(rel_counts, indent=2))
        
        # 3. Sample Campaign
        sample_campaign = conn.execute_query("MATCH (c:Campaign) RETURN c LIMIT 1")
        print("\nSample Campaign:")
        print(json.dumps(sample_campaign, indent=2))
        
        # 4. Sample Metric
        sample_metric = conn.execute_query("MATCH (m:Metric) RETURN m LIMIT 1")
        print("\nSample Metric:")
        print(json.dumps(sample_metric, indent=2))

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_kg()
