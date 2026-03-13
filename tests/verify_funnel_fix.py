
import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path("/Users/ashwin/Desktop/pca_agent_copy")
sys.path.insert(0, str(project_root))

from src.platform.query_engine.hybrid_retrieval import EntityExtractor, QueryIntent, IntentClassifier

def test_funnel_extraction():
    print("Initializing components...")
    extractor = EntityExtractor()
    classifier = IntentClassifier()
    
    test_queries = [
        "marketing funnel performance",
        "TOFU ads spend",
        "upper funnel conversions"
    ]
    
    for test_query in test_queries:
        print(f"\nTesting Query: '{test_query}'")
        
        # 1. Test Intent
        intent = classifier.classify(test_query)
        print(f"Intent: {intent.value}")
        
        # 2. Test Entities
        entities = extractor.extract(test_query)
        print(f"Extracted Group By: {entities.group_by}")
        print(f"Extracted Filters: {entities.filters}")
        
        # Specific assertions for "marketing funnel performance"
        if "marketing funnel performance" in test_query:
            if intent == QueryIntent.BREAKDOWN:
                print("SUCCESS: Intent identified as BREAKDOWN.")
            else:
                print(f"WARNING: Intent identified as {intent.value}")
                
            if 'funnel' in entities.group_by:
                print("SUCCESS: Funnel dimension correctly identified for grouping.")
            else:
                print(f"FAIL: Funnel dimension NOT identified.")
        
        # Specific assertions for aliases
        if "TOFU" in test_query:
            if 'funnel' in entities.filters and 'awareness' in entities.filters['funnel']:
                print("SUCCESS: 'TOFU' correctly mapped to 'awareness'.")
            else:
                print(f"FAIL: Funnel filter: {entities.filters.get('funnel')}")

if __name__ == "__main__":
    test_funnel_extraction()
