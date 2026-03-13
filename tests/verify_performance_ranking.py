
import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path("/Users/ashwin/Desktop/pca_agent_copy")
sys.path.insert(0, str(project_root))

from src.platform.query_engine.hybrid_retrieval import EntityExtractor, QueryIntent, IntentClassifier

def test_performance_mapping():
    print("Initializing components...")
    extractor = EntityExtractor()
    classifier = IntentClassifier()
    
    test_query = "Top performing campaigns"
    print(f"\nTesting Query: '{test_query}'")
    
    # 1. Test Intent
    intent = classifier.classify(test_query)
    print(f"Intent: {intent.value}")
    
    # 2. Test Entities
    entities = extractor.extract(test_query)
    print(f"Extracted Group By: {entities.group_by}")
    print(f"Extracted Metrics: {entities.metrics}")
    
    # Assertions
    if intent == QueryIntent.RANKING:
        print("SUCCESS: Intent correctly identified as RANKING.")
    else:
        print(f"FAIL: Intent identified as {intent.value}")
        
    if 'campaign' in entities.group_by:
        print("SUCCESS: Campaign dimension correctly identified.")
    else:
        print(f"FAIL: Group by dimensions: {entities.group_by}")
        
    if 'roas' in entities.metrics:
        print("SUCCESS: 'performing' correctly mapped to 'roas' metric.")
    else:
        print(f"FAIL: Metrics extracted: {entities.metrics}")

if __name__ == "__main__":
    test_performance_mapping()
