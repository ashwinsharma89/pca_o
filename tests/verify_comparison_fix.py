
import sys
import os
from pathlib import Path

# Add project root to sys.path
project_root = Path("/Users/ashwin/Desktop/pca_agent_copy")
sys.path.insert(0, str(project_root))

from src.platform.query_engine.hybrid_retrieval import EntityExtractor, QueryIntent, IntentClassifier

def test_comparison_extraction():
    print("Initializing Extractor...")
    extractor = EntityExtractor()
    classifier = IntentClassifier()
    
    test_query = "Compare Search vs Social"
    print(f"\nTesting Query: '{test_query}'")
    
    # 1. Test Intent
    intent = classifier.classify(test_query)
    print(f"Intent: {intent.value}")
    
    # 2. Test Entities
    entities = extractor.extract(test_query)
    print(f"Extracted Group By: {entities.group_by}")
    print(f"Extracted Filters: {entities.filters}")
    
    # Assertions
    if intent == QueryIntent.COMPARISON:
        print("SUCCESS: Intent correctly identified as COMPARISON.")
    else:
        print(f"FAIL: Intent identified as {intent.value}")
        
    if 'channel' in entities.filters and set(entities.filters['channel']) == {'search', 'social'}:
        print("SUCCESS: Both channels correctly extracted.")
    else:
        print(f"FAIL: Channels extracted: {entities.filters.get('channel')}")

def test_soc_alias():
    test_query = "Compare Search vs Soc"
    print(f"\nTesting Query with Alias: '{test_query}'")
    
    extractor = EntityExtractor()
    entities = extractor.extract(test_query)
    print(f"Extracted Filters: {entities.filters}")
    
    if 'channel' in entities.filters and set(entities.filters['channel']) == {'search', 'social'}:
        print("SUCCESS: 'Soc' correctly mapped to 'social'.")
    else:
        print(f"FAIL: Channels extracted: {entities.filters.get('channel')}")

if __name__ == "__main__":
    test_comparison_extraction()
    test_soc_alias()
