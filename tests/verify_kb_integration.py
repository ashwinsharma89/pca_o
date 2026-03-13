
import sys
import os
from pathlib import Path
import json

# Add project root to sys.path
project_root = Path("/Users/ashwin/Desktop/pca_agent_copy")
sys.path.insert(0, str(project_root))

from src.platform.query_engine.hybrid_retrieval import HybridSQLRetrieval, QueryIntent

def test_kb_integration():
    print("Initializing HybridSQLRetrieval...")
    retriever = HybridSQLRetrieval()
    
    # Test query from data/training_questions.json (Question ID 2)
    test_query = "Which platform generated the highest total conversions?"
    
    print(f"\nTesting Query: '{test_query}'")
    
    # Check if similarity matcher finds it
    local_examples = retriever.retrieve_local_examples(test_query, k=1)
    
    if not local_examples:
        print("FAIL: No local examples retrieved.")
        return
        
    best_match = local_examples[0]
    print(f"Best Match Question: '{best_match.question}'")
    print(f"Similarity Score: {best_match.relevance_score:.4f}")
    print(f"Expected SQL from KB: {best_match.sql}")
    
    if best_match.relevance_score > 0.9:
        print("SUCCESS: High-confidence match found in local Knowledge Base.")
    else:
        print(f"WARNING: Match confidence lower than expected ({best_match.relevance_score:.4f}).")

    # Test analysis
    analysis = retriever.analyze_question(test_query)
    print(f"Classified Intent: {analysis['intent'].value}")
    
    if analysis['intent'] == QueryIntent.RANKING:
        print("SUCCESS: Intent correctly classified as RANKING.")
    else:
        print(f"FAIL: Intent classified as {analysis['intent'].value}, expected ranking.")

if __name__ == "__main__":
    test_kb_integration()
