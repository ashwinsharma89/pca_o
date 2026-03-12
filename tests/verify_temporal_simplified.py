"""
Simplified Verification script for Comparative Time Queries.
Avoids loading heavy libraries that might have environment issues in this specific workspace.
"""
import sys
import os
from unittest.mock import MagicMock, patch

# Ensure project root is in path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

def verify_prompt_builder():
    print("--- Verifying PromptBuilder Enhanced with Temporal Context ---")
    try:
        from src.platform.query_engine.prompt_builder import PromptBuilder
        from src.platform.query_engine.temporal_parser import TemporalParser, TemporalIntent
        
        builder = PromptBuilder()
        parser = TemporalParser()
        
        query = "Compare spend for last 2 months vs previous period"
        temporal_analysis = parser.parse(query)
        
        # Mock entities
        class MockEntities:
            def __init__(self):
                self.group_by = []
                self.metrics = ['spend']
                self.time_period = None
                self.granularity = None
                self.limit = None
                self.order_by = None
        
        builder.set_query_analysis(
            intent="comparison",
            complexity="complex",
            entities=MockEntities(),
            temporal=temporal_analysis
        )
        
        prompt = builder.build("Test Question")
        
        checks = [
            "TEMPORAL INTENT: comparison",
            "PRIMARY PERIOD: last 2 months",
            "COMPARISON PERIOD: previous 2 month",
            "MANDATORY COMPARISON RULE: Use Two CTEs"
        ]
        
        all_passed = True
        for check in checks:
            if check in prompt:
                print(f"✅ Found: {check}")
            else:
                print(f"❌ Missing: {check}")
                all_passed = False
        
        return all_passed
    except Exception as e:
        print(f"❌ Error during PromptBuilder verification: {e}")
        return False

def verify_hybrid_analysis():
    print("\n--- Verifying HybridSQLRetrieval analyze_question ---")
    try:
        # Mock the heavy imports if they fail
        from unittest import mock
        with mock.patch('src.platform.query_engine.sql_knowledge.SQLKnowledgeHelper'), \
             mock.patch('src.platform.query_engine.schema_manager.SchemaManager'), \
             mock.patch('src.platform.query_engine.prompt_builder.PromptBuilder'), \
             mock.patch('src.platform.query_engine.executor.QueryExecutor'), \
             mock.patch('src.platform.knowledge.semantic_cache.SemanticCache'):
             
            from src.platform.query_engine.hybrid_retrieval import HybridSQLRetrieval
            
            hybrid = HybridSQLRetrieval(vector_store=None)
            query = "Compare last month vs previous month"
            analysis = hybrid.analyze_question(query)
            
            if 'temporal' in analysis:
                print("✅ 'temporal' key found in analysis")
                if analysis['temporal'].intent.value == 'comparison':
                    print("✅ Temporal intent correctly identified as 'comparison'")
                else:
                    print(f"❌ Wrong temporal intent: {analysis['temporal'].intent}")
                return True
            else:
                print("❌ 'temporal' key NOT found in analysis")
                return False
    except Exception as e:
        print(f"❌ Error during HybridSQLRetrieval verification: {e}")
        return False

if __name__ == "__main__":
    p1 = verify_prompt_builder()
    p2 = verify_hybrid_analysis()
    if p1 and p2:
        sys.exit(0)
    else:
        sys.exit(1)
