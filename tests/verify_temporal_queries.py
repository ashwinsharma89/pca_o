"""
Verification script for Comparative Time Queries.
"""
import os
import sys
from unittest.mock import MagicMock, patch

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.kg_rag.query.query_router import QueryRouter
from src.platform.query_engine.nl_to_sql import NaturalLanguageQueryEngine


def verify_nl_to_sql_prompt():
    print("--- Verifying NL-to-SQL Prompt Construction ---")
    # Mock OpenAI and other dependencies
    with patch('src.platform.query_engine.nl_to_sql.OpenAI'), \
         patch('src.platform.query_engine.nl_to_sql.SQLKnowledgeHelper'), \
         patch('src.platform.query_engine.nl_to_sql.HybridSQLRetrieval') as mock_hybrid:

        # Setup mock hybrid analysis
        from src.platform.query_engine.hybrid_retrieval import QueryComplexity, QueryIntent
        from src.platform.query_engine.temporal_parser import TemporalParser

        parser = TemporalParser()
        query = "Compare spend for last 2 months vs previous period"
        temporal_analysis = parser.parse(query)

        mock_hybrid.return_value.analyze_question.return_value = {
            'intent': QueryIntent.COMPARISON,
            'complexity': QueryComplexity.COMPLEX,
            'entities': MagicMock(group_by=[], metrics=['spend'], time_period=None, granularity=None, limit=None, order_by=None),
            'temporal': temporal_analysis
        }
        mock_hybrid.analyze_question = mock_hybrid.return_value.analyze_question

        engine = NaturalLanguageQueryEngine(api_key="test")
        engine.schema_info = {'columns': ['Spend', 'Date']}
        engine.available_models = [('openai', 'gpt-4o')]

        # We want to check the prompt passed to OpenAI
        with patch.object(engine.openai_client.chat.completions, 'create') as mock_create:
            mock_create.return_value.choices = [MagicMock(message=MagicMock(content="SELECT 1"))]

            try:
                engine.generate_sql(query)
            except:
                pass # We only care about the prompt

            call_args = mock_create.call_args
            if not call_args:
                print("❌ OpenAI create was not called")
                return

            prompt = call_args[1]['messages'][1]['content']

            print(f"Query: {query}")
            print(f"Temporal Intent: {temporal_analysis.intent.value}")

            # Checks
            checks = [
                "TEMPORAL INTENT: comparison",
                "PRIMARY PERIOD: last 2 months",
                "COMPARISON PERIOD: previous 2 month",
                "MANDATORY COMPARISON RULE: Use Two CTEs"
            ]

            for check in checks:
                if check in prompt:
                    print(f"✅ Found: {check}")
                else:
                    print(f"❌ Missing: {check}")

def verify_kg_rag_routing():
    print("\n--- Verifying KG-RAG Routing ---")
    with patch('src.kg_rag.query.query_router.get_kuzu_connection'), \
         patch('src.kg_rag.query.query_router.get_kg_rag_settings'):

        llm = MagicMock()
        llm.predict.return_value = "NOVEL_QUERY"

        router = QueryRouter(llm=llm)
        query = "What is the MoM growth in conversions?"

        router.route(query)

        routing_prompt = llm.predict.call_args[1]['text']

        print(f"Query: {query}")

        checks = [
            "Intent: comparison",
            "POP: True",
            "P1: last month",
            "P2: previous 1 month"
        ]

        for check in checks:
            if check in routing_prompt:
                print(f"✅ Found in Routing Prompt: {check}")
            else:
                print(f"❌ Missing in Routing Prompt: {check}")

if __name__ == "__main__":
    verify_nl_to_sql_prompt()
    verify_kg_rag_routing()
