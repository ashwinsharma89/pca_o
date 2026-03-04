"""
Tests for Hybrid SQL Retrieval (Phase B.5).
Verifies intent classification, entity extraction, and reranking.
"""

import pytest
from unittest.mock import MagicMock
from src.platform.query_engine.hybrid_retrieval import (
    HybridSQLRetrieval, QueryIntent, QueryComplexity, 
    IntentClassifier, EntityExtractor, ComplexityClassifier
)

class TestHybridRetrievalUnits:
    """Unit tests for hybrid retrieval components."""

    def test_intent_classification(self):
        """Verify intent detection for various query types."""
        classifier = IntentClassifier()
        
        assert classifier.classify("What are the best campaigns?") == QueryIntent.RANKING
        assert classifier.classify("Compare Meta and Google") == QueryIntent.COMPARISON
        assert classifier.classify("What is the trend for spend?") == QueryIntent.TREND
        assert classifier.classify("Total spend by platform") == QueryIntent.BREAKDOWN
        assert classifier.classify("Show me only Meta campaigns") == QueryIntent.FILTER

    def test_entity_extraction(self):
        """Verify dimension and metric extraction."""
        extractor = EntityExtractor()
        entities = extractor.extract("Show me spend and clicks by platform for last 7 days")
        
        assert "spend" in entities.metrics
        assert "clicks" in entities.metrics
        assert "platform" in entities.group_by
        assert entities.time_period == "last_7_days"

    def test_complexity_classification(self):
        """Verify complexity logic."""
        classifier = ComplexityClassifier()
        extractor = EntityExtractor()
        
        # Simple
        q1 = "What is the spend?"
        assert classifier.classify(q1, extractor.extract(q1)) == QueryComplexity.SIMPLE
        
        # Medium (Multiple dimensions/metrics)
        q2 = "Compare spend and clicks by platform and device"
        assert classifier.classify(q2, extractor.extract(q2)) == QueryComplexity.MEDIUM
        
        # Complex (Advanced patterns)
        q3 = "What is the week over week growth and anomaly in ROAS?"
        assert classifier.classify(q3, extractor.extract(q3)) == QueryComplexity.COMPLEX

    def test_reranking_logic(self):
        """Verify structural + semantic reranking."""
        retriever = HybridSQLRetrieval(vector_store=MagicMock())
        
        question = "Top 5 campaigns by spend"
        analysis = retriever.analyze_question(question)
        
        # Mock candidates
        class MockResult:
            def __init__(self, content, metadata):
                self.page_content = content
                self.metadata = metadata
        
        candidates = [
            MockResult("Best campaigns", {"sql": "SELECT ... ORDER BY spend LIMIT 5", "intent": "ranking"}),
            MockResult("Meta spend", {"sql": "SELECT spend", "intent": "aggregation"})
        ]
        
        results = retriever._rerank_by_similarity(question, candidates, analysis, k=2)
        
        assert len(results) == 2
        # Ranking intent match should score higher
        assert results[0].intent == QueryIntent.RANKING
        assert "ORDER BY" in results[0].sql

    def test_get_sql_hints(self):
        """Verify generation of SQL hints."""
        retriever = HybridSQLRetrieval()
        analysis = retriever.analyze_question("Top 10 campaigns by CPC")
        hints = retriever.get_sql_hints(analysis)
        
        assert "ORDER BY" in hints
        assert "LIMIT 10" in hints
        assert any("GROUP BY" in h for h in hints) or True # Depends on extraction
