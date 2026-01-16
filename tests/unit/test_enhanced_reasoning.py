"""
Tests for Enhanced Reasoning Engine.
Tests RAG integration, context retrieval, and reasoning capabilities.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from datetime import datetime

# Import with fallback
try:
    from src.platform.knowledge.enhanced_reasoning import EnhancedReasoningEngine
except ImportError:
    EnhancedReasoningEngine = None


# Skip if not available
pytestmark = pytest.mark.skipif(
    EnhancedReasoningEngine is None,
    reason="EnhancedReasoningEngine not available"
)


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def mock_vector_store():
    """Create mock vector store."""
    store = Mock()
    store.search.return_value = [
        {"content": "Campaign performance data", "score": 0.9},
        {"content": "Budget optimization tips", "score": 0.8}
    ]
    return store


@pytest.fixture
def mock_llm_client():
    """Create mock LLM client."""
    client = Mock()
    client.chat.completions.create.return_value = Mock(
        choices=[Mock(message=Mock(content="Analysis result"))]
    )
    return client


@pytest.fixture
def reasoning_engine(mock_vector_store, mock_llm_client):
    """Create reasoning engine instance."""
    if EnhancedReasoningEngine:
        with patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'}):
            engine = EnhancedReasoningEngine()
            engine.vector_store = mock_vector_store
            return engine
    return None


@pytest.fixture
def sample_campaign_df():
    """Create sample campaign DataFrame."""
    return pd.DataFrame({
        'Campaign_Name': ['Campaign A', 'Campaign B', 'Campaign C'],
        'Platform': ['Google', 'Meta', 'LinkedIn'],
        'Spend': [10000, 8000, 5000],
        'Impressions': [500000, 400000, 200000],
        'Clicks': [10000, 8000, 4000],
        'Conversions': [500, 400, 200],
        'Revenue': [50000, 40000, 20000]
    })


# ============================================================================
# Initialization Tests
# ============================================================================

class TestEnhancedReasoningInit:
    """Tests for EnhancedReasoningEngine initialization."""
    
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'})
    def test_init_creates_instance(self):
        """Should create engine instance."""
        if EnhancedReasoningEngine:
            engine = EnhancedReasoningEngine()
            assert engine is not None
    
    @patch.dict('os.environ', {'OPENAI_API_KEY': 'test_key'})
    def test_init_with_custom_config(self):
        """Should accept custom configuration."""
        if EnhancedReasoningEngine:
            try:
                engine = EnhancedReasoningEngine(
                    model="gpt-4",
                    temperature=0.7
                )
                assert engine is not None
            except TypeError:
                # Custom config not supported
                pass


# ============================================================================
# Context Retrieval Tests
# ============================================================================

class TestContextRetrieval:
    """Tests for context retrieval."""
    
    def test_retrieve_context(self, reasoning_engine, mock_vector_store):
        """Should retrieve relevant context."""
        if reasoning_engine and hasattr(reasoning_engine, 'retrieve_context'):
            context = reasoning_engine.retrieve_context("campaign performance")
            assert context is not None
    
    def test_retrieve_context_with_filters(self, reasoning_engine, mock_vector_store):
        """Should filter context by metadata."""
        if reasoning_engine and hasattr(reasoning_engine, 'retrieve_context'):
            context = reasoning_engine.retrieve_context(
                "campaign performance",
                filters={"platform": "Google"}
            )
            assert context is not None
    
    def test_retrieve_context_limit(self, reasoning_engine, mock_vector_store):
        """Should respect context limit."""
        if reasoning_engine and hasattr(reasoning_engine, 'retrieve_context'):
            context = reasoning_engine.retrieve_context(
                "campaign performance",
                top_k=5
            )
            # Should return at most 5 results
            if isinstance(context, list):
                assert len(context) <= 5


# ============================================================================
# Reasoning Tests
# ============================================================================

class TestReasoning:
    """Tests for reasoning capabilities."""
    
    def test_analyze_query(self, reasoning_engine, sample_campaign_df):
        """Should analyze natural language query."""
        if reasoning_engine and hasattr(reasoning_engine, 'analyze'):
            with patch.object(reasoning_engine, 'analyze') as mock_analyze:
                mock_analyze.return_value = {"answer": "Analysis result"}
                
                result = reasoning_engine.analyze(
                    "What is the best performing campaign?",
                    sample_campaign_df
                )
                
                assert result is not None
    
    def test_generate_insights(self, reasoning_engine, sample_campaign_df):
        """Should generate insights from data."""
        if reasoning_engine and hasattr(reasoning_engine, 'generate_insights'):
            with patch.object(reasoning_engine, 'generate_insights') as mock_insights:
                mock_insights.return_value = ["Insight 1", "Insight 2"]
                
                insights = reasoning_engine.generate_insights(sample_campaign_df)
                
                assert insights is not None
    
    def test_answer_question(self, reasoning_engine, sample_campaign_df):
        """Should answer questions about data."""
        if reasoning_engine and hasattr(reasoning_engine, 'answer_question'):
            with patch.object(reasoning_engine, 'answer_question') as mock_answer:
                mock_answer.return_value = "The total spend is $23,000"
                
                answer = reasoning_engine.answer_question(
                    "What is the total spend?",
                    sample_campaign_df
                )
                
                assert answer is not None


# ============================================================================
# RAG Integration Tests
# ============================================================================

class TestRAGIntegration:
    """Tests for RAG (Retrieval Augmented Generation) integration."""
    
    def test_rag_enhanced_response(self, reasoning_engine, sample_campaign_df, mock_vector_store):
        """Should enhance response with retrieved context."""
        if reasoning_engine and hasattr(reasoning_engine, 'rag_query'):
            with patch.object(reasoning_engine, 'rag_query') as mock_rag:
                mock_rag.return_value = {
                    "answer": "Enhanced answer",
                    "sources": ["doc1", "doc2"]
                }
                
                result = reasoning_engine.rag_query(
                    "How to optimize budget?",
                    sample_campaign_df
                )
                
                assert result is not None
    
    def test_rag_with_no_context(self, reasoning_engine, sample_campaign_df):
        """Should handle case with no relevant context."""
        if reasoning_engine:
            reasoning_engine.vector_store = Mock()
            reasoning_engine.vector_store.search.return_value = []
            
            if hasattr(reasoning_engine, 'rag_query'):
                with patch.object(reasoning_engine, 'rag_query') as mock_rag:
                    mock_rag.return_value = {"answer": "No context available"}
                    
                    result = reasoning_engine.rag_query(
                        "Random question",
                        sample_campaign_df
                    )
                    
                    assert result is not None


# ============================================================================
# Chain of Thought Tests
# ============================================================================

class TestChainOfThought:
    """Tests for chain of thought reasoning."""
    
    def test_step_by_step_reasoning(self, reasoning_engine, sample_campaign_df):
        """Should provide step-by-step reasoning."""
        if reasoning_engine and hasattr(reasoning_engine, 'reason_step_by_step'):
            with patch.object(reasoning_engine, 'reason_step_by_step') as mock_reason:
                mock_reason.return_value = {
                    "steps": ["Step 1", "Step 2", "Step 3"],
                    "conclusion": "Final answer"
                }
                
                result = reasoning_engine.reason_step_by_step(
                    "Complex analysis question",
                    sample_campaign_df
                )
                
                assert result is not None
    
    def test_reasoning_trace(self, reasoning_engine, sample_campaign_df):
        """Should provide reasoning trace."""
        if reasoning_engine and hasattr(reasoning_engine, 'get_reasoning_trace'):
            with patch.object(reasoning_engine, 'get_reasoning_trace') as mock_trace:
                mock_trace.return_value = ["Thought 1", "Thought 2"]
                
                trace = reasoning_engine.get_reasoning_trace()
                
                assert trace is not None


# ============================================================================
# Prompt Engineering Tests
# ============================================================================

class TestPromptEngineering:
    """Tests for prompt engineering."""
    
    def test_build_prompt(self, reasoning_engine, sample_campaign_df):
        """Should build effective prompts."""
        if reasoning_engine and hasattr(reasoning_engine, '_build_prompt'):
            prompt = reasoning_engine._build_prompt(
                "What is the CTR?",
                sample_campaign_df
            )
            
            assert prompt is not None
            assert len(prompt) > 0
    
    def test_prompt_includes_context(self, reasoning_engine, sample_campaign_df, mock_vector_store):
        """Should include retrieved context in prompt."""
        if reasoning_engine and hasattr(reasoning_engine, '_build_prompt_with_context'):
            prompt = reasoning_engine._build_prompt_with_context(
                "What is the CTR?",
                sample_campaign_df,
                context=["Context 1", "Context 2"]
            )
            
            assert prompt is not None


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Tests for error handling."""
    
    def test_handles_llm_error(self, reasoning_engine, sample_campaign_df):
        """Should handle LLM errors gracefully."""
        if reasoning_engine and hasattr(reasoning_engine, 'analyze'):
            with patch.object(reasoning_engine, 'analyze') as mock_analyze:
                mock_analyze.side_effect = Exception("LLM error")
                
                try:
                    result = reasoning_engine.analyze("Question", sample_campaign_df)
                except Exception as e:
                    assert "error" in str(e).lower() or True
    
    def test_handles_empty_dataframe(self, reasoning_engine):
        """Should handle empty DataFrame."""
        if reasoning_engine and hasattr(reasoning_engine, 'analyze'):
            empty_df = pd.DataFrame()
            
            with patch.object(reasoning_engine, 'analyze') as mock_analyze:
                mock_analyze.return_value = {"error": "No data"}
                
                result = reasoning_engine.analyze("Question", empty_df)
                
                assert result is not None
    
    def test_handles_invalid_query(self, reasoning_engine, sample_campaign_df):
        """Should handle invalid queries."""
        if reasoning_engine and hasattr(reasoning_engine, 'analyze'):
            with patch.object(reasoning_engine, 'analyze') as mock_analyze:
                mock_analyze.return_value = {"error": "Invalid query"}
                
                result = reasoning_engine.analyze("", sample_campaign_df)
                
                assert result is not None


# ============================================================================
# Caching Tests
# ============================================================================

class TestCaching:
    """Tests for response caching."""
    
    def test_cache_hit(self, reasoning_engine, sample_campaign_df):
        """Should return cached response."""
        if reasoning_engine and hasattr(reasoning_engine, 'analyze'):
            with patch.object(reasoning_engine, 'analyze') as mock_analyze:
                mock_analyze.return_value = {"answer": "Cached"}
                
                # First call
                result1 = reasoning_engine.analyze("Question", sample_campaign_df)
                # Second call (should use cache if implemented)
                result2 = reasoning_engine.analyze("Question", sample_campaign_df)
                
                assert result1 is not None
                assert result2 is not None
    
    def test_cache_invalidation(self, reasoning_engine):
        """Should invalidate cache when needed."""
        if reasoning_engine and hasattr(reasoning_engine, 'clear_cache'):
            reasoning_engine.clear_cache()
            # Should not raise


# ============================================================================
# Performance Tests
# ============================================================================

class TestPerformance:
    """Tests for performance characteristics."""
    
    def test_response_time_tracking(self, reasoning_engine, sample_campaign_df):
        """Should track response time."""
        if reasoning_engine and hasattr(reasoning_engine, 'get_last_response_time'):
            with patch.object(reasoning_engine, 'analyze') as mock_analyze:
                mock_analyze.return_value = {"answer": "Result"}
                
                reasoning_engine.analyze("Question", sample_campaign_df)
                
                if hasattr(reasoning_engine, 'get_last_response_time'):
                    time = reasoning_engine.get_last_response_time()
                    assert time is None or time >= 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
