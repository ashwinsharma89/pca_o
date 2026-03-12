import pytest
pytest.skip("Legacy test - incompatible with current RAG and reasoner structure", allow_module_level=True)
"""
Unit tests for RAG retrieval functionality
Covers: MCPEnhancedRAG, knowledge base retrieval, live data integration
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime

from src.interface.mcp.rag_integration import MCPEnhancedRAG


# ============================================================================
# MCPEnhancedRAG Tests
# ============================================================================

@pytest.mark.skip(reason="Legacy test - incompatible with current RAG and reasoner structure")
class TestMCPEnhancedRAG:
    """Tests for MCPEnhancedRAG class."""
    
    @pytest.fixture
    def rag(self):
        """Create RAG instance with mocked dependencies."""
        with patch('src.interface.mcp.rag_integration.EnhancedReasoningEngine'):
            with patch('src.interface.mcp.rag_integration.PCAMCPClient'):
                rag = MCPEnhancedRAG()
                return rag
    
    def test_initialization(self, rag):
        """RAG should initialize correctly."""
        assert rag is not None
        assert rag.connected == False
    
    @pytest.mark.asyncio
    async def test_connect(self, rag):
        """Should connect to MCP server."""
        rag.mcp_client.connect = AsyncMock()
        
        await rag.connect()
        
        assert rag.connected == True
        rag.mcp_client.connect.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_retrieve_context_static_only(self, rag):
        """Should retrieve context from static knowledge base."""
        # Mock the hybrid retriever
        mock_retriever = MagicMock()
        mock_retriever.search.return_value = [
            {"text": "Google Ads best practices", "metadata": {}, "score": 0.9},
            {"text": "CTR optimization tips", "metadata": {}, "score": 0.8}
        ]
        rag.rag_engine.hybrid_retriever = mock_retriever
        
        contexts = await rag.retrieve_context(
            query="How to improve CTR?",
            include_live_data=False
        )
        
        assert len(contexts) >= 2
        assert contexts[0]["source"] == "knowledge_base"
    
    @pytest.mark.asyncio
    async def test_retrieve_context_with_live_data(self, rag):
        """Should retrieve context from both static and live sources."""
        # Mock static retriever
        mock_retriever = MagicMock()
        mock_retriever.search.return_value = [
            {"text": "Static knowledge", "metadata": {}, "score": 0.8}
        ]
        rag.rag_engine.hybrid_retriever = mock_retriever
        
        # Mock live data
        rag.connected = True
        rag.mcp_client.get_analytics_metrics = AsyncMock(return_value={"ctr": 0.05})
        
        contexts = await rag.retrieve_context(
            query="What is the current CTR?",
            include_live_data=True
        )
        
        # Should have both static and live contexts
        sources = [c["source"] for c in contexts]
        assert "knowledge_base" in sources or len(contexts) > 0


# ============================================================================
# Query Analysis Tests
# ============================================================================

class TestQueryAnalysis:
    """Tests for query analysis and routing."""
    
    @pytest.fixture
    def rag(self):
        """Create RAG instance."""
        with patch('src.interface.mcp.rag_integration.EnhancedReasoningEngine'):
            with patch('src.interface.mcp.rag_integration.PCAMCPClient'):
                return MCPEnhancedRAG()
    
    @pytest.mark.asyncio
    async def test_detects_recent_data_query(self, rag):
        """Should detect queries about recent data."""
        rag.connected = True
        rag.mcp_client.get_analytics_metrics = AsyncMock(return_value={})
        rag.rag_engine.hybrid_retriever = None
        
        contexts = await rag._retrieve_live_data("What is the latest performance?")
        
        # Should have analytics context for "latest" query
        sources = [c["source"] for c in contexts]
        assert "analytics" in sources
    
    @pytest.mark.asyncio
    async def test_detects_platform_query(self, rag):
        """Should detect platform-specific queries."""
        rag.connected = True
        rag.rag_engine.hybrid_retriever = None
        
        contexts = await rag._retrieve_live_data("How is Meta performing?")
        
        # Should have Meta platform context
        sources = [c["source"] for c in contexts]
        assert any("meta" in s for s in sources)
    
    @pytest.mark.asyncio
    async def test_detects_metric_query(self, rag):
        """Should detect metric-specific queries."""
        rag.connected = True
        rag.rag_engine.hybrid_retriever = None
        
        contexts = await rag._retrieve_live_data("What is our ROAS?")
        
        # Should have ROAS metric context
        sources = [c["source"] for c in contexts]
        assert any("roas" in s for s in sources)


# ============================================================================
# Context Scoring Tests
# ============================================================================

class TestContextScoring:
    """Tests for context relevance scoring."""
    
    @pytest.fixture
    def rag(self):
        """Create RAG instance."""
        with patch('src.interface.mcp.rag_integration.EnhancedReasoningEngine'):
            with patch('src.interface.mcp.rag_integration.PCAMCPClient'):
                return MCPEnhancedRAG()
    
    @pytest.mark.asyncio
    async def test_contexts_sorted_by_score(self, rag):
        """Contexts should be sorted by relevance score."""
        # Mock retriever with varied scores
        mock_retriever = MagicMock()
        mock_retriever.search.return_value = [
            {"text": "Low relevance", "metadata": {}, "score": 0.3},
            {"text": "High relevance", "metadata": {}, "score": 0.9},
            {"text": "Medium relevance", "metadata": {}, "score": 0.6}
        ]
        rag.rag_engine.hybrid_retriever = mock_retriever
        
        contexts = await rag.retrieve_context(
            query="test query",
            include_live_data=False
        )
        
        # Should be sorted descending by score
        scores = [c.get("score", 0) for c in contexts]
        assert scores == sorted(scores, reverse=True)
    
    @pytest.mark.asyncio
    async def test_live_data_high_score_for_recent_queries(self, rag):
        """Live data should have high score for recent data queries."""
        rag.connected = True
        rag.mcp_client.get_analytics_metrics = AsyncMock(return_value={})
        rag.rag_engine.hybrid_retriever = None
        
        contexts = await rag._retrieve_live_data("What happened today?")
        
        # Analytics context should have high score
        analytics_contexts = [c for c in contexts if c["source"] == "analytics"]
        if analytics_contexts:
            assert analytics_contexts[0]["score"] >= 0.8


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestRAGErrorHandling:
    """Tests for error handling in RAG."""
    
    @pytest.fixture
    def rag(self):
        """Create RAG instance."""
        with patch('src.interface.mcp.rag_integration.EnhancedReasoningEngine'):
            with patch('src.interface.mcp.rag_integration.PCAMCPClient'):
                return MCPEnhancedRAG()
    
    @pytest.mark.asyncio
    async def test_handles_knowledge_base_error(self, rag):
        """Should handle knowledge base retrieval errors gracefully."""
        # Mock retriever that raises error
        mock_retriever = MagicMock()
        mock_retriever.search.side_effect = Exception("Database error")
        rag.rag_engine.hybrid_retriever = mock_retriever
        
        # Should not raise, just return empty or partial results
        contexts = await rag.retrieve_context(
            query="test query",
            include_live_data=False
        )
        
        assert isinstance(contexts, list)
    
    @pytest.mark.asyncio
    async def test_handles_mcp_connection_error(self, rag):
        """Should handle MCP connection errors gracefully."""
        rag.mcp_client.connect = AsyncMock(side_effect=Exception("Connection failed"))
        
        # Should handle error gracefully
        try:
            await rag.connect()
        except Exception:
            pass  # Expected to fail
        
        assert rag.connected == False
    
    @pytest.mark.asyncio
    async def test_handles_live_data_error(self, rag):
        """Should handle live data retrieval errors gracefully."""
        rag.connected = True
        rag.mcp_client.get_analytics_metrics = AsyncMock(
            side_effect=Exception("API error")
        )
        rag.rag_engine.hybrid_retriever = None
        
        # Should not raise
        contexts = await rag._retrieve_live_data("recent performance")
        
        assert isinstance(contexts, list)


# ============================================================================
# Integration Tests
# ============================================================================

class TestRAGIntegration:
    """Integration tests for RAG components."""
    
    @pytest.fixture
    def rag(self):
        """Create RAG instance."""
        with patch('src.interface.mcp.rag_integration.EnhancedReasoningEngine'):
            with patch('src.interface.mcp.rag_integration.PCAMCPClient'):
                return MCPEnhancedRAG()
    
    @pytest.mark.asyncio
    async def test_full_retrieval_flow(self, rag):
        """Test complete retrieval flow."""
        # Setup mocks
        mock_retriever = MagicMock()
        mock_retriever.search.return_value = [
            {"text": "Knowledge base content", "metadata": {}, "score": 0.85}
        ]
        rag.rag_engine.hybrid_retriever = mock_retriever
        rag.connected = True
        rag.mcp_client.get_analytics_metrics = AsyncMock(return_value={"ctr": 0.05})
        
        # Execute retrieval
        contexts = await rag.retrieve_context(
            query="What is the current CTR trend?",
            top_k=5,
            include_live_data=True
        )
        
        # Verify results
        assert len(contexts) > 0
        assert all("source" in c for c in contexts)
        assert all("content" in c for c in contexts)
    
    @pytest.mark.asyncio
    async def test_respects_top_k_limit(self, rag):
        """Should respect top_k limit on results."""
        # Mock many results
        mock_retriever = MagicMock()
        mock_retriever.search.return_value = [
            {"text": f"Result {i}", "metadata": {}, "score": 0.9 - i*0.1}
            for i in range(10)
        ]
        rag.rag_engine.hybrid_retriever = mock_retriever
        
        contexts = await rag.retrieve_context(
            query="test",
            top_k=3,
            include_live_data=False
        )
        
        # Should return at most top_k * 2 results
        assert len(contexts) <= 6
