"""
Tests for ContextBuilder (Phase B.4).
Verifies RAG context construction with mocked Neo4j.
"""

import pytest
from unittest.mock import MagicMock, patch
from src.kg_rag.context.context_builder import ContextBuilder

@pytest.fixture
def mock_conn():
    """Create a mock Neo4j connection."""
    conn = MagicMock()
    conn.execute_query = MagicMock()
    return conn

class TestContextBuilder:
    """Unit tests for ContextBuilder with mocked graph responses."""

    def test_build_schema_context(self, mock_conn):
        """Verify schema context formatting."""
        builder = ContextBuilder(connection=mock_conn)
        
        # Setup mock responses
        mock_conn.execute_query.side_effect = [
            # _get_node_info
            [{"label": "Campaign", "count": 100, "properties": ["id", "name"]}],
            # _get_relationship_info
            [{"type": "BELONGS_TO"}],
            # _get_relationship_info sample
            [{"from_label": "Campaign", "to_label": "Platform", "count": 100}],
            # _get_sample_platforms
            [{"name": "Google"}, {"name": "Meta"}]
        ]
        
        context = builder.build_schema_context()
        
        assert "# Knowledge Graph Schema" in context
        assert "**Campaign** (100 nodes)" in context
        assert "[:BELONGS_TO]" in context
        assert "Platforms: Google, Meta" in context

    def test_build_query_context_platform(self, mock_conn):
        """Verify query context building for specific platform."""
        builder = ContextBuilder(connection=mock_conn)
        
        # Setup mock for _get_platform_context
        mock_conn.execute_query.side_effect = [
            # _get_platform_context
            [{"platform": "Meta Ads", "campaigns": 5, "spend": 1000.0}],
            # _get_general_stats
            [{"campaigns": 10, "spend": 2000.0, "min_date": "2024-01-01", "max_date": "2024-01-31"}],
            # _get_sample_campaigns
            [{"name": "C1", "platform": "meta", "spend": 500.0}]
        ]
        
        context = builder.build_query_context("Show me Meta campaigns")
        
        assert "## Meta Context" in context
        assert "Meta Ads" in context
        assert "Total Spend: $2,000.00" in context
        assert "C1 (meta): $500.00 spend" in context

    def test_build_compact_context_truncation(self, mock_conn):
        """Verify compact context respect token/char limits."""
        builder = ContextBuilder(connection=mock_conn)
        
        mock_conn.execute_query.return_value = [] # Minimal stats
        
        # Character limit for 10 tokens = 40 chars
        context = builder.build_compact_context(max_tokens=10)
        
        assert len(context) <= 43 # 40 + "..."
        assert context.endswith("...")

    def test_general_stats_formatting(self, mock_conn):
        """Verify stats extraction handles Neo4j date types."""
        builder = ContextBuilder(connection=mock_conn)
        
        class MockDate:
            def iso_format(self): return "2024-05-10"
            def __str__(self): return "2024-05-10"
            
        mock_conn.execute_query.return_value = [{
            "campaigns": 5,
            "spend": 1234.56,
            "min_date": MockDate(),
            "max_date": MockDate()
        }]
        
        stats = builder._get_general_stats()
        assert stats['min_date'] == "2024-05-10"
        assert stats['spend'] == 1234.56
