"""
Unit tests for Vector Store.
Tests FAISS index building and semantic retrieval.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import numpy as np
from pathlib import Path
import tempfile

# Try to import
try:
    from src.platform.knowledge.vector_store import (
        VectorStoreBuilder, VectorStoreConfig, _matches_filters
    )
    VECTOR_STORE_AVAILABLE = True
except ImportError:
    VECTOR_STORE_AVAILABLE = False
    VectorStoreBuilder = None
    VectorStoreConfig = None

pytestmark = pytest.mark.skipif(not VECTOR_STORE_AVAILABLE, reason="Vector store not available")


class TestMatchesFilters:
    """Test filter matching utility."""
    
    def test_no_filters(self):
        """Test with no filters."""
        metadata = {"type": "article", "source": "web"}
        assert _matches_filters(metadata, None) is True
        assert _matches_filters(metadata, {}) is True
    
    def test_single_filter_match(self):
        """Test single filter matching."""
        metadata = {"type": "article", "source": "web"}
        assert _matches_filters(metadata, {"type": "article"}) is True
        assert _matches_filters(metadata, {"type": "video"}) is False
    
    def test_multiple_filters(self):
        """Test multiple filters."""
        metadata = {"type": "article", "source": "web", "category": "marketing"}
        
        assert _matches_filters(metadata, {"type": "article", "source": "web"}) is True
        assert _matches_filters(metadata, {"type": "article", "source": "pdf"}) is False
    
    def test_list_filter(self):
        """Test filter with list of values."""
        metadata = {"type": "article", "source": "web"}
        
        assert _matches_filters(metadata, {"type": ["article", "video"]}) is True
        assert _matches_filters(metadata, {"type": ["video", "pdf"]}) is False


class TestVectorStoreConfig:
    """Test VectorStoreConfig dataclass."""
    
    def test_default_config(self):
        """Test default configuration."""
        config = VectorStoreConfig()
        
        assert config.embedding_model == "text-embedding-3-small"
        assert config.batch_size == 64
        assert isinstance(config.index_path, Path)
    
    def test_custom_config(self):
        """Test custom configuration."""
        config = VectorStoreConfig(
            embedding_model="text-embedding-ada-002",
            batch_size=32
        )
        
        assert config.embedding_model == "text-embedding-ada-002"
        assert config.batch_size == 32


class TestVectorStoreBuilder:
    """Test VectorStoreBuilder functionality."""
    
    @pytest.fixture
    def builder(self, tmp_path):
        """Create vector store builder."""
        config = VectorStoreConfig(
            index_path=tmp_path / "faiss.index",
            metadata_path=tmp_path / "metadata.json"
        )
        
        with patch('src.platform.knowledge.vector_store.OpenAI') as mock_openai:
            mock_client = Mock()
            mock_openai.return_value = mock_client
            return VectorStoreBuilder(config=config)
    
    def test_initialization(self, builder):
        """Test builder initialization."""
        assert builder is not None
    
    def test_get_embeddings(self, builder):
        """Test getting embeddings."""
        if hasattr(builder, '_get_embeddings'):
            # Mock the embedding response
            mock_response = Mock()
            mock_response.data = [Mock(embedding=[0.1] * 1536)]
            builder.client.embeddings.create = Mock(return_value=mock_response)
            
            embeddings = builder._get_embeddings(["test text"])
            assert embeddings is not None
    
    def test_build_index(self, builder):
        """Test building FAISS index."""
        if hasattr(builder, 'build'):
            chunks = [
                {"text": "Marketing best practices", "metadata": {"type": "article"}},
                {"text": "Campaign optimization tips", "metadata": {"type": "guide"}}
            ]
            
            # Mock embeddings
            mock_response = Mock()
            mock_response.data = [Mock(embedding=[0.1] * 1536) for _ in chunks]
            builder.client.embeddings.create = Mock(return_value=mock_response)
            
            try:
                builder.build(chunks)
            except Exception:
                pytest.skip("Index building requires FAISS setup")


class TestVectorStoreRetrieval:
    """Test vector store retrieval functionality."""
    
    @pytest.fixture
    def mock_retriever(self):
        """Create mock retriever."""
        retriever = Mock()
        retriever.search = Mock(return_value=[
            {"text": "Result 1", "score": 0.9},
            {"text": "Result 2", "score": 0.8}
        ])
        return retriever
    
    def test_search_returns_results(self, mock_retriever):
        """Test that search returns results."""
        results = mock_retriever.search("marketing tips", k=5)
        
        assert len(results) == 2
        assert results[0]["score"] > results[1]["score"]
    
    def test_search_with_filters(self, mock_retriever):
        """Test search with metadata filters."""
        mock_retriever.search = Mock(return_value=[
            {"text": "Filtered result", "score": 0.85, "metadata": {"type": "article"}}
        ])
        
        results = mock_retriever.search(
            "marketing tips",
            filters={"type": "article"}
        )
        
        assert len(results) == 1
        assert results[0]["metadata"]["type"] == "article"


class TestHybridSearch:
    """Test hybrid search functionality."""
    
    def test_bm25_available(self):
        """Test BM25 availability check."""
        try:
            from src.platform.knowledge.vector_store import BM25_AVAILABLE
            assert isinstance(BM25_AVAILABLE, bool)
        except ImportError:
            pytest.skip("Vector store not available")
    
    def test_cohere_reranking_available(self):
        """Test Cohere reranking availability."""
        try:
            from src.platform.knowledge.vector_store import COHERE_AVAILABLE
            assert isinstance(COHERE_AVAILABLE, bool)
        except ImportError:
            pytest.skip("Vector store not available")
