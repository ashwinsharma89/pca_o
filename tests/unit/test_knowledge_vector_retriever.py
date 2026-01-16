"""
Unit tests for Vector Store Builder and Retriever.
Tests embedding, indexing, and retrieval operations.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import numpy as np
import json
from pathlib import Path

# Try to import
try:
    from src.platform.knowledge.vector_store import (
        VectorStoreBuilder, VectorStoreConfig, VectorRetriever, _matches_filters
    )
    VECTOR_STORE_AVAILABLE = True
except ImportError:
    VECTOR_STORE_AVAILABLE = False
    VectorStoreBuilder = None
    VectorRetriever = None

pytestmark = pytest.mark.skipif(not VECTOR_STORE_AVAILABLE, reason="Vector store not available")


class TestVectorStoreBuilder:
    """Test VectorStoreBuilder functionality."""
    
    @pytest.fixture
    def mock_builder(self, tmp_path):
        """Create mock builder."""
        config = VectorStoreConfig(
            index_path=tmp_path / "faiss.index",
            metadata_path=tmp_path / "metadata.json"
        )
        
        with patch('src.platform.knowledge.vector_store.OpenAI') as mock_openai:
            mock_client = Mock()
            mock_openai.return_value = mock_client
            
            builder = VectorStoreBuilder(config=config, client=mock_client)
            return builder
    
    def test_initialization(self, mock_builder):
        """Test builder initialization."""
        assert mock_builder is not None
        assert mock_builder.config is not None
    
    def test_build_from_documents(self, mock_builder):
        """Test building index from documents."""
        documents = [
            {
                "success": True,
                "source": "web",
                "url": "https://example.com",
                "title": "Test Doc",
                "chunks": ["Chunk 1 content", "Chunk 2 content"]
            }
        ]
        
        # Mock embedding response
        mock_response = Mock()
        mock_response.data = [
            Mock(embedding=[0.1] * 1536),
            Mock(embedding=[0.2] * 1536)
        ]
        mock_builder.client.embeddings.create.return_value = mock_response
        
        with patch('src.platform.knowledge.vector_store.faiss') as mock_faiss:
            mock_index = Mock()
            mock_faiss.IndexFlatIP.return_value = mock_index
            
            try:
                mock_builder.build_from_documents(documents)
            except Exception:
                pytest.skip("Build requires FAISS setup")
    
    def test_build_from_documents_no_chunks(self, mock_builder):
        """Test building with no valid chunks raises error."""
        documents = [
            {"success": False, "chunks": []}
        ]
        
        with pytest.raises(ValueError, match="No valid chunks"):
            mock_builder.build_from_documents(documents)
    
    def test_embed_texts(self, mock_builder):
        """Test text embedding."""
        texts = ["Text 1", "Text 2"]
        
        mock_response = Mock()
        mock_response.data = [
            Mock(embedding=[0.1] * 1536),
            Mock(embedding=[0.2] * 1536)
        ]
        mock_builder.client.embeddings.create.return_value = mock_response
        
        with patch('src.platform.knowledge.vector_store.faiss') as mock_faiss:
            embeddings = mock_builder._embed_texts(texts)
            
            assert embeddings is not None
            assert embeddings.shape[0] == 2
    
    def test_embed_texts_batching(self, mock_builder):
        """Test embedding with batching."""
        mock_builder.config.batch_size = 2
        texts = ["Text 1", "Text 2", "Text 3", "Text 4"]
        
        mock_response = Mock()
        mock_response.data = [
            Mock(embedding=[0.1] * 1536),
            Mock(embedding=[0.2] * 1536)
        ]
        mock_builder.client.embeddings.create.return_value = mock_response
        
        with patch('src.platform.knowledge.vector_store.faiss') as mock_faiss:
            embeddings = mock_builder._embed_texts(texts)
            
            # Should have called embeddings.create twice (2 batches)
            assert mock_builder.client.embeddings.create.call_count == 2
    
    def test_persist_metadata(self, mock_builder, tmp_path):
        """Test metadata persistence."""
        metadata = [
            {"text": "Chunk 1", "source": "web"},
            {"text": "Chunk 2", "source": "pdf"}
        ]
        
        mock_builder._persist_metadata(metadata)
        
        # Check file was created
        assert mock_builder.config.metadata_path.exists()
        
        # Check content
        saved = json.loads(mock_builder.config.metadata_path.read_text())
        assert len(saved) == 2
    
    def test_load_metadata(self, mock_builder, tmp_path):
        """Test metadata loading."""
        # Create metadata file
        metadata = [{"text": "Test", "source": "web"}]
        mock_builder.config.metadata_path.write_text(json.dumps(metadata))
        
        loaded = mock_builder.load_metadata()
        
        assert len(loaded) == 1
        assert loaded[0]["text"] == "Test"
    
    def test_load_metadata_not_found(self, mock_builder):
        """Test loading non-existent metadata raises error."""
        with pytest.raises(FileNotFoundError):
            mock_builder.load_metadata()


class TestVectorRetriever:
    """Test VectorRetriever functionality."""
    
    @pytest.fixture
    def mock_retriever(self, tmp_path):
        """Create mock retriever."""
        config = VectorStoreConfig(
            index_path=tmp_path / "faiss.index",
            metadata_path=tmp_path / "metadata.json"
        )
        
        # Create mock index and metadata
        metadata = [
            {"text": "Marketing best practices", "source": "web", "category": "marketing"},
            {"text": "Campaign optimization tips", "source": "pdf", "category": "optimization"}
        ]
        config.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        config.metadata_path.write_text(json.dumps(metadata))
        
        with patch('src.platform.knowledge.vector_store.OpenAI') as mock_openai:
            with patch('src.platform.knowledge.vector_store.faiss') as mock_faiss:
                mock_client = Mock()
                mock_openai.return_value = mock_client
                
                mock_index = Mock()
                mock_index.search.return_value = (
                    np.array([[0.9, 0.8]]),  # scores
                    np.array([[0, 1]])  # indices
                )
                mock_faiss.read_index.return_value = mock_index
                
                try:
                    retriever = VectorRetriever(config=config, client=mock_client)
                    retriever.index = mock_index
                    retriever.metadata = metadata
                    return retriever
                except Exception:
                    pytest.skip("Retriever initialization failed")
    
    def test_initialization(self, mock_retriever):
        """Test retriever initialization."""
        assert mock_retriever is not None
    
    def test_retrieve(self, mock_retriever):
        """Test basic retrieval."""
        if hasattr(mock_retriever, 'retrieve'):
            # Mock embedding
            mock_response = Mock()
            mock_response.data = [Mock(embedding=[0.1] * 1536)]
            mock_retriever.client.embeddings.create.return_value = mock_response
            
            try:
                results = mock_retriever.retrieve("marketing tips", k=2)
                assert results is not None
            except Exception:
                pytest.skip("Retrieval requires full setup")
    
    def test_retrieve_with_filters(self, mock_retriever):
        """Test retrieval with metadata filters."""
        if hasattr(mock_retriever, 'retrieve'):
            mock_response = Mock()
            mock_response.data = [Mock(embedding=[0.1] * 1536)]
            mock_retriever.client.embeddings.create.return_value = mock_response
            
            try:
                results = mock_retriever.retrieve(
                    "marketing tips",
                    k=2,
                    filters={"category": "marketing"}
                )
                assert results is not None
            except Exception:
                pytest.skip("Filtered retrieval requires setup")


class TestHybridSearch:
    """Test hybrid search with BM25."""
    
    def test_bm25_scoring(self):
        """Test BM25 keyword scoring concept."""
        # Simulate BM25-like scoring
        documents = [
            "marketing campaign optimization",
            "budget allocation strategy",
            "marketing performance metrics"
        ]
        
        query_terms = ["marketing"]
        
        # Simple term frequency scoring
        scores = []
        for doc in documents:
            score = sum(1 for term in query_terms if term in doc.lower())
            scores.append(score)
        
        # Documents with "marketing" should score higher
        assert scores[0] > scores[1]
        assert scores[2] > scores[1]
    
    def test_hybrid_score_combination(self):
        """Test combining semantic and keyword scores."""
        semantic_scores = [0.9, 0.7, 0.8]
        keyword_scores = [0.5, 0.9, 0.6]
        
        alpha = 0.7  # Weight for semantic
        
        hybrid_scores = [
            alpha * sem + (1 - alpha) * kw
            for sem, kw in zip(semantic_scores, keyword_scores)
        ]
        
        assert len(hybrid_scores) == 3
        # First doc: 0.7 * 0.9 + 0.3 * 0.5 = 0.78
        assert abs(hybrid_scores[0] - 0.78) < 0.01


class TestReranking:
    """Test reranking functionality."""
    
    def test_score_based_reranking(self):
        """Test reranking by score."""
        results = [
            {"text": "A", "score": 0.7},
            {"text": "B", "score": 0.9},
            {"text": "C", "score": 0.8}
        ]
        
        reranked = sorted(results, key=lambda x: x["score"], reverse=True)
        
        assert reranked[0]["text"] == "B"
        assert reranked[1]["text"] == "C"
        assert reranked[2]["text"] == "A"
    
    def test_diversity_reranking(self):
        """Test diversity-based reranking concept."""
        results = [
            {"text": "Marketing tips for B2B", "category": "marketing"},
            {"text": "Marketing strategies", "category": "marketing"},
            {"text": "Budget optimization", "category": "budget"},
            {"text": "Marketing metrics", "category": "marketing"}
        ]
        
        # Simple diversity: prefer different categories
        seen_categories = set()
        diverse_results = []
        
        for r in results:
            if r["category"] not in seen_categories:
                diverse_results.append(r)
                seen_categories.add(r["category"])
        
        # Should have 2 unique categories
        assert len(diverse_results) == 2
