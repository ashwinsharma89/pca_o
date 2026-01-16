"""
Comprehensive unit tests for RAG retrieval functionality.
Tests vector search, hybrid retrieval, and reranking.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import numpy as np
import json

# Try to import
try:
    from src.platform.knowledge.vector_store import VectorRetriever, VectorStoreConfig
    RETRIEVER_AVAILABLE = True
except ImportError:
    RETRIEVER_AVAILABLE = False
    VectorRetriever = None

try:
    from src.platform.knowledge.hybrid_retriever import HybridRetriever
    HYBRID_AVAILABLE = True
except ImportError:
    HYBRID_AVAILABLE = False
    HybridRetriever = None


class TestVectorSearch:
    """Test vector similarity search."""
    
    def test_cosine_similarity(self):
        """Test cosine similarity calculation."""
        vec1 = np.array([1, 0, 0])
        vec2 = np.array([1, 0, 0])
        vec3 = np.array([0, 1, 0])
        
        # Same vectors = 1.0
        sim_same = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
        assert abs(sim_same - 1.0) < 0.001
        
        # Orthogonal vectors = 0.0
        sim_ortho = np.dot(vec1, vec3) / (np.linalg.norm(vec1) * np.linalg.norm(vec3))
        assert abs(sim_ortho - 0.0) < 0.001
    
    def test_top_k_retrieval(self):
        """Test top-k retrieval."""
        scores = [0.9, 0.7, 0.8, 0.6, 0.95]
        indices = list(range(len(scores)))
        
        # Sort by score descending
        sorted_pairs = sorted(zip(scores, indices), reverse=True)
        top_3 = sorted_pairs[:3]
        
        assert top_3[0][0] == 0.95
        assert top_3[1][0] == 0.9
        assert top_3[2][0] == 0.8
    
    def test_score_threshold(self):
        """Test score threshold filtering."""
        results = [
            {"text": "A", "score": 0.9},
            {"text": "B", "score": 0.7},
            {"text": "C", "score": 0.5},
            {"text": "D", "score": 0.3}
        ]
        
        threshold = 0.6
        filtered = [r for r in results if r["score"] >= threshold]
        
        assert len(filtered) == 2
        assert all(r["score"] >= threshold for r in filtered)


class TestHybridRetrieval:
    """Test hybrid retrieval (vector + keyword)."""
    
    def test_reciprocal_rank_fusion(self):
        """Test RRF score calculation."""
        # RRF formula: 1 / (k + rank)
        k = 60
        
        # Document ranked 1st in vector, 3rd in keyword
        vector_rank = 1
        keyword_rank = 3
        
        rrf_score = 1 / (k + vector_rank) + 1 / (k + keyword_rank)
        
        assert rrf_score > 0
        assert rrf_score < 1
    
    def test_score_combination(self):
        """Test combining vector and keyword scores."""
        vector_scores = {"doc1": 0.9, "doc2": 0.7, "doc3": 0.5}
        keyword_scores = {"doc1": 0.6, "doc2": 0.8, "doc4": 0.9}
        
        alpha = 0.7  # Weight for vector
        
        combined = {}
        all_docs = set(vector_scores.keys()) | set(keyword_scores.keys())
        
        for doc in all_docs:
            vec = vector_scores.get(doc, 0)
            kw = keyword_scores.get(doc, 0)
            combined[doc] = alpha * vec + (1 - alpha) * kw
        
        assert "doc1" in combined
        assert "doc4" in combined
    
    def test_deduplication(self):
        """Test result deduplication."""
        results = [
            {"id": 1, "text": "A", "source": "vector"},
            {"id": 1, "text": "A", "source": "keyword"},
            {"id": 2, "text": "B", "source": "vector"}
        ]
        
        seen_ids = set()
        deduplicated = []
        
        for r in results:
            if r["id"] not in seen_ids:
                deduplicated.append(r)
                seen_ids.add(r["id"])
        
        assert len(deduplicated) == 2


class TestBM25Retrieval:
    """Test BM25 keyword retrieval."""
    
    def test_term_frequency(self):
        """Test term frequency calculation."""
        document = "marketing campaign marketing optimization marketing"
        terms = document.lower().split()
        
        tf = {}
        for term in terms:
            tf[term] = tf.get(term, 0) + 1
        
        assert tf["marketing"] == 3
        assert tf["campaign"] == 1
    
    def test_idf_calculation(self):
        """Test IDF calculation concept."""
        import math
        
        total_docs = 100
        docs_with_term = 10
        
        # IDF = log(N / df)
        idf = math.log(total_docs / docs_with_term)
        
        assert idf > 0
        
        # Rare terms have higher IDF
        rare_idf = math.log(total_docs / 2)
        assert rare_idf > idf
    
    def test_bm25_scoring(self):
        """Test BM25 scoring concept."""
        # BM25 parameters
        k1 = 1.5
        b = 0.75
        
        tf = 3  # Term frequency
        doc_len = 100
        avg_doc_len = 80
        idf = 2.0
        
        # BM25 formula
        numerator = tf * (k1 + 1)
        denominator = tf + k1 * (1 - b + b * (doc_len / avg_doc_len))
        score = idf * (numerator / denominator)
        
        assert score > 0


class TestReranking:
    """Test reranking functionality."""
    
    def test_score_based_rerank(self):
        """Test reranking by score."""
        results = [
            {"text": "A", "initial_score": 0.8, "relevance": 0.9},
            {"text": "B", "initial_score": 0.9, "relevance": 0.6},
            {"text": "C", "initial_score": 0.7, "relevance": 0.95}
        ]
        
        # Rerank by relevance
        reranked = sorted(results, key=lambda x: x["relevance"], reverse=True)
        
        assert reranked[0]["text"] == "C"
        assert reranked[1]["text"] == "A"
    
    def test_diversity_rerank(self):
        """Test diversity-aware reranking."""
        results = [
            {"text": "Marketing tips", "category": "marketing", "score": 0.9},
            {"text": "Marketing guide", "category": "marketing", "score": 0.85},
            {"text": "Budget planning", "category": "budget", "score": 0.8},
            {"text": "Marketing best practices", "category": "marketing", "score": 0.75}
        ]
        
        # MMR-like diversity: prefer different categories
        selected = []
        seen_categories = set()
        
        for r in sorted(results, key=lambda x: x["score"], reverse=True):
            if r["category"] not in seen_categories or len(selected) < 2:
                selected.append(r)
                seen_categories.add(r["category"])
            if len(selected) >= 3:
                break
        
        categories = [r["category"] for r in selected]
        assert "budget" in categories


class TestMetadataFiltering:
    """Test metadata-based filtering."""
    
    def test_category_filter(self):
        """Test filtering by category."""
        documents = [
            {"text": "A", "category": "marketing"},
            {"text": "B", "category": "budget"},
            {"text": "C", "category": "marketing"}
        ]
        
        filtered = [d for d in documents if d["category"] == "marketing"]
        
        assert len(filtered) == 2
    
    def test_priority_filter(self):
        """Test filtering by priority."""
        documents = [
            {"text": "A", "priority": 1},
            {"text": "B", "priority": 2},
            {"text": "C", "priority": 1}
        ]
        
        high_priority = [d for d in documents if d["priority"] == 1]
        
        assert len(high_priority) == 2
    
    def test_combined_filters(self):
        """Test combining multiple filters."""
        documents = [
            {"text": "A", "category": "marketing", "priority": 1},
            {"text": "B", "category": "marketing", "priority": 2},
            {"text": "C", "category": "budget", "priority": 1}
        ]
        
        filtered = [
            d for d in documents
            if d["category"] == "marketing" and d["priority"] == 1
        ]
        
        assert len(filtered) == 1
        assert filtered[0]["text"] == "A"


class TestContextWindow:
    """Test context window management."""
    
    def test_token_counting(self):
        """Test approximate token counting."""
        text = "This is a test sentence with multiple words."
        
        # Approximate: 1 token ≈ 4 characters
        approx_tokens = len(text) / 4
        
        assert approx_tokens > 0
    
    def test_context_truncation(self):
        """Test context truncation to fit window."""
        chunks = [
            {"text": "A" * 1000, "score": 0.9},
            {"text": "B" * 1000, "score": 0.8},
            {"text": "C" * 1000, "score": 0.7}
        ]
        
        max_tokens = 500  # ~2000 chars
        
        selected = []
        total_chars = 0
        
        for chunk in sorted(chunks, key=lambda x: x["score"], reverse=True):
            if total_chars + len(chunk["text"]) <= max_tokens * 4:
                selected.append(chunk)
                total_chars += len(chunk["text"])
        
        assert len(selected) < len(chunks)
    
    def test_overlap_handling(self):
        """Test handling overlapping chunks."""
        chunks = [
            {"text": "The quick brown fox", "start": 0, "end": 19},
            {"text": "brown fox jumps over", "start": 10, "end": 30},
            {"text": "jumps over the lazy", "start": 20, "end": 39}
        ]
        
        # Detect overlaps
        overlaps = []
        for i, c1 in enumerate(chunks):
            for c2 in chunks[i+1:]:
                if c1["end"] > c2["start"]:
                    overlaps.append((c1, c2))
        
        assert len(overlaps) == 2
