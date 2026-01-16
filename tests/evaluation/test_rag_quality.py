"""
RAG Retrieval Quality Tests

Tests retrieval quality using ground truth test set and standard IR metrics.
"""

import pytest
from pathlib import Path
from typing import List, Dict, Any

from src.platform.knowledge.vector_store import HybridRetriever, VectorStoreConfig
from src.platform.knowledge.retrieval_metrics import RetrievalQualityEvaluator, RetrievalMetrics
from tests.evaluation.rag_ground_truth import (
    GROUND_TRUTH_TEST_SET,
    get_test_set_by_category,
    get_test_set_by_platform,
    get_test_set_stats
)


class TestRAGRetrievalQuality:
    """Test suite for RAG retrieval quality."""
    
    @pytest.fixture(scope="class")
    def retriever(self):
        """Initialize hybrid retriever."""
        config = VectorStoreConfig()
        return HybridRetriever(config=config)
    
    @pytest.fixture(scope="class")
    def evaluator(self):
        """Initialize quality evaluator."""
        return RetrievalQualityEvaluator()
    
    def test_ground_truth_test_set_loaded(self):
        """Test that ground truth test set is properly loaded."""
        assert len(GROUND_TRUTH_TEST_SET) > 0, "Ground truth test set is empty"
        
        # Verify structure
        for query_data in GROUND_TRUTH_TEST_SET:
            assert 'query' in query_data
            assert 'relevant_docs' in query_data
            assert 'category' in query_data
            assert len(query_data['relevant_docs']) > 0
    
    def test_test_set_statistics(self):
        """Test ground truth test set statistics."""
        stats = get_test_set_stats()
        
        assert stats['total_queries'] >= 20, "Should have at least 20 test queries"
        assert stats['avg_relevant_docs_per_query'] >= 3, "Should have at least 3 relevant docs per query"
        
        # Should cover multiple categories
        assert len(stats['categories']) >= 4
        
        # Should cover multiple platforms
        assert len(stats['platforms']) >= 3
    
    def test_precision_at_k_calculation(self, evaluator):
        """Test Precision@K calculation."""
        retrieved = ['doc1', 'doc2', 'doc3', 'doc4', 'doc5']
        relevant = {'doc1', 'doc3', 'doc5'}
        
        # Precision@3 = 2/3 (doc1 and doc3 are relevant)
        p3 = evaluator.precision_at_k(retrieved, relevant, 3)
        assert p3 == pytest.approx(2/3, abs=0.01)
        
        # Precision@5 = 3/5 (doc1, doc3, doc5 are relevant)
        p5 = evaluator.precision_at_k(retrieved, relevant, 5)
        assert p5 == pytest.approx(3/5, abs=0.01)
    
    def test_recall_at_k_calculation(self, evaluator):
        """Test Recall@K calculation."""
        retrieved = ['doc1', 'doc2', 'doc3']
        relevant = {'doc1', 'doc3', 'doc5', 'doc7'}  # 4 relevant docs total
        
        # Recall@3 = 2/4 (doc1 and doc3 retrieved out of 4 relevant)
        r3 = evaluator.recall_at_k(retrieved, relevant, 3)
        assert r3 == pytest.approx(0.5, abs=0.01)
    
    def test_mrr_calculation(self, evaluator):
        """Test Mean Reciprocal Rank calculation."""
        # First relevant doc at position 3
        retrieved = ['doc1', 'doc2', 'doc3', 'doc4']
        relevant = {'doc3', 'doc5'}
        
        mrr = evaluator.mean_reciprocal_rank(retrieved, relevant)
        assert mrr == pytest.approx(1/3, abs=0.01)
        
        # First relevant doc at position 1
        retrieved2 = ['doc3', 'doc1', 'doc2']
        mrr2 = evaluator.mean_reciprocal_rank(retrieved2, relevant)
        assert mrr2 == pytest.approx(1.0, abs=0.01)
    
    def test_ndcg_calculation(self, evaluator):
        """Test NDCG@K calculation."""
        retrieved = ['doc1', 'doc2', 'doc3', 'doc4', 'doc5']
        relevant = {'doc1', 'doc3', 'doc5'}
        
        # NDCG should be between 0 and 1
        ndcg5 = evaluator.ndcg_at_k(retrieved, relevant, 5)
        assert 0 <= ndcg5 <= 1
        
        # Perfect ranking should have NDCG = 1
        perfect_retrieved = ['doc1', 'doc3', 'doc5', 'doc2', 'doc4']
        ndcg_perfect = evaluator.ndcg_at_k(perfect_retrieved, relevant, 5)
        assert ndcg_perfect == pytest.approx(1.0, abs=0.01)
    
    def test_average_precision_calculation(self, evaluator):
        """Test Average Precision calculation."""
        retrieved = ['doc1', 'doc2', 'doc3', 'doc4', 'doc5']
        relevant = {'doc1', 'doc3', 'doc5'}
        
        # AP should be between 0 and 1
        ap = evaluator.average_precision(retrieved, relevant)
        assert 0 <= ap <= 1
    
    @pytest.mark.slow
    def test_retrieval_quality_optimization_queries(self, retriever, evaluator):
        """Test retrieval quality on optimization queries."""
        optimization_queries = get_test_set_by_category('optimization')
        
        test_results = []
        for query_data in optimization_queries:
            query = query_data['query']
            relevant_docs = set(query_data['relevant_docs'])
            
            # Retrieve documents
            results = retriever.search(query, top_k=10)
            retrieved_ids = [self._extract_doc_id(r) for r in results]
            
            # Evaluate
            metrics = evaluator.evaluate_query(retrieved_ids, relevant_docs)
            
            test_results.append({
                'query': query,
                'retrieved': retrieved_ids,
                'relevant': list(relevant_docs)
            })
        
        # Calculate average metrics
        avg_metrics, _ = evaluator.evaluate_test_set(test_results)
        
        # Assert minimum quality thresholds
        assert avg_metrics.precision_at_5 >= 0.4, f"Precision@5 too low: {avg_metrics.precision_at_5}"
        assert avg_metrics.recall_at_10 >= 0.5, f"Recall@10 too low: {avg_metrics.recall_at_10}"
        assert avg_metrics.mrr >= 0.5, f"MRR too low: {avg_metrics.mrr}"
    
    @pytest.mark.slow
    def test_retrieval_quality_troubleshooting_queries(self, retriever, evaluator):
        """Test retrieval quality on troubleshooting queries."""
        troubleshooting_queries = get_test_set_by_category('troubleshooting')
        
        test_results = []
        for query_data in troubleshooting_queries:
            query = query_data['query']
            relevant_docs = set(query_data['relevant_docs'])
            
            results = retriever.search(query, top_k=10)
            retrieved_ids = [self._extract_doc_id(r) for r in results]
            
            test_results.append({
                'query': query,
                'retrieved': retrieved_ids,
                'relevant': list(relevant_docs)
            })
        
        avg_metrics, _ = evaluator.evaluate_test_set(test_results)
        
        # Troubleshooting queries should have good recall
        assert avg_metrics.recall_at_10 >= 0.5, f"Recall@10 too low for troubleshooting: {avg_metrics.recall_at_10}"
    
    @pytest.mark.slow
    def test_retrieval_quality_full_test_set(self, retriever, evaluator):
        """Test retrieval quality on full test set."""
        test_results = []
        
        for query_data in GROUND_TRUTH_TEST_SET:
            query = query_data['query']
            relevant_docs = set(query_data['relevant_docs'])
            
            # Retrieve documents
            results = retriever.search(query, top_k=10)
            retrieved_ids = [self._extract_doc_id(r) for r in results]
            
            test_results.append({
                'query': query,
                'retrieved': retrieved_ids,
                'relevant': list(relevant_docs)
            })
        
        # Evaluate
        avg_metrics, per_query_metrics = evaluator.evaluate_test_set(test_results)
        
        # Save evaluation report
        report_path = Path("data/rag_evaluation_report.json")
        evaluator.save_evaluation_report(avg_metrics, per_query_metrics, report_path)
        
        # Print summary
        print("\n" + "="*60)
        print("RAG RETRIEVAL QUALITY EVALUATION")
        print("="*60)
        print(f"\nTest Set Size: {len(GROUND_TRUTH_TEST_SET)} queries")
        print(f"\nAverage Metrics:")
        print(avg_metrics)
        print("\n" + "="*60)
        
        # Assert minimum quality thresholds
        assert avg_metrics.precision_at_5 >= 0.4, f"Overall Precision@5 too low: {avg_metrics.precision_at_5}"
        assert avg_metrics.recall_at_10 >= 0.5, f"Overall Recall@10 too low: {avg_metrics.recall_at_10}"
        assert avg_metrics.ndcg_at_5 >= 0.5, f"Overall NDCG@5 too low: {avg_metrics.ndcg_at_5}"
        assert avg_metrics.mrr >= 0.5, f"Overall MRR too low: {avg_metrics.mrr}"
    
    def test_hybrid_retrieval_better_than_vector_only(self, evaluator):
        """Test that hybrid retrieval outperforms vector-only."""
        # This test would compare HybridRetriever vs VectorRetriever
        # on the same test set and verify hybrid is better
        pytest.skip("Requires comparison implementation")
    
    def test_reranking_improves_quality(self, evaluator):
        """Test that Cohere reranking improves retrieval quality."""
        # This test would compare with/without reranking
        # and verify reranking improves metrics
        pytest.skip("Requires reranking comparison implementation")
    
    @staticmethod
    def _extract_doc_id(result: Dict[str, Any]) -> str:
        """
        Extract document ID from retrieval result.
        
        This is a placeholder - you'll need to implement based on
        how your documents are structured.
        """
        # Option 1: Use URL as ID
        if 'metadata' in result and 'url' in result['metadata']:
            return result['metadata']['url']
        
        # Option 2: Use title as ID
        if 'metadata' in result and 'title' in result['metadata']:
            return result['metadata']['title']
        
        # Option 3: Use text hash
        return str(hash(result.get('text', '')))


# ============================================================================
# Benchmark Tests
# ============================================================================

class TestRAGBenchmarks:
    """Benchmark tests for RAG performance."""
    
    @pytest.mark.benchmark
    def test_retrieval_latency(self, retriever, benchmark):
        """Benchmark retrieval latency."""
        query = "How do I reduce my Facebook ad CPC?"
        
        result = benchmark(retriever.search, query, top_k=5)
        
        # Should retrieve in < 500ms
        assert benchmark.stats['mean'] < 0.5
    
    @pytest.mark.benchmark
    def test_retrieval_throughput(self, retriever):
        """Test retrieval throughput (queries per second)."""
        import time
        
        queries = [q['query'] for q in GROUND_TRUTH_TEST_SET[:10]]
        
        start = time.time()
        for query in queries:
            retriever.search(query, top_k=5)
        elapsed = time.time() - start
        
        qps = len(queries) / elapsed
        
        # Should handle at least 5 queries per second
        assert qps >= 5, f"Throughput too low: {qps:.2f} QPS"
