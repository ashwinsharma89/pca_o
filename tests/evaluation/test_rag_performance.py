"""
RAG Performance Benchmark Tests

Tests RAG system performance under various load conditions.
"""

import pytest
from pathlib import Path

from src.platform.knowledge.vector_store import HybridRetriever, VectorStoreConfig
from src.platform.knowledge.performance_benchmarks import RAGPerformanceBenchmark
from tests.evaluation.rag_ground_truth import GROUND_TRUTH_TEST_SET


class TestRAGPerformance:
    """Performance benchmark tests for RAG system."""
    
    @pytest.fixture(scope="class")
    def retriever(self):
        """Initialize hybrid retriever."""
        config = VectorStoreConfig()
        return HybridRetriever(config=config)
    
    @pytest.fixture(scope="class")
    def benchmark(self, retriever):
        """Initialize performance benchmark."""
        return RAGPerformanceBenchmark(retriever)
    
    @pytest.fixture(scope="class")
    def test_queries(self):
        """Get test queries from ground truth set."""
        return [q['query'] for q in GROUND_TRUTH_TEST_SET]
    
    def test_single_user_latency(self, benchmark, test_queries):
        """Test single user latency."""
        # Use subset for faster testing
        queries = test_queries[:10]
        
        result = benchmark.benchmark_single_user(queries, top_k=5)
        
        # Print results
        print("\n" + "="*60)
        print("SINGLE USER LATENCY TEST")
        print("="*60)
        print(f"\nLatency Stats:")
        print(result.latency_stats)
        print(f"\nThroughput Stats:")
        print(result.throughput_stats)
        print("="*60)
        
        # Assertions
        assert result.throughput_stats.success_rate >= 0.9, "Success rate should be >= 90%"
        assert result.latency_stats.p95_ms < 500, f"P95 latency too high: {result.latency_stats.p95_ms}ms"
    
    def test_5_concurrent_users(self, benchmark, test_queries):
        """Test 5 concurrent users."""
        result = benchmark.benchmark_concurrent_users(
            queries=test_queries,
            concurrent_users=5,
            top_k=5
        )
        
        print("\n" + "="*60)
        print("5 CONCURRENT USERS TEST")
        print("="*60)
        print(f"\nLatency Stats:")
        print(result.latency_stats)
        print(f"\nThroughput Stats:")
        print(result.throughput_stats)
        print("="*60)
        
        # Assertions
        assert result.throughput_stats.success_rate >= 0.9
        assert result.latency_stats.p95_ms < 500
    
    def test_10_concurrent_users(self, benchmark, test_queries):
        """Test 10 concurrent users."""
        result = benchmark.benchmark_concurrent_users(
            queries=test_queries,
            concurrent_users=10,
            top_k=5
        )
        
        print("\n" + "="*60)
        print("10 CONCURRENT USERS TEST")
        print("="*60)
        print(f"\nLatency Stats:")
        print(result.latency_stats)
        print(f"\nThroughput Stats:")
        print(result.throughput_stats)
        print("="*60)
        
        # Assertions
        assert result.throughput_stats.success_rate >= 0.85
        assert result.latency_stats.p95_ms < 1000
    
    def test_20_concurrent_users(self, benchmark, test_queries):
        """Test 20 concurrent users."""
        result = benchmark.benchmark_concurrent_users(
            queries=test_queries,
            concurrent_users=20,
            top_k=5
        )
        
        print("\n" + "="*60)
        print("20 CONCURRENT USERS TEST")
        print("="*60)
        print(f"\nLatency Stats:")
        print(result.latency_stats)
        print(f"\nThroughput Stats:")
        print(result.throughput_stats)
        print("="*60)
        
        # Assertions
        assert result.throughput_stats.success_rate >= 0.80
        assert result.latency_stats.p95_ms < 2000
    
    @pytest.mark.slow
    def test_full_benchmark_suite(self, benchmark, test_queries):
        """Run full benchmark suite with all concurrent user levels."""
        results = benchmark.run_full_benchmark_suite(
            queries=test_queries,
            concurrent_users_list=[1, 5, 10, 20],
            top_k=5
        )
        
        # Print summary table
        benchmark.print_summary_table(results)
        
        # Save report
        report_path = Path("data/rag_performance_report.json")
        benchmark.save_benchmark_report(results, report_path)
        
        # Assertions
        assert len(results) == 4, "Should have 4 benchmark results"
        
        # Check SLA compliance
        single_user_result = results[0]
        assert single_user_result.passes_sla(200.0), \
            f"Single user P95 ({single_user_result.latency_stats.p95_ms}ms) should be < 200ms"
        
        # All tests should have high success rate
        for result in results:
            assert result.throughput_stats.success_rate >= 0.80, \
                f"{result.test_name} success rate too low: {result.throughput_stats.success_rate:.1%}"
    
    def test_latency_percentiles_calculated_correctly(self, benchmark):
        """Test that latency percentiles are calculated correctly."""
        # Simple test with known values
        queries = ["test query"] * 100
        
        result = benchmark.benchmark_single_user(queries, top_k=5)
        
        # Verify percentiles are in order
        assert result.latency_stats.p50_ms <= result.latency_stats.p95_ms
        assert result.latency_stats.p95_ms <= result.latency_stats.p99_ms
        assert result.latency_stats.min_ms <= result.latency_stats.mean_ms
        assert result.latency_stats.mean_ms <= result.latency_stats.max_ms
    
    def test_throughput_calculation(self, benchmark, test_queries):
        """Test throughput (RPS) calculation."""
        queries = test_queries[:20]
        
        result = benchmark.benchmark_single_user(queries, top_k=5)
        
        # RPS should be reasonable (> 1 query per second)
        assert result.throughput_stats.requests_per_second > 1.0, \
            f"RPS too low: {result.throughput_stats.requests_per_second}"
        
        # Total requests should match
        assert result.throughput_stats.total_requests == len(queries)
    
    def test_sla_validation(self, benchmark, test_queries):
        """Test SLA validation logic."""
        queries = test_queries[:10]
        
        result = benchmark.benchmark_single_user(queries, top_k=5)
        
        # Test SLA check
        passes_200ms = result.passes_sla(200.0)
        passes_500ms = result.passes_sla(500.0)
        
        # 500ms SLA should be easier to pass than 200ms
        if not passes_200ms:
            assert passes_500ms or result.latency_stats.p95_ms > 500
    
    @pytest.mark.benchmark
    def test_retrieval_latency_benchmark(self, retriever, benchmark_fixture):
        """Pytest benchmark for retrieval latency."""
        query = "How do I reduce my Facebook ad CPC?"
        
        # Use pytest-benchmark
        result = benchmark_fixture(retriever.search, query, top_k=5)
        
        # Should be fast
        assert benchmark_fixture.stats['mean'] < 0.5  # < 500ms
    
    @pytest.mark.stress
    def test_sustained_load(self, benchmark, test_queries):
        """Test sustained load over time."""
        # Run benchmark multiple times to simulate sustained load
        results = []
        
        for i in range(3):
            print(f"\nRun {i+1}/3...")
            result = benchmark.benchmark_concurrent_users(
                queries=test_queries,
                concurrent_users=5,
                top_k=5
            )
            results.append(result)
        
        # Check that performance doesn't degrade significantly
        p95_latencies = [r.latency_stats.p95_ms for r in results]
        
        # Last run should not be more than 50% slower than first
        assert p95_latencies[-1] < p95_latencies[0] * 1.5, \
            "Performance degraded significantly under sustained load"
    
    @pytest.mark.stress
    def test_spike_load(self, benchmark, test_queries):
        """Test performance under sudden load spike."""
        # Start with 1 user
        result_1 = benchmark.benchmark_concurrent_users(
            queries=test_queries[:10],
            concurrent_users=1,
            top_k=5
        )
        
        # Spike to 20 users
        result_20 = benchmark.benchmark_concurrent_users(
            queries=test_queries,
            concurrent_users=20,
            top_k=5
        )
        
        # System should handle spike gracefully
        assert result_20.throughput_stats.success_rate >= 0.75, \
            "System should handle load spikes with >= 75% success rate"


# ============================================================================
# Comparison Tests
# ============================================================================

class TestRAGPerformanceComparison:
    """Compare performance of different retrieval strategies."""
    
    @pytest.mark.slow
    def test_vector_vs_hybrid_performance(self):
        """Compare vector-only vs hybrid retrieval performance."""
        pytest.skip("Requires implementation of vector-only benchmark")
    
    @pytest.mark.slow
    def test_with_vs_without_reranking(self):
        """Compare performance with and without Cohere reranking."""
        pytest.skip("Requires implementation of reranking toggle")
    
    @pytest.mark.slow
    def test_different_top_k_values(self, benchmark, test_queries):
        """Test performance with different top_k values."""
        queries = test_queries[:10]
        
        results = {}
        for top_k in [1, 5, 10, 20]:
            result = benchmark.benchmark_single_user(queries, top_k=top_k)
            results[top_k] = result
        
        # Print comparison
        print("\n" + "="*60)
        print("TOP_K PERFORMANCE COMPARISON")
        print("="*60)
        for top_k, result in results.items():
            print(f"\ntop_k={top_k}:")
            print(f"  P95: {result.latency_stats.p95_ms:.2f}ms")
            print(f"  RPS: {result.throughput_stats.requests_per_second:.2f}")
        print("="*60)
        
        # Higher top_k should be slower
        assert results[20].latency_stats.mean_ms >= results[1].latency_stats.mean_ms
