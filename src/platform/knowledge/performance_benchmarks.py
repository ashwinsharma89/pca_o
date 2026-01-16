"""
RAG Performance Benchmarks

Measures RAG system performance under various load conditions:
- Single user latency
- Concurrent user throughput
- P50, P95, P99 latency percentiles
- Requests per second (RPS)
- Resource utilization
"""

import time
import statistics
import asyncio
from typing import List, Dict, Any, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json
import numpy as np
from datetime import datetime


@dataclass
class LatencyStats:
    """Statistics for latency measurements."""
    
    min_ms: float
    max_ms: float
    mean_ms: float
    median_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    std_dev_ms: float
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary."""
        return asdict(self)
    
    def __str__(self) -> str:
        """Pretty print stats."""
        return (
            f"Min: {self.min_ms:.2f}ms\n"
            f"Max: {self.max_ms:.2f}ms\n"
            f"Mean: {self.mean_ms:.2f}ms\n"
            f"Median: {self.median_ms:.2f}ms\n"
            f"P50: {self.p50_ms:.2f}ms\n"
            f"P95: {self.p95_ms:.2f}ms\n"
            f"P99: {self.p99_ms:.2f}ms\n"
            f"StdDev: {self.std_dev_ms:.2f}ms"
        )


@dataclass
class ThroughputStats:
    """Statistics for throughput measurements."""
    
    total_requests: int
    successful_requests: int
    failed_requests: int
    total_duration_s: float
    requests_per_second: float
    success_rate: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)
    
    def __str__(self) -> str:
        """Pretty print stats."""
        return (
            f"Total Requests: {self.total_requests}\n"
            f"Successful: {self.successful_requests}\n"
            f"Failed: {self.failed_requests}\n"
            f"Duration: {self.total_duration_s:.2f}s\n"
            f"RPS: {self.requests_per_second:.2f}\n"
            f"Success Rate: {self.success_rate:.1%}"
        )


@dataclass
class BenchmarkResult:
    """Complete benchmark result."""
    
    test_name: str
    concurrent_users: int
    latency_stats: LatencyStats
    throughput_stats: ThroughputStats
    timestamp: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'test_name': self.test_name,
            'concurrent_users': self.concurrent_users,
            'latency_stats': self.latency_stats.to_dict(),
            'throughput_stats': self.throughput_stats.to_dict(),
            'timestamp': self.timestamp
        }
    
    def passes_sla(self, p95_target_ms: float = 200.0) -> bool:
        """Check if benchmark passes SLA."""
        return self.latency_stats.p95_ms <= p95_target_ms


class RAGPerformanceBenchmark:
    """
    Benchmark RAG system performance.
    
    Tests:
    - Single user latency
    - Concurrent user throughput (1, 5, 10, 20 users)
    - Latency percentiles (P50, P95, P99)
    - Requests per second
    """
    
    def __init__(self, retriever):
        """
        Initialize benchmark.
        
        Args:
            retriever: RAG retriever instance to benchmark
        """
        self.retriever = retriever
    
    def _calculate_latency_stats(self, latencies_ms: List[float]) -> LatencyStats:
        """
        Calculate latency statistics.
        
        Args:
            latencies_ms: List of latency measurements in milliseconds
            
        Returns:
            LatencyStats with percentiles
        """
        if not latencies_ms:
            return LatencyStats(0, 0, 0, 0, 0, 0, 0, 0)
        
        sorted_latencies = sorted(latencies_ms)
        
        return LatencyStats(
            min_ms=min(latencies_ms),
            max_ms=max(latencies_ms),
            mean_ms=statistics.mean(latencies_ms),
            median_ms=statistics.median(latencies_ms),
            p50_ms=np.percentile(sorted_latencies, 50),
            p95_ms=np.percentile(sorted_latencies, 95),
            p99_ms=np.percentile(sorted_latencies, 99),
            std_dev_ms=statistics.stdev(latencies_ms) if len(latencies_ms) > 1 else 0
        )
    
    def _single_query(self, query: str, top_k: int = 5) -> Tuple[float, bool]:
        """
        Execute single query and measure latency.
        
        Args:
            query: Search query
            top_k: Number of results to retrieve
            
        Returns:
            Tuple of (latency_ms, success)
        """
        start = time.perf_counter()
        try:
            results = self.retriever.search(query, top_k=top_k)
            elapsed_ms = (time.perf_counter() - start) * 1000
            success = len(results) > 0
            return elapsed_ms, success
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            return elapsed_ms, False
    
    def benchmark_single_user(
        self,
        queries: List[str],
        top_k: int = 5
    ) -> BenchmarkResult:
        """
        Benchmark single user performance.
        
        Args:
            queries: List of test queries
            top_k: Number of results per query
            
        Returns:
            BenchmarkResult with latency and throughput stats
        """
        latencies = []
        successes = 0
        failures = 0
        
        start_time = time.perf_counter()
        
        for query in queries:
            latency_ms, success = self._single_query(query, top_k)
            latencies.append(latency_ms)
            if success:
                successes += 1
            else:
                failures += 1
        
        total_duration = time.perf_counter() - start_time
        
        latency_stats = self._calculate_latency_stats(latencies)
        throughput_stats = ThroughputStats(
            total_requests=len(queries),
            successful_requests=successes,
            failed_requests=failures,
            total_duration_s=total_duration,
            requests_per_second=len(queries) / total_duration if total_duration > 0 else 0,
            success_rate=successes / len(queries) if queries else 0
        )
        
        return BenchmarkResult(
            test_name="Single User",
            concurrent_users=1,
            latency_stats=latency_stats,
            throughput_stats=throughput_stats,
            timestamp=datetime.utcnow().isoformat()
        )
    
    def benchmark_concurrent_users(
        self,
        queries: List[str],
        concurrent_users: int,
        top_k: int = 5
    ) -> BenchmarkResult:
        """
        Benchmark concurrent user performance.
        
        Args:
            queries: List of test queries
            concurrent_users: Number of concurrent users to simulate
            top_k: Number of results per query
            
        Returns:
            BenchmarkResult with latency and throughput stats
        """
        latencies = []
        successes = 0
        failures = 0
        
        # Distribute queries across users
        queries_per_user = len(queries) // concurrent_users
        if queries_per_user == 0:
            queries_per_user = 1
        
        start_time = time.perf_counter()
        
        with ThreadPoolExecutor(max_workers=concurrent_users) as executor:
            futures = []
            
            for i in range(concurrent_users):
                start_idx = i * queries_per_user
                end_idx = start_idx + queries_per_user
                user_queries = queries[start_idx:end_idx]
                
                for query in user_queries:
                    future = executor.submit(self._single_query, query, top_k)
                    futures.append(future)
            
            for future in as_completed(futures):
                latency_ms, success = future.result()
                latencies.append(latency_ms)
                if success:
                    successes += 1
                else:
                    failures += 1
        
        total_duration = time.perf_counter() - start_time
        
        latency_stats = self._calculate_latency_stats(latencies)
        throughput_stats = ThroughputStats(
            total_requests=len(latencies),
            successful_requests=successes,
            failed_requests=failures,
            total_duration_s=total_duration,
            requests_per_second=len(latencies) / total_duration if total_duration > 0 else 0,
            success_rate=successes / len(latencies) if latencies else 0
        )
        
        return BenchmarkResult(
            test_name=f"{concurrent_users} Concurrent Users",
            concurrent_users=concurrent_users,
            latency_stats=latency_stats,
            throughput_stats=throughput_stats,
            timestamp=datetime.utcnow().isoformat()
        )
    
    def run_full_benchmark_suite(
        self,
        queries: List[str],
        concurrent_users_list: List[int] = [1, 5, 10, 20],
        top_k: int = 5
    ) -> List[BenchmarkResult]:
        """
        Run complete benchmark suite.
        
        Args:
            queries: List of test queries
            concurrent_users_list: List of concurrent user counts to test
            top_k: Number of results per query
            
        Returns:
            List of BenchmarkResults for each test
        """
        results = []
        
        for concurrent_users in concurrent_users_list:
            print(f"\nBenchmarking {concurrent_users} concurrent users...")
            
            if concurrent_users == 1:
                result = self.benchmark_single_user(queries, top_k)
            else:
                result = self.benchmark_concurrent_users(queries, concurrent_users, top_k)
            
            results.append(result)
            
            # Print summary
            print(f"\n{result.test_name}:")
            print(f"  P95 Latency: {result.latency_stats.p95_ms:.2f}ms")
            print(f"  RPS: {result.throughput_stats.requests_per_second:.2f}")
            print(f"  Success Rate: {result.throughput_stats.success_rate:.1%}")
            
            # Check SLA
            if result.passes_sla(200.0):
                print(f"  ✅ PASS - P95 < 200ms")
            else:
                print(f"  ❌ FAIL - P95 >= 200ms")
        
        return results
    
    def save_benchmark_report(
        self,
        results: List[BenchmarkResult],
        output_path: Path
    ) -> None:
        """
        Save benchmark report to JSON.
        
        Args:
            results: List of benchmark results
            output_path: Path to save report
        """
        report = {
            'summary': {
                'total_tests': len(results),
                'timestamp': datetime.utcnow().isoformat(),
                'sla_target_p95_ms': 200.0,
                'tests_passing_sla': sum(1 for r in results if r.passes_sla(200.0))
            },
            'results': [r.to_dict() for r in results]
        }
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        
        print(f"\n📊 Benchmark report saved to: {output_path}")
    
    def print_summary_table(self, results: List[BenchmarkResult]) -> None:
        """Print summary table of all results."""
        print("\n" + "="*80)
        print("RAG PERFORMANCE BENCHMARK SUMMARY")
        print("="*80)
        print(f"\n{'Test':<25} {'P50':>10} {'P95':>10} {'P99':>10} {'RPS':>10} {'SLA':>10}")
        print("-"*80)
        
        for result in results:
            sla_status = "✅ PASS" if result.passes_sla(200.0) else "❌ FAIL"
            print(
                f"{result.test_name:<25} "
                f"{result.latency_stats.p50_ms:>9.1f}ms "
                f"{result.latency_stats.p95_ms:>9.1f}ms "
                f"{result.latency_stats.p99_ms:>9.1f}ms "
                f"{result.throughput_stats.requests_per_second:>9.1f} "
                f"{sla_status:>10}"
            )
        
        print("="*80)
        print(f"\nSLA Target: P95 < 200ms")
        print(f"Tests Passing: {sum(1 for r in results if r.passes_sla(200.0))}/{len(results)}")
        print("="*80)
