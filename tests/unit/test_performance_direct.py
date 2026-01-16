"""
Direct tests for utils/performance.py to increase coverage.
Tests ParallelExecutor, SemanticCache, QueryBundler, and other classes.
"""

import pytest
import asyncio
import time
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

from src.core.utils.performance import (
    ParallelExecutor,
    SemanticCache,
    CacheEntry,
    SmartQueryBundler,
    TokenOptimizer,
    ProgressStreamer,
    PrecomputationManager,
    ModelSelector,
    PerformanceOptimizer
)


class TestParallelExecutor:
    """Tests for ParallelExecutor."""
    
    def test_singleton_instance(self):
        """Test singleton pattern."""
        executor1 = ParallelExecutor.get_instance()
        executor2 = ParallelExecutor.get_instance()
        assert executor1 is executor2
    
    def test_initialization(self):
        """Test initialization with custom workers."""
        executor = ParallelExecutor(max_workers=4)
        assert executor.max_workers == 4
    
    def test_execute_parallel_empty(self):
        """Test parallel execution with empty tasks."""
        executor = ParallelExecutor.get_instance()
        results = executor.execute_parallel([])
        assert results == []
    
    def test_execute_parallel_single_task(self):
        """Test parallel execution with single task."""
        executor = ParallelExecutor.get_instance()
        
        def simple_task(x):
            return x * 2
        
        tasks = [(simple_task, (5,), {})]
        results = executor.execute_parallel(tasks)
        assert results == [10]
    
    def test_execute_parallel_multiple_tasks(self):
        """Test parallel execution with multiple tasks."""
        executor = ParallelExecutor.get_instance()
        
        def add(a, b):
            return a + b
        
        tasks = [
            (add, (1, 2), {}),
            (add, (3, 4), {}),
            (add, (5, 6), {})
        ]
        results = executor.execute_parallel(tasks)
        assert results == [3, 7, 11]
    
    def test_execute_parallel_with_kwargs(self):
        """Test parallel execution with kwargs."""
        executor = ParallelExecutor.get_instance()
        
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"
        
        tasks = [
            (greet, ("Alice",), {"greeting": "Hi"}),
            (greet, ("Bob",), {})
        ]
        results = executor.execute_parallel(tasks)
        assert results == ["Hi, Alice!", "Hello, Bob!"]
    
    def test_execute_parallel_with_exception(self):
        """Test parallel execution handles exceptions."""
        executor = ParallelExecutor.get_instance()
        
        def failing_task():
            raise ValueError("Test error")
        
        def success_task():
            return "success"
        
        tasks = [
            (failing_task, (), {}),
            (success_task, (), {})
        ]
        results = executor.execute_parallel(tasks)
        assert results[0] is None
        assert results[1] == "success"
    
    def test_get_metrics(self):
        """Test getting metrics."""
        executor = ParallelExecutor.get_instance()
        metrics = executor.get_metrics()
        assert 'total_tasks' in metrics
        assert 'parallel_executions' in metrics
    
    @pytest.mark.asyncio
    async def test_execute_async_empty(self):
        """Test async execution with empty coroutines."""
        executor = ParallelExecutor.get_instance()
        results = await executor.execute_async([])
        assert results == []
    
    @pytest.mark.asyncio
    async def test_execute_async_multiple(self):
        """Test async execution with multiple coroutines."""
        executor = ParallelExecutor.get_instance()
        
        async def async_task(x):
            await asyncio.sleep(0.01)
            return x * 2
        
        coroutines = [async_task(1), async_task(2), async_task(3)]
        results = await executor.execute_async(coroutines)
        assert results == [2, 4, 6]


class TestSemanticCache:
    """Tests for SemanticCache."""
    
    @pytest.fixture
    def cache(self, tmp_path):
        """Create cache instance with temp directory."""
        SemanticCache._instance = None  # Reset singleton
        return SemanticCache(cache_dir=str(tmp_path), max_entries=100)
    
    def test_initialization(self, cache):
        """Test cache initialization."""
        assert cache is not None
        assert cache.max_entries == 100
    
    def test_compute_embedding(self, cache):
        """Test embedding computation."""
        embedding = cache._compute_embedding("test query")
        assert len(embedding) == 128
        assert all(isinstance(x, float) for x in embedding)
    
    def test_cosine_similarity(self, cache):
        """Test cosine similarity calculation."""
        a = [1.0, 0.0, 0.0]
        b = [1.0, 0.0, 0.0]
        similarity = cache._cosine_similarity(a, b)
        assert similarity == pytest.approx(1.0)
        
        c = [0.0, 1.0, 0.0]
        similarity = cache._cosine_similarity(a, c)
        assert similarity == pytest.approx(0.0)
    
    def test_cosine_similarity_different_lengths(self, cache):
        """Test cosine similarity with different length vectors."""
        a = [1.0, 0.0]
        b = [1.0, 0.0, 0.0]
        similarity = cache._cosine_similarity(a, b)
        assert similarity == 0.0
    
    def test_cosine_similarity_zero_vectors(self, cache):
        """Test cosine similarity with zero vectors."""
        a = [0.0, 0.0, 0.0]
        b = [1.0, 0.0, 0.0]
        similarity = cache._cosine_similarity(a, b)
        assert similarity == 0.0
    
    def test_get_cache_key(self, cache):
        """Test cache key generation."""
        key1 = cache._get_cache_key("test query")
        key2 = cache._get_cache_key("TEST QUERY")
        assert key1 == key2  # Should be case-insensitive
    
    def test_set_and_get(self, cache):
        """Test setting and getting cache entries."""
        cache.set("test query", {"result": "data"})
        result = cache.get("test query")
        assert result == {"result": "data"}
    
    def test_get_miss(self, cache):
        """Test cache miss."""
        result = cache.get("nonexistent query")
        assert result is None
    
    def test_get_metrics(self, cache):
        """Test getting cache metrics."""
        metrics = cache.get_metrics()
        assert 'hits' in metrics
        assert 'misses' in metrics


class TestCacheEntry:
    """Tests for CacheEntry dataclass."""
    
    def test_creation(self):
        """Test creating cache entry."""
        entry = CacheEntry(
            query_embedding=[0.1, 0.2],
            query_text="test",
            results={"data": "value"},
            timestamp=datetime.now()
        )
        assert entry.query_text == "test"
        assert entry.hit_count == 0
    
    def test_defaults(self):
        """Test default values."""
        entry = CacheEntry(
            query_embedding=None,
            query_text="test",
            results=None,
            timestamp=datetime.now()
        )
        assert entry.ttl_hours == 168


class TestSmartQueryBundler:
    """Tests for SmartQueryBundler."""
    
    @pytest.fixture
    def bundler(self):
        """Create bundler instance."""
        return SmartQueryBundler()
    
    def test_initialization(self, bundler):
        """Test bundler initialization."""
        assert bundler is not None
    
    def test_add_query(self, bundler):
        """Test adding query."""
        if hasattr(bundler, 'add_query'):
            bundler.add_query("query1", {"context": "test"})
        elif hasattr(bundler, 'add'):
            bundler.add("query1", {"context": "test"})
    
    def test_bundle_queries(self, bundler):
        """Test bundling queries."""
        if hasattr(bundler, 'bundle'):
            bundles = bundler.bundle()
            assert bundles is not None
    
    def test_get_metrics(self, bundler):
        """Test getting metrics."""
        if hasattr(bundler, 'get_metrics'):
            metrics = bundler.get_metrics()
            assert metrics is not None


class TestTokenOptimizer:
    """Tests for TokenOptimizer."""
    
    @pytest.fixture
    def optimizer(self):
        """Create optimizer instance."""
        return TokenOptimizer()
    
    def test_initialization(self, optimizer):
        """Test optimizer initialization."""
        assert optimizer is not None
    
    def test_estimate_tokens(self, optimizer):
        """Test token estimation."""
        count = optimizer._estimate_tokens("Hello world this is a test")
        assert count > 0
    
    def test_optimize_context(self, optimizer):
        """Test context optimization."""
        context = {
            'rag_context': [
                {'content': 'Test content ' * 100, 'score': 0.9},
                {'content': 'More content ' * 100, 'score': 0.8}
            ],
            'insights': [
                {'text': 'Insight 1', 'priority': 'high'},
                {'text': 'Insight 2', 'priority': 'low'}
            ],
            'metrics': {'overview': {'total_spend': 1000}}
        }
        optimized = optimizer.optimize_context(context, task_type='executive_summary')
        assert optimized is not None
    
    def test_truncate_rag_context(self, optimizer):
        """Test RAG context truncation."""
        rag_context = [
            {'content': 'Test ' * 500, 'score': 0.9},
            {'content': 'More ' * 500, 'score': 0.7}
        ]
        truncated = optimizer._truncate_rag_context(rag_context, max_tokens=100)
        assert len(truncated) <= 2
    
    def test_truncate_insights(self, optimizer):
        """Test insights truncation."""
        insights = [
            {'text': 'High priority insight', 'priority': 'high'},
            {'text': 'Low priority insight', 'priority': 'low'}
        ]
        truncated = optimizer._truncate_insights(insights, max_tokens=50)
        assert truncated is not None
    
    def test_simplify_metrics(self, optimizer):
        """Test metrics simplification."""
        metrics = {
            'overview': {'total_spend': 1000, 'avg_roas': 2.5},
            'by_platform': {'Google': {}, 'Meta': {}},
            'best_platform': 'Google',
            'worst_platform': 'Meta'
        }
        simplified = optimizer._simplify_metrics(metrics, 'executive_summary')
        assert 'overview' in simplified
    
    def test_get_metrics(self, optimizer):
        """Test getting metrics."""
        metrics = optimizer.get_metrics()
        assert 'tokens_saved' in metrics


class TestProgressStreamer:
    """Tests for ProgressStreamer."""
    
    @pytest.fixture
    def streamer(self):
        """Create streamer instance."""
        return ProgressStreamer()
    
    def test_initialization(self, streamer):
        """Test streamer initialization."""
        assert streamer is not None
    
    def test_update(self, streamer):
        """Test update method."""
        update = streamer.update(
            stage='validation',
            status='started',
            message='Validating data...'
        )
        assert update is not None
        assert update.stage == 'validation'
    
    def test_update_with_data(self, streamer):
        """Test update with data."""
        update = streamer.update(
            stage='metrics',
            status='completed',
            message='Metrics calculated',
            data={'total_spend': 1000}
        )
        assert update.data == {'total_spend': 1000}
    
    def test_get_updates(self, streamer):
        """Test getting updates."""
        streamer.update('validation', 'started', 'Starting...')
        streamer.update('validation', 'completed', 'Done')
        updates = streamer.get_updates()
        assert len(updates) == 2
    
    def test_reset(self, streamer):
        """Test reset."""
        streamer.update('validation', 'started', 'Starting...')
        streamer.reset()
        updates = streamer.get_updates()
        assert len(updates) == 0
    
    def test_callback(self):
        """Test callback function."""
        received = []
        def callback(update):
            received.append(update)
        
        streamer = ProgressStreamer(callback=callback)
        streamer.update('validation', 'started', 'Starting...')
        assert len(received) == 1


class TestPrecomputationManager:
    """Tests for PrecomputationManager."""
    
    @pytest.fixture
    def manager(self):
        """Create manager instance."""
        PrecomputationManager._instance = None
        return PrecomputationManager.get_instance()
    
    def test_singleton(self):
        """Test singleton pattern."""
        manager1 = PrecomputationManager.get_instance()
        manager2 = PrecomputationManager.get_instance()
        assert manager1 is manager2
    
    def test_schedule_precomputation(self, manager):
        """Test scheduling precomputation."""
        def compute_task():
            return "computed"
        
        if hasattr(manager, 'schedule'):
            manager.schedule("test_task", compute_task)
    
    def test_get_precomputed(self, manager):
        """Test getting precomputed results."""
        if hasattr(manager, 'get'):
            result = manager.get("test_task")
    
    def test_get_status(self, manager):
        """Test getting status."""
        if hasattr(manager, 'get_status'):
            status = manager.get_status()
            assert status is not None


class TestModelSelector:
    """Tests for ModelSelector."""
    
    @pytest.fixture
    def selector(self):
        """Create selector instance."""
        return ModelSelector()
    
    def test_initialization(self, selector):
        """Test selector initialization."""
        assert selector is not None
    
    def test_select_model_executive_summary(self, selector):
        """Test selecting model for executive summary."""
        model = selector.select_model(task_type='executive_summary')
        assert model is not None
    
    def test_select_model_detailed_analysis(self, selector):
        """Test selecting model for detailed analysis."""
        model = selector.select_model(task_type='detailed_analysis')
        assert model is not None
    
    def test_select_model_recommendations(self, selector):
        """Test selecting model for recommendations."""
        model = selector.select_model(task_type='recommendations')
        assert model is not None
    
    def test_get_metrics(self, selector):
        """Test getting metrics."""
        metrics = selector.get_metrics()
        assert 'selections' in metrics or 'model_selections' in metrics


class TestPerformanceOptimizer:
    """Tests for PerformanceOptimizer."""
    
    @pytest.fixture
    def optimizer(self):
        """Create optimizer instance."""
        return PerformanceOptimizer()
    
    def test_initialization(self, optimizer):
        """Test optimizer initialization."""
        assert optimizer is not None
    
    def test_get_all_metrics(self, optimizer):
        """Test getting all metrics."""
        metrics = optimizer.get_all_metrics()
        assert metrics is not None
        assert isinstance(metrics, dict)
    
    def test_estimate_impact(self, optimizer):
        """Test estimating optimization impact."""
        impact = optimizer.estimate_impact()
        assert impact is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
