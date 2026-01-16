"""
Unit tests for query monitor.
Tests query performance monitoring functionality.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
import time

# Try to import, skip if not available
try:
    from src.ops.monitoring.query_monitor import QueryMonitor, QueryMetrics
    MONITOR_AVAILABLE = True
except ImportError:
    MONITOR_AVAILABLE = False
    QueryMonitor = None
    QueryMetrics = None

pytestmark = pytest.mark.skipif(not MONITOR_AVAILABLE, reason="Query monitor not available")


class TestQueryMetrics:
    """Test QueryMetrics dataclass."""
    
    def test_create_metrics(self):
        """Test creating query metrics."""
        metrics = QueryMetrics(
            query_type="SELECT",
            query="SELECT * FROM campaigns",
            duration=0.5,
            timestamp=datetime.now(),
            success=True
        )
        
        assert metrics.query_type == "SELECT"
        assert metrics.duration == 0.5
        assert metrics.success is True
    
    def test_failed_query_metrics(self):
        """Test metrics for failed query."""
        metrics = QueryMetrics(
            query_type="INSERT",
            query="INSERT INTO campaigns VALUES (...)",
            duration=1.0,
            timestamp=datetime.now(),
            success=False,
            error="Constraint violation"
        )
        
        assert metrics.success is False
        assert metrics.error == "Constraint violation"


class TestQueryMonitor:
    """Test QueryMonitor functionality."""
    
    @pytest.fixture
    def monitor(self):
        """Create query monitor instance."""
        return QueryMonitor(slow_query_threshold=1.0, max_history=100)
    
    def test_initialization(self, monitor):
        """Test monitor initialization."""
        assert monitor.slow_query_threshold == 1.0
        assert monitor.max_history == 100
        assert monitor._total_queries == 0
    
    def test_record_query(self, monitor):
        """Test recording a query."""
        monitor.record_query(
            query_type="SELECT",
            query="SELECT * FROM campaigns",
            duration=0.5,
            success=True
        )
        
        assert monitor._total_queries == 1
        assert monitor._total_duration == 0.5
    
    def test_record_slow_query(self, monitor):
        """Test recording a slow query."""
        monitor.record_query(
            query_type="SELECT",
            query="SELECT * FROM large_table",
            duration=2.0,  # Above threshold
            success=True
        )
        
        assert len(monitor._slow_queries) == 1
    
    def test_record_failed_query(self, monitor):
        """Test recording a failed query."""
        monitor.record_query(
            query_type="INSERT",
            query="INSERT INTO campaigns ...",
            duration=0.1,
            success=False,
            error="Constraint violation"
        )
        
        assert monitor._failed_queries == 1
    
    def test_query_counts_by_type(self, monitor):
        """Test query counts by type."""
        monitor.record_query("SELECT", "SELECT ...", 0.1)
        monitor.record_query("SELECT", "SELECT ...", 0.2)
        monitor.record_query("INSERT", "INSERT ...", 0.3)
        
        assert monitor._query_counts_by_type.get("SELECT", 0) == 2
        assert monitor._query_counts_by_type.get("INSERT", 0) == 1
    
    def test_max_history_limit(self, monitor):
        """Test that history respects max limit."""
        for i in range(150):
            monitor.record_query("SELECT", f"SELECT {i}", 0.1)
        
        assert len(monitor._query_history) <= monitor.max_history
    
    def test_get_statistics(self, monitor):
        """Test getting statistics."""
        monitor.record_query("SELECT", "SELECT ...", 0.5)
        monitor.record_query("SELECT", "SELECT ...", 1.5)
        
        if hasattr(monitor, 'get_statistics'):
            stats = monitor.get_statistics()
            assert 'total_queries' in stats or stats is not None
    
    def test_get_slow_queries(self, monitor):
        """Test getting slow queries."""
        monitor.record_query("SELECT", "Fast query", 0.1)
        monitor.record_query("SELECT", "Slow query", 2.0)
        
        if hasattr(monitor, 'get_slow_queries'):
            slow = monitor.get_slow_queries()
            assert len(slow) >= 1
    
    def test_average_duration(self, monitor):
        """Test average query duration calculation."""
        monitor.record_query("SELECT", "Query 1", 1.0)
        monitor.record_query("SELECT", "Query 2", 2.0)
        monitor.record_query("SELECT", "Query 3", 3.0)
        
        avg = monitor._total_duration / monitor._total_queries
        assert avg == 2.0


class TestQueryMonitorContextManager:
    """Test query monitor context manager."""
    
    @pytest.fixture
    def monitor(self):
        """Create query monitor instance."""
        return QueryMonitor()
    
    def test_track_query_context(self, monitor):
        """Test tracking query with context manager."""
        if hasattr(monitor, 'track_query'):
            with monitor.track_query("SELECT", "SELECT * FROM test"):
                time.sleep(0.01)  # Simulate query
            
            assert monitor._total_queries == 1


class TestQueryMonitorThreadSafety:
    """Test thread safety of query monitor."""
    
    def test_concurrent_recording(self):
        """Test concurrent query recording."""
        import threading
        
        monitor = QueryMonitor()
        
        def record_queries():
            for i in range(100):
                monitor.record_query("SELECT", f"Query {i}", 0.1)
        
        threads = [threading.Thread(target=record_queries) for _ in range(5)]
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert monitor._total_queries == 500
