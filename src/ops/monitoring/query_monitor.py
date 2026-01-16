"""
Query performance monitoring.
Tracks database query performance and identifies slow queries.
"""

import time
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from functools import wraps
from contextlib import contextmanager
import threading
from collections import deque

logger = logging.getLogger(__name__)


class QueryMetrics:
    """Metrics for a single query."""
    
    def __init__(
        self,
        query_type: str,
        query: str,
        duration: float,
        timestamp: datetime,
        success: bool = True,
        error: Optional[str] = None
    ):
        self.query_type = query_type
        self.query = query
        self.duration = duration
        self.timestamp = timestamp
        self.success = success
        self.error = error


class QueryMonitor:
    """
    Monitor database query performance.
    Tracks slow queries, query counts, and performance metrics.
    """
    
    def __init__(self, slow_query_threshold: float = 1.0, max_history: int = 1000):
        """
        Initialize query monitor.
        
        Args:
            slow_query_threshold: Threshold in seconds for slow queries
            max_history: Maximum number of queries to keep in history
        """
        self.slow_query_threshold = slow_query_threshold
        self.max_history = max_history
        
        # Thread-safe query history
        self._lock = threading.Lock()
        self._query_history = deque(maxlen=max_history)
        self._slow_queries = deque(maxlen=100)
        
        # Metrics
        self._total_queries = 0
        self._total_duration = 0.0
        self._failed_queries = 0
        self._query_counts_by_type = {}
    
    def record_query(
        self,
        query_type: str,
        query: str,
        duration: float,
        success: bool = True,
        error: Optional[str] = None
    ):
        """
        Record a query execution.
        
        Args:
            query_type: Type of query (e.g., 'SELECT', 'INSERT', 'UPDATE')
            query: Query string
            duration: Execution duration in seconds
            success: Whether query succeeded
            error: Error message if failed
        """
        metrics = QueryMetrics(
            query_type=query_type,
            query=query,
            duration=duration,
            timestamp=datetime.now(),
            success=success,
            error=error
        )
        
        with self._lock:
            # Add to history
            self._query_history.append(metrics)
            
            # Track slow queries
            if duration >= self.slow_query_threshold:
                self._slow_queries.append(metrics)
                logger.warning(
                    f"Slow query detected ({duration:.2f}s): {query_type} - {query[:100]}"
                )
            
            # Update metrics
            self._total_queries += 1
            self._total_duration += duration
            
            if not success:
                self._failed_queries += 1
            
            # Count by type
            self._query_counts_by_type[query_type] = \
                self._query_counts_by_type.get(query_type, 0) + 1
    
    @contextmanager
    def track_query(self, query_type: str, query: str):
        """
        Context manager to track query execution.
        
        Usage:
            with monitor.track_query('SELECT', 'SELECT * FROM campaigns'):
                # Execute query
                pass
        """
        start_time = time.time()
        success = True
        error = None
        
        try:
            yield
        except Exception as e:
            success = False
            error = str(e)
            raise
        finally:
            duration = time.time() - start_time
            self.record_query(query_type, query, duration, success, error)
    
    def get_stats(self, time_window: Optional[timedelta] = None) -> Dict[str, Any]:
        """
        Get query statistics.
        
        Args:
            time_window: Optional time window to filter queries
            
        Returns:
            Dictionary with statistics
        """
        with self._lock:
            # Filter by time window if specified
            if time_window:
                cutoff = datetime.now() - time_window
                queries = [q for q in self._query_history if q.timestamp >= cutoff]
            else:
                queries = list(self._query_history)
            
            if not queries:
                return {
                    'total_queries': 0,
                    'avg_duration': 0.0,
                    'min_duration': 0.0,
                    'max_duration': 0.0,
                    'slow_queries': 0,
                    'failed_queries': 0,
                    'success_rate': 100.0,
                    'queries_by_type': {}
                }
            
            durations = [q.duration for q in queries]
            failed = sum(1 for q in queries if not q.success)
            slow = sum(1 for q in queries if q.duration >= self.slow_query_threshold)
            
            # Count by type
            by_type = {}
            for q in queries:
                by_type[q.query_type] = by_type.get(q.query_type, 0) + 1
            
            return {
                'total_queries': len(queries),
                'avg_duration': sum(durations) / len(durations),
                'min_duration': min(durations),
                'max_duration': max(durations),
                'p95_duration': self._percentile(durations, 95),
                'p99_duration': self._percentile(durations, 99),
                'slow_queries': slow,
                'failed_queries': failed,
                'success_rate': ((len(queries) - failed) / len(queries)) * 100,
                'queries_by_type': by_type,
                'time_window': str(time_window) if time_window else 'all'
            }
    
    def get_slow_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent slow queries.
        
        Args:
            limit: Maximum number of queries to return
            
        Returns:
            List of slow query dictionaries
        """
        with self._lock:
            slow_queries = list(self._slow_queries)[-limit:]
            
            return [
                {
                    'query_type': q.query_type,
                    'query': q.query[:200],  # Truncate long queries
                    'duration': q.duration,
                    'timestamp': q.timestamp.isoformat(),
                    'success': q.success,
                    'error': q.error
                }
                for q in reversed(slow_queries)
            ]
    
    def get_query_breakdown(self) -> Dict[str, Dict[str, Any]]:
        """Get breakdown of queries by type."""
        with self._lock:
            breakdown = {}
            
            for query_type in self._query_counts_by_type:
                type_queries = [q for q in self._query_history if q.query_type == query_type]
                
                if type_queries:
                    durations = [q.duration for q in type_queries]
                    failed = sum(1 for q in type_queries if not q.success)
                    
                    breakdown[query_type] = {
                        'count': len(type_queries),
                        'avg_duration': sum(durations) / len(durations),
                        'total_duration': sum(durations),
                        'failed': failed,
                        'success_rate': ((len(type_queries) - failed) / len(type_queries)) * 100
                    }
            
            return breakdown
    
    def clear_history(self):
        """Clear query history."""
        with self._lock:
            self._query_history.clear()
            self._slow_queries.clear()
            self._total_queries = 0
            self._total_duration = 0.0
            self._failed_queries = 0
            self._query_counts_by_type.clear()
    
    @staticmethod
    def _percentile(data: List[float], percentile: int) -> float:
        """Calculate percentile."""
        if not data:
            return 0.0
        
        sorted_data = sorted(data)
        index = int((percentile / 100) * len(sorted_data))
        
        if index >= len(sorted_data):
            index = len(sorted_data) - 1
        
        return sorted_data[index]


# Global monitor instance
_monitor: Optional[QueryMonitor] = None


def get_query_monitor() -> QueryMonitor:
    """Get or create global query monitor."""
    global _monitor
    if _monitor is None:
        _monitor = QueryMonitor()
    return _monitor


def track_query(query_type: str, query: str):
    """
    Decorator to track query execution.
    
    Example:
        @track_query('SELECT', 'SELECT * FROM campaigns')
        def get_campaigns():
            # Execute query
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            monitor = get_query_monitor()
            
            with monitor.track_query(query_type, query):
                return func(*args, **kwargs)
        
        return wrapper
    return decorator
