"""
Database Query Profiling and Optimization

Provides tools for:
- Query performance profiling
- Slow query detection
- Index usage analysis
- Query optimization recommendations
"""

import time
import functools
from typing import Callable, Any, Dict, List, Optional
from contextlib import contextmanager
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path
import json

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from loguru import logger


@dataclass
class QueryProfile:
    """Profile data for a single query."""
    
    query: str
    duration_ms: float
    timestamp: datetime
    rows_returned: int
    query_type: str  # 'SELECT', 'INSERT', 'UPDATE', 'DELETE'
    table_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            **asdict(self),
            'timestamp': self.timestamp.isoformat()
        }
    
    def is_slow(self, threshold_ms: float = 1000.0) -> bool:
        """Check if query is slow."""
        return self.duration_ms >= threshold_ms


class QueryProfiler:
    """
    Profile database queries and detect slow queries.
    
    Usage:
        profiler = QueryProfiler()
        profiler.enable()
        
        # Run queries...
        
        slow_queries = profiler.get_slow_queries()
        profiler.save_report('query_profile.json')
    """
    
    def __init__(self, slow_query_threshold_ms: float = 1000.0):
        """
        Initialize query profiler.
        
        Args:
            slow_query_threshold_ms: Threshold for slow query detection (default: 1000ms)
        """
        self.slow_query_threshold_ms = slow_query_threshold_ms
        self.profiles: List[QueryProfile] = []
        self._enabled = False
    
    def enable(self, engine: Engine) -> None:
        """
        Enable query profiling on SQLAlchemy engine.
        
        Args:
            engine: SQLAlchemy engine to profile
        """
        if self._enabled:
            return
        
        @event.listens_for(engine, "before_cursor_execute")
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            conn.info.setdefault('query_start_time', []).append(time.perf_counter())
        
        @event.listens_for(engine, "after_cursor_execute")
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
            start_time = conn.info['query_start_time'].pop()
            duration_ms = (time.perf_counter() - start_time) * 1000
            
            # Extract query type
            query_type = statement.strip().split()[0].upper()
            
            # Extract table name (simple heuristic)
            table_name = None
            if 'FROM' in statement.upper():
                parts = statement.upper().split('FROM')[1].split()
                if parts:
                    table_name = parts[0].strip('()')
            
            # Create profile
            profile = QueryProfile(
                query=statement[:500],  # Truncate long queries
                duration_ms=duration_ms,
                timestamp=datetime.utcnow(),
                rows_returned=cursor.rowcount if cursor.rowcount >= 0 else 0,
                query_type=query_type,
                table_name=table_name
            )
            
            self.profiles.append(profile)
            
            # Log slow queries
            if profile.is_slow(self.slow_query_threshold_ms):
                logger.warning(
                    f"Slow query detected ({duration_ms:.2f}ms): {statement[:100]}..."
                )
        
        self._enabled = True
        logger.info(f"Query profiler enabled (threshold: {self.slow_query_threshold_ms}ms)")
    
    def disable(self) -> None:
        """Disable query profiling."""
        self._enabled = False
        logger.info("Query profiler disabled")
    
    def get_slow_queries(self, threshold_ms: Optional[float] = None) -> List[QueryProfile]:
        """
        Get all slow queries.
        
        Args:
            threshold_ms: Custom threshold (uses default if not provided)
            
        Returns:
            List of slow query profiles
        """
        threshold = threshold_ms or self.slow_query_threshold_ms
        return [p for p in self.profiles if p.duration_ms >= threshold]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get profiling statistics."""
        if not self.profiles:
            return {
                'total_queries': 0,
                'slow_queries': 0,
                'avg_duration_ms': 0,
                'max_duration_ms': 0,
                'min_duration_ms': 0
            }
        
        durations = [p.duration_ms for p in self.profiles]
        slow_count = len(self.get_slow_queries())
        
        # Query type breakdown
        query_types = {}
        for profile in self.profiles:
            query_types[profile.query_type] = query_types.get(profile.query_type, 0) + 1
        
        return {
            'total_queries': len(self.profiles),
            'slow_queries': slow_count,
            'slow_query_rate': slow_count / len(self.profiles) if self.profiles else 0,
            'avg_duration_ms': sum(durations) / len(durations),
            'max_duration_ms': max(durations),
            'min_duration_ms': min(durations),
            'query_types': query_types,
            'threshold_ms': self.slow_query_threshold_ms
        }
    
    def save_report(self, output_path: Path) -> None:
        """
        Save profiling report to JSON.
        
        Args:
            output_path: Path to save report
        """
        report = {
            'summary': self.get_stats(),
            'slow_queries': [p.to_dict() for p in self.get_slow_queries()],
            'all_queries': [p.to_dict() for p in self.profiles]
        }
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2), encoding='utf-8')
        
        logger.info(f"Query profile report saved to: {output_path}")
    
    def clear(self) -> None:
        """Clear all profiles."""
        self.profiles.clear()


def profile_query(threshold_ms: float = 1000.0):
    """
    Decorator to profile individual query functions.
    
    Usage:
        @profile_query(threshold_ms=500)
        def get_campaigns(session, filters):
            return session.query(Campaign).filter_by(**filters).all()
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start) * 1000
                
                if duration_ms >= threshold_ms:
                    logger.warning(
                        f"Slow query in {func.__name__}: {duration_ms:.2f}ms"
                    )
                else:
                    logger.debug(
                        f"Query {func.__name__} completed in {duration_ms:.2f}ms"
                    )
                
                return result
            except Exception as e:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.error(
                    f"Query {func.__name__} failed after {duration_ms:.2f}ms: {e}"
                )
                raise
        
        return wrapper
    return decorator


@contextmanager
def profile_queries(engine: Engine, threshold_ms: float = 1000.0):
    """
    Context manager for profiling queries.
    
    Usage:
        with profile_queries(engine, threshold_ms=500) as profiler:
            # Run queries...
            pass
        
        # Get results
        slow_queries = profiler.get_slow_queries()
    """
    profiler = QueryProfiler(threshold_ms)
    profiler.enable(engine)
    
    try:
        yield profiler
    finally:
        profiler.disable()


class IndexAnalyzer:
    """Analyze index usage and recommend optimizations."""
    
    def __init__(self, engine: Engine):
        """
        Initialize index analyzer.
        
        Args:
            engine: SQLAlchemy engine
        """
        self.engine = engine
    
    def get_table_indexes(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get all indexes for a table.
        
        Args:
            table_name: Name of table
            
        Returns:
            List of index information
        """
        query = text("""
            SELECT
                indexname as index_name,
                indexdef as definition
            FROM pg_indexes
            WHERE tablename = :table_name
            ORDER BY indexname
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {'table_name': table_name})
            return [dict(row) for row in result]
    
    def get_index_usage_stats(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get index usage statistics.
        
        Args:
            table_name: Name of table
            
        Returns:
            List of index usage stats
        """
        query = text("""
            SELECT
                schemaname,
                tablename,
                indexname,
                idx_scan as scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched
            FROM pg_stat_user_indexes
            WHERE tablename = :table_name
            ORDER BY idx_scan DESC
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {'table_name': table_name})
            return [dict(row) for row in result]
    
    def get_unused_indexes(self, min_scans: int = 10) -> List[Dict[str, Any]]:
        """
        Find indexes that are rarely used.
        
        Args:
            min_scans: Minimum number of scans to consider index as used
            
        Returns:
            List of unused indexes
        """
        query = text("""
            SELECT
                schemaname,
                tablename,
                indexname,
                idx_scan as scans,
                pg_size_pretty(pg_relation_size(indexrelid)) as size
            FROM pg_stat_user_indexes
            WHERE idx_scan < :min_scans
            AND indexname NOT LIKE '%_pkey'  -- Exclude primary keys
            ORDER BY pg_relation_size(indexrelid) DESC
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(query, {'min_scans': min_scans})
            return [dict(row) for row in result]
    
    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """
        Comprehensive index analysis for a table.
        
        Args:
            table_name: Name of table
            
        Returns:
            Analysis results with recommendations
        """
        indexes = self.get_table_indexes(table_name)
        usage_stats = self.get_index_usage_stats(table_name)
        
        # Match indexes with usage stats
        for idx in indexes:
            idx_name = idx['index_name']
            stats = next((s for s in usage_stats if s['indexname'] == idx_name), None)
            if stats:
                idx['scans'] = stats['scans']
                idx['tuples_read'] = stats['tuples_read']
                idx['tuples_fetched'] = stats['tuples_fetched']
            else:
                idx['scans'] = 0
                idx['tuples_read'] = 0
                idx['tuples_fetched'] = 0
        
        # Generate recommendations
        recommendations = []
        for idx in indexes:
            if idx['scans'] == 0 and not idx['index_name'].endswith('_pkey'):
                recommendations.append(
                    f"Consider dropping unused index: {idx['index_name']}"
                )
        
        return {
            'table_name': table_name,
            'total_indexes': len(indexes),
            'indexes': indexes,
            'recommendations': recommendations
        }
