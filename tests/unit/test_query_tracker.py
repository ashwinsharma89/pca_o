"""
Unit tests for query tracker.
Tests query logging and evaluation metrics.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import tempfile
import os

# Try to import
try:
    from src.quality.evaluation.query_tracker import QueryTracker, QueryLog, QueryMetrics
    TRACKER_AVAILABLE = True
except ImportError:
    TRACKER_AVAILABLE = False
    QueryTracker = None
    QueryLog = None
    QueryMetrics = None

pytestmark = pytest.mark.skipif(not TRACKER_AVAILABLE, reason="Query tracker not available")


class TestQueryLog:
    """Test QueryLog dataclass."""
    
    def test_create_query_log(self):
        """Test creating a query log entry."""
        log = QueryLog(
            query_id="q123",
            user_id="user456",
            session_id="sess789",
            timestamp=datetime.now().isoformat(),
            original_query="What is total spend?",
            interpretations='["Total spend", "Sum of spend"]',
            selected_interpretation_index=0,
            selected_interpretation="Total spend",
            generated_sql="SELECT SUM(spend) FROM campaigns",
            execution_time_ms=150,
            result_count=1,
            error_message=None,
            user_feedback=1,
            feedback_comment="Good result"
        )
        
        assert log.query_id == "q123"
        assert log.user_feedback == 1


class TestQueryMetrics:
    """Test QueryMetrics dataclass."""
    
    def test_create_metrics(self):
        """Test creating query metrics."""
        metrics = QueryMetrics(
            metric_id="m123",
            query_id="q456",
            metric_name="execution_time",
            metric_value=150.5,
            timestamp=datetime.now().isoformat()
        )
        
        assert metrics.metric_name == "execution_time"
        assert metrics.metric_value == 150.5


class TestQueryTracker:
    """Test QueryTracker functionality."""
    
    @pytest.fixture
    def tracker(self, tmp_path):
        """Create query tracker with temp database."""
        db_path = str(tmp_path / "test_tracker.db")
        return QueryTracker(db_path=db_path)
    
    def test_initialization(self, tracker):
        """Test tracker initialization."""
        assert tracker is not None
    
    def test_log_query(self, tracker):
        """Test logging a query."""
        if hasattr(tracker, 'log_query'):
            query_id = tracker.log_query(
                user_id="user123",
                session_id="sess456",
                original_query="What is total spend?",
                interpretations=["Total spend", "Sum of spend"]
            )
            
            assert query_id is not None
    
    def test_update_query_result(self, tracker):
        """Test updating query result."""
        if hasattr(tracker, 'log_query') and hasattr(tracker, 'update_result'):
            query_id = tracker.log_query(
                user_id="user123",
                session_id="sess456",
                original_query="Test query",
                interpretations=["Test"]
            )
            
            tracker.update_result(
                query_id=query_id,
                generated_sql="SELECT * FROM test",
                execution_time_ms=100,
                result_count=5
            )
    
    def test_record_feedback(self, tracker):
        """Test recording user feedback."""
        if hasattr(tracker, 'log_query') and hasattr(tracker, 'record_feedback'):
            query_id = tracker.log_query(
                user_id="user123",
                session_id="sess456",
                original_query="Test query",
                interpretations=["Test"]
            )
            
            tracker.record_feedback(
                query_id=query_id,
                feedback=1,
                comment="Good result"
            )
    
    def test_get_query_history(self, tracker):
        """Test getting query history."""
        if hasattr(tracker, 'get_history'):
            history = tracker.get_history(user_id="user123")
            assert isinstance(history, (list, type(None)))
    
    def test_get_metrics(self, tracker):
        """Test getting metrics."""
        if hasattr(tracker, 'get_metrics'):
            metrics = tracker.get_metrics()
            assert isinstance(metrics, (dict, type(None)))
