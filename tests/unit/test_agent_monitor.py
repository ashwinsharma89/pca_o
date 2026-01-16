"""
Unit tests for Agent Monitor.
Tests agent metrics tracking, monitoring, and dashboard generation.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import time
from collections import deque


class TestAgentMetrics:
    """Tests for AgentMetrics dataclass."""
    
    def test_metrics_initialization(self):
        """Test AgentMetrics initialization."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test_agent")
        
        assert metrics.agent_name == "test_agent"
        assert metrics.total_calls == 0
        assert metrics.successful_calls == 0
        assert metrics.failed_calls == 0
        assert metrics.total_execution_time == 0.0
        assert metrics.min_execution_time == float('inf')
        assert metrics.max_execution_time == 0.0
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test")
        metrics.total_calls = 100
        metrics.successful_calls = 95
        
        assert metrics.success_rate == 95.0
    
    def test_success_rate_zero_calls(self):
        """Test success rate with zero calls."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test")
        
        assert metrics.success_rate == 0.0
    
    def test_avg_execution_time(self):
        """Test average execution time calculation."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test")
        metrics.total_calls = 10
        metrics.total_execution_time = 5.0
        
        assert metrics.avg_execution_time == 0.5
    
    def test_recent_avg_execution_time(self):
        """Test recent average execution time."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test")
        metrics.recent_execution_times.extend([0.1, 0.2, 0.3, 0.4, 0.5])
        
        assert metrics.recent_avg_execution_time == pytest.approx(0.3)
    
    def test_avg_accuracy(self):
        """Test average accuracy calculation."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test")
        metrics.accuracy_scores.extend([90, 95, 85, 100])
        
        assert metrics.avg_accuracy == pytest.approx(92.5)
    
    def test_status_healthy(self):
        """Test healthy status."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test")
        metrics.total_calls = 100
        metrics.successful_calls = 98
        
        assert metrics.status == "healthy"
    
    def test_status_degraded(self):
        """Test degraded status."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test")
        metrics.total_calls = 100
        metrics.successful_calls = 92
        
        assert metrics.status == "degraded"
    
    def test_status_unhealthy(self):
        """Test unhealthy status."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test")
        metrics.total_calls = 100
        metrics.successful_calls = 80
        
        assert metrics.status == "unhealthy"
    
    def test_status_unknown(self):
        """Test unknown status with no calls."""
        from src.core.utils.agent_monitor import AgentMetrics
        
        metrics = AgentMetrics(agent_name="test")
        
        assert metrics.status == "unknown"


class TestAgentMonitor:
    """Tests for AgentMonitor class."""
    
    def test_monitor_initialization(self):
        """Test AgentMonitor initialization."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        
        assert monitor.metrics == {}
        assert monitor.lock is not None
    
    def test_record_successful_execution(self):
        """Test recording successful execution."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        monitor.record_execution(
            agent_name="test_agent",
            execution_time=0.5,
            success=True
        )
        
        metrics = monitor.get_metrics("test_agent")
        assert metrics.total_calls == 1
        assert metrics.successful_calls == 1
        assert metrics.failed_calls == 0
        assert metrics.total_execution_time == 0.5
    
    def test_record_failed_execution(self):
        """Test recording failed execution."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        monitor.record_execution(
            agent_name="test_agent",
            execution_time=0.3,
            success=False,
            error_message="Test error"
        )
        
        metrics = monitor.get_metrics("test_agent")
        assert metrics.total_calls == 1
        assert metrics.successful_calls == 0
        assert metrics.failed_calls == 1
        assert len(metrics.error_messages) == 1
    
    def test_record_with_accuracy(self):
        """Test recording execution with accuracy score."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        monitor.record_execution(
            agent_name="test_agent",
            execution_time=0.5,
            success=True,
            accuracy_score=95.0
        )
        
        metrics = monitor.get_metrics("test_agent")
        assert len(metrics.accuracy_scores) == 1
        assert metrics.accuracy_scores[0] == 95.0
    
    def test_min_max_execution_time(self):
        """Test min/max execution time tracking."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        monitor.record_execution("agent", 0.5, True)
        monitor.record_execution("agent", 0.2, True)
        monitor.record_execution("agent", 0.8, True)
        
        metrics = monitor.get_metrics("agent")
        assert metrics.min_execution_time == 0.2
        assert metrics.max_execution_time == 0.8
    
    def test_get_all_metrics(self):
        """Test getting all metrics."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        monitor.record_execution("agent1", 0.5, True)
        monitor.record_execution("agent2", 0.3, True)
        
        all_metrics = monitor.get_all_metrics()
        
        assert "agent1" in all_metrics
        assert "agent2" in all_metrics
        assert all_metrics["agent1"]["total_calls"] == 1
    
    def test_get_dashboard(self):
        """Test dashboard generation."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        monitor.record_execution("vision_agent", 0.5, True)
        monitor.record_execution("reasoning_agent", 0.3, True)
        
        dashboard = monitor.get_dashboard()
        
        assert "Agent Performance Dashboard" in dashboard
        assert "vision_agent" in dashboard
        assert "reasoning_agent" in dashboard
    
    def test_get_dashboard_empty(self):
        """Test dashboard with no metrics."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        dashboard = monitor.get_dashboard()
        
        assert "No agent metrics available" in dashboard
    
    def test_reset_specific_agent(self):
        """Test resetting specific agent metrics."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        monitor.record_execution("agent1", 0.5, True)
        monitor.record_execution("agent2", 0.3, True)
        
        monitor.reset_metrics("agent1")
        
        assert monitor.get_metrics("agent1") is None
        assert monitor.get_metrics("agent2") is not None
    
    def test_reset_all_agents(self):
        """Test resetting all agent metrics."""
        from src.core.utils.agent_monitor import AgentMonitor
        
        monitor = AgentMonitor()
        monitor.record_execution("agent1", 0.5, True)
        monitor.record_execution("agent2", 0.3, True)
        
        monitor.reset_metrics()
        
        assert len(monitor.metrics) == 0


class TestGlobalMonitor:
    """Tests for global monitor functions."""
    
    def test_get_agent_monitor(self):
        """Test getting global monitor instance."""
        from src.core.utils.agent_monitor import get_agent_monitor
        
        monitor1 = get_agent_monitor()
        monitor2 = get_agent_monitor()
        
        # Should return same instance
        assert monitor1 is monitor2
    
    def test_record_agent_accuracy(self):
        """Test recording agent accuracy."""
        from src.core.utils.agent_monitor import get_agent_monitor, record_agent_accuracy
        
        monitor = get_agent_monitor()
        monitor.record_execution("test_agent", 0.5, True)
        
        record_agent_accuracy("test_agent", 92.5)
        
        metrics = monitor.get_metrics("test_agent")
        assert 92.5 in metrics.accuracy_scores


class TestMonitorDecorator:
    """Tests for monitor_agent decorator."""
    
    def test_decorator_tracks_success(self):
        """Test decorator tracks successful execution."""
        from src.core.utils.agent_monitor import monitor_agent, get_agent_monitor
        
        @monitor_agent("decorated_agent")
        def sample_function():
            return "result"
        
        result = sample_function()
        
        assert result == "result"
        
        monitor = get_agent_monitor()
        metrics = monitor.get_metrics("decorated_agent")
        assert metrics is not None
        assert metrics.successful_calls >= 1
    
    def test_decorator_tracks_failure(self):
        """Test decorator tracks failed execution."""
        from src.core.utils.agent_monitor import monitor_agent, get_agent_monitor
        
        @monitor_agent("failing_agent")
        def failing_function():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError):
            failing_function()
        
        monitor = get_agent_monitor()
        metrics = monitor.get_metrics("failing_agent")
        assert metrics is not None
        assert metrics.failed_calls >= 1
    
    def test_decorator_uses_function_name(self):
        """Test decorator uses function name if not specified."""
        from src.core.utils.agent_monitor import monitor_agent, get_agent_monitor
        
        @monitor_agent()
        def my_agent_function():
            return True
        
        my_agent_function()
        
        monitor = get_agent_monitor()
        metrics = monitor.get_metrics("my_agent_function")
        assert metrics is not None
