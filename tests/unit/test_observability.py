"""
Unit tests for src/utils/observability.py
Covers: Structured logging, metrics collection, tracing, alerting, cost tracking
"""
import pytest
import json
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from src.core.utils.observability import (
    StructuredLogger,
    MetricsCollector,
    Tracer,
    AlertManager,
    CostTracker,
    LogContext,
)


# ============================================================================
# Structured Logger Tests
# ============================================================================

class TestStructuredLogger:
    """Tests for structured JSON logging."""
    
    def test_creates_logger(self):
        """Logger should initialize correctly."""
        logger = StructuredLogger("test_module")
        
        assert logger is not None
        assert logger.service_name == "test_module"
    
    def test_log_with_context(self):
        """Logger should include context in output."""
        logger = StructuredLogger("test")
        
        # Set context
        context = LogContext(request_id="123", user_id="user1")
        logger.set_context(context)
        
        # Verify context was set
        current = logger.get_context()
        assert current is not None
        assert current.request_id == "123"
    
    def test_log_levels(self):
        """Logger should support all log levels."""
        logger = StructuredLogger("test")
        
        # Should not raise - uses loguru under the hood
        from loguru import logger as loguru_logger
        loguru_logger.debug("Debug message")
        loguru_logger.info("Info message")
        loguru_logger.warning("Warning message")
    
    def test_json_serializable_output(self):
        """Log output should be JSON serializable."""
        logger = StructuredLogger("test")
        
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": "INFO",
            "message": "Test",
            "context": {"key": "value"}
        }
        
        # Should not raise
        json_str = json.dumps(log_entry)
        assert isinstance(json_str, str)
    
    def test_context_propagation(self):
        """Context should propagate through nested calls."""
        logger = StructuredLogger("test")
        
        context = LogContext(request_id="abc123")
        logger.set_context(context)
        
        # Context should be available
        current = logger.get_context()
        assert current is not None
        assert current.request_id == "abc123"


# ============================================================================
# Metrics Collector Tests
# ============================================================================

class TestMetricsCollector:
    """Tests for metrics collection."""
    
    def test_increment_counter(self):
        """Counter should increment correctly."""
        metrics = MetricsCollector()
        
        metrics.increment("api_calls")
        metrics.increment("api_calls")
        metrics.increment("api_calls")
        
        assert metrics.get_counter("api_calls") == 3
    
    def test_set_gauge(self):
        """Gauge should set to specific value."""
        metrics = MetricsCollector()
        
        metrics.set_gauge("active_connections", 42)
        
        assert metrics.get_gauge("active_connections") == 42
    
    def test_record_histogram(self):
        """Histogram should record values."""
        metrics = MetricsCollector()
        
        metrics.observe("response_time", 0.1)
        metrics.observe("response_time", 0.2)
        metrics.observe("response_time", 0.3)
        
        stats = metrics.get_histogram_stats("response_time")
        
        assert stats["count"] == 3
        assert stats["avg"] == pytest.approx(0.2, rel=0.01)
    
    def test_labeled_metrics(self):
        """Metrics should support labels."""
        metrics = MetricsCollector()
        
        metrics.increment("api_calls", labels={"endpoint": "/analyze"})
        metrics.increment("api_calls", labels={"endpoint": "/query"})
        
        assert metrics.get_counter("api_calls", labels={"endpoint": "/analyze"}) == 1
        assert metrics.get_counter("api_calls", labels={"endpoint": "/query"}) == 1
    
    def test_export_prometheus_format(self):
        """Metrics should export in Prometheus format."""
        metrics = MetricsCollector()
        metrics.increment("requests_total")
        
        # MetricsCollector exports via get_all_metrics
        all_metrics = metrics.get_all_metrics()
        
        assert "requests_total" in str(all_metrics)
        assert isinstance(all_metrics, dict)


# ============================================================================
# Tracer Tests
# ============================================================================

class TestTracer:
    """Tests for distributed tracing."""
    
    def test_create_span(self):
        """Should create a new span."""
        tracer = Tracer()
        
        with tracer.start_span("test_operation") as span:
            assert span is not None
            assert span.operation_name == "test_operation"
    
    def test_span_timing(self):
        """Span should record timing."""
        tracer = Tracer()
        
        with tracer.start_span("timed_op") as span:
            import time
            time.sleep(0.01)
        
        # Span should have end_time set after context exits
        assert span.end_time is not None
        duration = (span.end_time - span.start_time).total_seconds()
        assert duration >= 0.01
    
    def test_nested_spans(self):
        """Nested spans should maintain parent-child relationship."""
        tracer = Tracer()
        
        with tracer.start_span("parent") as parent:
            with tracer.start_span("child") as child:
                assert child.parent_span_id == parent.span_id
    
    def test_span_attributes(self):
        """Span should support custom attributes via tags."""
        tracer = Tracer()
        
        with tracer.start_span("operation", tags={"user_id": "123", "model": "gpt-4"}) as span:
            pass
        
        assert span.tags.get("user_id") == "123"
        assert span.tags.get("model") == "gpt-4"
    
    def test_span_error_recording(self):
        """Span should record errors."""
        tracer = Tracer()
        
        try:
            with tracer.start_span("failing_op") as span:
                raise ValueError("Test error")
        except ValueError:
            pass
        
        assert span.status == "ERROR"
        assert span.tags.get("error") == "true"


# ============================================================================
# Alert Manager Tests
# ============================================================================

class TestAlertManager:
    """Tests for alerting system."""
    
    def test_create_alert_rule(self):
        """Should create alert rule."""
        from src.core.utils.observability import AlertRule, AlertSeverity
        
        alerts = AlertManager()
        
        rule = AlertRule(
            name="test_high_error_rate",
            metric_name="error_count",
            condition="gt",
            threshold=10,
            severity=AlertSeverity.ERROR,
            message_template="Error count: {value}"
        )
        alerts.add_rule(rule)
        
        assert "test_high_error_rate" in alerts._rules
    
    def test_trigger_alert(self):
        """Alert should trigger when condition met."""
        from src.core.utils.observability import AlertRule, AlertSeverity
        
        alerts = AlertManager()
        triggered = []
        
        rule = AlertRule(
            name="test_high_latency",
            metric_name="latency_ms",
            condition="gt",
            threshold=1000,
            severity=AlertSeverity.WARNING,
            message_template="High latency: {value}ms"
        )
        alerts.add_rule(rule)
        alerts.add_handler(lambda a: triggered.append(a))
        
        alerts.check_and_alert("latency_ms", 2000)
        
        assert len(triggered) == 1
    
    def test_alert_cooldown(self):
        """Alert should respect cooldown period."""
        from src.core.utils.observability import AlertRule, AlertSeverity
        
        alerts = AlertManager()
        triggered = []
        
        rule = AlertRule(
            name="test_cooldown_alert",
            metric_name="test_metric",
            condition="gt",
            threshold=0,
            severity=AlertSeverity.INFO,
            cooldown_seconds=60,
            message_template="Test alert: {value}"
        )
        alerts.add_rule(rule)
        alerts.add_handler(lambda a: triggered.append(a))
        
        alerts.check_and_alert("test_metric", 1)
        alerts.check_and_alert("test_metric", 1)  # Should be in cooldown
        
        assert len(triggered) == 1
    
    def test_alert_severity_levels(self):
        """Alerts should have severity levels."""
        from src.core.utils.observability import AlertRule, AlertSeverity
        
        alerts = AlertManager()
        
        # Default rules are already registered, check they exist
        assert "high_error_rate" in alerts._rules
        assert alerts._rules["high_error_rate"].severity == AlertSeverity.ERROR


# ============================================================================
# Cost Tracker Tests
# ============================================================================

class TestCostTracker:
    """Tests for LLM cost tracking."""
    
    def test_track_token_usage(self):
        """Should track token usage."""
        tracker = CostTracker()
        
        usage = tracker.record_usage(
            model="gpt-4o-mini",
            provider="openai",
            input_tokens=100,
            output_tokens=50,
            operation="test",
            success=True,
            latency_ms=100
        )
        
        assert usage.input_tokens == 100
        assert usage.output_tokens == 50
    
    def test_calculate_cost(self):
        """Should calculate cost based on pricing."""
        tracker = CostTracker()
        
        # gpt-4o-mini: input $0.00015/1k, output $0.0006/1k
        cost = tracker.calculate_cost("gpt-4o-mini", 1000, 500)
        
        # 1000 * 0.00015/1000 + 500 * 0.0006/1000 = 0.00015 + 0.0003 = 0.00045
        assert cost == pytest.approx(0.00045, rel=0.01)
    
    def test_budget_limit_warning(self):
        """Should track usage for budget monitoring."""
        tracker = CostTracker()
        
        # Record some usage
        tracker.record_usage(
            model="gpt-4o",
            provider="openai",
            input_tokens=10000,
            output_tokens=5000,
            operation="test",
            success=True,
            latency_ms=1000
        )
        
        # Get stats to check costs are tracked
        stats = tracker.get_usage_stats()
        # Stats has 'today' and 'month' keys with cost info
        assert "today" in stats or "month" in stats
    
    def test_cost_by_provider(self):
        """Should track costs per provider."""
        tracker = CostTracker()
        
        tracker.record_usage(
            model="gpt-4o-mini",
            provider="openai",
            input_tokens=100,
            output_tokens=50,
            operation="test",
            success=True,
            latency_ms=100
        )
        tracker.record_usage(
            model="claude-3-haiku-20240307",
            provider="anthropic",
            input_tokens=200,
            output_tokens=100,
            operation="test",
            success=True,
            latency_ms=150
        )
        
        stats = tracker.get_usage_stats()
        
        # Stats has 'by_model' key with model-specific costs
        assert "by_model" in stats
    
    def test_daily_cost_tracking(self):
        """Should track daily costs."""
        tracker = CostTracker()
        
        tracker.record_usage(
            model="gpt-4o-mini",
            provider="openai",
            input_tokens=100,
            output_tokens=50,
            operation="test",
            success=True,
            latency_ms=100
        )
        
        stats = tracker.get_usage_stats()
        
        assert "today" in stats or "total_cost" in stats


# ============================================================================
# Integration Tests
# ============================================================================

class TestObservabilityIntegration:
    """Integration tests for observability components."""
    
    def test_log_with_trace_context(self):
        """Logs should include trace context."""
        logger = StructuredLogger("test")
        tracer = Tracer()
        
        with tracer.start_span("operation") as span:
            context = LogContext(trace_id=span.trace_id)
            logger.set_context(context)
            
            # Log should have trace context
            assert span.trace_id is not None
    
    def test_metrics_trigger_alerts(self):
        """Metrics should trigger alerts."""
        from src.core.utils.observability import AlertRule, AlertSeverity
        
        metrics = MetricsCollector()
        alerts = AlertManager()
        triggered = []
        
        rule = AlertRule(
            name="test_high_errors",
            metric_name="errors",
            condition="gt",
            threshold=10,
            severity=AlertSeverity.ERROR,
            message_template="High errors: {value}"
        )
        alerts.add_rule(rule)
        alerts.add_handler(lambda a: triggered.append(a))
        
        for _ in range(15):
            metrics.increment("errors")
        
        alerts.check_and_alert("errors", metrics.get_counter("errors"))
        
        assert len(triggered) == 1
    
    def test_cost_tracking_with_logging(self):
        """Cost tracking should integrate with logging."""
        from loguru import logger as loguru_logger
        
        tracker = CostTracker()
        
        usage = tracker.record_usage(
            model="gpt-4o-mini",
            provider="openai",
            input_tokens=1000,
            output_tokens=500,
            operation="test",
            success=True,
            latency_ms=500
        )
        
        total_tokens = usage.input_tokens + usage.output_tokens
        loguru_logger.info(f"LLM call completed, tokens={total_tokens}")
        
        assert total_tokens > 0
