"""
Observability Module - Structured Logging, Metrics, Tracing, Alerting, and Cost Tracking

This module provides production-grade observability for the PCA Agent system:
- Structured JSON logging with context
- Metrics collection (counters, gauges, histograms)
- Distributed tracing with span context
- Alerting with configurable thresholds
- LLM cost tracking and budgeting
"""

import time
import json
import threading
import functools
import uuid
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
from pathlib import Path
import statistics
from contextlib import contextmanager
from loguru import logger
import sys


# =============================================================================
# STRUCTURED LOGGING
# =============================================================================

class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogContext:
    """Context for structured logging."""
    request_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    component: Optional[str] = None
    operation: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {k: v for k, v in asdict(self).items() if v is not None and k != 'extra'}
        result.update(self.extra)
        return result


class StructuredLogger:
    """
    Structured JSON logger with context propagation.
    
    Features:
    - JSON formatted logs for log aggregation (ELK, Splunk, etc.)
    - Context propagation across function calls
    - Automatic timing and error tracking
    - Log sampling for high-volume operations
    """
    
    _instance: Optional['StructuredLogger'] = None
    _lock = threading.Lock()
    _context = threading.local()
    
    def __init__(self, service_name: str = "pca-agent", log_file: Optional[str] = None):
        self.service_name = service_name
        self.log_file = log_file or os.path.join(
            os.path.dirname(__file__), "..", "..", "logs", "pca_agent.json"
        )
        self._setup_logger()
    
    @classmethod
    def get_instance(cls, service_name: str = "pca-agent") -> 'StructuredLogger':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(service_name)
            return cls._instance
    
    def _setup_logger(self):
        """Configure loguru for structured JSON output."""
        # Ensure log directory exists
        try:
            Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            # If we can't create the log directory, just skip file logging
            logger.warning(f"Could not create log directory: {e}")
            return
        
        # Use a sink function instead of format string to avoid parsing issues
        service_name = self.service_name
        context_holder = self._context
        
        def json_sink(message):
            """Custom sink that writes JSON formatted logs."""
            record = message.record
            log_entry = {
                "timestamp": record["time"].isoformat(),
                "level": record["level"].name,
                "service": service_name,
                "message": record["message"],
                "module": record["module"],
                "function": record["function"],
                "line": record["line"],
            }
            
            # Add context if available
            if hasattr(context_holder, 'current') and context_holder.current:
                log_entry["context"] = context_holder.current.to_dict()
            
            # Add extra data
            if record["extra"]:
                log_entry["data"] = {k: v for k, v in record["extra"].items() 
                                     if k not in ['context']}
            
            # Add exception info if present
            if record["exception"]:
                log_entry["exception"] = {
                    "type": record["exception"].type.__name__ if record["exception"].type else None,
                    "value": str(record["exception"].value) if record["exception"].value else None,
                }
            
            # Write to file
            try:
                with open(self.log_file, 'a') as f:
                    f.write(json.dumps(log_entry) + "\n")
            except Exception:
                pass  # Silently fail if we can't write
        
        # Add JSON sink handler - but don't interfere with existing handlers
        try:
            logger.add(
                json_sink,
                level="INFO",
                filter=lambda record: record["level"].no >= 20  # INFO and above
            )
        except Exception as e:
            logger.warning(f"Could not add JSON sink: {e}")
    
    def set_context(self, context: LogContext):
        """Set the current logging context."""
        self._context.current = context
    
    def get_context(self) -> Optional[LogContext]:
        """Get the current logging context."""
        return getattr(self._context, 'current', None)
    
    def clear_context(self):
        """Clear the current logging context."""
        self._context.current = None
    
    @contextmanager
    def context(self, **kwargs):
        """Context manager for temporary logging context."""
        old_context = self.get_context()
        new_context = LogContext(**kwargs)
        self.set_context(new_context)
        try:
            yield new_context
        finally:
            if old_context:
                self.set_context(old_context)
            else:
                self.clear_context()
    
    def log(self, level: LogLevel, message: str, **kwargs):
        """Log a structured message."""
        log_func = getattr(logger, level.value.lower())
        log_func(message, **kwargs)
    
    def debug(self, message: str, **kwargs):
        self.log(LogLevel.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self.log(LogLevel.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self.log(LogLevel.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self.log(LogLevel.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        self.log(LogLevel.CRITICAL, message, **kwargs)


# Global structured logger
structured_logger = StructuredLogger.get_instance()


def log_operation(operation_name: str, component: str = "unknown"):
    """Decorator to log function entry/exit with timing."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            request_id = str(uuid.uuid4())[:8]
            start_time = time.time()
            
            with structured_logger.context(
                request_id=request_id,
                component=component,
                operation=operation_name
            ):
                structured_logger.info(f"Starting {operation_name}")
                
                try:
                    result = func(*args, **kwargs)
                    elapsed = time.time() - start_time
                    structured_logger.info(
                        f"Completed {operation_name}",
                        duration_ms=round(elapsed * 1000, 2),
                        success=True
                    )
                    return result
                except Exception as e:
                    elapsed = time.time() - start_time
                    structured_logger.error(
                        f"Failed {operation_name}: {str(e)}",
                        duration_ms=round(elapsed * 1000, 2),
                        success=False,
                        error_type=type(e).__name__
                    )
                    raise
        
        return wrapper
    return decorator


# =============================================================================
# METRICS COLLECTION
# =============================================================================

class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class MetricPoint:
    """A single metric data point."""
    name: str
    value: float
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)
    metric_type: MetricType = MetricType.GAUGE


class MetricsCollector:
    """
    Metrics collection system for monitoring.
    
    Collects:
    - Counters: Monotonically increasing values (requests, errors)
    - Gauges: Point-in-time values (active connections, queue size)
    - Histograms: Distribution of values (latency, response size)
    - Timers: Duration measurements
    
    Compatible with Prometheus, StatsD, CloudWatch, etc.
    """
    
    _instance: Optional['MetricsCollector'] = None
    _lock = threading.Lock()
    
    def __init__(self):
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timers: Dict[str, List[float]] = defaultdict(list)
        self._labels: Dict[str, Dict[str, str]] = {}
        self._metrics_lock = threading.Lock()
        self._start_time = datetime.utcnow()
        
        # Metric retention (keep last N values for histograms)
        self._max_histogram_size = 1000
    
    @classmethod
    def get_instance(cls) -> 'MetricsCollector':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance
    
    def _make_key(self, name: str, labels: Optional[Dict[str, str]] = None) -> str:
        """Create a unique key for a metric with labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"
    
    # Counter operations
    def increment(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Increment a counter."""
        key = self._make_key(name, labels)
        with self._metrics_lock:
            self._counters[key] += value
            if labels:
                self._labels[key] = labels
    
    def get_counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        """Get counter value."""
        key = self._make_key(name, labels)
        return self._counters.get(key, 0.0)
    
    # Gauge operations
    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set a gauge value."""
        key = self._make_key(name, labels)
        with self._metrics_lock:
            self._gauges[key] = value
            if labels:
                self._labels[key] = labels
    
    def get_gauge(self, name: str, labels: Optional[Dict[str, str]] = None) -> Optional[float]:
        """Get gauge value."""
        key = self._make_key(name, labels)
        return self._gauges.get(key)
    
    # Histogram operations
    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a value in a histogram."""
        key = self._make_key(name, labels)
        with self._metrics_lock:
            self._histograms[key].append(value)
            # Trim to max size
            if len(self._histograms[key]) > self._max_histogram_size:
                self._histograms[key] = self._histograms[key][-self._max_histogram_size:]
            if labels:
                self._labels[key] = labels
    
    def get_histogram_stats(self, name: str, labels: Optional[Dict[str, str]] = None) -> Dict[str, float]:
        """Get histogram statistics."""
        key = self._make_key(name, labels)
        values = self._histograms.get(key, [])
        
        if not values:
            return {"count": 0, "min": 0, "max": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0}
        
        sorted_values = sorted(values)
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": statistics.mean(values),
            "p50": sorted_values[int(len(sorted_values) * 0.5)],
            "p95": sorted_values[int(len(sorted_values) * 0.95)] if len(sorted_values) > 20 else max(values),
            "p99": sorted_values[int(len(sorted_values) * 0.99)] if len(sorted_values) > 100 else max(values)
        }
    
    # Timer operations
    @contextmanager
    def timer(self, name: str, labels: Optional[Dict[str, str]] = None):
        """Context manager to time an operation."""
        start = time.time()
        try:
            yield
        finally:
            elapsed = (time.time() - start) * 1000  # Convert to ms
            self.observe(name, elapsed, labels)
    
    def record_time(self, name: str, duration_ms: float, labels: Optional[Dict[str, str]] = None):
        """Record a duration directly."""
        self.observe(name, duration_ms, labels)
    
    # Export metrics
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics in a structured format."""
        with self._metrics_lock:
            return {
                "uptime_seconds": (datetime.utcnow() - self._start_time).total_seconds(),
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
                "histograms": {
                    name: self.get_histogram_stats(name) 
                    for name in self._histograms.keys()
                }
            }
    
    def to_prometheus_format(self) -> str:
        """Export metrics in Prometheus text format."""
        lines = []
        
        # Counters
        for key, value in self._counters.items():
            lines.append(f"# TYPE {key.split('{')[0]} counter")
            lines.append(f"{key} {value}")
        
        # Gauges
        for key, value in self._gauges.items():
            lines.append(f"# TYPE {key.split('{')[0]} gauge")
            lines.append(f"{key} {value}")
        
        # Histograms (as summary)
        for key, values in self._histograms.items():
            base_name = key.split('{')[0]
            stats = self.get_histogram_stats(key)
            lines.append(f"# TYPE {base_name} summary")
            lines.append(f'{base_name}_count{{{key.split("{")[1] if "{" in key else ""}}} {stats["count"]}')
            lines.append(f'{base_name}_sum{{{key.split("{")[1] if "{" in key else ""}}} {sum(values)}')
        
        return "\n".join(lines)
    
    def reset(self):
        """Reset all metrics."""
        with self._metrics_lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
            self._timers.clear()
            self._start_time = datetime.utcnow()


# Global metrics collector
metrics = MetricsCollector.get_instance()


def track_metrics(name: str, labels: Optional[Dict[str, str]] = None):
    """Decorator to track function call metrics."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            metrics.increment(f"{name}_calls_total", labels=labels)
            
            start = time.time()
            try:
                result = func(*args, **kwargs)
                metrics.increment(f"{name}_success_total", labels=labels)
                return result
            except Exception as e:
                metrics.increment(f"{name}_errors_total", labels={**(labels or {}), "error_type": type(e).__name__})
                raise
            finally:
                elapsed = (time.time() - start) * 1000
                metrics.observe(f"{name}_duration_ms", elapsed, labels=labels)
        
        return wrapper
    return decorator


# =============================================================================
# DISTRIBUTED TRACING
# =============================================================================

@dataclass
class Span:
    """A single span in a distributed trace."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    operation_name: str
    service_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "OK"
    tags: Dict[str, str] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    
    @property
    def duration_ms(self) -> Optional[float]:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "operation_name": self.operation_name,
            "service_name": self.service_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "tags": self.tags,
            "logs": self.logs
        }


class Tracer:
    """
    Distributed tracing system.
    
    Provides:
    - Trace context propagation
    - Span creation and management
    - Timing of operations
    - Error tracking
    
    Compatible with OpenTelemetry, Jaeger, Zipkin, etc.
    """
    
    _instance: Optional['Tracer'] = None
    _lock = threading.Lock()
    _context = threading.local()
    
    def __init__(self, service_name: str = "pca-agent"):
        self.service_name = service_name
        self._spans: Dict[str, Span] = {}
        self._completed_spans: List[Span] = []
        self._max_completed_spans = 10000
        self._spans_lock = threading.Lock()
    
    @classmethod
    def get_instance(cls, service_name: str = "pca-agent") -> 'Tracer':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(service_name)
            return cls._instance
    
    def _generate_id(self) -> str:
        """Generate a unique ID for traces/spans."""
        return uuid.uuid4().hex[:16]
    
    def get_current_span(self) -> Optional[Span]:
        """Get the current active span."""
        return getattr(self._context, 'current_span', None)
    
    def get_current_trace_id(self) -> Optional[str]:
        """Get the current trace ID."""
        span = self.get_current_span()
        return span.trace_id if span else None
    
    @contextmanager
    def start_span(self, operation_name: str, tags: Optional[Dict[str, str]] = None):
        """Start a new span as a context manager."""
        parent_span = self.get_current_span()
        
        span = Span(
            trace_id=parent_span.trace_id if parent_span else self._generate_id(),
            span_id=self._generate_id(),
            parent_span_id=parent_span.span_id if parent_span else None,
            operation_name=operation_name,
            service_name=self.service_name,
            start_time=datetime.utcnow(),
            tags=tags or {}
        )
        
        with self._spans_lock:
            self._spans[span.span_id] = span
        
        # Set as current span
        old_span = self.get_current_span()
        self._context.current_span = span
        
        try:
            yield span
            span.status = "OK"
        except Exception as e:
            span.status = "ERROR"
            span.tags["error"] = "true"
            span.tags["error.type"] = type(e).__name__
            span.tags["error.message"] = str(e)[:200]
            span.logs.append({
                "timestamp": datetime.utcnow().isoformat(),
                "event": "error",
                "message": str(e)
            })
            raise
        finally:
            span.end_time = datetime.utcnow()
            
            # Move to completed spans
            with self._spans_lock:
                if span.span_id in self._spans:
                    del self._spans[span.span_id]
                self._completed_spans.append(span)
                
                # Trim completed spans
                if len(self._completed_spans) > self._max_completed_spans:
                    self._completed_spans = self._completed_spans[-self._max_completed_spans:]
            
            # Restore parent span
            self._context.current_span = old_span
    
    def add_tag(self, key: str, value: str):
        """Add a tag to the current span."""
        span = self.get_current_span()
        if span:
            span.tags[key] = value
    
    def add_log(self, message: str, **kwargs):
        """Add a log entry to the current span."""
        span = self.get_current_span()
        if span:
            span.logs.append({
                "timestamp": datetime.utcnow().isoformat(),
                "message": message,
                **kwargs
            })
    
    def get_trace(self, trace_id: str) -> List[Dict[str, Any]]:
        """Get all spans for a trace."""
        with self._spans_lock:
            spans = [s for s in self._completed_spans if s.trace_id == trace_id]
            spans.extend([s for s in self._spans.values() if s.trace_id == trace_id])
        return [s.to_dict() for s in sorted(spans, key=lambda x: x.start_time)]
    
    def get_recent_traces(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent completed traces."""
        with self._spans_lock:
            # Group by trace_id
            traces = defaultdict(list)
            for span in self._completed_spans[-limit * 10:]:  # Get more spans to find unique traces
                traces[span.trace_id].append(span)
            
            # Return most recent traces
            result = []
            for trace_id, spans in list(traces.items())[-limit:]:
                root_span = next((s for s in spans if s.parent_span_id is None), spans[0])
                result.append({
                    "trace_id": trace_id,
                    "operation": root_span.operation_name,
                    "start_time": root_span.start_time.isoformat(),
                    "duration_ms": root_span.duration_ms,
                    "status": root_span.status,
                    "span_count": len(spans)
                })
            
            return result


# Global tracer
tracer = Tracer.get_instance()


def trace(operation_name: str, tags: Optional[Dict[str, str]] = None):
    """Decorator to trace a function."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with tracer.start_span(operation_name, tags):
                return func(*args, **kwargs)
        return wrapper
    return decorator


# =============================================================================
# ALERTING
# =============================================================================

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Alert:
    """An alert triggered by a threshold breach."""
    alert_id: str
    name: str
    severity: AlertSeverity
    message: str
    metric_name: str
    threshold: float
    current_value: float
    triggered_at: datetime
    resolved_at: Optional[datetime] = None
    acknowledged: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "alert_id": self.alert_id,
            "name": self.name,
            "severity": self.severity.value,
            "message": self.message,
            "metric_name": self.metric_name,
            "threshold": self.threshold,
            "current_value": self.current_value,
            "triggered_at": self.triggered_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "acknowledged": self.acknowledged,
            "is_active": self.resolved_at is None
        }


@dataclass
class AlertRule:
    """Rule for triggering alerts."""
    name: str
    metric_name: str
    condition: str  # "gt", "lt", "gte", "lte", "eq"
    threshold: float
    severity: AlertSeverity
    message_template: str
    cooldown_seconds: int = 300  # Minimum time between alerts
    
    def check(self, value: float) -> bool:
        """Check if the condition is met."""
        if self.condition == "gt":
            return value > self.threshold
        elif self.condition == "lt":
            return value < self.threshold
        elif self.condition == "gte":
            return value >= self.threshold
        elif self.condition == "lte":
            return value <= self.threshold
        elif self.condition == "eq":
            return value == self.threshold
        return False


class AlertManager:
    """
    Alert management system.
    
    Features:
    - Configurable alert rules
    - Alert deduplication and cooldown
    - Alert history
    - Webhook notifications (extensible)
    """
    
    _instance: Optional['AlertManager'] = None
    _lock = threading.Lock()
    
    def __init__(self):
        self._rules: Dict[str, AlertRule] = {}
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_history: List[Alert] = []
        self._last_alert_time: Dict[str, datetime] = {}
        self._alert_handlers: List[Callable[[Alert], None]] = []
        self._alerts_lock = threading.Lock()
        
        # Register default rules
        self._register_default_rules()
    
    @classmethod
    def get_instance(cls) -> 'AlertManager':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance
    
    def _register_default_rules(self):
        """Register default alert rules."""
        default_rules = [
            AlertRule(
                name="high_error_rate",
                metric_name="llm_errors_total",
                condition="gt",
                threshold=10,
                severity=AlertSeverity.ERROR,
                message_template="High LLM error rate: {value} errors (threshold: {threshold})"
            ),
            AlertRule(
                name="high_latency",
                metric_name="llm_duration_ms_p95",
                condition="gt",
                threshold=30000,  # 30 seconds
                severity=AlertSeverity.WARNING,
                message_template="High LLM latency: {value}ms p95 (threshold: {threshold}ms)"
            ),
            AlertRule(
                name="high_cost",
                metric_name="llm_cost_total_usd",
                condition="gt",
                threshold=100,  # $100
                severity=AlertSeverity.WARNING,
                message_template="High LLM cost: ${value} (threshold: ${threshold})"
            ),
            AlertRule(
                name="circuit_breaker_open",
                metric_name="circuit_breaker_open_count",
                condition="gt",
                threshold=0,
                severity=AlertSeverity.CRITICAL,
                message_template="Circuit breaker(s) open: {value} services affected"
            ),
            AlertRule(
                name="dlq_backlog",
                metric_name="dead_letter_queue_size",
                condition="gt",
                threshold=50,
                severity=AlertSeverity.WARNING,
                message_template="Dead letter queue backlog: {value} jobs (threshold: {threshold})"
            )
        ]
        
        for rule in default_rules:
            self.add_rule(rule)
    
    def add_rule(self, rule: AlertRule):
        """Add an alert rule."""
        self._rules[rule.name] = rule
    
    def remove_rule(self, name: str):
        """Remove an alert rule."""
        if name in self._rules:
            del self._rules[name]
    
    def add_handler(self, handler: Callable[[Alert], None]):
        """Add an alert handler (e.g., for webhooks, email, Slack)."""
        self._alert_handlers.append(handler)
    
    def check_and_alert(self, metric_name: str, value: float):
        """Check all rules for a metric and trigger alerts if needed."""
        for rule in self._rules.values():
            if rule.metric_name == metric_name and rule.check(value):
                self._trigger_alert(rule, value)
    
    def _trigger_alert(self, rule: AlertRule, value: float):
        """Trigger an alert."""
        with self._alerts_lock:
            # Check cooldown
            last_time = self._last_alert_time.get(rule.name)
            if last_time and (datetime.utcnow() - last_time).total_seconds() < rule.cooldown_seconds:
                return  # Still in cooldown
            
            # Create alert
            alert = Alert(
                alert_id=str(uuid.uuid4())[:8],
                name=rule.name,
                severity=rule.severity,
                message=rule.message_template.format(value=value, threshold=rule.threshold),
                metric_name=rule.metric_name,
                threshold=rule.threshold,
                current_value=value,
                triggered_at=datetime.utcnow()
            )
            
            self._active_alerts[alert.alert_id] = alert
            self._alert_history.append(alert)
            self._last_alert_time[rule.name] = datetime.utcnow()
            
            # Log the alert
            log_func = getattr(logger, rule.severity.value)
            log_func(f"ALERT [{rule.severity.value.upper()}]: {alert.message}")
            
            # Call handlers
            for handler in self._alert_handlers:
                try:
                    handler(alert)
                except Exception as e:
                    logger.error(f"Alert handler failed: {e}")
    
    def resolve_alert(self, alert_id: str):
        """Resolve an active alert."""
        with self._alerts_lock:
            if alert_id in self._active_alerts:
                alert = self._active_alerts[alert_id]
                alert.resolved_at = datetime.utcnow()
                del self._active_alerts[alert_id]
                logger.info(f"Alert resolved: {alert.name}")
    
    def acknowledge_alert(self, alert_id: str):
        """Acknowledge an alert."""
        with self._alerts_lock:
            if alert_id in self._active_alerts:
                self._active_alerts[alert_id].acknowledged = True
    
    def get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get all active alerts."""
        with self._alerts_lock:
            return [a.to_dict() for a in self._active_alerts.values()]
    
    def get_alert_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get alert history."""
        with self._alerts_lock:
            return [a.to_dict() for a in self._alert_history[-limit:]]


# Global alert manager
alerts = AlertManager.get_instance()


# =============================================================================
# LLM COST TRACKING
# =============================================================================

@dataclass
class LLMPricing:
    """Pricing for an LLM model."""
    model: str
    provider: str
    input_cost_per_1k: float  # Cost per 1000 input tokens
    output_cost_per_1k: float  # Cost per 1000 output tokens


@dataclass
class LLMUsage:
    """Record of LLM usage."""
    request_id: str
    model: str
    provider: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    timestamp: datetime
    operation: str
    success: bool
    latency_ms: float
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CostTracker:
    """
    LLM cost tracking and budgeting.
    
    Features:
    - Per-model cost tracking
    - Daily/monthly budgets
    - Cost alerts
    - Usage analytics
    """
    
    _instance: Optional['CostTracker'] = None
    _lock = threading.Lock()
    
    # Pricing data (as of late 2024)
    PRICING = {
        # Anthropic
        "claude-sonnet-4-5-20250929": LLMPricing("claude-sonnet-4-5-20250929", "anthropic", 0.003, 0.015),
        "claude-3-5-sonnet-20241022": LLMPricing("claude-3-5-sonnet-20241022", "anthropic", 0.003, 0.015),
        "claude-3-haiku-20240307": LLMPricing("claude-3-haiku-20240307", "anthropic", 0.00025, 0.00125),
        # OpenAI
        "gpt-4o": LLMPricing("gpt-4o", "openai", 0.005, 0.015),
        "gpt-4o-mini": LLMPricing("gpt-4o-mini", "openai", 0.00015, 0.0006),
        "gpt-4-turbo": LLMPricing("gpt-4-turbo", "openai", 0.01, 0.03),
        # Google
        "gemini-2.0-flash-exp": LLMPricing("gemini-2.0-flash-exp", "google", 0.0, 0.0),  # Free tier
        "gemini-1.5-pro": LLMPricing("gemini-1.5-pro", "google", 0.00125, 0.005),
        # Groq (free tier)
        "llama-3.3-70b-versatile": LLMPricing("llama-3.3-70b-versatile", "groq", 0.0, 0.0),
    }
    
    def __init__(self):
        self._usage: List[LLMUsage] = []
        self._daily_budget: float = 50.0  # Default $50/day
        self._monthly_budget: float = 500.0  # Default $500/month
        self._usage_lock = threading.Lock()
        self._storage_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "llm_usage.json"
        )
        self._load_usage()
    
    @classmethod
    def get_instance(cls) -> 'CostTracker':
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance
    
    def _load_usage(self):
        """Load usage from disk."""
        try:
            path = Path(self._storage_path)
            if path.exists():
                with open(path, 'r') as f:
                    data = json.load(f)
                    for item in data:
                        item['timestamp'] = datetime.fromisoformat(item['timestamp'])
                        self._usage.append(LLMUsage(**item))
        except Exception as e:
            logger.warning(f"Could not load LLM usage: {e}")
    
    def _save_usage(self):
        """Save usage to disk."""
        try:
            path = Path(self._storage_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # Keep only last 30 days
            cutoff = datetime.utcnow() - timedelta(days=30)
            recent_usage = [u for u in self._usage if u.timestamp > cutoff]
            
            with open(path, 'w') as f:
                json.dump([{**u.to_dict(), 'timestamp': u.timestamp.isoformat()} 
                          for u in recent_usage], f, indent=2)
        except Exception as e:
            logger.error(f"Could not save LLM usage: {e}")
    
    def calculate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost for a request."""
        pricing = self.PRICING.get(model)
        if not pricing:
            # Default to gpt-4o-mini pricing if unknown
            pricing = self.PRICING["gpt-4o-mini"]
        
        input_cost = (input_tokens / 1000) * pricing.input_cost_per_1k
        output_cost = (output_tokens / 1000) * pricing.output_cost_per_1k
        return round(input_cost + output_cost, 6)
    
    def record_usage(
        self,
        model: str,
        provider: str,
        input_tokens: int,
        output_tokens: int,
        operation: str,
        success: bool,
        latency_ms: float
    ) -> LLMUsage:
        """Record LLM usage."""
        cost = self.calculate_cost(model, input_tokens, output_tokens)
        
        usage = LLMUsage(
            request_id=str(uuid.uuid4())[:8],
            model=model,
            provider=provider,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=cost,
            timestamp=datetime.utcnow(),
            operation=operation,
            success=success,
            latency_ms=latency_ms
        )
        
        with self._usage_lock:
            self._usage.append(usage)
            self._save_usage()
        
        # Update metrics
        metrics.increment("llm_requests_total", labels={"model": model, "provider": provider})
        metrics.increment("llm_tokens_input_total", value=input_tokens, labels={"model": model})
        metrics.increment("llm_tokens_output_total", value=output_tokens, labels={"model": model})
        metrics.increment("llm_cost_total_usd", value=cost, labels={"model": model})
        metrics.observe("llm_latency_ms", latency_ms, labels={"model": model})
        
        if not success:
            metrics.increment("llm_errors_total", labels={"model": model})
        
        # Check budget alerts
        daily_cost = self.get_daily_cost()
        monthly_cost = self.get_monthly_cost()
        
        if daily_cost > self._daily_budget * 0.8:
            alerts.check_and_alert("llm_cost_daily_usd", daily_cost)
        
        if monthly_cost > self._monthly_budget * 0.8:
            alerts.check_and_alert("llm_cost_monthly_usd", monthly_cost)
        
        return usage
    
    def set_budget(self, daily: Optional[float] = None, monthly: Optional[float] = None):
        """Set budget limits."""
        if daily is not None:
            self._daily_budget = daily
        if monthly is not None:
            self._monthly_budget = monthly
    
    def get_daily_cost(self, date: Optional[datetime] = None) -> float:
        """Get total cost for a day."""
        target_date = (date or datetime.utcnow()).date()
        with self._usage_lock:
            return sum(u.cost_usd for u in self._usage if u.timestamp.date() == target_date)
    
    def get_monthly_cost(self, year: Optional[int] = None, month: Optional[int] = None) -> float:
        """Get total cost for a month."""
        now = datetime.utcnow()
        target_year = year or now.year
        target_month = month or now.month
        
        with self._usage_lock:
            return sum(
                u.cost_usd for u in self._usage 
                if u.timestamp.year == target_year and u.timestamp.month == target_month
            )
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get usage statistics."""
        now = datetime.utcnow()
        today = now.date()
        
        with self._usage_lock:
            today_usage = [u for u in self._usage if u.timestamp.date() == today]
            month_usage = [u for u in self._usage 
                          if u.timestamp.year == now.year and u.timestamp.month == now.month]
        
        # By model
        by_model = defaultdict(lambda: {"requests": 0, "cost": 0.0, "tokens": 0})
        for u in month_usage:
            by_model[u.model]["requests"] += 1
            by_model[u.model]["cost"] += u.cost_usd
            by_model[u.model]["tokens"] += u.input_tokens + u.output_tokens
        
        return {
            "today": {
                "requests": len(today_usage),
                "cost_usd": round(sum(u.cost_usd for u in today_usage), 4),
                "tokens": sum(u.input_tokens + u.output_tokens for u in today_usage),
                "budget_remaining": round(self._daily_budget - self.get_daily_cost(), 2)
            },
            "month": {
                "requests": len(month_usage),
                "cost_usd": round(sum(u.cost_usd for u in month_usage), 4),
                "tokens": sum(u.input_tokens + u.output_tokens for u in month_usage),
                "budget_remaining": round(self._monthly_budget - self.get_monthly_cost(), 2)
            },
            "by_model": dict(by_model),
            "budgets": {
                "daily": self._daily_budget,
                "monthly": self._monthly_budget
            }
        }


# Global cost tracker
cost_tracker = CostTracker.get_instance()


def track_llm_cost(operation: str):
    """Decorator to track LLM costs (requires function to return token counts)."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            try:
                result = func(*args, **kwargs)
                latency = (time.time() - start) * 1000
                
                # Extract token counts if available
                if isinstance(result, dict) and 'usage' in result:
                    usage = result['usage']
                    cost_tracker.record_usage(
                        model=result.get('model', 'unknown'),
                        provider=result.get('provider', 'unknown'),
                        input_tokens=usage.get('input_tokens', 0),
                        output_tokens=usage.get('output_tokens', 0),
                        operation=operation,
                        success=True,
                        latency_ms=latency
                    )
                
                return result
            except Exception as e:
                latency = (time.time() - start) * 1000
                # Record failed request
                cost_tracker.record_usage(
                    model='unknown',
                    provider='unknown',
                    input_tokens=0,
                    output_tokens=0,
                    operation=operation,
                    success=False,
                    latency_ms=latency
                )
                raise
        
        return wrapper
    return decorator


# =============================================================================
# OBSERVABILITY DASHBOARD DATA
# =============================================================================

def get_observability_status() -> Dict[str, Any]:
    """Get complete observability status for dashboard."""
    return {
        "metrics": metrics.get_all_metrics(),
        "traces": tracer.get_recent_traces(limit=20),
        "alerts": {
            "active": alerts.get_active_alerts(),
            "history": alerts.get_alert_history(limit=50)
        },
        "costs": cost_tracker.get_usage_stats()
    }


# =============================================================================
# INITIALIZATION
# =============================================================================

logger.info("Observability module loaded successfully")
