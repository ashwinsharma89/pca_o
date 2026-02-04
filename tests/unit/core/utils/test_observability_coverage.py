import unittest
import os
import json
from datetime import datetime
from src.core.utils.observability import (
    StructuredLogger, LogContext, LogLevel, 
    MetricsCollector, MetricType, 
    Tracer, Span
)

class TestObservabilityCoverage(unittest.TestCase):
    def setUp(self):
        self.logger = StructuredLogger.get_instance("test-service")
        self.metrics = MetricsCollector.get_instance()
        self.tracer = Tracer.get_instance("test-service")
        self.metrics.reset()

    def test_structured_logging_context(self):
        with self.logger.context(request_id="req-123", user_id="user-456"):
            ctx = self.logger.get_context()
            self.assertEqual(ctx.request_id, "req-123")
            self.assertEqual(ctx.user_id, "user-456")
            
        self.assertIsNone(self.logger.get_context())

    def test_metrics_collection_counter(self):
        self.metrics.increment("test_counter", 5.0, labels={"env": "test"})
        val = self.metrics.get_counter("test_counter", labels={"env": "test"})
        self.assertEqual(val, 5.0)

    def test_metrics_collection_gauge(self):
        self.metrics.set_gauge("test_gauge", 42.0)
        self.assertEqual(self.metrics.get_gauge("test_gauge"), 42.0)

    def test_tracer_span_creation(self):
        with self.tracer.start_span("test_op", tags={"key": "val"}) as span:
            self.assertEqual(span.operation_name, "test_op")
            self.assertEqual(span.tags["key"], "val")
            self.tracer.add_tag("new_tag", "new_val")
            self.assertEqual(span.tags["new_tag"], "new_val")
            
        self.assertIsNotNone(span.end_time)
        self.assertTrue(span.duration_ms >= 0)

    def test_tracer_nested_spans(self):
        with self.tracer.start_span("parent") as p_span:
            with self.tracer.start_span("child") as c_span:
                self.assertEqual(c_span.parent_span_id, p_span.span_id)
                self.assertEqual(c_span.trace_id, p_span.trace_id)

if __name__ == "__main__":
    unittest.main()
