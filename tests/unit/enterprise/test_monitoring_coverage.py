import unittest
import os
import json
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from src.enterprise.monitoring import (
    PerformanceMonitor, RequestTracker, AlertManager, 
    HealthCheckManager, HealthStatus
)

class TestPerformanceMonitor(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path("./tests/tmp/metrics")
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.monitor = PerformanceMonitor(str(self.test_dir))

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_record_and_summary(self):
        self.monitor.record_metric("cpu_percent", 45.0, unit="%")
        self.monitor.record_metric("cpu_percent", 55.0, unit="%")
        self.monitor.record_metric("memory_percent", 60.0, unit="%")
        
        summary = self.monitor.get_performance_summary(hours=1)
        self.assertEqual(summary["total_metrics"], 3)
        self.assertEqual(summary["by_metric"]["cpu_percent"]["mean"], 50.0)

    def test_threshold_alerts(self):
        # Default threshold for cpu_percent is 80.0
        self.monitor.record_metric("cpu_percent", 90.0)
        self.assertEqual(len(self.monitor.alerts), 1)
        self.assertIn("exceeded threshold", self.monitor.alerts[0]["message"])

    def test_health_status(self):
        status = self.monitor.get_health_status()
        self.assertIn("status", status)
        self.assertIn("healthy", status)
        self.assertIsInstance(status["healthy"], bool)

class TestRequestTracker(unittest.TestCase):
    def setUp(self):
        self.tracker = RequestTracker()

    def test_request_stats(self):
        self.tracker.track_request("/api/v1/analyze", "POST", 200, 150.5)
        self.tracker.track_request("/api/v1/analyze", "POST", 500, 2000.0, error="Timeout")
        
        stats = self.tracker.get_stats(minutes=1)
        self.assertEqual(stats["total_requests"], 2)
        self.assertEqual(stats["successful_requests"], 1)
        self.assertEqual(stats["failed_requests"], 1)
        self.assertEqual(stats["avg_response_time_ms"], 1075.25)

class TestAlertManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path("./tests/tmp/alerts")
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.alerts_file = self.test_dir / "alerts.json"
        self.manager = AlertManager(str(self.alerts_file))

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_alert_lifecycle(self):
        # Create alert
        self.manager.create_alert("critical", "System Down", "Critical Failure")
        active = self.manager.get_active_alerts()
        self.assertEqual(len(active), 1)
        alert_id = active[0]["alert_id"]
        
        # Acknowledge
        self.manager.acknowledge_alert(alert_id)
        active = self.manager.get_active_alerts()
        self.assertTrue(active[0]["acknowledged"])
        
        # Resolve
        self.manager.resolve_alert(alert_id)
        self.assertEqual(len(self.manager.get_active_alerts()), 0)

    def test_alert_handler(self):
        received = []
        def handler(alert): received.append(alert)
        self.manager.register_handler(handler)
        
        self.manager.create_alert("info", "Test", "Msg")
        self.assertEqual(len(received), 1)

class TestHealthCheckManager(unittest.TestCase):
    def setUp(self):
        self.manager = HealthCheckManager()

    def test_health_checks(self):
        self.manager.register_check("DB", lambda: True)
        self.manager.register_check("Redis", lambda: False)
        
        results = self.manager.run_checks()
        self.assertFalse(results["overall_healthy"])
        self.assertTrue(results["checks"]["DB"]["healthy"])
        self.assertFalse(results["checks"]["Redis"]["healthy"])

if __name__ == "__main__":
    unittest.main()
