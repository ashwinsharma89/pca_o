import unittest
import os
import json
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from src.enterprise.audit import AuditLogger, AuditEventType, AuditSeverity

class TestAuditLogger(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path("./tests/tmp/audit")
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        self.test_dir.mkdir(parents=True, exist_ok=True)
        self.audit = AuditLogger(str(self.test_dir))

    def tearDown(self):
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    def test_log_event(self):
        username = "test_user"
        action = "Created analysis"
        self.audit.log_event(
            AuditEventType.ANALYSIS_CREATED,
            username,
            action,
            severity=AuditSeverity.INFO
        )
        
        events = self.audit._load_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["user"], username)
        self.assertEqual(events[0]["action"], action)
        self.assertEqual(events[0]["event_type"], AuditEventType.ANALYSIS_CREATED.value)

    def test_get_user_activity_filters(self):
        self.audit.log_event(AuditEventType.USER_LOGIN, "user1", "Login")
        self.audit.log_event(AuditEventType.USER_LOGIN, "user2", "Login")
        self.audit.log_event(AuditEventType.ANALYSIS_CREATED, "user1", "Create")
        
        # Filter by user
        user1_events = self.audit.get_user_activity("user1")
        self.assertEqual(len(user1_events), 2)
        
        # Filter by event type
        login_events = self.audit.get_user_activity("user1", event_types=[AuditEventType.USER_LOGIN])
        self.assertEqual(len(login_events), 1)

    def test_security_alerts_filter(self):
        self.audit.log_event(AuditEventType.USER_LOGIN, "user1", "Login", severity=AuditSeverity.INFO)
        self.audit.log_event(AuditEventType.SECURITY_ALERT, "attacker", "SQL Injection", severity=AuditSeverity.CRITICAL)
        self.audit.log_event(AuditEventType.ERROR_OCCURRED, "sys", "Crash", severity=AuditSeverity.ERROR)
        
        alerts = self.audit.get_security_alerts()
        self.assertEqual(len(alerts), 2)
        
        critical_alerts = self.audit.get_security_alerts(severity=AuditSeverity.CRITICAL)
        self.assertEqual(len(critical_alerts), 1)

    def test_compliance_report(self):
        self.audit.log_event(AuditEventType.USER_LOGIN, "u1", "Login", details={"success": True})
        self.audit.log_event(AuditEventType.USER_LOGIN, "u1", "Failed", details={"success": False})
        self.audit.log_event(AuditEventType.DATA_EXPORTED, "u1", "Export")
        
        start = datetime.now() - timedelta(days=1)
        end = datetime.now() + timedelta(days=1)
        
        report = self.audit.generate_compliance_report(start, end)
        self.assertEqual(report["summary"]["total_events"], 3)
        self.assertEqual(report["security"]["failed_logins"], 1)
        self.assertEqual(report["data_access"]["data_exported"], 1)

    def test_export_audit_log(self):
        self.audit.log_event(AuditEventType.USER_LOGIN, "u1", "Login")
        start = datetime.now() - timedelta(days=1)
        end = datetime.now() + timedelta(days=1)
        
        csv_path = self.audit.export_audit_log(start, end, format="csv")
        self.assertTrue(os.path.exists(csv_path))
        self.assertTrue(csv_path.endswith(".csv"))
        
        json_path = self.audit.export_audit_log(start, end, format="json")
        self.assertTrue(os.path.exists(json_path))

    def test_log_rotation(self):
        # Create a mock old log file
        old_file = self.test_dir / "audit_202201.jsonl"
        old_file.touch()
        
        self.audit.rotate_logs(keep_months=1)
        
        archive_dir = self.test_dir / "archive"
        self.assertTrue((archive_dir / "audit_202201.jsonl").exists())
        self.assertFalse(old_file.exists())

if __name__ == "__main__":
    unittest.main()
