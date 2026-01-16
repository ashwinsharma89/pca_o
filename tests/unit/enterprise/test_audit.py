import pytest
import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from src.enterprise.audit import AuditLogger, AuditEventType, AuditSeverity

class TestAuditLogger:
    @pytest.fixture
    def audit_dir(self, tmp_path):
        d = tmp_path / "audit"
        d.mkdir()
        return str(d)

    @pytest.fixture
    def logger(self, audit_dir):
        return AuditLogger(audit_dir=audit_dir)

    def test_log_event_creates_file(self, logger, audit_dir):
        logger.log_event(
            AuditEventType.USER_LOGIN,
            user="testadmin",
            action="login",
            details={"success": True}
        )
        
        log_files = list(Path(audit_dir).glob("audit_*.jsonl"))
        assert len(log_files) == 1
        
        with open(log_files[0], 'r') as f:
            content = f.read()
            event = json.loads(content)
            assert event['user'] == "testadmin"
            assert event['event_type'] == "user_login"

    def test_get_user_activity(self, logger):
        logger.log_event(AuditEventType.USER_LOGIN, user="user1", action="login")
        logger.log_event(AuditEventType.USER_LOGIN, user="user2", action="login")
        
        activity = logger.get_user_activity("user1")
        assert len(activity) == 1
        assert activity[0]['user'] == "user1"

    def test_compliance_report(self, logger):
        logger.log_event(AuditEventType.ANALYSIS_CREATED, user="user1", action="create")
        logger.log_event(AuditEventType.SECURITY_ALERT, user="system", action="alert", severity=AuditSeverity.CRITICAL)
        
        start = datetime.now() - timedelta(days=1)
        end = datetime.now() + timedelta(days=1)
        report = logger.generate_compliance_report(start, end)
        
        assert report['summary']['total_events'] == 2
        assert report['security']['critical_events'] == 1
