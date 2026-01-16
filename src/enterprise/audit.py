"""
Enterprise Audit Logging System
Tracks all user actions for compliance and security
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from enum import Enum
from loguru import logger
import pandas as pd


class AuditEventType(Enum):
    """Types of audit events."""
    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout"
    ANALYSIS_CREATED = "analysis_created"
    ANALYSIS_VIEWED = "analysis_viewed"
    ANALYSIS_DELETED = "analysis_deleted"
    DATA_UPLOADED = "data_uploaded"
    DATA_EXPORTED = "data_exported"
    SETTINGS_CHANGED = "settings_changed"
    USER_CREATED = "user_created"
    USER_UPDATED = "user_updated"
    USER_DEACTIVATED = "user_deactivated"
    PERMISSION_CHANGED = "permission_changed"
    API_CALL = "api_call"
    ERROR_OCCURRED = "error_occurred"
    SECURITY_ALERT = "security_alert"


class AuditSeverity(Enum):
    """Severity levels for audit events."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditLogger:
    """Enterprise-grade audit logging system."""
    
    def __init__(self, audit_dir: str = "./data/enterprise/audit"):
        """Initialize audit logger."""
        self.audit_dir = Path(audit_dir)
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.current_log_file = self.audit_dir / f"audit_{datetime.now().strftime('%Y%m')}.jsonl"
    
    def log_event(
        self,
        event_type: AuditEventType,
        user: str,
        action: str,
        resource: Optional[str] = None,
        details: Optional[Dict] = None,
        severity: AuditSeverity = AuditSeverity.INFO,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        organization: str = "default"
    ):
        """
        Log an audit event.
        
        Args:
            event_type: Type of event
            user: Username
            action: Action description
            resource: Resource affected
            details: Additional details
            severity: Event severity
            ip_address: User's IP address
            user_agent: User's browser/client
            organization: Organization ID
        """
        event = {
            "event_id": self._generate_event_id(),
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type.value,
            "severity": severity.value,
            "user": user,
            "organization": organization,
            "action": action,
            "resource": resource,
            "details": details or {},
            "ip_address": ip_address,
            "user_agent": user_agent,
            "version": "1.0"
        }
        
        # Write to log file
        with open(self.current_log_file, 'a') as f:
            f.write(json.dumps(event) + '\n')
        
        # Also log to standard logger
        log_message = f"[AUDIT] {user} - {action}"
        if severity == AuditSeverity.CRITICAL:
            logger.critical(log_message)
        elif severity == AuditSeverity.ERROR:
            logger.error(log_message)
        elif severity == AuditSeverity.WARNING:
            logger.warning(log_message)
        else:
            logger.info(log_message)
    
    def _generate_event_id(self) -> str:
        """Generate unique event ID."""
        import uuid
        return str(uuid.uuid4())
    
    def get_user_activity(
        self,
        user: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        event_types: Optional[List[AuditEventType]] = None
    ) -> List[Dict]:
        """
        Get activity for a specific user.
        
        Args:
            user: Username
            start_date: Start date filter
            end_date: End date filter
            event_types: Filter by event types
            
        Returns:
            List of audit events
        """
        events = self._load_events()
        
        # Filter by user
        user_events = [e for e in events if e['user'] == user]
        
        # Filter by date range
        if start_date:
            user_events = [e for e in user_events if datetime.fromisoformat(e['timestamp']) >= start_date]
        if end_date:
            user_events = [e for e in user_events if datetime.fromisoformat(e['timestamp']) <= end_date]
        
        # Filter by event types
        if event_types:
            event_type_values = [et.value for et in event_types]
            user_events = [e for e in user_events if e['event_type'] in event_type_values]
        
        return user_events
    
    def get_security_alerts(
        self,
        start_date: Optional[datetime] = None,
        severity: Optional[AuditSeverity] = None
    ) -> List[Dict]:
        """Get security-related alerts."""
        events = self._load_events()
        
        # Filter security events
        security_events = [
            e for e in events
            if e['event_type'] == AuditEventType.SECURITY_ALERT.value
            or e['severity'] in [AuditSeverity.ERROR.value, AuditSeverity.CRITICAL.value]
        ]
        
        if start_date:
            security_events = [
                e for e in security_events
                if datetime.fromisoformat(e['timestamp']) >= start_date
            ]
        
        if severity:
            security_events = [e for e in security_events if e['severity'] == severity.value]
        
        return security_events
    
    def generate_compliance_report(
        self,
        start_date: datetime,
        end_date: datetime,
        organization: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate compliance report.
        
        Args:
            start_date: Report start date
            end_date: Report end date
            organization: Filter by organization
            
        Returns:
            Compliance report data
        """
        events = self._load_events()
        
        # Filter by date range
        period_events = [
            e for e in events
            if start_date <= datetime.fromisoformat(e['timestamp']) <= end_date
        ]
        
        # Filter by organization
        if organization:
            period_events = [e for e in period_events if e.get('organization') == organization]
        
        df = pd.DataFrame(period_events)
        
        report = {
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "organization": organization or "all",
            "summary": {
                "total_events": len(period_events),
                "unique_users": df['user'].nunique() if len(df) > 0 else 0,
                "events_by_type": df['event_type'].value_counts().to_dict() if len(df) > 0 else {},
                "events_by_severity": df['severity'].value_counts().to_dict() if len(df) > 0 else {}
            },
            "security": {
                "security_alerts": len([e for e in period_events if e['event_type'] == AuditEventType.SECURITY_ALERT.value]),
                "failed_logins": len([e for e in period_events if e['event_type'] == AuditEventType.USER_LOGIN.value and e.get('details', {}).get('success') == False]),
                "critical_events": len([e for e in period_events if e['severity'] == AuditSeverity.CRITICAL.value])
            },
            "data_access": {
                "analyses_created": len([e for e in period_events if e['event_type'] == AuditEventType.ANALYSIS_CREATED.value]),
                "data_uploaded": len([e for e in period_events if e['event_type'] == AuditEventType.DATA_UPLOADED.value]),
                "data_exported": len([e for e in period_events if e['event_type'] == AuditEventType.DATA_EXPORTED.value])
            },
            "user_activity": {
                "most_active_users": df['user'].value_counts().head(10).to_dict() if len(df) > 0 else {},
                "activity_by_hour": df.groupby(pd.to_datetime(df['timestamp']).dt.hour).size().to_dict() if len(df) > 0 else {}
            }
        }
        
        return report
    
    def export_audit_log(
        self,
        start_date: datetime,
        end_date: datetime,
        format: str = "csv"
    ) -> str:
        """
        Export audit log.
        
        Args:
            start_date: Start date
            end_date: End date
            format: Export format (csv, json, excel)
            
        Returns:
            Path to exported file
        """
        events = self._load_events()
        
        # Filter by date range
        period_events = [
            e for e in events
            if start_date <= datetime.fromisoformat(e['timestamp']) <= end_date
        ]
        
        df = pd.DataFrame(period_events)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if format == "csv":
            export_path = self.audit_dir / f"audit_export_{timestamp}.csv"
            df.to_csv(export_path, index=False)
        elif format == "excel":
            export_path = self.audit_dir / f"audit_export_{timestamp}.xlsx"
            df.to_excel(export_path, index=False)
        else:  # json
            export_path = self.audit_dir / f"audit_export_{timestamp}.json"
            df.to_json(export_path, orient='records', indent=2)
        
        logger.info(f"Exported audit log to {export_path}")
        return str(export_path)
    
    def _load_events(self) -> List[Dict]:
        """Load all audit events."""
        events = []
        
        # Load all audit log files
        for log_file in sorted(self.audit_dir.glob("audit_*.jsonl")):
            with open(log_file, 'r') as f:
                for line in f:
                    events.append(json.loads(line))
        
        return events
    
    def rotate_logs(self, keep_months: int = 12):
        """Rotate old audit logs."""
        current_date = datetime.now()
        
        for log_file in self.audit_dir.glob("audit_*.jsonl"):
            # Extract date from filename
            try:
                file_date_str = log_file.stem.split('_')[1]
                file_date = datetime.strptime(file_date_str, '%Y%m')
                
                age_months = (current_date.year - file_date.year) * 12 + (current_date.month - file_date.month)
                
                if age_months > keep_months:
                    # Archive or delete old logs
                    archive_dir = self.audit_dir / "archive"
                    archive_dir.mkdir(exist_ok=True)
                    log_file.rename(archive_dir / log_file.name)
                    logger.info(f"Archived old audit log: {log_file.name}")
            except (ValueError, IndexError):
                continue

# Global instance
audit_logger = AuditLogger()
