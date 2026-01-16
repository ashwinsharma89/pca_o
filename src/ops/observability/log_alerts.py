"""
Log Pattern Alerting System
Monitor logs for specific patterns and trigger alerts
"""

import re
from typing import Dict, Any, List, Callable
from datetime import datetime, timedelta
from collections import defaultdict
from loguru import logger

class LogAlert:
    """Log alert rule."""
    
    def __init__(
        self,
        name: str,
        pattern: str,
        severity: str,
        threshold: int = 1,
        time_window_minutes: int = 5,
        action: Callable = None
    ):
        """
        Initialize log alert.
        
        Args:
            name: Alert name
            pattern: Regex pattern to match
            severity: Alert severity (low, medium, high, critical)
            threshold: Number of matches to trigger alert
            time_window_minutes: Time window for threshold
            action: Action to take when alert triggers
        """
        self.name = name
        self.pattern = re.compile(pattern, re.IGNORECASE)
        self.severity = severity
        self.threshold = threshold
        self.time_window_minutes = time_window_minutes
        self.action = action or self._default_action
        self.matches = []
    
    def check(self, log_message: str) -> bool:
        """
        Check if log message matches alert pattern.
        
        Args:
            log_message: Log message to check
        
        Returns:
            True if alert should trigger
        """
        if self.pattern.search(log_message):
            # Add match with timestamp
            self.matches.append({
                "timestamp": datetime.utcnow(),
                "message": log_message
            })
            
            # Clean old matches outside time window
            cutoff = datetime.utcnow() - timedelta(minutes=self.time_window_minutes)
            self.matches = [
                m for m in self.matches
                if m["timestamp"] > cutoff
            ]
            
            # Check if threshold exceeded
            if len(self.matches) >= self.threshold:
                self.action(self.matches)
                self.matches = []  # Reset after triggering
                return True
        
        return False
    
    def _default_action(self, matches: List[Dict]):
        """Default action: log the alert."""
        logger.warning(
            f"🚨 ALERT: {self.name} | "
            f"Severity: {self.severity} | "
            f"Matches: {len(matches)} in {self.time_window_minutes} minutes"
        )


class LogAlertManager:
    """Manage log alerts and pattern monitoring."""
    
    def __init__(self):
        """Initialize log alert manager."""
        self.alerts = []
        self._setup_default_alerts()
    
    def _setup_default_alerts(self):
        """Setup default alert rules."""
        
        # Critical errors
        self.add_alert(LogAlert(
            name="Critical Errors",
            pattern=r"CRITICAL|FATAL",
            severity="critical",
            threshold=1,
            time_window_minutes=1,
            action=self._critical_error_action
        ))
        
        # Database connection failures
        self.add_alert(LogAlert(
            name="Database Connection Failures",
            pattern=r"database.*connection.*failed|could not connect to database",
            severity="high",
            threshold=3,
            time_window_minutes=5,
            action=self._database_failure_action
        ))
        
        # LLM API failures
        self.add_alert(LogAlert(
            name="LLM API Failures",
            pattern=r"llm.*error|openai.*error|anthropic.*error",
            severity="high",
            threshold=5,
            time_window_minutes=5,
            action=self._llm_failure_action
        ))
        
        # High error rate
        self.add_alert(LogAlert(
            name="High Error Rate",
            pattern=r"ERROR",
            severity="medium",
            threshold=50,
            time_window_minutes=5,
            action=self._high_error_rate_action
        ))
        
        # Authentication failures
        self.add_alert(LogAlert(
            name="Authentication Failures",
            pattern=r"authentication.*failed|unauthorized|invalid.*credentials",
            severity="high",
            threshold=10,
            time_window_minutes=5,
            action=self._auth_failure_action
        ))
        
        # Slow requests
        self.add_alert(LogAlert(
            name="Slow Requests",
            pattern=r"duration_ms.*[5-9]\d{3,}|duration_ms.*\d{5,}",  # >5000ms
            severity="medium",
            threshold=10,
            time_window_minutes=5,
            action=self._slow_request_action
        ))
        
        # Memory warnings
        self.add_alert(LogAlert(
            name="Memory Warnings",
            pattern=r"memory.*warning|out of memory|memory.*exceeded",
            severity="high",
            threshold=3,
            time_window_minutes=5,
            action=self._memory_warning_action
        ))
        
        # Security events
        self.add_alert(LogAlert(
            name="Security Events",
            pattern=r"security.*violation|intrusion.*detected|suspicious.*activity",
            severity="critical",
            threshold=1,
            time_window_minutes=1,
            action=self._security_event_action
        ))
    
    def add_alert(self, alert: LogAlert):
        """Add alert rule."""
        self.alerts.append(alert)
        logger.info(f"Added alert rule: {alert.name}")
    
    def check_log(self, log_message: str):
        """
        Check log message against all alert rules.
        
        Args:
            log_message: Log message to check
        """
        for alert in self.alerts:
            alert.check(log_message)
    
    # Alert actions
    
    def _critical_error_action(self, matches: List[Dict]):
        """Action for critical errors."""
        logger.critical(
            f"🚨 CRITICAL ERROR DETECTED!\n"
            f"Message: {matches[0]['message']}\n"
            f"Immediate action required!"
        )
        
        # Send to incident management
        self._send_to_pagerduty(
            severity="critical",
            message="Critical error in PCA Agent",
            details=matches[0]
        )
    
    def _database_failure_action(self, matches: List[Dict]):
        """Action for database failures."""
        logger.error(
            f"🚨 DATABASE CONNECTION FAILURES!\n"
            f"Count: {len(matches)} in 5 minutes\n"
            f"Attempting automatic recovery..."
        )
        
        # Trigger database reconnection
        # TODO: Implement auto-recovery in src/ops/resilience
        # from src.resilience.auto_recovery import auto_recovery_manager
        # auto_recovery_manager.handle_error(
        #     Exception("Database connection failures"),
        #     {"service": "database"}
        # )
        logger.warning("Auto-recovery not yet implemented")
    
    def _llm_failure_action(self, matches: List[Dict]):
        """Action for LLM API failures."""
        logger.error(
            f"🚨 LLM API FAILURES!\n"
            f"Count: {len(matches)} in 5 minutes\n"
            f"Check API status and rate limits"
        )
        
        # Send notification
        self._send_notification(
            channel="slack",
            message=f"LLM API failures: {len(matches)} in 5 minutes"
        )
    
    def _high_error_rate_action(self, matches: List[Dict]):
        """Action for high error rate."""
        logger.warning(
            f"⚠️  HIGH ERROR RATE!\n"
            f"Count: {len(matches)} errors in 5 minutes\n"
            f"Investigating..."
        )
        
        # Trigger error analysis
        self._analyze_error_patterns(matches)
    
    def _auth_failure_action(self, matches: List[Dict]):
        """Action for authentication failures."""
        logger.warning(
            f"🚨 AUTHENTICATION FAILURES!\n"
            f"Count: {len(matches)} in 5 minutes\n"
            f"Possible brute force attack"
        )
        
        # Trigger intrusion detection
        # TODO: Implement intrusion detection in src/enterprise/security
        # from src.security.intrusion_detection import intrusion_detection_system
        # for match in matches:
        #     # Extract IP from log message
        #     ip_match = re.search(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', match['message'])
        #     if ip_match:
        #         ip_address = ip_match.group()
        #         # intrusion_detection_system.record_failed_login("unknown", ip_address)
        logger.warning("Intrusion detection not yet implemented")
    
    def _slow_request_action(self, matches: List[Dict]):
        """Action for slow requests."""
        logger.warning(
            f"⚠️  SLOW REQUESTS DETECTED!\n"
            f"Count: {len(matches)} in 5 minutes\n"
            f"Performance degradation detected"
        )
        
        # Trigger performance analysis
        self._analyze_performance(matches)
    
    def _memory_warning_action(self, matches: List[Dict]):
        """Action for memory warnings."""
        logger.error(
            f"🚨 MEMORY WARNINGS!\n"
            f"Count: {len(matches)} in 5 minutes\n"
            f"System may be under memory pressure"
        )
        
        # Send alert
        self._send_notification(
            channel="ops",
            message="Memory warnings detected - investigate immediately"
        )
    
    def _security_event_action(self, matches: List[Dict]):
        """Action for security events."""
        logger.critical(
            f"🚨 SECURITY EVENT!\n"
            f"Message: {matches[0]['message']}\n"
            f"IMMEDIATE INVESTIGATION REQUIRED!"
        )
        
        # Send to security team
        self._send_to_security_team(matches[0])
    
    # Helper methods
    
    def _send_to_pagerduty(self, severity: str, message: str, details: Dict):
        """Send alert to PagerDuty."""
        # Placeholder - implement PagerDuty integration
        logger.info(f"Would send to PagerDuty: {message}")
    
    def _send_notification(self, channel: str, message: str):
        """Send notification to communication channel."""
        # Placeholder - implement Slack/Teams integration
        logger.info(f"Would send to {channel}: {message}")
    
    def _analyze_error_patterns(self, matches: List[Dict]):
        """Analyze error patterns."""
        # Group errors by type
        error_types = defaultdict(int)
        for match in matches:
            # Extract error type from message
            error_match = re.search(r'(\w+Error|\w+Exception)', match['message'])
            if error_match:
                error_types[error_match.group()] += 1
        
        logger.info(f"Error pattern analysis: {dict(error_types)}")
    
    def _analyze_performance(self, matches: List[Dict]):
        """Analyze performance issues."""
        # Extract durations
        durations = []
        for match in matches:
            duration_match = re.search(r'duration_ms[:\s]+(\d+)', match['message'])
            if duration_match:
                durations.append(int(duration_match.group(1)))
        
        if durations:
            avg_duration = sum(durations) / len(durations)
            max_duration = max(durations)
            logger.info(
                f"Performance analysis: "
                f"Avg: {avg_duration:.0f}ms, Max: {max_duration}ms"
            )
    
    def _send_to_security_team(self, event: Dict):
        """Send security event to security team."""
        # Placeholder - implement security team notification
        logger.critical(f"Would alert security team: {event['message']}")
    
    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary of all alerts."""
        return {
            "total_alerts": len(self.alerts),
            "alerts": [
                {
                    "name": alert.name,
                    "severity": alert.severity,
                    "threshold": alert.threshold,
                    "time_window": alert.time_window_minutes,
                    "current_matches": len(alert.matches)
                }
                for alert in self.alerts
            ]
        }


# Global instance
log_alert_manager = LogAlertManager()


def setup_log_alerting():
    """Setup log alerting integration with Loguru."""
    
    def alert_sink(message):
        """Custom sink for log alerting."""
        log_message = message.record["message"]
        log_alert_manager.check_log(log_message)
    
    # Add alert sink to logger
    logger.add(
        alert_sink,
        level="INFO",
        format="{message}",
        serialize=False
    )
    
    logger.info("✅ Log alerting initialized")


# Initialize on import
setup_log_alerting()
