"""
Monitoring & Alerting System
Real-time alerting, SLA tracking, cost monitoring, and user analytics
"""
import os
import time
import json
import httpx
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import threading
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
PAGERDUTY_KEY = os.getenv("PAGERDUTY_KEY", "")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
EMAIL_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "")


# ============================================================================
# Alert Severity & Types
# ============================================================================

class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(str, Enum):
    SLACK = "slack"
    PAGERDUTY = "pagerduty"
    DISCORD = "discord"
    EMAIL = "email"
    LOG = "log"


@dataclass
class Alert:
    """Alert data structure"""
    id: str
    title: str
    message: str
    severity: AlertSeverity
    source: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict = field(default_factory=dict)
    acknowledged: bool = False


# ============================================================================
# Real-Time Alerting
# ============================================================================

class AlertManager:
    """
    Real-time alerting with multiple channels
    
    Usage:
        alerter = AlertManager()
        alerter.alert("High Error Rate", "Error rate exceeded 5%", AlertSeverity.ERROR)
    """
    
    def __init__(self):
        self.alerts: List[Alert] = []
        self._alert_count = 0
        self._channels = self._setup_channels()
    
    def _setup_channels(self) -> Dict[AlertChannel, Callable]:
        channels = {AlertChannel.LOG: self._send_log}
        
        if SLACK_WEBHOOK_URL:
            channels[AlertChannel.SLACK] = self._send_slack
        if PAGERDUTY_KEY:
            channels[AlertChannel.PAGERDUTY] = self._send_pagerduty
        if DISCORD_WEBHOOK_URL:
            channels[AlertChannel.DISCORD] = self._send_discord
        
        return channels
    
    def alert(
        self,
        title: str,
        message: str,
        severity: AlertSeverity = AlertSeverity.WARNING,
        source: str = "system",
        channels: List[AlertChannel] = None,
        metadata: Dict = None
    ) -> Alert:
        """
        Send alert to configured channels
        
        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity
            source: Alert source
            channels: Specific channels (default: all configured)
            metadata: Additional data
        """
        self._alert_count += 1
        alert = Alert(
            id=f"alert-{self._alert_count}-{int(time.time())}",
            title=title,
            message=message,
            severity=severity,
            source=source,
            metadata=metadata or {}
        )
        
        self.alerts.append(alert)
        
        # Send to channels
        target_channels = channels or list(self._channels.keys())
        for channel in target_channels:
            if channel in self._channels:
                try:
                    self._channels[channel](alert)
                except Exception as e:
                    logger.error(f"Failed to send alert to {channel}: {e}")
        
        return alert
    
    def _send_log(self, alert: Alert):
        log_func = getattr(logger, alert.severity.value, logger.warning)
        log_func(f"[ALERT] {alert.title}: {alert.message}")
    
    def _send_slack(self, alert: Alert):
        color_map = {
            AlertSeverity.INFO: "#36a64f",
            AlertSeverity.WARNING: "#ff9800",
            AlertSeverity.ERROR: "#f44336",
            AlertSeverity.CRITICAL: "#9c27b0"
        }
        
        payload = {
            "attachments": [{
                "color": color_map.get(alert.severity, "#gray"),
                "title": f"🚨 {alert.title}",
                "text": alert.message,
                "fields": [
                    {"title": "Severity", "value": alert.severity.value, "short": True},
                    {"title": "Source", "value": alert.source, "short": True}
                ],
                "footer": f"Alert ID: {alert.id}",
                "ts": int(alert.timestamp.timestamp())
            }]
        }
        
        httpx.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
    
    def _send_pagerduty(self, alert: Alert):
        if alert.severity not in [AlertSeverity.ERROR, AlertSeverity.CRITICAL]:
            return
        
        payload = {
            "routing_key": PAGERDUTY_KEY,
            "event_action": "trigger",
            "payload": {
                "summary": f"{alert.title}: {alert.message}",
                "severity": "critical" if alert.severity == AlertSeverity.CRITICAL else "error",
                "source": alert.source
            }
        }
        
        httpx.post(
            "https://events.pagerduty.com/v2/enqueue",
            json=payload,
            timeout=5
        )
    
    def _send_discord(self, alert: Alert):
        color_map = {
            AlertSeverity.INFO: 3066993,
            AlertSeverity.WARNING: 16776960,
            AlertSeverity.ERROR: 15158332,
            AlertSeverity.CRITICAL: 10181046
        }
        
        payload = {
            "embeds": [{
                "title": f"🚨 {alert.title}",
                "description": alert.message,
                "color": color_map.get(alert.severity, 8421504),
                "fields": [
                    {"name": "Severity", "value": alert.severity.value, "inline": True},
                    {"name": "Source", "value": alert.source, "inline": True}
                ],
                "footer": {"text": f"Alert ID: {alert.id}"}
            }]
        }
        
        httpx.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
    
    def get_recent_alerts(self, hours: int = 24) -> List[Alert]:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return [a for a in self.alerts if a.timestamp >= cutoff]


# ============================================================================
# SLA Monitoring
# ============================================================================

@dataclass
class SLAConfig:
    """SLA configuration"""
    name: str
    target: float  # Target percentage (e.g., 99.9)
    metric: str  # Metric to track
    window_hours: int = 24  # Measurement window


class SLAMonitor:
    """
    SLA monitoring with automatic alerting
    
    Usage:
        sla = SLAMonitor()
        sla.add_sla("API Availability", target=99.9, metric="availability")
        sla.record_success("availability")
        sla.record_failure("availability")
        sla.check_all()
    """
    
    def __init__(self, alert_manager: AlertManager = None):
        self.slas: Dict[str, SLAConfig] = {}
        self.metrics: Dict[str, List[tuple]] = defaultdict(list)
        self.alerter = alert_manager or AlertManager()
    
    def add_sla(self, name: str, target: float, metric: str, window_hours: int = 24):
        self.slas[name] = SLAConfig(
            name=name,
            target=target,
            metric=metric,
            window_hours=window_hours
        )
    
    def record_success(self, metric: str):
        self.metrics[metric].append((datetime.utcnow(), True))
        self._cleanup_old_metrics(metric)
    
    def record_failure(self, metric: str):
        self.metrics[metric].append((datetime.utcnow(), False))
        self._cleanup_old_metrics(metric)
    
    def _cleanup_old_metrics(self, metric: str, max_hours: int = 168):
        cutoff = datetime.utcnow() - timedelta(hours=max_hours)
        self.metrics[metric] = [
            (ts, val) for ts, val in self.metrics[metric] if ts >= cutoff
        ]
    
    def get_sla_status(self, name: str) -> Dict:
        if name not in self.slas:
            return {"error": "SLA not found"}
        
        sla = self.slas[name]
        cutoff = datetime.utcnow() - timedelta(hours=sla.window_hours)
        
        records = [
            val for ts, val in self.metrics[sla.metric] if ts >= cutoff
        ]
        
        if not records:
            return {"name": name, "current": 100.0, "target": sla.target, "status": "no_data"}
        
        success_rate = (sum(records) / len(records)) * 100
        
        return {
            "name": name,
            "current": round(success_rate, 2),
            "target": sla.target,
            "total_requests": len(records),
            "successful": sum(records),
            "failed": len(records) - sum(records),
            "status": "ok" if success_rate >= sla.target else "breached",
            "window_hours": sla.window_hours
        }
    
    def check_all(self) -> List[Dict]:
        results = []
        for name in self.slas:
            status = self.get_sla_status(name)
            results.append(status)
            
            if status.get("status") == "breached":
                self.alerter.alert(
                    title=f"SLA Breach: {name}",
                    message=f"Current: {status['current']}%, Target: {status['target']}%",
                    severity=AlertSeverity.ERROR,
                    source="sla_monitor"
                )
        
        return results


# ============================================================================
# Cost Tracking
# ============================================================================

@dataclass
class CostEntry:
    """Cost entry"""
    timestamp: datetime
    service: str
    cost: float
    units: float
    unit_type: str


class CostTracker:
    """
    Cost tracking for cloud services and API usage
    
    Usage:
        costs = CostTracker()
        costs.record("openai", cost=0.05, units=1000, unit_type="tokens")
        costs.get_daily_summary()
    """
    
    def __init__(self, alert_manager: AlertManager = None):
        self.entries: List[CostEntry] = []
        self.budgets: Dict[str, float] = {}  # service: daily_budget
        self.alerter = alert_manager or AlertManager()
    
    def set_budget(self, service: str, daily_budget: float):
        self.budgets[service] = daily_budget
    
    def record(self, service: str, cost: float, units: float = 0, unit_type: str = ""):
        entry = CostEntry(
            timestamp=datetime.utcnow(),
            service=service,
            cost=cost,
            units=units,
            unit_type=unit_type
        )
        self.entries.append(entry)
        
        # Check budget
        if service in self.budgets:
            daily_cost = self.get_daily_cost(service)
            if daily_cost > self.budgets[service]:
                self.alerter.alert(
                    title=f"Budget Exceeded: {service}",
                    message=f"Daily cost ${daily_cost:.2f} exceeds budget ${self.budgets[service]:.2f}",
                    severity=AlertSeverity.WARNING,
                    source="cost_tracker"
                )
    
    def get_daily_cost(self, service: str = None) -> float:
        today = datetime.utcnow().date()
        entries = [
            e for e in self.entries
            if e.timestamp.date() == today
            and (service is None or e.service == service)
        ]
        return sum(e.cost for e in entries)
    
    def get_daily_summary(self) -> Dict:
        today = datetime.utcnow().date()
        entries = [e for e in self.entries if e.timestamp.date() == today]
        
        by_service = defaultdict(lambda: {"cost": 0, "units": 0})
        for e in entries:
            by_service[e.service]["cost"] += e.cost
            by_service[e.service]["units"] += e.units
        
        return {
            "date": today.isoformat(),
            "total_cost": sum(e.cost for e in entries),
            "by_service": dict(by_service),
            "budgets": self.budgets
        }
    
    def get_monthly_summary(self) -> Dict:
        now = datetime.utcnow()
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        entries = [e for e in self.entries if e.timestamp >= month_start]
        
        by_service = defaultdict(float)
        for e in entries:
            by_service[e.service] += e.cost
        
        return {
            "month": now.strftime("%Y-%m"),
            "total_cost": sum(e.cost for e in entries),
            "by_service": dict(by_service)
        }


# ============================================================================
# User Analytics
# ============================================================================

@dataclass
class UserEvent:
    """User analytics event"""
    timestamp: datetime
    user_id: str
    event_type: str
    properties: Dict


class UserAnalytics:
    """
    User analytics tracking
    
    Usage:
        analytics = UserAnalytics()
        analytics.track("user_123", "page_view", {"page": "/dashboard"})
        analytics.get_active_users()
    """
    
    def __init__(self):
        self.events: List[UserEvent] = []
        self._lock = threading.Lock()
    
    def track(self, user_id: str, event_type: str, properties: Dict = None):
        event = UserEvent(
            timestamp=datetime.utcnow(),
            user_id=user_id,
            event_type=event_type,
            properties=properties or {}
        )
        
        with self._lock:
            self.events.append(event)
    
    def get_active_users(self, hours: int = 24) -> int:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        users = set(e.user_id for e in self.events if e.timestamp >= cutoff)
        return len(users)
    
    def get_event_counts(self, hours: int = 24) -> Dict[str, int]:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        counts = defaultdict(int)
        
        for e in self.events:
            if e.timestamp >= cutoff:
                counts[e.event_type] += 1
        
        return dict(counts)
    
    def get_user_journey(self, user_id: str, hours: int = 24) -> List[Dict]:
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return [
            {
                "timestamp": e.timestamp.isoformat(),
                "event": e.event_type,
                "properties": e.properties
            }
            for e in self.events
            if e.user_id == user_id and e.timestamp >= cutoff
        ]
    
    def get_summary(self) -> Dict:
        return {
            "active_users_24h": self.get_active_users(24),
            "active_users_7d": self.get_active_users(168),
            "event_counts_24h": self.get_event_counts(24),
            "total_events": len(self.events)
        }


# ============================================================================
# Global Instances
# ============================================================================

_alert_manager: Optional[AlertManager] = None
_sla_monitor: Optional[SLAMonitor] = None
_cost_tracker: Optional[CostTracker] = None
_user_analytics: Optional[UserAnalytics] = None


def get_alert_manager() -> AlertManager:
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager()
    return _alert_manager


def get_sla_monitor() -> SLAMonitor:
    global _sla_monitor
    if _sla_monitor is None:
        _sla_monitor = SLAMonitor(get_alert_manager())
        # Default SLAs
        _sla_monitor.add_sla("API Availability", target=99.9, metric="api_availability")
        _sla_monitor.add_sla("Response Time", target=95.0, metric="response_time")
    return _sla_monitor


def get_cost_tracker() -> CostTracker:
    global _cost_tracker
    if _cost_tracker is None:
        _cost_tracker = CostTracker(get_alert_manager())
    return _cost_tracker


def get_user_analytics() -> UserAnalytics:
    global _user_analytics
    if _user_analytics is None:
        _user_analytics = UserAnalytics()
    return _user_analytics
