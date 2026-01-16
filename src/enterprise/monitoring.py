"""
Enterprise Monitoring & Performance Tracking
Real-time metrics, health checks, and alerting
"""
import time
import psutil
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from loguru import logger
import threading


class HealthStatus(Enum):
    """System health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class PerformanceMetric:
    """Performance metric data."""
    timestamp: str
    metric_name: str
    value: float
    unit: str
    tags: Dict[str, str]


class PerformanceMonitor:
    """Monitors system performance and health."""
    
    def __init__(self, metrics_dir: str = "./data/enterprise/metrics"):
        """Initialize performance monitor."""
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_file = self.metrics_dir / f"metrics_{datetime.now().strftime('%Y%m%d')}.jsonl"
        self.alerts = []
        self.thresholds = {
            "cpu_percent": 80.0,
            "memory_percent": 85.0,
            "disk_percent": 90.0,
            "response_time_ms": 5000.0,
            "error_rate_percent": 5.0
        }
    
    def record_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "",
        tags: Optional[Dict[str, str]] = None
    ):
        """
        Record a performance metric.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Unit of measurement
            tags: Additional tags
        """
        metric = PerformanceMetric(
            timestamp=datetime.now().isoformat(),
            metric_name=metric_name,
            value=value,
            unit=unit,
            tags=tags or {}
        )
        
        # Write to file
        with open(self.metrics_file, 'a') as f:
            f.write(json.dumps(asdict(metric)) + '\n')
        
        # Check thresholds
        self._check_threshold(metric_name, value)
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get current system metrics."""
        return {
            "cpu": {
                "percent": psutil.cpu_percent(interval=1),
                "count": psutil.cpu_count(),
                "per_cpu": psutil.cpu_percent(interval=1, percpu=True)
            },
            "memory": {
                "percent": psutil.virtual_memory().percent,
                "total_gb": psutil.virtual_memory().total / (1024**3),
                "available_gb": psutil.virtual_memory().available / (1024**3),
                "used_gb": psutil.virtual_memory().used / (1024**3)
            },
            "disk": {
                "percent": psutil.disk_usage('/').percent,
                "total_gb": psutil.disk_usage('/').total / (1024**3),
                "free_gb": psutil.disk_usage('/').free / (1024**3),
                "used_gb": psutil.disk_usage('/').used / (1024**3)
            },
            "timestamp": datetime.now().isoformat()
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall system health status."""
        metrics = self.get_system_metrics()
        
        # Determine health status
        issues = []
        status = HealthStatus.HEALTHY
        
        if metrics['cpu']['percent'] > self.thresholds['cpu_percent']:
            issues.append(f"High CPU usage: {metrics['cpu']['percent']:.1f}%")
            status = HealthStatus.DEGRADED
        
        if metrics['memory']['percent'] > self.thresholds['memory_percent']:
            issues.append(f"High memory usage: {metrics['memory']['percent']:.1f}%")
            status = HealthStatus.DEGRADED
        
        if metrics['disk']['percent'] > self.thresholds['disk_percent']:
            issues.append(f"High disk usage: {metrics['disk']['percent']:.1f}%")
            status = HealthStatus.UNHEALTHY
        
        return {
            "status": status.value,
            "timestamp": datetime.now().isoformat(),
            "metrics": metrics,
            "issues": issues,
            "healthy": status == HealthStatus.HEALTHY
        }
    
    def _check_threshold(self, metric_name: str, value: float):
        """Check if metric exceeds threshold."""
        if metric_name in self.thresholds:
            threshold = self.thresholds[metric_name]
            if value > threshold:
                alert = {
                    "timestamp": datetime.now().isoformat(),
                    "metric": metric_name,
                    "value": value,
                    "threshold": threshold,
                    "message": f"{metric_name} exceeded threshold: {value} > {threshold}"
                }
                self.alerts.append(alert)
                logger.warning(alert['message'])
    
    def get_performance_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get performance summary for the last N hours."""
        metrics = self._load_recent_metrics(hours)
        
        if not metrics:
            return {"message": "No metrics available"}
        
        import pandas as pd
        df = pd.DataFrame(metrics)
        
        summary = {
            "period_hours": hours,
            "total_metrics": len(df),
            "by_metric": {}
        }
        
        for metric_name in df['metric_name'].unique():
            metric_data = df[df['metric_name'] == metric_name]['value']
            summary['by_metric'][metric_name] = {
                "count": len(metric_data),
                "mean": float(metric_data.mean()),
                "min": float(metric_data.min()),
                "max": float(metric_data.max()),
                "std": float(metric_data.std()) if len(metric_data) > 1 else 0
            }
        
        return summary
    
    def _load_recent_metrics(self, hours: int) -> List[Dict]:
        """Load metrics from the last N hours."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        metrics = []
        
        for metrics_file in sorted(self.metrics_dir.glob("metrics_*.jsonl")):
            with open(metrics_file, 'r') as f:
                for line in f:
                    metric = json.loads(line)
                    if datetime.fromisoformat(metric['timestamp']) >= cutoff_time:
                        metrics.append(metric)
        
        return metrics


class RequestTracker:
    """Tracks API requests and response times."""
    
    def __init__(self):
        """Initialize request tracker."""
        self.requests = []
        self.max_history = 10000
    
    def track_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        response_time_ms: float,
        user: Optional[str] = None,
        error: Optional[str] = None
    ):
        """Track an API request."""
        request_data = {
            "timestamp": datetime.now().isoformat(),
            "endpoint": endpoint,
            "method": method,
            "status_code": status_code,
            "response_time_ms": response_time_ms,
            "user": user,
            "error": error,
            "success": 200 <= status_code < 300
        }
        
        self.requests.append(request_data)
        
        # Keep only recent requests
        if len(self.requests) > self.max_history:
            self.requests = self.requests[-self.max_history:]
    
    def get_stats(self, minutes: int = 60) -> Dict[str, Any]:
        """Get request statistics for the last N minutes."""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        
        recent_requests = [
            r for r in self.requests
            if datetime.fromisoformat(r['timestamp']) >= cutoff_time
        ]
        
        if not recent_requests:
            return {"message": "No requests in the specified period"}
        
        import pandas as pd
        df = pd.DataFrame(recent_requests)
        
        return {
            "period_minutes": minutes,
            "total_requests": len(df),
            "successful_requests": int(df['success'].sum()),
            "failed_requests": int((~df['success']).sum()),
            "success_rate": float(df['success'].mean() * 100),
            "avg_response_time_ms": float(df['response_time_ms'].mean()),
            "p50_response_time_ms": float(df['response_time_ms'].quantile(0.5)),
            "p95_response_time_ms": float(df['response_time_ms'].quantile(0.95)),
            "p99_response_time_ms": float(df['response_time_ms'].quantile(0.99)),
            "by_endpoint": df.groupby('endpoint').agg({
                'response_time_ms': ['count', 'mean'],
                'success': 'mean'
            }).to_dict(),
            "by_status_code": df['status_code'].value_counts().to_dict()
        }


class AlertManager:
    """Manages system alerts and notifications."""
    
    def __init__(self, alerts_file: str = "./data/enterprise/alerts.json"):
        """Initialize alert manager."""
        self.alerts_file = Path(alerts_file)
        self.alerts_file.parent.mkdir(parents=True, exist_ok=True)
        self.alert_handlers = []
    
    def register_handler(self, handler: Callable[[Dict], None]):
        """Register an alert handler function."""
        self.alert_handlers.append(handler)
    
    def create_alert(
        self,
        severity: str,
        title: str,
        message: str,
        category: str = "system",
        metadata: Optional[Dict] = None
    ):
        """Create a new alert."""
        alert = {
            "alert_id": self._generate_alert_id(),
            "timestamp": datetime.now().isoformat(),
            "severity": severity,
            "title": title,
            "message": message,
            "category": category,
            "metadata": metadata or {},
            "acknowledged": False,
            "resolved": False
        }
        
        # Save alert
        self._save_alert(alert)
        
        # Notify handlers
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")
        
        logger.warning(f"[ALERT] {severity.upper()}: {title}")
    
    def _generate_alert_id(self) -> str:
        """Generate unique alert ID."""
        import uuid
        return str(uuid.uuid4())
    
    def _save_alert(self, alert: Dict):
        """Save alert to file."""
        alerts = self._load_alerts()
        alerts.append(alert)
        
        with open(self.alerts_file, 'w') as f:
            json.dump(alerts, f, indent=2)
    
    def _load_alerts(self) -> List[Dict]:
        """Load all alerts."""
        if not self.alerts_file.exists():
            return []
        
        with open(self.alerts_file, 'r') as f:
            return json.load(f)
    
    def get_active_alerts(self) -> List[Dict]:
        """Get all active (unresolved) alerts."""
        alerts = self._load_alerts()
        return [a for a in alerts if not a['resolved']]
    
    def acknowledge_alert(self, alert_id: str):
        """Acknowledge an alert."""
        alerts = self._load_alerts()
        
        for alert in alerts:
            if alert['alert_id'] == alert_id:
                alert['acknowledged'] = True
                alert['acknowledged_at'] = datetime.now().isoformat()
                break
        
        with open(self.alerts_file, 'w') as f:
            json.dump(alerts, f, indent=2)
    
    def resolve_alert(self, alert_id: str):
        """Resolve an alert."""
        alerts = self._load_alerts()
        
        for alert in alerts:
            if alert['alert_id'] == alert_id:
                alert['resolved'] = True
                alert['resolved_at'] = datetime.now().isoformat()
                break
        
        with open(self.alerts_file, 'w') as f:
            json.dump(alerts, f, indent=2)


class HealthCheckManager:
    """Manages health checks for various system components."""
    
    def __init__(self):
        """Initialize health check manager."""
        self.checks = {}
    
    def register_check(self, name: str, check_func: Callable[[], bool], interval_seconds: int = 60):
        """Register a health check."""
        self.checks[name] = {
            "func": check_func,
            "interval": interval_seconds,
            "last_check": None,
            "status": None,
            "error": None
        }
    
    def run_checks(self) -> Dict[str, Any]:
        """Run all health checks."""
        results = {}
        overall_healthy = True
        
        for name, check in self.checks.items():
            try:
                status = check['func']()
                check['last_check'] = datetime.now().isoformat()
                check['status'] = status
                check['error'] = None
                
                results[name] = {
                    "healthy": status,
                    "last_check": check['last_check']
                }
                
                if not status:
                    overall_healthy = False
            
            except Exception as e:
                check['status'] = False
                check['error'] = str(e)
                results[name] = {
                    "healthy": False,
                    "error": str(e),
                    "last_check": datetime.now().isoformat()
                }
                overall_healthy = False
        
        return {
            "overall_healthy": overall_healthy,
            "checks": results,
            "timestamp": datetime.now().isoformat()
        }
