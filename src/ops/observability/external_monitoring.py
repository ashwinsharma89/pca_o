"""
External Monitoring Integration
Integrate with Datadog and New Relic for APM and monitoring
"""

import os
import time
import socket
from typing import Dict, Any, Optional, List
from datetime import datetime
from functools import wraps
import requests
from loguru import logger

class DatadogMonitoring:
    """Datadog APM and monitoring integration."""
    
    def __init__(self):
        """Initialize Datadog monitoring."""
        self.api_key = os.getenv("DATADOG_API_KEY")
        self.app_key = os.getenv("DATADOG_APP_KEY")
        self.api_url = "https://api.datadoghq.com/api/v1"
        self.service_name = "pca-agent"
        self.environment = os.getenv("ENVIRONMENT", "production")
        self.hostname = socket.gethostname()
        
        if self.api_key:
            self._initialize_datadog_apm()
        else:
            logger.warning("⚠️  Datadog API key not configured")
    
    def _initialize_datadog_apm(self):
        """Initialize Datadog APM tracer."""
        try:
            from ddtrace import tracer, patch_all, config
            
            # Configure tracer
            config.service = self.service_name
            config.env = self.environment
            
            # Patch all supported libraries
            patch_all()
            
            logger.info("✅ Datadog APM initialized")
            
        except ImportError:
            logger.warning(
                "⚠️  Datadog library not installed. "
                "Install with: pip install ddtrace"
            )
    
    def send_metric(
        self,
        metric_name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None,
        metric_type: str = "gauge"
    ):
        """
        Send metric to Datadog.
        
        Args:
            metric_name: Metric name
            value: Metric value
            tags: Tags dictionary
            metric_type: Metric type (gauge, count, rate)
        """
        if not self.api_key:
            return
        
        try:
            # Format tags
            tag_list = [f"{k}:{v}" for k, v in (tags or {}).items()]
            tag_list.extend([
                f"service:{self.service_name}",
                f"env:{self.environment}",
                f"host:{self.hostname}"
            ])
            
            # Send metric
            data = {
                "series": [{
                    "metric": f"pca_agent.{metric_name}",
                    "points": [[int(time.time()), value]],
                    "type": metric_type,
                    "tags": tag_list
                }]
            }
            
            response = requests.post(
                f"{self.api_url}/series",
                headers={
                    "DD-API-KEY": self.api_key,
                    "Content-Type": "application/json"
                },
                json=data,
                timeout=5
            )
            
            if response.status_code != 202:
                logger.debug(f"Failed to send metric to Datadog: {response.status_code}")
        
        except Exception as e:
            logger.debug(f"Datadog metric error: {e}")
    
    def send_event(
        self,
        title: str,
        text: str,
        alert_type: str = "info",
        tags: Optional[Dict[str, str]] = None
    ):
        """
        Send event to Datadog.
        
        Args:
            title: Event title
            text: Event description
            alert_type: Alert type (info, warning, error, success)
            tags: Tags dictionary
        """
        if not self.api_key:
            return
        
        try:
            tag_list = [f"{k}:{v}" for k, v in (tags or {}).items()]
            tag_list.extend([
                f"service:{self.service_name}",
                f"env:{self.environment}"
            ])
            
            data = {
                "title": title,
                "text": text,
                "alert_type": alert_type,
                "tags": tag_list,
                "host": self.hostname
            }
            
            response = requests.post(
                f"{self.api_url}/events",
                headers={
                    "DD-API-KEY": self.api_key,
                    "Content-Type": "application/json"
                },
                json=data,
                timeout=5
            )
            
            if response.status_code not in [200, 202]:
                logger.debug(f"Failed to send event to Datadog: {response.status_code}")
        
        except Exception as e:
            logger.debug(f"Datadog event error: {e}")
    
    def track_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Track error in Datadog.
        
        Args:
            error: Exception object
            context: Additional context
        """
        self.send_event(
            title=f"Error: {type(error).__name__}",
            text=f"{str(error)}\n\nContext: {context}",
            alert_type="error",
            tags={"error_type": type(error).__name__}
        )
        
        # Increment error counter
        self.send_metric(
            "errors.count",
            1,
            tags={"error_type": type(error).__name__},
            metric_type="count"
        )
    
    def create_dashboard(self):
        """Create Datadog dashboard."""
        if not self.api_key or not self.app_key:
            logger.warning("⚠️  Datadog credentials incomplete for dashboard creation")
            return
        
        dashboard = {
            "title": "PCA Agent - Production Dashboard",
            "description": "Main monitoring dashboard for PCA Agent",
            "widgets": [
                # API Response Time
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [{
                            "q": "avg:pca_agent.api.response_time{*}",
                            "display_type": "line",
                            "style": {"palette": "dog_classic"}
                        }],
                        "title": "API Response Time (avg)"
                    }
                },
                # Request Rate
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [{
                            "q": "sum:pca_agent.api.requests{*}.as_rate()",
                            "aggregator": "sum"
                        }],
                        "title": "Request Rate (req/s)",
                        "precision": 2
                    }
                },
                # Error Rate
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [{
                            "q": "sum:pca_agent.errors.count{*}.as_rate()",
                            "display_type": "bars",
                            "style": {"palette": "warm"}
                        }],
                        "title": "Error Rate"
                    }
                },
                # Cache Hit Rate
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [{
                            "q": "(sum:pca_agent.cache.hits{*} / (sum:pca_agent.cache.hits{*} + sum:pca_agent.cache.misses{*})) * 100",
                            "aggregator": "avg"
                        }],
                        "title": "Cache Hit Rate (%)",
                        "precision": 1
                    }
                },
                # Database Query Time
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [{
                            "q": "avg:pca_agent.database.query_time{*}",
                            "display_type": "line"
                        }],
                        "title": "Database Query Time (ms)"
                    }
                },
                # LLM API Latency
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [{
                            "q": "avg:pca_agent.llm.latency{*} by {provider}",
                            "display_type": "line"
                        }],
                        "title": "LLM API Latency by Provider"
                    }
                },
                # Active Users
                {
                    "definition": {
                        "type": "query_value",
                        "requests": [{
                            "q": "sum:pca_agent.users.active{*}",
                            "aggregator": "last"
                        }],
                        "title": "Active Users"
                    }
                },
                # Memory Usage
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [{
                            "q": "avg:system.mem.used{service:pca-agent}",
                            "display_type": "area"
                        }],
                        "title": "Memory Usage"
                    }
                },
                # CPU Usage
                {
                    "definition": {
                        "type": "timeseries",
                        "requests": [{
                            "q": "avg:system.cpu.user{service:pca-agent}",
                            "display_type": "area"
                        }],
                        "title": "CPU Usage"
                    }
                },
                # Top Errors
                {
                    "definition": {
                        "type": "toplist",
                        "requests": [{
                            "q": "top(sum:pca_agent.errors.count{*} by {error_type}.as_count(), 10, 'sum', 'desc')"
                        }],
                        "title": "Top Errors"
                    }
                }
            ],
            "layout_type": "ordered",
            "notify_list": [],
            "template_variables": []
        }
        
        try:
            response = requests.post(
                f"{self.api_url}/dashboard",
                headers={
                    "DD-API-KEY": self.api_key,
                    "DD-APPLICATION-KEY": self.app_key,
                    "Content-Type": "application/json"
                },
                json=dashboard,
                timeout=10
            )
            
            if response.status_code == 200:
                dashboard_url = response.json().get("url")
                logger.info(f"✅ Datadog dashboard created: {dashboard_url}")
                return dashboard_url
            else:
                logger.warning(f"⚠️  Failed to create dashboard: {response.status_code}")
        
        except Exception as e:
            logger.error(f"Error creating Datadog dashboard: {e}")


class NewRelicMonitoring:
    """New Relic APM and monitoring integration."""
    
    def __init__(self):
        """Initialize New Relic monitoring."""
        self.license_key = os.getenv("NEW_RELIC_LICENSE_KEY")
        self.app_name = "PCA Agent"
        self.environment = os.getenv("ENVIRONMENT", "production")
        
        if self.license_key:
            self._initialize_new_relic_apm()
        else:
            logger.warning("⚠️  New Relic license key not configured")
    
    def _initialize_new_relic_apm(self):
        """Initialize New Relic APM agent."""
        try:
            import newrelic.agent
            
            # Configure agent
            config = {
                'app_name': self.app_name,
                'license_key': self.license_key,
                'environment': self.environment,
                'monitor_mode': True,
                'log_level': 'info'
            }
            
            newrelic.agent.initialize(config_file=None, environment=self.environment)
            
            logger.info("✅ New Relic APM initialized")
            
        except ImportError:
            logger.warning(
                "⚠️  New Relic library not installed. "
                "Install with: pip install newrelic"
            )
    
    def record_custom_event(
        self,
        event_type: str,
        params: Dict[str, Any]
    ):
        """
        Record custom event to New Relic.
        
        Args:
            event_type: Event type name
            params: Event parameters
        """
        if not self.license_key:
            return
        
        try:
            import newrelic.agent
            newrelic.agent.record_custom_event(event_type, params)
        
        except Exception as e:
            logger.debug(f"New Relic event error: {e}")
    
    def record_custom_metric(
        self,
        name: str,
        value: float
    ):
        """
        Record custom metric to New Relic.
        
        Args:
            name: Metric name
            value: Metric value
        """
        if not self.license_key:
            return
        
        try:
            import newrelic.agent
            newrelic.agent.record_custom_metric(f"Custom/{name}", value)
        
        except Exception as e:
            logger.debug(f"New Relic metric error: {e}")
    
    def track_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Track error in New Relic.
        
        Args:
            error: Exception object
            context: Additional context
        """
        try:
            import newrelic.agent
            newrelic.agent.notice_error(
                error=error,
                attributes=context or {}
            )
        
        except Exception as e:
            logger.debug(f"New Relic error tracking error: {e}")
    
    def add_custom_parameters(self, params: Dict[str, Any]):
        """
        Add custom parameters to current transaction.
        
        Args:
            params: Parameters to add
        """
        try:
            import newrelic.agent
            for key, value in params.items():
                newrelic.agent.add_custom_parameter(key, value)
        
        except Exception as e:
            logger.debug(f"New Relic parameter error: {e}")


class ExternalMonitoring:
    """Unified external monitoring system."""
    
    def __init__(self):
        """Initialize external monitoring."""
        self.datadog = DatadogMonitoring()
        self.new_relic = NewRelicMonitoring()
    
    def send_metric(
        self,
        metric_name: str,
        value: float,
        tags: Optional[Dict[str, str]] = None
    ):
        """
        Send metric to all configured monitoring systems.
        
        Args:
            metric_name: Metric name
            value: Metric value
            tags: Tags dictionary
        """
        self.datadog.send_metric(metric_name, value, tags)
        self.new_relic.record_custom_metric(metric_name, value)
    
    def send_event(
        self,
        title: str,
        text: str,
        alert_type: str = "info",
        tags: Optional[Dict[str, str]] = None
    ):
        """
        Send event to all configured monitoring systems.
        
        Args:
            title: Event title
            text: Event description
            alert_type: Alert type
            tags: Tags dictionary
        """
        self.datadog.send_event(title, text, alert_type, tags)
        self.new_relic.record_custom_event("PCAEvent", {
            "title": title,
            "text": text,
            "alert_type": alert_type,
            **(tags or {})
        })
    
    def track_error(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None
    ):
        """
        Track error in all configured monitoring systems.
        
        Args:
            error: Exception object
            context: Additional context
        """
        self.datadog.track_error(error, context)
        self.new_relic.track_error(error, context)
    
    def track_api_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        duration_ms: float,
        user_id: Optional[str] = None
    ):
        """
        Track API request.
        
        Args:
            endpoint: API endpoint
            method: HTTP method
            status_code: Response status code
            duration_ms: Request duration in milliseconds
            user_id: User ID (optional)
        """
        tags = {
            "endpoint": endpoint,
            "method": method,
            "status_code": str(status_code)
        }
        
        # Send metrics
        self.send_metric("api.response_time", duration_ms, tags)
        self.send_metric("api.requests", 1, tags)
        
        if status_code >= 500:
            self.send_metric("api.errors", 1, tags)
        
        # Record event in New Relic
        self.new_relic.record_custom_event("APIRequest", {
            "endpoint": endpoint,
            "method": method,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "user_id": user_id
        })
    
    def track_database_query(
        self,
        query_type: str,
        duration_ms: float,
        success: bool = True
    ):
        """
        Track database query.
        
        Args:
            query_type: Type of query (SELECT, INSERT, etc.)
            duration_ms: Query duration in milliseconds
            success: Whether query succeeded
        """
        tags = {
            "query_type": query_type,
            "success": str(success)
        }
        
        self.send_metric("database.query_time", duration_ms, tags)
        self.send_metric("database.queries", 1, tags)
    
    def track_cache_operation(
        self,
        operation: str,
        hit: bool,
        duration_ms: float = 0
    ):
        """
        Track cache operation.
        
        Args:
            operation: Operation type (get, set, delete)
            hit: Whether it was a cache hit
            duration_ms: Operation duration
        """
        tags = {"operation": operation}
        
        if operation == "get":
            if hit:
                self.send_metric("cache.hits", 1, tags)
            else:
                self.send_metric("cache.misses", 1, tags)
        
        if duration_ms > 0:
            self.send_metric("cache.operation_time", duration_ms, tags)
    
    def track_llm_call(
        self,
        provider: str,
        model: str,
        duration_ms: float,
        tokens_used: int,
        cost: float,
        success: bool = True
    ):
        """
        Track LLM API call.
        
        Args:
            provider: LLM provider (openai, anthropic, etc.)
            model: Model name
            duration_ms: Call duration
            tokens_used: Number of tokens used
            cost: API call cost
            success: Whether call succeeded
        """
        tags = {
            "provider": provider,
            "model": model,
            "success": str(success)
        }
        
        self.send_metric("llm.latency", duration_ms, tags)
        self.send_metric("llm.tokens", tokens_used, tags)
        self.send_metric("llm.cost", cost, tags)
        self.send_metric("llm.calls", 1, tags)
    
    def track_user_action(
        self,
        user_id: str,
        action: str,
        resource: Optional[str] = None
    ):
        """
        Track user action.
        
        Args:
            user_id: User ID
            action: Action performed
            resource: Resource affected (optional)
        """
        self.new_relic.record_custom_event("UserAction", {
            "user_id": user_id,
            "action": action,
            "resource": resource,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    def setup_dashboards(self):
        """Setup dashboards in all monitoring systems."""
        logger.info("Setting up monitoring dashboards...")
        
        # Create Datadog dashboard
        datadog_url = self.datadog.create_dashboard()
        if datadog_url:
            logger.info(f"Datadog dashboard: {datadog_url}")
        
        logger.info("✅ Monitoring dashboards setup complete")


# Global instance
external_monitoring = ExternalMonitoring()


def monitor(metric_name: str = None):
    """
    Decorator to monitor function execution.
    
    Args:
        metric_name: Custom metric name (optional)
    
    Usage:
        @monitor("my_function")
        def my_function():
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            success = True
            error = None
            
            try:
                result = func(*args, **kwargs)
                return result
            
            except Exception as e:
                success = False
                error = e
                raise
            
            finally:
                duration_ms = (time.time() - start_time) * 1000
                
                # Send metrics
                name = metric_name or f"{func.__module__}.{func.__name__}"
                external_monitoring.send_metric(
                    f"function.{name}.duration",
                    duration_ms,
                    tags={"success": str(success)}
                )
                
                if error:
                    external_monitoring.track_error(error, {
                        "function": name,
                        "args": str(args)[:100],
                        "kwargs": str(kwargs)[:100]
                    })
        
        return wrapper
    return decorator


def track_performance(operation: str):
    """
    Context manager to track operation performance.
    
    Usage:
        with track_performance("database_query"):
            # Your code here
            pass
    """
    class PerformanceTracker:
        def __init__(self, operation_name):
            self.operation_name = operation_name
            self.start_time = None
        
        def __enter__(self):
            self.start_time = time.time()
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            duration_ms = (time.time() - self.start_time) * 1000
            
            external_monitoring.send_metric(
                f"operation.{self.operation_name}.duration",
                duration_ms,
                tags={"success": str(exc_type is None)}
            )
            
            if exc_type:
                external_monitoring.track_error(exc_val, {
                    "operation": self.operation_name
                })
    
    return PerformanceTracker(operation)
