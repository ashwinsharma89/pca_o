"""
Prometheus Metrics Exporter
Export application metrics in Prometheus format
"""

from prometheus_client import Counter, Histogram, Gauge, generate_latest, REGISTRY
from typing import Dict, Any
from fastapi import Response
from loguru import logger

# API Metrics
api_requests_total = Counter(
    'pca_agent_api_requests_total',
    'Total API requests',
    ['method', 'endpoint', 'status_code']
)

api_response_time = Histogram(
    'pca_agent_api_response_time',
    'API response time in seconds',
    ['method', 'endpoint']
)

# Error Metrics
errors_total = Counter(
    'pca_agent_errors_total',
    'Total errors',
    ['error_type']
)

# Cache Metrics
cache_hits_total = Counter(
    'pca_agent_cache_hits_total',
    'Total cache hits',
    ['cache_type']
)

cache_misses_total = Counter(
    'pca_agent_cache_misses_total',
    'Total cache misses',
    ['cache_type']
)

# Database Metrics
database_query_duration = Histogram(
    'pca_agent_database_query_duration',
    'Database query duration in seconds',
    ['query_type']
)

db_connections_active = Gauge(
    'pca_agent_db_connections_active',
    'Active database connections'
)

db_connections_max = Gauge(
    'pca_agent_db_connections_max',
    'Maximum database connections'
)

# LLM Metrics
llm_latency = Histogram(
    'pca_agent_llm_latency',
    'LLM API latency in seconds',
    ['provider', 'model']
)

llm_tokens_used = Counter(
    'pca_agent_llm_tokens_used',
    'Total LLM tokens used',
    ['provider', 'model']
)

llm_cost = Counter(
    'pca_agent_llm_cost',
    'Total LLM API cost in USD',
    ['provider', 'model']
)

llm_errors_total = Counter(
    'pca_agent_llm_errors_total',
    'Total LLM errors',
    ['provider', 'error_type']
)

# User Metrics
users_active = Gauge(
    'pca_agent_users_active',
    'Currently active users'
)

user_sessions_total = Counter(
    'pca_agent_user_sessions_total',
    'Total user sessions'
)

# Knowledge Base Metrics
kb_documents_total = Gauge(
    'pca_agent_kb_documents_total',
    'Total documents in knowledge base'
)

kb_refresh_total = Counter(
    'pca_agent_kb_refresh_total',
    'Total knowledge base refreshes'
)

class PrometheusMetrics:
    """Prometheus metrics manager."""
    
    @staticmethod
    def track_api_request(method: str, endpoint: str, status_code: int, duration: float):
        """Track API request."""
        api_requests_total.labels(
            method=method,
            endpoint=endpoint,
            status_code=str(status_code)
        ).inc()
        
        api_response_time.labels(
            method=method,
            endpoint=endpoint
        ).observe(duration)
    
    @staticmethod
    def track_error(error_type: str):
        """Track error."""
        errors_total.labels(error_type=error_type).inc()
    
    @staticmethod
    def track_cache_hit(cache_type: str = "default"):
        """Track cache hit."""
        cache_hits_total.labels(cache_type=cache_type).inc()
    
    @staticmethod
    def track_cache_miss(cache_type: str = "default"):
        """Track cache miss."""
        cache_misses_total.labels(cache_type=cache_type).inc()
    
    @staticmethod
    def track_database_query(query_type: str, duration: float):
        """Track database query."""
        database_query_duration.labels(query_type=query_type).observe(duration)
    
    @staticmethod
    def set_db_connections(active: int, max_connections: int):
        """Set database connection metrics."""
        db_connections_active.set(active)
        db_connections_max.set(max_connections)
    
    @staticmethod
    def track_llm_call(
        provider: str,
        model: str,
        latency: float,
        tokens: int,
        cost: float
    ):
        """Track LLM API call."""
        llm_latency.labels(provider=provider, model=model).observe(latency)
        llm_tokens_used.labels(provider=provider, model=model).inc(tokens)
        llm_cost.labels(provider=provider, model=model).inc(cost)
    
    @staticmethod
    def track_llm_error(provider: str, error_type: str):
        """Track LLM error."""
        llm_errors_total.labels(provider=provider, error_type=error_type).inc()
    
    @staticmethod
    def set_active_users(count: int):
        """Set active users count."""
        users_active.set(count)
    
    @staticmethod
    def track_user_session():
        """Track user session."""
        user_sessions_total.inc()
    
    @staticmethod
    def set_kb_documents(count: int):
        """Set knowledge base document count."""
        kb_documents_total.set(count)
    
    @staticmethod
    def track_kb_refresh():
        """Track knowledge base refresh."""
        kb_refresh_total.inc()
    
    @staticmethod
    def get_metrics() -> Response:
        """Get metrics in Prometheus format."""
        return Response(
            content=generate_latest(REGISTRY),
            media_type="text/plain"
        )


# Global instance
prometheus_metrics = PrometheusMetrics()

logger.info("✅ Prometheus metrics initialized")
