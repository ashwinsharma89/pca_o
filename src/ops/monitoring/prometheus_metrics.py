"""
Prometheus Metrics Exporter for FastAPI
Exposes /metrics endpoint for Prometheus scraping
"""
from prometheus_client import Counter, Histogram, Gauge, Info, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.routing import Match
import time
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# Metrics Definitions
# ============================================================================

# Request metrics
REQUEST_COUNT = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)

REQUEST_DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint'],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
)

REQUESTS_IN_PROGRESS = Gauge(
    'http_requests_in_progress',
    'Number of HTTP requests in progress',
    ['method', 'endpoint']
)

# Application metrics
ACTIVE_USERS = Gauge(
    'app_active_users',
    'Number of active users'
)

CACHE_HITS = Counter(
    'cache_hits_total',
    'Total cache hits',
    ['cache_name']
)

CACHE_MISSES = Counter(
    'cache_misses_total',
    'Total cache misses',
    ['cache_name']
)

# Database metrics
DB_QUERY_DURATION = Histogram(
    'db_query_duration_seconds',
    'Database query duration',
    ['query_type'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5]
)

DB_CONNECTIONS_ACTIVE = Gauge(
    'db_connections_active',
    'Number of active database connections'
)

# AI/LLM metrics
LLM_REQUESTS = Counter(
    'llm_requests_total',
    'Total LLM API requests',
    ['provider', 'model']
)

LLM_DURATION = Histogram(
    'llm_request_duration_seconds',
    'LLM API request duration',
    ['provider', 'model'],
    buckets=[0.5, 1, 2.5, 5, 10, 30, 60]
)

LLM_TOKENS = Counter(
    'llm_tokens_total',
    'Total LLM tokens used',
    ['provider', 'model', 'type']  # type: input, output
)

# Error metrics
ERRORS = Counter(
    'app_errors_total',
    'Total application errors',
    ['error_type', 'endpoint']
)

# App info
APP_INFO = Info('app', 'Application information')
APP_INFO.info({
    'name': 'pca-agent',
    'version': '3.0.0',
    'python_version': '3.11'
})


# ============================================================================
# Prometheus Middleware
# ============================================================================

class PrometheusMiddleware(BaseHTTPMiddleware):
    """
    Middleware to collect Prometheus metrics for all requests
    """
    
    async def dispatch(self, request: Request, call_next):
        # Get endpoint name
        endpoint = self._get_endpoint(request)
        method = request.method
        
        # Track in-progress requests
        REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).inc()
        
        # Time the request
        start_time = time.time()
        
        try:
            response = await call_next(request)
            status = response.status_code
            
            # Record metrics
            REQUEST_COUNT.labels(
                method=method,
                endpoint=endpoint,
                status=status
            ).inc()
            
            REQUEST_DURATION.labels(
                method=method,
                endpoint=endpoint
            ).observe(time.time() - start_time)
            
            return response
            
        except Exception as e:
            ERRORS.labels(
                error_type=type(e).__name__,
                endpoint=endpoint
            ).inc()
            raise
            
        finally:
            REQUESTS_IN_PROGRESS.labels(method=method, endpoint=endpoint).dec()
    
    def _get_endpoint(self, request: Request) -> str:
        """Get endpoint path template (not actual path with IDs)"""
        for route in request.app.routes:
            match, _ = route.matches(request.scope)
            if match == Match.FULL:
                return route.path
        return request.url.path


# ============================================================================
# Metrics Endpoint
# ============================================================================

async def metrics_endpoint(request: Request) -> Response:
    """
    Prometheus metrics endpoint
    
    Usage: Add to FastAPI app with:
        app.add_route("/metrics", metrics_endpoint)
    """
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


# ============================================================================
# Helper Functions for Recording Metrics
# ============================================================================

def record_cache_hit(cache_name: str = "default"):
    """Record a cache hit"""
    CACHE_HITS.labels(cache_name=cache_name).inc()


def record_cache_miss(cache_name: str = "default"):
    """Record a cache miss"""
    CACHE_MISSES.labels(cache_name=cache_name).inc()


def record_db_query(query_type: str, duration: float):
    """Record database query duration"""
    DB_QUERY_DURATION.labels(query_type=query_type).observe(duration)


def record_llm_request(provider: str, model: str, duration: float, input_tokens: int, output_tokens: int):
    """Record LLM API request"""
    LLM_REQUESTS.labels(provider=provider, model=model).inc()
    LLM_DURATION.labels(provider=provider, model=model).observe(duration)
    LLM_TOKENS.labels(provider=provider, model=model, type="input").inc(input_tokens)
    LLM_TOKENS.labels(provider=provider, model=model, type="output").inc(output_tokens)


def set_active_users(count: int):
    """Set active users gauge"""
    ACTIVE_USERS.set(count)


def set_db_connections(count: int):
    """Set active DB connections gauge"""
    DB_CONNECTIONS_ACTIVE.set(count)


# ============================================================================
# Setup Function
# ============================================================================

def setup_prometheus(app):
    """
    Setup Prometheus metrics for FastAPI app
    
    Usage:
        from src.ops.monitoring.prometheus_metrics import setup_prometheus
        setup_prometheus(app)
    """
    # Add middleware
    app.add_middleware(PrometheusMiddleware)
    
    # Add metrics endpoint
    app.add_route("/metrics", metrics_endpoint)
    
    logger.info("Prometheus metrics enabled at /metrics")
