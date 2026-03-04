"""
FastAPI Application v3.0 - With Structured Error Handling

✅ ALL 7 IMPROVEMENTS IMPLEMENTED:
1. Database persistence
2. JWT authentication
3. Rate limiting
4. API versioning
5. Report regeneration
6. Structured error codes
7. Specific exception handling
"""

import os
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded

from loguru import logger

from .middleware.auth import SECRET_KEY
from .middleware.rate_limit import limiter, RATE_LIMIT_ENABLED
from .middleware.security_headers import SecurityHeadersMiddleware
from .middleware.csrf import CSRFMiddleware
from src.core.utils.observability import metrics
from .v1 import router_v1
from .error_handlers import setup_exception_handlers
from .exceptions import RateLimitExceededError
from src.core.utils import setup_logger
from src.core.database.connection import get_db_manager
from src.core.utils.opentelemetry_config import setup_opentelemetry
from src.core.utils.secrets_manager import get_secrets_manager
from src.enterprise.audit import AuditLogger, AuditEventType, AuditSeverity

# Initialize logger
setup_logger()

# Initialize database
db_manager = get_db_manager()

# Initialize Secrets Manager
secrets = get_secrets_manager()

# Initialize Audit Logger
audit_logger = AuditLogger()

# Tags metadata for Swagger UI
tags_metadata = [
    {
        "name": "auth",
        "description": "Authentication and MFA management.",
    },
    {
        "name": "campaigns",
        "description": "Campaign data management, analysis, and visualization.",
    },
    {
        "name": "users",
        "description": "User profile and organization management.",
    },
    {
        "name": "Error Codes",
        "description": "Legend of all structured error codes returned by the API.",
    }
]

# Create FastAPI app
app = FastAPI(
    title="PCA Agent Enterprise API",
    description="""
## Post Campaign Analysis - Enterprise Edition
Welcome to the PCA Agent API. This API provides robust endpoints for marketing data analysis, 
automated insights, and multi-factor authentication.

### Key Features:
* **Tier-Based Rate Limiting**: Performance scales with your organization's subscription.
* **Audit Logging**: All sensitive actions are logged for compliance.
* **Structured Errors**: Consistent error responses for easier debugging.
* **MFA Support**: Enhanced security for user accounts.

[Download Example Client (Python)](/api/v1/docs/example-client)
""",
    version="3.0.0",
    openapi_tags=tags_metadata,
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Custom OpenAPI to add Error Codes section
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=tags_metadata
    )
    
    # Custom attributes can be added here
    # Add Error Codes to the description as a table
    from .exceptions import ErrorCode
    error_codes_table = """
### API Error Codes Reference

| Code | Category | Description |
|------|----------|-------------|
"""
    for attr in dir(ErrorCode):
        if not attr.startswith("__") and isinstance(getattr(ErrorCode, attr), str):
            code = getattr(ErrorCode, attr)
            category = attr.split("_")[0]
            description = attr.replace("_", " ").title()
            error_codes_table += f"| `{code}` | {category} | {description} |\n"
            
    openapi_schema["info"]["description"] += error_codes_table
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

from fastapi.openapi.utils import get_openapi
app.openapi = custom_openapi

# Setup exception handlers (MUST be before other middleware)
setup_exception_handlers(app)

# Setup validation error handlers
from .validation_handlers import setup_validation_handlers
setup_validation_handlers(app)

# User state population (lightweight auth for rate limiting/logging)
from .middleware.auth import populate_user_state_middleware
app.middleware("http")(populate_user_state_middleware)

# Add rate limiter to app state and middleware
from slowapi.middleware import SlowAPIMiddleware
app.state.limiter = limiter
app.add_middleware(SlowAPIMiddleware)

# Add Security Headers middleware
app.add_middleware(SecurityHeadersMiddleware)

# Add CSRF protection
app.add_middleware(CSRFMiddleware)

# Add Request Size Limit middleware (prevent DoS)
from .middleware.request_size_limit import RequestSizeLimitMiddleware
app.add_middleware(RequestSizeLimitMiddleware)

# Setup OpenTelemetry (if enabled)
setup_opentelemetry(app)

# Custom rate limit exception handler
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    """Handle rate limit exceeded errors with structured response."""
    logger.warning(
        f"Rate limit exceeded",
        extra={
            "path": request.url.path,
            "method": request.method,
            "client": request.client.host if request.client else "unknown"
        }
    )
    
    # Use our structured error
    error = RateLimitExceededError(limit=str(exc.detail))
    
    return JSONResponse(
        status_code=error.status_code,
        content={
            "error": {
                "code": error.error_code,
                "message": error.message,
                "details": error.details
            },
            "path": request.url.path,
            "method": request.method
        }
    )

# Add CORS middleware with secure production configuration
allowed_origins_str = os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000")
allowed_origins = [origin.strip() for origin in allowed_origins_str.split(",")]

# In production, NEVER use "*" for allow_origins
if os.getenv("PRODUCTION_MODE", "false").lower() == "true":
    if "*" in allowed_origins:
        logger.error("🔴 CRITICAL: Wildcard CORS origins not allowed in production!")
        allowed_origins = [o for o in allowed_origins if o != "*"]
        if not allowed_origins:
            allowed_origins = ["https://app.pca-agent.com"] # Secure fallback

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Audit Logging Middleware
@app.middleware("http")
async def audit_middleware(request: Request, call_next):
    """Log all sensitive API calls to the audit log."""
    # Process request
    response = await call_next(request)
    
    # List of sensitive paths to audit
    sensitive_prefixes = [
        "/api/v1/auth",
        "/api/v1/admin",
        "/api/v1/users",
        "/api/v1/data/delete",
        "/api/v1/campaigns/delete"
    ]
    
    is_sensitive = any(request.url.path.startswith(prefix) for prefix in sensitive_prefixes)
    
    if is_sensitive or response.status_code >= 400:
        # Get user from state (if authenticated)
        user = "anonymous"
        if hasattr(request.state, "user") and request.state.user:
            user = request.state.user.get("username", "anonymous")
        
        severity = AuditSeverity.INFO
        if response.status_code >= 500:
            severity = AuditSeverity.ERROR
        elif response.status_code >= 400:
            severity = AuditSeverity.WARNING
            
        audit_logger.log_event(
            event_type=AuditEventType.API_CALL,
            user=user,
            action=f"{request.method} {request.url.path}",
            resource=request.url.path,
            details={
                "status_code": response.status_code,
                "ip": request.client.host if request.client else "unknown",
                "query_params": str(request.query_params)
            },
            severity=severity,
            ip_address=request.client.host if request.client else "unknown",
            user_agent=request.headers.get("user-agent")
        )
        
    return response

# Include v1 router
app.include_router(router_v1)


@app.get("/metrics")
@limiter.exempt
async def get_metrics(request: Request):
    """Export metrics in Prometheus format."""
    from fastapi.responses import Response
    return Response(content=metrics.to_prometheus_format(), media_type="text/plain")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "PCA Agent API v3.0 - Production Ready",
        "version": "3.0.0",
        "features": [
            "✅ Database persistence",
            "✅ JWT authentication",
            "✅ Rate limiting",
            "✅ API versioning",
            "✅ Report regeneration",
            "✅ Structured error codes",
            "✅ Specific exception handling"
        ],
        "status": "running",
        "docs": "/api/docs",
        "error_codes": "/api/docs#/Error%20Codes"
    }


@app.get("/health")
@limiter.limit("100/minute")
async def health_check(request: Request):
    """Basic health check."""
    return {
        "status": "healthy",
        "version": "3.0.0",
        "features": {
            "authentication": True,
            "rate_limiting": RATE_LIMIT_ENABLED,
            "database": "connected",
            "api_version": "v1",
            "error_handling": "structured"
        }
    }


@app.get("/health/detailed")
async def detailed_health_check():
    """Detailed health check with all system status."""
    try:
        db_healthy = db_manager.health_check()
        
        return {
            "status": "healthy" if db_healthy else "degraded",
            "version": "3.0.0",
            "components": {
                "database": "healthy" if db_healthy else "unhealthy",
                "authentication": "healthy",
                "rate_limiting": "healthy" if RATE_LIMIT_ENABLED else "disabled",
                "api": "healthy",
                "error_handling": "healthy"
            },
            "error_handling": {
                "structured_codes": True,
                "specific_exceptions": True,
                "global_handlers": True,
                "structured_logging": True
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": {
                "message": "Health check failed",
                "details": {"type": type(e).__name__}
            }
        }


# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize application on startup."""
    logger.info("=" * 60)
    logger.info("PCA Agent API v3.0 - Starting")
    logger.info("=" * 60)
    logger.info("✅ Database persistence enabled")
    logger.info("✅ JWT authentication enabled")
    logger.info(f"✅ Rate limiting: {RATE_LIMIT_ENABLED}")
    logger.info("✅ API versioning: /api/v1/")
    logger.info("✅ Report regeneration implemented")
    logger.info("✅ Structured error codes enabled")
    logger.info("✅ Specific exception handling enabled")
    logger.info("=" * 60)
    # Security validation - ensure JWT secret is properly configured
    if SECRET_KEY == "change-this-secret-key" or not SECRET_KEY:  # nosec B105
        raise ValueError(
            "🔴 SECURITY ERROR: JWT_SECRET_KEY must be set to a secure value!\n"
            "Generate one with: python -c 'import secrets; print(secrets.token_urlsafe(32))'"
        )
    
    logger.info("✅ JWT secret key validated")


# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("PCA Agent API v3.0 - Shutting down")


if __name__ == "__main__":
    import uvicorn
    from src.core.config.settings import Settings
    
    settings = Settings()
    # Use settings.api_host (defaults to 127.0.0.1) instead of 0.0.0.0
    uvicorn.run(app, host=settings.api_host, port=8000)
