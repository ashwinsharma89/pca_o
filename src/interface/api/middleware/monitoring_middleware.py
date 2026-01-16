"""
Monitoring Middleware for FastAPI
Automatically track all API requests
"""

import time
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from src.ops.observability.external_monitoring import external_monitoring

class MonitoringMiddleware(BaseHTTPMiddleware):
    """Middleware to monitor all API requests."""
    
    async def dispatch(self, request: Request, call_next):
        """Monitor request and response."""
        # Start timing
        start_time = time.time()
        
        # Get request details
        method = request.method
        path = request.url.path
        user_id = None
        
        # Get user ID if authenticated
        if hasattr(request.state, 'user'):
            user_id = request.state.user.get('user_id')
        
        try:
            # Process request
            response = await call_next(request)
            
            # Calculate duration
            duration_ms = (time.time() - start_time) * 1000
            
            # Track request
            external_monitoring.track_api_request(
                endpoint=path,
                method=method,
                status_code=response.status_code,
                duration_ms=duration_ms,
                user_id=user_id
            )
            
            # Add monitoring headers
            response.headers["X-Response-Time"] = f"{duration_ms:.2f}ms"
            
            return response
        
        except Exception as e:
            # Track error
            duration_ms = (time.time() - start_time) * 1000
            
            external_monitoring.track_error(e, {
                "endpoint": path,
                "method": method,
                "user_id": user_id,
                "duration_ms": duration_ms
            })
            
            # Track failed request
            external_monitoring.track_api_request(
                endpoint=path,
                method=method,
                status_code=500,
                duration_ms=duration_ms,
                user_id=user_id
            )
            
            raise
