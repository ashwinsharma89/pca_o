"""
Request Size Limit Middleware

Prevents DoS attacks by limiting request body size.
"""

from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import os

# Max request size in bytes (default: 110MB to allow 100MB files + metadata)
MAX_REQUEST_SIZE = int(os.getenv("MAX_REQUEST_SIZE_MB", "110")) * 1024 * 1024


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
    """
    Middleware that enforces maximum request body size.
    
    Prevents memory exhaustion from oversized requests.
    """
    
    async def dispatch(self, request: Request, call_next):
        # Check Content-Length header
        content_length = request.headers.get("content-length")
        
        if content_length:
            content_length = int(content_length)
            if content_length > MAX_REQUEST_SIZE:
                size_mb = content_length / (1024 * 1024)
                max_mb = MAX_REQUEST_SIZE / (1024 * 1024)
                raise HTTPException(
                    status_code=413,
                    detail=f"Request too large: {size_mb:.1f}MB. Maximum allowed: {max_mb:.0f}MB"
                )
        
        response: Response = await call_next(request)
        return response
