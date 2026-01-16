"""
CSRF Protection Middleware.

Verifies X-CSRF-Token header for state-changing requests (POST, PUT, DELETE, PATCH).
"""

from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import os
from loguru import logger

class CSRFMiddleware(BaseHTTPMiddleware):
    """
    Middleware that enforces CSRF protection for non-safe HTTP methods.
    """
    
    # HTTP methods that require CSRF protection
    UNSAFE_METHODS = {"POST", "PUT", "DELETE", "PATCH"}
    
    # Whitelisted paths (e.g., public endpoints that don't need CSRF)
    WHITELIST = {
        "/api/v1/auth/login",
        "/api/v1/auth/register",
        "/api/docs",
        "/api/redoc",
        "/openapi.json"
    }

    async def dispatch(self, request: Request, call_next):
        # 1. Skip safe methods
        if request.method not in self.UNSAFE_METHODS:
            return await call_next(request)
        
        # 2. Skip whitelisted paths
        path = request.url.path
        if any(path.startswith(w) for w in self.WHITELIST):
            return await call_next(request)
        
        # 3. Check for CSRF token in header
        csrf_token = request.headers.get("X-CSRF-Token")
        
        # In a real enterprise app, this would be validated against a session-stored token.
        # For this implementation, we enforce the presence of the header to prevent 
        # simple form-based cross-site attacks.
        
        # If the user is authenticated via JWT in headers, CSRF is naturally mitigated 
        # because browsers don't automatically send headers like they do with cookies.
        # However, we add this as an extra layer of defense-in-depth as requested.
        
        if not csrf_token:
            logger.warning(f"CSRF attempt detected: Missing X-CSRF-Token header for {path}")
            return JSONResponse(
                status_code=403,
                content={
                    "success": False,
                    "error": {
                        "message": "CSRF token missing. Please include X-CSRF-Token in your headers.",
                        "code": "CSRF_TOKEN_MISSING"
                    }
                }
            )
            
        # In production, you would verify the token here:
        # if not self.verify_csrf_token(csrf_token, request):
        #     return JSONResponse(...)

        return await call_next(request)
