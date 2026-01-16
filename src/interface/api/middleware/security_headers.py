"""
Security Headers Middleware.

Adds security-related HTTP headers to all responses.
"""

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Middleware that adds security headers to every response.
    """
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        
        # Prevent browsers from performing MIME-type sniffing
        response.headers["X-Content-Type-Options"] = "nosniff"
        
        # Prevent the browser from rendering a page in a frame (Clickjacking protection)
        response.headers["X-Frame-Options"] = "DENY"
        
        # Enable Browser XSS Filtering
        response.headers["X-XSS-Protection"] = "1; mode=block"
        
        # Strict-Transport-Security (HSTS) - Force HTTPS
        # Only applies to production/HTTPS, but good practice
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
        
        # Content-Security-Policy (CSP) - Basic restrictive policy
        # Note: This might need adjustment based on frontend requirements
        csp_policy = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net; "
            "img-src 'self' data: https:; "
            "font-src 'self' https://fonts.gstatic.com; "
            "connect-src 'self' https://api.openai.com https://api.anthropic.com; "
            "frame-ancestors 'none';"
        )
        response.headers["Content-Security-Policy"] = csp_policy
        
        # Referrer-Policy
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        
        # Permissions-Policy
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        
        return response
