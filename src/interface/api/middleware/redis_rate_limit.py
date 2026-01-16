"""
Redis-based Rate Limiting Middleware for FastAPI.

Replaces in-memory slowapi with distributed Redis rate limiting.
"""

from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from typing import Callable
import time

from src.core.utils.redis_rate_limiter import get_redis_limiter, get_llm_limiter
from src.interface.api.exceptions import RateLimitExceededError
from loguru import logger


class RedisRateLimitMiddleware:
    """
    FastAPI middleware for Redis-based rate limiting.
    
    Provides distributed rate limiting across multiple instances.
    """
    
    def __init__(self, app):
        self.app = app
        self.redis_limiter = get_redis_limiter()
    
    async def __call__(self, scope, receive, send):
        """Process request with rate limiting."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        request = Request(scope, receive)
        
        # Get user identifier
        user_id = self._get_user_id(request)
        
        # Get rate limit for endpoint
        limit, window = self._get_endpoint_limit(request)
        
        if limit > 0:
            # Check rate limit
            allowed, info = self.redis_limiter.check_rate_limit(
                identifier=user_id,
                resource=f"api:{request.url.path}",
                limit=limit,
                window_seconds=window
            )
            
            if not allowed:
                # Rate limit exceeded
                response = JSONResponse(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    content={
                        "error": {
                            "code": "RATE_5001",
                            "message": "Rate limit exceeded",
                            "details": {
                                "limit": info["limit"],
                                "window_seconds": info["window_seconds"],
                                "reset_at": info["reset_at"],
                                "retry_after": info["reset_at"] - int(time.time())
                            }
                        }
                    },
                    headers={
                        "X-RateLimit-Limit": str(info["limit"]),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(info["reset_at"]),
                        "Retry-After": str(info["reset_at"] - int(time.time()))
                    }
                )
                
                await response(scope, receive, send)
                return
            
            # Add rate limit headers to response
            async def send_with_headers(message):
                if message["type"] == "http.response.start":
                    headers = list(message.get("headers", []))
                    headers.extend([
                        (b"x-ratelimit-limit", str(info["limit"]).encode()),
                        (b"x-ratelimit-remaining", str(info["remaining"]).encode()),
                        (b"x-ratelimit-reset", str(info["reset_at"]).encode()),
                    ])
                    message["headers"] = headers
                
                await send(message)
            
            await self.app(scope, receive, send_with_headers)
        else:
            await self.app(scope, receive, send)
    
    def _get_user_id(self, request: Request) -> str:
        """Get user identifier from request."""
        # Try to get from authenticated user
        user = getattr(request.state, "user", None)
        if user:
            return f"user:{user.get('username', user.get('id', 'unknown'))}"
        
        # Fall back to IP address
        client_ip = request.client.host if request.client else "unknown"
        return f"ip:{client_ip}"
    
    def _get_endpoint_limit(self, request: Request) -> tuple[int, int]:
        """
        Get rate limit for endpoint.
        
        Returns:
            (limit, window_seconds)
        """
        path = request.url.path
        
        # Define endpoint-specific limits
        endpoint_limits = {
            "/api/v1/auth/login": (100, 60),  # Increased to 100 per minute for E2E testing
            "/api/v1/auth/register": (10, 3600),  # Increased to 10 per hour
            "/api/v1/campaigns": (100, 60),  # 100 per minute
            "/api/v1/users": (50, 60),  # 50 per minute
        }
        
        # Check for exact match
        if path in endpoint_limits:
            return endpoint_limits[path]
        
        # Check for prefix match
        for endpoint, (limit, window) in endpoint_limits.items():
            if path.startswith(endpoint):
                return limit, window
        
        # Default limit
        return 100, 60  # 100 per minute


def rate_limit_decorator(limit: int, window: int = 60):
    """
    Decorator for endpoint-specific rate limiting.
    
    Args:
        limit: Maximum requests allowed
        window: Time window in seconds
        
    Usage:
        @app.get("/endpoint")
        @rate_limit_decorator(limit=10, window=60)
        async def endpoint():
            pass
    """
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            # Get request from kwargs
            request = kwargs.get("request")
            if not request:
                # Try to find request in args
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
            
            if request:
                redis_limiter = get_redis_limiter()
                
                # Get user identifier
                user = getattr(request.state, "user", None)
                if user:
                    user_id = f"user:{user.get('username', 'unknown')}"
                else:
                    user_id = f"ip:{request.client.host if request.client else 'unknown'}"
                
                # Check rate limit
                allowed, info = redis_limiter.check_rate_limit(
                    identifier=user_id,
                    resource=f"endpoint:{func.__name__}",
                    limit=limit,
                    window_seconds=window
                )
                
                if not allowed:
                    raise RateLimitExceededError(
                        limit=f"{limit}/{window}s",
                        details={
                            "reset_at": info["reset_at"],
                            "retry_after": info["reset_at"] - int(time.time())
                        }
                    )
            
            return await func(*args, **kwargs)
        
        return wrapper
    return decorator


def check_llm_rate_limit(user_id: str, provider: str, tier: str = "free"):
    """
    Check LLM rate limit before making API call.
    
    Args:
        user_id: User identifier
        provider: LLM provider (openai, anthropic, gemini)
        tier: User tier (free, pro, enterprise)
        
    Raises:
        RateLimitExceededError: If rate limit exceeded
    """
    llm_limiter = get_llm_limiter()
    
    allowed, info = llm_limiter.check_llm_limit(user_id, provider, tier)
    
    if not allowed:
        logger.warning(f"LLM rate limit exceeded: {user_id} on {provider}")
        raise RateLimitExceededError(
            limit=f"{info['limit']}/{info['window_seconds']}s",
            details={
                "provider": provider,
                "reset_at": info["reset_at"],
                "retry_after": info["reset_at"] - int(time.time())
            }
        )
    
    logger.debug(
        f"LLM rate limit check passed: {user_id} on {provider} "
        f"({info['current']}/{info['limit']})"
    )
