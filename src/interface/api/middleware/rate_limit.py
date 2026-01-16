"""
Rate Limiting Middleware.

Implements request rate limiting to prevent abuse.
"""

import os
from slowapi import Limiter
from slowapi.util import get_remote_address
from fastapi import Request

# Rate limit configuration
RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "true").lower() == "true"
RATE_LIMIT_DEFAULT = os.getenv("RATE_LIMIT_DEFAULT", "10/minute")

# Rate limit tiers
RATE_LIMITS = {
    "free": "10/minute",
    "pro": "100/minute",
    "enterprise": "1000/minute"
}


def get_rate_limit_key(request: Request) -> str:
    """
    Get rate limit key from request.
    
    Uses user ID if authenticated, otherwise IP address.
    
    Args:
        request: FastAPI request
        
    Returns:
        Rate limit key
    """
    # Try to get user from request state (set by auth middleware)
    user = getattr(request.state, "user", None)
    
    if user:
        return f"user:{user.get('username')}"
    
    # Fall back to IP address
    return get_remote_address(request)


from typing import Optional

def get_user_rate_limit(request: Optional[Request] = None) -> str:
    """Get rate limit string for the current user."""
    if not RATE_LIMIT_ENABLED:
        return "10000/minute"
    
    if request is None:
        return RATE_LIMIT_DEFAULT
        
    user = getattr(request.state, "user", None)
    if user:
        # User might be a dict or a model (if using real DB in some tests)
        tier = user.get("tier") if isinstance(user, dict) else getattr(user, "tier", "free")
        return RATE_LIMITS.get(tier or "free", RATE_LIMIT_DEFAULT)
    
    return RATE_LIMIT_DEFAULT


def setup_rate_limiter() -> Limiter:
    """
    Setup and configure rate limiter.
    """
    # We use a custom key function that includes the user ID
    # And we use get_user_rate_limit as the default limit provider
    limiter = Limiter(
        key_func=get_rate_limit_key,
        default_limits=[get_user_rate_limit] if RATE_LIMIT_ENABLED else []
    )
    
    return limiter


# Create limiter instance
limiter = setup_rate_limiter()

# Truly disable rate limiting if the flag is set to false
# This prevents @limiter.limit decorators from doing anything during tests
if not RATE_LIMIT_ENABLED:
    def disabled_limit(limit_value, **kwargs):
        def decorator(f):
            return f
        return decorator
    limiter.limit = disabled_limit
