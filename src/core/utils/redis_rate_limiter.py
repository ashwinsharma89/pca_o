"""
Redis-based Rate Limiting for API and LLM calls.

Provides distributed rate limiting with Redis backend.
"""

import os
import time
from typing import Optional, Tuple
from datetime import datetime, timedelta
from datetime import datetime, timedelta
import redis
from loguru import logger
from src.core.exceptions import RateLimitExceededError

# ... (Existing RedisRateLimiter class) ...

def check_llm_rate_limit(user_id: str, provider: str, tier: str = "free"):
    """
    Check LLM rate limit before making API call.
    Raised from core/utils to verify limits without API middleware dependency.
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



class RedisRateLimiter:
    """
    Redis-based rate limiter with sliding window algorithm.
    
    Supports distributed rate limiting across multiple instances.
    """
    
    def __init__(
        self,
        redis_host: str = None,
        redis_port: int = None,
        redis_db: int = 0,
        redis_password: str = None,
        enabled: bool = True
    ):
        """
        Initialize Redis rate limiter.
        
        Args:
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
            redis_password: Redis password
            enabled: Enable/disable rate limiting
        """
        self.enabled = enabled and os.getenv("REDIS_ENABLED", "false").lower() == "true"
        
        if not self.enabled:
            logger.info("Redis rate limiting disabled")
            return
        
        # Get Redis configuration from environment
        self.redis_host = redis_host or os.getenv("REDIS_HOST", "localhost")
        self.redis_port = redis_port or int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = redis_db
        self.redis_password = redis_password or os.getenv("REDIS_PASSWORD")
        
        # Initialize Redis connection
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=True,
                socket_connect_timeout=5
            )
            
            # Test connection
            self.redis_client.ping()
            logger.info(f"Redis rate limiter connected: {self.redis_host}:{self.redis_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.enabled = False
    
    def _get_key(self, identifier: str, resource: str) -> str:
        """Generate Redis key for rate limit."""
        return f"rate_limit:{resource}:{identifier}"
    
    def check_rate_limit(
        self,
        identifier: str,
        resource: str,
        limit: int,
        window_seconds: int
    ) -> Tuple[bool, dict]:
        """
        Check if request is within rate limit.
        
        Uses sliding window algorithm for accurate rate limiting.
        
        Args:
            identifier: User ID, IP address, or other identifier
            resource: Resource being accessed (e.g., 'api', 'llm_openai')
            limit: Maximum requests allowed in window
            window_seconds: Time window in seconds
            
        Returns:
            (allowed, info_dict) where info_dict contains:
                - remaining: Requests remaining
                - reset_at: Timestamp when limit resets
                - current: Current request count
        """
        if not self.enabled:
            # If Redis is disabled, allow all requests
            return True, {
                "remaining": limit,
                "reset_at": int(time.time() + window_seconds),
                "current": 0,
                "limit": limit
            }
        
        try:
            key = self._get_key(identifier, resource)
            now = time.time()
            window_start = now - window_seconds
            
            # Use Redis pipeline for atomic operations
            pipe = self.redis_client.pipeline()
            
            # Remove old entries outside the window
            pipe.zremrangebyscore(key, 0, window_start)
            
            # Count requests in current window
            pipe.zcard(key)
            
            # Add current request with timestamp as score
            pipe.zadd(key, {str(now): now})
            
            # Set expiration on key
            pipe.expire(key, window_seconds + 1)
            
            # Execute pipeline
            results = pipe.execute()
            current_count = results[1]  # Count before adding current request
            
            # Check if limit exceeded
            allowed = current_count < limit
            remaining = max(0, limit - current_count - 1)
            reset_at = int(now + window_seconds)
            
            info = {
                "remaining": remaining,
                "reset_at": reset_at,
                "current": current_count + 1,
                "limit": limit,
                "window_seconds": window_seconds
            }
            
            if not allowed:
                logger.warning(
                    f"Rate limit exceeded: {identifier} on {resource} "
                    f"({current_count + 1}/{limit} in {window_seconds}s)"
                )
            
            return allowed, info
            
        except Exception as e:
            logger.error(f"Rate limit check failed: {e}")
            # On error, allow request (fail open)
            return True, {
                "remaining": limit,
                "reset_at": int(time.time() + window_seconds),
                "current": 0,
                "limit": limit,
                "error": str(e)
            }
    
    def get_usage(self, identifier: str, resource: str, window_seconds: int) -> dict:
        """
        Get current usage statistics.
        
        Args:
            identifier: User ID or identifier
            resource: Resource name
            window_seconds: Time window in seconds
            
        Returns:
            Usage statistics
        """
        if not self.enabled:
            return {"current": 0, "window_seconds": window_seconds}
        
        try:
            key = self._get_key(identifier, resource)
            now = time.time()
            window_start = now - window_seconds
            
            # Remove old entries
            self.redis_client.zremrangebyscore(key, 0, window_start)
            
            # Count current requests
            current = self.redis_client.zcard(key)
            
            return {
                "current": current,
                "window_seconds": window_seconds,
                "window_start": datetime.fromtimestamp(window_start).isoformat(),
                "window_end": datetime.fromtimestamp(now).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get usage: {e}")
            return {"current": 0, "window_seconds": window_seconds, "error": str(e)}
    
    def reset_limit(self, identifier: str, resource: str) -> bool:
        """
        Reset rate limit for identifier.
        
        Args:
            identifier: User ID or identifier
            resource: Resource name
            
        Returns:
            True if successful
        """
        if not self.enabled:
            return True
        
        try:
            key = self._get_key(identifier, resource)
            self.redis_client.delete(key)
            logger.info(f"Rate limit reset: {identifier} on {resource}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to reset limit: {e}")
            return False
    
    def get_all_limits(self, identifier: str) -> dict:
        """
        Get all rate limits for an identifier.
        
        Args:
            identifier: User ID or identifier
            
        Returns:
            Dictionary of resource -> usage
        """
        if not self.enabled:
            return {}
        
        try:
            pattern = f"rate_limit:*:{identifier}"
            keys = self.redis_client.keys(pattern)
            
            limits = {}
            for key in keys:
                parts = key.split(":")
                if len(parts) >= 3:
                    resource = parts[1]
                    count = self.redis_client.zcard(key)
                    limits[resource] = count
            
            return limits
            
        except Exception as e:
            logger.error(f"Failed to get all limits: {e}")
            return {}


class LLMRateLimiter:
    """
    Rate limiter specifically for LLM API calls.
    
    Tracks and limits calls to different LLM providers.
    """
    
    # Default limits per provider (requests per minute)
    DEFAULT_LIMITS = {
        "openai": {
            "free": 3,
            "pro": 60,
            "enterprise": 500
        },
        "anthropic": {
            "free": 5,
            "pro": 50,
            "enterprise": 500
        },
        "gemini": {
            "free": 15,
            "pro": 60,
            "enterprise": 1000
        }
    }
    
    def __init__(self, redis_limiter: RedisRateLimiter = None):
        """
        Initialize LLM rate limiter.
        
        Args:
            redis_limiter: Redis rate limiter instance
        """
        self.redis_limiter = redis_limiter or RedisRateLimiter()
    
    def check_llm_limit(
        self,
        user_id: str,
        provider: str,
        tier: str = "free"
    ) -> Tuple[bool, dict]:
        """
        Check if LLM call is within rate limit.
        
        Args:
            user_id: User identifier
            provider: LLM provider (openai, anthropic, gemini)
            tier: User tier (free, pro, enterprise)
            
        Returns:
            (allowed, info_dict)
        """
        # Get limit for provider and tier
        provider_limits = self.DEFAULT_LIMITS.get(provider.lower(), {})
        limit = provider_limits.get(tier, 10)  # Default 10/min
        
        resource = f"llm_{provider.lower()}"
        
        return self.redis_limiter.check_rate_limit(
            identifier=user_id,
            resource=resource,
            limit=limit,
            window_seconds=60  # 1 minute window
        )
    
    def track_llm_call(
        self,
        user_id: str,
        provider: str,
        model: str,
        tokens: int,
        cost: float
    ):
        """
        Track LLM call metrics.
        
        Args:
            user_id: User identifier
            provider: LLM provider
            model: Model name
            tokens: Token count
            cost: Cost in USD
        """
        if not self.redis_limiter.enabled:
            return
        
        try:
            # Store call metadata
            key = f"llm_calls:{user_id}:{provider}"
            timestamp = time.time()
            
            call_data = {
                "timestamp": timestamp,
                "model": model,
                "tokens": tokens,
                "cost": cost
            }
            
            # Add to sorted set with timestamp as score
            self.redis_limiter.redis_client.zadd(
                key,
                {str(call_data): timestamp}
            )
            
            # Keep only last 24 hours
            self.redis_limiter.redis_client.zremrangebyscore(
                key,
                0,
                timestamp - 86400
            )
            
            # Set expiration
            self.redis_limiter.redis_client.expire(key, 86400)
            
        except Exception as e:
            logger.error(f"Failed to track LLM call: {e}")
    
    def get_llm_usage(self, user_id: str, hours: int = 24) -> dict:
        """
        Get LLM usage statistics.
        
        Args:
            user_id: User identifier
            hours: Number of hours to look back
            
        Returns:
            Usage statistics by provider
        """
        if not self.redis_limiter.enabled:
            return {}
        
        try:
            pattern = f"llm_calls:{user_id}:*"
            keys = self.redis_limiter.redis_client.keys(pattern)
            
            usage = {}
            window_start = time.time() - (hours * 3600)
            
            for key in keys:
                provider = key.split(":")[-1]
                
                # Get calls in window
                calls = self.redis_limiter.redis_client.zrangebyscore(
                    key,
                    window_start,
                    time.time()
                )
                
                usage[provider] = {
                    "calls": len(calls),
                    "window_hours": hours
                }
            
            return usage
            
        except Exception as e:
            logger.error(f"Failed to get LLM usage: {e}")
            return {}


# Global instances
_redis_limiter = None
_llm_limiter = None


def get_redis_limiter() -> RedisRateLimiter:
    """Get global Redis rate limiter instance."""
    global _redis_limiter
    if _redis_limiter is None:
        _redis_limiter = RedisRateLimiter()
    return _redis_limiter


def get_llm_limiter() -> LLMRateLimiter:
    """Get global LLM rate limiter instance."""
    global _llm_limiter
    if _llm_limiter is None:
        _llm_limiter = LLMRateLimiter(get_redis_limiter())
    return _llm_limiter
