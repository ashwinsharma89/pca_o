"""
Redis caching layer for performance optimization.
"""

import os
import json
import logging
import pickle  # nosec B403
from typing import Any, Optional, Callable
from functools import wraps
import redis
from datetime import timedelta

logger = logging.getLogger(__name__)


class RedisCache:
    """Redis cache manager with connection pooling."""
    
    def __init__(
        self,
        host: str = None,
        port: int = None,
        db: int = 0,
        password: str = None,
        decode_responses: bool = False
    ):
        """
        Initialize Redis cache.
        
        Args:
            host: Redis host (default from env)
            port: Redis port (default from env)
            db: Redis database number
            password: Redis password
            decode_responses: Whether to decode responses to strings
        """
        self.host = host or os.getenv('REDIS_HOST', 'localhost')
        self.port = port or int(os.getenv('REDIS_PORT', '6379'))
        self.db = db
        self.password = password or os.getenv('REDIS_PASSWORD')
        self.decode_responses = decode_responses
        
        self._client = None
        self._enabled = os.getenv('REDIS_ENABLED', 'true').lower() == 'true'
        self._in_memory_fallback = {}
        self._fallback_enabled = True # Always allow fallback for unit tests/local dev
    
    @property
    def client(self) -> redis.Redis:
        """Get or create Redis client."""
        if not self._enabled:
            raise RuntimeError("Redis is disabled")
        
        if self._client is None:
            try:
                self._client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    password=self.password,
                    decode_responses=self.decode_responses,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    retry_on_timeout=True,
                    health_check_interval=30
                )
                # Test connection
                self._client.ping()
                logger.info(f"✅ Redis connected: {self.host}:{self.port}/{self.db}")
            except Exception as e:
                logger.error(f"❌ Redis connection failed: {e}")
                self._enabled = False
                raise
        
        return self._client
    
    def is_enabled(self) -> bool:
        """Check if Redis is enabled and available."""
        return self._enabled
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            default: Default value if key not found
            
        Returns:
            Cached value or default
        """
        if self._enabled:
            try:
                value = self.client.get(key)
                if value is not None:
                    # Try to unpickle
                    try:
                        return pickle.loads(value)  # nosec B301
                    except Exception as e:
                        # If unpickling fails, return as string
                        logger.debug(f"Unpickling failed for key '{key}': {e}")
                        return value.decode('utf-8') if isinstance(value, bytes) else value
            except Exception as e:
                logger.debug(f"Redis get failed for key '{key}': {e}")
        
        if self._fallback_enabled:
            logger.debug(f"Using in-memory fallback for get('{key}')")
            entry = self._in_memory_fallback.get(key)
            if entry:
                val, expiry = entry
                import time
                if expiry is None or time.time() < expiry:
                    return val
                else:
                    del self._in_memory_fallback[key]
        
        return default
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        ex: Optional[timedelta] = None
    ) -> bool:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
            ex: Time to live as timedelta
            
        Returns:
            True if successful
        """
        if self._enabled:
            try:
                # Pickle the value
                pickled_value = pickle.dumps(value)  # nosec B301
                
                # Set with TTL
                if ex:
                    self.client.setex(key, ex, pickled_value)
                elif ttl:
                    self.client.setex(key, ttl, pickled_value)
                else:
                    self.client.set(key, pickled_value)
                
                return True
            except Exception as e:
                logger.debug(f"Redis set failed for key '{key}': {e}")
        
        if self._fallback_enabled:
            logger.debug(f"Using in-memory fallback for set('{key}')")
            import time
            expiry = None
            if ttl:
                expiry = time.time() + ttl
            elif ex:
                expiry = time.time() + ex.total_seconds()
            
            self._in_memory_fallback[key] = (value, expiry)
            return True
        
        return False
    
    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        success = False
        if self._enabled:
            try:
                self.client.delete(key)
                success = True
            except Exception as e:
                logger.debug(f"Redis delete failed for key '{key}': {e}")
        
        if self._fallback_enabled:
            if key in self._in_memory_fallback:
                del self._in_memory_fallback[key]
            success = True
            
        return success
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache."""
        if self._enabled:
            try:
                if bool(self.client.exists(key)):
                    return True
            except Exception as e:
                logger.debug(f"Redis exists check failed for key '{key}': {e}")
        
        if self._fallback_enabled:
            return key in self._in_memory_fallback
            
        return False
    
    def clear_pattern(self, pattern: str) -> int:
        """
        Clear all keys matching pattern.
        
        Args:
            pattern: Key pattern (e.g., 'campaign:*')
            
        Returns:
            Number of keys deleted
        """
        if not self._enabled:
            return 0
        
        try:
            keys = self.client.keys(pattern)
            if keys:
                return self.client.delete(*keys)
            return 0
        except Exception as e:
            logger.warning(f"Cache clear pattern failed for '{pattern}': {e}")
            return 0
            
    def clear(self) -> bool:
        """Clear all keys in the current database."""
        try:
            if self._enabled:
                self.client.flushdb()
                return True
            raise redis.RedisError("Redis disabled")
        except Exception as e:
            if self._fallback_enabled:
                self._in_memory_fallback.clear()
                return True
            logger.warning(f"Cache clear failed: {e}")
            return False
    
    def health_check(self) -> bool:
        """Check Redis health."""
        if not self._enabled:
            return False
        
        try:
            return self.client.ping()
        except Exception as e:
            logger.error(f"Redis health check failed: {e}")
            return False
    
    def get_stats(self) -> dict:
        """Get Redis statistics."""
        if not self._enabled:
            return {'enabled': False}
        
        try:
            info = self.client.info()
            return {
                'enabled': True,
                'connected_clients': info.get('connected_clients', 0),
                'used_memory': info.get('used_memory_human', 'N/A'),
                'total_commands_processed': info.get('total_commands_processed', 0),
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'hit_rate': self._calculate_hit_rate(info)
            }
        except Exception as e:
            logger.error(f"Failed to get Redis stats: {e}")
            return {'enabled': True, 'error': str(e)}
    
    def _calculate_hit_rate(self, info: dict) -> float:
        """Calculate cache hit rate."""
        hits = info.get('keyspace_hits', 0)
        misses = info.get('keyspace_misses', 0)
        total = hits + misses
        
        if total == 0:
            return 0.0
        
        return (hits / total) * 100


# Global cache instance
_cache: Optional[RedisCache] = None


def get_cache() -> RedisCache:
    """Get or create global cache instance."""
    global _cache
    if _cache is None:
        _cache = RedisCache()
    return _cache


def cached(
    ttl: int = 300,
    key_prefix: str = "",
    key_func: Optional[Callable] = None
):
    """
    Decorator for caching function results.
    
    Args:
        ttl: Time to live in seconds (default 5 minutes)
        key_prefix: Prefix for cache key
        key_func: Custom function to generate cache key
        
    Example:
        @cached(ttl=600, key_prefix="campaign")
        def get_campaign(campaign_id: str):
            return expensive_operation(campaign_id)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache = get_cache()
            
            # Skip cache if disabled
            if not cache.is_enabled():
                return func(*args, **kwargs)
            
            # Generate cache key
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                # Default key generation
                key_parts = [key_prefix or func.__name__]
                key_parts.extend(str(arg) for arg in args)
                key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
                cache_key = ":".join(key_parts)
            
            # Try to get from cache
            cached_value = cache.get(cache_key)
            if cached_value is not None:
                logger.debug(f"Cache hit: {cache_key}")
                return cached_value
            
            # Execute function
            logger.debug(f"Cache miss: {cache_key}")
            result = func(*args, **kwargs)
            
            # Store in cache
            cache.set(cache_key, result, ttl=ttl)
            
            return result
        
        return wrapper
    return decorator


class CacheNamespace:
    """Namespace for organizing cache keys."""
    
    CAMPAIGNS = "campaign"
    ANALYSES = "analysis"
    QUERIES = "query"
    METRICS = "metrics"
    LLM_USAGE = "llm_usage"
    
    @staticmethod
    def key(namespace: str, *parts) -> str:
        """Generate namespaced cache key."""
        return f"{namespace}:{':'.join(str(p) for p in parts)}"
