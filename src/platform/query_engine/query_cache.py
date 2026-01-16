"""
Enhanced Query Cache - Production-ready caching with all enterprise features
Addresses: key naming, invalidation, monitoring, and distributed locking
"""
import redis
import hashlib
import json
import pickle  # nosec B403
import time
from typing import Optional, Any, Callable, Dict, List
from functools import wraps
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
import logging
import threading

logger = logging.getLogger(__name__)


# ============================================================================
# Cache Key Strategy - Consistent Naming Convention
# ============================================================================

class CacheKeyBuilder:
    """
    Consistent cache key naming strategy
    
    Format: {prefix}:{namespace}:{entity}:{identifier}:{hash}
    Example: pca:query:campaigns:list:abc123
    """
    
    PREFIX = "pca"
    
    @staticmethod
    def build(
        namespace: str,
        entity: str,
        identifier: str = "",
        params: dict = None
    ) -> str:
        """
        Build consistent cache key
        
        Args:
            namespace: Category (query, api, session, etc.)
            entity: Entity type (campaigns, analytics, etc.)
            identifier: Unique identifier (user_id, query, etc.)
            params: Additional parameters to include in key
            
        Returns:
            Formatted cache key
        """
        parts = [CacheKeyBuilder.PREFIX, namespace, entity]
        
        if identifier:
            parts.append(identifier)
        
        if params:
            param_hash = hashlib.sha256(
                json.dumps(params, sort_keys=True).encode()
            ).hexdigest()[:12]
            parts.append(param_hash)
        
        return ":".join(parts)
    
    @staticmethod
    def query_key(sql: str, params: dict = None) -> str:
        """Build key for SQL query result"""
        sql_hash = hashlib.sha256(sql.encode()).hexdigest()[:16]
        return CacheKeyBuilder.build("query", "result", sql_hash, params)
    
    @staticmethod
    def api_key(endpoint: str, user_id: str, params: dict = None) -> str:
        """Build key for API response"""
        return CacheKeyBuilder.build("api", endpoint, user_id, params)
    
    @staticmethod
    def analytics_key(metric: str, filters: dict = None) -> str:
        """Build key for analytics data"""
        return CacheKeyBuilder.build("analytics", metric, "", filters)
    
    @staticmethod
    def session_key(session_id: str) -> str:
        """Build key for session data"""
        return CacheKeyBuilder.build("session", "data", session_id)
    
    @staticmethod
    def pattern(namespace: str = "*", entity: str = "*") -> str:
        """Build pattern for key matching"""
        return f"{CacheKeyBuilder.PREFIX}:{namespace}:{entity}:*"


# ============================================================================
# Cache Metrics - Hit/Miss Monitoring
# ============================================================================

@dataclass
class CacheMetrics:
    """Cache performance metrics"""
    hits: int = 0
    misses: int = 0
    errors: int = 0
    sets: int = 0
    deletes: int = 0
    lock_acquired: int = 0
    lock_failed: int = 0
    last_reset: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def total_requests(self) -> int:
        return self.hits + self.misses
    
    @property
    def hit_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.hits / self.total_requests
    
    @property
    def miss_rate(self) -> float:
        return 1.0 - self.hit_rate
    
    def to_dict(self) -> dict:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "errors": self.errors,
            "sets": self.sets,
            "deletes": self.deletes,
            "lock_acquired": self.lock_acquired,
            "lock_failed": self.lock_failed,
            "total_requests": self.total_requests,
            "hit_rate": round(self.hit_rate * 100, 2),
            "miss_rate": round(self.miss_rate * 100, 2),
            "last_reset": self.last_reset.isoformat()
        }
    
    def reset(self):
        self.hits = 0
        self.misses = 0
        self.errors = 0
        self.sets = 0
        self.deletes = 0
        self.lock_acquired = 0
        self.lock_failed = 0
        self.last_reset = datetime.utcnow()


# ============================================================================
# Distributed Lock - Prevent Cache Stampede
# ============================================================================

class DistributedLock:
    """
    Redis-based distributed lock for cache operations
    Prevents cache stampede and ensures atomic operations
    """
    
    LOCK_PREFIX = "pca:lock:"
    DEFAULT_TIMEOUT = 10  # seconds
    DEFAULT_RETRY = 3
    RETRY_DELAY = 0.1  # seconds
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
    
    @contextmanager
    def acquire(
        self,
        key: str,
        timeout: int = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRY
    ):
        """
        Acquire distributed lock
        
        Args:
            key: Lock key
            timeout: Lock timeout in seconds
            retries: Number of retry attempts
            
        Yields:
            True if lock acquired
            
        Usage:
            with lock.acquire("my_key"):
                # Critical section
                pass
        """
        lock_key = f"{self.LOCK_PREFIX}{key}"
        lock_value = f"{time.time()}:{threading.get_ident()}"
        acquired = False
        
        for attempt in range(retries):
            # Try to acquire lock with NX (only if not exists)
            acquired = self.redis.set(
                lock_key,
                lock_value,
                nx=True,
                ex=timeout
            )
            
            if acquired:
                logger.debug(f"Lock acquired: {key}")
                break
            
            time.sleep(self.RETRY_DELAY * (attempt + 1))
        
        try:
            yield acquired
        finally:
            if acquired:
                # Release lock (check if we still own it)
                if self.redis.get(lock_key) == lock_value.encode():
                    self.redis.delete(lock_key)
                    logger.debug(f"Lock released: {key}")


# ============================================================================
# Cache Invalidation Strategy
# ============================================================================

class CacheInvalidator:
    """
    Smart cache invalidation with tags and patterns
    
    Strategies:
    1. Tag-based: Invalidate all keys with a specific tag
    2. Pattern-based: Invalidate keys matching a pattern
    3. Time-based: Auto-expire (handled by TTL)
    4. Event-based: Invalidate on specific events
    """
    
    TAG_PREFIX = "pca:tag:"
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
    
    def tag_key(self, key: str, tags: List[str]):
        """
        Tag a cache key for later invalidation
        
        Args:
            key: Cache key
            tags: List of tags
        """
        for tag in tags:
            tag_key = f"{self.TAG_PREFIX}{tag}"
            self.redis.sadd(tag_key, key)
    
    def invalidate_by_tag(self, tag: str) -> int:
        """
        Invalidate all keys with a specific tag
        
        Args:
            tag: Tag to invalidate
            
        Returns:
            Number of keys invalidated
        """
        tag_key = f"{self.TAG_PREFIX}{tag}"
        keys = self.redis.smembers(tag_key)
        
        if keys:
            count = self.redis.delete(*keys)
            self.redis.delete(tag_key)
            logger.info(f"Invalidated {count} keys with tag: {tag}")
            return count
        return 0
    
    def invalidate_by_pattern(self, pattern: str) -> int:
        """
        Invalidate keys matching pattern
        
        Args:
            pattern: Redis pattern (e.g., "pca:query:*")
            
        Returns:
            Number of keys invalidated
        """
        keys = list(self.redis.scan_iter(pattern))
        
        if keys:
            count = self.redis.delete(*keys)
            logger.info(f"Invalidated {count} keys matching: {pattern}")
            return count
        return 0
    
    def invalidate_entity(self, entity: str) -> int:
        """
        Invalidate all cache for an entity type
        
        Args:
            entity: Entity type (campaigns, analytics, etc.)
            
        Returns:
            Number of keys invalidated
        """
        pattern = f"pca:*:{entity}:*"
        return self.invalidate_by_pattern(pattern)
    
    def invalidate_user(self, user_id: str) -> int:
        """
        Invalidate all cache for a user
        
        Args:
            user_id: User ID
            
        Returns:
            Number of keys invalidated
        """
        return self.invalidate_by_tag(f"user:{user_id}")


# ============================================================================
# Enhanced Query Cache
# ============================================================================

class EnhancedQueryCache:
    """
    Production-ready query cache with all enterprise features
    
    Features:
    - Consistent key naming
    - Hit/miss monitoring
    - Distributed locking
    - Tag-based invalidation
    - Pattern invalidation
    """
    
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/1",
        default_ttl: int = 3600
    ):
        """
        Initialize enhanced cache
        
        Args:
            redis_url: Redis connection URL
            default_ttl: Default TTL in seconds
        """
        self.default_ttl = default_ttl
        self.metrics = CacheMetrics()
        
        try:
            self.redis = redis.from_url(redis_url, decode_responses=False)
            self.redis.ping()
            self.lock = DistributedLock(self.redis)
            self.invalidator = CacheInvalidator(self.redis)
            self._enabled = True
            logger.info(f"Enhanced cache initialized (TTL={default_ttl}s)")
        except redis.ConnectionError as e:
            logger.warning(f"Redis connection failed: {e}. Caching disabled.")
            self._enabled = False
            self.redis = None
            self.lock = None
            self.invalidator = None
    
    @property
    def enabled(self) -> bool:
        return self._enabled
    
    def get(
        self,
        key: str,
        use_lock: bool = False
    ) -> Optional[Any]:
        """
        Get value from cache
        
        Args:
            key: Cache key
            use_lock: Whether to use distributed lock
            
        Returns:
            Cached value or None
        """
        if not self._enabled:
            return None
        
        try:
            cached = self.redis.get(key)
            if cached:
                self.metrics.hits += 1
                logger.debug(f"Cache HIT: {key[:50]}")
                return pickle.loads(cached)  # nosec B301
            
            self.metrics.misses += 1
            logger.debug(f"Cache MISS: {key[:50]}")
            return None
            
        except Exception as e:
            self.metrics.errors += 1
            logger.error(f"Cache get error: {e}")
            return None
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: int = None,
        tags: List[str] = None
    ):
        """
        Set value in cache
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: TTL in seconds
            tags: Tags for invalidation
        """
        if not self._enabled:
            return
        
        try:
            self.redis.setex(
                key,
                ttl or self.default_ttl,
                pickle.dumps(value)
            )
            self.metrics.sets += 1
            
            # Tag for invalidation
            if tags and self.invalidator:
                self.invalidator.tag_key(key, tags)
            
            logger.debug(f"Cache SET: {key[:50]} (TTL={ttl or self.default_ttl}s)")
            
        except Exception as e:
            self.metrics.errors += 1
            logger.error(f"Cache set error: {e}")
    
    def delete(self, key: str):
        """Delete key from cache"""
        if not self._enabled:
            return
        
        try:
            self.redis.delete(key)
            self.metrics.deletes += 1
            logger.debug(f"Cache DELETE: {key[:50]}")
        except Exception as e:
            self.metrics.errors += 1
            logger.error(f"Cache delete error: {e}")
    
    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int = None,
        tags: List[str] = None,
        use_lock: bool = True
    ) -> Any:
        """
        Get from cache or compute and cache
        
        Args:
            key: Cache key
            factory: Function to compute value if not cached
            ttl: TTL in seconds
            tags: Tags for invalidation
            use_lock: Use distributed lock to prevent stampede
            
        Returns:
            Cached or computed value
        """
        # Try cache first
        cached = self.get(key)
        if cached is not None:
            return cached
        
        # Compute value (with optional lock)
        if use_lock and self.lock:
            with self.lock.acquire(key) as acquired:
                if acquired:
                    self.metrics.lock_acquired += 1
                    # Double-check cache after acquiring lock
                    cached = self.get(key)
                    if cached is not None:
                        return cached
                    
                    value = factory()
                    self.set(key, value, ttl, tags)
                    return value
                else:
                    self.metrics.lock_failed += 1
                    # Failed to acquire lock, compute anyway
                    return factory()
        else:
            value = factory()
            self.set(key, value, ttl, tags)
            return value
    
    def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate by tag"""
        if self.invalidator:
            count = self.invalidator.invalidate_by_tag(tag)
            self.metrics.deletes += count
            return count
        return 0
    
    def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate by pattern"""
        if self.invalidator:
            count = self.invalidator.invalidate_by_pattern(pattern)
            self.metrics.deletes += count
            return count
        return 0
    
    def get_metrics(self) -> dict:
        """Get cache performance metrics"""
        metrics = self.metrics.to_dict()
        
        if self._enabled:
            try:
                info = self.redis.info("memory")
                metrics["memory_used"] = info.get("used_memory_human", "N/A")
                metrics["redis_connected"] = True
            except:
                metrics["redis_connected"] = False
        else:
            metrics["redis_connected"] = False
        
        return metrics
    
    def reset_metrics(self):
        """Reset cache metrics"""
        self.metrics.reset()
    
    def clear_all(self):
        """Clear all cache entries"""
        if self._enabled:
            self.invalidate_by_pattern("pca:*")


# ============================================================================
# Decorator for Cached Functions
# ============================================================================

def cached(
    ttl: int = 3600,
    namespace: str = "func",
    tags: List[str] = None,
    use_lock: bool = True
):
    """
    Decorator to cache function results
    
    Args:
        ttl: Cache TTL in seconds
        namespace: Cache namespace
        tags: Tags for invalidation
        use_lock: Use distributed lock
        
    Usage:
        @cached(ttl=1800, namespace="analytics", tags=["campaigns"])
        def get_campaign_metrics(campaign_id: str):
            return expensive_calculation()
    """
    _cache = None
    
    def get_cache():
        nonlocal _cache
        if _cache is None:
            _cache = EnhancedQueryCache()
        return _cache
    
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache = get_cache()
            
            # Build cache key
            func_key = f"{func.__module__}.{func.__name__}"
            params = {
                "args": args,
                "kwargs": kwargs
            }
            key = CacheKeyBuilder.build(namespace, func_key, "", params)
            
            # Get or compute
            return cache.get_or_set(
                key,
                lambda: func(*args, **kwargs),
                ttl=ttl,
                tags=tags,
                use_lock=use_lock
            )
        return wrapper
    return decorator


# ============================================================================
# Global Cache Instance
# ============================================================================

_global_cache: Optional[EnhancedQueryCache] = None

def get_cache() -> EnhancedQueryCache:
    """Get global cache instance"""
    global _global_cache
    if _global_cache is None:
        _global_cache = EnhancedQueryCache()
    return _global_cache


def invalidate_entity_cache(entity: str):
    """Convenience function to invalidate entity cache"""
    cache = get_cache()
    if cache.invalidator:
        return cache.invalidator.invalidate_entity(entity)
    return 0
