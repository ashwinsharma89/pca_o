"""
Unit tests for Redis cache.
Tests caching functionality with mocked Redis and fallback support.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import timedelta
import json
import pickle
import time

from src.core.cache.redis_cache import RedisCache, CacheNamespace, get_cache, cached

class TestRedisCache:
    """Test Redis cache functionality."""
    
    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        mock = MagicMock()
        mock.ping.return_value = True
        mock.get.return_value = None
        mock.set.return_value = True
        mock.setex.return_value = True
        mock.delete.return_value = 1
        mock.exists.return_value = 0
        mock.expire.return_value = True
        mock.flushdb.return_value = True
        mock.info.return_value = {'connected_clients': 1, 'used_memory_human': '1MB', 'total_commands_processed': 100}
        return mock
    
    def test_initialization(self):
        """Test cache initialization."""
        with patch.dict('os.environ', {'REDIS_ENABLED': 'false'}):
            cache = RedisCache(host='test_host', port=1234)
            assert cache.host == 'test_host'
            assert cache.port == 1234
            assert cache._enabled is False
    
    def test_client_property(self, mock_redis):
        """Test client property lazy initialization."""
        with patch('redis.Redis', return_value=mock_redis):
            cache = RedisCache()
            cache._enabled = True
            client = cache.client # Should trigger initialization
            assert client == mock_redis
            assert cache._client == mock_redis
            mock_redis.ping.assert_called_once()
            
            # Second call should not trigger another ping
            client2 = cache.client
            assert client2 == mock_redis
            assert mock_redis.ping.call_count == 1

    def test_client_connection_error(self):
        """Test client property when connection fails."""
        mock_redis = Mock()
        mock_redis.ping.side_effect = Exception("Connection failed")
        
        with patch('redis.Redis', return_value=mock_redis):
            cache = RedisCache()
            cache._enabled = True
            with pytest.raises(Exception):
                _ = cache.client
            
            assert cache._enabled is False

    def test_get_existing_pickle(self, mock_redis):
        """Test get with picklable value."""
        cache = RedisCache()
        cache._client = mock_redis
        cache._enabled = True
        
        test_val = {'a': 1}
        mock_redis.get.return_value = pickle.dumps(test_val)
        
        assert cache.get('key') == test_val

    def test_get_unpickle_failure(self, mock_redis):
        """Test get when unpickling fails."""
        cache = RedisCache()
        cache._client = mock_redis
        cache._enabled = True
        
        # Invalid pickle data
        mock_redis.get.return_value = b"invalid pickle"
        
        # Should return raw bytes or decoded string
        assert cache.get('key') == "invalid pickle"

    def test_set_scenarios(self, mock_redis):
        """Test set with different arguments."""
        cache = RedisCache()
        cache._client = mock_redis
        cache._enabled = True
        
        # Basic set
        cache.set('key1', 'val1')
        mock_redis.set.assert_called_with('key1', pickle.dumps('val1'))
        
        # Set with ttl
        cache.set('key2', 'val2', ttl=60)
        mock_redis.setex.assert_called_with('key2', 60, pickle.dumps('val2'))
        
        # Set with ex (timedelta)
        ex = timedelta(seconds=120)
        cache.set('key3', 'val3', ex=ex)
        mock_redis.setex.assert_called_with('key3', ex, pickle.dumps('val3'))

    def test_fallback_mechanisms(self):
        """Test in-memory fallback when Redis is disabled."""
        cache = RedisCache()
        cache._enabled = False
        cache._fallback_enabled = True
        
        # Set
        cache.set('fkey', 'fval', ttl=1)
        assert 'fkey' in cache._in_memory_fallback
        
        # Get
        assert cache.get('fkey') == 'fval'
        
        # Expiry check
        with patch('time.time', return_value=time.time() + 10):
            assert cache.get('fkey') is None
            assert 'fkey' not in cache._in_memory_fallback

        # Exists
        cache.set('ekey', 'eval')
        assert cache.exists('ekey') is True
        
        # Delete
        cache.delete('ekey')
        assert cache.exists('ekey') is False
        
        # Clear
        cache.set('ckey', 'cval')
        cache.clear()
        assert len(cache._in_memory_fallback) == 0

    def test_health_check(self, mock_redis):
        """Test health check method."""
        cache = RedisCache()
        cache._client = mock_redis
        cache._enabled = True
        
        mock_redis.ping.return_value = True
        assert cache.health_check() is True
        
        mock_redis.ping.side_effect = Exception()
        assert cache.health_check() is False

    def test_get_stats(self, mock_redis):
        """Test statistics collection."""
        cache = RedisCache()
        cache._client = mock_redis
        cache._enabled = True
        
        mock_redis.info.return_value = {
            'connected_clients': 10,
            'used_memory_human': '256KB',
            'total_commands_processed': 500,
            'keyspace_hits': 80,
            'keyspace_misses': 20
        }
        
        stats = cache.get_stats()
        assert stats['connected_clients'] == 10
        assert stats['hit_rate'] == 80.0
        
        # Error case
        mock_redis.info.side_effect = Exception("Info failed")
        stats_err = cache.get_stats()
        assert 'error' in stats_err

    def test_clear_pattern(self, mock_redis):
        """Test clearing keys by pattern."""
        cache = RedisCache()
        cache._client = mock_redis
        cache._enabled = True
        
        mock_redis.keys.return_value = ['key1', 'key2']
        cache.clear_pattern('key*')
        mock_redis.delete.assert_called_with('key1', 'key2')
        
        # Empty pattern
        mock_redis.keys.return_value = []
        cache.clear_pattern('empty*')
        # delete should NOT be called again with empty args
        assert mock_redis.delete.call_count == 1 

class TestCacheNamespace:
    """Test CacheNamespace static helper."""
    
    def test_key_generation(self):
        assert CacheNamespace.key('test', 1, 'a') == 'test:1:a'
        assert CacheNamespace.key(CacheNamespace.CAMPAIGNS, '123') == 'campaign:123'

class TestCachedDecorator:
    """Test cached decorator."""
    
    @patch('src.core.cache.redis_cache.get_cache')
    def test_cached_success(self, mock_get_cache):
        mock_cache = Mock()
        mock_cache.is_enabled.return_value = True
        mock_cache.get.return_value = None
        mock_get_cache.return_value = mock_cache
        
        call_count = 0
        @cached(ttl=60, key_prefix="test")
        def my_func(x):
            nonlocal call_count
            call_count += 1
            return x * 2
        
        # First call - cache miss
        assert my_func(5) == 10
        assert call_count == 1
        mock_cache.get.assert_called_once()
        mock_cache.set.assert_called_once_with("test:5", 10, ttl=60)
        
        # Second call - cache hit (if we mock it)
        mock_cache.get.return_value = 10
        assert my_func(5) == 10
        assert call_count == 1 # Still 1

    @patch('src.core.cache.redis_cache.get_cache')
    def test_cached_disabled(self, mock_get_cache):
        mock_cache = Mock()
        mock_cache.is_enabled.return_value = False
        mock_get_cache.return_value = mock_cache
        
        @cached()
        def my_func():
            return "ok"
            
        assert my_func() == "ok"
        mock_cache.get.assert_not_called()

    @patch('src.core.cache.redis_cache.get_cache')
    def test_cached_custom_key_func(self, mock_get_cache):
        mock_cache = Mock()
        mock_cache.is_enabled.return_value = True
        mock_cache.get.return_value = None
        mock_get_cache.return_value = mock_cache
        
        def my_key(x, y):
            return f"custom:{x+y}"
            
        @cached(key_func=my_key)
        def add(x, y):
            return x + y
            
        add(1, 2)
        mock_cache.get.assert_called_with("custom:3")

def test_get_global_cache():
    """Test get_cache singleton."""
    from src.core.cache.redis_cache import _cache
    import src.core.cache.redis_cache as rc
    rc._cache = None
    
    c1 = rc.get_cache()
    c2 = rc.get_cache()
    assert c1 is c2
    assert rc._cache is not None
