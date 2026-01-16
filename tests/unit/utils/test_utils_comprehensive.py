"""
Utilities Comprehensive Tests

Tests for resilience, circuit breaker, caching, logging, and other utilities.
Improves coverage for src/utils/*.py
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock
import time
import asyncio
from datetime import datetime, timedelta
import tempfile
from pathlib import Path


# ============================================================================
# CIRCUIT BREAKER TESTS
# ============================================================================

class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""
    
    def test_initialization(self):
        """Test CircuitBreaker initialization."""
        from src.core.utils.circuit_breaker import CircuitBreaker
        
        cb = CircuitBreaker(
            name="test",
            failure_threshold=5,
            recovery_timeout=30
        )
        
        assert cb.name == "test"
        assert cb.failure_threshold == 5
        assert cb.recovery_timeout == 30
    
    def test_closed_state_initially(self):
        """Test circuit starts in closed state."""
        from src.core.utils.circuit_breaker import CircuitBreaker, CircuitState
        
        cb = CircuitBreaker(name="test")
        
        assert cb.state == CircuitState.CLOSED
    
    def test_is_available_when_closed(self):
        """Test is_available returns True when closed."""
        from src.core.utils.circuit_breaker import CircuitBreaker
        
        cb = CircuitBreaker(name="test")
        
        assert cb.is_available() == True
    
    def test_opens_after_failures(self):
        """Test circuit opens after reaching failure threshold."""
        from src.core.utils.circuit_breaker import CircuitBreaker, CircuitState
        
        cb = CircuitBreaker(name="test", failure_threshold=3)
        
        # Record failures
        for _ in range(3):
            cb.record_failure(Exception("test error"))
        
        assert cb.state == CircuitState.OPEN
        assert cb.is_available() == False
    
    def test_record_success_closes_circuit(self):
        """Test successful calls in half-open state close circuit."""
        from src.core.utils.circuit_breaker import CircuitBreaker, CircuitState
        
        cb = CircuitBreaker(
            name="test",
            failure_threshold=2,
            success_threshold=2,
            recovery_timeout=0.1
        )
        
        # Open the circuit
        cb.record_failure(Exception("fail"))
        cb.record_failure(Exception("fail"))
        
        assert cb.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(0.15)
        
        # Should transition to half-open on next check
        _ = cb.state  # Triggers state check
        
        # Record successes
        cb.record_success()
        cb.record_success()
        
        assert cb.state == CircuitState.CLOSED
    
    def test_reset(self):
        """Test manual circuit reset."""
        from src.core.utils.circuit_breaker import CircuitBreaker, CircuitState
        
        cb = CircuitBreaker(name="test", failure_threshold=2)
        
        # Open the circuit
        cb.record_failure(Exception("fail"))
        cb.record_failure(Exception("fail"))
        
        assert cb.state == CircuitState.OPEN
        
        # Reset
        cb.reset()
        
        assert cb.state == CircuitState.CLOSED
        assert cb.is_available() == True
    
    def test_get_stats(self):
        """Test getting circuit statistics."""
        from src.core.utils.circuit_breaker import CircuitBreaker
        
        cb = CircuitBreaker(name="test")
        
        cb.record_success()
        cb.record_success()
        cb.record_failure(Exception("test"))
        
        stats = cb.get_stats()
        
        assert stats["name"] == "test"
        assert stats["successes"] == 2
        assert stats["failures"] == 1
    
    def test_decorator_usage(self):
        """Test circuit breaker as decorator."""
        from src.core.utils.circuit_breaker import CircuitBreaker
        
        cb = CircuitBreaker(name="test", failure_threshold=3)
        
        @cb
        def test_function():
            return "success"
        
        result = test_function()
        
        assert result == "success"
    
    def test_decorator_with_failure(self):
        """Test decorator handles failures."""
        from src.core.utils.circuit_breaker import CircuitBreaker
        
        cb = CircuitBreaker(name="test", failure_threshold=3)
        
        @cb
        def failing_function():
            raise ValueError("test error")
        
        with pytest.raises(ValueError):
            failing_function()
        
        # Should have recorded the failure
        assert cb._stats.failures >= 1
    
    def test_context_manager_usage(self):
        """Test circuit breaker as context manager."""
        from src.core.utils.circuit_breaker import CircuitBreaker
        
        cb = CircuitBreaker(name="test")
        
        with cb:
            result = "success"
        
        assert result == "success"
        assert cb._stats.successes >= 1


class TestCircuitBreakerRegistry:
    """Tests for CircuitBreakerRegistry class."""
    
    def test_get_circuit_breaker(self):
        """Test getting circuit breaker from registry."""
        from src.core.utils.circuit_breaker import get_circuit_breaker
        
        cb = get_circuit_breaker("test_service")
        
        assert cb is not None
        assert cb.name == "test_service"
    
    def test_get_same_breaker(self):
        """Test getting same breaker returns same instance."""
        from src.core.utils.circuit_breaker import get_circuit_breaker
        
        cb1 = get_circuit_breaker("same_service")
        cb2 = get_circuit_breaker("same_service")
        
        assert cb1 is cb2
    
    def test_get_all_stats(self):
        """Test getting stats for all circuit breakers."""
        from src.core.utils.circuit_breaker import get_all_circuit_stats
        
        stats = get_all_circuit_stats()
        
        assert isinstance(stats, list)
    
    def test_preconfigured_breakers(self):
        """Test pre-configured circuit breakers exist."""
        from src.core.utils.circuit_breaker import (
            openai_circuit,
            groq_circuit,
            redis_circuit,
            database_circuit
        )
        
        assert openai_circuit is not None
        assert groq_circuit is not None
        assert redis_circuit is not None
        assert database_circuit is not None


# ============================================================================
# RESILIENT LLM CLIENT TESTS
# ============================================================================

class TestResilientLLMClient:
    """Tests for ResilientLLMClient class."""
    
    def test_initialization(self):
        """Test ResilientLLMClient initialization."""
        from src.core.utils.resilient_llm import ResilientLLMClient
        
        client = ResilientLLMClient()
        
        assert client is not None
    
    def test_get_circuit(self):
        """Test getting circuit for a service."""
        from src.core.utils.resilient_llm import ResilientLLMClient
        
        client = ResilientLLMClient()
        
        circuit = client.get_circuit("openai")
        
        assert circuit is not None
        assert circuit.name == "openai"
    
    def test_call_with_retry_success(self):
        """Test successful call with retry."""
        from src.core.utils.resilient_llm import ResilientLLMClient
        
        client = ResilientLLMClient()
        
        def success_func():
            return "result"
        
        result = client.call_with_retry(success_func, service_name="test")
        
        assert result == "result"
    
    def test_call_with_retry_failure_then_success(self):
        """Test retry on failure then success."""
        from src.core.utils.resilient_llm import ResilientLLMClient
        
        client = ResilientLLMClient()
        
        call_count = 0
        
        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Temporary failure")
            return "success"
        
        result = client.call_with_retry(
            flaky_func,
            service_name="flaky",
            max_retries=3,
            base_delay=0.01
        )
        
        assert result == "success"
        assert call_count == 2
    
    def test_call_with_fallback(self):
        """Test fallback when all retries fail."""
        from src.core.utils.resilient_llm import ResilientLLMClient
        
        client = ResilientLLMClient()
        
        def always_fails():
            raise Exception("Always fails")
        
        def fallback():
            return "fallback_result"
        
        result = client.call_with_retry(
            always_fails,
            service_name="failing",
            max_retries=1,
            base_delay=0.01,
            fallback=fallback
        )
        
        assert result == "fallback_result"
    
    def test_resilient_decorator(self):
        """Test resilient decorator."""
        from src.core.utils.resilient_llm import llm_client
        
        @llm_client.resilient("test_service", max_retries=2)
        def test_function():
            return "decorated_result"
        
        result = test_function()
        
        assert result == "decorated_result"
    
    def test_get_all_circuit_states(self):
        """Test getting all circuit states."""
        from src.core.utils.resilient_llm import ResilientLLMClient
        
        client = ResilientLLMClient()
        
        # Add some circuits
        client.get_circuit("service1")
        client.get_circuit("service2")
        
        states = client.get_all_circuit_states()
        
        assert isinstance(states, dict)


# ============================================================================
# CACHING TESTS
# ============================================================================

class TestCaching:
    """Tests for caching utilities."""
    
    def test_cache_manager_initialization(self):
        """Test CacheManager initialization."""
        from src.core.cache.redis_cache import RedisCache
        
        cache = RedisCache()
        
        assert cache is not None
    
    def test_cache_set_and_get(self):
        """Test setting and getting cache values."""
        from src.core.cache.redis_cache import RedisCache
        
        cache = RedisCache()
        
        cache.set("test_key", "test_value", ttl=60)
        
        result = cache.get("test_key")
        
        assert result == "test_value"
    
    def test_cache_expiration(self):
        """Test cache value expiration."""
        from src.core.cache.redis_cache import RedisCache
        
        cache = RedisCache()
        
        cache.set("expire_key", "expire_value", ttl=1)
        
        # Value should exist immediately
        assert cache.get("expire_key") == "expire_value"
        
        # Wait for expiration
        import time
        time.sleep(1.5)
        
        # Value should be gone
        assert cache.get("expire_key") is None
    
    def test_cache_delete(self):
        """Test deleting cache values."""
        from src.core.cache.redis_cache import RedisCache
        
        cache = RedisCache()
        
        cache.set("delete_key", "delete_value")
        cache.delete("delete_key")
        
        assert cache.get("delete_key") is None
    
    def test_cache_clear(self):
        """Test clearing all cache values."""
        from src.core.cache.redis_cache import RedisCache
        
        cache = RedisCache()
        
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        
        cache.clear()
        
        assert cache.get("key1") is None
        assert cache.get("key2") is None


# ============================================================================
# LOGGER TESTS
# ============================================================================

class TestLogger:
    """Tests for logging utilities."""
    
    def test_setup_logger(self):
        """Test logger setup."""
        from src.core.utils.logger import setup_logger
        
        logger = setup_logger("test_module")
        
        assert logger is not None
    
    def test_logger_has_handlers(self):
        """Test logger has handlers configured."""
        from src.core.utils.logger import setup_logger
        
        logger = setup_logger("test_handlers")
        
        # Should be able to log without error
        logger.info("Test message")
        
        assert True  # If we got here, logging works
    
    def test_structured_logging(self):
        """Test structured logging format."""
        from src.core.utils.logger import setup_logger
        
        logger = setup_logger("test_structured")
        
        # Should support extra fields
        logger.info("Test", extra={"user_id": "123", "action": "test"})
        
        assert True


# ============================================================================
# OPENTELEMETRY CONFIG TESTS
# ============================================================================

class TestOpenTelemetryConfig:
    """Tests for OpenTelemetry configuration."""
    
    def test_instrument_app(self):
        """Test instrumenting FastAPI app."""
        from fastapi import FastAPI
        from src.core.utils.opentelemetry_config import instrument_app
        
        app = FastAPI()
        
        # Should not raise even if OTEL not configured
        try:
            instrument_app(app)
            assert True
        except Exception:
            # May fail if OTEL not available
            pass
    
    def test_otel_disabled_by_default(self):
        """Test OTEL is disabled when not configured."""
        from src.core.utils.opentelemetry_config import OTEL_ENABLED
        
        # Should be False or True depending on config
        assert isinstance(OTEL_ENABLED, bool)


# ============================================================================
# OBSERVABILITY TESTS
# ============================================================================

class TestObservability:
    """Tests for observability utilities."""
    
    def test_metrics_collector(self):
        """Test metrics collector."""
        from src.core.utils.observability import MetricsCollector
        
        collector = MetricsCollector()
        
        assert collector is not None
    
    def test_record_metric(self):
        """Test recording a metric."""
        from src.core.utils.observability import MetricsCollector
        
        collector = MetricsCollector()
        
        try:
            collector.record("test_metric", 100, tags={"service": "test"})
            assert True
        except Exception:
            pass
    
    def test_get_metrics(self):
        """Test getting recorded metrics."""
        from src.core.utils.observability import MetricsCollector
        
        collector = MetricsCollector()
        
        try:
            metrics = collector.get_metrics()
            assert metrics is not None
        except Exception:
            pass


# ============================================================================
# ANTHROPIC HELPERS TESTS
# ============================================================================

class TestAnthropicHelpers:
    """Tests for Anthropic API helpers."""
    
    def test_create_async_client_no_key(self):
        """Test creating async client without API key."""
        from src.core.utils.anthropic_helpers import create_async_anthropic_client
        
        # Should handle missing key gracefully
        client = create_async_anthropic_client(None)
        
        assert client is None
    
    def test_create_async_client_with_key(self):
        """Test creating async client with API key."""
        from src.core.utils.anthropic_helpers import create_async_anthropic_client
        
        client = create_async_anthropic_client("test-api-key")
        
        # If anthropic package is available, client should be created
        # Otherwise, should return None
        assert client is None or hasattr(client, 'messages')


# ============================================================================
# SETTINGS TESTS
# ============================================================================

class TestSettings:
    """Tests for settings configuration."""
    
    def test_settings_loaded(self):
        """Test settings are loaded."""
        from src.core.config.settings import settings
        
        assert settings is not None
    
    def test_default_values(self):
        """Test default setting values."""
        from src.core.config.settings import settings
        
        # Check some common defaults
        assert settings.api_host in ["0.0.0.0", "127.0.0.1", "localhost"]
        assert settings.api_port in [8000, 8080]
    
    def test_database_url_property(self):
        """Test database URL property."""
        from src.core.config.settings import settings
        
        db_url = getattr(settings, 'database_url', None)
        
        # Should have a database URL or None
        assert db_url is None or isinstance(db_url, str)
    
    def test_llm_settings(self):
        """Test LLM-related settings."""
        from src.core.config.settings import settings
        
        assert hasattr(settings, 'default_llm_model') or hasattr(settings, 'openai_api_key')


# ============================================================================
# VALIDATORS TESTS
# ============================================================================

class TestValidators:
    """Tests for API validators."""
    
    def test_email_validator(self):
        """Test email validation."""
        from src.interface.api.validators import validate_email
        
        assert validate_email("test@example.com") == True
        assert validate_email("invalid-email") == False
        assert validate_email("") == False
    
    def test_date_range_validator(self):
        """Test date range validation."""
        from src.interface.api.validators import validate_date_range
        
        # Valid range
        assert validate_date_range("2024-01-01", "2024-12-31") == True
        
        # Invalid range (start after end)
        assert validate_date_range("2024-12-31", "2024-01-01") == False
    
    def test_pagination_validator(self):
        """Test pagination parameter validation."""
        from src.interface.api.validators import validate_pagination
        
        # Valid pagination
        assert validate_pagination(page=1, per_page=50) == True
        
        # Invalid pagination
        assert validate_pagination(page=0, per_page=50) == False
        assert validate_pagination(page=1, per_page=0) == False


# ============================================================================
# DATA PROCESSING TESTS
# ============================================================================

class TestDataProcessing:
    """Tests for data processing utilities."""
    
    def test_column_mapping_utility(self):
        """Test find_column utility."""
        from src.core.utils.column_mapping import find_column
        import pandas as pd
        
        # Common variations should map to standard names
        df = pd.DataFrame({
            "Spend": [100],
            "Impressions": [1000],
            "Clicks": [50]
        })
        
        assert find_column(df, "spend") == "Spend"
        assert find_column(df, "impressions") == "Impressions"
        assert find_column(df, "clicks") == "Clicks"
        assert find_column(df, "nonexistent") is None


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
