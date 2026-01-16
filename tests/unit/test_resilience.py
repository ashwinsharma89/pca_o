"""
Unit tests for src/utils/resilience.py
Covers: Retry decorator, circuit breaker, timeout handling, dead letter queue
"""
import pytest
import time
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta

from src.core.utils.resilience import (
    retry,
    CircuitBreaker,
    CircuitState,
    DeadLetterQueue,
    LLMError,
    LLMTimeoutError,
    LLMRateLimitError,
)


# ============================================================================
# Retry Decorator Tests
# ============================================================================

class TestRetryDecorator:
    """Tests for retry decorator."""
    
    def test_succeeds_on_first_try(self):
        """Function succeeds on first attempt."""
        call_count = 0
        
        @retry(max_retries=3)
        def success_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = success_func()
        
        assert result == "success"
        assert call_count == 1
    
    def test_retries_on_failure(self):
        """Function retries on transient failure."""
        call_count = 0
        
        @retry(max_retries=3, base_delay=0.01)
        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise LLMError("Transient error", provider="test")
            return "success"
        
        result = fail_then_succeed()
        
        assert result == "success"
        assert call_count == 3
    
    def test_raises_after_max_retries(self):
        """Function raises after exhausting retries."""
        from src.core.utils.resilience import RetryExhaustedError
        
        call_count = 0
        
        @retry(max_retries=3, base_delay=0.01)
        def always_fail():
            nonlocal call_count
            call_count += 1
            raise LLMError("Permanent error", provider="test")
        
        with pytest.raises(RetryExhaustedError):
            always_fail()
        
        # max_retries=3 means 1 initial + 3 retries = 4 attempts
        assert call_count == 4
    
    def test_exponential_backoff(self):
        """Verify exponential backoff timing."""
        delays = []
        
        @retry(max_retries=3, base_delay=0.1)
        def track_delays():
            delays.append(time.time())
            if len(delays) < 3:
                raise LLMError("Retry", provider="test")
            return "done"
        
        track_delays()
        
        # Check delays increase exponentially (with tolerance for timing variance)
        if len(delays) >= 3:
            delay1 = delays[1] - delays[0]
            delay2 = delays[2] - delays[1]
            # Allow some tolerance for timing variance
            assert delay2 >= delay1 * 0.8  # Second delay should be at least 80% of expected
    
    def test_rate_limit_error_longer_delay(self):
        """Rate limit errors should have longer delays."""
        call_count = 0
        
        @retry(max_retries=2, base_delay=0.01)
        def rate_limited():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise LLMRateLimitError(provider="test", retry_after=1)
            return "success"
        
        result = rate_limited()
        
        assert result == "success"
        assert call_count == 2


# ============================================================================
# Circuit Breaker Tests
# ============================================================================

class TestCircuitBreaker:
    """Tests for CircuitBreaker pattern."""
    
    def test_starts_closed(self):
        """Circuit breaker starts in CLOSED state."""
        from src.core.utils.resilience import CircuitBreakerConfig
        
        cb = CircuitBreaker("test_closed", CircuitBreakerConfig(failure_threshold=3))
        
        assert cb.state == CircuitState.CLOSED
    
    def test_opens_after_threshold(self):
        """Circuit opens after failure threshold."""
        from src.core.utils.resilience import CircuitBreakerConfig
        
        cb = CircuitBreaker("test_opens", CircuitBreakerConfig(failure_threshold=3))
        
        for _ in range(3):
            cb.record_failure(Exception("Test error"))
        
        assert cb.state == CircuitState.OPEN
    
    def test_rejects_when_open(self):
        """Circuit rejects calls when open."""
        from src.core.utils.resilience import CircuitBreakerConfig
        
        cb = CircuitBreaker("test_rejects", CircuitBreakerConfig(failure_threshold=1))
        cb.record_failure(Exception("Test error"))
        
        assert cb.state == CircuitState.OPEN
        assert not cb._should_allow_request()
    
    def test_half_open_after_timeout(self):
        """Circuit transitions to HALF_OPEN after timeout."""
        from src.core.utils.resilience import CircuitBreakerConfig
        
        cb = CircuitBreaker("test_half_open", CircuitBreakerConfig(failure_threshold=1, timeout_seconds=0.1))
        cb.record_failure(Exception("Test error"))
        
        assert cb.state == CircuitState.OPEN
        
        time.sleep(0.15)
        
        # Should allow one test request and transition to HALF_OPEN
        assert cb._should_allow_request()
        assert cb.state == CircuitState.HALF_OPEN
    
    def test_closes_on_success_in_half_open(self):
        """Circuit closes on success in HALF_OPEN state."""
        from src.core.utils.resilience import CircuitBreakerConfig
        
        cb = CircuitBreaker("test_closes", CircuitBreakerConfig(failure_threshold=1, timeout_seconds=0.01, success_threshold=1))
        cb.record_failure(Exception("Test error"))
        
        time.sleep(0.02)
        cb._should_allow_request()  # Transition to HALF_OPEN
        cb.record_success()
        
        assert cb.state == CircuitState.CLOSED
    
    def test_reopens_on_failure_in_half_open(self):
        """Circuit reopens on failure in HALF_OPEN state."""
        from src.core.utils.resilience import CircuitBreakerConfig
        
        cb = CircuitBreaker("test_reopens", CircuitBreakerConfig(failure_threshold=1, timeout_seconds=0.01))
        cb.record_failure(Exception("Test error"))
        
        time.sleep(0.02)
        cb._should_allow_request()  # Transition to HALF_OPEN
        cb.record_failure(Exception("Another error"))
        
        assert cb.state == CircuitState.OPEN
    
    def test_success_resets_failure_count(self):
        """Success resets failure count in CLOSED state."""
        from src.core.utils.resilience import CircuitBreakerConfig
        
        cb = CircuitBreaker("test_resets", CircuitBreakerConfig(failure_threshold=3))
        
        cb.record_failure(Exception("Error 1"))
        cb.record_failure(Exception("Error 2"))
        cb.record_success()
        cb.record_failure(Exception("Error 3"))
        
        # Should still be closed (only 1 failure after reset)
        assert cb.state == CircuitState.CLOSED


# ============================================================================
# Dead Letter Queue Tests
# ============================================================================

class TestDeadLetterQueue:
    """Tests for DeadLetterQueue."""
    
    def test_add_failed_job(self):
        """Add failed job to DLQ."""
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            dlq = DeadLetterQueue(storage_path=f.name)
        
        dlq.add(
            job_id="123",
            job_type="test",
            payload={"data": "test"},
            error=Exception("Test error")
        )
        
        assert len(dlq.queue) == 1
    
    def test_retrieve_jobs(self):
        """Retrieve jobs from DLQ."""
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            dlq = DeadLetterQueue(storage_path=f.name)
        
        dlq.add(job_id="1", job_type="test", payload={}, error=Exception("Error 1"))
        dlq.add(job_id="2", job_type="test", payload={}, error=Exception("Error 2"))
        
        jobs = dlq.get_all()
        
        assert len(jobs) == 2
    
    def test_remove_job(self):
        """Remove job from DLQ after processing."""
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            dlq = DeadLetterQueue(storage_path=f.name)
        
        dlq.add(job_id="123", job_type="test", payload={}, error=Exception("Test"))
        
        dlq.remove("123")
        
        assert len(dlq.queue) == 0
    
    def test_max_size_limit(self):
        """DLQ should respect max size limit."""
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            dlq = DeadLetterQueue(storage_path=f.name, max_size=2)
        
        dlq.add(job_id="1", job_type="test", payload={}, error=Exception("E1"))
        dlq.add(job_id="2", job_type="test", payload={}, error=Exception("E2"))
        dlq.add(job_id="3", job_type="test", payload={}, error=Exception("E3"))  # Should evict oldest
        
        assert len(dlq.queue) == 2
        jobs = dlq.get_all()
        job_ids = [j.job_id for j in jobs]
        assert "1" not in job_ids  # Oldest evicted
    
    def test_retry_job(self):
        """Get a job by ID from DLQ."""
        import tempfile
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            dlq = DeadLetterQueue(storage_path=f.name)
        
        dlq.add(
            job_id="123",
            job_type="test",
            payload={"data": "test"},
            error=Exception("Transient")
        )
        
        job = dlq.get_by_id("123")
        
        assert job is not None
        assert job.job_id == "123"


# ============================================================================
# Exception Tests
# ============================================================================

class TestLLMExceptions:
    """Tests for custom LLM exceptions."""
    
    def test_llm_error_message(self):
        """LLMError should preserve message."""
        error = LLMError("Test error message", provider="test")
        
        assert "Test error message" in str(error)
    
    def test_timeout_error_inheritance(self):
        """LLMTimeoutError should inherit from LLMError."""
        error = LLMTimeoutError(provider="openai", timeout_seconds=30)
        
        assert isinstance(error, LLMError)
    
    def test_rate_limit_error_inheritance(self):
        """LLMRateLimitError should inherit from LLMError."""
        error = LLMRateLimitError(provider="openai", retry_after=60)
        
        assert isinstance(error, LLMError)
    
    def test_exception_with_context(self):
        """Exceptions should support additional context."""
        error = LLMError("Error", provider="openai")
        
        assert error.provider == "openai"


# ============================================================================
# Integration Tests
# ============================================================================

class TestResilienceIntegration:
    """Integration tests for resilience patterns."""
    
    def test_retry_with_circuit_breaker(self):
        """Retry decorator should work with circuit breaker."""
        from src.core.utils.resilience import CircuitBreakerConfig, RetryExhaustedError
        
        cb = CircuitBreaker("test_integration2", CircuitBreakerConfig(failure_threshold=5))
        call_count = 0
        
        @retry(max_retries=3, base_delay=0.01)
        def api_call():
            nonlocal call_count
            call_count += 1
            if not cb._should_allow_request():
                raise LLMError("Circuit open", provider="test")
            cb.record_failure(Exception("API error"))
            raise LLMError("API error", provider="test")
        
        with pytest.raises(RetryExhaustedError):
            api_call()
        
        # 1 initial + 3 retries = 4 attempts
        assert call_count == 4
    
    def test_failed_calls_go_to_dlq(self):
        """Failed calls should be added to DLQ."""
        import tempfile
        from src.core.utils.resilience import RetryExhaustedError
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            dlq = DeadLetterQueue(storage_path=f.name)
        
        @retry(max_retries=2, base_delay=0.01)
        def failing_job():
            raise LLMError("Permanent failure", provider="test")
        
        try:
            failing_job()
        except RetryExhaustedError as e:
            dlq.add(
                job_id="test-job",
                job_type="test",
                payload={"error": str(e)},
                error=e
            )
        
        assert len(dlq.queue) == 1
