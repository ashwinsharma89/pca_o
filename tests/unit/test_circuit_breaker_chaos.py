"""
Tests for Circuit Breaker Chaos and Resilience (Phase A.3).
Verifies state transitions and threshold logic.
"""

import pytest
import time
from unittest.mock import Mock, patch
from src.core.utils.circuit_breaker import CircuitBreaker, CircuitState, CircuitOpenError

class TestCircuitBreakerChaos:
    """Chaos and state transition tests for CircuitBreaker."""

    def test_circuit_closes_on_success(self):
        """Verify circuit stays closed on successful calls."""
        cb = CircuitBreaker("test", failure_threshold=2)
        assert cb.state == CircuitState.CLOSED
        
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb.is_available() is True

    def test_circuit_opens_on_threshold(self):
        """Verify circuit opens after threshold reached."""
        cb = CircuitBreaker("test", failure_threshold=2)
        
        cb.record_failure()
        assert cb.state == CircuitState.CLOSED
        
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        assert cb.is_available() is False
        
        with pytest.raises(CircuitOpenError):
            cb.call(lambda: "won't run")

    def test_recovery_timeout_to_half_open(self):
        """Verify circuit moves to half-open after timeout."""
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.1)
        
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        
        time.sleep(0.15)
        # Accessing state property triggers the transition check
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.is_available() is True

    def test_half_open_to_closed(self):
        """Verify circuit recovers fully after success in half-open."""
        cb = CircuitBreaker("test", failure_threshold=1, success_threshold=2, recovery_timeout=0.01)
        
        cb.record_failure()
        assert cb.state == CircuitState.OPEN
        
        time.sleep(0.02)
        assert cb.state == CircuitState.HALF_OPEN
        
        cb.record_success()
        assert cb.state == CircuitState.HALF_OPEN # Needs 2 successes
        
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
        assert cb._stats.failures == 0

    def test_half_open_to_open_on_failure(self):
        """Verify circuit re-opens immediately on failure in half-open."""
        cb = CircuitBreaker("test", failure_threshold=1, recovery_timeout=0.01)
        
        cb.record_failure()
        time.sleep(0.02)
        assert cb.state == CircuitState.HALF_OPEN
        
        cb.record_failure()
        assert cb.state == CircuitState.OPEN

    def test_context_manager_usage(self):
        """Verify context manager records results correctly."""
        cb = CircuitBreaker("test", failure_threshold=1)
        
        # Success
        with cb:
            pass
        assert cb._stats.successes == 1
        
        # Failure
        try:
            with cb:
                raise ValueError("error")
        except ValueError:
            pass
        
        assert cb.state == CircuitState.OPEN

    def test_decorator_usage(self):
        """Verify decorator records results correctly."""
        cb = CircuitBreaker("test", failure_threshold=1)
        
        @cb
        def failing_func():
            raise RuntimeError("fail")
            
        with pytest.raises(RuntimeError):
            failing_func()
            
        assert cb.state == CircuitState.OPEN
