"""
Tests for agent resilience mechanisms (fallback, retry, circuit breaker).
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta

from src.engine.agents.agent_resilience import (
    AgentFallback,
    AgentExecutionError,
    retry_with_backoff,
    CircuitBreaker
)


class MockAgent:
    """Mock agent for testing"""
    
    def __init__(self, should_fail=False, fail_count=0):
        self.should_fail = should_fail
        self.fail_count = fail_count
        self.call_count = 0
    
    async def analyze(self, data):
        """Async analyze method (simulating standard async agent)"""
        self.call_count += 1
        
        if self.should_fail:
            if self.fail_count == 0 or self.call_count <= self.fail_count:
                # Simulate async failure
                await asyncio.sleep(0.01)
                raise ValueError(f"Mock failure #{self.call_count}")
        
        return {"result": "success", "data": data}
    
    async def analyze_async(self, data):
        """Async analyze method"""
        self.call_count += 1
        
        if self.should_fail:
            if self.fail_count == 0 or self.call_count <= self.fail_count:
                raise ValueError(f"Mock async failure #{self.call_count}")
        
        return {"result": "success", "data": data}


class TestAgentFallback:
    """Test AgentFallback mechanism"""
    
    def test_primary_success(self):
        """Test successful execution with primary agent"""
        primary = MockAgent(should_fail=False)
        fallback_agent = AgentFallback(primary, name="TestAgent")
        
        result = asyncio.run(fallback_agent.execute('analyze', {"test": "data"}))
        
        assert result["result"] == "success"
        assert primary.call_count == 1
        assert fallback_agent.fallback_count == 0
    
    def test_fallback_on_primary_failure(self):
        """Test fallback when primary fails"""
        primary = MockAgent(should_fail=True)
        fallback = MockAgent(should_fail=False)
        fallback_agent = AgentFallback(primary, fallback, name="TestAgent")
        
        result = asyncio.run(fallback_agent.execute('analyze', {"test": "data"}))
        
        assert result["result"] == "success"
        assert primary.call_count == 1
        assert fallback.call_count == 1
        assert fallback_agent.fallback_count == 1
    
    def test_both_fail(self):
        """Test when both primary and fallback fail"""
        primary = MockAgent(should_fail=True)
        fallback = MockAgent(should_fail=True)
        fallback_agent = AgentFallback(primary, fallback, name="TestAgent")
        
        with pytest.raises(AgentExecutionError) as exc_info:
            asyncio.run(fallback_agent.execute('analyze', {"test": "data"}))
        
        assert "Both primary and fallback agents failed" in str(exc_info.value)
        assert primary.call_count == 1
        assert fallback.call_count == 1
    
    def test_no_fallback_configured(self):
        """Test failure when no fallback is configured"""
        primary = MockAgent(should_fail=True)
        fallback_agent = AgentFallback(primary, name="TestAgent")
        
        with pytest.raises(AgentExecutionError) as exc_info:
            asyncio.run(fallback_agent.execute('analyze', {"test": "data"}))
        
        assert "no fallback available" in str(exc_info.value)
    
    def test_stats_tracking(self):
        """Test fallback statistics tracking"""
        primary = MockAgent(should_fail=True)
        fallback = MockAgent(should_fail=False)
        fallback_agent = AgentFallback(primary, fallback, name="TestAgent")
        
        # Execute multiple times
        for _ in range(5):
            asyncio.run(fallback_agent.execute('analyze', {"test": "data"}))
        
        stats = fallback_agent.get_stats()
        
        assert stats["total_executions"] == 5
        assert stats["fallback_count"] == 5
        assert stats["fallback_rate_percent"] == 100.0
        assert stats["has_fallback"] is True
        assert stats["last_fallback_time"] is not None
    
    def test_async_method_support(self):
        """Test support for async methods"""
        primary = MockAgent(should_fail=True)
        fallback = MockAgent(should_fail=False)
        fallback_agent = AgentFallback(primary, fallback, name="TestAgent")
        
        result = asyncio.run(fallback_agent.execute('analyze_async', {"test": "data"}))
        
        assert result["result"] == "success"
        assert fallback.call_count == 1


class TestRetryWithBackoff:
    """Test retry_with_backoff decorator"""
    
    @pytest.mark.asyncio
    async def test_success_first_try(self):
        """Test successful execution on first try"""
        call_count = 0
        
        @retry_with_backoff(max_retries=3)
        async def test_func():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = await test_func()
        
        assert result == "success"
        assert call_count == 1
    
    @pytest.mark.asyncio
    async def test_success_after_retries(self):
        """Test successful execution after retries"""
        call_count = 0
        
        @retry_with_backoff(max_retries=3, initial_delay=0.01)
        async def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary failure")
            return "success"
        
        result = await test_func()
        
        assert result == "success"
        assert call_count == 3
    
    @pytest.mark.asyncio
    async def test_max_retries_exceeded(self):
        """Test failure after max retries"""
        call_count = 0
        
        @retry_with_backoff(max_retries=2, initial_delay=0.01)
        async def test_func():
            nonlocal call_count
            call_count += 1
            raise ValueError("Persistent failure")
        
        with pytest.raises(ValueError) as exc_info:
            await test_func()
        
        assert "Persistent failure" in str(exc_info.value)
        assert call_count == 3  # Initial + 2 retries
    
    @pytest.mark.asyncio
    async def test_exponential_backoff(self):
        """Test exponential backoff timing"""
        call_times = []
        
        @retry_with_backoff(max_retries=3, initial_delay=0.1, backoff_factor=2.0)
        async def test_func():
            call_times.append(datetime.utcnow())
            if len(call_times) < 3:
                raise ValueError("Retry needed")
            return "success"
        
        await test_func()
        
        # Check delays are increasing
        assert len(call_times) == 3
        delay1 = (call_times[1] - call_times[0]).total_seconds()
        delay2 = (call_times[2] - call_times[1]).total_seconds()
        
        # Second delay should be roughly 2x first delay
        assert delay2 > delay1
    
    def test_sync_function_support(self):
        """Test retry works with sync functions"""
        call_count = 0
        
        @retry_with_backoff(max_retries=2, initial_delay=0.01)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("Retry needed")
            return "success"
        
        result = test_func()
        
        assert result == "success"
        assert call_count == 2


class TestCircuitBreaker:
    """Test CircuitBreaker pattern"""
    
    def test_closed_state_normal_operation(self):
        """Test normal operation in CLOSED state"""
        breaker = CircuitBreaker(failure_threshold=3)
        
        def test_func():
            return "success"
        
        result = breaker.call(test_func)
        
        assert result == "success"
        assert breaker.state == "CLOSED"
        assert breaker.failure_count == 0
    
    def test_open_after_threshold(self):
        """Test circuit opens after failure threshold"""
        breaker = CircuitBreaker(failure_threshold=3, expected_exception=ValueError)
        
        def failing_func():
            raise ValueError("Failure")
        
        # Trigger failures
        for _ in range(3):
            with pytest.raises(ValueError):
                breaker.call(failing_func)
        
        assert breaker.state == "OPEN"
        assert breaker.failure_count == 3
        
        # Next call should fail fast
        with pytest.raises(AgentExecutionError) as exc_info:
            breaker.call(failing_func)
        
        assert "Circuit breaker is OPEN" in str(exc_info.value)
    
    def test_half_open_recovery(self):
        """Test recovery through HALF_OPEN state"""
        breaker = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=0.1,
            expected_exception=ValueError
        )
        
        def failing_func():
            raise ValueError("Failure")
        
        def success_func():
            return "success"
        
        # Open the circuit
        for _ in range(2):
            with pytest.raises(ValueError):
                breaker.call(failing_func)
        
        assert breaker.state == "OPEN"
        
        # Wait for recovery timeout
        import time
        time.sleep(0.15)
        
        # Should enter HALF_OPEN and succeed
        result = breaker.call(success_func)
        
        assert result == "success"
        assert breaker.state == "CLOSED"
        assert breaker.failure_count == 0
    
    def test_manual_reset(self):
        """Test manual circuit breaker reset"""
        breaker = CircuitBreaker(failure_threshold=2, expected_exception=ValueError)
        
        def failing_func():
            raise ValueError("Failure")
        
        # Open the circuit
        for _ in range(2):
            with pytest.raises(ValueError):
                breaker.call(failing_func)
        
        assert breaker.state == "OPEN"
        
        # Manual reset
        breaker.reset()
        
        assert breaker.state == "CLOSED"
        assert breaker.failure_count == 0
    
    def test_get_state(self):
        """Test getting circuit breaker state"""
        breaker = CircuitBreaker(failure_threshold=5)
        
        state = breaker.get_state()
        
        assert state["state"] == "CLOSED"
        assert state["failure_count"] == 0
        assert state["failure_threshold"] == 5
        assert state["last_failure_time"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
