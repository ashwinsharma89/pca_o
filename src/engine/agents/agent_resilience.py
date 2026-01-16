"""
Agent resilience mechanisms: fallback and retry logic.

Prevents single agent failures from cascading to system-wide failures.
"""

from typing import Callable, Any, Optional, Type, TypeVar, Union
from functools import wraps
import time
import asyncio
from loguru import logger
from datetime import datetime

T = TypeVar('T')


class AgentExecutionError(Exception):
    """Raised when an agent execution fails"""
    pass


class AgentFallback:
    """
    Fallback mechanism for agent failures.
    
    If primary agent fails, automatically falls back to a simpler agent.
    Tracks fallback usage for monitoring.
    """
    
    def __init__(self, primary_agent: Any, fallback_agent: Optional[Any] = None, name: str = "Agent"):
        """
        Initialize fallback mechanism.
        
        Args:
            primary_agent: Primary agent to use
            fallback_agent: Fallback agent if primary fails (optional)
            name: Name for logging purposes
        """
        self.primary = primary_agent
        self.fallback = fallback_agent
        self.name = name
        self.fallback_count = 0
        self.total_executions = 0
        self.last_fallback_time: Optional[datetime] = None
    
    async def execute(self, method_name: str, *args, **kwargs) -> Any:
        """
        Execute method with fallback support (async).
        
        Args:
            method_name: Name of method to call on agent
            *args: Positional arguments for method
            **kwargs: Keyword arguments for method
            
        Returns:
            Result from primary or fallback agent
            
        Raises:
            AgentExecutionError: If both primary and fallback fail
        """
        self.total_executions += 1
        
        try:
            logger.debug(f"Executing {self.name}.{method_name} (primary)")
            method = getattr(self.primary, method_name)
            
            if asyncio.iscoroutinefunction(method):
                return await method(*args, **kwargs)
            else:
                return method(*args, **kwargs)
                
        except Exception as e:
            logger.warning(f"{self.name} primary agent failed: {e}")
            
            if self.fallback:
                self.fallback_count += 1
                self.last_fallback_time = datetime.utcnow()
                
                logger.info(f"Attempting {self.name} fallback agent (fallback #{self.fallback_count})")
                
                try:
                    method = getattr(self.fallback, method_name)
                    if asyncio.iscoroutinefunction(method):
                        result = await method(*args, **kwargs)
                    else:
                        result = method(*args, **kwargs)
                    
                    logger.info(f"{self.name} fallback successful")
                    return result
                    
                except Exception as fallback_error:
                    logger.error(f"{self.name} fallback also failed: {fallback_error}")
                    raise AgentExecutionError(
                        f"Both primary and fallback agents failed. "
                        f"Primary: {e}, Fallback: {fallback_error}"
                    )
            else:
                logger.error(f"{self.name} failed and no fallback configured")
                raise AgentExecutionError(f"Agent failed and no fallback available: {e}")
    
    def get_stats(self) -> dict:
        """Get fallback usage statistics"""
        fallback_rate = (self.fallback_count / self.total_executions * 100) if self.total_executions > 0 else 0
        
        return {
            "agent_name": self.name,
            "total_executions": self.total_executions,
            "fallback_count": self.fallback_count,
            "fallback_rate_percent": round(fallback_rate, 2),
            "last_fallback_time": self.last_fallback_time.isoformat() if self.last_fallback_time else None,
            "has_fallback": self.fallback is not None
        }


def retry_with_backoff(
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,)
):
    """
    Retry decorator with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Multiplier for delay between retries
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        exceptions: Tuple of exceptions to catch and retry
        
    Example:
        @retry_with_backoff(max_retries=3, backoff_factor=2)
        async def analyze_data(data):
            # This will retry up to 3 times with exponential backoff
            return await some_api_call(data)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries} retries. "
                            f"Last error: {e}"
                        )
                        raise
                    
                    # Calculate delay with exponential backoff
                    current_delay = min(delay * (backoff_factor ** attempt), max_delay)
                    
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}. "
                        f"Retrying in {current_delay:.1f}s"
                    )
                    
                    await asyncio.sleep(current_delay)
            
            # Should never reach here, but just in case
            raise last_exception
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            delay = initial_delay
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries} retries. "
                            f"Last error: {e}"
                        )
                        raise
                    
                    current_delay = min(delay * (backoff_factor ** attempt), max_delay)
                    
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}. "
                        f"Retrying in {current_delay:.1f}s"
                    )
                    
                    time.sleep(current_delay)
            
            raise last_exception
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


class CircuitBreaker:
    """
    Circuit breaker pattern to prevent repeated calls to failing agents.
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Agent is failing, requests fail fast
    - HALF_OPEN: Testing if agent has recovered
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: Type[Exception] = Exception
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type to catch
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If circuit is open or function fails
        """
        if self.state == "OPEN":
            # Check if recovery timeout has passed
            if self.last_failure_time:
                time_since_failure = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if time_since_failure >= self.recovery_timeout:
                    logger.info("Circuit breaker entering HALF_OPEN state")
                    self.state = "HALF_OPEN"
                else:
                    raise AgentExecutionError(
                        f"Circuit breaker is OPEN. Agent is unavailable. "
                        f"Retry in {self.recovery_timeout - time_since_failure:.1f}s"
                    )
        
        try:
            result = func(*args, **kwargs)
            
            # Success - reset if in HALF_OPEN
            if self.state == "HALF_OPEN":
                logger.info("Circuit breaker recovered, returning to CLOSED state")
                self.failure_count = 0
                self.state = "CLOSED"
            
            return result
            
        except self.expected_exception as e:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow()
            
            logger.warning(f"Circuit breaker failure {self.failure_count}/{self.failure_threshold}")
            
            if self.failure_count >= self.failure_threshold:
                logger.error("Circuit breaker threshold reached, opening circuit")
                self.state = "OPEN"
            
            raise
    
    def reset(self):
        """Manually reset circuit breaker"""
        self.failure_count = 0
        self.state = "CLOSED"
        self.last_failure_time = None
        logger.info("Circuit breaker manually reset")
    
    def get_state(self) -> dict:
        """Get current circuit breaker state"""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "failure_threshold": self.failure_threshold,
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None
        }
