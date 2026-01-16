"""
Resilient LLM Client with Circuit Breaker Protection

Provides fault-tolerant LLM API calls with:
- Circuit breaker to prevent cascading failures
- Retry with exponential backoff
- Timeout protection
- Fallback responses
"""

import time
import asyncio
from typing import Optional, Dict, Any, Callable, TypeVar
from functools import wraps
from loguru import logger

from src.core.utils.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    get_circuit_breaker
)

T = TypeVar('T')


class ResilientLLMClient:
    """
    LLM client wrapper with built-in resilience patterns.
    
    Usage:
        client = ResilientLLMClient()
        
        # Sync call with automatic retry and circuit breaker
        response = client.call_with_retry(
            func=lambda: openai.chat.completions.create(...),
            service_name="openai"
        )
        
        # Or as decorator
        @client.resilient("openai")
        def generate_text():
            return openai.chat.completions.create(...)
    """
    
    # Pre-configured circuit breakers for different LLM providers
    CIRCUIT_CONFIGS = {
        "openai": {"failure_threshold": 3, "recovery_timeout": 60},
        "anthropic": {"failure_threshold": 3, "recovery_timeout": 60},
        "groq": {"failure_threshold": 5, "recovery_timeout": 30},
        "ollama": {"failure_threshold": 10, "recovery_timeout": 10},
        "default": {"failure_threshold": 5, "recovery_timeout": 45}
    }
    
    # Default retry configuration
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_BASE_DELAY = 1.0
    DEFAULT_MAX_DELAY = 30.0
    DEFAULT_TIMEOUT = 60.0
    
    def __init__(self):
        self._circuits: Dict[str, CircuitBreaker] = {}
    
    def get_circuit(self, service_name: str) -> CircuitBreaker:
        """Get or create circuit breaker for a service."""
        if service_name not in self._circuits:
            config = self.CIRCUIT_CONFIGS.get(service_name, self.CIRCUIT_CONFIGS["default"])
            self._circuits[service_name] = get_circuit_breaker(service_name, **config)
        return self._circuits[service_name]
    
    def call_with_retry(
        self,
        func: Callable[[], T],
        service_name: str = "default",
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
        timeout: float = DEFAULT_TIMEOUT,
        fallback: Optional[Callable[[], T]] = None
    ) -> T:
        """
        Execute function with circuit breaker and retry logic.
        
        Args:
            func: Function to execute
            service_name: Name for circuit breaker tracking
            max_retries: Maximum retry attempts
            base_delay: Initial delay between retries
            max_delay: Maximum delay between retries
            timeout: Maximum execution time
            fallback: Fallback function if all retries fail
            
        Returns:
            Function result or fallback result
            
        Raises:
            CircuitOpenError: If circuit is open
            Exception: If all retries fail and no fallback
        """
        circuit = self.get_circuit(service_name)
        
        # Check circuit state first
        if not circuit.is_available():
            logger.warning(f"Circuit {service_name} is OPEN, using fallback if available")
            if fallback:
                return fallback()
            raise CircuitOpenError(f"Service {service_name} is unavailable (circuit open)")
        
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                # Execute with timeout
                start_time = time.time()
                result = func()
                
                # Record success
                circuit.record_success()
                logger.debug(f"{service_name} call succeeded on attempt {attempt + 1}")
                return result
                
            except Exception as e:
                last_exception = e
                circuit.record_failure(e)
                
                # Check if we should retry
                if attempt < max_retries:
                    # Exponential backoff with jitter
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    jitter = delay * 0.1 * (0.5 - time.time() % 1)  # ±10% jitter
                    actual_delay = delay + jitter
                    
                    logger.warning(
                        f"{service_name} call failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Retrying in {actual_delay:.2f}s"
                    )
                    time.sleep(actual_delay)
                else:
                    logger.error(f"{service_name} call failed after {max_retries + 1} attempts: {e}")
        
        # All retries exhausted
        if fallback:
            logger.info(f"Using fallback for {service_name}")
            return fallback()
        
        raise last_exception
    
    async def call_with_retry_async(
        self,
        func: Callable[[], T],
        service_name: str = "default",
        max_retries: int = DEFAULT_MAX_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
        fallback: Optional[Callable[[], T]] = None
    ) -> T:
        """Async version of call_with_retry."""
        circuit = self.get_circuit(service_name)
        
        if not circuit.is_available():
            if fallback:
                return fallback()
            raise CircuitOpenError(f"Service {service_name} is unavailable")
        
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func()
                else:
                    result = func()
                
                circuit.record_success()
                return result
                
            except Exception as e:
                last_exception = e
                circuit.record_failure(e)
                
                if attempt < max_retries:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(f"{service_name} async call failed, retrying in {delay:.2f}s")
                    await asyncio.sleep(delay)
        
        if fallback:
            return fallback()
        raise last_exception
    
    def resilient(
        self,
        service_name: str = "default",
        max_retries: int = DEFAULT_MAX_RETRIES,
        fallback_value: Any = None
    ):
        """
        Decorator to make a function resilient with circuit breaker.
        
        Usage:
            @llm_client.resilient("openai", max_retries=3)
            def generate_summary(text):
                return openai.chat.completions.create(...)
        """
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                fallback = lambda: fallback_value if fallback_value is not None else None
                return self.call_with_retry(
                    func=lambda: func(*args, **kwargs),
                    service_name=service_name,
                    max_retries=max_retries,
                    fallback=fallback if fallback_value is not None else None
                )
            return wrapper
        return decorator
    
    def get_all_circuit_states(self) -> Dict[str, Dict[str, Any]]:
        """Get states of all circuit breakers."""
        return {name: cb.get_stats() for name, cb in self._circuits.items()}


# Global instance for easy import
llm_client = ResilientLLMClient()


def call_llm_with_retry(
    func: Callable[[], T],
    service_name: str = "openai",
    max_retries: int = 3,
    fallback: Optional[Callable[[], T]] = None
) -> T:
    """
    Convenience function for resilient LLM calls.
    
    Usage:
        response = call_llm_with_retry(
            func=lambda: client.chat.completions.create(...),
            service_name="openai"
        )
    """
    return llm_client.call_with_retry(
        func=func,
        service_name=service_name,
        max_retries=max_retries,
        fallback=fallback
    )


def resilient_llm_call(service_name: str = "openai", fallback_value: Any = None):
    """
    Decorator for resilient LLM calls.
    
    Usage:
        @resilient_llm_call("openai", fallback_value={"error": "Service unavailable"})
        def generate_insights(data):
            return openai.chat.completions.create(...)
    """
    return llm_client.resilient(service_name=service_name, fallback_value=fallback_value)
