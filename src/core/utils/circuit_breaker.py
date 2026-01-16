"""
Circuit Breaker Pattern
Resilience for external service calls
"""
import time
import logging
from enum import Enum
from typing import Callable, Any, Optional
from dataclasses import dataclass
from functools import wraps
from threading import Lock

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitStats:
    """Circuit breaker statistics"""
    failures: int = 0
    successes: int = 0
    last_failure_time: float = 0
    last_success_time: float = 0


class CircuitBreaker:
    """
    Circuit Breaker for resilient external calls
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Too many failures, requests fail fast
    - HALF_OPEN: Testing if service recovered
    
    Usage:
        cb = CircuitBreaker("openai", failure_threshold=5, recovery_timeout=30)
        
        @cb
        def call_openai():
            return openai.chat.completions.create(...)
        
        # Or manual
        with cb:
            result = call_external_api()
    """
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 3
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self._state = CircuitState.CLOSED
        self._stats = CircuitStats()
        self._half_open_calls = 0
        self._lock = Lock()
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state"""
        with self._lock:
            if self._state == CircuitState.OPEN:
                # Check if we should move to half-open
                if time.time() - self._stats.last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info(f"Circuit {self.name}: OPEN -> HALF_OPEN")
            return self._state
    
    def is_available(self) -> bool:
        """Check if circuit allows requests"""
        state = self.state
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.HALF_OPEN:
            with self._lock:
                return self._half_open_calls < self.half_open_max_calls
        return False
    
    def record_success(self):
        """Record a successful call"""
        with self._lock:
            self._stats.successes += 1
            self._stats.last_success_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                if self._stats.successes >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._stats.failures = 0
                    logger.info(f"Circuit {self.name}: HALF_OPEN -> CLOSED (recovered)")
    
    def record_failure(self, exception: Exception = None):
        """Record a failed call"""
        with self._lock:
            self._stats.failures += 1
            self._stats.last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                logger.warning(f"Circuit {self.name}: HALF_OPEN -> OPEN (failed again)")
            elif self._state == CircuitState.CLOSED:
                if self._stats.failures >= self.failure_threshold:
                    self._state = CircuitState.OPEN
                    logger.warning(f"Circuit {self.name}: CLOSED -> OPEN (threshold reached)")
            
            if exception:
                logger.error(f"Circuit {self.name} failure: {exception}")
    
    def reset(self):
        """Reset circuit to closed state"""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._stats = CircuitStats()
            self._half_open_calls = 0
            logger.info(f"Circuit {self.name}: Reset to CLOSED")
    
    def get_stats(self) -> dict:
        """Get circuit statistics"""
        return {
            "name": self.name,
            "state": self.state.value,
            "failures": self._stats.failures,
            "successes": self._stats.successes,
            "last_failure": self._stats.last_failure_time,
            "last_success": self._stats.last_success_time
        }
    
    # =========================================================================
    # Decorator & Context Manager
    # =========================================================================
    
    def __call__(self, func: Callable) -> Callable:
        """Use as decorator"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def __enter__(self):
        """Context manager entry"""
        if not self.is_available():
            raise CircuitOpenError(f"Circuit {self.name} is OPEN")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if exc_type is None:
            self.record_success()
        else:
            self.record_failure(exc_val)
        return False
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker"""
        if not self.is_available():
            raise CircuitOpenError(f"Circuit {self.name} is OPEN")
        
        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure(e)
            raise


class CircuitOpenError(Exception):
    """Raised when circuit is open"""
    pass


# ============================================================================
# Circuit Breaker Registry
# ============================================================================

class CircuitBreakerRegistry:
    """Registry of circuit breakers for different services"""
    
    def __init__(self):
        self._breakers: dict[str, CircuitBreaker] = {}
    
    def get(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0
    ) -> CircuitBreaker:
        """Get or create a circuit breaker"""
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(
                name=name,
                failure_threshold=failure_threshold,
                recovery_timeout=recovery_timeout
            )
        return self._breakers[name]
    
    def get_all_stats(self) -> list[dict]:
        """Get stats for all circuits"""
        return [cb.get_stats() for cb in self._breakers.values()]
    
    def reset_all(self):
        """Reset all circuits"""
        for cb in self._breakers.values():
            cb.reset()


# Global registry
_registry = CircuitBreakerRegistry()

def get_circuit_breaker(name: str, **kwargs) -> CircuitBreaker:
    """Get a circuit breaker from the registry"""
    return _registry.get(name, **kwargs)

def get_all_circuit_stats() -> list[dict]:
    """Get stats for all circuit breakers"""
    return _registry.get_all_stats()


# Pre-configured circuit breakers for common services
openai_circuit = get_circuit_breaker("openai", failure_threshold=3, recovery_timeout=60)
groq_circuit = get_circuit_breaker("groq", failure_threshold=5, recovery_timeout=30)
redis_circuit = get_circuit_breaker("redis", failure_threshold=10, recovery_timeout=10)
database_circuit = get_circuit_breaker("database", failure_threshold=5, recovery_timeout=20)
