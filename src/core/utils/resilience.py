"""
Resilience Module - Error Handling, Retry Logic, Circuit Breakers, and Graceful Degradation

This module provides production-grade resilience patterns for the PCA Agent system:
- Retry with exponential backoff
- Circuit breaker pattern
- Timeout handling
- Dead letter queue for failed jobs
- Custom exceptions
- Health checks
"""

import time
import functools
import threading
import asyncio
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Type, Union
from enum import Enum
from dataclasses import dataclass, field
from collections import deque
import json
import os
from pathlib import Path
from loguru import logger


# =============================================================================
# CUSTOM EXCEPTIONS
# =============================================================================

class PCAAgentError(Exception):
    """Base exception for all PCA Agent errors."""
    def __init__(self, message: str, error_code: str = "PCA_ERROR", details: Optional[Dict] = None):
        self.message = message
        self.error_code = error_code
        self.details = details or {}
        self.timestamp = datetime.utcnow().isoformat()
        super().__init__(self.message)
    
    def to_dict(self) -> Dict:
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp
        }


class LLMError(PCAAgentError):
    """Errors related to LLM API calls."""
    def __init__(self, message: str, provider: str = "unknown", details: Optional[Dict] = None):
        super().__init__(message, f"LLM_{provider.upper()}_ERROR", details)
        self.provider = provider


class LLMRateLimitError(LLMError):
    """Rate limit exceeded on LLM API."""
    def __init__(self, provider: str, retry_after: Optional[int] = None):
        details = {"retry_after_seconds": retry_after} if retry_after else {}
        super().__init__(f"Rate limit exceeded for {provider}", provider, details)
        self.retry_after = retry_after


class LLMTimeoutError(LLMError):
    """Timeout on LLM API call."""
    def __init__(self, provider: str, timeout_seconds: float):
        super().__init__(f"Timeout after {timeout_seconds}s for {provider}", provider, {"timeout": timeout_seconds})


class LLMConnectionError(LLMError):
    """Connection error to LLM API."""
    def __init__(self, provider: str, original_error: str):
        super().__init__(f"Connection failed to {provider}: {original_error}", provider)


class DatabaseError(PCAAgentError):
    """Database-related errors."""
    def __init__(self, message: str, query: Optional[str] = None):
        details = {"query": query[:200] + "..." if query and len(query) > 200 else query}
        super().__init__(message, "DATABASE_ERROR", details)


class DataValidationError(PCAAgentError):
    """Data validation errors."""
    def __init__(self, message: str, field: Optional[str] = None, value: Any = None):
        details = {"field": field, "value": str(value)[:100] if value else None}
        super().__init__(message, "VALIDATION_ERROR", details)


class RAGError(PCAAgentError):
    """RAG/Knowledge base errors."""
    def __init__(self, message: str, source: Optional[str] = None):
        super().__init__(message, "RAG_ERROR", {"source": source})


class CircuitOpenError(PCAAgentError):
    """Circuit breaker is open, requests are blocked."""
    def __init__(self, service_name: str, reset_time: datetime):
        super().__init__(
            f"Circuit breaker open for {service_name}. Resets at {reset_time.isoformat()}",
            "CIRCUIT_OPEN",
            {"service": service_name, "reset_time": reset_time.isoformat()}
        )


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5          # Failures before opening
    success_threshold: int = 2          # Successes in half-open to close
    timeout_seconds: float = 30.0       # Time before trying half-open
    excluded_exceptions: tuple = ()     # Exceptions that don't count as failures


class CircuitBreaker:
    """
    Circuit Breaker Pattern Implementation.
    
    Prevents cascade failures by stopping requests to failing services.
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Service is failing, requests are rejected immediately
    - HALF_OPEN: Testing if service recovered, limited requests allowed
    """
    
    _instances: Dict[str, 'CircuitBreaker'] = {}
    _lock = threading.Lock()
    
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self._state_lock = threading.Lock()
    
    @classmethod
    def get_instance(cls, name: str, config: Optional[CircuitBreakerConfig] = None) -> 'CircuitBreaker':
        """Get or create a circuit breaker instance by name."""
        with cls._lock:
            if name not in cls._instances:
                cls._instances[name] = cls(name, config)
            return cls._instances[name]
    
    @classmethod
    def get_all_states(cls) -> Dict[str, Dict]:
        """Get states of all circuit breakers."""
        return {
            name: {
                "state": cb.state.value,
                "failure_count": cb.failure_count,
                "success_count": cb.success_count,
                "last_failure": cb.last_failure_time.isoformat() if cb.last_failure_time else None
            }
            for name, cb in cls._instances.items()
        }
    
    def _should_allow_request(self) -> bool:
        """Check if request should be allowed based on current state."""
        with self._state_lock:
            if self.state == CircuitState.CLOSED:
                return True
            
            if self.state == CircuitState.OPEN:
                # Check if timeout has passed
                if self.last_failure_time:
                    elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if elapsed >= self.config.timeout_seconds:
                        self.state = CircuitState.HALF_OPEN
                        self.success_count = 0
                        logger.info(f"Circuit breaker '{self.name}' transitioning to HALF_OPEN")
                        return True
                return False
            
            # HALF_OPEN - allow request for testing
            return True
    
    def record_success(self):
        """Record a successful call."""
        with self._state_lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    logger.info(f"Circuit breaker '{self.name}' CLOSED - service recovered")
            elif self.state == CircuitState.CLOSED:
                # Reset failure count on success
                self.failure_count = 0
    
    def record_failure(self, exception: Exception):
        """Record a failed call."""
        # Check if this exception should be excluded
        if isinstance(exception, self.config.excluded_exceptions):
            return
        
        with self._state_lock:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow()
            
            if self.state == CircuitState.HALF_OPEN:
                # Immediately open on failure in half-open
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker '{self.name}' OPEN - failed in half-open state")
            elif self.state == CircuitState.CLOSED:
                if self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitState.OPEN
                    logger.warning(f"Circuit breaker '{self.name}' OPEN - threshold reached ({self.failure_count} failures)")
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if not self._should_allow_request():
            reset_time = self.last_failure_time + timedelta(seconds=self.config.timeout_seconds)
            raise CircuitOpenError(self.name, reset_time)
        
        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure(e)
            raise
    
    def reset(self):
        """Manually reset the circuit breaker."""
        with self._state_lock:
            self.state = CircuitState.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            logger.info(f"Circuit breaker '{self.name}' manually reset")


def circuit_breaker(name: str, config: Optional[CircuitBreakerConfig] = None):
    """Decorator to apply circuit breaker to a function."""
    def decorator(func: Callable) -> Callable:
        cb = CircuitBreaker.get_instance(name, config)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return cb.call(func, *args, **kwargs)
        
        # Attach circuit breaker for inspection
        wrapper.circuit_breaker = cb
        return wrapper
    
    return decorator


# =============================================================================
# RETRY WITH EXPONENTIAL BACKOFF
# =============================================================================

@dataclass
class RetryConfig:
    """Configuration for retry logic."""
    max_retries: int = 3
    base_delay: float = 1.0           # Initial delay in seconds
    max_delay: float = 60.0           # Maximum delay
    exponential_base: float = 2.0     # Multiplier for exponential backoff
    jitter: bool = True               # Add randomness to prevent thundering herd
    retryable_exceptions: tuple = (Exception,)  # Exceptions to retry on
    non_retryable_exceptions: tuple = ()        # Exceptions to never retry


class RetryExhaustedError(PCAAgentError):
    """All retry attempts exhausted."""
    def __init__(self, operation: str, attempts: int, last_error: Exception):
        super().__init__(
            f"Retry exhausted for '{operation}' after {attempts} attempts: {str(last_error)}",
            "RETRY_EXHAUSTED",
            {"operation": operation, "attempts": attempts, "last_error": str(last_error)}
        )
        self.last_error = last_error


def retry(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    retryable_exceptions: tuple = (Exception,),
    non_retryable_exceptions: tuple = (),
    on_retry: Optional[Callable[[int, Exception], None]] = None
):
    """
    Decorator for retry with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries
        max_delay: Maximum delay between retries
        exponential_base: Multiplier for exponential backoff
        jitter: Add randomness to delay
        retryable_exceptions: Tuple of exceptions to retry on
        non_retryable_exceptions: Tuple of exceptions to never retry
        on_retry: Callback function called on each retry (attempt, exception)
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import random
            
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except non_retryable_exceptions as e:
                    # Never retry these
                    raise
                except retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # Last attempt failed
                        break
                    
                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)
                    
                    if jitter:
                        delay = delay * (0.5 + random.random())  # nosec B311
                    
                    logger.warning(
                        f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                        f"after {delay:.2f}s - Error: {str(e)[:100]}"
                    )
                    
                    if on_retry:
                        on_retry(attempt + 1, e)
                    
                    time.sleep(delay)
            
            raise RetryExhaustedError(func.__name__, max_retries + 1, last_exception)
        
        return wrapper
    return decorator


async def async_retry(
    func: Callable,
    *args,
    max_retries: int = 3,
    base_delay: float = 1.0,
    **kwargs
) -> Any:
    """Async version of retry logic."""
    import random
    
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_exception = e
            
            if attempt == max_retries:
                break
            
            delay = min(base_delay * (2 ** attempt), 60.0)
            delay = delay * (0.5 + random.random())  # nosec B311
            
            logger.warning(f"Async retry {attempt + 1}/{max_retries} after {delay:.2f}s")
            await asyncio.sleep(delay)
    
    raise RetryExhaustedError(func.__name__, max_retries + 1, last_exception)


# =============================================================================
# TIMEOUT HANDLING
# =============================================================================

class TimeoutError(PCAAgentError):
    """Operation timed out."""
    def __init__(self, operation: str, timeout_seconds: float):
        super().__init__(
            f"Operation '{operation}' timed out after {timeout_seconds}s",
            "TIMEOUT",
            {"operation": operation, "timeout_seconds": timeout_seconds}
        )


def timeout(seconds: float, error_message: Optional[str] = None):
    """
    Decorator to add timeout to a function.
    
    Note: This uses threading and may not work with all functions.
    For async functions, use asyncio.wait_for instead.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = [None]
            exception = [None]
            
            def target():
                try:
                    result[0] = func(*args, **kwargs)
                except Exception as e:
                    exception[0] = e
            
            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()
            thread.join(timeout=seconds)
            
            if thread.is_alive():
                msg = error_message or f"Function {func.__name__} timed out"
                raise TimeoutError(func.__name__, seconds)
            
            if exception[0]:
                raise exception[0]
            
            return result[0]
        
        return wrapper
    return decorator


# =============================================================================
# DEAD LETTER QUEUE
# =============================================================================

@dataclass
class FailedJob:
    """Represents a failed job in the dead letter queue."""
    job_id: str
    job_type: str
    payload: Dict
    error: str
    error_code: str
    timestamp: datetime
    retry_count: int = 0
    max_retries: int = 3
    metadata: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "payload": self.payload,
            "error": self.error,
            "error_code": self.error_code,
            "timestamp": self.timestamp.isoformat(),
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'FailedJob':
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        return cls(**data)


class DeadLetterQueue:
    """
    Dead Letter Queue for tracking and managing failed jobs.
    
    Provides:
    - Persistent storage of failed jobs
    - Retry mechanism
    - Job inspection and management
    """
    
    _instance: Optional['DeadLetterQueue'] = None
    _lock = threading.Lock()
    
    def __init__(self, storage_path: Optional[str] = None, max_size: int = 1000):
        self.storage_path = storage_path or os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "dead_letter_queue.json"
        )
        self.max_size = max_size
        self.queue: deque[FailedJob] = deque(maxlen=max_size)
        self._queue_lock = threading.Lock()
        self._load_from_disk()
    
    @classmethod
    def get_instance(cls, storage_path: Optional[str] = None) -> 'DeadLetterQueue':
        """Get singleton instance."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(storage_path)
            return cls._instance
    
    def _load_from_disk(self):
        """Load queue from persistent storage."""
        try:
            path = Path(self.storage_path)
            if path.exists():
                with open(path, 'r') as f:
                    data = json.load(f)
                    for item in data:
                        self.queue.append(FailedJob.from_dict(item))
                logger.info(f"Loaded {len(self.queue)} failed jobs from disk")
        except Exception as e:
            logger.warning(f"Could not load dead letter queue: {e}")
    
    def _save_to_disk(self):
        """Persist queue to disk."""
        try:
            path = Path(self.storage_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'w') as f:
                json.dump([job.to_dict() for job in self.queue], f, indent=2)
        except Exception as e:
            logger.error(f"Could not save dead letter queue: {e}")
    
    def add(
        self,
        job_id: str,
        job_type: str,
        payload: Dict,
        error: Exception,
        metadata: Optional[Dict] = None
    ) -> FailedJob:
        """Add a failed job to the queue."""
        error_code = getattr(error, 'error_code', 'UNKNOWN_ERROR')
        
        failed_job = FailedJob(
            job_id=job_id,
            job_type=job_type,
            payload=payload,
            error=str(error),
            error_code=error_code,
            timestamp=datetime.utcnow(),
            metadata=metadata or {}
        )
        
        with self._queue_lock:
            self.queue.append(failed_job)
            self._save_to_disk()
        
        logger.error(f"Job {job_id} added to dead letter queue: {error}")
        return failed_job
    
    def get_all(self) -> List[FailedJob]:
        """Get all failed jobs."""
        with self._queue_lock:
            return list(self.queue)
    
    def get_by_type(self, job_type: str) -> List[FailedJob]:
        """Get failed jobs by type."""
        with self._queue_lock:
            return [job for job in self.queue if job.job_type == job_type]
    
    def get_by_id(self, job_id: str) -> Optional[FailedJob]:
        """Get a specific failed job by ID."""
        with self._queue_lock:
            for job in self.queue:
                if job.job_id == job_id:
                    return job
            return None
    
    def remove(self, job_id: str) -> bool:
        """Remove a job from the queue."""
        with self._queue_lock:
            for i, job in enumerate(self.queue):
                if job.job_id == job_id:
                    del self.queue[i]
                    self._save_to_disk()
                    return True
            return False
    
    def retry(self, job_id: str, handler: Callable[[FailedJob], Any]) -> bool:
        """
        Retry a failed job.
        
        Args:
            job_id: ID of the job to retry
            handler: Function to execute the job
            
        Returns:
            True if retry succeeded, False otherwise
        """
        job = self.get_by_id(job_id)
        if not job:
            logger.warning(f"Job {job_id} not found in dead letter queue")
            return False
        
        if job.retry_count >= job.max_retries:
            logger.warning(f"Job {job_id} has exceeded max retries ({job.max_retries})")
            return False
        
        try:
            handler(job)
            self.remove(job_id)
            logger.info(f"Job {job_id} successfully retried and removed from DLQ")
            return True
        except Exception as e:
            job.retry_count += 1
            job.error = str(e)
            job.timestamp = datetime.utcnow()
            self._save_to_disk()
            logger.warning(f"Job {job_id} retry failed (attempt {job.retry_count}): {e}")
            return False
    
    def clear(self):
        """Clear all jobs from the queue."""
        with self._queue_lock:
            self.queue.clear()
            self._save_to_disk()
        logger.info("Dead letter queue cleared")
    
    def stats(self) -> Dict:
        """Get queue statistics."""
        with self._queue_lock:
            jobs = list(self.queue)
        
        by_type = {}
        by_error_code = {}
        
        for job in jobs:
            by_type[job.job_type] = by_type.get(job.job_type, 0) + 1
            by_error_code[job.error_code] = by_error_code.get(job.error_code, 0) + 1
        
        return {
            "total_jobs": len(jobs),
            "by_type": by_type,
            "by_error_code": by_error_code,
            "oldest_job": jobs[0].timestamp.isoformat() if jobs else None,
            "newest_job": jobs[-1].timestamp.isoformat() if jobs else None
        }


# =============================================================================
# HEALTH CHECK
# =============================================================================

@dataclass
class HealthStatus:
    """Health status of a service."""
    name: str
    healthy: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    details: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "healthy": self.healthy,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "details": self.details
        }


class HealthChecker:
    """
    Health checker for monitoring service dependencies.
    """
    
    def __init__(self):
        self.checks: Dict[str, Callable[[], HealthStatus]] = {}
    
    def register(self, name: str, check_func: Callable[[], HealthStatus]):
        """Register a health check."""
        self.checks[name] = check_func
    
    def check(self, name: str) -> HealthStatus:
        """Run a specific health check."""
        if name not in self.checks:
            return HealthStatus(name=name, healthy=False, error="Check not registered")
        
        start = time.time()
        try:
            status = self.checks[name]()
            status.latency_ms = (time.time() - start) * 1000
            return status
        except Exception as e:
            return HealthStatus(
                name=name,
                healthy=False,
                latency_ms=(time.time() - start) * 1000,
                error=str(e)
            )
    
    def check_all(self) -> Dict[str, HealthStatus]:
        """Run all health checks."""
        return {name: self.check(name) for name in self.checks}
    
    def is_healthy(self) -> bool:
        """Check if all services are healthy."""
        return all(status.healthy for status in self.check_all().values())


# Global health checker instance
health_checker = HealthChecker()


def register_health_check(name: str):
    """Decorator to register a function as a health check."""
    def decorator(func: Callable[[], bool]):
        @functools.wraps(func)
        def wrapper() -> HealthStatus:
            try:
                result = func()
                return HealthStatus(name=name, healthy=result)
            except Exception as e:
                return HealthStatus(name=name, healthy=False, error=str(e))
        
        health_checker.register(name, wrapper)
        return wrapper
    return decorator


# =============================================================================
# GRACEFUL DEGRADATION
# =============================================================================

class FallbackChain:
    """
    Fallback chain for graceful degradation.
    
    Tries multiple strategies in order until one succeeds.
    """
    
    def __init__(self, name: str):
        self.name = name
        self.strategies: List[tuple[str, Callable]] = []
    
    def add(self, name: str, func: Callable) -> 'FallbackChain':
        """Add a fallback strategy."""
        self.strategies.append((name, func))
        return self
    
    def execute(self, *args, **kwargs) -> tuple[str, Any]:
        """
        Execute strategies in order until one succeeds.
        
        Returns:
            Tuple of (strategy_name, result)
        """
        errors = []
        
        for strategy_name, func in self.strategies:
            try:
                logger.debug(f"Trying strategy '{strategy_name}' for {self.name}")
                result = func(*args, **kwargs)
                logger.info(f"Strategy '{strategy_name}' succeeded for {self.name}")
                return (strategy_name, result)
            except Exception as e:
                logger.warning(f"Strategy '{strategy_name}' failed: {e}")
                errors.append((strategy_name, e))
        
        # All strategies failed
        error_summary = "; ".join([f"{name}: {str(e)[:50]}" for name, e in errors])
        raise PCAAgentError(
            f"All fallback strategies failed for {self.name}: {error_summary}",
            "FALLBACK_EXHAUSTED",
            {"strategies_tried": [name for name, _ in errors]}
        )


def with_fallback(primary: Callable, fallback: Callable, fallback_exceptions: tuple = (Exception,)):
    """
    Decorator/wrapper to add a fallback to a function.
    
    Args:
        primary: Primary function to try
        fallback: Fallback function if primary fails
        fallback_exceptions: Exceptions that trigger fallback
    """
    @functools.wraps(primary)
    def wrapper(*args, **kwargs):
        try:
            return primary(*args, **kwargs)
        except fallback_exceptions as e:
            logger.warning(f"Primary function failed, using fallback: {e}")
            return fallback(*args, **kwargs)
    
    return wrapper


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def safe_execute(
    func: Callable,
    *args,
    default: Any = None,
    log_error: bool = True,
    **kwargs
) -> Any:
    """
    Safely execute a function, returning default on error.
    
    Args:
        func: Function to execute
        default: Default value to return on error
        log_error: Whether to log the error
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_error:
            logger.error(f"Safe execute failed for {func.__name__}: {e}")
        return default


def get_resilience_status() -> Dict:
    """Get overall resilience system status."""
    return {
        "circuit_breakers": CircuitBreaker.get_all_states(),
        "dead_letter_queue": DeadLetterQueue.get_instance().stats(),
        "health_checks": {
            name: status.to_dict() 
            for name, status in health_checker.check_all().items()
        }
    }


# =============================================================================
# PRE-CONFIGURED DECORATORS FOR COMMON USE CASES
# =============================================================================

# LLM API calls - retry with longer delays, circuit breaker
llm_retry = retry(
    max_retries=3,
    base_delay=2.0,
    max_delay=30.0,
    retryable_exceptions=(LLMConnectionError, LLMTimeoutError),
    non_retryable_exceptions=(LLMRateLimitError,)
)

# Database operations - quick retries
db_retry = retry(
    max_retries=2,
    base_delay=0.5,
    max_delay=5.0,
    retryable_exceptions=(DatabaseError,)
)

# API endpoints - fast timeout
api_timeout = timeout(seconds=30.0)

# LLM circuit breaker config
llm_circuit_config = CircuitBreakerConfig(
    failure_threshold=3,
    success_threshold=2,
    timeout_seconds=60.0
)


logger.info("Resilience module loaded successfully")
