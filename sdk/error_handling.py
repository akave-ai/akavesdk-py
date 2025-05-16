import threading
import time
import logging
from functools import wraps
from typing import Type, Callable, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class SDKError(Exception):
    """Base exception for SDK errors"""
    pass

class RetryableError(SDKError):
    """Exception indicating that the operation can be retried"""
    pass

class ConnectionError(SDKError):
    """Exception for connection-related errors"""
    pass

class CircuitBreakerError(SDKError):
    """Exception raised when circuit breaker is open"""
    pass

class CircuitBreaker:
    """
    Implementation of the Circuit Breaker pattern to prevent repeated failures.
    """
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.last_failure_time = None
        self._lock = threading.RLock()
        self._state = "CLOSED"  # CLOSED, OPEN, HALF-OPEN

    @property
    def is_open(self) -> bool:
        """Check if the circuit breaker is open"""
        return self._state == "OPEN"

    def success(self):
        """Record a successful operation"""
        with self._lock:
            self.failure_count = 0
            self._state = "CLOSED"

    def failure(self):
        """Record a failed operation"""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self._state = "OPEN"

    def reset(self):
        """Reset the circuit breaker state"""
        with self._lock:
            self.failure_count = 0
            self._state = "HALF-OPEN"

    def __call__(self, func: Callable) -> Callable:
        """
        Decorator to wrap functions with circuit breaker functionality.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self._lock:
                if self.is_open:
                    if time.time() - self.last_failure_time > self.reset_timeout:
                        self.reset()
                    else:
                        raise CircuitBreakerError(
                            f"Circuit breaker is open. Too many failures (>{self.failure_threshold})"
                        )
                
                try:
                    result = func(*args, **kwargs)
                    self.success()
                    return result
                except Exception as e:
                    self.failure()
                    raise

        return wrapper

def retry(
    max_attempts: int = 3,
    retry_exceptions: tuple = (RetryableError,),
    delay: float = 1.0,
    backoff: float = 2.0,
    max_delay: float = 30.0
) -> Callable:
    """
    Decorator for retrying operations that may fail transiently.
    
    Args:
        max_attempts: Maximum number of retry attempts
        retry_exceptions: Tuple of exceptions that trigger a retry
        delay: Initial delay between retries in seconds
        backoff: Multiplier for the delay after each retry
        max_delay: Maximum delay between retries in seconds
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except retry_exceptions as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        sleep_time = min(current_delay, max_delay)
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}. "
                            f"Retrying in {sleep_time:.2f} seconds..."
                        )
                        time.sleep(sleep_time)
                        current_delay *= backoff
                    continue
                except Exception as e:
                    # Non-retryable exception
                    raise

            # If we get here, we've exhausted our retries
            raise SDKError(f"Operation failed after {max_attempts} attempts. Last error: {str(last_exception)}")

        return wrapper
    return decorator

def with_timeout(timeout: float) -> Callable:
    """
    Decorator to add timeout functionality to operations.
    
    Args:
        timeout: Timeout in seconds
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            import signal

            def handler(signum, frame):
                raise TimeoutError(f"Operation timed out after {timeout} seconds")

            # Set up the timeout
            original_handler = signal.signal(signal.SIGALRM, handler)
            signal.alarm(int(timeout))

            try:
                result = func(*args, **kwargs)
            finally:
                # Restore the original handler and cancel the alarm
                signal.alarm(0)
                signal.signal(signal.SIGALRM, original_handler)

            return result
        return wrapper
    return decorator

def log_exceptions(logger_name: Optional[str] = None) -> Callable:
    """
    Decorator to log exceptions before re-raising them.
    
    Args:
        logger_name: Name of the logger to use
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal logger_name
            if logger_name is None:
                logger_name = func.__module__

            log = logging.getLogger(logger_name)

            try:
                return func(*args, **kwargs)
            except Exception as e:
                log.exception(f"Exception in {func.__name__}: {str(e)}")
                raise

        return wrapper
    return decorator