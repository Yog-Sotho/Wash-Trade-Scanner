"""
Circuit breaker pattern for RPC resilience.
"""

import asyncio
import logging
import time
from collections.abc import Callable
from enum import Enum, auto
from typing import Any

from core.exceptions import CircuitBreakerOpen

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 3,
        success_threshold: int = 2,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.success_threshold = success_threshold


class CircuitBreaker:
    """Circuit breaker for protecting RPC calls."""

    def __init__(self, name: str, config: CircuitBreakerConfig | None = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failures = 0
        self.successes = 0
        self.last_failure_time: float | None = None
        self._lock = asyncio.Lock()

    async def call(self, coro: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute coroutine with circuit breaker protection."""
        async with self._lock:
            if self.state == CircuitState.OPEN:
                if time.time() - (self.last_failure_time or 0) > self.config.recovery_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self.successes = 0
                    logger.info(f"Circuit {self.name} entering HALF_OPEN")
                else:
                    raise CircuitBreakerOpen(f"Circuit {self.name} is OPEN")

            if self.state == CircuitState.HALF_OPEN and self.successes >= self.config.half_open_max_calls:
                raise CircuitBreakerOpen(f"Circuit {self.name} half-open limit reached")

        try:
            result = await coro(*args, **kwargs)
            await self._on_success()
            return result
        except Exception:
            await self._on_failure()
            raise

    async def _on_success(self) -> None:
        async with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.successes += 1
                if self.successes >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failures = 0
                    self.successes = 0
                    logger.info(f"Circuit {self.name} CLOSED")
            else:
                self.failures = max(0, self.failures - 1)

    async def _on_failure(self) -> None:
        async with self._lock:
            self.failures += 1
            self.last_failure_time = time.time()

            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit {self.name} OPEN (half-open failure)")
            elif self.failures >= self.config.failure_threshold:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit {self.name} OPEN ({self.failures} failures)")

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    @property
    def is_closed(self) -> bool:
        return self.state == CircuitState.CLOSED
