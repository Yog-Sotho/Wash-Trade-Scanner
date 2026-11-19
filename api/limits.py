"""
Rate limits and authentication lockouts, counted per client IP in a store
shared by every API instance (Redis, RATE_LIMIT_REDIS_URL) or in this process
(the default, for a single instance).

Two mechanisms use it:

- Rate limiting: a sliding window of API_RATE_LIMIT_PER_MINUTE requests.
- Authentication lockout: every wrong API key counts as a failure. From the
  AUTH_MAX_FAILURES-th failure on, the client is locked out for
  AUTH_LOCKOUT_SECONDS, doubling with each further failure up to
  AUTH_LOCKOUT_MAX_SECONDS. Failures are forgotten AUTH_FAILURE_WINDOW_SECONDS
  after the last one (or the last lockout's end), and a success forgets them.

The Redis operations are Lua scripts, so each is atomic across instances,
and they read the time from the Redis server, so instances with skewed
clocks agree. If the store cannot be reached, requests are let through
(fail open: the API stays available; every request still needs a valid key)
and the error is counted in wash_limit_store_errors_total.
"""

from __future__ import annotations

import logging
import math
import secrets
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Protocol

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from config.settings import settings
from core import metrics

logger = logging.getLogger(__name__)

_MAX_TRACKED_CLIENTS = 10_000  # memory store: prune idle entries beyond this
_ERROR_LOG_INTERVAL = 60.0  # seconds between logged store errors


def client_ip(connection: Any) -> str:
    """The address a limit counts against, from a Request or a WebSocket: the
    peer, or the X-Forwarded-For client when uvicorn trusts the proxy
    (FORWARDED_ALLOW_IPS, loopback by default)."""
    client = connection.client
    return client.host if client else "unknown"


@dataclass(frozen=True)
class AuthState:
    locked_for: float  # seconds of lockout left (0 when not locked)
    failures: int  # failures still remembered


class LimitStore(Protocol):
    async def hit(self, key: str, limit: int, window: float) -> float:
        """Record a request unless `limit` were made in the last `window`
        seconds. Returns 0 when allowed, else the seconds until one is."""

    async def auth_state(self, key: str) -> AuthState: ...

    async def auth_failure(self, key: str) -> float:
        """Count a failure; returns the lockout it starts (0 if none)."""

    async def auth_reset(self, key: str) -> None: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...


def lockout_seconds(failures: int) -> float:
    """Lockout after the `failures`-th failure: none below the threshold, then
    AUTH_LOCKOUT_SECONDS doubling per failure, capped."""
    excess = failures - settings.AUTH_MAX_FAILURES
    if excess < 0:
        return 0.0
    return float(
        min(settings.AUTH_LOCKOUT_SECONDS * 2 ** min(excess, 32), settings.AUTH_LOCKOUT_MAX_SECONDS)
    )


class MemoryLimitStore:
    """In-process store: exact, but per instance. All operations are
    synchronous, hence atomic on the event loop."""

    def __init__(self) -> None:
        self._hits: dict[str, deque[float]] = {}
        self._failures: dict[str, tuple[int, float]] = {}  # key -> (count, forget_at)
        self._locks: dict[str, float] = {}  # key -> locked_until

    async def hit(self, key: str, limit: int, window: float) -> float:
        now = time.monotonic()
        hits = self._hits.setdefault(key, deque())
        while hits and hits[0] <= now - window:
            hits.popleft()
        if len(hits) >= limit:
            return hits[0] + window - now
        hits.append(now)
        if len(self._hits) > _MAX_TRACKED_CLIENTS:
            self._hits = {k: v for k, v in self._hits.items() if v and v[-1] > now - window}
        return 0.0

    async def auth_state(self, key: str) -> AuthState:
        now = time.monotonic()
        locked = max(0.0, self._locks.get(key, 0.0) - now)
        count, forget_at = self._failures.get(key, (0, 0.0))
        return AuthState(locked, count if forget_at > now else 0)

    async def auth_failure(self, key: str) -> float:
        now = time.monotonic()
        count, forget_at = self._failures.get(key, (0, 0.0))
        count = count + 1 if forget_at > now else 1
        lock = lockout_seconds(count)
        if lock:
            self._locks[key] = now + lock
        self._failures[key] = (count, now + lock + settings.AUTH_FAILURE_WINDOW_SECONDS)
        if len(self._failures) > _MAX_TRACKED_CLIENTS:
            self._failures = {k: v for k, v in self._failures.items() if v[1] > now}
            self._locks = {k: v for k, v in self._locks.items() if v > now}
        return lock

    async def auth_reset(self, key: str) -> None:
        self._failures.pop(key, None)
        self._locks.pop(key, None)

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        return None


# KEYS[1] = sorted set of this client's request times.
# ARGV = limit, window (s), a unique member. Returns seconds to wait (0 = allowed).
_HIT_SCRIPT = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local limit, window = tonumber(ARGV[1]), tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - window)
if redis.call('ZCARD', KEYS[1]) >= limit then
  local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
  return tostring(tonumber(oldest[2]) + window - now)
end
redis.call('ZADD', KEYS[1], now, ARGV[3])
redis.call('PEXPIRE', KEYS[1], math.ceil(window * 1000))
return '0'
"""

# KEYS[1] = failure counter, KEYS[2] = lock flag.
# ARGV = max failures, base lockout, max lockout, window (all seconds).
# Returns the lockout started, in seconds (0 = none).
_FAILURE_SCRIPT = """
local n = redis.call('INCR', KEYS[1])
local maxf, base, cap, window = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local lock = 0
if n >= maxf then
  lock = math.min(base * 2 ^ math.min(n - maxf, 32), cap)
  redis.call('SET', KEYS[2], '1', 'EX', math.ceil(lock))
end
redis.call('EXPIRE', KEYS[1], math.ceil(lock + window))
return math.ceil(lock)
"""


class RedisLimitStore:
    """Redis store, shared by every instance pointing at the same Redis."""

    PREFIX = "wash:"

    def __init__(self, url: str, timeout: float = 1.0) -> None:
        self.client: aioredis.Redis = aioredis.from_url(
            url, socket_timeout=timeout, socket_connect_timeout=timeout
        )
        self._hit = self.client.register_script(_HIT_SCRIPT)
        self._failure = self.client.register_script(_FAILURE_SCRIPT)

    def _keys(self, key: str) -> tuple[str, str]:
        return f"{self.PREFIX}authfail:{key}", f"{self.PREFIX}authlock:{key}"

    async def hit(self, key: str, limit: int, window: float) -> float:
        member = f"{time.time_ns()}-{secrets.token_hex(4)}"
        wait = await self._hit(keys=[f"{self.PREFIX}rl:{key}"], args=[limit, window, member])
        return max(0.0, float(wait))

    async def auth_state(self, key: str) -> AuthState:
        failures_key, lock_key = self._keys(key)
        async with self.client.pipeline(transaction=False) as pipe:
            pipe.get(failures_key)
            pipe.pttl(lock_key)
            failures, lock_ms = await pipe.execute()
        return AuthState(max(0, lock_ms) / 1000, int(failures or 0))

    async def auth_failure(self, key: str) -> float:
        lock = await self._failure(
            keys=list(self._keys(key)),
            args=[
                settings.AUTH_MAX_FAILURES,
                settings.AUTH_LOCKOUT_SECONDS,
                settings.AUTH_LOCKOUT_MAX_SECONDS,
                settings.AUTH_FAILURE_WINDOW_SECONDS,
            ],
        )
        return float(lock)

    async def auth_reset(self, key: str) -> None:
        await self.client.delete(*self._keys(key))

    async def ping(self) -> bool:
        return bool(await self.client.ping())

    async def close(self) -> None:
        await self.client.aclose()


class Limits:
    """The API's view of the store: fails open, with metrics and logs."""

    def __init__(self, store: LimitStore) -> None:
        self.store = store
        self._last_error_log = -math.inf

    @classmethod
    def from_settings(cls) -> Limits:
        if settings.RATE_LIMIT_REDIS_URL:
            return cls(RedisLimitStore(settings.RATE_LIMIT_REDIS_URL))
        return cls(MemoryLimitStore())

    @property
    def shared(self) -> bool:
        return isinstance(self.store, RedisLimitStore)

    def _store_failed(self, operation: str, exc: BaseException) -> None:
        metrics.LIMIT_STORE_ERRORS.labels(operation).inc()
        now = time.monotonic()
        if now - self._last_error_log >= _ERROR_LOG_INTERVAL:
            self._last_error_log = now
            # The type only: a connection error's text can include the URL.
            logger.error(
                f"Rate-limit store unavailable ({type(exc).__name__}); "
                "requests are let through unlimited until it recovers"
            )

    async def _safely(self, operation: str, call: Any, fallback: Any) -> Any:
        try:
            return await call
        except (RedisError, OSError) as exc:
            self._store_failed(operation, exc)
            return fallback

    async def rate_limit(self, client: str) -> float:
        """0 if `client` may make a request now, else seconds to wait."""
        wait: float = await self._safely(
            "hit", self.store.hit(client, settings.API_RATE_LIMIT_PER_MINUTE, 60.0), 0.0
        )
        if wait:
            metrics.RATE_LIMITED.inc()
        return wait

    async def auth_state(self, client: str) -> AuthState:
        state: AuthState = await self._safely(
            "auth_state", self.store.auth_state(client), AuthState(0.0, 0)
        )
        return state

    async def auth_failed(self, client: str) -> None:
        metrics.AUTH_FAILURES.inc()
        lock: float = await self._safely("auth_failure", self.store.auth_failure(client), 0.0)
        if lock:
            metrics.AUTH_LOCKOUTS.inc()
            logger.warning(
                f"Client {client} locked out for {lock:.0f}s after repeated wrong API keys",
                extra={"client_ip": client, "lockout_seconds": lock},
            )

    async def auth_succeeded(self, client: str, state: AuthState) -> None:
        if state.failures:
            await self._safely("auth_reset", self.store.auth_reset(client), None)

    async def ping(self) -> bool:
        ok: bool = await self._safely("ping", self.store.ping(), False)
        return ok

    async def close(self) -> None:
        await self.store.close()
