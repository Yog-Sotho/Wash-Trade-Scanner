"""
Liveness and readiness (GET /health/live, GET /health/ready).

Liveness says only that the process serves requests: a failed dependency must
not get a working process restarted. Readiness says whether it can do its job
now: the database answers, and the RPC endpoint of every chain in
HEALTH_RPC_CHAIN_IDS answers eth_chainId with that chain's id. An endpoint
serving another network is not ready: every audit through it would be wrong.

Each check is bounded by HEALTH_CHECK_TIMEOUT_SECONDS. RPC answers are reused
for HEALTH_RPC_CACHE_SECONDS, and concurrent probes of a chain share one
request, so probes from several orchestrators cost one call per period. RPC
probes go through the chain's shared circuit breaker: an endpoint the audits
found down is reported without being called again.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from web3 import AsyncWeb3

from config.chains import get_chain_config
from config.settings import settings
from core.exceptions import CircuitBreakerOpenError
from core.ingestor import chain_breaker
from core.rpc import InstrumentedHTTPProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CheckResult:
    ok: bool
    latency_ms: float
    # Why it failed, as one of a few fixed words (never an error message,
    # which could carry an RPC URL to an unauthenticated caller).
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"ok": self.ok, "latency_ms": round(self.latency_ms, 1)}
        if self.reason:
            body["reason"] = self.reason
        return body


def _is_placeholder(url: str) -> bool:
    return not url or "YOUR_KEY" in url or "placeholder" in url.lower()


class ReadinessChecker:
    def __init__(
        self,
        database_check: Callable[[], Awaitable[bool]],
        extra_checks: dict[str, Callable[[], Awaitable[bool]]] | None = None,
    ) -> None:
        self._database_check = database_check
        # Further dependencies, by check name (the shared rate-limit store).
        self._extra_checks = extra_checks or {}
        self._cache: dict[int, tuple[float, CheckResult]] = {}  # chain -> (when, result)
        self._locks: dict[int, asyncio.Lock] = {}
        self._clients: dict[int, tuple[str, AsyncWeb3]] = {}

    async def check(self) -> tuple[bool, dict[str, Any]]:
        """(ready, {check name: result}) with every check run concurrently."""
        chain_ids = settings.health_rpc_chain_id_list
        extra = list(self._extra_checks.items())
        results = await asyncio.gather(
            self._dependency("database", self._database_check),
            *(self._dependency(name, check) for name, check in extra),
            *(self._rpc(c) for c in chain_ids),
        )
        checks = {"database": results[0]}
        checks.update({name: r for (name, _), r in zip(extra, results[1:], strict=False)})
        rpc_results = results[1 + len(extra) :]
        checks.update({f"rpc_{c}": r for c, r in zip(chain_ids, rpc_results, strict=True)})
        ready = all(result.ok for result in checks.values())
        return ready, {name: result.as_dict() for name, result in checks.items()}

    async def _dependency(self, name: str, check: Callable[[], Awaitable[bool]]) -> CheckResult:
        start = time.perf_counter()
        try:
            ok = await asyncio.wait_for(check(), settings.HEALTH_CHECK_TIMEOUT_SECONDS)
        except TimeoutError:
            return CheckResult(False, _ms(start), "timeout")
        except Exception:
            logger.warning(f"Readiness: {name} check failed", exc_info=True)
            ok = False
        return CheckResult(ok, _ms(start), None if ok else "unreachable")

    async def _rpc(self, chain_id: int) -> CheckResult:
        lock = self._locks.setdefault(chain_id, asyncio.Lock())
        async with lock:  # concurrent probes share one request
            cached = self._cache.get(chain_id)
            if cached and time.monotonic() - cached[0] < settings.HEALTH_RPC_CACHE_SECONDS:
                return cached[1]
            result = await self._probe_rpc(chain_id)
            self._cache[chain_id] = (time.monotonic(), result)
            return result

    async def _probe_rpc(self, chain_id: int) -> CheckResult:
        start = time.perf_counter()
        url = get_chain_config(chain_id)["rpc_url"]
        if _is_placeholder(url):
            return CheckResult(False, 0.0, "not_configured")
        client = self._clients.get(chain_id)
        if client is None or client[0] != url:
            client = (url, AsyncWeb3(InstrumentedHTTPProvider(url, chain_id)))
            self._clients[chain_id] = client
        web3 = client[1]
        try:
            answered = await asyncio.wait_for(
                chain_breaker(chain_id).call(lambda: web3.eth.chain_id),
                settings.HEALTH_CHECK_TIMEOUT_SECONDS,
            )
        except CircuitBreakerOpenError:
            return CheckResult(False, _ms(start), "circuit_open")
        except TimeoutError:
            return CheckResult(False, _ms(start), "timeout")
        except Exception as exc:
            # The type only: an HTTP error's message holds the endpoint URL.
            logger.warning(
                f"Readiness: RPC check for chain {chain_id} failed: {type(exc).__name__}"
            )
            return CheckResult(False, _ms(start), "unreachable")
        if answered != chain_id:
            logger.error(
                f"Readiness: the RPC endpoint configured for chain {chain_id} "
                f"serves chain {answered}"
            )
            return CheckResult(False, _ms(start), "wrong_chain")
        return CheckResult(True, _ms(start))


def _ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000
