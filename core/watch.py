"""
Background monitoring: the API server watches every MONITOR_POOLS pool
continuously, whether or not anyone has the panel open, and sends what each
monitor pass newly flags to the alert sinks (core/alerts.py) as one batch.

Each pool's RealtimeMonitor runs under a supervisor: if it fails to start or
stops unexpectedly (an RPC endpoint down at startup, say), it is restarted
after a backoff that doubles up to five minutes. A monitor starts at the
chain head, so blocks mined while the server was down are not watched (an
audit covers them).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from config.chains import get_chain_config
from core.alerts import AlertBatch, AlertDispatcher
from core.realtime_monitor import RealtimeMonitor
from core.storage import Storage

logger = logging.getLogger(__name__)

INITIAL_BACKOFF_SECONDS = 5.0
MAX_BACKOFF_SECONDS = 300.0


class PoolWatcher:
    """Runs one supervised RealtimeMonitor per pool and publishes its alerts."""

    def __init__(
        self,
        storage: Storage,
        pools: list[tuple[int, str]],
        dispatcher: AlertDispatcher,
        initial_backoff: float = INITIAL_BACKOFF_SECONDS,
    ) -> None:
        self.storage = storage
        self.pools = pools
        self.dispatcher = dispatcher
        self.initial_backoff = initial_backoff
        self._monitors: dict[tuple[int, str], RealtimeMonitor] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._stopping = asyncio.Event()

    async def start(self) -> None:
        for chain_id, pool in self.pools:
            self._tasks.append(
                asyncio.create_task(
                    self._supervise(chain_id, pool), name=f"watch-{chain_id}-{pool}"
                )
            )
        if self.pools:
            logger.info(f"Watching {len(self.pools)} pool(s) for wash trades")

    async def stop(self, grace: float = 10.0) -> None:
        """Stop every monitor after its current pass (cancelled after `grace`)."""
        self._stopping.set()
        for monitor in self._monitors.values():
            monitor.stop()
        if self._tasks:
            _done, pending = await asyncio.wait(self._tasks, timeout=grace)
            for task in pending:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks = []

    async def _supervise(self, chain_id: int, pool: str) -> None:
        backoff = self.initial_backoff
        chain_name = get_chain_config(chain_id)["name"]
        while not self._stopping.is_set():
            monitor = RealtimeMonitor(self.storage, chain_id, pool)
            self._monitors[(chain_id, pool)] = monitor
            try:
                async for result in monitor.stream_passes():
                    if result.error is None:
                        backoff = self.initial_backoff  # a working pass resets it
                    if result.alerts:
                        self._publish(chain_id, chain_name, pool, result.alerts)
                if self._stopping.is_set():
                    return
                logger.warning(f"Monitor for {chain_name} pool {pool} stopped; restarting")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    f"Monitor for {chain_name} pool {pool} failed; restarting in {backoff:.0f}s"
                )
            try:
                await asyncio.wait_for(self._stopping.wait(), backoff)
                return
            except TimeoutError:
                backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

    def _publish(
        self, chain_id: int, chain_name: str, pool: str, alerts: list[dict[str, Any]]
    ) -> None:
        batch = AlertBatch(
            chain_id=chain_id,
            chain_name=chain_name,
            pool_address=pool,
            trades=tuple(alerts),
            detected_at=datetime.now(UTC),
        )
        # Logged as well as sent: the log is the alert of last resort, and the
        # record of what was sent.
        logger.warning(
            f"{batch.title()}: ${batch.wash_volume_usd:,.2f}",
            extra={
                "alert_chain_id": chain_id,
                "alert_pool": pool,
                "alert_trade_ids": [t.get("id") for t in alerts],
            },
        )
        self.dispatcher.publish(batch)
