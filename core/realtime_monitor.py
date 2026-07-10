"""
Real-time streaming wash trade detection.

Polls the chain head for a single pool, ingests new swap events as they land,
re-runs the full detector stack over a rolling time window, and yields alert
events for newly flagged trades. Consumed by the API's websocket endpoint but
usable standalone::

    monitor = RealtimeMonitor(storage, chain_id=1, pool_address="0x...")
    async for event in monitor.stream():
        ...

HTTP block polling (rather than an RPC websocket subscription) keeps the
monitor working against every provider the scanner already supports, and it
inherits the ingestor's rate limiting and circuit breaker.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import and_, select

from config.chains import get_chain_config
from config.settings import settings
from core.exceptions import CircuitBreakerOpenError
from core.heuristics import HeuristicDetector
from core.ingestor import ChainIngestor
from core.storage import Storage
from models.schemas import SwapTrade, SwapTradeResponse

logger = logging.getLogger(__name__)


@dataclass
class MonitorEvent:
    """Single event emitted by the monitor stream."""

    type: str  # "status" | "alert" | "stats" | "error"
    payload: dict[str, Any] = field(default_factory=dict)


def _trade_payload(trade: SwapTrade) -> dict[str, Any]:
    return SwapTradeResponse.model_validate(trade).model_dump(mode="json")


class RealtimeMonitor:
    """Rolling-window streaming detection for one pool."""

    def __init__(
        self,
        storage: Storage,
        chain_id: int,
        pool_address: str,
        poll_interval: float | None = None,
        window_minutes: int | None = None,
    ) -> None:
        self.storage = storage
        self.chain_id = chain_id
        self.pool_address = pool_address
        self.poll_interval = poll_interval or settings.MONITOR_POLL_INTERVAL_SECONDS
        self.window_minutes = window_minutes or settings.MONITOR_WINDOW_MINUTES
        self.detector = HeuristicDetector()
        self.ingestor: ChainIngestor | None = None
        self.last_block = 0
        self._seen_alert_ids: set[int] = set()
        self._stop_event = asyncio.Event()

    async def initialize(self) -> None:
        """Connect a single-chain ingestor and anchor at the current head."""
        chain_config = get_chain_config(self.chain_id)
        self.ingestor = ChainIngestor(chain_config, self.storage)
        await self.ingestor.connect()
        self.last_block = await self._latest_block()
        logger.info(
            f"Realtime monitor initialized for pool {self.pool_address} "
            f"on chain {self.chain_id} at block {self.last_block}"
        )

    def stop(self) -> None:
        """Signal the stream loop to finish after the current iteration."""
        self._stop_event.set()

    async def _latest_block(self) -> int:
        assert self.ingestor is not None, "call initialize() first"
        assert self.ingestor.web3 is not None
        await self.ingestor.rate_limiter.acquire()
        web3 = self.ingestor.web3
        block: int = await self.ingestor.circuit_breaker.call(lambda: web3.eth.block_number)
        return block

    async def poll_once(self) -> tuple[list[dict[str, Any]], dict[str, int]]:
        """Ingest new blocks and detect over the rolling window.

        Returns (new alert payloads, detector stats for this pass).
        """
        assert self.ingestor is not None, "call initialize() first"

        latest = await self._latest_block()
        if latest <= self.last_block:
            return [], {}

        chain_config = get_chain_config(self.chain_id)
        synced = 0
        for dex in chain_config["dexes"]:
            synced += await self.ingestor.sync_historical_swaps(
                dex,
                self.last_block + 1,
                latest,
                pool_address=self.pool_address,
            )
        self.last_block = latest

        if synced == 0:
            return [], {}

        # block_timestamp is stored timezone-naive in UTC.
        window_start = datetime.now(UTC).replace(tzinfo=None) - timedelta(
            minutes=self.window_minutes
        )
        async with await self.storage.get_session() as session:
            stmt = (
                select(SwapTrade)
                .where(
                    and_(
                        SwapTrade.chain_id == self.chain_id,
                        SwapTrade.pool_address == self.pool_address,
                        SwapTrade.block_timestamp >= window_start,
                    )
                )
                .order_by(SwapTrade.block_timestamp)
            )
            result = await session.execute(stmt)
            window_trades = list(result.scalars().all())

            wash_trades, stats = await self.detector.run_detectors_on_trades(window_trades, session)
            if wash_trades:
                # Persist labels set by the detectors on this session's ORM rows.
                await session.commit()

        alerts = []
        for trade in wash_trades:
            if trade.id in self._seen_alert_ids:
                continue
            self._seen_alert_ids.add(trade.id)
            alerts.append(_trade_payload(trade))

        return alerts, stats

    async def stream(self) -> AsyncIterator[MonitorEvent]:
        """Yield monitor events until stop() is called.

        Emits a `status` event on start, an `alert` per newly flagged trade,
        a `stats` summary after each pass that saw activity, and `error`
        events (without terminating) on transient RPC failures.
        """
        if self.ingestor is None:
            await self.initialize()

        yield MonitorEvent(
            type="status",
            payload={
                "state": "monitoring",
                "chain_id": self.chain_id,
                "pool_address": self.pool_address,
                "from_block": self.last_block,
                "poll_interval_seconds": self.poll_interval,
                "window_minutes": self.window_minutes,
            },
        )

        while not self._stop_event.is_set():
            try:
                alerts, stats = await self.poll_once()
                for alert in alerts:
                    yield MonitorEvent(type="alert", payload=alert)
                if stats:
                    yield MonitorEvent(
                        type="stats",
                        payload={
                            "block": self.last_block,
                            "detections_by_method": stats,
                            "new_alerts": len(alerts),
                        },
                    )
            except CircuitBreakerOpenError:
                yield MonitorEvent(
                    type="error",
                    payload={"reason": "rpc_circuit_breaker_open", "recoverable": True},
                )
            except Exception as exc:
                logger.exception(f"Realtime monitor poll failed: {exc}")
                yield MonitorEvent(
                    type="error",
                    payload={"reason": str(exc), "recoverable": True},
                )

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
            except TimeoutError:
                continue

        yield MonitorEvent(type="status", payload={"state": "stopped"})
