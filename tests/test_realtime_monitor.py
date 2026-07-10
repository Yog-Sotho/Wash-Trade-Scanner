"""
Unit tests for the real-time streaming monitor.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.realtime_monitor import MonitorEvent, RealtimeMonitor
from models.schemas import SwapTrade

POOL = "0x" + "9" * 40


def _trade(trade_id: int, is_wash: bool = False) -> SwapTrade:
    return SwapTrade(
        id=trade_id,
        chain_id=1,
        dex_name="uniswap_v2",
        pool_address=POOL,
        token_in="0x" + "1" * 40,
        token_out="0x" + "2" * 40,
        amount_in=100.0,
        amount_out=200.0,
        sender="0x" + "a" * 40,
        recipient="0x" + "b" * 40,
        transaction_hash="0x" + "f" * 64,
        block_number=100,
        block_timestamp=datetime.now(UTC).replace(tzinfo=None),
        log_index=trade_id,
        volume_usd=1000.0,
        is_wash_trade=is_wash,
        wash_trade_score=0.9 if is_wash else 0.0,
        detection_method="self_trading" if is_wash else None,
    )


def _monitor_with_mocks(window_trades=None):
    storage = AsyncMock()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    storage.get_session.return_value = session_cm

    result = MagicMock()
    result.scalars.return_value.all.return_value = window_trades or []
    session.execute = AsyncMock(return_value=result)

    monitor = RealtimeMonitor(storage, chain_id=1, pool_address=POOL, poll_interval=0.01)
    monitor.ingestor = AsyncMock()
    monitor.ingestor.sync_historical_swaps = AsyncMock(return_value=len(window_trades or []))
    return monitor, session


@pytest.mark.asyncio
async def test_poll_once_no_new_blocks():
    monitor, _ = _monitor_with_mocks()
    monitor.last_block = 100
    with patch.object(monitor, "_latest_block", AsyncMock(return_value=100)):
        alerts, stats = await monitor.poll_once()
    assert alerts == []
    assert stats == {}


@pytest.mark.asyncio
async def test_poll_once_no_new_swaps_advances_head():
    monitor, _ = _monitor_with_mocks()
    monitor.last_block = 100
    monitor.ingestor.sync_historical_swaps = AsyncMock(return_value=0)
    with (
        patch("core.realtime_monitor.get_chain_config", return_value={"dexes": [{"n": 1}]}),
        patch.object(monitor, "_latest_block", AsyncMock(return_value=110)),
    ):
        alerts, stats = await monitor.poll_once()
    assert alerts == []
    assert monitor.last_block == 110


@pytest.mark.asyncio
async def test_poll_once_emits_alerts_for_flagged_trades():
    # One self-trade in the window: sender == recipient triggers detection.
    wash = _trade(1)
    wash.recipient = wash.sender
    clean = _trade(2)
    monitor, session = _monitor_with_mocks([wash, clean])
    monitor.last_block = 100

    with (
        patch("core.realtime_monitor.get_chain_config", return_value={"dexes": [{"n": 1}]}),
        patch.object(monitor, "_latest_block", AsyncMock(return_value=105)),
    ):
        alerts, stats = await monitor.poll_once()

    assert len(alerts) == 1
    assert alerts[0]["id"] == 1
    assert alerts[0]["detection_method"] == "self_trading"
    assert stats["self_trading"] == 1
    session.commit.assert_awaited()  # labels persisted


@pytest.mark.asyncio
async def test_poll_once_deduplicates_alerts_across_polls():
    wash = _trade(1)
    wash.recipient = wash.sender
    monitor, _ = _monitor_with_mocks([wash])
    monitor.last_block = 100

    with (
        patch("core.realtime_monitor.get_chain_config", return_value={"dexes": [{"n": 1}]}),
        patch.object(monitor, "_latest_block", AsyncMock(side_effect=[105, 110])),
    ):
        first, _ = await monitor.poll_once()
        second, _ = await monitor.poll_once()

    assert len(first) == 1
    assert second == []  # same trade must not re-alert


@pytest.mark.asyncio
async def test_stream_yields_status_alerts_and_stops():
    wash = _trade(1)
    wash.recipient = wash.sender
    monitor, _ = _monitor_with_mocks([wash])
    monitor.last_block = 100

    events: list[MonitorEvent] = []
    with (
        patch("core.realtime_monitor.get_chain_config", return_value={"dexes": [{"n": 1}]}),
        patch.object(monitor, "_latest_block", AsyncMock(return_value=105)),
    ):
        async for event in monitor.stream():
            events.append(event)
            if event.type == "stats":
                monitor.stop()

    types = [e.type for e in events]
    assert types[0] == "status"
    assert "alert" in types
    assert "stats" in types
    assert types[-1] == "status"
    assert events[-1].payload["state"] == "stopped"


@pytest.mark.asyncio
async def test_stream_survives_poll_errors():
    monitor, _ = _monitor_with_mocks()
    monitor.last_block = 100

    events = []
    with patch.object(monitor, "poll_once", AsyncMock(side_effect=RuntimeError("rpc down"))):
        async for event in monitor.stream():
            events.append(event)
            if event.type == "error":
                monitor.stop()

    error_events = [e for e in events if e.type == "error"]
    assert error_events and error_events[0].payload["recoverable"] is True


@pytest.mark.asyncio
async def test_initialize_connects_single_chain():
    storage = AsyncMock()
    monitor = RealtimeMonitor(storage, chain_id=1, pool_address=POOL)

    fake_ingestor = AsyncMock()
    with (
        patch("core.realtime_monitor.get_chain_config", return_value={"chain_id": 1}),
        patch("core.realtime_monitor.ChainIngestor", return_value=fake_ingestor),
        patch.object(RealtimeMonitor, "_latest_block", AsyncMock(return_value=123)),
    ):
        await monitor.initialize()

    fake_ingestor.connect.assert_awaited_once()
    assert monitor.last_block == 123
