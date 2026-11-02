"""
Unit tests for the real-time streaming monitor.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core.calibration import DetectorCalibration, override_calibration
from core.realtime_monitor import MonitorEvent, RealtimeMonitor
from models.schemas import SwapTrade

POOL = "0x" + "9" * 40
TRADER = "0x" + "c" * 40


@pytest.fixture(autouse=True)
def calibration():
    """Round trips are decisive, everything else is not: keeps these tests
    about the monitor rather than about the benchmark's numbers."""
    names = (
        "circular_trading",
        "position_neutral",
        "closed_cluster",
        "repeated_amounts",
        "high_frequency_bot",
        "volume_anomaly",
        "wash_cluster",
    )
    table = {name: DetectorCalibration(tpr=0.0, fpr_upper=1.0) for name in names}
    table["round_trip"] = DetectorCalibration(tpr=0.95, fpr_upper=0.0001)
    override_calibration(table)
    yield
    override_calibration(None)


def _round_trips() -> list[SwapTrade]:
    """Three buy-and-sell-back pairs by one trader, legs two blocks apart."""
    trades = []
    for i in range(6):
        trade = _trade(10 + i)
        trade.tx_from = TRADER
        trade.block_number = 100 + 2 * i
        if i % 2:
            trade.token_in, trade.token_out = trade.token_out, trade.token_in
            trade.amount_in, trade.amount_out = 200.0, 99.4
            trade.volume_usd = 997.0
        trades.append(trade)
    return trades


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
        detection_method="round_trip" if is_wash else None,
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
    monitor.ingestor.sync_pool = AsyncMock(return_value=len(window_trades or []))
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
    monitor.ingestor.sync_pool = AsyncMock(return_value=0)
    with (
        patch("core.realtime_monitor.get_chain_config", return_value={"dexes": [{"n": 1}]}),
        patch.object(monitor, "_latest_block", AsyncMock(return_value=110)),
    ):
        alerts, stats = await monitor.poll_once()
    assert alerts == []
    assert monitor.last_block == 110


@pytest.mark.asyncio
async def test_poll_once_emits_alerts_for_flagged_trades():
    # Three round trips by one trader in the window, plus an unrelated swap.
    wash = _round_trips()
    clean = _trade(2)
    monitor, session = _monitor_with_mocks([*wash, clean])
    monitor.last_block = 100

    with (
        patch("core.realtime_monitor.get_chain_config", return_value={"dexes": [{"n": 1}]}),
        patch.object(monitor, "_latest_block", AsyncMock(return_value=105)),
    ):
        alerts, stats = await monitor.poll_once()

    assert sorted(a["id"] for a in alerts) == [t.id for t in wash]
    assert {a["detection_method"] for a in alerts} == {"round_trip"}
    assert stats["round_trip"] == 6
    session.commit.assert_awaited()  # labels persisted


@pytest.mark.asyncio
async def test_poll_once_deduplicates_alerts_across_polls():
    monitor, _ = _monitor_with_mocks(_round_trips())
    monitor.last_block = 100

    with (
        patch("core.realtime_monitor.get_chain_config", return_value={"dexes": [{"n": 1}]}),
        patch.object(monitor, "_latest_block", AsyncMock(side_effect=[105, 110])),
    ):
        first, _ = await monitor.poll_once()
        second, _ = await monitor.poll_once()

    assert len(first) == 6
    assert second == []  # same trades must not re-alert


@pytest.mark.asyncio
async def test_stream_yields_status_alerts_and_stops():
    monitor, _ = _monitor_with_mocks(_round_trips())
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


def test_monitor_normalizes_pool_to_stored_form():
    monitor = RealtimeMonitor(
        AsyncMock(), chain_id=1, pool_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
    )
    assert monitor.pool_address == "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"


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


@pytest.mark.asyncio
async def test_poll_once_never_removes_labels_an_audit_gave():
    """Regression: detection resets labels in memory, and the commit would have
    persisted that for every trade in the window, erasing a full audit's labels
    (e.g. a 24h position-neutral finding) that a 60-minute window cannot see."""
    audited = _trade(2, is_wash=True)
    audited.detection_method = "position_neutral"
    audited.wash_trade_score = 0.85
    wash = _round_trips()
    monitor, session = _monitor_with_mocks([*wash, audited])
    monitor.last_block = 100

    with patch.object(monitor, "_latest_block", AsyncMock(return_value=105)):
        alerts, _ = await monitor.poll_once()

    assert len(alerts) == 6
    session.commit.assert_awaited()
    assert audited.is_wash_trade is True
    assert audited.detection_method == "position_neutral"
    assert audited.wash_trade_score == 0.85


@pytest.mark.asyncio
async def test_poll_once_keeps_a_stronger_existing_label():
    wash = _round_trips()
    wash[0].is_wash_trade = True
    wash[0].wash_trade_score = 0.99
    wash[0].detection_method = "position_neutral"
    monitor, _ = _monitor_with_mocks(wash)
    monitor.last_block = 100

    with patch.object(monitor, "_latest_block", AsyncMock(return_value=105)):
        await monitor.poll_once()

    assert wash[0].detection_method == "position_neutral"
    assert wash[0].wash_trade_score == 0.99
    assert wash[1].detection_method == "round_trip"
