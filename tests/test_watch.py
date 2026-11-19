"""
Background monitoring (roadmap 4.3): the API server watches MONITOR_POOLS and
sends each pass's new detections to the alert sinks.
"""

import asyncio
import io
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from api.server import create_app
from config.settings import settings
from core.alerts import AlertBatch
from core.log_config import configure_logging
from core.realtime_monitor import MonitorPass
from core.watch import PoolWatcher

POOL = "0x" + "9" * 40


def alert(i: int) -> dict:
    return {"id": i, "volume_usd": 100.0, "detection_method": "round_trip"}


class FakeMonitor:
    """Stands in for RealtimeMonitor: yields scripted passes, then waits for
    stop(). A script entry that is an exception is raised instead."""

    scripts: list[list] = []
    instances: list["FakeMonitor"] = []

    def __init__(self, storage, chain_id, pool):
        self.chain_id, self.pool = chain_id, pool
        self.script = FakeMonitor.scripts.pop(0) if FakeMonitor.scripts else []
        self._stop = asyncio.Event()
        FakeMonitor.instances.append(self)

    def stop(self):
        self._stop.set()

    async def stream_passes(self):
        for step in self.script:
            if isinstance(step, BaseException):
                raise step
            yield step
        await self._stop.wait()


@pytest.fixture
def fake_monitors(monkeypatch):
    FakeMonitor.scripts, FakeMonitor.instances = [], []
    monkeypatch.setattr("core.watch.RealtimeMonitor", FakeMonitor)
    return FakeMonitor


async def settle(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition never became true")
        await asyncio.sleep(0.01)


async def test_each_pass_with_new_detections_becomes_one_batch(fake_monitors):
    fake_monitors.scripts = [
        [
            MonitorPass(alerts=[alert(1), alert(2)], stats={"round_trip": 2}),
            MonitorPass(stats={"round_trip": 2}),  # nothing new: no alert
            MonitorPass(error={"reason": "rpc_circuit_breaker_open"}),  # errors are not alerts
            MonitorPass(alerts=[alert(3)], stats={"round_trip": 3}),
        ]
    ]
    dispatcher = MagicMock()
    watcher = PoolWatcher(AsyncMock(), [(1, POOL)], dispatcher)

    await watcher.start()
    await settle(lambda: dispatcher.publish.call_count == 2)
    await watcher.stop()

    first, second = (call.args[0] for call in dispatcher.publish.call_args_list)
    assert isinstance(first, AlertBatch)
    assert (first.chain_id, first.chain_name, first.pool_address) == (1, "Ethereum", POOL)
    assert [t["id"] for t in first.trades] == [1, 2]
    assert [t["id"] for t in second.trades] == [3]


async def test_every_batch_is_logged_with_its_trades(fake_monitors):
    fake_monitors.scripts = [[MonitorPass(alerts=[alert(7)], stats={"round_trip": 1})]]
    root = logging.getLogger()
    level = root.level
    buffer = io.StringIO()
    handler = configure_logging(level="INFO", fmt="json", stream=buffer)
    dispatcher = MagicMock()
    watcher = PoolWatcher(AsyncMock(), [(1, POOL)], dispatcher)
    try:
        await watcher.start()
        await settle(lambda: dispatcher.publish.called)
        await watcher.stop()
    finally:
        root.removeHandler(handler)
        root.setLevel(level)
    lines = [json.loads(line) for line in buffer.getvalue().splitlines()]
    (logged,) = [line for line in lines if line.get("alert_trade_ids")]
    assert logged["level"] == "WARNING"
    assert logged["alert_trade_ids"] == [7]
    assert logged["message"].startswith(f"1 wash trade flagged in Ethereum pool {POOL}")


async def test_a_monitor_that_fails_is_restarted_with_backoff(fake_monitors):
    fake_monitors.scripts = [
        [ConnectionError("node down at startup")],
        [ConnectionError("still down")],
        [MonitorPass(alerts=[alert(1)], stats={"round_trip": 1})],
    ]
    dispatcher = MagicMock()
    watcher = PoolWatcher(AsyncMock(), [(1, POOL)], dispatcher, initial_backoff=0.01)

    await watcher.start()
    await settle(lambda: dispatcher.publish.called)
    await watcher.stop()

    assert len(fake_monitors.instances) == 3


async def test_backoff_doubles_and_resets(fake_monitors, monkeypatch):
    waits: list[float] = []
    real_wait_for = asyncio.wait_for

    async def record(awaitable, timeout):
        waits.append(timeout)
        return await real_wait_for(awaitable, 0.001)

    monkeypatch.setattr("core.watch.asyncio.wait_for", record)
    fake_monitors.scripts = [
        [ConnectionError()],
        [ConnectionError()],
        [MonitorPass(stats={"round_trip": 0}), ConnectionError()],  # a good pass, then a failure
        [MonitorPass(alerts=[alert(1)])],
    ]
    dispatcher = MagicMock()
    watcher = PoolWatcher(AsyncMock(), [(1, POOL)], dispatcher, initial_backoff=1.0)
    await watcher.start()
    await settle(lambda: dispatcher.publish.called)
    await watcher.stop()
    assert waits[:3] == [1.0, 2.0, 1.0]


async def test_stop_ends_every_monitor(fake_monitors):
    dispatcher = MagicMock()
    pools = [(1, POOL), (8453, "0x" + "8" * 40)]
    watcher = PoolWatcher(AsyncMock(), pools, dispatcher)
    await watcher.start()
    await settle(lambda: len(fake_monitors.instances) == 2)

    await asyncio.wait_for(watcher.stop(), 5)

    assert all(m._stop.is_set() for m in fake_monitors.instances)
    assert watcher._tasks == []


@pytest.mark.usefixtures("fake_monitors")
async def test_a_monitor_that_ignores_stop_is_cancelled_after_the_grace():
    class Stubborn(FakeMonitor):
        def stop(self):  # never ends its stream
            pass

    with patch("core.watch.RealtimeMonitor", Stubborn):
        watcher = PoolWatcher(AsyncMock(), [(1, POOL)], MagicMock())
        await watcher.start()
        await settle(lambda: len(FakeMonitor.instances) == 1)
        await asyncio.wait_for(watcher.stop(grace=0.05), 5)
    assert watcher._tasks == []


# ------------------------------------------------------------------ the app


@pytest.fixture
def storage():
    mock = AsyncMock()
    mock.health_check.return_value = True
    return mock


def test_the_server_watches_configured_pools_for_its_lifetime(storage, monkeypatch):
    monkeypatch.setattr(settings, "MONITOR_POOLS", f"1:{POOL}")
    monkeypatch.setattr(settings, "HEALTH_RPC_CHAIN_IDS", "")
    started, stopped = AsyncMock(), AsyncMock()
    monkeypatch.setattr(PoolWatcher, "start", started)
    monkeypatch.setattr(PoolWatcher, "stop", stopped)
    app = create_app(storage=storage)
    with TestClient(app):
        started.assert_awaited_once()
        assert app.state.watcher.pools == [(1, POOL)]
        stopped.assert_not_awaited()
    stopped.assert_awaited_once()


def test_an_unknown_monitored_chain_fails_at_startup(storage, monkeypatch):
    monkeypatch.setattr(settings, "MONITOR_POOLS", f"999999:{POOL}")
    with pytest.raises(ValueError, match="999999"):
        create_app(storage=storage)


def test_a_half_configured_sink_fails_at_startup(storage, monkeypatch):
    monkeypatch.setattr(settings, "ALERT_EMAIL_TO", "ops@example.org")
    with pytest.raises(ValueError, match="ALERT_SMTP_HOST"):
        create_app(storage=storage)
