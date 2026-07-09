"""
Unit tests for heuristic detectors using configurable thresholds.
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from core.heuristics import HeuristicDetector, RobustAnomalyDetector
from models.schemas import AddressCluster, SwapTrade


@pytest.fixture
def detector():
    return HeuristicDetector()


@pytest.fixture
def sample_trades():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    return [
        SwapTrade(
            id=1,
            chain_id=1,
            pool_address="0xpool",
            sender="0xAlice",
            recipient="0xBob",
            volume_usd=1000.0,
            block_timestamp=base_time,
            is_wash_trade=False,
        ),
        SwapTrade(
            id=2,
            chain_id=1,
            pool_address="0xpool",
            sender="0xCarol",
            recipient="0xCarol",
            volume_usd=500.0,
            block_timestamp=base_time + timedelta(minutes=1),
            is_wash_trade=False,
        ),
        SwapTrade(
            id=3,
            chain_id=1,
            pool_address="0xpool",
            sender="0xDave",
            recipient="0xEve",
            volume_usd=200.0,
            block_timestamp=base_time + timedelta(minutes=2),
            is_wash_trade=False,
        ),
        SwapTrade(
            id=4,
            chain_id=1,
            pool_address="0xpool",
            sender="0xEve",
            recipient="0xDave",
            volume_usd=200.0,
            block_timestamp=base_time + timedelta(minutes=3),
            is_wash_trade=False,
        ),
    ]


@pytest.mark.asyncio
async def test_self_trading(detector, sample_trades):
    wash = await detector.detect_self_trading(sample_trades, AsyncMock())
    assert len(wash) == 1
    assert wash[0].id == 2


@pytest.mark.asyncio
async def test_circular_trading(detector, sample_trades):
    wash = await detector.detect_circular_trading(sample_trades, AsyncMock())
    assert len(wash) >= 2


@pytest.mark.asyncio
async def test_high_frequency_bot_detected(detector):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    trades = [
        SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender="0xBot",
            recipient="0xVictim",
            volume_usd=100.0,
            block_timestamp=base_time + timedelta(seconds=i),
            is_wash_trade=False,
        )
        for i in range(12)
    ]
    wash = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(wash) == 12
    assert all(t.detection_method == "high_frequency_bot" for t in wash)


@pytest.mark.asyncio
async def test_high_frequency_bot_respects_allowlist(detector, monkeypatch):
    monkeypatch.setattr(detector, "bot_allowlist", {"0xbot"})
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    trades = [
        SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender="0xBot",
            recipient="0xVictim",
            volume_usd=100.0,
            block_timestamp=base_time + timedelta(seconds=i),
            is_wash_trade=False,
        )
        for i in range(12)
    ]
    wash = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert wash == []


@pytest.mark.asyncio
async def test_volume_anomaly_detects_outlier(detector):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    volumes = [100.0, 110.0, 95.0, 105.0, 90.0, 50_000.0]
    trades = [
        SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xSender{i}",
            recipient=f"0xRecipient{i}",
            volume_usd=v,
            block_timestamp=base_time + timedelta(minutes=i),
            is_wash_trade=False,
        )
        for i, v in enumerate(volumes)
    ]
    wash = await detector.detect_volume_anomaly(trades, AsyncMock())
    assert len(wash) >= 1
    assert wash[0].volume_usd == 50_000.0


@pytest.mark.asyncio
async def test_wash_clusters(detector, sample_trades):
    cluster = AddressCluster(
        cluster_id="1:0xpool:0",
        addresses=["0xdave", "0xeve"],
        confidence_score=0.8,
    )
    wash = await detector.detect_wash_clusters(sample_trades, [cluster], AsyncMock())
    ids = {t.id for t in wash}
    assert 3 in ids
    assert 4 in ids


class TestRobustAnomalyDetector:
    def test_mad_fit_and_score(self):
        d = RobustAnomalyDetector(method="mad")
        d.fit([100.0, 110.0, 95.0, 105.0, 90.0])
        assert d.score(50_000.0) > d.score(100.0)
        assert d.is_anomaly(50_000.0, threshold=3.5)
        assert not d.is_anomaly(100.0, threshold=3.5)

    def test_iqr_fit_and_score(self):
        d = RobustAnomalyDetector(method="iqr")
        d.fit([float(v) for v in range(1, 21)])
        assert d.score(1000.0) > 0
        assert d.score(10.0) == 0.0

    def test_fit_empty_raises(self):
        d = RobustAnomalyDetector()
        with pytest.raises(ValueError):
            d.fit([])

    def test_score_before_fit_raises(self):
        d = RobustAnomalyDetector()
        with pytest.raises(RuntimeError):
            d.score(1.0)

    def test_unknown_method_raises(self):
        d = RobustAnomalyDetector(method="bogus")
        with pytest.raises(ValueError):
            d.fit([1.0, 2.0])
