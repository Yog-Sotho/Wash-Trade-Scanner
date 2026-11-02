"""
Unit tests for heuristic detectors using configurable thresholds.
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from core.calibration import DetectorCalibration, override_calibration
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


@pytest.mark.asyncio
async def test_wash_clusters_key_on_the_transaction_sender(detector):
    """A router-mediated swap whose output a trader sends to a sibling wallet
    moves value inside the entity; one delivered to the trader itself does not."""
    base = datetime(2024, 1, 1)
    common = {
        "chain_id": 1,
        "pool_address": "0xpool",
        "sender": "0xrouter",
        "block_timestamp": base,
    }
    to_sibling = SwapTrade(id=1, recipient="0xeve", tx_from="0xdave", **common)
    to_self = SwapTrade(id=2, recipient="0xdave", tx_from="0xdave", **common)
    cluster = AddressCluster(cluster_id="c", addresses=["0xdave", "0xeve"])

    wash = await detector.detect_wash_clusters([to_sibling, to_self], [cluster], AsyncMock())

    assert [t.id for t in wash] == [1]


@pytest.mark.asyncio
async def test_high_frequency_bot_keys_on_the_transaction_sender(detector):
    """Twelve fast swaps through one router by twelve different people are not a bot."""
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    trades = [
        SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender="0xrouter",
            recipient=f"0xuser{i}",
            tx_from=f"0xuser{i}",
            volume_usd=100.0,
            block_timestamp=base_time + timedelta(seconds=i),
        )
        for i in range(12)
    ]
    assert await detector.detect_high_frequency_bot(trades, AsyncMock()) == []


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


@pytest.fixture
def calibration():
    """Scoring rates for the composite tests, independent of the benchmark."""
    table = {
        name: DetectorCalibration(tpr=0.0, fpr_upper=1.0)
        for name in (
            "round_trip",
            "position_neutral",
            "closed_cluster",
            "repeated_amounts",
            "high_frequency_bot",
            "volume_anomaly",
            "wash_cluster",
        )
    }
    # circular_trading: precise enough to label a trade on its own.
    table["circular_trading"] = DetectorCalibration(tpr=0.9, fpr_upper=0.0001)
    override_calibration(table)
    yield table
    override_calibration(None)


@pytest.mark.asyncio
@pytest.mark.usefixtures("calibration")
async def test_run_all_heuristics_combines_detectors(detector, sample_trades):
    from unittest.mock import MagicMock

    session = AsyncMock()
    trades_result = MagicMock()
    trades_result.scalars.return_value.all.return_value = sample_trades
    clusters_result = MagicMock()
    clusters_result.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(side_effect=[trades_result, clusters_result])

    wash, stats = await detector.run_all_heuristics(1, "0xpool", session)

    assert set(stats) == {
        "round_trip",
        "circular_trading",
        "position_neutral",
        "closed_cluster",
        "repeated_amounts",
        "high_frequency_bot",
        "volume_anomaly",
    }
    assert stats["circular_trading"] == 2  # Dave<->Eve round trip
    assert {t.id for t in wash} == {3, 4}
    for trade in wash:
        assert trade.detection_method == "circular_trading"
        # P(wash | hit) = 0.9 * 0.01 / (0.9 * 0.01 + 0.0001 * 0.99)
        assert trade.wash_trade_score == pytest.approx(0.009 / (0.009 + 0.000099))


@pytest.mark.asyncio
async def test_composite_scores_hits_by_measured_rates(detector, calibration):
    """A detector that fires as often on honest trades as on wash ones cannot
    label a trade, however confident its own score; a precise one can."""
    base = datetime(2024, 1, 1)
    trades = [
        SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xs{i}",
            recipient=f"0xr{i}",
            volume_usd=100.0,
            block_timestamp=base + timedelta(minutes=i),
        )
        for i in range(3)
    ]
    calibration["volume_anomaly"] = DetectorCalibration(tpr=0.5, fpr_upper=0.5)
    calibration["round_trip"] = DetectorCalibration(tpr=0.95, fpr_upper=0.0001)

    async def noisy(ts, _session):
        return [ts[0], ts[1]]

    async def precise(ts, _session):
        return [ts[1]]

    async def none(*_args):
        return []

    detector.detect_volume_anomaly = noisy
    detector.advanced.detect_round_trips = precise
    for name in ("detect_circular_trading", "detect_high_frequency_bot"):
        setattr(detector, name, none)
    for name in ("detect_closed_cluster", "detect_repeated_amounts", "detect_position_neutral"):
        setattr(detector.advanced, name, none)

    wash, stats = await detector.run_detectors_on_trades(trades, AsyncMock())

    assert [t.id for t in wash] == [1]
    assert trades[1].detection_method == "round_trip"  # the most probative hit
    assert trades[0].is_wash_trade is False
    assert trades[0].wash_trade_score == pytest.approx(0.01)  # 0.5*0.01/(0.5*0.01+0.5*0.99)
    assert trades[0].detection_method == "volume_anomaly"
    assert stats["volume_anomaly"] == 2 and stats["round_trip"] == 1


@pytest.mark.asyncio
@pytest.mark.usefixtures("calibration")
async def test_composite_discards_labels_from_earlier_runs(detector):
    trade = SwapTrade(
        id=1,
        chain_id=1,
        pool_address="0xpool",
        sender="0xs",
        recipient="0xr",
        volume_usd=100.0,
        block_timestamp=datetime(2024, 1, 1),
        is_wash_trade=True,
        wash_trade_score=1.0,
        detection_method="self_trading",
    )
    wash, _ = await detector.run_detectors_on_trades([trade], AsyncMock())
    assert wash == []
    assert trade.is_wash_trade is False
    assert trade.detection_method is None and trade.wash_trade_score == 0.0


@pytest.mark.usefixtures("calibration")
def test_uncalibrated_detector_cannot_label():
    from core.calibration import wash_probability

    assert wash_probability("not_a_detector", 0.01) == 0.0


@pytest.mark.asyncio
async def test_run_all_heuristics_no_trades(detector):
    from unittest.mock import MagicMock

    session = AsyncMock()
    empty_result = MagicMock()
    empty_result.scalars.return_value.all.return_value = []
    session.execute = AsyncMock(return_value=empty_result)

    wash, stats = await detector.run_all_heuristics(1, "0xpool", session)

    assert wash == []
    assert stats == {}
