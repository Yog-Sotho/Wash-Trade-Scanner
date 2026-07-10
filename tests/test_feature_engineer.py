"""
Unit tests for feature engineering, including the trade_id label-lookup fix.
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.feature_engineer import (
    FeatureEngineer,
    benford_deviation,
    normalized_hour_entropy,
    significant_digits,
)
from models.schemas import SwapTrade


def test_significant_digits():
    assert significant_digits(1000.0) == 1
    assert significant_digits(1234.0) == 4
    assert significant_digits(0.00125) == 3
    assert significant_digits(0) == 0
    assert significant_digits(float("inf")) == 0


def test_benford_deviation_organic_vs_fabricated():
    # Amounts spanning several orders of magnitude follow Benford closely.
    organic = [1.2 * (1.7**i) for i in range(60)]
    # A bot recycling one leading digit deviates strongly.
    fabricated = [900.0 + i for i in range(60)]
    assert benford_deviation(organic) < benford_deviation(fabricated)
    assert benford_deviation([]) == 0.0
    assert benford_deviation([0.0, -5.0]) == 0.0


def test_normalized_hour_entropy():
    # All trades in one hour: zero entropy.
    same_hour = [datetime(2024, 1, 1, 12, m % 60) for m in range(10)]
    assert normalized_hour_entropy(same_hour) == 0.0
    # Uniform around the clock: entropy 1.
    uniform = [datetime(2024, 1, 1, h) for h in range(24)]
    assert normalized_hour_entropy(uniform) == pytest.approx(1.0)
    assert normalized_hour_entropy([]) == 0.0


@pytest.fixture
def engineer():
    return FeatureEngineer(storage=AsyncMock())


def _session_returning(rows):
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = rows
    session.execute.return_value = result
    return session


@pytest.mark.asyncio
async def test_compute_trade_features_includes_trade_id(engineer):
    trade = SwapTrade(
        id=42,
        chain_id=1,
        pool_address="0xpool",
        sender="0xalice",
        recipient="0xbob",
        amount_in=1234.5,
        volume_usd=1000.0,
        amount_in_usd=1000.0,
        amount_out_usd=990.0,
        block_timestamp=datetime(2024, 1, 1, 12, 0, 0),
        gas_price=10.0,
    )
    session = _session_returning([])

    features = await engineer.compute_trade_features(trade, session)

    # This is the field ml_detector.train() relies on to match heuristic labels
    # back to specific trades - without it, sample weighting silently never fires.
    assert features["trade_id"] == 42
    assert features["volume_usd"] == 1000.0
    assert features["sender_trade_count_1h"] == 0
    assert features["sender_trade_count_24h"] == 0
    assert features["amount_significant_digits"] == 5
    # Only the trade's own timestamp counts -> one hour bucket -> zero entropy.
    assert features["sender_hour_entropy_24h"] == 0.0
    assert features["slippage_ratio"] == pytest.approx(0.01, abs=1e-6)


@pytest.mark.asyncio
async def test_compute_trade_features_counts_prior_activity(engineer):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    trade = SwapTrade(
        id=2,
        chain_id=1,
        pool_address="0xpool",
        sender="0xalice",
        recipient="0xbob",
        volume_usd=500.0,
        block_timestamp=base_time,
    )
    prior_sender_trade = SwapTrade(
        id=1,
        chain_id=1,
        sender="0xalice",
        recipient="0xcarol",
        volume_usd=200.0,
        block_timestamp=base_time - timedelta(minutes=30),
    )

    session = AsyncMock()
    # compute_trade_features issues 4 sequential queries: sender, recipient, pair,
    # reverse-pair. Only the first (sender) has a matching prior trade here.
    results = []
    for rows in ([prior_sender_trade], [], [], []):
        r = MagicMock()
        r.scalars.return_value.all.return_value = rows
        results.append(r)
    session.execute = AsyncMock(side_effect=results)

    features = await engineer.compute_trade_features(trade, session)

    assert features["sender_trade_count_1h"] == 1
    assert features["sender_trade_count_24h"] == 1
    assert features["sender_volume_1h"] == 200.0
    assert features["recipient_trade_count_1h"] == 0
    assert features["pair_trade_count_1h"] == 0
    assert features["reverse_pair_trade_count_1h"] == 0
    assert features["time_since_last_sender_trade"] == pytest.approx(1800.0)


@pytest.mark.asyncio
async def test_compute_pool_features_empty(engineer):
    session = _session_returning([])
    features = await engineer.compute_pool_features(1, "0xpool", session)
    assert features == {}


@pytest.mark.asyncio
async def test_compute_pool_features_with_data(engineer):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    trades = [
        SwapTrade(
            id=1,
            chain_id=1,
            pool_address="0xpool",
            sender="0xalice",
            recipient="0xbob",
            volume_usd=100.0,
            block_timestamp=base_time,
        ),
        SwapTrade(
            id=2,
            chain_id=1,
            pool_address="0xpool",
            sender="0xbob",
            recipient="0xalice",
            volume_usd=100.0,
            block_timestamp=base_time + timedelta(minutes=5),
        ),
        SwapTrade(
            id=3,
            chain_id=1,
            pool_address="0xpool",
            sender="0xcarol",
            recipient="0xcarol",
            volume_usd=50.0,
            block_timestamp=base_time + timedelta(minutes=10),
        ),
    ]
    session = _session_returning(trades)

    features = await engineer.compute_pool_features(1, "0xpool", session)

    assert features["total_volume_usd"] == 250.0
    assert features["unique_senders"] == 3
    assert features["circular_trade_ratio"] > 0  # alice<->bob is circular
    assert features["self_trade_ratio"] == pytest.approx(1 / 4)  # carol self-trades
    assert "benford_deviation" in features
    assert 0.0 <= features["hour_entropy"] <= 1.0


@pytest.mark.asyncio
async def test_build_ml_features_empty(engineer):
    session = _session_returning([])
    df = await engineer.build_ml_features(1, "0xpool", session)
    assert df.empty


@pytest.mark.asyncio
async def test_build_ml_features_with_data(engineer):
    trade = SwapTrade(
        id=1,
        chain_id=1,
        pool_address="0xpool",
        sender="0xalice",
        recipient="0xbob",
        volume_usd=100.0,
        block_timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )

    session = AsyncMock()
    empty_result = MagicMock()
    empty_result.scalars.return_value.all.return_value = []
    trades_result = MagicMock()
    trades_result.scalars.return_value.all.return_value = [trade]
    # 1st call (build_ml_features' own trades query) -> [trade]
    # next 4 calls (compute_trade_features' sender/recipient/pair/reverse) -> empty
    # last call (compute_pool_features' trades query) -> [trade]
    session.execute = AsyncMock(
        side_effect=[
            trades_result,
            empty_result,
            empty_result,
            empty_result,
            empty_result,
            trades_result,
        ]
    )

    df = await engineer.build_ml_features(1, "0xpool", session)

    assert not df.empty
    assert df["trade_id"].iloc[0] == 1
    assert "pool_total_volume_usd" in df.columns
