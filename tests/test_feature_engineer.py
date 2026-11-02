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
    trade_features,
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


def _swap(trade_id, trader, recipient, minutes, volume=100.0, sender="0xrouter", **kw):
    return SwapTrade(
        id=trade_id,
        chain_id=1,
        pool_address="0xpool",
        sender=sender,
        recipient=recipient,
        tx_from=trader,
        volume_usd=volume,
        amount_in=kw.pop("amount_in", 1234.5),
        block_number=trade_id,
        log_index=0,
        block_timestamp=datetime(2024, 1, 1, 12, 0, 0) + timedelta(minutes=minutes),
        **kw,
    )


def test_trade_features_window_counts_by_trader():
    trades = [
        _swap(1, "0xalice", "0xalice", 0, volume=100.0),
        _swap(2, "0xbob", "0xbob", 10),
        _swap(3, "0xalice", "0xalice", 30, volume=250.0),
        _swap(4, "0xalice", "0xalice", 100, volume=50.0, gas_price=21.5),  # 70 min after #3
    ]
    rows = {r["trade_id"]: r for r in trade_features(trades)}

    assert rows[1]["sender_trade_count_1h"] == 0
    assert rows[1]["time_since_last_sender_trade"] == 3600.0
    assert rows[3]["sender_trade_count_1h"] == 1  # Alice's trade 30 min earlier
    assert rows[3]["sender_volume_1h"] == pytest.approx(100.0)
    assert rows[3]["time_since_last_sender_trade"] == pytest.approx(1800.0)
    # Trade 4: Alice's trade at minute 30 is 70 minutes back -> outside 1h, inside 24h.
    assert rows[4]["sender_trade_count_1h"] == 0
    assert rows[4]["sender_trade_count_24h"] == 2
    assert rows[4]["time_since_last_sender_trade"] == 3600.0
    assert rows[4]["gas_price"] == 21.5
    # Bob is a different trader even though the router is every trade's sender.
    assert rows[2]["sender_trade_count_1h"] == 0


def test_trade_features_pair_and_reverse_pair():
    trades = [
        _swap(1, "0xalice", "0xbob", 0),
        _swap(2, "0xbob", "0xalice", 5),
        _swap(3, "0xalice", "0xbob", 10),
    ]
    rows = {r["trade_id"]: r for r in trade_features(trades)}

    assert rows[3]["pair_trade_count_1h"] == 1  # Alice -> Bob before
    assert rows[3]["reverse_pair_trade_count_1h"] == 1  # Bob -> Alice before
    assert rows[2]["reverse_pair_trade_count_1h"] == 1
    assert rows[3]["recipient_trade_count_1h"] == 1


def test_trade_features_slippage_and_digits():
    trade = _swap(1, "0xa", "0xa", 0, amount_in_usd=100.0, amount_out_usd=99.0, amount_in=1000.0)
    (row,) = trade_features([trade])
    assert row["slippage_ratio"] == pytest.approx(0.01)
    assert row["amount_significant_digits"] == 1


def test_trade_features_match_a_brute_force_reference():
    """The binary-search windows agree with counting every prior trade."""
    import random

    rng = random.Random(7)
    traders = ["0xa", "0xb", "0xc"]
    trades = [
        _swap(
            i, rng.choice(traders), rng.choice(traders), rng.randint(0, 3000), volume=rng.random()
        )
        for i in range(1, 200)
    ]
    rows = {r["trade_id"]: r for r in trade_features(trades)}
    ordered = sorted(trades, key=lambda t: (t.block_timestamp, t.block_number))
    for k, t in enumerate(ordered):
        prior = ordered[:k]
        in_1h = [
            p for p in prior if (t.block_timestamp - p.block_timestamp).total_seconds() <= 3600
        ]
        in_24h = [
            p for p in prior if (t.block_timestamp - p.block_timestamp).total_seconds() <= 86_400
        ]
        own_1h = [p for p in in_1h if p.tx_from == t.tx_from]
        r = rows[t.id]
        assert r["sender_trade_count_1h"] == len(own_1h)
        assert r["sender_volume_1h"] == pytest.approx(sum(p.volume_usd for p in own_1h))
        assert r["sender_trade_count_24h"] == len([p for p in in_24h if p.tx_from == t.tx_from])
        assert r["recipient_trade_count_1h"] == len(
            [p for p in in_1h if p.recipient == t.recipient]
        )
        assert r["pair_trade_count_1h"] == len(
            [p for p in in_1h if p.tx_from == t.tx_from and p.recipient == t.recipient]
        )
        assert r["reverse_pair_trade_count_1h"] == len(
            [
                p
                for p in in_1h
                if p.tx_from == t.recipient
                and p.recipient == t.tx_from
                and p.block_timestamp < t.block_timestamp
            ]
        )


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
        block_number=1,
        log_index=0,
        block_timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )
    session = _session_returning([trade])

    df = await engineer.build_ml_features(1, "0xpool", session)

    assert not df.empty
    assert df["trade_id"].iloc[0] == 1
    assert "pool_total_volume_usd" in df.columns
    # Regression (N+1): one query for the whole pool, not four per trade.
    assert session.execute.await_count == 1
