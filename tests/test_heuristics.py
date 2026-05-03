"""
Unit tests for heuristic detectors.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from models.schemas import SwapTrade
from core.heuristics import HeuristicDetector


@pytest.fixture
def sample_trades():
    """Create sample trades for testing."""
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    trades = []

    # Normal trade
    trades.append(SwapTrade(
        id=1,
        chain_id=1,
        pool_address="0xpool1",
        sender="0xAlice",
        recipient="0xBob",
        volume_usd=1000.0,
        block_timestamp=base_time,
        is_wash_trade=False,
    ))

    # Self-trade (wash)
    trades.append(SwapTrade(
        id=2,
        chain_id=1,
        pool_address="0xpool1",
        sender="0xCarol",
        recipient="0xCarol",
        volume_usd=500.0,
        block_timestamp=base_time + timedelta(minutes=1),
        is_wash_trade=False,
    ))

    # Circular trade pair (wash)
    trades.append(SwapTrade(
        id=3,
        chain_id=1,
        pool_address="0xpool1",
        sender="0xDave",
        recipient="0xEve",
        volume_usd=200.0,
        block_timestamp=base_time + timedelta(minutes=2),
        is_wash_trade=False,
    ))
    trades.append(SwapTrade(
        id=4,
        chain_id=1,
        pool_address="0xpool1",
        sender="0xEve",
        recipient="0xDave",
        volume_usd=200.0,
        block_timestamp=base_time + timedelta(minutes=3),
        is_wash_trade=False,
    ))

    return trades


@pytest.mark.asyncio
async def test_self_trading_detection(sample_trades):
    detector = HeuristicDetector()
    session = AsyncMock()

    wash_trades = await detector.detect_self_trading(sample_trades, session)

    assert len(wash_trades) == 1
    assert wash_trades[0].id == 2
    assert wash_trades[0].detection_method == "self_trading"
    assert wash_trades[0].wash_trade_score == 1.0


@pytest.mark.asyncio
async def test_circular_trading_detection(sample_trades):
    detector = HeuristicDetector()
    session = AsyncMock()

    wash_trades = await detector.detect_circular_trading(sample_trades, session)

    # Should detect both sides of the circular trade
    assert len(wash_trades) >= 2
    detected_ids = {t.id for t in wash_trades}
    assert 3 in detected_ids
    assert 4 in detected_ids


@pytest.mark.asyncio
async def test_high_frequency_bot_detection():
    detector = HeuristicDetector()
    session = AsyncMock()

    base_time = datetime(2024, 1, 1, 12, 0, 0)
    trades = []

    # Simulate bot: 20 trades from same sender within 10 seconds
    for i in range(20):
        trades.append(SwapTrade(
            id=i+1,
            chain_id=1,
            pool_address="0xpool1",
            sender="0xBot",
            recipient=f"0xRecipient{i}",
            volume_usd=100.0,
            block_timestamp=base_time + timedelta(seconds=i*0.5),
            is_wash_trade=False,
        ))

    wash_trades = await detector.detect_high_frequency_bot(trades, session)

    # All trades should be flagged as bot activity
    assert len(wash_trades) == 20
    assert all(t.detection_method == "high_frequency_bot" for t in wash_trades)