"""
Unit tests for heuristic detectors using configurable thresholds.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

from models.schemas import SwapTrade
from core.heuristics import HeuristicDetector

@pytest.fixture
def detector():
    return HeuristicDetector()

@pytest.fixture
def sample_trades():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    return [
        SwapTrade(id=1, chain_id=1, pool_address="0xpool", sender="0xAlice", recipient="0xBob",
                  volume_usd=1000.0, block_timestamp=base_time, is_wash_trade=False),
        SwapTrade(id=2, chain_id=1, pool_address="0xpool", sender="0xCarol", recipient="0xCarol",
                  volume_usd=500.0, block_timestamp=base_time + timedelta(minutes=1), is_wash_trade=False),
        SwapTrade(id=3, chain_id=1, pool_address="0xpool", sender="0xDave", recipient="0xEve",
                  volume_usd=200.0, block_timestamp=base_time + timedelta(minutes=2), is_wash_trade=False),
        SwapTrade(id=4, chain_id=1, pool_address="0xpool", sender="0xEve", recipient="0xDave",
                  volume_usd=200.0, block_timestamp=base_time + timedelta(minutes=3), is_wash_trade=False),
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
async def test_detect_high_frequency_bot(detector):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    # Create 12 trades for the same sender to trigger the bot detection (threshold is usually 10)
    bot_trades = []
    for i in range(12):
        bot_trades.append(SwapTrade(
            id=100 + i,
            sender="0xBot",
            recipient="0xExchange",
            # Inter-trade time: 10 seconds (threshold is 60s)
            block_timestamp=base_time + timedelta(seconds=i * 10),
            # Constant volume (CV will be 0, threshold is 0.5)
            volume_usd=100.0,
            pool_address="0xpool"
        ))

    wash = await detector.detect_high_frequency_bot(bot_trades, AsyncMock())
    assert len(wash) == 12
    for t in wash:
        assert t.detection_method == "high_frequency_bot"

@pytest.mark.asyncio
async def test_detect_high_frequency_bot_not_suspicious(detector):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    # Slow trades
    trades = []
    for i in range(12):
        trades.append(SwapTrade(
            id=200 + i,
            sender="0xRegular",
            recipient="0xExchange",
            # Inter-trade time: 1 hour (threshold is 60s)
            block_timestamp=base_time + timedelta(hours=i),
            volume_usd=100.0,
            pool_address="0xpool"
        ))

    wash = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(wash) == 0
