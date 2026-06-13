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
async def test_high_frequency_bot(detector):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    # Create 15 trades from the same sender with 10s intervals and identical volumes
    bot_trades = []
    for i in range(15):
        bot_trades.append(SwapTrade(
            id=i+1, chain_id=1, pool_address="0xpool", sender="0xBot", recipient="0xRecipient",
            volume_usd=100.0, block_timestamp=base_time + timedelta(seconds=i*10), is_wash_trade=False
        ))

    wash = await detector.detect_high_frequency_bot(bot_trades, AsyncMock())
    # Should detect 15 trades (the whole sequence from the bot)
    assert len(wash) == 15
    for t in wash:
        assert t.detection_method == "high_frequency_bot"
        assert t.is_wash_trade == True
