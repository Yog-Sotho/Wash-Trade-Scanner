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
    base_time = datetime(2023, 1, 1)
    # HFT bot: many trades with low time diff and low volume CV
    trades = []
    for i in range(10):
        trades.append(SwapTrade(
            id=i,
            sender="0xbot",
            recipient="0xrecipient",
            block_timestamp=base_time + timedelta(seconds=i*2),
            volume_usd=100.0,
            pool_address="0xpool",
            chain_id=1
        ))

    # Non-bot: few trades
    trades.append(SwapTrade(
        id=10,
        sender="0xuser",
        recipient="0xrecipient",
        block_timestamp=base_time,
        volume_usd=100.0,
        pool_address="0xpool",
        chain_id=1
    ))

    wash = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(wash) == 10
    assert all(t.sender == "0xbot" for t in wash)
    assert all(t.detection_method == "high_frequency_bot" for t in wash)
