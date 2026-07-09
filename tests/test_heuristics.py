"""
Unit tests for heuristic detectors using configurable thresholds.
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade


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
    # 10 trades in 10 seconds (1s average)
    trades = [
        SwapTrade(id=i, chain_id=1, pool_address="0xpool", sender="0xBot", recipient="0xBob",
                  volume_usd=100.0, block_timestamp=base_time + timedelta(seconds=i), is_wash_trade=False)
        for i in range(10)
    ]
    # Adjust settings for the test
    from config.settings import settings
    settings.BOT_TRADE_COUNT_THRESHOLD = 5
    settings.BOT_TRADE_TIME_THRESHOLD_SECONDS = 10.0
    settings.BOT_VOLUME_CV_THRESHOLD = 0.1 # Low CV since volumes are all 100.0

    wash = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(wash) == 10
    assert all(t.detection_method == "high_frequency_bot" for t in wash)

@pytest.mark.asyncio
async def test_detect_high_frequency_bot_not_suspicious(detector):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    # 10 trades in 1000 seconds (100s average)
    trades = [
        SwapTrade(id=i, chain_id=1, pool_address="0xpool", sender="0xLegit", recipient="0xBob",
                  volume_usd=100.0, block_timestamp=base_time + timedelta(seconds=i*100), is_wash_trade=False)
        for i in range(10)
    ]
    from config.settings import settings
    settings.BOT_TRADE_COUNT_THRESHOLD = 5
    settings.BOT_TRADE_TIME_THRESHOLD_SECONDS = 10.0 # Threshold is 10s, avg is 100s

    wash = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(wash) == 0
