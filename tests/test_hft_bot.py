
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
import numpy as np

from models.schemas import SwapTrade
from core.heuristics import HeuristicDetector

@pytest.fixture
def detector():
    return HeuristicDetector()

@pytest.mark.asyncio
async def test_detect_high_frequency_bot_success(detector):
    sender = "0xbot"
    trades = []
    base_time = datetime(2023, 1, 1, 10, 0, 0)
    # 10 trades, 2 seconds apart, constant volume
    # This should trigger count >= 10, avg_time < 60s, and volume_cv < 0.5
    for i in range(10):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=sender,
            recipient="0xrecipient",
            block_timestamp=base_time + timedelta(seconds=i*2),
            volume_usd=100.0,
            is_wash_trade=False
        ))

    detected = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(detected) == 10
    for t in detected:
        assert t.is_wash_trade is True
        assert t.detection_method == "high_frequency_bot"

@pytest.mark.asyncio
async def test_detect_high_frequency_bot_low_count(detector):
    sender = "0xbot"
    trades = []
    base_time = datetime(2023, 1, 1, 10, 0, 0)
    # Only 5 trades (threshold is 10)
    for i in range(5):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=sender,
            recipient="0xrecipient",
            block_timestamp=base_time + timedelta(seconds=i*2),
            volume_usd=100.0,
            is_wash_trade=False
        ))

    detected = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(detected) == 0

@pytest.mark.asyncio
async def test_detect_high_frequency_bot_high_cv(detector):
    sender = "0xbot"
    trades = []
    base_time = datetime(2023, 1, 1, 10, 0, 0)
    # 10 trades, but volume varies significantly (CV > 0.5)
    for i in range(10):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=sender,
            recipient="0xrecipient",
            block_timestamp=base_time + timedelta(seconds=i*2),
            volume_usd=100.0 * (i + 1), # Increasing volume
            is_wash_trade=False
        ))

    detected = await detector.detect_high_frequency_bot(trades, AsyncMock())
    # volumes_array = [200, 300, ..., 1000]
    # mean = 600, std ~= 258, CV ~= 0.43... wait, let's make it even more variable
    # Let's just use 100 and 1000
    for i in range(len(trades)):
        trades[i].volume_usd = 100.0 if i % 2 == 0 else 1000.0

    detected = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(detected) == 0
