
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
import numpy as np

from models.schemas import SwapTrade
from core.heuristics import HeuristicDetector

@pytest.mark.asyncio
async def test_detect_high_frequency_bot():
    detector = HeuristicDetector()
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    # Create 15 trades for a single sender, spaced 1 second apart
    # settings.BOT_TRADE_COUNT_THRESHOLD is 10
    # settings.BOT_TRADE_TIME_THRESHOLD_SECONDS is 60.0
    # settings.BOT_VOLUME_CV_THRESHOLD is 0.5

    sender = "0xBot"
    trades = []
    for i in range(15):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=sender,
            recipient="0xBob",
            volume_usd=100.0, # Constant volume -> CV = 0
            block_timestamp=base_time + timedelta(seconds=i)
        ))

    wash = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(wash) == 15
    assert all(t.detection_method == "high_frequency_bot" for t in wash)

@pytest.mark.asyncio
async def test_detect_high_frequency_bot_not_suspicious():
    detector = HeuristicDetector()
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    # Spaced 2 minutes apart (120s > 60s threshold)
    sender = "0xHuman"
    trades = []
    for i in range(15):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=sender,
            recipient="0xBob",
            volume_usd=100.0,
            block_timestamp=base_time + timedelta(minutes=i*2)
        ))

    wash = await detector.detect_high_frequency_bot(trades, AsyncMock())
    assert len(wash) == 0
