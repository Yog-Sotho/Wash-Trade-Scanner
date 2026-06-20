import pytest
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade

@pytest.mark.asyncio
async def test_high_frequency_bot_detection():
    detector = HeuristicDetector()
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    # 1. Bot trades: High frequency, low volume CV
    bot_trades = [
        SwapTrade(id=i, sender="0xBot", recipient="0xUser", volume_usd=100.0,
                  block_timestamp=base_time + timedelta(seconds=i * 5),
                  pool_address="0xpool")
        for i in range(15)
    ]

    # 2. Normal trades: Low frequency
    normal_trades = [
        SwapTrade(id=i+100, sender="0xUser", recipient="0xPool", volume_usd=500.0,
                  block_timestamp=base_time + timedelta(minutes=i * 10),
                  pool_address="0xpool")
        for i in range(5)
    ]

    all_trades = bot_trades + normal_trades
    wash = await detector.detect_high_frequency_bot(all_trades, AsyncMock())

    # Verify bot trades are detected
    detected_ids = [t.id for t in wash]
    for i in range(15):
        assert i in detected_ids

    # Verify normal trades are NOT detected
    for i in range(5):
        assert (i + 100) not in detected_ids

@pytest.mark.asyncio
async def test_volume_anomaly_detection():
    detector = HeuristicDetector()
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    # trades with varied volume to avoid MAD=0
    base_volumes = [100.0, 110.0, 90.0, 105.0, 95.0, 100.0, 110.0, 90.0, 105.0, 95.0]
    trades = [
        SwapTrade(id=i, sender=f"0xUser{i}", recipient="0xPool", volume_usd=base_volumes[i],
                  block_timestamp=base_time + timedelta(minutes=i),
                  pool_address="0xpool")
        for i in range(len(base_volumes))
    ]
    # 1 anomaly trade
    trades.append(
        SwapTrade(id=11, sender="0xWhale", recipient="0xPool", volume_usd=10000.0,
                  block_timestamp=base_time + timedelta(minutes=11),
                  pool_address="0xpool")
    )

    wash = await detector.detect_volume_anomaly(trades, AsyncMock())

    # Verify anomaly is detected
    detected_ids = [t.id for t in wash]
    assert 11 in detected_ids
    assert len(wash) == 1
