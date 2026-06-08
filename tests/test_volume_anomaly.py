
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
from models.schemas import SwapTrade
from core.heuristics import HeuristicDetector

@pytest.mark.asyncio
async def test_detect_volume_anomaly_optimized():
    detector = HeuristicDetector()
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    # Create a batch of trades where one is an outlier
    trades = []
    # 10 identical trades
    for i in range(10):
        trades.append(SwapTrade(
            id=i, chain_id=1, pool_address="0xpool",
            sender=f"0xsender_{i}", recipient=f"0xrecipient_{i}",
            volume_usd=100.0,
            block_timestamp=base_time + timedelta(seconds=i),
            is_wash_trade=False
        ))

    # Add an outlier (but RobustAnomalyDetector may ignore it if it's too clean,
    # and MAD might be 0 if all are 100.0)
    # Let's add some variety to ensure MAD > 0
    for i in range(10):
        trades[i].volume_usd = 100.0 + (i % 3)

    # One big outlier
    trades.append(SwapTrade(
        id=10, chain_id=1, pool_address="0xpool",
        sender="0xoutlier", recipient="0xrecipient",
        volume_usd=1000000.0,
        block_timestamp=base_time + timedelta(seconds=11),
        is_wash_trade=False
    ))

    wash = await detector.detect_volume_anomaly(trades, AsyncMock())

    assert len(wash) > 0
    assert any(t.id == 10 for t in wash)
    assert all(t.detection_method == "volume_anomaly" for t in wash)

@pytest.mark.asyncio
async def test_detect_volume_anomaly_bucket_caching():
    detector = HeuristicDetector()
    # Use the same timestamp for multiple trades to hit the cache
    ts = datetime(2024, 1, 1, 12, 0, 0)
    trades = []
    for i in range(10):
        trades.append(SwapTrade(
            id=i, chain_id=1, pool_address="0xpool",
            sender=f"0xsender_{i}", recipient=f"0xrecipient_{i}",
            volume_usd=100.0 + i,
            block_timestamp=ts,
            is_wash_trade=False
        ))
    # Add an outlier with the same timestamp
    trades.append(SwapTrade(
        id=10, chain_id=1, pool_address="0xpool",
        sender="0xoutlier", recipient="0xrecipient",
        volume_usd=1000000.0,
        block_timestamp=ts,
        is_wash_trade=False
    ))

    wash = await detector.detect_volume_anomaly(trades, AsyncMock())
    assert len(wash) > 0
    assert any(t.id == 10 for t in wash)
