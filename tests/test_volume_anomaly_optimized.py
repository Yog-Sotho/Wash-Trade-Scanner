
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
import numpy as np

from models.schemas import SwapTrade
from core.heuristics import HeuristicDetector, RobustAnomalyDetector

@pytest.fixture
def detector():
    return HeuristicDetector()

@pytest.mark.asyncio
async def test_detect_volume_anomaly_optimized(detector):
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    # Generate 20 trades with varied volume and 1 anomaly
    # Varied volume ensures MAD > 0
    trades = []
    for i in range(20):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xsender_{i}",
            recipient=f"0xrecipient_{i}",
            volume_usd=100.0 + (i * 10),
            block_timestamp=base_time + timedelta(minutes=i)
        ))

    # Add an anomaly
    trades.append(SwapTrade(
        id=20,
        chain_id=1,
        pool_address="0xpool",
        sender="0xsender_anomaly",
        recipient="0xrecipient_anomaly",
        volume_usd=1000000.0,
        block_timestamp=base_time + timedelta(minutes=20)
    ))

    wash = await detector.detect_volume_anomaly(trades, AsyncMock())
    assert len(wash) >= 1
    assert any(t.id == 20 for t in wash)

def test_robust_anomaly_detector_numpy():
    detector = RobustAnomalyDetector(method="mad")
    # Varied volumes to avoid MAD=0
    volumes = [100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 10000.0]
    detector.fit(volumes)

    # Score for normal volume should be low
    assert detector.score(120.0) < 1.0
    # Score for anomaly should be high
    assert detector.score(10000.0) > 3.0

    # Test IQR method
    detector_iqr = RobustAnomalyDetector(method="iqr")
    detector_iqr.fit(volumes)
    assert detector_iqr.score(10000.0) > 0

def test_robust_anomaly_detector_zero_mad():
    detector = RobustAnomalyDetector(method="mad")
    volumes = [100.0] * 10 # All same, MAD will be 0
    detector.fit(volumes)
    assert detector.score(100.0) == 0.0
    assert detector.score(1000.0) == 0.0
