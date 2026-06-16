
import pytest
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
from models.schemas import SwapTrade
from core.heuristics import HeuristicDetector, RobustAnomalyDetector

@pytest.mark.asyncio
async def test_high_frequency_bot_vectorized():
    detector = HeuristicDetector()
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    # Create 15 trades (threshold is 10) with very low time interval and identical volume
    trades = []
    for i in range(15):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender="0xBot",
            recipient="0xTarget",
            volume_usd=1000.0,
            block_timestamp=base_time + timedelta(seconds=i), # 1s between trades
            is_wash_trade=False
        ))

    wash = await detector.detect_high_frequency_bot(trades, AsyncMock())

    # Should be detected as high frequency bot because:
    # avg_time = 1s < threshold (60s)
    # volume_cv = 0 < threshold (0.1)
    assert len(wash) == 15
    for t in wash:
        assert t.detection_method == "high_frequency_bot"

def test_robust_anomaly_detector_consolidation():
    detector = RobustAnomalyDetector(method="mad")
    volumes = [100.0, 100.0, 100.0, 100.0, 1000000.0]
    detector.fit(volumes)

    # Test with List
    scores_list = detector.score_batch([100.0, 1000000.0])
    assert isinstance(scores_list, np.ndarray)
    assert len(scores_list) == 2

    # Test with Array
    scores_arr = detector.score_batch(np.array([100.0, 1000000.0]))
    assert isinstance(scores_arr, np.ndarray)
    assert len(scores_arr) == 2

    assert np.allclose(scores_list, scores_arr)
