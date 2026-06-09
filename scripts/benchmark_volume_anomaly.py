
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade
import numpy as np

async def run_benchmark():
    # 1. Setup mock trades
    num_trades = 100000
    base_time = datetime(2024, 1, 1, 10, 0, 0)

    # Generate trades with varied volumes to avoid MAD=0
    # We use a log-normal distribution for volumes as it's common in finance
    volumes = np.random.lognormal(mean=4, sigma=1, size=num_trades)

    trades = []
    for i in range(num_trades):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xsender_{i % 100}",
            recipient=f"0xrecipient_{i % 100}",
            volume_usd=float(volumes[i]),
            block_timestamp=base_time + timedelta(seconds=i)
        ))

    hd = HeuristicDetector()
    mock_session = AsyncMock()

    print(f"--- Benchmarking detect_volume_anomaly with {num_trades} trades ---")

    # Warm up
    # await hd.detect_volume_anomaly(trades[:100], mock_session)

    start_time = time.perf_counter()
    results = await hd.detect_volume_anomaly(trades, mock_session)
    end_time = time.perf_counter()

    duration = end_time - start_time
    print(f"detect_volume_anomaly took: {duration:.4f} seconds")
    print(f"Detected {len(results)} anomalies")

    return duration

if __name__ == "__main__":
    asyncio.run(run_benchmark())
