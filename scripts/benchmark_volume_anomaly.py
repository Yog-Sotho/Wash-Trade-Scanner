
import asyncio
import time
from datetime import datetime, timedelta
from typing import List
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade
from unittest.mock import AsyncMock

def generate_mock_trades(n: int) -> List[SwapTrade]:
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    trades = []
    for i in range(n):
        # Many identical volumes to test score_cache
        volume = 100.0 if i % 100 != 0 else 10000.0
        # Use few distinct timestamps to test bucket_cache
        ts = base_time + timedelta(minutes=(i // 100))
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xsender_{i % 10}",
            recipient=f"0xrecipient_{i % 10 + 1}",
            volume_usd=volume,
            block_timestamp=ts,
            transaction_hash=f"0xhash_{i}",
            log_index=0
        ))
    return trades

async def benchmark():
    num_trades = 100000
    print(f"Generating {num_trades} trades...")
    trades = generate_mock_trades(num_trades)

    detector = HeuristicDetector()
    mock_session = AsyncMock()

    print("Starting benchmark for detect_volume_anomaly...")
    start_time = time.perf_counter()
    results = await detector.detect_volume_anomaly(trades, mock_session)
    end_time = time.perf_counter()

    duration = end_time - start_time
    print(f"detect_volume_anomaly took {duration:.4f} seconds for {num_trades} trades")
    print(f"Detected {len(results)} anomalies")

if __name__ == "__main__":
    asyncio.run(benchmark())
