
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
import os

# Set dummy env vars for settings validation
os.environ["DATABASE_HOST"] = "localhost"
os.environ["DATABASE_NAME"] = "test"
os.environ["DATABASE_USER"] = "test"
os.environ["DATABASE_PASSWORD"] = "testtest"
os.environ["ETH_RPC_URL"] = "http://localhost:8545"

from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade

async def run_benchmark():
    # 1. Setup mock trades
    num_trades = 100000
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    trades = []
    # 10 trades per second, so many trades share the same timestamp
    for i in range(num_trades):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xsender_{i % 100}",
            recipient=f"0xrecipient_{i % 100}",
            volume_usd=100.0 + (i % 10), # variation to avoid MAD=0 if possible, but many identical
            block_timestamp=base_time + timedelta(seconds=i // 10),
            transaction_hash=f"0xhash_{i}",
            log_index=0
        ))

    hd = HeuristicDetector()
    mock_session = AsyncMock()

    print(f"--- Benchmarking detect_volume_anomaly with {num_trades} trades ---")

    # Warm up
    await hd.detect_volume_anomaly(trades[:100], mock_session)

    durations = []
    for _ in range(3):
        start_time = time.perf_counter()
        await hd.detect_volume_anomaly(trades, mock_session)
        end_time = time.perf_counter()
        durations.append(end_time - start_time)

    avg_duration = sum(durations) / len(durations)
    print(f"Average time taken: {avg_duration:.4f} seconds")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
