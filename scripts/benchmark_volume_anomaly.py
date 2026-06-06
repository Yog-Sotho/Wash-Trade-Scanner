
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade

async def run_benchmark():
    num_trades = 100000
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    # Most trades in the same hour to stress bucket grouping
    trades = []
    for i in range(num_trades):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xsender_{i % 10}",
            recipient=f"0xrecipient_{i % 11}",
            volume_usd=100.0 + (i % 10),
            block_timestamp=base_time + timedelta(seconds=(i % 3600)),
            gas_price=20.0,
            transaction_hash=f"0xhash_{i}",
            log_index=0
        ))

    hd = HeuristicDetector()
    mock_session = AsyncMock()

    print(f"--- detect_volume_anomaly Benchmark with {num_trades} trades ---")

    start_time = time.perf_counter()
    await hd.detect_volume_anomaly(trades, mock_session)
    end_time = time.perf_counter()

    print(f"Time taken: {end_time - start_time:.4f} seconds")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
