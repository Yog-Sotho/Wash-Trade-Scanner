
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade, AddressCluster
from config.settings import settings

async def run_benchmark():
    num_trades = 50000
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    trades = []
    for i in range(num_trades):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xsender_{i % 1000}".lower(),
            recipient=f"0xrecipient_{i % 1001}".lower(),
            volume_usd=100.0,
            amount_in_usd=100.0,
            amount_out_usd=99.0,
            block_timestamp=base_time + timedelta(seconds=i),
            gas_price=20.0,
            transaction_hash=f"0xhash_{i}",
            log_index=0
        ))

    hd = HeuristicDetector()
    mock_session = AsyncMock()

    print(f"--- Benchmarking Heuristic Detectors with {num_trades} trades ---")

    # 1. detect_self_trading
    start = time.perf_counter()
    await hd.detect_self_trading(trades, mock_session)
    end = time.perf_counter()
    print(f"detect_self_trading: {end - start:.4f}s")

    # 2. detect_high_frequency_bot
    start = time.perf_counter()
    await hd.detect_high_frequency_bot(trades, mock_session)
    end = time.perf_counter()
    print(f"detect_high_frequency_bot: {end - start:.4f}s")

    # 3. detect_volume_anomaly
    start = time.perf_counter()
    await hd.detect_volume_anomaly(trades, mock_session)
    end = time.perf_counter()
    print(f"detect_volume_anomaly: {end - start:.4f}s")

    # 4. detect_wash_clusters
    clusters = [
        AddressCluster(cluster_id="1:0xpool:1", addresses=[f"0xsender_{i}".lower() for i in range(10)])
    ]
    start = time.perf_counter()
    await hd.detect_wash_clusters(trades, clusters, mock_session)
    end = time.perf_counter()
    print(f"detect_wash_clusters: {end - start:.4f}s")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
