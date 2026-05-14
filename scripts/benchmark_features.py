
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
import pandas as pd
from core.feature_engineer import FeatureEngineer
from models.schemas import SwapTrade

async def run_benchmark():
    # 1. Setup mock trades
    num_trades = 1000
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    trades = []
    for i in range(num_trades):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xsender_{i % 10}",
            recipient=f"0xrecipient_{i % 10}",
            volume_usd=100.0,
            amount_in_usd=100.0,
            amount_out_usd=99.0,
            block_timestamp=base_time + timedelta(seconds=i*10),
            gas_price=20.0,
            transaction_hash=f"0xhash_{i}",
            log_index=0
        ))

    # 2. Setup mock storage and session
    mock_storage = MagicMock()
    fe = FeatureEngineer(mock_storage)

    mock_session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute.return_value = mock_result

    print(f"--- Benchmarking with {num_trades} trades ---")

    # 3. Benchmark compute_pool_features
    # We need to mock the initial query inside compute_pool_features
    mock_result_pool = MagicMock()
    mock_result_pool.scalars.return_value.all.return_value = trades

    # We'll patch session.execute to return trades for the first call (in compute_pool_features)
    # and empty list for subsequent calls (if any)
    mock_session.execute.side_effect = [mock_result_pool] + [mock_result] * 10000

    start_time = time.perf_counter()
    await fe.compute_pool_features(1, "0xpool", mock_session)
    end_time = time.perf_counter()
    pool_features_time = end_time - start_time
    print(f"compute_pool_features time: {pool_features_time:.4f} seconds")

    # 4. Benchmark build_ml_features
    # reset side effect
    mock_session.execute.side_effect = [mock_result_pool] + [mock_result] * 10000

    start_time = time.perf_counter()
    await fe.build_ml_features(1, "0xpool", mock_session)
    end_time = time.perf_counter()
    build_ml_features_time = end_time - start_time
    print(f"build_ml_features time: {build_ml_features_time:.4f} seconds")

    # Count calls to session.execute
    print(f"Number of session.execute calls: {mock_session.execute.call_count}")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
