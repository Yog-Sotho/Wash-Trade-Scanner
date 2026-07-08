
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

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
            recipient=f"0xrecipient_{i % 11}", # slightly different to have variety
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

    # Generic mock result
    def make_mock_result(data):
        m = MagicMock()
        m.scalars.return_value.all.return_value = data
        return m

    print(f"--- Benchmarking with {num_trades} trades ---")

    # 3. Benchmark compute_pool_features
    mock_session.execute.reset_mock()
    mock_session.execute.side_effect = [make_mock_result(trades)]

    start_time = time.perf_counter()
    await fe.compute_pool_features(1, "0xpool", mock_session)
    end_time = time.perf_counter()
    pool_features_time = end_time - start_time
    print(f"compute_pool_features time: {pool_features_time:.4f} seconds")
    print(f"compute_pool_features execute calls: {mock_session.execute.call_count}")

    # 4. Benchmark build_ml_features
    # Redundant query expectation:
    # 1. Initial query in build_ml_features
    # 2. History query in build_ml_features
    # 3. Redundant query in compute_pool_features (called inside build_ml_features)

    mock_session.execute.reset_mock()
    # History includes the trades themselves plus maybe some buffer
    mock_session.execute.side_effect = [
        make_mock_result(trades), # Initial trades
        make_mock_result(trades), # History
        make_mock_result(trades), # REDUNDANT call from compute_pool_features
    ]

    start_time = time.perf_counter()
    await fe.build_ml_features(1, "0xpool", mock_session)
    end_time = time.perf_counter()
    build_ml_features_time = end_time - start_time
    print(f"build_ml_features time: {build_ml_features_time:.4f} seconds")
    print(f"build_ml_features total execute calls: {mock_session.execute.call_count}")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
