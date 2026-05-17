
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from collections import defaultdict
import numpy as np
import pandas as pd
from core.feature_engineer import FeatureEngineer
from models.schemas import SwapTrade

async def run_benchmark():
    num_trades = 5000
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    trades = []
    for i in range(num_trades):
        # Create some circular patterns
        s_idx = i % 10
        r_idx = (i + 1) % 10
        if i % 2 == 0:
            s_idx, r_idx = r_idx, s_idx # reverse trade

        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=f"0xaddr_{s_idx}",
            recipient=f"0xaddr_{r_idx}",
            volume_usd=100.0 + (i % 50),
            amount_in_usd=100.0,
            amount_out_usd=99.0,
            block_timestamp=base_time + timedelta(seconds=i*2),
            gas_price=20.0,
            transaction_hash=f"0xhash_{i}",
            log_index=0
        ))

    mock_storage = MagicMock()
    fe = FeatureEngineer(mock_storage)

    mock_session = AsyncMock()

    mock_result_pool = MagicMock()
    mock_result_pool.scalars.return_value.all.return_value = trades

    mock_result_hist = MagicMock()
    mock_result_hist.scalars.return_value.all.return_value = trades

    print(f"--- Benchmarking with {num_trades} trades ---")

    # 1. Benchmark compute_pool_features
    mock_session.execute.side_effect = [mock_result_pool]
    start_time = time.perf_counter()
    await fe.compute_pool_features(1, "0xpool", mock_session)
    end_time = time.perf_counter()
    print(f"compute_pool_features time: {end_time - start_time:.4f} seconds")

    # 2. Benchmark build_ml_features
    mock_session.execute.side_effect = [mock_result_pool, mock_result_hist, mock_result_pool]
    start_time = time.perf_counter()
    await fe.build_ml_features(1, "0xpool", mock_session)
    end_time = time.perf_counter()
    print(f"build_ml_features time: {end_time - start_time:.4f} seconds")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
