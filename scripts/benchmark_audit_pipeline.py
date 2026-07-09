
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from core.feature_engineer import FeatureEngineer
from core.heuristics import HeuristicDetector
from core.ml_detector import MLDetector
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
            recipient=f"0xrecipient_{i % 11}",
            volume_usd=100.0,
            amount_in_usd=100.0,
            amount_out_usd=99.0,
            block_timestamp=base_time + timedelta(seconds=i*10),
            gas_price=20.0,
            transaction_hash=f"0xhash_{i}",
            log_index=0
        ))

    mock_storage = MagicMock()
    mock_session = AsyncMock()
    fe = FeatureEngineer(mock_storage)
    hd = HeuristicDetector()
    md = MLDetector(mock_storage, fe)
    md.is_trained = True
    md.model = MagicMock()
    md.model.decision_function.return_value = [0.0] * num_trades

    def make_mock_result(data, type='trade'):
        m = MagicMock()
        if type == 'trade':
            m.scalars.return_value.all.return_value = data
        else: # clusters
            m.scalars.return_value.all.return_value = []
        return m

    # Mocking the session factory to return our mock session
    session_context = MagicMock()
    session_context.__aenter__ = AsyncMock(return_value=mock_session)
    session_context.__aexit__ = AsyncMock()
    mock_storage.get_session = AsyncMock(return_value=session_context)

    # Mock storage.get_pool_trades
    async def mock_get_pool_trades(chain_id, pool_address, limit=None, offset=0, ascending=False):
        # Simulate DB latency
        await asyncio.sleep(0.05)
        return trades

    mock_storage.get_pool_trades = mock_get_pool_trades

    # Case A: Passing trades (Optimized)
    mock_session.execute.reset_mock()
    def side_effect(stmt):
        # Cluster query
        return make_mock_result([], 'cluster')

    mock_session.execute.side_effect = side_effect

    print(f"--- Full Audit Pipeline Benchmark with {num_trades} trades and 50ms DB latency ---")

    start_time = time.perf_counter()
    # 1. Heuristics
    await hd.run_all_heuristics(1, "0xpool", mock_session, trades=trades)
    # 2. ML
    await md.detect_wash_trades(1, "0xpool", trades=trades)
    end_time = time.perf_counter()
    optimized_time = end_time - start_time
    print(f"Optimized time (with trades passed): {optimized_time:.4f} seconds")

    # Case B: Not passing trades (Unoptimized)
    mock_session.execute.reset_mock()
    start_time = time.perf_counter()
    # 1. Heuristics
    await hd.run_all_heuristics(1, "0xpool", mock_session)
    # 2. ML
    await md.detect_wash_trades(1, "0xpool")
    end_time = time.perf_counter()
    unoptimized_time = end_time - start_time
    print(f"Unoptimized time (refetching): {unoptimized_time:.4f} seconds")

    if unoptimized_time > 0:
        improvement = (unoptimized_time - optimized_time) / unoptimized_time
        print(f"Performance Improvement: {improvement:.2%}")
        print(f"Time Saved: {unoptimized_time - optimized_time:.4f} seconds")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
