import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from models.schemas import SwapTrade
from scripts.run_audit import AuditParameters, AuditRunner


async def run_benchmark():
    # 1. Setup mock trades
    num_trades = 1000
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    trades = []
    for i in range(num_trades):
        trades.append(
            SwapTrade(
                id=i,
                chain_id=1,
                pool_address="0xpool",
                sender=f"0xsender_{i % 10}",
                recipient=f"0xrecipient_{i % 11}",
                volume_usd=100.0,
                amount_in_usd=100.0,
                amount_out_usd=99.0,
                block_timestamp=base_time + timedelta(seconds=i * 10),
                gas_price=20.0,
                transaction_hash=f"0xhash_{i}",
                log_index=0,
            )
        )

    runner = AuditRunner()
    runner.storage = MagicMock()
    mock_session = AsyncMock()

    # Mocking the session factory to return our mock session
    session_context = MagicMock()
    session_context.__aenter__ = AsyncMock(return_value=mock_session)
    session_context.__aexit__ = AsyncMock()
    runner.storage.get_session = AsyncMock(return_value=session_context)

    # Mock storage methods
    async def mock_get_pool_trades(*args, **kwargs):
        return trades

    runner.storage.get_pool_trades = mock_get_pool_trades
    runner.storage.update_trade_labels = AsyncMock()
    runner.storage.create_audit_log = AsyncMock()

    params = AuditParameters(
        chain_id=1,
        pool_address="0x1234567890123456789012345678901234567890",
        use_ml=True,
        use_heuristics=True,
    )

    # We need to mock MultiChainIngestor.initialize and audit_pool
    # and MLDetector.load_model and detect_wash_trades
    # and HeuristicDetector.run_all_heuristics

    with (
        patch("scripts.run_audit.MultiChainIngestor") as MockIngestor,
        patch("scripts.run_audit.MLDetector") as MockMLDetector,
        patch("scripts.run_audit.HeuristicDetector") as MockHeuristicDetector,
    ):

        mock_ingestor = MockIngestor.return_value
        mock_ingestor.initialize = AsyncMock()
        mock_ingestor.audit_pool = AsyncMock(return_value=num_trades)

        mock_ml = MockMLDetector.return_value
        mock_ml.load_model = MagicMock()

        # Simulate long running ML detection
        async def slow_ml(*args, **kwargs):
            await asyncio.sleep(0.1)
            return []

        mock_ml.detect_wash_trades = slow_ml

        mock_hd = MockHeuristicDetector.return_value

        # Simulate long running heuristic detection
        async def slow_hd(*args, **kwargs):
            await asyncio.sleep(0.1)
            return [], {}

        mock_hd.run_all_heuristics = slow_hd

        print(f"--- run_audit parallelization benchmark ---")
        start_time = time.perf_counter()
        await runner.run_audit(params)
        duration = time.perf_counter() - start_time
        print(f"Audit duration with parallelization: {duration:.4f} seconds")

        # If they were serial, it would be at least 0.2s (0.1s + 0.1s)
        # With parallelization, it should be close to 0.1s
        if duration < 0.15:
            print("Parallelization SUCCESS: Audit took less than the sum of its parts.")
        else:
            print("Parallelization FAILURE: Audit took as long as serial execution.")


if __name__ == "__main__":
    asyncio.run(run_benchmark())
