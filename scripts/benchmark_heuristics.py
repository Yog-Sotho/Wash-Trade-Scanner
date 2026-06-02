import asyncio
import time
import random
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

from models.schemas import SwapTrade, AddressCluster
from core.heuristics import HeuristicDetector

def generate_mock_trades(n=10000):
    trades = []
    base_time = datetime(2024, 1, 1)
    senders = [f"0xsender_{i}" for i in range(100)]
    recipients = [f"0xrecipient_{i}" for i in range(100)]

    for i in range(n):
        sender = random.choice(senders)
        recipient = random.choice(recipients)
        # Inject some self-trading
        if random.random() < 0.05:
            recipient = sender

        trade = SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            dex_name="uniswap_v2",
            token_in="0xtoken1",
            token_out="0xtoken2",
            amount_in=1.0,
            amount_out=1.0,
            sender=sender.lower(),
            recipient=recipient.lower(),
            transaction_hash=f"0xhash_{i}",
            block_number=1000 + i // 10,
            block_timestamp=base_time + timedelta(seconds=i),
            volume_usd=random.uniform(10, 1000)
        )
        trades.append(trade)
    return trades

async def benchmark():
    detector = HeuristicDetector()
    trades = generate_mock_trades(10000)

    # Mock session
    session = AsyncMock()

    print(f"Benchmarking with {len(trades)} trades...")

    start_time = time.time()
    # We need to mock clusters too since run_all_heuristics uses them
    # But wait, run_all_heuristics fetches them from the DB if not provided,
    # but it doesn't take clusters as an argument.
    # It executes a query.

    # Let's mock the session.execute to return an empty list of clusters
    class MockScalars:
        def all(self):
            return []

    class MockResult:
        def scalars(self):
            return MockScalars()

    session.execute = AsyncMock(return_value=MockResult())

    await detector.run_all_heuristics(1, "0xpool", session, trades=trades)
    end_time = time.time()

    duration = end_time - start_time
    print(f"Total time for run_all_heuristics: {duration:.4f} seconds")

if __name__ == "__main__":
    asyncio.run(benchmark())
