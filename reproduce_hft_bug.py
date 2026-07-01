import asyncio
from datetime import datetime, timedelta
from unittest.mock import MagicMock
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade

async def main():
    detector = HeuristicDetector()

    # Mock some trades
    base_time = datetime(2023, 1, 1)
    trades = []
    for i in range(10):
        trade = SwapTrade(
            id=i,
            sender="0x123",
            recipient="0x456",
            block_timestamp=base_time + timedelta(seconds=i*5),
            volume_usd=100.0,
            pool_address="0xpool",
            chain_id=1
        )
        trades.append(trade)

    session = MagicMock()

    print("Running detect_high_frequency_bot...")
    try:
        results = await detector.detect_high_frequency_bot(trades, session)
        print(f"Detected {len(results)} trades")
    except Exception as e:
        print(f"Caught exception: {type(e).__name__}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
