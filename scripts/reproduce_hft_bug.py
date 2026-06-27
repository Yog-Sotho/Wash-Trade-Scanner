
import asyncio
from datetime import datetime, timedelta
from unittest.mock import MagicMock
import numpy as np
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade

async def test_hft_bug():
    detector = HeuristicDetector()

    # Create some mock trades for a single sender
    sender = "0x123"
    trades = []
    base_time = datetime(2023, 1, 1, 10, 0, 0)
    for i in range(10):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xabc",
            sender=sender,
            recipient="0xdef",
            block_timestamp=base_time + timedelta(seconds=i*2), # 2 seconds apart
            volume_usd=100.0,
            gas_price=1e9,
            transaction_hash=f"0x{i}",
            log_index=0
        ))

    session = MagicMock()

    print("Running detect_high_frequency_bot...")
    try:
        detected = await detector.detect_high_frequency_bot(trades, session)
        print(f"Detected {len(detected)} trades")
    except NameError as e:
        print(f"Caught expected NameError: {e}")
    except Exception as e:
        print(f"Caught unexpected exception: {type(e).__name__}: {e}")

if __name__ == "__main__":
    asyncio.run(test_hft_bug())
