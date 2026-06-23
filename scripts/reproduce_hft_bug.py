
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade
import numpy as np
import time

async def reproduce():
    detector = HeuristicDetector()
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    # Create 1000 trades for one sender to trigger the HFT detector
    sender = "0xbot"
    trades = []
    for i in range(1000):
        trades.append(SwapTrade(
            id=i,
            chain_id=1,
            pool_address="0xpool",
            sender=sender,
            recipient="0xrecipient",
            volume_usd=100.0,
            block_timestamp=base_time + timedelta(seconds=i),
            is_wash_trade=False
        ))

    print("Testing detect_high_frequency_bot...")
    try:
        start = time.perf_counter()
        wash_trades = await detector.detect_high_frequency_bot(trades, AsyncMock())
        end = time.perf_counter()
        print(f"Detected {len(wash_trades)} wash trades in {end - start:.4f}s")
    except Exception as e:
        print(f"Caught expected error: {e}")

if __name__ == "__main__":
    asyncio.run(reproduce())
