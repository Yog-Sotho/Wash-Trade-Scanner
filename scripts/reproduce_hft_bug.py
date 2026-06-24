import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
import sys
import os
import numpy as np

# Add current directory to sys.path
sys.path.append(os.getcwd())

from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade

async def benchmark_hft():
    detector = HeuristicDetector()
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    num_trades = 10000
    trades = [
        SwapTrade(id=i, sender="0xAlice", recipient="0xBob", volume_usd=100.0,
                  block_timestamp=base_time + timedelta(seconds=i))
        for i in range(num_trades)
    ]

    print(f"--- High-Frequency Bot Detector Benchmark with {num_trades} trades ---")

    start_time = time.perf_counter()
    try:
        await detector.detect_high_frequency_bot(trades, AsyncMock())
        end_time = time.perf_counter()
        print(f"Execution successful in {end_time - start_time:.4f} seconds")
    except NameError as e:
        print(f"CAUGHT EXPECTED BUG: {e}")
    except Exception as e:
        print(f"Caught unexpected error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(benchmark_hft())
