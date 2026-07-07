
import os
os.environ["DATABASE_HOST"] = "localhost"
os.environ["DATABASE_NAME"] = "test"
os.environ["DATABASE_USER"] = "test"
os.environ["DATABASE_PASSWORD"] = "testtest"

import asyncio
import time
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade

# A reference implementation of the loop-based logic (fixed)
async def detect_high_frequency_bot_loop(detector, trades):
    wash_trades = []
    sender_groups = {}
    for t in trades:
        if t.sender not in sender_groups:
            sender_groups[t.sender] = []
        sender_groups[t.sender].append(t)

    for sender, sender_trades in sender_groups.items():
        if sender.lower() in detector.bot_allowlist:
            continue

        from config.settings import settings
        count_threshold = settings.BOT_TRADE_COUNT_THRESHOLD
        time_threshold = settings.BOT_TRADE_TIME_THRESHOLD_SECONDS
        cv_threshold = settings.BOT_VOLUME_CV_THRESHOLD

        if len(sender_trades) < count_threshold:
            continue

        inter_trade_times = []
        volumes = []
        for i in range(1, len(sender_trades)):
            delta = (
                sender_trades[i].block_timestamp
                - sender_trades[i - 1].block_timestamp
            ).total_seconds()
            inter_trade_times.append(delta)
            volumes.append(sender_trades[i].volume_usd or 0.0)

        if not inter_trade_times:
            continue

        avg_time = sum(inter_trade_times) / len(inter_trade_times)
        mean_vol = sum(volumes) / len(volumes) if volumes else 0
        volume_variance = (
            sum((v - mean_vol) ** 2 for v in volumes) / len(volumes)
            if volumes
            else 0
        )
        volume_std = volume_variance**0.5
        volume_cv = volume_std / (mean_vol + 1e-9)

        if avg_time < time_threshold and volume_cv < cv_threshold:
            for trade in sender_trades:
                trade.is_wash_trade = True
                trade.wash_trade_score = 0.8
                trade.detection_method = "high_frequency_bot"
                wash_trades.append(trade)
    return wash_trades

async def run_benchmark():
    detector = HeuristicDetector()
    num_trades = 1000000 # Increase to 1,000,000 to better see the difference
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    # Generate trades for a few "bots"
    trades = []
    for bot_id in range(10):
        sender = f"0xBot_{bot_id}"
        for i in range(num_trades // 10):
            trades.append(SwapTrade(
                id=bot_id * num_trades + i,
                chain_id=1,
                pool_address="0xpool",
                sender=sender,
                recipient="0xRecipient",
                volume_usd=100.0 + (i % 10), # Slight variation
                block_timestamp=base_time + timedelta(seconds=i),
                is_wash_trade=False
            ))

    print(f"--- High-Frequency Bot Detection Benchmark ({len(trades)} trades) ---")

    # Benchmark loop-based (fixed)
    start = time.perf_counter()
    res_loop = await detect_high_frequency_bot_loop(detector, trades)
    loop_time = time.perf_counter() - start
    print(f"Loop-based time (fixed): {loop_time:.4f}s (Detected {len(res_loop)})")

    # Benchmark current (NumPy optimized)
    start = time.perf_counter()
    res_current = await detector.detect_high_frequency_bot(trades, AsyncMock())
    current_time = time.perf_counter() - start
    print(f"Current implementation (NumPy) time: {current_time:.4f}s (Detected {len(res_current)})")

    speedup = loop_time / current_time
    print(f"Speedup: {speedup:.2f}x")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
