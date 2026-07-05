
import asyncio
import time
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import AsyncMock
from core.heuristics import HeuristicDetector
from models.schemas import SwapTrade
from collections import defaultdict
from config.settings import settings

# Simulated old logic (with NameError fixed and slow loop)
async def old_detect_high_frequency_bot_logic(trades, bot_allowlist):
    wash_trades = []
    sender_groups = defaultdict(list)
    for trade in trades:
        sender_groups[trade.sender].append(trade)

    for sender, sender_trades in sender_groups.items():
        if sender.lower() in bot_allowlist:
            continue
        count_threshold = settings.BOT_TRADE_COUNT_THRESHOLD
        time_threshold = settings.BOT_TRADE_TIME_THRESHOLD_SECONDS
        cv_threshold = settings.BOT_VOLUME_CV_THRESHOLD
        if len(sender_trades) < count_threshold:
            continue

        inter_trade_times = []
        volumes = []
        for i in range(1, len(sender_trades)):
            delta = (sender_trades[i].block_timestamp - sender_trades[i-1].block_timestamp).total_seconds()
            inter_trade_times.append(delta)
            volumes.append(sender_trades[i].volume_usd or 0.0)

        if not inter_trade_times:
            continue

        avg_time = sum(inter_trade_times) / len(inter_trade_times)
        mean_vol = sum(volumes) / len(volumes) if volumes else 0
        volume_variance = sum((v - mean_vol)**2 for v in volumes) / len(volumes) if volumes else 0
        volume_std = volume_variance**0.5
        volume_cv = volume_std / (mean_vol + 1e-9)

        if avg_time < time_threshold and volume_cv < cv_threshold:
            for trade in sender_trades:
                trade.is_wash_trade = True
                trade.wash_trade_score = 0.8
                trade.detection_method = "high_frequency_bot"
                wash_trades.append(trade)
    return wash_trades

async def benchmark():
    detector = HeuristicDetector()
    base_time = datetime(2024, 1, 1, 12, 0, 0)

    # Increase scale to see better differences
    num_senders = 100
    trades_per_sender = 1000
    trades = []
    for s in range(num_senders):
        sender_addr = f"0xSender{s}"
        for i in range(trades_per_sender):
            trades.append(SwapTrade(
                id=s*trades_per_sender + i,
                chain_id=1,
                pool_address="0xpool",
                sender=sender_addr,
                recipient="0xBob",
                volume_usd=100.0 + (i % 10),
                block_timestamp=base_time + timedelta(seconds=s*1000 + i*10),
                is_wash_trade=False
            ))

    print(f"Benchmarking with {len(trades)} trades...")

    # Measure old logic
    start_old = time.time()
    await old_detect_high_frequency_bot_logic(trades, detector.bot_allowlist)
    end_old = time.time()
    old_duration = end_old - start_old
    print(f"Old logic duration: {old_duration:.4f}s")

    # Measure new optimized logic
    start_new = time.time()
    await detector.detect_high_frequency_bot(trades, AsyncMock())
    end_new = time.time()
    new_duration = end_new - start_new
    print(f"New logic duration: {new_duration:.4f}s")

    speedup = (old_duration / new_duration) if new_duration > 0 else 0
    print(f"Speedup: {speedup:.2f}x")

if __name__ == "__main__":
    asyncio.run(benchmark())
