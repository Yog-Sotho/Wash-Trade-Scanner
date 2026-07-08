
import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np


@dataclass
class MockTrade:
    sender: str
    block_timestamp: datetime
    volume_usd: float
    is_wash_trade: bool = False
    wash_trade_score: float = 0.0
    detection_method: str = ""

def baseline_detect_high_frequency_bot(
    trades: list[MockTrade],
    bot_allowlist: set[str],
    count_threshold: int,
    time_threshold: float,
    cv_threshold: float
) -> list[MockTrade]:
    wash_trades = []
    sender_groups = defaultdict(list)
    for trade in trades:
        sender_groups[trade.sender].append(trade)

    for sender, sender_trades in sender_groups.items():
        if sender.lower() in bot_allowlist:
            continue
        if len(sender_trades) < count_threshold:
            continue

        inter_trade_times = []
        volumes = []
        for i in range(1, len(sender_trades)):
            delta = (
                sender_trades[i].block_timestamp - sender_trades[i - 1].block_timestamp
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

def optimized_detect_high_frequency_bot(
    trades: list[MockTrade],
    bot_allowlist: set[str],
    count_threshold: int,
    time_threshold: float,
    cv_threshold: float
) -> list[MockTrade]:
    wash_trades = []
    sender_groups = defaultdict(list)
    for trade in trades:
        sender_groups[trade.sender].append(trade)

    for sender, sender_trades in sender_groups.items():
        if sender.lower() in bot_allowlist:
            continue
        if len(sender_trades) < count_threshold:
            continue

        # Vectorized implementation
        timestamps = np.array([t.block_timestamp.timestamp() for t in sender_trades])
        # np.diff gives us the inter-trade times
        inter_trade_times = np.diff(timestamps)

        # Volumes (skipping the first one as in baseline)
        volumes = np.array([t.volume_usd or 0.0 for t in sender_trades[1:]])

        avg_time = np.mean(inter_trade_times)
        mean_vol = np.mean(volumes)
        volume_std = np.std(volumes, ddof=0) # Population std to match baseline
        volume_cv = volume_std / (mean_vol + 1e-9)

        if avg_time < time_threshold and volume_cv < cv_threshold:
            for trade in sender_trades:
                trade.is_wash_trade = True
                trade.wash_trade_score = 0.8
                trade.detection_method = "high_frequency_bot"
                wash_trades.append(trade)
    return wash_trades

async def run_benchmark():
    num_senders = 1000
    trades_per_sender = 1000
    base_time = datetime(2024, 1, 1, 10, 0, 0)

    print(f"Generating {num_senders * trades_per_sender} trades...")
    trades = []
    for s in range(num_senders):
        sender_addr = f"0xsender_{s}"
        for i in range(trades_per_sender):
            trades.append(MockTrade(
                sender=sender_addr,
                block_timestamp=base_time + timedelta(seconds=i * 2),
                volume_usd=100.0 + (i % 10)
            ))

    bot_allowlist = set()
    count_threshold = 10
    time_threshold = 5.0
    cv_threshold = 0.5

    print("--- Benchmarking detect_high_frequency_bot ---")

    # Warmup
    baseline_detect_high_frequency_bot(trades[:100], bot_allowlist, count_threshold, time_threshold, cv_threshold)
    optimized_detect_high_frequency_bot(trades[:100], bot_allowlist, count_threshold, time_threshold, cv_threshold)

    start_time = time.perf_counter()
    baseline_detect_high_frequency_bot(trades, bot_allowlist, count_threshold, time_threshold, cv_threshold)
    baseline_time = time.perf_counter() - start_time
    print(f"Baseline (fixed loop) time: {baseline_time:.4f} seconds")

    start_time = time.perf_counter()
    optimized_detect_high_frequency_bot(trades, bot_allowlist, count_threshold, time_threshold, cv_threshold)
    optimized_time = time.perf_counter() - start_time
    print(f"Optimized (vectorized) time: {optimized_time:.4f} seconds")

    if optimized_time > 0:
        speedup = baseline_time / optimized_time
        print(f"Speedup: {speedup:.2f}x")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
