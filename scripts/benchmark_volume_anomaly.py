
import asyncio
import time
import math
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np

# Mock settings
class MockSettings:
    VOLUME_ANOMALY_MIN_TRADES = 5
    VOLUME_ANOMALY_BUCKET_MINUTES = 60
    VOLUME_ANOMALY_METHOD = "mad"
    VOLUME_ANOMALY_THRESHOLD = 3.5
    SUSPICIOUS_ACTIVITY_THRESHOLD = 0.8
    bot_allowlist_set = set()

settings = MockSettings()

class RobustAnomalyDetectorOriginal:
    def __init__(self, method: str = "mad"):
        self.method = method
        self.median = None
        self.mad = None
        self._fitted = False

    def fit(self, volumes):
        if not volumes: return
        log_volumes = [math.log1p(max(v, 0.0)) for v in volumes]
        sorted_vals = sorted(log_volumes)
        n = len(sorted_vals)
        self.median = sorted_vals[n // 2] if n % 2 else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
        self.mad = sorted([abs(v - self.median) for v in log_volumes])[n // 2]
        self._fitted = True

    def score(self, volume):
        if not self._fitted: return 0.0
        log_vol = math.log1p(max(volume, 0.0))
        if self.mad == 0: return 0.0
        modified_z = 0.6745 * (log_vol - self.median) / self.mad
        return abs(modified_z)

    def is_anomaly(self, volume, threshold=3.5):
        return self.score(volume) > threshold

class RobustAnomalyDetectorOptimized:
    def __init__(self, method: str = "mad"):
        self.method = method
        self.median = None
        self.mad = None
        self._fitted = False

    def fit(self, volumes):
        if len(volumes) == 0: return
        log_volumes = np.log1p(np.maximum(volumes, 0.0))
        if self.method == "mad":
            self.median = np.median(log_volumes)
            self.mad = np.median(np.abs(log_volumes - self.median))
        self._fitted = True

    def score_batch(self, volumes):
        if not self._fitted: return np.zeros(len(volumes))
        log_vols = np.log1p(np.maximum(volumes, 0.0))
        if self.mad == 0: return np.zeros(len(volumes))
        return np.abs(0.6745 * (log_vols - self.median) / self.mad)

class MockTrade:
    def __init__(self, id, pool, volume, timestamp):
        self.id = id
        self.pool_address = pool
        self.volume_usd = volume
        self.block_timestamp = timestamp
        self.is_wash_trade = False
        self.wash_trade_score = 0.0
        self.detection_method = None

async def detect_volume_anomaly_original(trades):
    # Reset trades
    for t in trades:
        t.is_wash_trade = False
        t.wash_trade_score = 0.0
        t.detection_method = None

    wash_trades = []
    min_trades = settings.VOLUME_ANOMALY_MIN_TRADES
    bucket_minutes = settings.VOLUME_ANOMALY_BUCKET_MINUTES
    pool_bucket_groups = defaultdict(list)

    for trade in trades:
        bucket = trade.block_timestamp.replace(
            minute=(trade.block_timestamp.minute // bucket_minutes) * bucket_minutes,
            second=0,
            microsecond=0,
        )
        key = (trade.pool_address, bucket)
        pool_bucket_groups[key].append(trade)

    for (pool, hour), bucket_trades in pool_bucket_groups.items():
        if len(bucket_trades) < min_trades:
            continue
        volumes = [t.volume_usd or 0.0 for t in bucket_trades]
        detector = RobustAnomalyDetectorOriginal(method=settings.VOLUME_ANOMALY_METHOD)
        detector.fit(volumes)
        threshold = settings.VOLUME_ANOMALY_THRESHOLD
        for trade in bucket_trades:
            vol = trade.volume_usd or 0.0
            score = detector.score(vol)
            if detector.is_anomaly(vol, threshold):
                trade.is_wash_trade = True
                trade.wash_trade_score = min(0.7 + (score - threshold) * 0.05, 1.0)
                trade.detection_method = "volume_anomaly"
                wash_trades.append(trade)
    return wash_trades

async def detect_volume_anomaly_optimized(trades):
    # Reset trades
    for t in trades:
        t.is_wash_trade = False
        t.wash_trade_score = 0.0
        t.detection_method = None

    wash_trades = []
    min_trades = settings.VOLUME_ANOMALY_MIN_TRADES
    if len(trades) < min_trades:
        return wash_trades

    bucket_minutes = settings.VOLUME_ANOMALY_BUCKET_MINUTES
    pool_bucket_groups = defaultdict(list)

    # Optimization 1: Cache bucket calculations
    bucket_cache = {}

    for trade in trades:
        ts = trade.block_timestamp
        if ts not in bucket_cache:
            bucket_cache[ts] = ts.replace(
                minute=(ts.minute // bucket_minutes) * bucket_minutes,
                second=0,
                microsecond=0,
            )
        bucket = bucket_cache[ts]
        key = (trade.pool_address, bucket)
        pool_bucket_groups[key].append(trade)

    threshold = settings.VOLUME_ANOMALY_THRESHOLD

    for (pool, bucket), bucket_trades in pool_bucket_groups.items():
        if len(bucket_trades) < min_trades:
            continue

        # Optimization 2: Use NumPy for batch processing
        volumes = np.array([t.volume_usd or 0.0 for t in bucket_trades])

        detector = RobustAnomalyDetectorOptimized(method=settings.VOLUME_ANOMALY_METHOD)
        detector.fit(volumes)

        scores = detector.score_batch(volumes)

        for i, score in enumerate(scores):
            if score > threshold:
                trade = bucket_trades[i]
                trade.is_wash_trade = True
                trade.wash_trade_score = min(0.7 + (score - threshold) * 0.05, 1.0)
                trade.detection_method = "volume_anomaly"
                wash_trades.append(trade)

    return wash_trades

def generate_trades(n):
    base_time = datetime(2024, 1, 1)
    trades = []
    for i in range(n):
        # 24 buckets of 1 hour
        bucket_idx = i % 24
        # Add some variation within the bucket
        ts = base_time + timedelta(hours=bucket_idx, minutes=(i // 24) % 60)
        vol = 1000.0 + (i % 10) * 10 # Some variation to avoid mad=0
        if i % 1000 == 0: # Anomaly
            vol = 50000.0
        trades.append(MockTrade(i, "0xpool", vol, ts))
    return trades

async def main():
    n_trades = 100000
    print(f"Benchmarking with {n_trades} trades...")
    trades = generate_trades(n_trades)

    # Original
    start = time.perf_counter()
    res_orig = await detect_volume_anomaly_original(trades)
    duration_orig = time.perf_counter() - start
    print(f"Original implementation: {duration_orig:.4f} seconds, detected: {len(res_orig)}")

    # Optimized
    start = time.perf_counter()
    res_opt = await detect_volume_anomaly_optimized(trades)
    duration_opt = time.perf_counter() - start
    print(f"Optimized implementation: {duration_opt:.4f} seconds, detected: {len(res_opt)}")

    print(f"Speedup: {duration_orig / duration_opt:.2f}x")
    assert len(res_orig) == len(res_opt)

if __name__ == "__main__":
    asyncio.run(main())
