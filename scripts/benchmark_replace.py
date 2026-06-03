
import time
from datetime import datetime, timedelta

def benchmark():
    num_iterations = 100000
    base_time = datetime(2024, 1, 1, 10, 30, 45)
    times = [base_time + timedelta(seconds=i) for i in range(num_iterations)]
    bucket_minutes = 60

    # Current approach: replace()
    start = time.perf_counter()
    buckets_1 = []
    for t in times:
        bucket = t.replace(
            minute=(t.minute // bucket_minutes) * bucket_minutes,
            second=0,
            microsecond=0,
        )
        buckets_1.append(bucket)
    end = time.perf_counter()
    print(f"replace() approach: {end - start:.4f}s")

    # Optimization: bucket_cache
    start = time.perf_counter()
    buckets_2 = []
    bucket_cache = {}
    for t in times:
        # Use minute-level resolution for cache key
        cache_key = (t.year, t.month, t.day, t.hour, (t.minute // bucket_minutes))
        if cache_key not in bucket_cache:
            bucket_cache[cache_key] = t.replace(
                minute=(t.minute // bucket_minutes) * bucket_minutes,
                second=0,
                microsecond=0,
            )
        buckets_2.append(bucket_cache[cache_key])
    end = time.perf_counter()
    print(f"bucket_cache approach: {end - start:.4f}s")

    # Verify
    assert buckets_1 == buckets_2

if __name__ == "__main__":
    benchmark()
