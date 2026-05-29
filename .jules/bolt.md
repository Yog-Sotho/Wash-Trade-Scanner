## 2024-05-24 - [Initial Assessment]
**Learning:** The application uses sequential RPC calls for fetching block timestamps and logs, which is a major bottleneck during historical data ingestion.
**Action:** Use `asyncio.gather` with the existing `RateLimiter` to parallelize RPC calls in `core/ingestor.py`.
