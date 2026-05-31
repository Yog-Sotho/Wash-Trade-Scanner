## 2025-05-15 - [Circular Trade Detection Optimization]
**Learning:** The initial implementation of circular trade detection used an O(N^2) approach within each SCC to find reverse trades, where N is the number of trades in the pool. This scales poorly as the number of trades increases.
**Action:** Use a hash map (defaultdict of lists) to pre-group trades by (sender, recipient) pairs. This reduces the inner search from O(N) to O(K), where K is the number of trades between a specific pair, resulting in >90% performance gain for large datasets.
## 2025-05-20 - [Feature Engineering Batch Optimization]
**Learning:** Feature calculation for large pools suffered from two major bottlenecks: N+1 database queries in `build_ml_features` (4 queries per trade) and an $O(N^2)$ loop for `circular_trade_ratio`.
**Action:** Implement batch pre-fetching of all relevant trade history for pool participants in a single query. Use the `bisect` module for efficient $O(\log N)$ window lookups in-memory, replacing both the N+1 queries and the $O(N^2)$ logic.

## 2025-05-24 - [SCC Mapping & Binary Search Lookup Optimization]
**Learning:** Nested loops over SCCs and trades create an $O(S \times T)$ bottleneck. Linear scans for time-window lookups are $O(K)$.
**Action:** Use an address-to-SCC index for $O(1)$ SCC membership checks and `bisect` for $O(\log K)$ time-window lookups. Removed redundant sorts where data is already ordered by the database query.
## 2025-05-25 - [NumPy & Query Elimination in Feature Engineering]
**Learning:** Pandas DataFrame creation and iterrows() inside tight loops (like feature engineering) introduce significant overhead (O(N) with high constant). Redundant database queries in nested function calls multiply this latency.
**Action:** Replace Pandas operations with NumPy arrays and direct loops for statistical calculations. Implement "pass-through" parameters for pre-fetched data to eliminate redundant queries. Resulted in ~20x speedup for pool features.
## 2025-05-30 - [Redundant Query Elimination & Storage Layer Abstraction]
**Learning:** The detection pipeline (heuristics + ML) was fetching the same trade data multiple times. Refactoring to pass pre-fetched data through the stack yielded ~40% speedup. However, modifying core storage methods' default behavior (e.g., sort order) introduces regressions for existing callers (e.g., UI/APIs expecting latest trades).
**Action:** Always prefer optional "pass-through" parameters for performance optimizations. If storage utility methods need modification, use optional parameters with safe defaults to maintain backward compatibility. Keep SQL logic within the storage layer rather than duplicating it in detectors.
## 2025-06-05 - [ML Pipeline & Feature Engineering Micro-optimizations]
**Learning:** Repeated `from scipy.special import expit, logit` calls inside hot functions (inference/training) and `df.iterrows()` in the ML training loop introduced avoidable latency and high constant overhead. Additionally, using `max(list, key=lambda t: t.timestamp)` on already-sorted lists is an $O(N)$ tax that should be $O(1)$.
**Action:** Hoist library-level imports to the top level. Replace `iterrows()` with list comprehensions or vectorized operations. Leverage the known sort order of trade history to use direct indexing (`[-1]`) for latest-state lookups.

## 2026-05-21 - [Batch ML Feature Explainability]
**Learning:** The initial implementation of `explain_prediction` used a nested loop that made $2 \times F$ calls to the prediction model for every explanation (where $F$ is the number of features). For each feature, it would copy the *entire* dataset and predict on it, leading to $O(F \times N)$ complexity and extreme memory pressure.
**Action:** Batch all perturbed rows into a single $F$-row prediction call. Move the baseline prediction outside the loop. Reduces complexity to $O(F + N)$ and results in ~4x speedup for typical datasets.

## 2026-05-26 - [Audit Pipeline Parallelization]
**Learning:** Heuristic and ML detection phases are independent but were executed sequentially, making the audit latency the sum of both. Parallelizing them with `asyncio.gather` reduces the bottleneck to the slowest individual task. Using `asyncio.sleep(0, result=...)` is an effective way to maintain consistent result structures for unpacking even when some tasks are conditionally disabled.
**Action:** Use `asyncio.gather` for independent detection modules. Ensure stable result unpacking by returning empty default structures for disabled paths.

## 2026-06-10 - [Entity Clustering Query Batching]
**Learning:** The `cluster_addresses` method suffered from an N+1 query pattern where it fetched existing cluster records one by one inside a loop over connected components. Additionally, fetching senders and recipients in separate queries doubled database round-trips.
**Action:** Use SQLAlchemy `union` to consolidate address extraction into a single query. Implement batch pre-fetching of all existing clusters for a pool using a `LIKE` pattern. Reduces database calls from O(N) to O(1) and improves execution time by ~90% for typical pools.
## 2024-05-24 - [Initial Assessment]
**Learning:** The application uses sequential RPC calls for fetching block timestamps and logs, which is a major bottleneck during historical data ingestion.
**Action:** Use `asyncio.gather` with the existing `RateLimiter` to parallelize RPC calls in `core/ingestor.py`.

## 2026-06-15 - [Heuristic Detection Vectorization & Parallelization]
**Learning:** Manual loops for statistical calculations (MAD/IQR/CV) in heuristics create a significant bottleneck as trade volume grows. Parallelizing independent detection methods (self-trading, circular, bot, anomaly) further reduces latency by leveraging concurrent execution.
**Action:** Use NumPy for vectorized statistical calculations in all detector classes. Use `asyncio.gather` to run independent heuristic detectors concurrently in `run_all_heuristics`.
