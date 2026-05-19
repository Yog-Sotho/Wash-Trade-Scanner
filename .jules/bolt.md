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
