## 2025-05-15 - [Circular Trade Detection Optimization]
**Learning:** The initial implementation of circular trade detection used an O(N^2) approach within each SCC to find reverse trades, where N is the number of trades in the pool. This scales poorly as the number of trades increases.
**Action:** Use a hash map (defaultdict of lists) to pre-group trades by (sender, recipient) pairs. This reduces the inner search from O(N) to O(K), where K is the number of trades between a specific pair, resulting in >90% performance gain for large datasets.
## 2025-05-20 - [Feature Engineering Batch Optimization]
**Learning:** Feature calculation for large pools suffered from two major bottlenecks: N+1 database queries in `build_ml_features` (4 queries per trade) and an $O(N^2)$ loop for `circular_trade_ratio`.
**Action:** Implement batch pre-fetching of all relevant trade history for pool participants in a single query. Use the `bisect` module for efficient $O(\log N)$ window lookups in-memory, replacing both the N+1 queries and the $O(N^2)$ logic.

## 2025-05-24 - [SCC Mapping & Binary Search Lookup Optimization]
**Learning:** Nested loops over SCCs and trades create an $O(S \times T)$ bottleneck. Linear scans for time-window lookups are $O(K)$.
**Action:** Use an address-to-SCC index for $O(1)$ SCC membership checks and `bisect` for $O(\log K)$ time-window lookups. Removed redundant sorts where data is already ordered by the database query.
