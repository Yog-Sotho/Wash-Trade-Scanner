# Performance

A full audit of a real 10,000-trade pool, measured end to end before and after
Phase 5 of the [roadmap](project/roadmap_to_9.md). Every number here comes from
`python -m evaluation.perf` (`make perf`). The raw results, per stage and per
RPC method, are in [`evaluation/perf_results.json`](../evaluation/perf_results.json).

## Headline

10,000 trades of the Uniswap V3 USDC/WETH 0.05% pool: 3,122 blocks, 9,344
transactions. Node round trip 20 ms; `RPC_RATE_LIMIT` 100 per second (the
default).

| Version | First audit | Re-audit | RPC requests (first audit) | Data from the node | SQL statements | Wash trades found |
|---|---|---|---|---|---|---|
| Phase 4 (`main` at `3b360ec`) | **fails** | **fails** | all of them, then it fails | | | |
| Phase 4 with only the INSERT fix | 135.4 s | 134.6 s | 12,482 | 65.5 MB | 23 | 10 |
| **Phase 5** | **41.4 s** | **2.8 s** | **3,137** | 689.6 MB | 28 | 10 |
| Phase 5, `INGEST_FULL_BLOCKS=false` | 133.0 s | 2.7 s | 12,481 | 65.5 MB | 28 | 10 |

- Unpatched, Phase 4 could not audit this pool at all. It saved every trade in
  one INSERT, which binds 22 parameters per trade, and PostgreSQL allows 32,767
  per statement. Any range holding 1,490 or more swaps failed, after all its RPC
  work. The "before" row applies only the fix for that (chunked INSERTs), so
  that there is something to compare against.
- **First audit: 3.3× faster, 4× fewer requests.**
- **Re-audit: 47× faster, 10 requests instead of 12,482.** Stored swaps are not
  fetched again.
- **The trade-off is bytes.** One request per block brings all of the block's
  transactions, so the first audit downloads 10.5× more. Hosted nodes
  typically bill and rate-limit per request (or per compute unit), not per
  byte, so fewer and larger requests is the default. Where bandwidth costs more than requests,
  `INGEST_FULL_BLOCKS=false` restores the old request pattern, keeping
  everything else in this phase.
- **The result is unchanged.** After each version's run, the 10,000 stored
  trades were compared row by row: identical in every column (timestamps,
  senders, gas prices, USD values, labels), the same 10 wash trades.

## Where the time goes

First audit, per stage (seconds, SQL statements, RPC requests):

| Stage | Before | Phase 5 |
|---|---|---|
| ingest | 133.49 s · 10 SQL · 12,478 RPC | 39.76 s · 17 SQL · 3,134 RPC |
| load trades | 0.24 s · 2 SQL | 0.07 s · 1 SQL |
| funding-link clustering | 0.08 s · 2 SQL · 3 RPC | 0.06 s · 2 SQL · 2 RPC |
| heuristics | 0.77 s · 2 SQL | 0.77 s · 2 SQL |
| ML | 0.73 s · 2 SQL | 0.64 s · 1 SQL |
| labels, audit log, other | 0.09 s · 5 SQL · 1 RPC | 0.10 s · 5 SQL · 1 RPC |

Ingestion is the audit: 98.6% of it before, 96% after. The roadmap expected an
N+1 in SQL ("4 queries per trade"). Phase 2 had already removed that: the whole
audit was 23 statements. The N+1 was in RPC: one `eth_getTransactionByHash` per
transaction (9,344) on top of one `eth_getBlockByNumber` per block (3,122).

## What changed

**One request per block.** `eth_getBlockByNumber` with full transaction objects
returns the block time and every transaction's sender, recipient and gas price.
Before, the block was fetched for its time only, and then each swap's
transaction separately. A node that ignores the flag and lists only hashes is
handled: those transactions are fetched one by one, as before.

**Stored swaps are skipped before anything else.** For each batch, one query
(the hashes go as one array parameter) finds the swaps already stored with
their transaction's sender. Their logs are not decoded, their blocks not
fetched, their prices not looked up. Saving them again would have changed
nothing (an upsert that only fills in a missing sender), so the result is
identical. A swap stored without its sender is fetched again and completed.

**Streaming in 1,000-block batches.** Each batch is fetched, priced and saved
before the next, with up to four `eth_getLogs` fetched ahead. Memory no longer
grows with the range, and an audit that stops early keeps what it saved: the
next audit skips it.

**Failures fail.** A batch whose logs could not be fetched after `RPC_RETRIES`
was skipped with a log line, leaving a silent gap. A block that could not be
fetched gave its swaps the current time. Both now fail the audit
(`IngestionError`), after the batches before them were saved.

**Shutdown is honoured.** SIGINT/SIGTERM stops `wash-audit` before its next
batch instead of after the whole sync.

**Bounded concurrency.** Each chain's ingestor keeps at most `RPC_CONCURRENCY`
(8) requests in flight. `RPC_RATE_LIMIT` is now shared by everything in the
process using the chain (audits, monitors, clustering) instead of applying to
each separately, so two concurrent audits stay within the endpoint's budget.
At most `AUDIT_MAX_CONCURRENT` audits run at once across instances (Phase 4.4).
When a pool's factory is unknown, the candidate DEXes are synced concurrently.

After the change, the rate limit no longer sets the pace (79 requests per
second against a limit of 100). Each full block is about 220 KB, and fetching
and decoding it takes about 0.1 s. Where the node allows it, a higher
`RPC_CONCURRENCY` goes faster.

## Detectors

**Circular trading** looked for each trade's reverse leg by scanning every
trade of the pool: O(n²). It now looks them up in each address pair's sorted
times: O(n log n). The output is the same, trade for trade and in the same order:
`tests/test_circular_trading.py` compares it with the old implementation on 40
random pools. It never dominated on this pool, whose components are small, but a
pool where a few addresses trade with each other made it explode. For n trades
among three addresses:

| Trades | Before | Phase 5 |
|---|---|---|
| 2,500 | 2.87 s | 0.016 s |
| 5,000 | 11.70 s | 0.032 s |
| 10,000 | 45.83 s | 0.063 s |

The ML feature `circular_trade_ratio`, also named in the roadmap, had already
been made O(n log n) in Phase 2.

**ML** read the pool's trades twice (once for features, once to label them);
now it reads them once. The audit no longer loads every trade before detection
only to log their number. Stored address clusters are looked up in one query,
not one per cluster.

## Funding-link clustering

Clustering looks for value transfers between the pool's addresses. With
`trace_filter` that is one query per 20 addresses. Most hosted nodes lack it,
and then the only way is to fetch every block of the window, one request each.

- **Capped.** `ENTITY_SCAN_MAX_BLOCKS` is at most 50,000 (about a week of
  Ethereum blocks, ~8 minutes at the default rate limit). The cap was
  2,000,000, about 6 hours. The scan stays off by default.
- **Honest.** Every audit result carries `funding_clusters`. It records how the
  links were looked for (`trace_filter`, `block_scan`, `skipped` or `failed`),
  which blocks were wanted and which were examined, how many failed, and whether
  coverage is `complete`. A pool with no clusters can no longer be mistaken for
  a pool whose funding links were never looked for, as on the benchmark's node,
  which has no `trace_filter`.
- **Bounded and shared.** The scan runs `RPC_CONCURRENCY` requests at a time
  under the chain's rate limit and circuit breaker. Before, it was one at a
  time with a fixed 10 ms sleep, bypassing both. Each block is retried, and
  blocks that still fail are counted instead of silently skipped.

## Method

- **Pool.** The first 10,000 trades (blocks 26,300,001–26,304,335) of the
  `univ3_usdc_weth_005` fixture, real mainnet swaps recorded by the production
  ingestor ([benchmarks.md](benchmarks.md)).
- **Node.** A fake JSON-RPC node, in its own process so that serving costs the
  audit no CPU, replays them as ABI-encoded Uniswap V3 `Swap` logs. It answers
  every request after 20 ms, standing for a hosted node's round trip. Every
  block also carries 160 other transactions with 68–900 bytes of calldata,
  211 KB as full objects. That is an assumption standing for a typical mainnet
  block, stated so the byte figures can be scaled; request counts and the
  re-audit do not depend on it. `trace_filter` is unsupported, as on most
  hosted nodes. USD prices come from the node's CoinGecko stand-in.
- **Audit.** `AuditRunner`, exactly as `wash-audit` runs it, against
  PostgreSQL 16 on the same machine, with heuristics and an Isolation Forest
  fitted on the pool. First on an empty database, then again.
- **Counts.** SQL statements via SQLAlchemy's `before_cursor_execute`; RPC
  requests and response bytes as counted by the node; wall time per stage.
- **Machine.** 4 cores, Python 3.11. Repeated runs: Phase 5 first audit
  40.7–41.4 s (four runs), before 135.0–135.4 s (three runs).

To reproduce, point `DATABASE_*` at a disposable database (its name must
contain `test`, `perf` or `bench`: every table in it is dropped) and run:

```bash
make perf                                  # prints the table per stage
python -m evaluation.perf --out run.json   # and keeps the full results
INGEST_FULL_BLOCKS=false make perf         # the bytes-saving mode
```

The "before" rows were measured the same way from `main` at `3b360ec` with
only the chunked INSERT applied.
