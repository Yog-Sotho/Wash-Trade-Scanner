# Roadmap: 5.5 → 9.0+

Derived from `codebase_audit.md` (commit `f99fe4d`, 2026-10-31). Every item has a
**verifiable** acceptance criterion — a command that passes or a number that is published.
No item is "done" because it was written; it is done because it was measured.

| Dimension | Now | Target | Gap closed by |
|---|---|---|---|
| Detection Efficacy | 3 | 9 | P1, P2 |
| Observability | 3 | 9 | P4 |
| Build & Types | 5 | 9 | P3 |
| Dead Code | 5 | 9 | P1, P3 |
| Code Principles | 6 | 9 | P1, P2 |
| Dependencies | 6 | 9 | P3 |
| Lifecycle | 6 | 9 | P3, P4 |
| Security | 7 | 9 | P3, P4 |
| Code Quality | 7 | 9 | P2, P5 |
| Concurrency | 7 | 9 | P5 |
| **Overall** | **5.5** | **9.0** | |

---

## Phase 1 — Make it actually run (unblocks everything)

The product cannot audit a single pool today. Nothing else matters until this is false.

### 1.1 Resolve RPC endpoints from configuration
- `get_chain_config()` overlays `settings.rpc_urls[chain_id]` onto the static chain record;
  the hardcoded value becomes a fallback, never an override.
- `MultiChainIngestor.initialize()` stops eagerly connecting 23 chains — connect lazily for
  the requested chain, and isolate per-chain failures.
- Placeholder detection stays, but fires only for the chain actually being audited.

**Acceptance:** `ETH_RPC_URL=<real> wash-audit --chain-id 1 --pool <addr>` reaches the
ingestion stage. Regression test asserts `get_chain_config(1)["rpc_url"]` equals the env
value, and that a bad chain-56 URL does not break a chain-1 audit.

### 1.2 Price oracle (`core/pricing.py`)
- **Reference-leg valuation:** when one side of a swap is a known stablecoin or WETH, value
  the swap from that side — the method Allium uses for DEX pricing.
- **Pool-reserve fallback:** derive the implied price from reserves/`slot0` at the swap's
  block for pairs with no USD leg.
- **Dust filter:** ignore swaps below a configurable floor (Allium uses $1) so micro-swaps
  don't distort derived prices.
- Wire `COINGECKO_API_KEY`/`COINGECKO_API_URL` as the optional external fallback they were
  always declared to be. Cache per (token, block-bucket).
- Populate `amount_in_usd`, `amount_out_usd`, `volume_usd` at ingestion; add a backfill
  command for existing rows.

**Acceptance:** closed-cluster and volume-anomaly detectors fire on a priced fixture
(currently impossible); `wash_volume_usd` is non-zero in an end-to-end audit; unit tests
cover stablecoin-leg, WETH-leg, reserve-derived and unpriceable paths.

### 1.3 Integration tests at the broken boundary
- Postgres-backed integration job (CI service container) exercising real schema, real
  upserts, real aggregate queries.
- End-to-end audit test against **recorded RPC fixtures** (VCR-style cassettes) — no
  network in CI, but the real ingestion→decode→price→detect→report path.

**Acceptance:** an integration test fails if either Phase-1 defect is reintroduced. This is
the specific gap that let both CRITICAL bugs ship under 90% coverage.

### Phase 1 — status: done (2026-10-31)

Acceptance met: `wash-audit` completes against live Ethereum mainnet, a real
PostgreSQL and real CoinGecko (175 swaps on the Uniswap V3 USDC/WETH pool,
all priced, ~14s). Making the pipeline actually run exposed **five more
defects beyond the two planned**, each of which also blocked every audit or a
whole product surface; all seven are now guarded by integration tests verified
to fail when the defect is reintroduced:

| Defect | Effect |
|---|---|
| `ETH_RPC_URL` ignored; all 23 chains connected eagerly | no audit could start on any chain |
| USD fields never written | every USD figure $0; closed-cluster could never fire |
| Duplicate index in `address_clusters` | `create_all` failed: no entry point could start on a real DB |
| Tx hash stored as `HexBytes` | asyncpg rejected every insert: no swap could be saved |
| `datetime` in the audit log's JSON column | every audit crashed at its final step |
| Pool address stored checksummed, queried lowercase; lowercase rejected by `eth_getLogs` | API/panel pool views always empty; live monitor never ingested a swap |
| Unbounded 2M-block entity-clustering scan without `trace_filter` | audits never finished on public nodes |

Also fixed: Uniswap V3 legs were inverted (every V3 swap recorded in the wrong
direction); 3 of 23 default RPC endpoints were dead. Plan corrections: the
"pool-reserve fallback" and "dust filter" above were dropped — the swap itself
already gives the exchange rate, and a dust filter only applies to deriving
prices from swaps, which this design does not do.

---

## Phase 2 — Prove detection quality (the SOTA core)

Today there is no precision, recall, F1, AUC, split or cross-validation anywhere. A
detection system without measured accuracy is an assertion, not a product.

**First measured target (found by the Phase 1 live run).** On the most liquid
USDC/WETH pool on Ethereum, the audit flagged 156 of 175 swaps (98.5% of volume)
as wash, all via `self_trading`. Every one of the 35 distinct addresses it
flagged is a contract (routers, aggregators, bots) — 0 are EOAs. The detector
compares the Swap event's `sender` and `recipient`, which for router-mediated
swaps are the router on both sides: that is ordinary AMM usage, not wash
trading, yet it scores 1.0. The economic actor is the transaction's `from`.
This is the first false-positive class the evaluation harness must measure and
the detector stack must fix — deliberately not changed in Phase 1, because a
methodology change without measurement is exactly what this phase exists to stop.

### 2.1 Labeled evaluation harness (`eval/`)
Ground truth for wash trading is genuinely scarce, so use a two-track approach — the
standard compromise in the literature:

- **Track A — synthetic injection.** Take real pool history, inject wash patterns with
  known labels (round-trip rings, position-neutral cycles, closed clusters, repeated-size
  bots) at controlled prevalence. Gives exact labels and tunable difficulty.
- **Track B — simulation.** Port the approach of
  [`LhyJason/WashTradingSimulation`](https://github.com/LhyJason/WashTradingSimulation),
  which generates ground-truth-labelled wash trading and scores network-based detectors
  against the known labels.
- **Track C — real spot-checks.** A small hand-labeled set from documented cases, to test
  whether synthetic performance transfers.

### 2.2 Published metrics
- Per-detector **precision / recall / F1 / PR-AUC**, plus false-positive rate on
  known-clean pools (the number that matters for a forensic tool).
- A `make eval` target and a committed `docs/benchmarks.md` regenerated from real runs.
- CI regression gate: a detector whose F1 drops more than a set delta fails the build.

### 2.3 Calibration and composite scoring
- Implement the composite score `HEURISTIC_WEIGHTS` already describes but never applies.
- Derive thresholds and weights **from the harness**, not from intuition.
- ML: train/test split + cross-validation in `train_model.py`; persist metrics inside the
  model artifact; refuse to save a model that regresses below a floor.

**Acceptance:** `docs/benchmarks.md` contains real measured numbers per detector; CI fails
on regression.

### Phase 2 — status: done (2026-11-01)

Acceptance met: [`docs/benchmarks.md`](../benchmarks.md) is generated by
`wash-eval` from real measurements and reproduces byte-for-byte; the CI job
`detection-quality` fails on a regression (verified by reintroducing two defects:
reverting trader identity raised `round_trip`'s clean-pool FPR from 0.07% to
1.25%, and changing a default without recalibrating dropped composite recall
from 0.487 to 0.276; both runs exited 1).

| Audit decision | Precision | Recall | F1 | PR-AUC | FPR on 27,385 honest trades |
|---|---|---|---|---|---|
| Phase 1 (`b513973`) | 0.001 | 0.274 | 0.001 | 0.000 | 78.93% |
| Phase 2 | 0.423 | 0.487 | 0.453 | 0.253 | 0.10% |

What was built, against the plan:

- **2.1** Track A as planned: three of the most liquid Ethereum pools recorded
  through the production ingestor, five labelled patterns from primary sources.
  **Track B was not ported**: inspection showed `LhyJason/WashTradingSimulation`
  simulates a limit-order book with explicit counterparties, which AMM swaps do
  not have. Its threshold calibration to a control false-positive rate was
  adopted instead (ML threshold, `ML_TARGET_FPR`). **Track C**: the NexFundAI
  sting token's market, linked on chain via the funder Kaiko identified. Its
  funder-linked swaps turned out to be one-directional sells, so it has no
  trade-level labels and is reported at pool level.
- **2.2** as planned: per-detector precision / recall / F1 / PR-AUC and
  clean-pool FPR, `make eval`, CI gate.
- **2.3** `HEURISTIC_WEIGHTS` was not implemented as a weighted sum: with the
  configured weights (max 0.20) and threshold (0.8) no single detector could
  ever label a trade. It was replaced by Bayes scoring from each detector's
  measured TPR and FPR upper bound (`config/detector_calibration.json`).
  Parameters were selected on development seeds (`python -m evaluation.tune`).
  ML: chronological holdout, leave-one-pool-out CV, measured rates stored in
  the model, save refused below 2× random PR-AUC.
- Found on the way: the Phase 1 "first measured target" was worse than
  estimated (`self_trading` flagged 70–79% of honest trades in all three
  pools); swaps were ingested once per DEX sharing an event signature;
  re-audits never cleared labels; the ML detection threshold (0.8) was
  unreachable (expit(-decision) ≤ expit(1) ≈ 0.73 for an Isolation Forest).

Open, measured gaps (the next detection work, in order of evidence):

1. **Multi-wallet wash without funding data**: `unlinked_ring` recall 0, and the
   audit labels nothing in the NexFundAI market, where Kaiko attributes the
   wash trading to many co-funded wallets (148 of 485 first funded in the same
   block). Next step: record funding transfers for the fixtures and add a
   co-funding detector, measured the same way.
2. **Slow round trips** (legs 26–300 blocks apart): composite recall 0.30.
3. **Isolation Forest**: no better than random ranking. In every
   leave-one-pool-out fold its PR-AUC equals the prevalence (0.006, 0.001,
   0.001), and its recall at the calibrated threshold is 0 in every fold:
   wash campaigns are not outliers in bot-dominated pools.
4. Clean-pool rates are upper bounds: the trades the audit flags there are
   address-level round trips of matching size (listed in the benchmark), some
   plausibly real wash or MEV.

---

## Phase 3 — Build integrity, supply chain, schema

### 3.1 Un-mask the gates
Remove `|| true` from mypy, bandit and pip-audit in CI; fix the 5 live mypy errors in
`core/storage.py`. **Acceptance:** a reintroduced type error turns CI red.

### 3.2 Reproducible builds
Lockfile with hashes (`uv.lock` or `pip-compile --generate-hashes`), installed from in CI
and Docker. Patch `filelock`/`setuptools` CVEs. **Acceptance:** `pip-audit` gate passes.

### 3.3 Alembic migrations
Initialize Alembic, baseline the current schema, stop using `create_all` at startup.
**Acceptance:** `alembic upgrade head` builds the schema from empty; a migration test runs
in CI.

### 3.4 Dependency and settings hygiene
Use or remove `redis`, `alembic`, `tqdm`, `aiohttp`, `python-json-logger`, `safety`. Wire
or delete the 8 dead settings. **Acceptance:** a test asserts every declared setting has a
consumer.

### 3.5 Docker
Multi-stage (no compilers in the final image), dependency layer before source copy,
`HEALTHCHECK`, base pinned by digest.

### Phase 3 — status: done (2026-11-17)

Acceptance met, each verified by reintroducing the defect:

| Item | Acceptance | Evidence |
|---|---|---|
| 3.1 gates | a type error turns CI red | mypy, bandit, `ruff format --check` and pip-audit each exit 1 on an injected defect; `|| true` and `continue-on-error` removed; Trivy now gates too |
| 3.2 locks | pip-audit passes | 0 known vulnerabilities on 3.11 and 3.12 from the hash locks (was 16: filelock ×13, setuptools ×3); image: 0 fixable HIGH/CRITICAL |
| 3.3 migrations | `alembic upgrade head` from empty; test in CI | CI runs it on an empty PostgreSQL; tests pin migrations to models (catches a model change without a migration), round-trip downgrade, adopt a pre-migration database |
| 3.4 hygiene | every setting has a consumer | test fails on an unused setting; 7 unused dependencies and 2 dead settings removed, 2 wired (`RPC_RETRIES`, `TRADE_RETENTION_DAYS` via `wash-prune`) |
| 3.5 Docker | multi-stage, dep layer, HEALTHCHECK, digest | built and run locally: migrates an empty database, reports healthy; no pip/setuptools/gcc in the image |

Deviations: `safety` was removed rather than used (it needs an interactive
login, and pinned `filelock<4` with 13 advisories). The roadmap's "8 dead
settings" were 4 by this phase (Phases 1-2 had wired or removed the rest).
CI no longer auto-commits ruff fixes to PR branches: that masked the format
gate and needed write access.

---

## Phase 4 — Observability and operational safety

### 4.1 Instrumentation
JSON logging via the already-declared `python-json-logger`; `X-Request-ID` middleware
propagated into log context and responses; `/metrics` exposing detector hit counts, RPC
latency/failures, circuit-breaker state, audit durations; optional OpenTelemetry traces.

### Phase 4.1 — status: done (2026-11-18)

| Item | Delivered | Evidence |
|---|---|---|
| JSON logging | `LOG_FORMAT=json` for every command, uvicorn included; default in the image | Run against the real server and image; tests |
| `X-Request-ID` | Kept if valid, else generated; on every log line of the request and of the audit it starts; in the response | Live run: all 18 audit-pipeline log lines carried the caller's id |
| `/metrics` | Detector hits and verdicts, RPC latency and failures, breaker state, audit durations, HTTP RED by route | Live audit over HTTP JSON-RPC; integration test reads them through the API |
| OpenTelemetry (optional) | FastAPI's built-in tracing plus audit and per-RPC-call spans; off unless configured | Exported to a local OTLP receiver and decoded: audit span under `POST /api/v1/audits`, 32 RPC client spans under it, no URL in the export |

Each behaviour has a test that fails when it is removed (checked by reintroducing
each defect). Deviations from the plan above:

- **No python-json-logger.** Phase 3 removed it as unused; a small stdlib
  formatter does the job without a dependency.
- **Tracing reuses FastAPI's.** FastAPI (locked at 0.163.1; its `opentelemetry`
  extra first appeared in 0.142.0) creates OpenTelemetry spans for every
  request itself; the project adds only the SDK and exporter
  (`fastapi[opentelemetry]`) and its own audit and RPC spans.
- **Circuit breakers became per chain.** One breaker per audit made a "breaker
  state" metric meaningless (several instances, one label) and let each new
  audit hammer an endpoint already known to be down.
- **A leak found on the way.** RPC URLs (with provider keys) reached ERROR logs
  and API error messages via aiohttp's exception text. Fixed by redaction.

### 4.2 Health semantics
Split `/health/live` (process) from `/health/ready` (DB + RPC reachable).

### Phase 4.2 — status: done (2026-11-18)

| Item | Delivered | Evidence |
|---|---|---|
| `/health/live` | Checks no dependency; the image `HEALTHCHECK` uses it | Image run: with the database and the node both down, four healthchecks passed and the container stayed `healthy` |
| `/health/ready` | 503 unless the database and each `HEALTH_RPC_CHAIN_IDS` endpoint answer, with per-check reasons | Same run: 503 `unreachable` for each outage, 200 again on its own once both returned; integration test on PostgreSQL and HTTP JSON-RPC |

Beyond the plan:

- **The node must serve the right chain.** Readiness asks `eth_chainId` and
  fails with `wrong_chain` on a mismatch: an endpoint configured for the wrong
  network would otherwise pass every check and audit the wrong chain. The
  integration test reproduces it (Base's setting pointing at an Ethereum node).
- **Probes cannot spend RPC quota or pile up.** Each check is time-bounded,
  RPC answers are cached briefly, concurrent probes share one request, and an
  endpoint whose circuit breaker is open is reported without being called.
- **`/health` kept, deprecated.** Its behaviour is unchanged, so existing
  probes keep working.

Each behaviour has a test that fails when it is removed (12 reintroduced
defects, all caught).

### 4.3 Alerting
Pluggable sink (webhook/Slack/email) so the real-time monitor can notify something other
than an open browser tab.

### Phase 4.3 — status: done (2026-11-19)

| Item | Delivered | Evidence |
|---|---|---|
| Pluggable sinks | Webhook (HMAC-signed), Slack, email; any combination | Tests against real local HTTP receivers and an SMTP server |
| Notify something other than a browser tab | The server watches `MONITOR_POOLS` itself and alerts the sinks | Real `wash-api` against the fake chain: six round trips became one signed webhook and one email; Slack (deliberately unreachable over TLS) retried 1 s, 2 s, then gave up without affecting the others; clean shutdown on SIGTERM |

Beyond the plan:

- **Background monitors.** A sink fed by the websocket monitor fires only while
  someone has the panel open, which is the limitation 4.3 exists to remove.
  So the server runs supervised monitors for configured pools.
- **One alert per pass, never repeated.** Each pass's new detections are one
  notification. A trade is announced when first labelled wash, and the label
  in the database is the record, so restarts and earlier audits do not cause
  repeats (the integration test restarts the watcher to show it).
- **Delivery that cannot hurt monitoring.** Queued per sink, retried with
  backoff or `Retry-After`, bounded queue, permanent refusals not retried.

Each behaviour has a test that fails when it is removed (14 reintroduced
defects, all caught).

### 4.4 Security hardening
Login-specific throttle with backoff (currently ~172k guesses/day/IP are permitted);
Redis-backed distributed rate limiting; bounded + DB-persisted audit tasks with TTL.

### Phase 4.4 — status: done (2026-11-19)

| Item | Delivered | Evidence |
|---|---|---|
| Throttle with backoff | Wrong keys lock the client out, 60 s doubling to 1 h by default, on every route, the panel login and websockets, not only the login | Live, two instances: three guesses spread over both locked the client out of both (429, `Retry-After: 5`); the right key worked again after the lockout |
| Redis-backed distributed limits | `RATE_LIMIT_REDIS_URL`; per process when empty; fail open with a metric | One contract suite run against both stores (real Redis 7.0 locally, 7.4 in CI) |
| Bounded, persisted audit tasks with TTL | PostgreSQL table, `AUDIT_MAX_CONCURRENT`, one per pool, TTL, heartbeat | Live: an instance killed with SIGKILL mid-audit; the other marked its task interrupted after 2 minutes and accepted the pool again |

Beyond the plan:

- **Every guess counts, not only logins.** Any route accepts an `X-API-Key`,
  so throttling only `/panel/login` would leave guessing open everywhere else.
- **Redis is optional.** Distributed limits matter only with several
  instances; a single instance needs no new infrastructure.
- **Exact bound across instances.** Audit-task creation takes a PostgreSQL
  advisory lock: ten simultaneous starts with a limit of two create exactly two.
- **A startup race fixed.** Running two instances (the point of shared limits)
  exposed that both ran the migrations at once on an empty database, and one
  failed to start. Migrations now take an advisory lock.
- **Sessions across instances.** `PANEL_SESSION_SECRET` lets every instance
  accept the same panel session (the per-process secret remains the default).

Each behaviour has a test that fails when it is removed (20 reintroduced
defects, all caught, including the startup race and the shared session secret).

### Phase 4 — status: done (2026-11-19)

All four items are done; see each item's status above.

---

## Phase 5 — Performance and scale

- **Fix the N+1:** one windowed query per pool instead of 4 per trade (~40k round-trips at
  10k trades today). Benchmark before/after.
- **Vectorize** the O(n²) circular-trade ratio via a reversed-key merge.
- **Cap the entity-clustering fallback:** the 2,000,000-block default scan is up to 2M
  sequential `eth_getBlock` calls. Hard window cap, explicit opt-in, honest "unsupported"
  signal instead of silent degradation.
- **Bound concurrency:** semaphore/queue for audit tasks; parallel DEX and block-batch
  ingestion.
- **Honor shutdown** inside the ingestion loop.

**Acceptance:** a published benchmark on a 10k-trade pool showing query count and wall time
before/after.

### Phase 5 — status: done (2026-11-19)

Published in [docs/performance.md](../performance.md): a full audit of 10,000 real trades
against PostgreSQL and a replaying node (20 ms round trip, default rate limit).

| | Before | After |
|---|---|---|
| First audit | 135.4 s, 12,482 RPC requests, 23 SQL statements | **41.4 s, 3,137 RPC requests**, 28 SQL statements |
| Re-audit | 134.6 s, 12,482 RPC requests | **2.8 s, 10 RPC requests** |

"Before" is `main` with one fix applied: unpatched, it could not audit this pool at all.

| Item | Delivered | Evidence |
|---|---|---|
| Fix the N+1 | The N+1 was RPC, not SQL (Phase 2 had already brought the audit to 23 statements): one request per transaction on top of one per block. Now one full-block request per block, and stored swaps are skipped before decoding | The benchmark above; e2e test: one `eth_getBlockByNumber` per block, no `eth_getTransactionByHash`, and none at all on a re-audit |
| Vectorize the O(n²) circular-trade ratio | The ML feature was already O(n log n) (Phase 2). The heuristic detector was the O(n²) one; it now looks reverse legs up in per-pair sorted times | Identical output to the old implementation on 40 random pools; 10,000 trades among three addresses: 45.8 s → 0.063 s |
| Cap the clustering fallback | `ENTITY_SCAN_MAX_BLOCKS` capped at 50,000 (was 2,000,000), still off by default; `funding_clusters` in every audit result says what was examined and whether coverage is complete | Tests per coverage case; the e2e audit reports `skipped` on a node without `trace_filter` |
| Bound concurrency | Audits: `AUDIT_MAX_CONCURRENT` (Phase 4.4). Ingestion: `RPC_CONCURRENCY` requests in flight per chain, `RPC_RATE_LIMIT` shared process-wide per chain, log batches fetched ahead, candidate DEXes in parallel | Tests that the cap holds and that a failing DEX cancels the others |
| Honor shutdown | Checked before every 1,000-block batch; finished batches stay saved and are skipped next time | Test: SIGTERM during a batch keeps it, stops before the next |

Beyond the plan:

- **Audits of 1,490+ swaps failed.** Building the benchmark exposed it: one
  INSERT of every trade passed PostgreSQL's 32,767 bind-parameter limit after
  all the RPC work was done. Trades are now saved 1,000 per statement.
- **Silent data loss removed.** A range whose logs could not be fetched was
  skipped, and a block that could not be fetched got the current time; the
  real-time monitor then moved past those blocks for good. All of them now fail
  and are retried.
- **Streaming.** Memory no longer grows with the block range; an interrupted
  audit resumes where it stopped.
- **A bandwidth switch.** One request per block downloads all of the block's
  transactions (10.5× the bytes). `INGEST_FULL_BLOCKS=false` trades that back,
  measured: 4.0× the requests, 0.095× the bytes.

Each behaviour has a test that fails when it is removed (31 reintroduced
defects, all caught; one more mutant was provably equivalent).

---

## Phase 6 — Reach beyond current SOTA (optional, 9 → 9.5+)

Only meaningful once Phase 2 makes improvement measurable.

- **Temporal graph modelling.** The CC 2026 AMM paper argues static-snapshot graph methods
  miss transaction ordering, repetition and temporal coordination, and proposes temporal
  graph networks instead. The current SCC detector is snapshot-based — a temporal variant
  is the natural next step, and the harness will say whether it actually wins.
- **Cross-pool / cross-chain correlation.** Detection is per-pool today; rings that split
  activity across pools or bridge chains are invisible.
- **MEV and arbitrage discrimination.** Arbitrage is position-neutral-ish and circular by
  construction — the most likely source of false positives. Explicitly model and exclude it.
- **Explainability.** Per-flag evidence bundles (the specific cycle, window and
  counterparties) so findings are defensible, not just scored.

---

## Sequencing

Phase 1 → 2 are the score-dominating ones and are strictly ordered: pricing must exist
before efficacy can be measured, and measurement must exist before tuning or Phase 6 means
anything. Phases 3–5 are largely independent and can interleave.

Projected: **P1+P2 ≈ 5.5 → 7.5**, **+P3+P4 ≈ 8.7**, **+P5 ≈ 9.1**.
