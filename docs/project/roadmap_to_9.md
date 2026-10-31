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

---

## Phase 2 — Prove detection quality (the SOTA core)

Today there is no precision, recall, F1, AUC, split or cross-validation anywhere. A
detection system without measured accuracy is an assertion, not a product.

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

---

## Phase 4 — Observability and operational safety

### 4.1 Instrumentation
JSON logging via the already-declared `python-json-logger`; `X-Request-ID` middleware
propagated into log context and responses; `/metrics` exposing detector hit counts, RPC
latency/failures, circuit-breaker state, audit durations; optional OpenTelemetry traces.

### 4.2 Health semantics
Split `/health/live` (process) from `/health/ready` (DB + RPC reachable).

### 4.3 Alerting
Pluggable sink (webhook/Slack/email) so the real-time monitor can notify something other
than an open browser tab.

### 4.4 Security hardening
Login-specific throttle with backoff (currently ~172k guesses/day/IP are permitted);
Redis-backed distributed rate limiting; bounded + DB-persisted audit tasks with TTL.

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
