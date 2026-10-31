# Codebase Audit Report

**Date:** 2026-10-31
**Project:** wash-trade-detector (Wash-Trade-Scanner)
**Language / Framework:** Python 3.11+ / FastAPI + SQLAlchemy 2.0 async + scikit-learn + web3.py
**Project Type:** Web Service (+ CLI tools)
**Audit Mode:** Global
**Commit audited:** `f99fe4d` (origin/main, 2026-08-09)

---

## Executive Summary

The engineering *scaffolding* of this project is genuinely strong: 154 passing tests at
90% coverage, a clean ruff baseline, zero bandit findings, a hardened API (hashed keys,
constant-time compare, HttpOnly/SameSite sessions, rate limiting, CSP, refuse-public-bind
guard), a self-contained web panel, and a 7-job CI pipeline with SBOM and image scanning.
Judged as infrastructure alone it would score around 8/10.

The problem is that the product underneath that scaffolding **does not currently run, and
its detection quality has never been measured.** Three verified, reproduced defects
dominate everything else:

1. **No audit can execute on any chain.** `config/chains.py` hardcodes RPC URLs and never
   reads `settings`; chain 1 carries a literal `YOUR_KEY` placeholder that the connect
   guard rejects, and `MultiChainIngestor.initialize()` connects *every* chain, so the
   first failure aborts all of them. Setting `ETH_RPC_URL` has no effect — the entire
   `settings.rpc_urls` property is dead code (0 references).
2. **The USD pipeline is never populated.** `volume_usd`, `amount_in_usd` and
   `amount_out_usd` are read in ~40 places but written in none. There is no price oracle;
   `COINGECKO_API_KEY`/`COINGECKO_API_URL` are unused. Consequently the closed-cluster
   detector can never fire (it `continue`s on zero volume), volume-anomaly detection
   operates on all-zeros, and every report and dashboard figure is $0.
3. **Detection accuracy is unmeasured.** There is no precision, recall, F1, AUC,
   train/test split, or cross-validation anywhere in the codebase. The ML model is trained
   and shipped with no evidence it discriminates anything.

The tests pass at 90% coverage precisely *because* they mock the boundary that is broken —
`MultiChainIngestor` is patched out in every audit test. High coverage is masking a
non-functional product.

**Top priority:** restore end-to-end executability (RPC resolution + price oracle), then
build an evaluation harness so detection quality becomes a measured number rather than an
assertion.

## Overall Score: 5.5 / 10

| Dimension | Score | Priority | Status |
|---|---|---|---|
| Security | 7/10 | CRITICAL | WARN |
| Build & Types | 5/10 | CRITICAL | WARN |
| Code Principles | 6/10 | HIGH | WARN |
| Code Quality | 7/10 | MEDIUM | PASS |
| Dependencies | 6/10 | MEDIUM | WARN |
| Dead Code | 5/10 | LOW | WARN |
| Observability | 3/10 | MEDIUM | FAIL |
| Concurrency | 7/10 | HIGH | PASS |
| Lifecycle | 6/10 | MEDIUM | WARN |
| Detection Efficacy † | 3/10 | CRITICAL | FAIL |

† Added dimension. For a detection product, "does it detect, and how well" is the core
quality axis and is not covered by the standard nine.

No hard cap applied: the CRITICAL findings are functional, not security- or build-breaking.

---

## Strengths

1. **Research-grounded detector design.** The position-neutral SCC detector faithfully
   implements Victor & Weintraud (WWW '21) with multi-window passes and a net-position
   margin; the closed-cluster detector adds a per-member in/out balance test that
   correctly separates recycling rings from organic directional flow. This is real
   methodology, not pattern-matching.
2. **Genuinely hardened API surface.** Keys stored only as SHA-256 digests, constant-time
   comparison evaluated across all candidates, HttpOnly + SameSite=Strict session cookies
   with a per-process signing secret, and a startup guard that *refuses* to bind a public
   interface without auth rather than merely warning. Bandit reports zero issues across
   4,525 LOC.
3. **Disciplined test and lint baseline.** 154 tests, 90% coverage, ruff clean (check and
   format), deterministic and fast (7.5s). Detector tests include negative cases, not just
   happy paths.
4. **Supply-chain awareness in CI.** trivy-action and trufflehog pinned to full commit
   SHAs, SBOM generation, secret scanning, forbidden-file checks, and a matrix across 3.11
   and 3.12.

---

## Findings by Dimension

### Detection Efficacy [3/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| CRITICAL | `core/ingestor.py:261-277` | `_process_swap_event` never sets `volume_usd`, `amount_in_usd`, `amount_out_usd`. No price oracle exists anywhere. | Add `core/pricing.py`: reference-leg valuation (stablecoin/WETH side of the swap) from pool reserves at block, with an optional CoinGecko fallback. Populate all three fields at ingestion. |
| CRITICAL | `core/advanced_heuristics.py` closed-cluster | With all volumes 0, `total <= 0` → `continue`. The detector **can never fire** in production. | Fixed by the pricing work; add a regression test asserting it fires on priced data. |
| CRITICAL | project-wide | No precision/recall/F1/AUC, no train/test split, no cross-validation. Detection quality is unmeasured and unprovable. | Build a labeled evaluation harness (synthetic injection + simulation); publish per-detector metrics. |
| HIGH | `core/heuristics.py` volume_anomaly | MAD/IQR fitted on an all-zero volume distribution; `mad == 0` → score 0.0 for every trade. Detector is inert. | Same pricing fix; assert non-degenerate distribution in tests. |
| HIGH | `config/settings.py:64` | `HEURISTIC_WEIGHTS` defines a composite weighting that is never used — detectors only ever write a hardcoded per-detector score. | Implement composite scoring, or delete the setting. Calibrate weights from the eval harness. |
| MEDIUM | `scripts/train_model.py` | Trains and saves unconditionally; no held-out evaluation, no metrics stored with the artifact. | Persist metrics alongside the model; refuse to save a model that regresses below a floor. |

### Security [7/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| HIGH | `.github/workflows/CI.yml:151,154,90` | `mypy`, `bandit` and `pip-audit` all end in `\|\| true`. Static analysis and CVE scanning **cannot fail the build** — they are decorative. mypy is in fact failing right now (5 errors) and CI is green. | Remove `\|\| true`. Gate on bandit HIGH/MEDIUM and on pip-audit criticals. |
| MEDIUM | `api/server.py` `/panel/login` | Login shares the global 120 req/min/IP budget; no login-specific throttle, lockout, or backoff. Permits ~172k key guesses/day/IP. | Add a strict per-IP login limiter (e.g. 5/min) with exponential backoff. |
| MEDIUM | `api/server.py:92,187` | `app.state.audit_tasks` is an unbounded dict, never pruned. Each `POST /audits` adds an entry permanently. | TTL eviction + max-size cap; persist task state to the DB. |
| LOW | dependency tree | Transitive CVEs unpatched: `filelock 3.32.7` (13 CVEs, fix 4.2.1), `setuptools 79.0.1` (PYSEC-2026-3447, GHSA-xmj3-9gf7-h52w, fix 84.1.0). | Add constraints/lockfile; re-run pip-audit as a gate. |
| LOW | `api/middleware.py` | Rate limiter is in-process memory: resets on restart, not shared across workers. Documented, but a real bypass under multi-worker deploys. | Redis-backed limiter (the `redis` dependency is already declared but unused). |

### Build & Types [5/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| HIGH | `core/storage.py:298,304,306,340,342` | 5 mypy errors (`float(float \| None)`), masked by `\|\| true` in CI. Runtime-safe today only because `func.coalesce` guarantees non-NULL — the types don't express that. | Wrap aggregates so the type is `float`, or cast explicitly. Then make mypy a hard gate. |
| HIGH | repo root | No lockfile and no hash pinning. `requirements.txt` + `pyproject.toml` use open ranges (`>=x,<y`). Builds are not reproducible. | Adopt `uv.lock` or `pip-compile` with `--generate-hashes`; install from it in CI and Docker. |
| MEDIUM | `alembic` declared, no migrations | `alembic>=1.12.0` is a dependency but there is no `alembic.ini` and no `migrations/`. Schema is created via `Base.metadata.create_all`, which cannot evolve a populated production database. | Initialize Alembic; baseline the current schema; switch startup off `create_all`. |
| MEDIUM | `Dockerfile` | Single-stage; `gcc`/`g++`/`libpq-dev` ship in the final image (larger surface). `COPY . .` precedes install, so any source change busts the dependency layer. No `HEALTHCHECK`, no pinned base digest. | Multi-stage build; copy dependency manifests first; add HEALTHCHECK; pin base by digest. |

### Code Principles [6/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| CRITICAL | `config/chains.py` vs `config/settings.py` | chains.py hardcodes every `rpc_url` and never imports settings. `settings.rpc_urls` (23 chains) has **0 references**. Chain 1 ships `.../v2/YOUR_KEY`, which `ChainIngestor.connect()` rejects — the error even says "Set a real endpoint in environment", which has no effect. **Verified by reproduction.** | Resolve `rpc_url` from settings at access time (`get_chain_config` overlays `settings.rpc_urls`). |
| CRITICAL | `core/ingestor.py:365-369` | `MultiChainIngestor.initialize()` connects to **all 23 chains** eagerly; the Ethereum placeholder raises and aborts initialization, so *every* audit on *every* chain fails. **Verified by reproduction.** | Connect lazily, per requested chain only; isolate per-chain failures. |
| HIGH | `core/feature_engineer.py:233` | N+1: `compute_trade_features` issues 4 queries per trade inside a loop. 10k trades ⇒ ~40k round-trips. | Batch: one windowed query per pool, aggregate in pandas. |
| HIGH | `core/entity_clustering.py:107,155` | Block-scan fallback iterates `range(from_block, to_block+1)` with a default window of 2,000,000 blocks ⇒ up to 2M sequential `eth_getBlock` calls (hours). Effectively a hang when `trace_filter` is unsupported. | Cap the fallback window hard; require explicit opt-in; surface "unsupported" rather than degrade silently. |
| MEDIUM | `core/feature_engineer.py` circular ratio | O(n²): a full DataFrame filter per row. | Vectorize via a merge on the reversed (sender, recipient) key. |

### Code Quality [7/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| INFO | repo-wide | ruff check + format clean across 47 files; bandit clean; comments explain *why* (non-obvious rationale documented at fix sites). | Maintain. |
| MEDIUM | `tests/` | Every audit test patches `MultiChainIngestor` — no integration test exercises real ingestion, a real database, or a real RPC. This is exactly why the two CRITICAL defects shipped. | Add a Postgres-backed integration job and a forked/recorded-RPC end-to-end audit test. |
| LOW | `config/chains.py` | 1,096 lines of hardcoded chain/DEX/ABI config in one module. | Split per-chain data into a data file (TOML/JSON) loaded at import. |

### Dependencies [6/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| MEDIUM | `pyproject.toml` | Declared but **entirely unused**: `redis`, `alembic`, `tqdm`, `aiohttp`, `python-json-logger` (0 references each). Install surface and CVE exposure for nothing. | Either use them (redis→limiter, alembic→migrations, json-logger→structured logs) or drop them. |
| MEDIUM | — | `safety>=3.0.0` still in dev deps though CI deliberately stopped using it. | Remove. |
| LOW | — | Unpatched transitive CVEs (see Security). | Lockfile + gated pip-audit. |

### Dead Code [5/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| MEDIUM | `config/settings.py` | Genuinely unreferenced settings: `rpc_urls` property (all 23 chains), `COINGECKO_API_KEY`, `COINGECKO_API_URL`, `MIN_WASH_TRADE_VOLUME_USD`, `HEURISTIC_WEIGHTS`, `TRADE_RETENTION_DAYS`, `REDIS_URL`, `RPC_RETRIES`. Settings that look like configuration but silently do nothing are worse than absent ones. | Wire up or delete each; add a test asserting every setting has a consumer. |
| LOW | `core/storage.py:363` | `cleanup_old_data()` has no caller; `TRADE_RETENTION_DAYS` is never applied. | Schedule it, or expose it via CLI/API. |

### Observability [3/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| HIGH | repo-wide | No structured logging (0 uses of `python-json-logger`, which *is* a dependency), no metrics endpoint, no Prometheus, no request/correlation IDs, no tracing. Logs are unstructured f-strings. | JSON logging; `X-Request-ID` middleware propagated into log context; `/metrics` with detector counters, RPC latency, queue depth; optional OpenTelemetry. |
| MEDIUM | `api/server.py:111` | Single `/health` conflates liveness and readiness; no `/ready`. A DB blip makes orchestrators kill an otherwise live pod. | Split `/health/live` and `/health/ready`. |
| MEDIUM | — | No alerting path for the real-time monitor: wash trades are streamed to a websocket only. No webhook/email/Slack sink. | Add a pluggable alert sink. |

### Concurrency [7/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| INFO | `circuit_breaker.py`, `ingestor.py`, `middleware.py` | Correct `asyncio.Lock` usage around all shared mutable state; token-bucket limiter and circuit breaker are sound. | Maintain. |
| MEDIUM | `api/server.py:188` | Audit tasks are fire-and-forget `create_task` with no concurrency cap — N concurrent POSTs spawn N full audits, each hammering RPC. | Bound with a semaphore or a real queue (the `redis` dep is available). |
| MEDIUM | `core/ingestor.py:401` | DEXes are synced strictly sequentially per audit; no parallelism across DEXes or block batches. | Bounded `asyncio.gather` over batches. |

### Lifecycle [6/10]

| Severity | Location | Issue | Recommendation |
|---|---|---|---|
| MEDIUM | `scripts/run_audit.py:77,97` | `_shutdown_event` is only checked twice, both before ingestion begins. A long sync cannot be interrupted — SIGTERM is effectively ignored for the duration. | Check the event inside the block-batch loop. |
| MEDIUM | `api/server.py` | In-flight audit tasks are neither awaited nor cancelled on shutdown, and task state is lost on restart. | Cancel-and-drain in the lifespan teardown; persist task state. |
| MEDIUM | — | No migrations (see Build). Production schema cannot evolve. | Alembic. |

---

## Advisory Findings

- `config/chains.py` size (1,096 lines) — **[High cohesion module]**: it is a single-purpose
  data registry with a uniform schema. Flagged LOW only; splitting is optional.
- In-memory rate limiter — **[Documented design decision]**: the module docstring states
  single-process is the supported topology. Retained as LOW rather than MEDIUM-plus.

---

## Recommended Actions (Priority Order)

1. **[CRITICAL]** Resolve RPC URLs from settings in `get_chain_config`; make
   `MultiChainIngestor` connect lazily per requested chain. Restores basic executability.
2. **[CRITICAL]** Implement `core/pricing.py` and populate `volume_usd` /
   `amount_in_usd` / `amount_out_usd` at ingestion. Revives the closed-cluster and
   volume-anomaly detectors and every USD figure in the product.
3. **[CRITICAL]** Build a labeled evaluation harness and publish per-detector
   precision/recall/F1. Detection quality must be measured, not asserted.
4. **[HIGH]** Remove `|| true` from mypy/bandit/pip-audit in CI; fix the 5 mypy errors.
5. **[HIGH]** Add a Postgres-backed integration test and an end-to-end audit test against
   a recorded/forked RPC — the gap that let defects 1 and 2 ship.
6. **[HIGH]** Fix the N+1 in feature engineering and cap the entity-clustering block scan.
7. **[MEDIUM]** Alembic migrations; lockfile with hashes; multi-stage Dockerfile.
8. **[MEDIUM]** Observability: JSON logs, request IDs, `/metrics`, `/ready`.
9. **[MEDIUM]** Login-specific throttling; bounded + persisted audit tasks.
10. **[LOW]** Remove or wire up the 8 dead settings and 5 unused dependencies.

---

## Sources Consulted

- Victor & Weintraud, *Detecting and Quantifying Wash Trading on Decentralized
  Cryptocurrency Exchanges*, WWW '21 — arXiv:2102.07001 (implemented by this project)
- *Wash Trading Detection in Automatic Market Makers*, CC 2026 (Springer
  10.1007/978-3-032-24807-7_34) — temporal graph networks for AMM wash detection
- Sirolly, Ma, Kanoria & Sethi, *Network-Based Detection of Wash Trading*, SSRN 5714122
  (rev. Jan 2026) — closed-cluster methodology
- LhyJason/WashTradingSimulation (GitHub) — ground-truth-labelled wash trade simulation and
  precision/recall evaluation of network-based detectors
- Allium docs, *DEX token prices hourly* — reference-leg USD valuation methodology and
  dust filtering
- Tooling run against commit `f99fe4d`: pytest 154 passed / 89.77% cov, ruff clean,
  mypy 5 errors, bandit 0 issues, pip-audit 2 vulnerable packages
