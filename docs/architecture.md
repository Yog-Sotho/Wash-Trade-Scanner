# Architecture

The system follows a modular, async‑first design built around the following components:

## High‑Level Overview

[External Data Sources (RPC nodes)]
          │
          ▼
┌─────────────────────┐
│  ChainIngestor       │  Fetches a pool's swap events from the DEX
│  (per chain)         │  that created it (pool.factory()), plus each
│                     │  swap's transaction sender (tx_from) and USD
│                     │  value. Rate limiting, retries, circuit breaker.
└─────────┬───────────┘
          │ stores trades
          ▼
┌─────────────────────┐
│  Storage             │  Async PostgreSQL interface (SQLAlchemy)
│                     │  with connection pooling.
└─────────┬───────────┘
          │
          ▼
┌──────────────────────────────────────────┐
│  Feature Engineer                         │
│  Computes per‑trade and pool‑level features│
│  (volume, frequency, graph metrics)       │
└─────────┬────────────────────────────────┘
          │
          ├─────────────────────────────┐
          ▼                             ▼
┌──────────────────┐          ┌──────────────────┐
│ HeuristicDetector │          │ MLDetector        │
│ (rule‑based, by   │          │ (Isolation Forest)│
│  transaction      │          │                   │
│  sender)          │          │                   │
└────────┬─────────┘          └────────┬─────────┘
          │ hits scored as P(wash) from measured rates
          │ (core/calibration.py); labelled at ≥ threshold
          └─────────┬───────────────────┘
                    ▼
          ┌──────────────────┐
          │ Entity Clustering  │  Identifies addresses
          │ (graph analysis)   │  controlled by same entity
          └───────────────────┘
                    │
                    ▼
          ┌──────────────────┐
          │ Audit Reporter     │  Generates risk scores,
          │ (run_audit.py)     │  exports reports (JSON/CSV)
          └───────────────────┘

## Detection quality

`evaluation/` is a benchmark harness, separate from the audit path: real pool
histories recorded by the production ingestor (`evaluation/fixtures/`), labelled
wash campaigns injected into them, and a documented real wash-trading market.
`wash-eval` measures every detector on it and writes `docs/benchmarks.md`; the
detector rates it measures are shipped as `config/detector_calibration.json`,
which the audit's scoring reads. CI re-runs it and fails if a detector regresses
or the shipped rates no longer match the detectors.
