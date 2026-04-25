# Architecture

The system follows a modular, async‑first design built around the following components:

## High‑Level Overview

[External Data Sources (RPC nodes)]
          │
          ▼
┌─────────────────────┐
│  ChainIngestor       │  Fetches swap events from DEX routers
│  (per chain)         │  via WebSocket or HTTP. Applies rate
│                     │  limiting and retry logic.
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
│ (rule‑based)      │          │ (Isolation Forest)│
└────────┬─────────┘          └────────┬─────────┘
          │ labels trades                │
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
