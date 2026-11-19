"""
Prometheus metrics, served by the API at /metrics (docs/observability.md).

Every metric lives in the default registry with the `wash_` prefix, beside
the client library's process_* and python_* collectors. Labels hold only
bounded values: chain ids, JSON-RPC method names, detector names, route
templates. RPC endpoints never appear (hosted endpoints carry API keys in
their URLs), and neither do pool or wallet addresses.

The process serves one uvicorn worker, so the in-process registry is the
whole picture; the one-shot commands (wash-audit, ...) record into a registry
nobody scrapes.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ------------------------------------------------------------------- RPC

RPC_REQUESTS = Counter(
    "wash_rpc_requests_total",
    "JSON-RPC requests sent to chain nodes, by outcome: ok, rpc_error (the "
    "node answered with a JSON-RPC error), transport_error (no usable answer) "
    "or cancelled.",
    ["chain_id", "method", "outcome"],
)
RPC_DURATION = Histogram(
    "wash_rpc_request_duration_seconds",
    "JSON-RPC request latency, including failed requests.",
    ["chain_id", "method"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

# The breaker's state as a number, so that `> 0` means "not closed".
CIRCUIT_STATE_VALUES = {"closed": 0, "half_open": 1, "open": 2}
CIRCUIT_STATE = Gauge(
    "wash_rpc_circuit_state",
    "RPC circuit breaker state per chain: 0 closed, 1 half-open, 2 open.",
    ["breaker"],
)
CIRCUIT_OPENED = Counter(
    "wash_rpc_circuit_opened_total",
    "Times the RPC circuit breaker opened.",
    ["breaker"],
)
CIRCUIT_REJECTED = Counter(
    "wash_rpc_circuit_rejected_total",
    "RPC calls refused without being sent because the breaker was open.",
    ["breaker"],
)

# ---------------------------------------------------------------- audits

AUDIT_DURATION = Histogram(
    "wash_audit_duration_seconds",
    "Wall time of pool audits, by outcome (completed or failed).",
    ["outcome"],
    buckets=(1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 1800.0, 3600.0),
)
AUDITS_IN_PROGRESS = Gauge("wash_audits_in_progress", "Audits currently running.")
AUDIT_DETECTOR_HITS = Counter(
    "wash_audit_detector_hits_total",
    "Trades each detector fired on in completed audits. A hit is not a "
    "verdict: the calibrated score decides (wash_audit_flagged_trades_total).",
    ["detector"],
)
AUDIT_FLAGGED_TRADES = Counter(
    "wash_audit_flagged_trades_total",
    "Trades labelled wash by completed audits, by the deciding method.",
    ["method"],
)
MONITOR_ALERTS = Counter(
    "wash_monitor_alerts_total",
    "Newly flagged trades the real-time monitor alerted on, by deciding method.",
    ["method"],
)

ALERT_DELIVERIES = Counter(
    "wash_alert_deliveries_total",
    "Alert batches handed to a sink, by outcome: sent; failed (every attempt "
    "failed, or the receiver refused it); dropped (the sink's queue was full).",
    ["sink", "outcome"],
)

# ------------------------------------------------------------------ HTTP

HTTP_REQUESTS = Counter(
    "wash_http_requests_total",
    "HTTP requests served, by route template and status code.",
    ["method", "route", "status"],
)
HTTP_DURATION = Histogram(
    "wash_http_request_duration_seconds",
    "HTTP request latency by route template.",
    ["method", "route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
