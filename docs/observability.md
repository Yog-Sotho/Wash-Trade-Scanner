# Observability

The API server writes structured logs, gives every request an id, serves
Prometheus metrics at `/metrics` and, when configured, exports OpenTelemetry
traces. Nothing in any of them contains an RPC endpoint URL (hosted endpoints
carry API keys in their URLs), and no metric label contains a pool or wallet
address.

## Logs

All commands (`wash-api`, `wash-audit`, `wash-train`, `wash-prune`,
`wash-reprice`) log to stderr at `LOG_LEVEL`, in one of two formats:

| `LOG_FORMAT` | Output | Default for |
|---|---|---|
| `text` | `2026-11-18 20:16:00,766 - scripts.run_audit - INFO - [<request id>] Synced 9 trades` | local runs |
| `json` | one JSON object per line | the container image |

A JSON line has `timestamp` (UTC, ISO 8601), `level`, `logger` and `message`,
plus, when they apply:

- `request_id`: the API request the line belongs to;
- `trace_id`, `span_id`: the active trace span, when tracing is on;
- `exception`: the traceback;
- any `extra=` fields, such as those of the access log below.

uvicorn's own logs go through the same handler, so they share the format.

### Access log

The `api.access` logger records every HTTP request once it completes:

```json
{"timestamp": "2026-11-18T20:16:00.766+00:00", "level": "INFO", "logger": "api.access",
 "message": "POST /api/v1/audits 202 6.7ms", "request_id": "verify-otel-1",
 "http_method": "POST", "http_path": "/api/v1/audits", "http_route": "/api/v1/audits",
 "http_status": 202, "duration_ms": 6.694, "client_ip": "127.0.0.1"}
```

The health probes and `/metrics` are logged at DEBUG, as orchestrators, the
container healthcheck and the scraper call them every few seconds. The query string is never logged.

## Request ids

Every HTTP request and websocket connection gets an id, returned in the
`X-Request-ID` response header. A caller (or a proxy in front) may send its
own `X-Request-ID`; it is kept if it is 1-128 characters of `A-Z a-z 0-9 . _
: -`, and replaced with a new id otherwise, so it cannot forge log lines.

The id is on every log line written while the request is handled, including
the lines of an audit the request started: `POST /api/v1/audits` returns at
once, and the audit's logs, minutes later, still carry that request's id.

## Metrics

`GET /metrics` serves the Prometheus text format. It needs the API key
whenever authentication is on, like the rest of the API. Give the scraper the
key in an `X-API-Key` header (Prometheus `http_headers`, available since
Prometheus 2.55):

```yaml
scrape_configs:
  - job_name: wash-trade-scanner
    static_configs:
      - targets: ["scanner.internal:8000"]
    http_headers:
      X-API-Key:
        files: [/etc/prometheus/wash_api_key]
```

The server runs one process, so its in-process registry is complete. The
one-shot commands (`wash-audit`, ...) record into a registry nobody scrapes:
audits you want to see in the metrics run through the API
(`POST /api/v1/audits`).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `wash_rpc_requests_total` | counter | `chain_id`, `method`, `outcome` | JSON-RPC requests to chain nodes, each retry included. `outcome`: `ok`; `rpc_error` (the node answered with a JSON-RPC error, e.g. an unsupported `trace_filter`); `transport_error` (no usable answer); `cancelled` |
| `wash_rpc_request_duration_seconds` | histogram | `chain_id`, `method` | Their latency, failures included |
| `wash_rpc_circuit_state` | gauge | `breaker` (`rpc_<chain id>`) | 0 closed, 1 half-open, 2 open |
| `wash_rpc_circuit_opened_total` | counter | `breaker` | Times the breaker opened |
| `wash_rpc_circuit_rejected_total` | counter | `breaker` | Calls refused unsent while it was open |
| `wash_audit_duration_seconds` | histogram | `outcome` (`completed`, `failed`) | Audit wall time |
| `wash_audits_in_progress` | gauge | | Audits running now |
| `wash_audit_detector_hits_total` | counter | `detector` | Trades each detector fired on, in completed audits (`ml` included). A hit is not a verdict |
| `wash_audit_flagged_trades_total` | counter | `method` | Trades completed audits labelled wash, by the deciding method |
| `wash_monitor_alerts_total` | counter | `method` | Alerts the real-time monitor raised (each flagged trade once) |
| `wash_alert_deliveries_total` | counter | `sink`, `outcome` | Alerts handed to a sink ([alerting.md](alerting.md)): `sent`, `failed` (attempts exhausted or refused), `dropped` (queue full) |
| `wash_http_requests_total` | counter | `method`, `route`, `status` | HTTP requests. `route` is the route template (`/api/v1/pools/{chain_id}/{pool_address}/trades`), `unmatched` for unknown paths |
| `wash_http_request_duration_seconds` | histogram | `method`, `route` | Their latency |

The client library's `process_*` and `python_*` metrics (memory, CPU, open
files, garbage collection) are served too.

The circuit breaker is shared, per chain, by everything in the process (audits
and live monitors alike), so `wash_rpc_circuit_state` is the state of that
chain's endpoint.

Some queries:

```promql
# Share of node requests failing, per chain
sum by (chain_id) (rate(wash_rpc_requests_total{outcome=~"rpc_error|transport_error"}[5m]))
  / sum by (chain_id) (rate(wash_rpc_requests_total[5m]))

# A breaker is not closed
max by (breaker) (wash_rpc_circuit_state) > 0

# 95th percentile audit time
histogram_quantile(0.95, sum by (le) (rate(wash_audit_duration_seconds_bucket[1h])))
```

## Traces

Tracing is off unless configured; until then every span is a no-op. The server
uses FastAPI's built-in OpenTelemetry support, and the image ships the SDK and
the OTLP/HTTP exporter. To export traces to a collector:

```bash
FASTAPI_OTEL_AUTO_CONFIGURE=true
OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://otel-collector:4318/v1/traces
OTEL_SERVICE_NAME=wash-trade-scanner
# optional sampling, e.g. 10% of new traces:
OTEL_TRACES_SAMPLER=parentbased_traceidratio
OTEL_TRACES_SAMPLER_ARG=0.1
```

Only the `http/protobuf` OTLP protocol is supported. Setting
`OTEL_EXPORTER_OTLP_ENDPOINT` instead (no `_TRACES`) also makes FastAPI export
its HTTP metrics and error logs to that collector.

A trace of an audit started through the API looks like this:

```
POST /api/v1/audits                      (FastAPI server span)
└─ fastapi.endpoint
   └─ audit                              wash.chain_id, wash.pool_address,
      │                                  wash.trades_processed, wash.wash_trades_detected
      ├─ eth_getLogs                     client span per JSON-RPC request:
      ├─ eth_getBlockByNumber            rpc.system=jsonrpc, rpc.method, wash.chain_id,
      ├─ eth_getTransactionByHash        rpc.jsonrpc.error_code on a JSON-RPC error
      └─ ...
```

A failed RPC span records the exception type, not its message: aiohttp's
messages include the request URL. Log lines written inside a span carry its
`trace_id` and `span_id`, so a trace and its logs can be joined. The one-shot
commands configure no exporter and record no traces.
