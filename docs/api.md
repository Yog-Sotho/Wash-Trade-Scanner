# API & Real-Time Monitoring

The scanner ships an HTTP/WebSocket API (`wash-api`) for on-demand audits,
risk reports and live streaming detection. It is the backend for the upcoming
web panel and is usable directly with `curl`/`websocat`.

## Starting the server

    wash-api

By default the server binds `127.0.0.1:8000` (loopback only). Interactive
docs are served at `http://127.0.0.1:8000/docs` (disable with
`API_DOCS_ENABLED=false`).

## Exposing the API to the internet

The server **refuses to start** on a non-loopback host unless authentication
is enabled and at least one API key is configured:

1. Generate a key pair:

       wash-genkey
       # API key (give this to the client - it is shown only once): <key>
       # SHA-256 hash (append to API_KEY_HASHES ...): <hash>

2. Configure the server (only the hash is stored server-side):

       API_HOST=0.0.0.0
       API_AUTH_ENABLED=true
       API_KEY_HASHES=<hash>[,<hash2>,...]

3. Clients authenticate every request (HTTP and WebSocket) with the header:

       X-API-Key: <key>

Additional hardening, always on:

- **Per-IP rate limiting** (`API_RATE_LIMIT_PER_MINUTE`, default 120 per
  sliding minute; returns 429 with `Retry-After`).
- **Wrong-key lockout.** Every wrong API key counts against the client IP, on
  any route, the panel login or a websocket. From the `AUTH_MAX_FAILURES`-th
  (default 5) on, the client is locked out for `AUTH_LOCKOUT_SECONDS` (60),
  doubling with each further failure up to `AUTH_LOCKOUT_MAX_SECONDS` (1 h).
  While locked out, even the right key gets 429 with `Retry-After` (websocket
  close code `4429`). Failures are forgotten `AUTH_FAILURE_WINDOW_SECONDS`
  (15 min) after the last one, and a success resets them. A missing key or an
  expired session cookie is not counted (the panel probes without one).
- **Several instances.** Limits and lockouts are counted per process unless
  `RATE_LIMIT_REDIS_URL` points every instance at one Redis; then they are
  shared, and `/health/ready` checks Redis. If Redis is unreachable, requests
  are let through without limits (every request still needs a valid key) and
  `wash_limit_store_errors_total` counts it. Set the same
  `PANEL_SESSION_SECRET` on every instance so a panel session is valid on all
  of them.
- **Client IP behind a proxy.** Limits count against the connection's address,
  or the `X-Forwarded-For` client when the request comes from a proxy uvicorn
  trusts: loopback by default, others via `FORWARDED_ALLOW_IPS`. Without that,
  every client behind a remote proxy shares one budget and one lockout.
- **Security headers**: `X-Content-Type-Options`, `X-Frame-Options: DENY`,
  `Referrer-Policy: no-referrer`, `Cache-Control: no-store`, and a deny-all
  `Content-Security-Policy` on API routes.
- **HSTS** (`API_HSTS_ENABLED=true`) when serving behind a TLS-terminating proxy.
- **CORS** disabled unless `API_CORS_ORIGINS` is set (needed by the web panel).
- Keys are verified by constant-time comparison of SHA-256 digests; plaintext
  keys are never stored.

Run the process behind a TLS-terminating reverse proxy (Caddy, nginx,
Traefik) when exposed publicly — the API itself speaks plain HTTP.

## Web panel

A self-contained dashboard ships with the server at **`/panel`** (no build
step, no CDN assets - everything is served same-origin under a strict CSP):

- **Overview** - global stats tiles, wash-volume-by-method chart, per-chain
  activity, top wash pools, recent audits.
- **Pool inspector** - severity, wash ratio and per-method breakdown for any
  pool, plus a paginated trade table with a wash-only filter.
- **Live monitor** - streams the websocket detection feed with alert/stats
  events in real time.
- **Audits** - launch background audits and follow task status.

With auth disabled (loopback) the panel needs no login. With
`API_AUTH_ENABLED=true` it shows a sign-in view: entering an API key calls
`POST /panel/login`, which verifies the key and sets an **HttpOnly,
SameSite=Strict session cookie** signed with a per-process HMAC secret and
expiring after `PANEL_SESSION_TTL_MINUTES` (default 12h; restarting the
server invalidates all sessions). The cookie authenticates REST calls and
the websocket alike - browsers cannot attach custom headers to websockets,
which is why cookie auth exists. Set `PANEL_ENABLED=false` to not serve the
panel at all.

## REST endpoints

All endpoints are under `/api/v1` and require `X-API-Key` when auth is
enabled. The health probes (`/health/live`, `/health/ready`, `/health`) are
always unauthenticated: orchestrators send no key.

Every response carries an `X-Request-ID` header: the caller's own, if it sent
a valid one, else a new id. Quote it when reporting a problem; it is on every
log line of the request (and of an audit the request started). See
[observability.md](observability.md).

| Method | Path | Description |
|--------|------|-------------|
| GET  | `/health/live` | Liveness: 200 while the process serves requests; checks no dependency |
| GET  | `/health/ready` | Readiness: 200 when the database and each `HEALTH_RPC_CHAIN_IDS` endpoint answer, else 503 (see below) |
| GET  | `/health` | Deprecated (use the two above): always 200; the body reports database connectivity |
| GET  | `/metrics` | Prometheus metrics ([observability.md](observability.md)); needs the key when auth is on |
| GET  | `/api/v1/pools/{chain_id}/{pool}/trades?limit=&offset=&wash_only=` | Paginated trades for a pool |
| GET  | `/api/v1/pools/{chain_id}/{pool}/report` | Risk metrics: wash volume, per-method breakdown, severity |
| POST | `/api/v1/audits` | Start a full audit in the background (body: `AuditRequest`), returns `202 {task_id}`; 409 (with the running `task_id`) if the pool is already being audited; 429 if `AUDIT_MAX_CONCURRENT` audits are running |
| GET  | `/api/v1/audits/{task_id}` | Poll audit status/result |

Example:

    curl -H "X-API-Key: $KEY" \
      "http://127.0.0.1:8000/api/v1/pools/1/0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc/report"

### Audit tasks

Audits started with `POST /api/v1/audits` are stored in the database, so any
instance can report them and a restart does not lose them:

```json
{"task_id": "fa4ed5a7e65e4b00972eff6ae6593aaf", "status": "completed",
 "chain_id": 1, "pool_address": "0x9e8c...",
 "created_at": "2026-11-19T01:40:20.6", "finished_at": "2026-11-19T01:41:31.2",
 "result": {"trades_processed": 9, "wash_trades_detected": 6, "risk_metrics": {...}}}
```

`status` is `running`, `completed` (with `result`), `failed` (with `error`) or
`interrupted`: the server running it stopped. A shutting-down server marks its
own audits interrupted; a running audit refreshes a heartbeat every 30 s, and
one silent for 2 minutes (a crashed server) is marked interrupted by the next
request about audits, freeing its slot. At most `AUDIT_MAX_CONCURRENT`
(default 2) run at once across all instances, one per pool; finished tasks are
deleted `AUDIT_TASK_TTL_HOURS` (24) after they end, after which their id
answers 404.

## Health probes

Liveness and readiness answer different questions, and an orchestrator acts
on them differently: a failed liveness probe restarts the process, a failed
readiness probe only takes it out of the load balancer.

- `/health/live` checks nothing but the process itself. A database or RPC
  outage cannot be fixed by a restart, so it never fails liveness. The
  container's `HEALTHCHECK` uses it.
- `/health/ready` checks, concurrently:
  - the database answers a query;
  - for each chain in `HEALTH_RPC_CHAIN_IDS` (default `1`), the configured RPC
    endpoint answers `eth_chainId` with that chain's id. An endpoint serving
    another network is not ready: every audit through it would be wrong;
  - with `RATE_LIMIT_REDIS_URL` set, Redis answers (check `rate_limit_store`).

  Each check is bounded by `HEALTH_CHECK_TIMEOUT_SECONDS` (default 5). RPC
  answers are reused for `HEALTH_RPC_CACHE_SECONDS` (default 15), and probes
  arriving together share one request, so frequent probes cost at most one
  RPC call per chain per period. RPC probes go through the chain's circuit
  breaker: an endpoint the audits found down is reported without being called.

```json
HTTP/1.1 503 Service Unavailable
{"status": "not_ready",
 "checks": {"database": {"ok": true, "latency_ms": 3.1},
            "rpc_1": {"ok": false, "latency_ms": 1213.3, "reason": "unreachable"}}}
```

`reason` is one of `unreachable`, `timeout`, `wrong_chain`, `not_configured`
(no RPC URL set for the chain) or `circuit_open`; never an error message, so
an unauthenticated caller learns nothing about the endpoints. A chain id in
`HEALTH_RPC_CHAIN_IDS` that does not exist stops the server at startup.

Kubernetes, for example (the readiness timeout must exceed
`HEALTH_CHECK_TIMEOUT_SECONDS`, as checks can take that long):

```yaml
livenessProbe:
  httpGet: {path: /health/live, port: 8000}
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet: {path: /health/ready, port: 8000}
  periodSeconds: 10
  timeoutSeconds: 7
```

## WebSocket: live streaming detection

    /api/v1/ws/monitor/{chain_id}/{pool_address}

On connect, the server anchors at the current chain head and then:

1. polls for new blocks every `MONITOR_POLL_INTERVAL_SECONDS` (default 12s),
2. ingests new swap events for the pool (rate-limited, circuit-breaker
   protected),
3. re-runs the full detector stack over a rolling
   `MONITOR_WINDOW_MINUTES` window (default 60),
4. pushes one event per trade **newly** labelled wash: a trade already
   labelled (by an audit, or earlier) is not announced again.

To be alerted without a browser open (webhook, Slack, email), set
`MONITOR_POOLS` and a sink: see [alerting.md](alerting.md).

Event frames:

```json
{"type": "status", "data": {"state": "monitoring", "from_block": 19000000, ...}}
{"type": "alert",  "data": {"id": 42, "detection_method": "round_trip", "wash_trade_score": 0.80, ...}}
{"type": "stats",  "data": {"block": 19000012, "detections_by_method": {"round_trip": 2}, "new_alerts": 2}}
{"type": "error",  "data": {"reason": "rpc_circuit_breaker_open", "recoverable": true}}
```

Transient RPC failures emit `error` events without dropping the connection;
the monitor keeps polling. Close codes: `4401` unauthorized, `4422` invalid
pool address.
