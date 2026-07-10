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

- **Per-IP rate limiting** (`API_RATE_LIMIT_PER_MINUTE`, default 120; returns 429).
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
enabled. `/health` is always unauthenticated for liveness probes.

| Method | Path | Description |
|--------|------|-------------|
| GET  | `/health` | Liveness + database connectivity |
| GET  | `/api/v1/pools/{chain_id}/{pool}/trades?limit=&offset=&wash_only=` | Paginated trades for a pool |
| GET  | `/api/v1/pools/{chain_id}/{pool}/report` | Risk metrics: wash volume, per-method breakdown, severity |
| POST | `/api/v1/audits` | Start a full audit in the background (body: `AuditRequest`), returns `202 {task_id}` |
| GET  | `/api/v1/audits/{task_id}` | Poll audit status/result |

Example:

    curl -H "X-API-Key: $KEY" \
      "http://127.0.0.1:8000/api/v1/pools/1/0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc/report"

## WebSocket: live streaming detection

    /api/v1/ws/monitor/{chain_id}/{pool_address}

On connect, the server anchors at the current chain head and then:

1. polls for new blocks every `MONITOR_POLL_INTERVAL_SECONDS` (default 12s),
2. ingests new swap events for the pool (rate-limited, circuit-breaker
   protected),
3. re-runs the full detector stack over a rolling
   `MONITOR_WINDOW_MINUTES` window (default 60),
4. pushes one event per **newly** flagged trade (de-duplicated across polls).

Event frames:

```json
{"type": "status", "data": {"state": "monitoring", "from_block": 19000000, ...}}
{"type": "alert",  "data": {"id": 42, "detection_method": "position_neutral_scc", "wash_trade_score": 0.95, ...}}
{"type": "stats",  "data": {"block": 19000012, "detections_by_method": {"self_trading": 1}, "new_alerts": 1}}
{"type": "error",  "data": {"reason": "rpc_circuit_breaker_open", "recoverable": true}}
```

Transient RPC failures emit `error` events without dropping the connection;
the monitor keeps polling. Close codes: `4401` unauthorized, `4422` invalid
pool address.
