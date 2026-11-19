# Alerting

The API server can watch pools continuously and report new wash trades to a
webhook, a Slack channel or email, with no browser open. Set the pools in
`MONITOR_POOLS` and at least one sink; `wash-api` does the rest.

```bash
MONITOR_POOLS=1:0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc,8453:0x...
ALERT_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T000/B000/XXXX
```

## How it works

For each `chain_id:pool` in `MONITOR_POOLS`, the server runs the same monitor
as the panel's live view: it polls the chain head every
`MONITOR_POLL_INTERVAL_SECONDS`, ingests new swaps and re-runs the detectors
over the last `MONITOR_WINDOW_MINUTES`. Everything one pass newly labels wash
becomes **one** alert, sent to every configured sink: a burst of twenty wash
trades is one Slack message, not twenty.

- **Only new verdicts.** A trade is announced when a pass labels it wash for
  the first time. A trade an audit already labelled, or one announced before a
  restart, is not announced again: the label in the database is the record.
- **Every alert is also logged** (WARNING, logger `core.watch`, with the trade
  ids in `alert_trade_ids`), so the log is the alert of last resort.
- **A monitor that fails is restarted**, after a backoff that doubles from 5
  seconds up to 5 minutes (an RPC endpoint down at startup, say). Transient
  RPC errors during monitoring do not stop it.
- **Limits.** A monitor starts at the chain head: blocks mined while the server
  was down are not watched (run an audit for those). The server is one process;
  running several replicas with the same `MONITOR_POOLS` sends each alert once
  per replica. The panel's live view (websocket) shows detections to its viewer
  only; it does not alert the sinks.

## Sinks

Each sink is on when its address is set; any combination works.

| Sink | Settings | Sends |
|---|---|---|
| Webhook | `ALERT_WEBHOOK_URL` (http or https), optional `ALERT_WEBHOOK_SECRET` | The alert as JSON (below), signed when the secret is set |
| Slack | `ALERT_SLACK_WEBHOOK_URL` (an [incoming webhook](https://docs.slack.dev/messaging/sending-messages-using-incoming-webhooks/), https) | One message: summary and up to 10 trades |
| Email | `ALERT_EMAIL_TO` (comma-separated), `ALERT_EMAIL_FROM`, `ALERT_SMTP_HOST`, `ALERT_SMTP_PORT` (587), `ALERT_SMTP_USERNAME`, `ALERT_SMTP_PASSWORD`, `ALERT_SMTP_TLS` (`starttls`, `ssl` or `none`) | One plain-text email: summary and up to 10 trades |

The server refuses to start with `ALERT_EMAIL_TO` set but no SMTP host or
sender, or with a chain id in `MONITOR_POOLS` that does not exist. With
`MONITOR_POOLS` set and no sink, alerts are only logged (and a warning says
so at startup).

The webhook URLs are secrets (a Slack webhook URL is the credential itself):
they are redacted from every log line, like the RPC URLs.

## Webhook payload

```json
{
  "type": "wash_trade_alert",
  "version": 1,
  "chain_id": 1,
  "chain_name": "Ethereum",
  "pool_address": "0x9e8ce2cc2fd75fb0de6a1e1b6b1bf6c7f33a4d6e",
  "detected_at": "2026-11-19T12:00:00.123456+00:00",
  "trade_count": 6,
  "wash_volume_usd": 5985.0,
  "methods": {"round_trip": 6},
  "trades": [
    {"id": 41, "transaction_hash": "0x...", "block_number": 201, "tx_from": "0x...",
     "volume_usd": 1000.0, "is_wash_trade": true, "wash_trade_score": 0.97,
     "detection_method": "round_trip", "...": "every field of the API's trade object"}
  ]
}
```

### Verifying the signature

With `ALERT_WEBHOOK_SECRET` set, each request carries `X-Wash-Timestamp` (Unix
seconds) and `X-Wash-Signature: sha256=<hex>`, the HMAC-SHA256 of
`<timestamp>.<raw body>` under the secret. Verify against the raw bytes, before
parsing, and reject old timestamps to stop replays:

```python
import hashlib, hmac, time


def verify(secret: bytes, headers, body: bytes, max_age: int = 300) -> bool:
    timestamp = headers["X-Wash-Timestamp"]
    if abs(time.time() - int(timestamp)) > max_age:
        return False
    expected = hmac.new(secret, timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(headers["X-Wash-Signature"], f"sha256={expected}")
```

## Delivery

Delivery never slows monitoring: each alert is queued per sink and sent in the
background.

- **Retries.** A failed attempt is retried up to `ALERT_MAX_ATTEMPTS` (3) in
  all, each attempt limited to `ALERT_TIMEOUT_SECONDS` (10). The wait doubles
  from 1 second (up to 5 minutes), or follows the receiver's `Retry-After`
  (Slack sends one when rate limiting).
  - Retried: no answer, HTTP 429 or 5xx, SMTP connection errors.
  - Not retried: any other HTTP status (a 4xx means the request will never be
    accepted), SMTP authentication failure, refused recipients.
- **Queue.** Each sink holds up to 1,000 waiting alerts; when full, the oldest
  is dropped.
- **Shutdown.** The server stops the monitors first, then gives queued alerts
  up to 5 seconds to go out.
- **Proxies.** Webhook and Slack requests honour `HTTPS_PROXY` / `NO_PROXY`.

Outcomes are counted in `wash_alert_deliveries_total{sink, outcome}`
(`sent`, `failed`, `dropped`), and alerts raised in
`wash_monitor_alerts_total{method}`; see [observability.md](observability.md).
For example, to be told when alerts stop getting through:

```promql
increase(wash_alert_deliveries_total{outcome=~"failed|dropped"}[1h]) > 0
```
