# Configuration Reference

All settings are managed through environment variables, loaded from a `.env` file using `python-dotenv`.

## Required Settings

### DATABASE_URL
PostgreSQL connection string for async communication.  
Format: `postgresql+asyncpg://user:password@host:port/database`  
Example: `postgresql+asyncpg://wash_user:wash_pass@localhost:5432/wash_detector`

## RPC Endpoints

Each chain can have its RPC endpoint configured. If a variable is not set, a public default endpoint is used (subject to rate limits). **Important**: if a default URL contains `YOUR_KEY`, you must replace it with a real API key or set the appropriate environment variable.

| Chain             | Variable               | Default Public Endpoint                                     |
|-------------------|------------------------|-------------------------------------------------------------|
| Ethereum          | `ETH_RPC_URL`          | *(none - required; set your own endpoint)*                  |
| BSC               | `BSC_RPC_URL`          | `https://bsc-dataseed1.binance.org`                         |
| Polygon           | `POLYGON_RPC_URL`      | `https://polygon.drpc.org`                                  |
| Arbitrum          | `ARBITRUM_RPC_URL`     | `https://arb1.arbitrum.io/rpc`                              |
| Optimism          | `OPTIMISM_RPC_URL`     | `https://mainnet.optimism.io`                               |
| Base              | `BASE_RPC_URL`         | `https://mainnet.base.org`                                  |
| Avalanche         | `AVALANCHE_RPC_URL`    | `https://api.avax.network/ext/bc/C/rpc`                     |
| Fantom            | `FANTOM_RPC_URL`       | `https://rpcapi.fantom.network`                             |
| Celo              | `CELO_RPC_URL`         | `https://forno.celo.org`                                    |
| Gnosis            | `GNOSIS_RPC_URL`       | `https://rpc.gnosischain.com`                               |
| Moonbeam          | `MOONBEAM_RPC_URL`     | `https://rpc.api.moonbeam.network`                          |
| Aurora            | `AURORA_RPC_URL`       | `https://mainnet.aurora.dev`                                |
| Harmony           | `HARMONY_RPC_URL`      | `https://api.harmony.one`                                   |
| Cronos            | `CRONOS_RPC_URL`       | `https://evm.cronos.org`                                    |
| Metis             | `METIS_RPC_URL`        | `https://andromeda.metis.io/?owner=1088`                    |
| Boba              | `BOBA_RPC_URL`         | `https://mainnet.boba.network`                              |
| zkSync Era        | `ZKSYNC_RPC_URL`       | `https://mainnet.era.zksync.io`                             |
| Polygon zkEVM     | `POLYGON_ZKEVM_RPC_URL`| `https://zkevm-rpc.com`                                     |
| Linea             | `LINEA_RPC_URL`        | `https://rpc.linea.build`                                   |
| Scroll            | `SCROLL_RPC_URL`       | `https://rpc.scroll.io`                                     |
| Mantle            | `MANTLE_RPC_URL`       | `https://rpc.mantle.xyz`                                    |
| Kava              | `KAVA_RPC_URL`         | `https://evm.kava.io`                                       |
| Klaytn (Kaia)     | `KLAYTN_RPC_URL`       | `https://public-en.node.kaia.io`                            |

Each chain's endpoint is read from its variable at startup; the defaults above
are public endpoints, verified on 2026-10-31 to answer with the correct chain
ID. Ethereum has no public default — set `ETH_RPC_URL`. If an endpoint contains
the placeholder `YOUR_KEY`, connecting to that chain fails with an error naming
the variable to set. Only the chain being audited is connected, so a
misconfigured chain never affects audits of the others.

The default Moonbeam endpoint could not be reached from the environment used
for that verification, but is still documented as Moonbeam's official one, so
it was left unchanged; set `MOONBEAM_RPC_URL` if it fails for you.

## Detection Parameters

Defaults marked † were chosen on the detection benchmark (`python -m
evaluation.tune`, results in [benchmarks.md](benchmarks.md)); after changing a
detector or its defaults, run `make calibrate` so the shipped calibration and
regression baseline match.

### Scoring

| Variable                        | Description                                                                                          | Default |
|---------------------------------|------------------------------------------------------------------------------------------------------|---------|
| `WASH_PRIOR`                    | Prior share of wash trades among audited trades; with each detector's measured rates (`config/detector_calibration.json`) it turns a hit into P(wash \| hit) | `0.01`  |
| `SUSPICIOUS_ACTIVITY_THRESHOLD` | A trade is labelled wash when P(wash \| hit) of its most probative detector reaches this               | `0.8`   |

### Round trips and position neutrality

| Variable                            | Description                                                                          | Default      |
|-------------------------------------|--------------------------------------------------------------------------------------|--------------|
| `ROUND_TRIP_MAX_BLOCKS`             | Max blocks between a trade and the opposite trade that sells it back                  | `50` †       |
| `ROUND_TRIP_VALUE_TOLERANCE`        | Max relative USD difference between the two legs                                      | `0.01` †     |
| `ROUND_TRIP_MIN_COUNT`              | Round trips a trader must make in a pool to be flagged                                | `2` †        |
| `POSITION_NEUTRAL_WINDOWS_HOURS`    | Multi‑pass window sizes (hours) over which each entity's net position is checked       | `[1,24,168]` † |
| `POSITION_NEUTRAL_MARGIN`           | Max net change in every token, relative to the amount moved, to call an entity flat    | `0.001` †    |
| `POSITION_NEUTRAL_MIN_TRADES`       | Minimum trades by an entity in a window before it can be flagged                       | `4` †        |

### Coordinated trading and MEV

| Variable                            | Description                                                                          | Default      |
|-------------------------------------|--------------------------------------------------------------------------------------|--------------|
| `COORDINATION_MAX_BLOCKS`           | Max blocks between two wallets' opposite trades to count as a matched pair            | `3` ‡        |
| `COORDINATION_VALUE_TOLERANCE`      | Max relative value difference of a matched pair                                       | `0.05`       |
| `COORDINATION_MIN_PAIRS`            | Matched pairs a group of wallets must make to be flagged                              | `3` ‡        |
| `COORDINATION_MIN_COVERAGE`         | Share of each member's trades that must be pairs with the group                       | `0.8` ‡      |
| `COORDINATION_NET_MARGIN`           | Max net change of the group in every token, relative to the amount moved              | `0.1`        |
| `SANDWICH_AMOUNT_TOLERANCE`         | Max relative difference between what a sandwich's front-run bought and its back-run sold | `0.1`     |

‡ Searched by `python -m evaluation.tune`; the shipped values are not its pick,
which costs 16 clean-pool false positives for 1.3 points of recall
([benchmarks.md](benchmarks.md#parameter-selection)). The window and the value
tolerance sit at the documented heuristic's 3 blocks and 5%: narrower misses
the injected rings, wider adds false positives.

### Supporting signals

| Variable                            | Description                                                                       | Default      |
|-------------------------------------|-----------------------------------------------------------------------------------|--------------|
| `WASH_TRADE_TIME_WINDOW_MINUTES`    | Time window (in minutes) for circular trading detection                           | `60`         |
| `BOT_ALLOWLIST`                     | Comma‑separated addresses (transaction senders) skipped by bot and repeated‑amount detection | (empty) |
| `BOT_TRADE_COUNT_THRESHOLD`         | Minimum trades by one trader to consider bot activity                             | `10`         |
| `BOT_TRADE_TIME_THRESHOLD_SECONDS`  | Maximum average inter‑trade time (seconds) to flag as bot activity                | `60`         |
| `BOT_VOLUME_CV_THRESHOLD`           | Maximum coefficient of variation in volume to flag as bot activity                | `0.5`        |
| `VOLUME_ANOMALY_METHOD`             | Volume anomaly method: `mad`, `iqr`, or `zscore`                                  | `mad`        |
| `VOLUME_ANOMALY_THRESHOLD`          | Anomaly score threshold for the selected method                                   | `3.5`        |
| `CLOSED_CLUSTER_INTERNAL_RATIO`     | Minimum fraction of a community's volume that must be internal                    | `0.9`        |
| `CLOSED_CLUSTER_MIN_TRADES`         | Minimum internal trades for a community to be considered                          | `6`          |
| `CLOSED_CLUSTER_MIN_MEMBERS`        | Minimum community size                                                            | `2`          |
| `CLOSED_CLUSTER_MAX_MEMBERS`        | Maximum community size                                                            | `50`         |
| `CLOSED_CLUSTER_BALANCE_TOLERANCE`  | Max per‑member internal in/out volume imbalance                                   | `0.2`        |
| `REPEATED_AMOUNT_MIN_COUNT`         | Repetitions of one (rounded) amount of one token by a trader before flagging       | `5` †        |
| `REPEATED_AMOUNT_SIG_FIGS`          | Significant digits used when grouping repeated amounts                            | `3` †        |
| `ENTITY_SCAN_MAX_BLOCKS`            | On nodes without `trace_filter`, max recent blocks to scan for funding links (one RPC call per block, at most 50,000: about a week of Ethereum, ~8 minutes at the default rate limit); `0` skips that clustering. Each audit reports what was examined in `funding_clusters` | `0` |

### Machine learning

| Variable               | Description                                                                                              | Default |
|------------------------|----------------------------------------------------------------------------------------------------------|---------|
| `ML_CONTAMINATION`     | Expected fraction of outliers in the Isolation Forest                                                    | `0.05`  |
| `ML_TARGET_FPR`        | `wash-train` sets the model's threshold so at most this share of the benchmark's honest trades is flagged | `0.01`  |
| `ML_MIN_PR_AUC_LIFT`   | `wash-train` refuses to save a model whose benchmark PR‑AUC is below this multiple of a random ranking's (`--allow-weak-model` overrides) | `2.0` |

## Operations

| Variable               | Description                                                                                          | Default |
|------------------------|------------------------------------------------------------------------------------------------------|---------|
| `RPC_RETRIES`          | Retries after a failed `eth_getLogs` or `eth_getBlockByNumber` request during ingestion (`RPC_RETRIES + 1` attempts each, 1 s doubling between them); a range still failing then fails the audit | `3`     |
| `INGEST_FULL_BLOCKS`   | Ingest each block with its full transactions: one request per block, each carrying the whole block. `false`: the block's hashes, then one request per swap transaction: about 4× the requests and a tenth of the bytes ([performance.md](performance.md)) | `true`  |
| `RPC_CONCURRENCY`      | Requests one chain's ingestor keeps in flight at once. `RPC_RATE_LIMIT` (requests per second per chain, shared by every audit and monitor in the process) caps their rate | `8`     |
| `TRADE_RETENTION_DAYS` | Default age limit for `wash-prune`, which counts older trades and deletes them only with `--yes`     | `90`    |

Every setting is read by the application (`tests/test_settings_hygiene.py`
fails otherwise).

## USD Pricing

Swaps are valued in USD at ingestion from **reference assets** listed in
`config/tokens.py`: USD stablecoins (valued at $1.00) and wrapped/bridged market
assets such as WETH, WBNB, WPOL and WAVAX (valued at CoinGecko's historical price
nearest the swap). A swap's `volume_usd` comes from its stablecoin leg when it has
one, otherwise from its market-priced leg. Tokens are matched by address, never by
symbol. Legs that cannot be valued are stored as `NULL`, never guessed — this
includes pairs with no reference asset, Curve swaps (whose events identify coins by
index), and chains without a registry entry (currently covered: Ethereum, BNB Smart
Chain, Polygon PoS, Arbitrum One, OP Mainnet, Base, Avalanche C-Chain).

| Variable                      | Description                                                                         | Default     |
|-------------------------------|-------------------------------------------------------------------------------------|-------------|
| `NATIVE_PRICE_SOURCE`         | `coingecko`, or `none` to skip external lookups (stablecoin legs are still valued)  | `coingecko` |
| `COINGECKO_API_KEY`           | Demo or Pro key; the keyless public API is heavily rate-limited                     | (empty)     |
| `COINGECKO_API_URL`           | Use `https://pro-api.coingecko.com/api/v3` with a Pro key                           | public API  |
| `COINGECKO_TIMEOUT_SECONDS`   | Per-request timeout                                                                 | `20`        |
| `PRICE_MAX_STALENESS_MINUTES` | A price is used only if a data point lies within this many minutes of the swap (max 720) | `120`  |

CoinGecko's Demo and keyless plans serve only the past 365 days of history; older
swaps keep a `NULL` USD value on those plans. To value rows ingested before pricing
existed, or whose price was unavailable at ingestion (e.g. throttling), run:

    wash-reprice --chain-id 1 [--pool 0x...]

## API Server

| Variable                    | Description                                                        | Default     |
|-----------------------------|--------------------------------------------------------------------|-------------|
| `API_HOST`                  | Bind address; non-loopback requires auth (enforced at startup)     | `127.0.0.1` |
| `API_PORT`                  | Bind port                                                          | `8000`      |
| `API_AUTH_ENABLED`          | Require `X-API-Key` on all API routes and websockets               | `false`     |
| `API_KEY_HASHES`            | Comma-separated SHA-256 hashes of accepted keys (`wash-genkey`)    | (empty)     |
| `API_CORS_ORIGINS`          | Comma-separated allowed CORS origins (empty = CORS disabled)       | (empty)     |
| `API_RATE_LIMIT_PER_MINUTE` | Per-client-IP request budget                                       | `120`       |
| `RATE_LIMIT_REDIS_URL`      | Redis shared by every instance for limits and lockouts (`redis://` or `rediss://`; empty = per process) | (empty) |
| `AUTH_MAX_FAILURES`         | Wrong API keys from one client before it is locked out             | `5`         |
| `AUTH_FAILURE_WINDOW_SECONDS` | Failures are forgotten this long after the last one              | `900`       |
| `AUTH_LOCKOUT_SECONDS`      | First lockout; doubles with each further failure                   | `60`        |
| `AUTH_LOCKOUT_MAX_SECONDS`  | Longest lockout                                                    | `3600`      |
| `AUDIT_MAX_CONCURRENT`      | Background audits running at once, across instances (more get 429) | `2`         |
| `AUDIT_TASK_TTL_HOURS`      | Finished audit tasks are kept this long                            | `24`        |
| `API_DOCS_ENABLED`          | Serve interactive docs at `/docs`                                  | `true`      |
| `API_HSTS_ENABLED`          | Emit `Strict-Transport-Security` (enable behind TLS proxy only)    | `false`     |
| `PANEL_ENABLED`             | Serve the web panel at `/panel`                                    | `true`      |
| `PANEL_SESSION_TTL_MINUTES` | Panel session cookie lifetime (minutes)                            | `720`       |
| `PANEL_SESSION_SECRET`      | Signs session cookies (32+ characters), the same on every instance; empty = random per process | (empty) |

## Health Probes

See [api.md](api.md#health-probes).

| Variable                       | Description                                                                    | Default |
|--------------------------------|--------------------------------------------------------------------------------|---------|
| `HEALTH_RPC_CHAIN_IDS`         | Comma-separated chains whose RPC endpoint `/health/ready` requires (empty = database only) | `1` |
| `HEALTH_CHECK_TIMEOUT_SECONDS` | Time limit of each readiness check                                              | `5`     |
| `HEALTH_RPC_CACHE_SECONDS`     | How long an RPC check's answer is reused (0 = probe every time)                 | `15`    |

## Real-Time Monitor

| Variable                        | Description                                    | Default |
|---------------------------------|------------------------------------------------|---------|
| `MONITOR_POLL_INTERVAL_SECONDS` | Chain-head poll cadence for websocket monitors | `12`    |
| `MONITOR_WINDOW_MINUTES`        | Rolling detection window size                  | `60`    |
| `MONITOR_POOLS`                 | `chain_id:pool` pairs the server watches continuously, alerting the sinks below | (empty) |

## Alerting

See [alerting.md](alerting.md). Each sink is on when its address is set.

| Variable                  | Description                                                        | Default    |
|---------------------------|--------------------------------------------------------------------|------------|
| `ALERT_WEBHOOK_URL`       | Webhook receiving each alert as JSON (http or https)               | (empty)    |
| `ALERT_WEBHOOK_SECRET`    | Signs webhook requests with HMAC-SHA256 (`X-Wash-Signature`)       | (empty)    |
| `ALERT_SLACK_WEBHOOK_URL` | Slack incoming webhook (https)                                     | (empty)    |
| `ALERT_EMAIL_TO`          | Comma-separated recipients                                         | (empty)    |
| `ALERT_EMAIL_FROM`        | Sender address (required with `ALERT_EMAIL_TO`)                    | (empty)    |
| `ALERT_SMTP_HOST`         | SMTP server (required with `ALERT_EMAIL_TO`)                       | (empty)    |
| `ALERT_SMTP_PORT`         | SMTP port                                                          | `587`      |
| `ALERT_SMTP_USERNAME`     | SMTP login (none if empty)                                         | (empty)    |
| `ALERT_SMTP_PASSWORD`     | SMTP password                                                      | (empty)    |
| `ALERT_SMTP_TLS`          | `starttls`, `ssl` or `none`                                        | `starttls` |
| `ALERT_TIMEOUT_SECONDS`   | Time limit of each delivery attempt                                | `10`       |
| `ALERT_MAX_ATTEMPTS`      | Attempts per alert and sink before it is given up                  | `3`        |

## Logging

| Variable     | Description                                                                 | Default |
|--------------|-----------------------------------------------------------------------------|---------|
| `LOG_LEVEL`  | Standard Python logging level, for every command                            | `INFO`  |
| `LOG_FORMAT` | `text`, or `json` (one object per line, with request and trace ids)         | `text` (`json` in the container image) |

Fields, metrics and tracing: [observability.md](observability.md). Tracing is
configured with the standard OpenTelemetry variables listed there.

## Model Storage

`ML_MODEL_PATH` – Path where the trained ML model is saved/loaded.  
Default: `models/ml_model.pkl`
