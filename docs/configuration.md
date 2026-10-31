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

| Variable                         | Description                                                                 | Default   |
|----------------------------------|-----------------------------------------------------------------------------|-----------|
| `WASH_TRADE_TIME_WINDOW_MINUTES` | Time window (in minutes) for circular trading detection                     | `60`      |
| `MIN_WASH_TRADE_VOLUME_USD`      | Minimum USD volume to consider a trade as potential wash trade              | `1000`    |
| `ML_CONTAMINATION`               | Expected fraction of outliers in Isolation Forest                           | `0.05`    |
| `BOT_ALLOWLIST`                  | Comma‑separated list of addresses to skip in high‑frequency bot detection   | (empty)   |
| `BOT_TRADE_TIME_THRESHOLD`       | Maximum average inter‑trade time (seconds) to flag as bot activity          | `60`      |
| `BOT_VOLUME_CV_THRESHOLD`        | Maximum coefficient of variation in volume to flag as bot activity          | `0.5`     |
| `VOLUME_ANOMALY_METHOD`          | Volume anomaly method: `mad`, `iqr`, or `zscore`                            | `mad`     |
| `VOLUME_ANOMALY_THRESHOLD`       | Anomaly score threshold for the selected method                             | `3.5`     |

## Advanced Detection Parameters

Research‑grade detectors in `core/advanced_heuristics.py`:

| Variable                            | Description                                                                       | Default      |
|-------------------------------------|-----------------------------------------------------------------------------------|--------------|
| `POSITION_NEUTRAL_WINDOWS_HOURS`    | Multi‑pass window sizes (hours) for position‑neutral SCC analysis                 | `[1,24,168]` |
| `POSITION_NEUTRAL_MARGIN`           | Max net‑position change relative to gross volume to call a window position‑neutral | `0.01`       |
| `POSITION_NEUTRAL_MIN_TRADES`       | Minimum trades in a window before it can be flagged                               | `4`          |
| `CLOSED_CLUSTER_INTERNAL_RATIO`     | Minimum fraction of a community's volume that must be internal                    | `0.9`        |
| `CLOSED_CLUSTER_MIN_TRADES`         | Minimum internal trades for a community to be considered                          | `6`          |
| `CLOSED_CLUSTER_MIN_MEMBERS`        | Minimum community size                                                            | `2`          |
| `CLOSED_CLUSTER_MAX_MEMBERS`        | Maximum community size                                                            | `50`         |
| `CLOSED_CLUSTER_BALANCE_TOLERANCE`  | Max per‑member internal in/out volume imbalance                                   | `0.2`        |
| `REPEATED_AMOUNT_MIN_COUNT`         | Repetitions of one (rounded) amount by a sender before flagging                   | `5`          |
| `REPEATED_AMOUNT_SIG_FIGS`          | Significant digits used when grouping repeated amounts                            | `3`          |
| `ENTITY_SCAN_MAX_BLOCKS`            | On nodes without `trace_filter`, max recent blocks to scan for funding links (one RPC call per block); `0` skips that clustering | `0` |

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
| `API_DOCS_ENABLED`          | Serve interactive docs at `/docs`                                  | `true`      |
| `API_HSTS_ENABLED`          | Emit `Strict-Transport-Security` (enable behind TLS proxy only)    | `false`     |
| `PANEL_ENABLED`             | Serve the web panel at `/panel`                                    | `true`      |
| `PANEL_SESSION_TTL_MINUTES` | Panel session cookie lifetime (minutes)                            | `720`       |

## Real-Time Monitor

| Variable                        | Description                                    | Default |
|---------------------------------|------------------------------------------------|---------|
| `MONITOR_POLL_INTERVAL_SECONDS` | Chain-head poll cadence for websocket monitors | `12`    |
| `MONITOR_WINDOW_MINUTES`        | Rolling detection window size                  | `60`    |

## Logging

`LOG_LEVEL` – Standard Python logging levels. Default `INFO`.

## Model Storage

`ML_MODEL_PATH` – Path where the trained ML model is saved/loaded.  
Default: `models/ml_model.pkl`
