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
| Ethereum          | `ETH_RPC_URL`          | `https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY`             |
| BSC               | `BSC_RPC_URL`          | `https://bsc-dataseed1.binance.org`                         |
| Polygon           | `POLYGON_RPC_URL`      | `https://polygon-rpc.com`                                   |
| Arbitrum          | `ARBITRUM_RPC_URL`     | `https://arb1.arbitrum.io/rpc`                              |
| Optimism          | `OPTIMISM_RPC_URL`     | `https://mainnet.optimism.io`                               |
| Base              | `BASE_RPC_URL`         | `https://mainnet.base.org`                                  |
| Avalanche         | `AVALANCHE_RPC_URL`    | `https://api.avax.network/ext/bc/C/rpc`                     |
| Fantom            | `FANTOM_RPC_URL`       | `https://rpc.ftm.tools`                                     |
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
| Klaytn            | `KLAYTN_RPC_URL`       | `https://public-node-api.klaytnapi.com/v1/cypress`          |

If an RPC URL contains the placeholder `YOUR_KEY`, the system will raise an error to prevent silent failures.

## Detection Parameters

| Variable                         | Description                                                                 | Default   |
|----------------------------------|-----------------------------------------------------------------------------|-----------|
| `WASH_TRADE_TIME_WINDOW_MINUTES` | Time window (in minutes) for circular trading detection                     | `60`      |
| `MIN_WASH_TRADE_VOLUME_USD`      | Minimum USD volume to consider a trade as potential wash trade              | `1000`    |
| `ML_CONTAMINATION`               | Expected fraction of outliers in Isolation Forest                           | `0.05`    |
| `BOT_ALLOWLIST`                  | Comma‑separated list of addresses to skip in high‑frequency bot detection   | (empty)   |
| `BOT_TRADE_TIME_THRESHOLD`       | Maximum average inter‑trade time (seconds) to flag as bot activity          | `60`      |
| `BOT_VOLUME_CV_THRESHOLD`        | Maximum coefficient of variation in volume to flag as bot activity          | `0.5`     |
| `Z_SCORE_THRESHOLD`              | Z‑score threshold for volume anomaly detection                              | `3.0`     |

## Logging

`LOG_LEVEL` – Standard Python logging levels. Default `INFO`.

## Model Storage

`ML_MODEL_PATH` – Path where the trained ML model is saved/loaded.  
Default: `models/ml_model.pkl`
