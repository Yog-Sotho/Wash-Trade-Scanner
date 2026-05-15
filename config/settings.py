"""
Global settings for the wash trade detection system.
Uses Pydantic for validation. No hardcoded credentials.
"""

import os
from typing import Optional, Set
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from sqlalchemy.engine import URL


class Settings(BaseSettings):
    """Validated application settings with secure defaults."""

    # Database — separate parameters, SSL enforced
    DATABASE_HOST: str = Field(..., description="PostgreSQL host")
    DATABASE_PORT: int = Field(5432, ge=1, le=65535)
    DATABASE_NAME: str = Field(..., description="PostgreSQL database name")
    DATABASE_USER: str = Field(..., description="PostgreSQL username")
    DATABASE_PASSWORD: str = Field(..., description="PostgreSQL password")
    DATABASE_SSL_MODE: str = Field("require", pattern="^(disable|allow|prefer|require|verify-ca|verify-full)$")

    # Redis
    REDIS_URL: str = Field("redis://localhost:6379/0")

    # RPC Configuration
    RPC_RATE_LIMIT: int = Field(100, ge=1, le=10000)
    RPC_TIMEOUT: int = Field(30, ge=1, le=300)
    RPC_RETRIES: int = Field(3, ge=0, le=10)
    RPC_MAX_FAILURES: int = Field(5, ge=1, le=100)
    RPC_RECOVERY_TIMEOUT: float = Field(30.0, ge=1.0, le=600.0)

    # Per-chain RPC URLs
    ETH_RPC_URL: str = Field("", description="Ethereum RPC URL")
    BSC_RPC_URL: str = Field("https://bsc-dataseed1.binance.org")
    POLYGON_RPC_URL: str = Field("https://polygon-rpc.com")
    ARBITRUM_RPC_URL: str = Field("https://arb1.arbitrum.io/rpc")
    OPTIMISM_RPC_URL: str = Field("https://mainnet.optimism.io")
    BASE_RPC_URL: str = Field("https://mainnet.base.org")
    AVALANCHE_RPC_URL: str = Field("https://api.avax.network/ext/bc/C/rpc")
    FANTOM_RPC_URL: str = Field("https://rpc.ftm.tools")
    CELO_RPC_URL: str = Field("https://forno.celo.org")
    GNOSIS_RPC_URL: str = Field("https://rpc.gnosischain.com")
    MOONBEAM_RPC_URL: str = Field("https://rpc.api.moonbeam.network")
    AURORA_RPC_URL: str = Field("https://mainnet.aurora.dev")
    HARMONY_RPC_URL: str = Field("https://api.harmony.one")
    CRONOS_RPC_URL: str = Field("https://evm.cronos.org")
    METIS_RPC_URL: str = Field("https://andromeda.metis.io/?owner=1088")
    BOBA_RPC_URL: str = Field("https://mainnet.boba.network")
    ZKSYNC_RPC_URL: str = Field("https://mainnet.era.zksync.io")
    POLYGON_ZKEVM_RPC_URL: str = Field("https://zkevm-rpc.com")
    LINEA_RPC_URL: str = Field("https://rpc.linea.build")
    SCROLL_RPC_URL: str = Field("https://rpc.scroll.io")
    MANTLE_RPC_URL: str = Field("https://rpc.mantle.xyz")
    KAVA_RPC_URL: str = Field("https://evm.kava.io")
    KLAYTN_RPC_URL: str = Field("https://public-node-api.klaytnapi.com/v1/cypress")

    # Detection Parameters
    WASH_TRADE_TIME_WINDOW_MINUTES: int = Field(60, ge=1, le=10080)
    MIN_WASH_TRADE_VOLUME_USD: float = Field(1000.0, ge=0.0)
    SUSPICIOUS_ACTIVITY_THRESHOLD: float = Field(0.8, ge=0.0, le=1.0)

    # Heuristic Weights
    HEURISTIC_WEIGHTS: dict = {
        "self_trading": 0.30,
        "circular_trading": 0.25,
        "high_frequency_bot": 0.20,
        "volume_anomaly": 0.15,
        "wash_cluster": 0.10,
    }

    # Bot Detection — configurable thresholds
    BOT_TRADE_COUNT_THRESHOLD: int = Field(10, ge=2, le=1000)
    BOT_TRADE_TIME_THRESHOLD_SECONDS: float = Field(60.0, ge=1.0, le=3600.0)
    BOT_VOLUME_CV_THRESHOLD: float = Field(0.5, ge=0.0, le=10.0)

    # Volume Anomaly — MAD-based instead of z-score
    VOLUME_ANOMALY_THRESHOLD: float = Field(3.5, ge=1.0, le=10.0)
    VOLUME_ANOMALY_METHOD: str = Field("mad", pattern="^(mad|iqr|zscore)$")
    VOLUME_ANOMALY_BUCKET_MINUTES: int = Field(60, ge=5, le=1440)
    VOLUME_ANOMALY_MIN_TRADES: int = Field(5, ge=2, le=100)

    # ML Model
    ML_MODEL_PATH: str = Field("models/ml_model.pkl")
    ML_CONTAMINATION: float = Field(0.05, ge=0.001, le=0.5)
    ML_EXPLAINABILITY: bool = Field(True)

    # Data Retention
    TRADE_RETENTION_DAYS: int = Field(90, ge=1, le=3650)

    # Logging
    LOG_LEVEL: str = Field("INFO", pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")

    # External APIs
    COINGECKO_API_KEY: str = Field("")
    COINGECKO_API_URL: str = "https://api.coingecko.com/api/v3"

    # Bot Allowlist
    BOT_ALLOWLIST: str = Field("")

    @field_validator("DATABASE_PASSWORD", mode="before")
    @classmethod
    def validate_password_not_empty(cls, v: str) -> str:
        if not v or len(v.strip()) < 8:
            raise ValueError("DATABASE_PASSWORD must be at least 8 characters")
        return v

    @field_validator("ETH_RPC_URL", "BSC_RPC_URL", "POLYGON_RPC_URL", mode="before")
    @classmethod
    def validate_no_placeholder(cls, v: str) -> str:
        if "YOUR_KEY" in v or "placeholder" in v.lower():
            raise ValueError(f"RPC URL contains placeholder: {v}")
        return v

    @property
    def DATABASE_URL(self) -> URL:
        """Build secure async PostgreSQL URL object with masked password."""
        return URL.create(
            drivername="postgresql+asyncpg",
            username=self.DATABASE_USER,
            password=self.DATABASE_PASSWORD,
            host=self.DATABASE_HOST,
            port=self.DATABASE_PORT,
            database=self.DATABASE_NAME,
            query={"ssl": self.DATABASE_SSL_MODE},
        )

    @property
    def bot_allowlist_set(self) -> Set[str]:
        """Parse allowlist into lowercase set."""
        if not self.BOT_ALLOWLIST:
            return set()
        return {addr.strip().lower() for addr in self.BOT_ALLOWLIST.split(",") if addr.strip()}

    @property
    def rpc_urls(self) -> dict:
        """Map chain IDs to RPC URLs."""
        return {
            1: self.ETH_RPC_URL,
            56: self.BSC_RPC_URL,
            137: self.POLYGON_RPC_URL,
            42161: self.ARBITRUM_RPC_URL,
            10: self.OPTIMISM_RPC_URL,
            8453: self.BASE_RPC_URL,
            43114: self.AVALANCHE_RPC_URL,
            250: self.FANTOM_RPC_URL,
            42220: self.CELO_RPC_URL,
            100: self.GNOSIS_RPC_URL,
            1284: self.MOONBEAM_RPC_URL,
            1313161554: self.AURORA_RPC_URL,
            1666600000: self.HARMONY_RPC_URL,
            25: self.CRONOS_RPC_URL,
            1088: self.METIS_RPC_URL,
            288: self.BOBA_RPC_URL,
            324: self.ZKSYNC_RPC_URL,
            1101: self.POLYGON_ZKEVM_RPC_URL,
            59144: self.LINEA_RPC_URL,
            534352: self.SCROLL_RPC_URL,
            5000: self.MANTLE_RPC_URL,
            2222: self.KAVA_RPC_URL,
            8217: self.KLAYTN_RPC_URL,
        }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


settings = Settings()
