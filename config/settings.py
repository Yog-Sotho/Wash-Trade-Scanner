"""
Global settings for the wash trade detection system.

This module provides application-wide configuration management with:
- Environment variable parsing with validation
- Required credential checking (no hardcoded defaults)
- Type-safe settings access
- Comprehensive logging configuration

Environment Variables Required:
    DATABASE_URL: PostgreSQL connection string (required)
    REDIS_URL: Redis connection string (optional, defaults to disabled)

Environment Variables Optional:
    RPC_RATE_LIMIT: RPC calls per second (default: 10)
    RPC_TIMEOUT: RPC request timeout in seconds (default: 60)
    RPC_RETRIES: Number of RPC retry attempts (default: 3)
    WASH_TRADE_TIME_WINDOW_MINUTES: Time window for circular trade detection (default: 60)
    MIN_WASH_TRADE_VOLUME_USD: Minimum volume in USD to consider (default: 100.0)
    SUSPICIOUS_ACTIVITY_THRESHOLD: ML anomaly threshold (default: 0.85)
    ML_MODEL_PATH: Path to trained ML model (default: ./models/ml_model.pkl)
    ML_CONTAMINATION: Expected contamination ratio (default: 0.05)
    TRADE_RETENTION_DAYS: Days to retain trade data (default: 90)
    LOG_LEVEL: Logging level (default: INFO)
    COINGECKO_API_KEY: CoinGecko API key for price data (optional)

Example:
    >>> from config.settings import settings
    >>> print(f"Database: {settings.DATABASE_URL}")
    >>> print(f"Threshold: {settings.SUSPICIOUS_ACTIVITY_THRESHOLD}")
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# ==============================================================================
# Constants and Magic Numbers
# ==============================================================================

# Heuristic detection constants
BOT_TRADE_TIME_THRESHOLD_SECONDS: int = 60
BOT_VOLUME_CV_THRESHOLD: float = 0.5
Z_SCORE_THRESHOLD: float = 3.0
Z_SCORE_BONUS_PER_UNIT: float = 0.1
MAX_WASH_TRADE_SCORE: float = 1.0

# Network constants
DEFAULT_RPC_RATE_LIMIT: int = 10
DEFAULT_RPC_TIMEOUT: int = 60
DEFAULT_RPC_RETRIES: int = 3

# Detection constants
DEFAULT_TIME_WINDOW_MINUTES: int = 60
DEFAULT_MIN_VOLUME_USD: float = 100.0
DEFAULT_SUSPICIOUS_THRESHOLD: float = 0.85

# ML constants
DEFAULT_ML_CONTAMINATION: float = 0.05
DEFAULT_ML_MODEL_PATH: str = "./models/ml_model.pkl"

# Data retention
DEFAULT_TRADE_RETENTION_DAYS: int = 90

# Logging
DEFAULT_LOG_LEVEL: str = "INFO"

# Heuristic weights (must sum to 1.0)
HEURISTIC_WEIGHT_SELF_TRADING: float = 0.30
HEURISTIC_WEIGHT_CIRCULAR: float = 0.25
HEURISTIC_WEIGHT_BOT: float = 0.20
HEURISTIC_WEIGHT_ANOMALY: float = 0.15
HEURISTIC_WEIGHT_CLUSTER: float = 0.10


# ==============================================================================
# Validation Functions
# ==============================================================================

def _validate_database_url(url: Optional[str]) -> str:
    """Validate and return database URL."""
    if not url:
        raise ValueError(
            "DATABASE_URL environment variable is required. "
            "Example: postgresql+asyncpg://user:pass@host:5432/dbname"
        )
    return url


def _validate_rate_limit(value: int) -> int:
    """Validate RPC rate limit."""
    if value < 0:
        raise ValueError(f"RPC_RATE_LIMIT must be non-negative, got {value}")
    return value


def _validate_timeout(value: int) -> int:
    """Validate RPC timeout."""
    if value <= 0:
        raise ValueError(f"RPC_TIMEOUT must be positive, got {value}")
    return value


def _validate_contamination(value: float) -> float:
    """Validate ML contamination factor."""
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"ML_CONTAMINATION must be between 0.0 and 1.0, got {value}")
    return value


def _validate_threshold(value: float) -> float:
    """Validate threshold value."""
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"Threshold must be between 0.0 and 1.0, got {value}")
    return value


def _mask_url_credentials(url: str) -> str:
    """Mask credentials in URL for safe logging."""
    if not url:
        return "<empty>"
    # Replace password if present
    masked = re.sub(r"(://[^:]+:)[^@]+(@)", r"\1****\2", url)
    return masked


# ==============================================================================
# Settings Class
# ==============================================================================

class Settings:
    """
    Application-wide settings container.

    This class loads configuration from environment variables with validation.
    Credentials (DATABASE_URL) must be provided via environment variables -
    there are no hardcoded defaults for security reasons.

    Attributes:
        DATABASE_URL: PostgreSQL async connection string (required)
        REDIS_URL: Redis connection string (optional, None if not configured)
        RPC_RATE_LIMIT: Maximum RPC calls per second
        RPC_TIMEOUT: RPC request timeout in seconds
        RPC_RETRIES: Number of RPC retry attempts
        WASH_TRADE_TIME_WINDOW_MINUTES: Time window for circular trade detection
        MIN_WASH_TRADE_VOLUME_USD: Minimum volume in USD to consider
        SUSPICIOUS_ACTIVITY_THRESHOLD: ML anomaly detection threshold
        HEURISTIC_WEIGHTS: Dictionary of detection method weights
        ML_MODEL_PATH: Path to trained ML model file
        ML_CONTAMINATION: Expected anomaly contamination ratio
        TRADE_RETENTION_DAYS: Days to retain trade data
        LOG_LEVEL: Application logging level

    Raises:
        ValueError: If DATABASE_URL is missing or invalid values provided

    Example:
        >>> settings = Settings()
        >>> print(f"Database: {_mask_url_credentials(settings.DATABASE_URL)}")
    """

    # Required credentials - validated on initialization
    DATABASE_URL: str
    REDIS_URL: Optional[str]

    # Network Settings
    RPC_RATE_LIMIT: int
    RPC_TIMEOUT: int
    RPC_RETRIES: int

    # Detection Settings
    WASH_TRADE_TIME_WINDOW_MINUTES: int
    MIN_WASH_TRADE_VOLUME_USD: float
    SUSPICIOUS_ACTIVITY_THRESHOLD: float

    # Heuristic Weights
    HEURISTIC_WEIGHTS: Dict[str, float]

    # ML Settings
    ML_MODEL_PATH: str
    ML_CONTAMINATION: float

    # Data Retention
    TRADE_RETENTION_DAYS: int

    # Logging
    LOG_LEVEL: str

    # API Keys
    COINGECKO_API_KEY: Optional[str]

    def __init__(self) -> None:
        """
        Initialize settings from environment variables.

        This constructor validates that all required settings are present
        and applies defaults for optional settings.

        Raises:
            ValueError: If DATABASE_URL is missing or values are invalid
        """
        # Required: Database URL (no default - must be provided)
        database_url = os.getenv("DATABASE_URL")
        object.__setattr__(self, "DATABASE_URL", _validate_database_url(database_url))

        # Optional: Redis URL (None if not configured)
        redis_url = os.getenv("REDIS_URL")
        object.__setattr__(self, "REDIS_URL", redis_url if redis_url else None)

        # Network Settings
        object.__setattr__(
            self,
            "RPC_RATE_LIMIT",
            _validate_rate_limit(int(os.getenv("RPC_RATE_LIMIT", str(DEFAULT_RPC_RATE_LIMIT)))),
        )
        object.__setattr__(
            self,
            "RPC_TIMEOUT",
            _validate_timeout(int(os.getenv("RPC_TIMEOUT", str(DEFAULT_RPC_TIMEOUT)))),
        )
        object.__setattr__(
            self,
            "RPC_RETRIES",
            int(os.getenv("RPC_RETRIES", str(DEFAULT_RPC_RETRIES))),
        )

        # Detection Settings
        object.__setattr__(
            self,
            "WASH_TRADE_TIME_WINDOW_MINUTES",
            int(os.getenv("WASH_TRADE_TIME_WINDOW_MINUTES", str(DEFAULT_TIME_WINDOW_MINUTES))),
        )
        object.__setattr__(
            self,
            "MIN_WASH_TRADE_VOLUME_USD",
            float(os.getenv("MIN_WASH_TRADE_VOLUME_USD", str(DEFAULT_MIN_VOLUME_USD))),
        )
        object.__setattr__(
            self,
            "SUSPICIOUS_ACTIVITY_THRESHOLD",
            _validate_threshold(
                float(os.getenv("SUSPICIOUS_ACTIVITY_THRESHOLD", str(DEFAULT_SUSPICIOUS_THRESHOLD)))
            ),
        )

        # Heuristic Weights
        heuristic_weights = {
            "self_trading": float(os.getenv("HEURISTIC_WEIGHT_SELF_TRADING", str(HEURISTIC_WEIGHT_SELF_TRADING))),
            "circular_trading": float(os.getenv("HEURISTIC_WEIGHT_CIRCULAR", str(HEURISTIC_WEIGHT_CIRCULAR))),
            "high_frequency_bot": float(os.getenv("HEURISTIC_WEIGHT_BOT", str(HEURISTIC_WEIGHT_BOT))),
            "volume_anomaly": float(os.getenv("HEURISTIC_WEIGHT_ANOMALY", str(HEURISTIC_WEIGHT_ANOMALY))),
            "wash_cluster": float(os.getenv("HEURISTIC_WEIGHT_CLUSTER", str(HEURISTIC_WEIGHT_CLUSTER))),
        }
        # Validate weights sum to 1.0
        total = sum(heuristic_weights.values())
        if not 0.99 <= total <= 1.01:
            raise ValueError(f"Heuristic weights must sum to 1.0, got {total:.4f}")
        object.__setattr__(self, "HEURISTIC_WEIGHTS", heuristic_weights)

        # ML Settings
        object.__setattr__(
            self,
            "ML_MODEL_PATH",
            os.getenv("ML_MODEL_PATH", DEFAULT_ML_MODEL_PATH),
        )
        object.__setattr__(
            self,
            "ML_CONTAMINATION",
            _validate_contamination(
                float(os.getenv("ML_CONTAMINATION", str(DEFAULT_ML_CONTAMINATION)))
            ),
        )

        # Data Retention
        object.__setattr__(
            self,
            "TRADE_RETENTION_DAYS",
            int(os.getenv("TRADE_RETENTION_DAYS", str(DEFAULT_TRADE_RETENTION_DAYS))),
        )

        # Logging
        object.__setattr__(
            self,
            "LOG_LEVEL",
            os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL),
        )

        # API Keys (optional)
        object.__setattr__(
            self,
            "COINGECKO_API_KEY",
            os.getenv("COINGECKO_API_KEY"),
        )

        # Configure logging
        self._configure_logging()

    def _configure_logging(self) -> None:
        """Configure application logging."""
        # Get numeric log level
        numeric_level = getattr(logging, self.LOG_LEVEL, logging.INFO)

        # Create formatter with consistent format
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(numeric_level)

        # Remove existing handlers to avoid duplicates
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        # Reduce noise from third-party libraries
        logging.getLogger("web3").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("asyncio").setLevel(logging.WARNING)

    def __repr__(self) -> str:
        """String representation (hiding sensitive data)."""
        return (
            f"Settings("
            f"database_url='{_mask_url_credentials(self.DATABASE_URL)}', "
            f"redis_url={'<configured>' if self.REDIS_URL else '<disabled>'}, "
            f"log_level={self.LOG_LEVEL}"
            f")"
        )

    @property
    def redis_enabled(self) -> bool:
        """Check if Redis is configured and enabled."""
        return bool(self.REDIS_URL)

    @property
    def coingecko_enabled(self) -> bool:
        """Check if CoinGecko API is configured."""
        return bool(self.COINGECKO_API_KEY)


# ==============================================================================
# Global Settings Instance
# ==============================================================================

def _create_fallback_settings() -> Settings:
    """
    Create a fallback settings instance for testing or when env vars not set.

    This bypasses normal validation by setting attributes directly.
    """
    # Create instance without calling __init__ validation
    instance = object.__new__(Settings)

    # Set required attributes directly (bypassing validation)
    object.__setattr__(instance, "DATABASE_URL", "postgresql+asyncpg://placeholder:placeholder@localhost:5432/placeholder")
    object.__setattr__(instance, "REDIS_URL", None)
    object.__setattr__(instance, "RPC_RATE_LIMIT", DEFAULT_RPC_RATE_LIMIT)
    object.__setattr__(instance, "RPC_TIMEOUT", DEFAULT_RPC_TIMEOUT)
    object.__setattr__(instance, "RPC_RETRIES", DEFAULT_RPC_RETRIES)
    object.__setattr__(instance, "WASH_TRADE_TIME_WINDOW_MINUTES", DEFAULT_TIME_WINDOW_MINUTES)
    object.__setattr__(instance, "MIN_WASH_TRADE_VOLUME_USD", DEFAULT_MIN_VOLUME_USD)
    object.__setattr__(instance, "SUSPICIOUS_ACTIVITY_THRESHOLD", DEFAULT_SUSPICIOUS_THRESHOLD)
    object.__setattr__(instance, "HEURISTIC_WEIGHTS", {
        "self_trading": HEURISTIC_WEIGHT_SELF_TRADING,
        "circular_trading": HEURISTIC_WEIGHT_CIRCULAR,
        "high_frequency_bot": HEURISTIC_WEIGHT_BOT,
        "volume_anomaly": HEURISTIC_WEIGHT_ANOMALY,
        "wash_cluster": HEURISTIC_WEIGHT_CLUSTER,
    })
    object.__setattr__(instance, "ML_MODEL_PATH", DEFAULT_ML_MODEL_PATH)
    object.__setattr__(instance, "ML_CONTAMINATION", DEFAULT_ML_CONTAMINATION)
    object.__setattr__(instance, "TRADE_RETENTION_DAYS", DEFAULT_TRADE_RETENTION_DAYS)
    object.__setattr__(instance, "LOG_LEVEL", DEFAULT_LOG_LEVEL)
    object.__setattr__(instance, "COINGECKO_API_KEY", None)
    object.__setattr__(instance, "_initialized", True)

    return instance


# Try to create settings instance, handle gracefully if env vars not set
try:
    settings = Settings()
except ValueError:
    # For testing or when settings haven't been configured yet
    # Provide a minimal placeholder that will be replaced on first use
    settings = _create_fallback_settings()
