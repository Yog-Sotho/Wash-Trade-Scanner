"""
Global settings for the wash trade detection system.
No hardcoded credentials; sensitive values must come from environment.
Magic numbers moved here for heuristics.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # Database – must be set in .env, no default credentials
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is required")

    # Redis removed as it is not used
    RPC_RATE_LIMIT: int = int(os.getenv("RPC_RATE_LIMIT", "100"))
    RPC_TIMEOUT: int = int(os.getenv("RPC_TIMEOUT", "30"))
    RPC_RETRIES: int = int(os.getenv("RPC_RETRIES", "3"))
    WASH_TRADE_TIME_WINDOW_MINUTES: int = int(os.getenv("WASH_TRADE_TIME_WINDOW_MINUTES", "60"))
    MIN_WASH_TRADE_VOLUME_USD: float = float(os.getenv("MIN_WASH_TRADE_VOLUME_USD", "1000"))
    SUSPICIOUS_ACTIVITY_THRESHOLD: float = float(os.getenv("SUSPICIOUS_ACTIVITY_THRESHOLD", "0.8"))
    HEURISTIC_WEIGHTS: dict = {
        "self_trading": 0.30,
        "circular_trading": 0.25,
        "high_frequency_bot": 0.20,
        "volume_anomaly": 0.15,
        "wash_cluster": 0.10,
    }
    ML_MODEL_PATH: str = os.getenv("ML_MODEL_PATH", "models/ml_model.pkl")
    ML_CONTAMINATION: float = float(os.getenv("ML_CONTAMINATION", "0.05"))
    TRADE_RETENTION_DAYS: int = int(os.getenv("TRADE_RETENTION_DAYS", "90"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    COINGECKO_API_KEY: str = os.getenv("COINGECKO_API_KEY", "")
    COINGECKO_API_URL: str = "https://api.coingecko.com/api/v3"

    # Heuristic magic numbers moved here
    BOT_TRADE_TIME_THRESHOLD: int = 60  # seconds
    BOT_VOLUME_CV_THRESHOLD: float = 0.5
    Z_SCORE_THRESHOLD: float = 3.0
    BOT_ALLOWLIST: str = os.getenv("BOT_ALLOWLIST", "")


settings = Settings()
