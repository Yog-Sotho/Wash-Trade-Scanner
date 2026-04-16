"""
Global settings for the wash trade detection system.
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL", "postgresql+asyncpg://wash_user:wash_pass@db:5432/wash_detector"
    )
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    
    # Network Settings
    RPC_RATE_LIMIT: int = int(os.getenv("RPC_RATE_LIMIT", "10"))
    RPC_TIMEOUT: int = int(os.getenv("RPC_TIMEOUT", "60"))
    RPC_RETRIES: int = int(os.getenv("RPC_RETRIES", "3"))
    
    # Detection Settings
    WASH_TRADE_TIME_WINDOW_MINUTES: int = int(os.getenv("WASH_TRADE_TIME_WINDOW_MINUTES", "60"))
    MIN_WASH_TRADE_VOLUME_USD: float = float(os.getenv("MIN_WASH_TRADE_VOLUME_USD", "100.0"))
    SUSPICIOUS_ACTIVITY_THRESHOLD: float = float(os.getenv("SUSPICIOUS_ACTIVITY_THRESHOLD", "0.85"))
    
    # Heuristic Weights
    HEURISTIC_WEIGHTS: dict = {
        "self_trading": 0.30,
        "circular_trading": 0.25,
        "high_frequency_bot": 0.20,
        "volume_anomaly": 0.15,
        "wash_cluster": 0.10,
    }
    
    # ML Settings
    ML_MODEL_PATH: str = os.getenv("ML_MODEL_PATH", "./models/ml_model.pkl")
    ML_CONTAMINATION: float = float(os.getenv("ML_CONTAMINATION", "0.05"))
    
    # Data Retention
    TRADE_RETENTION_DAYS: int = int(os.getenv("TRADE_RETENTION_DAYS", "90"))
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

settings = Settings()

# Configure root logger
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler()]
)
