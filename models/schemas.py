"""
SQLAlchemy and Pydantic models for the database.

This module provides:
- SQLAlchemy ORM models for PostgreSQL tables
- Pydantic models for API request/response validation
- Database schema definitions for all entities
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import ConfigDict, BaseModel, Field
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    JSON,
    String,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


# ==============================================================================
# SQLAlchemy ORM Models
# ==============================================================================

class SwapTrade(Base):
    """Swap trade record from DEX events."""

    __tablename__ = "swap_trades"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chain_id = Column(Integer, nullable=False, index=True)
    dex_name = Column(String(50), nullable=False, index=True)
    pool_address = Column(String(42), nullable=False, index=True)
    token_in = Column(String(42), nullable=False, index=True)
    token_out = Column(String(42), nullable=False, index=True)
    amount_in = Column(Float, nullable=False)
    amount_out = Column(Float, nullable=False)
    sender = Column(String(42), nullable=False, index=True)
    recipient = Column(String(42), nullable=False, index=True)
    transaction_hash = Column(String(66), nullable=False, index=True)
    block_number = Column(BigInteger, nullable=False)
    block_timestamp = Column(DateTime, nullable=False, index=True)
    gas_price = Column(Float, nullable=True)
    gas_used = Column(Float, nullable=True)
    log_index = Column(Integer, nullable=False)
    amount_in_usd = Column(Float, nullable=True)
    amount_out_usd = Column(Float, nullable=True)
    volume_usd = Column(Float, nullable=True)
    is_wash_trade = Column(Boolean, default=False, index=True)
    wash_trade_score = Column(Float, default=0.0)
    detection_method = Column(String(50), nullable=True)

    __table_args__ = (
        Index("ix_swap_trades_pool_timestamp", "pool_address", "block_timestamp"),
        Index("ix_swap_trades_sender_timestamp", "sender", "block_timestamp"),
        Index("ix_swap_trades_chain_dex_pool", "chain_id", "dex_name", "pool_address"),
        Index("ix_swap_trades_tx_log", "transaction_hash", "log_index", unique=True),
    )


class AddressCluster(Base):
    """Address cluster for entity clustering."""

    __tablename__ = "address_clusters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cluster_id = Column(String(64), nullable=False, index=True)
    addresses = Column(JSON, nullable=False)
    creation_date = Column(DateTime, server_default=func.now())
    last_updated = Column(DateTime, onupdate=func.now())
    confidence_score = Column(Float, default=0.0)
    evidence = Column(JSON, nullable=True)

    __table_args__ = (Index("ix_address_clusters_cluster_id", "cluster_id"),)


class TokenRiskProfile(Base):
    """Token risk profile based on detected wash trading."""

    __tablename__ = "token_risk_profiles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chain_id = Column(Integer, nullable=False, index=True)
    pool_address = Column(String(42), nullable=False, index=True)
    token_address = Column(String(42), nullable=False, index=True)
    overall_risk_score = Column(Float, default=0.0)
    wash_trade_volume_ratio = Column(Float, default=0.0)
    bot_trade_ratio = Column(Float, default=0.0)
    circular_trade_ratio = Column(Float, default=0.0)
    self_trade_ratio = Column(Float, default=0.0)
    total_trades_analyzed = Column(Integer, default=0)
    total_volume_usd = Column(Float, default=0.0)
    wash_trade_volume_usd = Column(Float, default=0.0)
    first_trade_timestamp = Column(DateTime, nullable=True)
    last_updated = Column(DateTime, onupdate=func.now())
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_token_risk_profiles_chain_pool", "chain_id", "pool_address"),
        Index("ix_token_risk_profiles_token", "token_address"),
    )


class DetectionAuditLog(Base):
    """Audit log for detection runs."""

    __tablename__ = "detection_audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chain_id = Column(Integer, nullable=False, index=True)
    pool_address = Column(String(42), nullable=False, index=True)
    detection_type = Column(String(50), nullable=False)
    start_block = Column(BigInteger, nullable=False)
    end_block = Column(BigInteger, nullable=False)
    trades_processed = Column(Integer, default=0)
    wash_trades_detected = Column(Integer, default=0)
    detection_duration_seconds = Column(Float, nullable=True)
    parameters_used = Column(JSON, nullable=True)
    results_summary = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index(
            "ix_detection_audit_logs_chain_pool_time",
            "chain_id", "pool_address", "created_at"
        ),
    )


# ==============================================================================
# Pydantic Models for API
# ==============================================================================

class SwapTradeResponse(BaseModel):
    """Pydantic model for swap trade API responses."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    chain_id: int
    dex_name: str
    pool_address: str
    token_in: str
    token_out: str
    amount_in: float
    amount_out: float
    sender: str
    recipient: str
    transaction_hash: str
    block_number: int
    block_timestamp: datetime
    volume_usd: Optional[float] = None
    is_wash_trade: bool = False
    wash_trade_score: float = 0.0
    detection_method: Optional[str] = None


class TokenRiskProfileResponse(BaseModel):
    """Pydantic model for token risk profile API responses."""

    model_config = ConfigDict(from_attributes=True)

    chain_id: int
    pool_address: str
    token_address: str
    overall_risk_score: float
    wash_trade_volume_ratio: float
    bot_trade_ratio: float
    circular_trade_ratio: float
    self_trade_ratio: float
    total_trades_analyzed: int
    total_volume_usd: float
    wash_trade_volume_usd: float
    last_updated: datetime


class AuditRequest(BaseModel):
    """Pydantic model for audit API requests."""

    chain_id: int = Field(..., description="Blockchain chain ID")
    pool_address: str = Field(..., description="Pool contract address")
    start_block: Optional[int] = Field(None, description="Starting block number")
    end_block: Optional[int] = Field(None, description="Ending block number")
    use_ml: bool = Field(True, description="Enable ML-based detection")
    use_heuristics: bool = Field(True, description="Enable heuristic detection")


class AuditResponse(BaseModel):
    """Pydantic model for audit API responses."""

    model_config = ConfigDict(from_attributes=True)

    audit_id: int
    chain_id: int
    pool_address: str
    trades_processed: int
    wash_trades_detected: int
    wash_trade_volume_usd: float
    wash_trade_ratio: float
    risk_score: float
    detection_methods_used: List[str]
    duration_seconds: float
    timestamp: datetime


class PriceData(BaseModel):
    """Pydantic model for token price data."""

    model_config = ConfigDict(from_attributes=True)

    token_address: str
    price_usd: float
    price_change_24h: Optional[float] = None
    price_change_7d: Optional[float] = None
    market_cap: Optional[float] = None
    volume_24h: Optional[float] = None
    last_updated: datetime


class HeuristicResult(BaseModel):
    """Pydantic model for heuristic detection results."""

    model_config = ConfigDict(from_attributes=True)

    method: str
    wash_trades_count: int
    confidence_scores: List[float]
    flags: Dict[str, Any] = Field(default_factory=dict)


class ClusterResult(BaseModel):
    """Pydantic model for entity clustering results."""

    model_config = ConfigDict(from_attributes=True)

    cluster_id: str
    addresses: List[str]
    confidence_score: float
    evidence: Dict[str, Any] = Field(default_factory=dict)


class HealthCheckResponse(BaseModel):
    """Pydantic model for health check responses."""

    model_config = ConfigDict(from_attributes=True)

    status: str
    database_connected: bool
    rpc_endpoints: Dict[str, bool]
    ml_model_loaded: bool
    timestamp: datetime
