"""
SQLAlchemy and Pydantic models for the database.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict
from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Float, Index, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class SwapTrade(Base):
    __tablename__ = "swap_trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chain_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    dex_name: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    pool_address: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    token_in: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    token_out: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    amount_in: Mapped[float] = mapped_column(Float, nullable=False)
    amount_out: Mapped[float] = mapped_column(Float, nullable=False)
    sender: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    recipient: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    transaction_hash: Mapped[str] = mapped_column(String(66), nullable=False, index=True)
    block_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    block_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    gas_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    gas_used: Mapped[float | None] = mapped_column(Float, nullable=True)
    log_index: Mapped[int] = mapped_column(Integer, nullable=False)
    amount_in_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    amount_out_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    volume_usd: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_wash_trade: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    wash_trade_score: Mapped[float] = mapped_column(Float, default=0.0)
    detection_method: Mapped[str | None] = mapped_column(String(50), nullable=True)

    __table_args__ = (
        Index("ix_swap_trades_pool_timestamp", "pool_address", "block_timestamp"),
        Index("ix_swap_trades_sender_timestamp", "sender", "block_timestamp"),
        Index("ix_swap_trades_chain_dex_pool", "chain_id", "dex_name", "pool_address"),
        Index("ix_swap_trades_tx_log", "transaction_hash", "log_index", unique=True),
    )


class AddressCluster(Base):
    __tablename__ = "address_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cluster_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    addresses: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    creation_date: Mapped[datetime | None] = mapped_column(DateTime, server_default=func.now())
    last_updated: Mapped[datetime | None] = mapped_column(DateTime, onupdate=func.now())
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    __table_args__ = (Index("ix_address_clusters_cluster_id", "cluster_id"),)


class TokenRiskProfile(Base):
    __tablename__ = "token_risk_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chain_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    pool_address: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    token_address: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    overall_risk_score: Mapped[float] = mapped_column(Float, default=0.0)
    wash_trade_volume_ratio: Mapped[float] = mapped_column(Float, default=0.0)
    bot_trade_ratio: Mapped[float] = mapped_column(Float, default=0.0)
    circular_trade_ratio: Mapped[float] = mapped_column(Float, default=0.0)
    self_trade_ratio: Mapped[float] = mapped_column(Float, default=0.0)
    total_trades_analyzed: Mapped[int] = mapped_column(Integer, default=0)
    total_volume_usd: Mapped[float] = mapped_column(Float, default=0.0)
    wash_trade_volume_usd: Mapped[float] = mapped_column(Float, default=0.0)
    first_trade_timestamp: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_updated: Mapped[datetime | None] = mapped_column(DateTime, onupdate=func.now())
    created_at: Mapped[datetime | None] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_token_risk_profiles_chain_pool", "chain_id", "pool_address"),
        Index("ix_token_risk_profiles_token", "token_address"),
    )


class DetectionAuditLog(Base):
    __tablename__ = "detection_audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chain_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    pool_address: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    detection_type: Mapped[str] = mapped_column(String(50), nullable=False)
    start_block: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_block: Mapped[int] = mapped_column(BigInteger, nullable=False)
    trades_processed: Mapped[int] = mapped_column(Integer, default=0)
    wash_trades_detected: Mapped[int] = mapped_column(Integer, default=0)
    detection_duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    parameters_used: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    results_summary: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime | None] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_detection_audit_logs_chain_pool_time", "chain_id", "pool_address", "created_at"),
    )


# Pydantic models for API responses
class SwapTradeResponse(BaseModel):
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
    volume_usd: float | None = None
    is_wash_trade: bool = False
    wash_trade_score: float = 0.0
    detection_method: str | None = None


class TokenRiskProfileResponse(BaseModel):
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
    chain_id: int
    pool_address: str
    start_block: int | None = None
    end_block: int | None = None
    use_ml: bool = True
    use_heuristics: bool = True


class AuditResponse(BaseModel):
    audit_id: int
    chain_id: int
    pool_address: str
    trades_processed: int
    wash_trades_detected: int
    wash_trade_volume_usd: float
    wash_trade_ratio: float
    risk_score: float
    detection_methods_used: list[str]
    duration_seconds: float
    timestamp: datetime
