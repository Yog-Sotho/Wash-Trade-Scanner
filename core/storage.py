"""
Database storage layer using SQLAlchemy async.
Secure connection handling with SSL enforcement.
"""

import logging
from typing import Optional, List, Dict, Any, Union

from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    AsyncEngine,
    async_sessionmaker,
)
from sqlalchemy import select, update, delete, and_, func

from models.schemas import (
    Base, SwapTrade, AddressCluster,
    TokenRiskProfile, DetectionAuditLog,
)
from config.settings import settings

logger = logging.getLogger(__name__)


class Storage:
    """Async database storage with connection pooling."""

    def __init__(self, database_url: Optional[Union[str, URL]] = None):
        self.database_url = database_url or settings.DATABASE_URL
        self.engine: Optional[AsyncEngine] = None
        self.session_factory: Optional[async_sessionmaker] = None

    async def initialize(self) -> None:
        """Initialize database engine and create tables."""
        self.engine = create_async_engine(
            self.database_url,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_recycle=3600,
            pool_pre_ping=True,
            connect_args={"timeout": settings.RPC_TIMEOUT},
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database initialized")

    async def close(self) -> None:
        """Dispose engine and release connections."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            self.session_factory = None
        logger.info("Database connections closed")

    async def health_check(self) -> bool:
        """Verify database connectivity."""
        if not self.engine:
            return False
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(select(func.now()))
                return result.scalar() is not None
        except Exception as exc:
            logger.error(f"Database health check failed: {exc}")
            return False

    async def get_session(self) -> AsyncSession:
        """Get a new database session."""
        if not self.session_factory:
            raise RuntimeError("Storage not initialized. Call initialize() first.")
        return self.session_factory()

    async def save_trade(self, trade_data: Dict[str, Any]) -> SwapTrade:
        """Save or update a single trade."""
        async with await self.get_session() as session:
            stmt = select(SwapTrade).where(
                and_(
                    SwapTrade.transaction_hash == trade_data["transaction_hash"],
                    SwapTrade.log_index == trade_data["log_index"],
                )
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()
            if existing:
                for key, value in trade_data.items():
                    setattr(existing, key, value)
                trade = existing
            else:
                trade = SwapTrade(**trade_data)
                session.add(trade)
            await session.commit()
            await session.refresh(trade)
            return trade

    async def save_trades_batch(self, trades_data: List[Dict[str, Any]]) -> int:
        """Bulk insert trades for performance."""
        if not trades_data:
            return 0
        async with await self.get_session() as session:
            trades = [SwapTrade(**data) for data in trades_data]
            session.add_all(trades)
            await session.commit()
            return len(trades)

    async def update_trade_labels(
        self,
        trade_ids: List[int],
        is_wash_trade: bool,
        wash_trade_score: float,
        detection_method: str,
    ) -> int:
        """Update wash trade labels for given trade IDs."""
        if not trade_ids:
            return 0
        async with await self.get_session() as session:
            stmt = (
                update(SwapTrade)
                .where(SwapTrade.id.in_(trade_ids))
                .values(
                    is_wash_trade=is_wash_trade,
                    wash_trade_score=wash_trade_score,
                    detection_method=detection_method,
                )
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def get_pool_trades(
        self,
        chain_id: int,
        pool_address: str,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> List[SwapTrade]:
        """Retrieve trades for a specific pool."""
        async with await self.get_session() as session:
            stmt = (
                select(SwapTrade)
                .where(
                    and_(
                        SwapTrade.chain_id == chain_id,
                        SwapTrade.pool_address == pool_address,
                    )
                )
                .order_by(SwapTrade.block_timestamp.desc())
            )
            if limit:
                stmt = stmt.limit(limit).offset(offset)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def update_token_risk_profile(
        self,
        chain_id: int,
        pool_address: str,
        token_address: str,
        risk_metrics: Dict[str, Any],
    ) -> TokenRiskProfile:
        """Update or create token risk profile."""
        async with await self.get_session() as session:
            stmt = select(TokenRiskProfile).where(
                and_(
                    TokenRiskProfile.chain_id == chain_id,
                    TokenRiskProfile.pool_address == pool_address,
                    TokenRiskProfile.token_address == token_address,
                )
            )
            result = await session.execute(stmt)
            profile = result.scalar_one_or_none()
            if profile:
                for key, value in risk_metrics.items():
                    if hasattr(profile, key):
                        setattr(profile, key, value)
                profile.last_updated = func.now()
            else:
                profile = TokenRiskProfile(
                    chain_id=chain_id,
                    pool_address=pool_address,
                    token_address=token_address,
                    **risk_metrics,
                )
                session.add(profile)
            await session.commit()
            await session.refresh(profile)
            return profile

    async def create_audit_log(
        self,
        chain_id: int,
        pool_address: str,
        detection_type: str,
        start_block: int,
        end_block: int,
        trades_processed: int,
        wash_trades_detected: int,
        detection_duration_seconds: float,
        parameters_used: Dict[str, Any],
        results_summary: Dict[str, Any],
    ) -> DetectionAuditLog:
        """Create audit log entry."""
        async with await self.get_session() as session:
            log = DetectionAuditLog(
                chain_id=chain_id,
                pool_address=pool_address,
                detection_type=detection_type,
                start_block=start_block,
                end_block=end_block,
                trades_processed=trades_processed,
                wash_trades_detected=wash_trades_detected,
                detection_duration_seconds=detection_duration_seconds,
                parameters_used=parameters_used,
                results_summary=results_summary,
            )
            session.add(log)
            await session.commit()
            await session.refresh(log)
            return log

    async def get_address_clusters(self, chain_id: int) -> List[AddressCluster]:
        """Retrieve address clusters for a chain."""
        async with await self.get_session() as session:
            stmt = select(AddressCluster).where(
                AddressCluster.cluster_id.like(f"{chain_id}:%")
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def cleanup_old_data(self, retention_days: int) -> int:
        """
        Remove trades older than retention period.
        Uses single delete statement to avoid memory exhaustion (DoS protection).
        """
        from datetime import datetime, timedelta
        cutoff = datetime.utcnow() - timedelta(days=retention_days)
        async with await self.get_session() as session:
            stmt = delete(SwapTrade).where(SwapTrade.block_timestamp < cutoff)
            result = await session.execute(stmt)
            await session.commit()
            count = result.rowcount
            logger.info(f"Cleaned up {count} old trades")
            return count
