"""
Database storage layer using SQLAlchemy async.
Secure connection handling with SSL enforcement.
"""

import logging
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from sqlalchemy import CursorResult, and_, delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from config.settings import settings
from models.schemas import (
    AddressCluster,
    Base,
    DetectionAuditLog,
    SwapTrade,
    TokenRiskProfile,
)

logger = logging.getLogger(__name__)


class Storage:
    """Async database storage with connection pooling."""

    def __init__(self, database_url: str | None = None):
        self.database_url = database_url or settings.DATABASE_URL
        self.engine: AsyncEngine | None = None
        self.session_factory: async_sessionmaker[AsyncSession] | None = None

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

    async def save_trade(self, trade_data: dict[str, Any]) -> SwapTrade:
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

    async def save_trades_batch(self, trades_data: list[dict[str, Any]]) -> int:
        """Bulk upsert trades for performance.

        Uses ON CONFLICT DO NOTHING on (transaction_hash, log_index) so that
        re-syncing overlapping block ranges is idempotent instead of raising
        an IntegrityError that aborts the whole batch.
        """
        if not trades_data:
            return 0
        async with await self.get_session() as session:
            stmt = pg_insert(SwapTrade).values(trades_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=["transaction_hash", "log_index"])
            # INSERT/UPDATE/DELETE always execute via CursorResult (which has .rowcount);
            # Session.execute() is typed generically as Result[Any] regardless of statement kind.
            result = cast("CursorResult[Any]", await session.execute(stmt))
            await session.commit()
            return result.rowcount

    async def update_trade_labels(
        self,
        trade_ids: list[int],
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
            result = cast("CursorResult[Any]", await session.execute(stmt))
            await session.commit()
            return result.rowcount

    async def get_pool_trades(
        self,
        chain_id: int,
        pool_address: str,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[SwapTrade]:
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
        risk_metrics: dict[str, Any],
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
        parameters_used: dict[str, Any],
        results_summary: dict[str, Any],
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

    async def get_global_stats(self) -> dict[str, Any]:
        """Aggregate dashboard statistics across all stored data."""
        async with await self.get_session() as session:
            totals_row = (
                await session.execute(
                    select(
                        func.count(SwapTrade.id),
                        func.coalesce(func.sum(SwapTrade.volume_usd), 0.0),
                        func.count(func.distinct(SwapTrade.pool_address)),
                        func.count(func.distinct(SwapTrade.chain_id)),
                    )
                )
            ).one()
            total_trades, total_volume, pool_count, chain_count = totals_row

            wash_row = (
                await session.execute(
                    select(
                        func.count(SwapTrade.id),
                        func.coalesce(func.sum(SwapTrade.volume_usd), 0.0),
                    ).where(SwapTrade.is_wash_trade)
                )
            ).one()
            wash_trades, wash_volume = wash_row

            method_rows = (
                await session.execute(
                    select(
                        SwapTrade.detection_method,
                        func.count(SwapTrade.id),
                        func.coalesce(func.sum(SwapTrade.volume_usd), 0.0),
                    )
                    .where(SwapTrade.is_wash_trade)
                    .group_by(SwapTrade.detection_method)
                )
            ).all()

            chain_rows = (
                await session.execute(
                    select(
                        SwapTrade.chain_id,
                        func.count(SwapTrade.id),
                        func.coalesce(func.sum(SwapTrade.volume_usd), 0.0),
                        func.count(SwapTrade.id).filter(SwapTrade.is_wash_trade),
                        func.coalesce(
                            func.sum(SwapTrade.volume_usd).filter(SwapTrade.is_wash_trade), 0.0
                        ),
                    ).group_by(SwapTrade.chain_id)
                )
            ).all()

        return {
            "total_trades": int(total_trades or 0),
            "total_volume_usd": float(total_volume or 0.0),
            "pools_tracked": int(pool_count or 0),
            "chains_active": int(chain_count or 0),
            "wash_trades": int(wash_trades or 0),
            "wash_volume_usd": float(wash_volume or 0.0),
            "by_method": {
                (method or "unknown"): {"trades": int(count), "volume_usd": float(volume)}
                for method, count, volume in method_rows
            },
            "by_chain": {
                int(chain): {
                    "trades": int(count),
                    "volume_usd": float(volume),
                    "wash_trades": int(wash_count),
                    "wash_volume_usd": float(wash_vol),
                }
                for chain, count, volume, wash_count, wash_vol in chain_rows
            },
        }

    async def get_top_wash_pools(self, limit: int = 10) -> list[dict[str, Any]]:
        """Pools ranked by flagged wash volume (only pools with detections)."""
        wash_volume = func.coalesce(
            func.sum(SwapTrade.volume_usd).filter(SwapTrade.is_wash_trade), 0.0
        )
        wash_count = func.count(SwapTrade.id).filter(SwapTrade.is_wash_trade)
        async with await self.get_session() as session:
            rows = (
                await session.execute(
                    select(
                        SwapTrade.chain_id,
                        SwapTrade.pool_address,
                        func.count(SwapTrade.id),
                        func.coalesce(func.sum(SwapTrade.volume_usd), 0.0),
                        wash_count,
                        wash_volume,
                    )
                    .group_by(SwapTrade.chain_id, SwapTrade.pool_address)
                    .having(wash_count > 0)
                    .order_by(wash_volume.desc())
                    .limit(limit)
                )
            ).all()
        return [
            {
                "chain_id": int(chain),
                "pool_address": pool,
                "trades": int(count),
                "volume_usd": float(volume),
                "wash_trades": int(n_wash),
                "wash_volume_usd": float(v_wash),
            }
            for chain, pool, count, volume, n_wash, v_wash in rows
        ]

    async def get_recent_audit_logs(self, limit: int = 20) -> list[DetectionAuditLog]:
        """Most recent audit runs, newest first."""
        async with await self.get_session() as session:
            stmt = (
                select(DetectionAuditLog).order_by(DetectionAuditLog.created_at.desc()).limit(limit)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_address_clusters(self, chain_id: int) -> list[AddressCluster]:
        """Retrieve address clusters for a chain."""
        async with await self.get_session() as session:
            stmt = select(AddressCluster).where(AddressCluster.cluster_id.like(f"{chain_id}:%"))
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def cleanup_old_data(self, retention_days: int) -> int:
        """Remove trades older than retention period."""
        cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=retention_days)
        async with await self.get_session() as session:
            stmt = delete(SwapTrade).where(SwapTrade.block_timestamp < cutoff)
            result = cast("CursorResult[Any]", await session.execute(stmt))
            await session.commit()
            logger.info(f"Cleaned up {result.rowcount} old trades")
            return result.rowcount
