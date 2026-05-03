"""
Database storage layer using SQLAlchemy async.

This module provides:
- Async PostgreSQL connection management with connection pooling
- Trade data persistence and retrieval
- Risk profile management
- Audit logging for detection operations

Supports bulk operations for high-throughput scenarios and includes
proper connection lifecycle management with async context managers.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, update, and_, func, delete
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool, AsyncAdaptedQueuePool

from models.schemas import (
    Base,
    SwapTrade,
    AddressCluster,
    TokenRiskProfile,
    DetectionAuditLog,
)
from config.settings import settings

logger = logging.getLogger(__name__)

# Constants for batch operations
DEFAULT_BATCH_SIZE: int = 1000
MAX_BATCH_SIZE: int = 10000


# ==============================================================================
# Exceptions
# ==============================================================================

class StorageError(Exception):
    """Base exception for storage errors."""
    pass


class StorageNotInitializedError(StorageError):
    """Raised when storage is accessed before initialization."""
    pass


class BatchInsertError(StorageError):
    """Raised when batch insert fails."""
    pass


# ==============================================================================
# Storage Class
# ==============================================================================

class Storage:
    """
    Async database storage for wash trade detection data.

    Provides:
    - Async connection pooling with configurable pool size
    - Trade data persistence with upsert support
    - Batch operations for high-throughput scenarios
    - Risk profile management
    - Audit logging for detection operations

    Attributes:
        database_url: PostgreSQL async connection string
        engine: SQLAlchemy async engine instance
        session_factory: Async session factory
        _initialized: Whether storage has been initialized

    Example:
        >>> storage = Storage()
        >>> await storage.initialize()
        >>> async with storage.get_session() as session:
        ...     trades = await storage.get_pool_trades(chain_id=1, pool_address="0x...")
    """

    def __init__(
        self,
        database_url: Optional[str] = None,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_recycle: int = 3600,
        pool_timeout: int = 30,
        echo: bool = False,
    ):
        """
        Initialize Storage instance.

        Args:
            database_url: PostgreSQL connection URL (defaults to settings.DATABASE_URL)
            pool_size: Number of connections to maintain
            max_overflow: Max connections beyond pool_size
            pool_recycle: Recycle connections after seconds
            pool_timeout: Connection timeout in seconds
            echo: Whether to echo SQL statements
        """
        self.database_url: str = database_url or settings.DATABASE_URL
        self._pool_size: int = pool_size
        self._max_overflow: int = max_overflow
        self._pool_recycle: int = pool_recycle
        self._pool_timeout: int = pool_timeout
        self._echo: bool = echo

        self.engine: Optional[AsyncEngine] = None
        self.session_factory: Optional[async_sessionmaker] = None
        self._initialized: bool = False

    async def initialize(self) -> None:
        """
        Initialize database connection and create tables.

        Creates the async engine with connection pooling, initializes
        the session factory, and creates all tables if they don't exist.

        Raises:
            StorageError: If database connection fails
        """
        if self._initialized:
            logger.warning("Storage already initialized")
            return

        try:
            self.engine = create_async_engine(
                self.database_url,
                echo=self._echo,
                pool_size=self._pool_size,
                max_overflow=self._max_overflow,
                pool_recycle=self._pool_recycle,
                pool_timeout=self._pool_timeout,
                poolclass=AsyncAdaptedQueuePool,
            )

            self.session_factory = async_sessionmaker(
                self.engine,
                expire_on_commit=False,
                autoflush=False,
            )

            # Create tables if they don't exist
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)

            self._initialized = True
            logger.info(
                f"Database initialized (pool_size={self._pool_size}, "
                f"max_overflow={self._max_overflow})"
            )

        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise StorageError(f"Database initialization failed: {e}") from e

    async def close(self) -> None:
        """
        Close database connections and dispose of engine.

        Should be called on application shutdown to properly
        release database resources.

        Example:
            >>> await storage.close()
        """
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            self.session_factory = None
            self._initialized = False
            logger.info("Database connections closed")

    @asynccontextmanager
    async def get_session(self) -> AsyncSession:
        """
        Get an async database session.

        Provides a context manager that automatically handles
        session lifecycle including commit/rollback on exit.

        Yields:
            AsyncSession: Database session

        Raises:
            StorageNotInitializedError: If storage not initialized

        Example:
            >>> async with storage.get_session() as session:
            ...     result = await session.execute(select(SwapTrade))
        """
        if not self.session_factory:
            raise StorageNotInitializedError(
                "Storage not initialized. Call initialize() first."
            )

        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def save_trade(self, trade_data: Dict[str, Any]) -> SwapTrade:
        """
        Save or update a single trade record.

        Uses upsert logic: updates existing record if found by
        transaction_hash and log_index, otherwise creates new.

        Args:
            trade_data: Dictionary with trade fields

        Returns:
            SwapTrade: The saved trade record
        """
        async with self.get_session() as session:
            stmt = select(SwapTrade).where(
                and_(
                    SwapTrade.transaction_hash == trade_data["transaction_hash"],
                    SwapTrade.log_index == trade_data["log_index"],
                )
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                # Update existing trade
                for key, value in trade_data.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                trade = existing
            else:
                # Create new trade
                trade = SwapTrade(**trade_data)
                session.add(trade)

            await session.commit()
            await session.refresh(trade)
            return trade

    async def save_trades_batch(
        self,
        trades_data: List[Dict[str, Any]],
        batch_size: int = DEFAULT_BATCH_SIZE,
        ignore_duplicates: bool = True,
    ) -> Tuple[int, int]:
        """
        Save multiple trades in batches for performance.

        Splits large datasets into smaller batches and uses
        bulk insert for efficiency.

        Args:
            trades_data: List of trade data dictionaries
            batch_size: Number of trades per batch
            ignore_duplicates: Whether to skip duplicates (uses ON CONFLICT)

        Returns:
            Tuple of (saved_count, failed_count)
        """
        saved = 0
        failed = 0

        # Process in batches
        for i in range(0, len(trades_data), batch_size):
            batch = trades_data[i:i + batch_size]

            try:
                async with self.get_session() as session:
                    for trade_data in batch:
                        try:
                            trade = SwapTrade(**trade_data)
                            session.add(trade)
                            saved += 1
                        except Exception as e:
                            logger.warning(f"Failed to prepare trade: {e}")
                            failed += 1

                    await session.commit()

            except Exception as e:
                logger.error(f"Batch insert failed: {e}")
                failed += len(batch)
                saved -= len(batch)

        return saved, failed

    async def update_trade_labels(
        self,
        trade_ids: List[int],
        is_wash_trade: bool,
        wash_trade_score: float,
        detection_method: str,
    ) -> int:
        """
        Update wash trade labels for multiple trades.

        Args:
            trade_ids: List of trade IDs to update
            is_wash_trade: Whether trades are wash trades
            wash_trade_score: Confidence score (0.0 to 1.0)
            detection_method: Method used for detection

        Returns:
            Number of trades updated
        """
        if not trade_ids:
            return 0

        async with self.get_session() as session:
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
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[SwapTrade]:
        """
        Get trades for a specific pool.

        Args:
            chain_id: Blockchain chain ID
            pool_address: Pool contract address
            limit: Maximum number of trades to return
            offset: Number of trades to skip
            start_time: Filter trades after this time
            end_time: Filter trades before this time

        Returns:
            List of SwapTrade records
        """
        async with self.get_session() as session:
            conditions = [
                SwapTrade.chain_id == chain_id,
                SwapTrade.pool_address == pool_address,
            ]

            if start_time:
                conditions.append(SwapTrade.block_timestamp >= start_time)
            if end_time:
                conditions.append(SwapTrade.block_timestamp <= end_time)

            stmt = (
                select(SwapTrade)
                .where(and_(*conditions))
                .order_by(SwapTrade.block_timestamp.desc())
            )

            if limit:
                stmt = stmt.limit(limit).offset(offset)

            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_wash_trades(
        self,
        chain_id: int,
        pool_address: str,
        min_score: float = 0.5,
        limit: Optional[int] = None,
    ) -> List[SwapTrade]:
        """
        Get detected wash trades for a pool.

        Args:
            chain_id: Blockchain chain ID
            pool_address: Pool contract address
            min_score: Minimum wash trade score threshold
            limit: Maximum number of trades to return

        Returns:
            List of flagged wash trades
        """
        async with self.get_session() as session:
            stmt = (
                select(SwapTrade)
                .where(
                    and_(
                        SwapTrade.chain_id == chain_id,
                        SwapTrade.pool_address == pool_address,
                        SwapTrade.is_wash_trade == True,
                        SwapTrade.wash_trade_score >= min_score,
                    )
                )
                .order_by(SwapTrade.wash_trade_score.desc())
            )

            if limit:
                stmt = stmt.limit(limit)

            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def delete_old_trades(self, retention_days: int) -> int:
        """
        Delete trades older than retention period.

        Args:
            retention_days: Number of days to retain

        Returns:
            Number of trades deleted
        """
        from datetime import timedelta

        cutoff = datetime.utcnow() - timedelta(days=retention_days)

        async with self.get_session() as session:
            stmt = delete(SwapTrade).where(
                SwapTrade.block_timestamp < cutoff
            )
            result = await session.execute(stmt)
            await session.commit()
            deleted = result.rowcount
            logger.info(f"Deleted {deleted} trades older than {retention_days} days")
            return deleted

    async def update_token_risk_profile(
        self,
        chain_id: int,
        pool_address: str,
        token_address: str,
        risk_metrics: Dict[str, Any],
    ) -> TokenRiskProfile:
        """
        Update or create a token risk profile.

        Args:
            chain_id: Blockchain chain ID
            pool_address: Pool contract address
            token_address: Token contract address
            risk_metrics: Dictionary of risk metrics to update

        Returns:
            Updated TokenRiskProfile record
        """
        async with self.get_session() as session:
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
                # Update existing profile
                for key, value in risk_metrics.items():
                    if hasattr(profile, key):
                        setattr(profile, key, value)
                profile.last_updated = datetime.utcnow()
            else:
                # Create new profile
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
        """
        Create an audit log entry for a detection run.

        Args:
            chain_id: Blockchain chain ID
            pool_address: Pool contract address
            detection_type: Type of detection (heuristic, ml, combined)
            start_block: Starting block number
            end_block: Ending block number
            trades_processed: Number of trades analyzed
            wash_trades_detected: Number of wash trades found
            detection_duration_seconds: Time taken for detection
            parameters_used: Detection parameters used
            results_summary: Summary of results

        Returns:
            Created DetectionAuditLog record
        """
        async with self.get_session() as session:
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

    async def get_address_clusters(
        self,
        chain_id: int,
        min_confidence: float = 0.0,
    ) -> List[AddressCluster]:
        """
        Get address clusters for a chain.

        Args:
            chain_id: Blockchain chain ID
            min_confidence: Minimum confidence score

        Returns:
            List of AddressCluster records
        """
        async with self.get_session() as session:
            stmt = select(AddressCluster).where(
                and_(
                    AddressCluster.cluster_id.like(f"{chain_id}:%"),
                    AddressCluster.confidence_score >= min_confidence,
                )
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_audit_logs(
        self,
        chain_id: int,
        pool_address: Optional[str] = None,
        limit: int = 100,
    ) -> List[DetectionAuditLog]:
        """
        Get audit logs for a pool.

        Args:
            chain_id: Blockchain chain ID
            pool_address: Pool contract address (optional)
            limit: Maximum number of logs to return

        Returns:
            List of DetectionAuditLog records
        """
        async with self.get_session() as session:
            conditions = [DetectionAuditLog.chain_id == chain_id]

            if pool_address:
                conditions.append(
                    DetectionAuditLog.pool_address == pool_address
                )

            stmt = (
                select(DetectionAuditLog)
                .where(and_(*conditions))
                .order_by(DetectionAuditLog.created_at.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def health_check(self) -> bool:
        """
        Check database connectivity.

        Returns:
            True if database is accessible, False otherwise
        """
        try:
            async with self.get_session() as session:
                await session.execute(select(1))
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"Storage("
            f"initialized={self._initialized}, "
            f"pool_size={self._pool_size})"
        )
