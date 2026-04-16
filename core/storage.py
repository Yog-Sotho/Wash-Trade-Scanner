"""
Database storage layer using SQLAlchemy async.
"""

import logging
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    AsyncEngine,
    async_sessionmaker
)
from sqlalchemy import select, update, and_, func

from models.schemas import (
    Base, SwapTrade, AddressCluster,
    TokenRiskProfile, DetectionAuditLog
)
from config.settings import settings

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, database_url: str = settings.DATABASE_URL):
        self.database_url = database_url
        self.engine: Optional[AsyncEngine] = None
        self.session_factory: Optional[async_sessionmaker] = None

    async def initialize(self):
        self.engine = create_async_engine(
            self.database_url,
            echo=False,
            pool_size=10,
            max_overflow=20,
        )
        self.session_factory = async_sessionmaker(
            self.engine,
            expire_on_commit=False,
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database initialized")

    @asynccontextmanager
    async def get_session(self) -> AsyncSession:
        if not self.session_factory:
            raise RuntimeError("Storage not initialized. Call initialize() first.")
        async with self.session_factory() as session:
            yield session

    async def save_trade(self, trade_data: Dict[str, Any]) -> SwapTrade:
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
        async with self.get_session() as session:
            saved = 0
            for trade_data in trades_data:
                trade = SwapTrade(**trade_data)
                session.add(trade)
                saved += 1
            await session.commit()
            return saved

    async def update_trade_labels(
        self,
        trade_ids: List[int],
        is_wash_trade: bool,
        wash_trade_score: float,
        detection_method: str
    ) -> int:
        async with self.get_session() as session:
            stmt = update(SwapTrade).where(
                SwapTrade.id.in_(trade_ids)
            ).values(
                is_wash_trade=is_wash_trade,
                wash_trade_score=wash_trade_score,
                detection_method=detection_method,
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def get_pool_trades(
        self,
        chain_id: int,
        pool_address: str,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[SwapTrade]:
        async with self.get_session() as session:
            stmt = select(SwapTrade).where(
                and_(
                    SwapTrade.chain_id == chain_id,
                    SwapTrade.pool_address == pool_address,
                )
            ).order_by(SwapTrade.block_timestamp.desc())
            if limit:
                stmt = stmt.limit(limit).offset(offset)
            result = await session.execute(stmt)
            return result.scalars().all()

    async def update_token_risk_profile(
        self,
        chain_id: int,
        pool_address: str,
        token_address: str,
        risk_metrics: Dict[str, Any]
    ) -> TokenRiskProfile:
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
                for key, value in risk_metrics.items():
                    setattr(profile, key, value)
                profile.last_updated = func.now()
            else:
                profile = TokenRiskProfile(
                    chain_id=chain_id,
                    pool_address=pool_address,
                    token_address=token_address,
                    **risk_metrics
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
        results_summary: Dict[str, Any]
    ) -> DetectionAuditLog:
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

    async def get_address_clusters(self, chain_id: int) -> List[AddressCluster]:
        async with self.get_session() as session:
            stmt = select(AddressCluster).where(
                AddressCluster.cluster_id.like(f"{chain_id}:%")
            )
            result = await session.execute(stmt)
            return result.scalars().all()