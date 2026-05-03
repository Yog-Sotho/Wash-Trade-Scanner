#!/usr/bin/env python3
"""
Main entry point for auditing a blockchain pool for wash trading.

This script provides comprehensive wash trade detection with:
- Multi-chain support via configurable DEX integrations
- Heuristic-based detection (self-trading, circular trades, bot detection)
- ML-based anomaly detection using Isolation Forest
- Entity clustering to identify related addresses
- Graceful shutdown handling for production deployment
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Optional, Set

from core.ingestor import MultiChainIngestor
from core.storage import Storage
from core.feature_engineer import FeatureEngineer
from core.heuristics import HeuristicDetector
from core.ml_detector import MLDetector
from core.entity_clustering import EntityClusterer
from config.settings import settings

# Configure logging early
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ==============================================================================
# Graceful Shutdown Handler
# ==============================================================================

class GracefulShutdown:
    """
    Context manager for graceful shutdown handling.

    Handles SIGINT and SIGTERM signals to ensure clean shutdown
    of async resources like database connections and WebSocket sessions.
    """

    def __init__(self) -> None:
        """Initialize shutdown handler."""
        self._shutdown_event = asyncio.Event()
        self._shutdown_initiated = False
        self._original_handlers: dict[int, Optional[signal.Handler]] = {}
        self._lock = asyncio.Lock()

    def __enter__(self) -> "GracefulShutdown":
        """Enter context manager - register signal handlers."""
        self._shutdown_event.set()  # Not shutdown yet
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: any) -> None:
        """Exit context manager - restore original handlers."""
        for sig, handler in self._original_handlers.items():
            if handler is not None:
                signal.signal(sig, handler)
            else:
                signal.signal(sig, signal.SIG_DFL)

    def _initiate_shutdown(self, signum: int, frame: any) -> None:
        """Signal handler callback."""
        sig_name = signal.Signals(signum).name
        logger.info(f"Received {sig_name}, initiating graceful shutdown...")

        # Run sync cleanup immediately (signal handler context)
        if not self._shutdown_initiated:
            self._shutdown_initiated = True
            self._shutdown_event.clear()

    def register(self) -> None:
        """Register signal handlers for graceful shutdown."""
        for sig in (signal.SIGINT, signal.SIGTERM):
            self._original_handlers[sig] = signal.signal(sig, self._initiate_shutdown)

    @property
    def shutdown_requested(self) -> asyncio.Event:
        """Check if shutdown has been requested."""
        return self._shutdown_event

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()


# ==============================================================================
# Resource Manager
# ==============================================================================

class AuditResourceManager:
    """
    Manages lifecycle of audit resources with automatic cleanup.

    Ensures that all resources (Storage, Ingestor, etc.) are properly
    closed even if an error occurs during the audit.
    """

    def __init__(self) -> None:
        """Initialize resource manager."""
        self._storage: Optional[Storage] = None
        self._ingestor: Optional[MultiChainIngestor] = None
        self._shutdown = GracefulShutdown()

    async def __aenter__(self) -> "AuditResourceManager":
        """Enter async context manager."""
        self._shutdown.register()
        return self

    async def __aexit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: any) -> bool:
        """Exit async context manager - cleanup resources."""
        logger.info("Cleaning up resources...")

        cleanup_errors: list[str] = []

        # Close ingestor first (closes WebSocket connections)
        if self._ingestor is not None:
            try:
                await self._ingestor.shutdown()
                logger.info("Ingestor shutdown complete")
            except Exception as e:
                cleanup_errors.append(f"Ingestor cleanup error: {e}")
                logger.error(f"Failed to shutdown ingestor: {e}")

        # Close storage (closes database connections)
        if self._storage is not None:
            try:
                await self._storage.close()
                logger.info("Storage shutdown complete")
            except Exception as e:
                cleanup_errors.append(f"Storage cleanup error: {e}")
                logger.error(f"Failed to close storage: {e}")

        if cleanup_errors:
            logger.warning(f"Cleanup completed with errors: {cleanup_errors}")
            return False  # Don't suppress original exception

        return False  # Don't suppress exceptions

    @property
    def storage(self) -> Storage:
        """Get storage instance (creates if needed)."""
        if self._storage is None:
            raise RuntimeError("Resources not initialized. Call initialize() first.")
        return self._storage

    @property
    def ingestor(self) -> MultiChainIngestor:
        """Get ingestor instance (creates if needed)."""
        if self._ingestor is None:
            raise RuntimeError("Resources not initialized. Call initialize() first.")
        return self._ingestor

    async def initialize(self) -> None:
        """Initialize all resources."""
        logger.info("Initializing resources...")

        self._storage = Storage()
        await self._storage.initialize()
        logger.info("Storage initialized")

        self._ingestor = MultiChainIngestor(self._storage)
        await self._ingestor.initialize()
        logger.info("Ingestor initialized")


# ==============================================================================
# Audit Logic
# ==============================================================================

async def run_audit(
    chain_id: int,
    pool_address: str,
    start_block: Optional[int] = None,
    end_block: Optional[int] = None,
    use_ml: bool = True,
    use_heuristics: bool = True,
    sync_historical: bool = True,
    export_format: Optional[str] = None,
    export_path: Optional[str] = None,
    shutdown_event: Optional[asyncio.Event] = None,
) -> dict:
    """
    Execute wash trade audit for a specific pool.

    Args:
        chain_id: Blockchain network ID
        pool_address: Pool contract address
        start_block: Starting block number (optional)
        end_block: Ending block number (optional)
        use_ml: Enable ML-based detection
        use_heuristics: Enable heuristic detection
        sync_historical: Sync historical data before analysis
        export_format: Export format ('json' or 'csv')
        export_path: Export file path
        shutdown_event: Event to check for shutdown requests

    Returns:
        Dictionary containing audit results
    """
    start_time = time.time()

    # Track components for shutdown
    feature_engineer: Optional[FeatureEngineer] = None
    heuristic_detector: Optional[HeuristicDetector] = None
    ml_detector: Optional[MLDetector] = None
    entity_clusterer: Optional[EntityClusterer] = None

    async with AuditResourceManager() as manager:
        await manager.initialize()

        feature_engineer = FeatureEngineer(manager.storage)
        heuristic_detector = HeuristicDetector()
        ml_detector = MLDetector(manager.storage, feature_engineer)
        entity_clusterer = EntityClusterer(manager.storage)

        if use_ml:
            try:
                ml_detector.load_model()
            except FileNotFoundError:
                logger.warning("ML model not found. Skipping ML detection.")
                use_ml = False

        # Sync historical data if requested
        if sync_historical:
            logger.info(f"Syncing historical data for pool {pool_address} on chain {chain_id}")

            # Check for shutdown before syncing
            if shutdown_event and shutdown_event.is_set():
                logger.info("Shutdown requested, aborting sync")
                return {"status": "shutdown", "message": "Shutdown requested during sync"}

            trades_synced = await manager.ingestor.audit_pool(
                chain_id=chain_id,
                pool_address=pool_address,
                start_block=start_block,
                end_block=end_block,
            )
            logger.info(f"Synced {trades_synced} trades")

        # Fetch trades for analysis
        trades = await manager.storage.get_pool_trades(chain_id, pool_address)
        logger.info(f"Analyzing {len(trades)} trades")

        wash_trades_detected = 0
        detection_methods: list[str] = []

        async with manager.storage.get_session() as session:
            if use_heuristics:
                # Check shutdown before heuristic detection
                if shutdown_event and shutdown_event.is_set():
                    logger.info("Shutdown requested, aborting heuristic detection")
                    return {"status": "shutdown", "message": "Shutdown during heuristics"}

                logger.info("Running heuristic detection...")
                heuristics_wash_trades, stats = await heuristic_detector.run_all_heuristics(
                    chain_id, pool_address, session
                )
                if heuristics_wash_trades:
                    trade_ids = [t.id for t in heuristics_wash_trades]
                    await manager.storage.update_trade_labels(
                        trade_ids,
                        is_wash_trade=True,
                        wash_trade_score=0.8,
                        detection_method="heuristic"
                    )
                    wash_trades_detected += len(heuristics_wash_trades)
                    detection_methods.extend(stats.keys())

            if use_ml:
                # Check shutdown before ML detection
                if shutdown_event and shutdown_event.is_set():
                    logger.info("Shutdown requested, aborting ML detection")
                    return {"status": "shutdown", "message": "Shutdown during ML"}

                logger.info("Running ML detection...")
                ml_wash_trades = await ml_detector.detect_wash_trades(
                    chain_id, pool_address, threshold=0.8
                )
                if ml_wash_trades:
                    trade_ids = [t.id for t in ml_wash_trades]
                    await manager.storage.update_trade_labels(
                        trade_ids,
                        is_wash_trade=True,
                        wash_trade_score=0.9,
                        detection_method="ml"
                    )
                    wash_trades_detected += len(ml_wash_trades)
                    detection_methods.append("ml")

            total_volume = sum(t.volume_usd or 0 for t in trades)
            wash_volume = sum(t.volume_usd or 0 for t in trades if t.is_wash_trade)
            risk_metrics = {
                "overall_risk_score": wash_trades_detected / max(len(trades), 1),
                "wash_trade_volume_ratio": wash_volume / max(total_volume, 1),
                "total_trades_analyzed": len(trades),
                "total_volume_usd": total_volume,
                "wash_trade_volume_usd": wash_volume,
                "first_trade_timestamp": trades[0].block_timestamp if trades else None,
            }

            await manager.storage.update_token_risk_profile(
                chain_id=chain_id,
                pool_address=pool_address,
                token_address=pool_address,
                risk_metrics=risk_metrics,
            )

            duration = time.time() - start_time

            await manager.storage.create_audit_log(
                chain_id=chain_id,
                pool_address=pool_address,
                detection_type="combined",
                start_block=start_block or 0,
                end_block=end_block or 0,
                trades_processed=len(trades),
                wash_trades_detected=wash_trades_detected,
                detection_duration_seconds=duration,
                parameters_used={
                    "use_ml": use_ml,
                    "use_heuristics": use_heuristics,
                    "sync_historical": sync_historical,
                },
                results_summary=risk_metrics,
            )

    # Build results after context manager exits (resources cleaned up)
    duration = time.time() - start_time

    # Print results
    print("\n" + "="*60)
    print(f"Audit Results for Pool: {pool_address}")
    print(f"Chain ID: {chain_id}")
    print("-"*60)
    print(f"Trades Analyzed: {len(trades)}")
    print(f"Wash Trades Detected: {wash_trades_detected}")
    print(f"Wash Trade Ratio: {risk_metrics['overall_risk_score']:.2%}")
    print(f"Total Volume (USD): ${total_volume:,.2f}")
    print(f"Wash Volume (USD): ${wash_volume:,.2f}")
    print(f"Detection Methods: {', '.join(detection_methods)}")
    print(f"Duration: {duration:.2f} seconds")
    print("="*60)

    # Export if requested
    results = {
        "chain_id": chain_id,
        "pool_address": pool_address,
        "trades_processed": len(trades),
        "wash_trades_detected": wash_trades_detected,
        "risk_metrics": risk_metrics,
        "detection_methods": detection_methods,
        "duration": duration,
    }

    if export_format:
        if export_format == "json":
            export_file = export_path or f"audit_{chain_id}_{pool_address[:8]}.json"
            with open(export_file, "w", encoding="utf-8") as f:
                json.dump(results, f, default=str, indent=2)
            logger.info(f"Results exported to {export_file}")
        elif export_format == "csv":
            export_file = export_path or f"audit_{chain_id}_{pool_address[:8]}.csv"
            with open(export_file, "w", newline='', encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["metric", "value"])
                for k, v in risk_metrics.items():
                    writer.writerow([k, v])
            logger.info(f"Results exported to {export_file}")

    return results


# ==============================================================================
# Main Entry Point
# ==============================================================================

async def async_main(args: argparse.Namespace) -> int:
    """
    Async main entry point with graceful shutdown support.

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code (0 for success, 1 for error)
    """
    shutdown = GracefulShutdown()
    shutdown.register()

    try:
        results = await run_audit(
            chain_id=args.chain_id,
            pool_address=args.pool,
            start_block=args.start_block,
            end_block=args.end_block,
            use_ml=not args.no_ml,
            use_heuristics=not args.no_heuristics,
            sync_historical=not args.no_sync,
            export_format=args.export,
            export_path=args.export_path,
            shutdown_event=shutdown.shutdown_requested,
        )

        if results.get("status") == "shutdown":
            logger.info("Audit aborted due to shutdown request")
            return 0  # Graceful exit

        return 0  # Success

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130  # Standard exit code for SIGINT
    except Exception as e:
        logger.exception(f"Audit failed with error: {e}")
        return 1  # Error exit code


def main() -> int:
    """
    Main entry point with signal handling setup.

    Returns:
        Exit code
    """
    parser = argparse.ArgumentParser(
        description="Wash Trade Detection Audit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --chain-id 1 --pool 0x...
  %(prog)s --chain-id 137 --pool 0x... --start-block 50000000
  %(prog)s --chain-id 56 --pool 0x... --no-ml --export json
        """
    )
    parser.add_argument(
        "--chain-id",
        type=int,
        required=True,
        help="Blockchain chain ID"
    )
    parser.add_argument(
        "--pool",
        type=str,
        required=True,
        help="Pool address to audit"
    )
    parser.add_argument(
        "--start-block",
        type=int,
        help="Starting block number"
    )
    parser.add_argument(
        "--end-block",
        type=int,
        help="Ending block number"
    )
    parser.add_argument(
        "--no-ml",
        action="store_true",
        help="Disable ML detection"
    )
    parser.add_argument(
        "--no-heuristics",
        action="store_true",
        help="Disable heuristic detection"
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="Skip historical sync (use existing data)"
    )
    parser.add_argument(
        "--export",
        choices=["json", "csv"],
        help="Export results to file"
    )
    parser.add_argument(
        "--export-path",
        type=str,
        help="Path for export file (optional)"
    )

    args = parser.parse_args()

    return asyncio.run(async_main(args))


if __name__ == "__main__":
    sys.exit(main())
