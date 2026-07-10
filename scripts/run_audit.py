#!/usr/bin/env python3
"""
Main entry point for auditing a blockchain pool for wash trading.
With input validation and graceful shutdown.
"""

import argparse
import asyncio
import csv
import json
import logging
import signal
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from config.settings import settings
from core.entity_clustering import EntityClusterer
from core.exceptions import WashTradeError
from core.feature_engineer import FeatureEngineer
from core.heuristics import HeuristicDetector
from core.ingestor import MultiChainIngestor
from core.ml_detector import MLDetector
from core.storage import Storage
from core.validators import AuditParameters

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def classify_severity(wash_volume_ratio: float) -> str:
    """Map the wash-trade volume ratio to a human-readable severity level."""
    if wash_volume_ratio >= 0.5:
        return "CRITICAL"
    if wash_volume_ratio >= 0.25:
        return "HIGH"
    if wash_volume_ratio >= 0.10:
        return "MEDIUM"
    if wash_volume_ratio >= 0.01:
        return "LOW"
    return "MINIMAL"


class AuditRunner:
    """Manages audit lifecycle with graceful shutdown."""

    def __init__(self) -> None:
        self.storage: Storage | None = None
        self.ingestor: MultiChainIngestor | None = None
        self._shutdown_event = asyncio.Event()

    async def initialize(self) -> None:
        self.storage = Storage()
        await self.storage.initialize()

    async def cleanup(self) -> None:
        if self.storage:
            await self.storage.close()
            self.storage = None

    def _signal_handler(self, signum: int, frame: object | None) -> None:
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self._shutdown_event.set()

    def initialize_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM handlers to trigger graceful shutdown."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    async def run_audit(
        self,
        params: AuditParameters,
        export_format: str | None = None,
        export_path: str | None = None,
    ) -> dict[str, Any]:
        """Execute full audit pipeline."""
        start_time = time.time()

        if self._shutdown_event.is_set():
            raise WashTradeError("Audit cancelled by shutdown signal")

        assert self.storage is not None, "call initialize() first"
        feature_engineer = FeatureEngineer(self.storage)
        heuristic_detector = HeuristicDetector()
        ml_detector = MLDetector(self.storage, feature_engineer)
        entity_clusterer = EntityClusterer(self.storage)

        use_ml = params.use_ml
        if use_ml:
            try:
                ml_detector.load_model()
            except FileNotFoundError:
                logger.warning("ML model not found. Skipping ML detection.")
                use_ml = False

        ingestor = MultiChainIngestor(self.storage)
        await ingestor.initialize()

        if self._shutdown_event.is_set():
            raise WashTradeError("Audit cancelled by shutdown signal")

        logger.info(
            f"Syncing historical data for pool {params.pool_address} on chain {params.chain_id}"
        )
        trades_synced = await ingestor.audit_pool(
            chain_id=params.chain_id,
            pool_address=params.pool_address,
            start_block=params.start_block,
            end_block=params.end_block,
        )
        logger.info(f"Synced {trades_synced} trades")

        trades = await self.storage.get_pool_trades(params.chain_id, params.pool_address)
        logger.info(f"Analyzing {len(trades)} trades")

        flagged_trade_ids: set[int] = set()
        detection_methods: list[str] = []

        async with await self.storage.get_session() as session:
            if params.use_heuristics:
                logger.info("Clustering addresses by funding links...")
                try:
                    await entity_clusterer.cluster_addresses(
                        params.chain_id, params.pool_address, session
                    )
                except Exception as exc:
                    logger.warning(f"Entity clustering skipped: {exc}")

                logger.info("Running heuristic detection...")
                heuristics_wash_trades, stats = await heuristic_detector.run_all_heuristics(
                    params.chain_id, params.pool_address, session
                )
                if heuristics_wash_trades:
                    # Persist each trade's real detector score and method instead
                    # of flattening everything to one label.
                    by_label: dict[tuple[str, float], list[int]] = defaultdict(list)
                    for t in heuristics_wash_trades:
                        method = t.detection_method or "heuristic"
                        score = t.wash_trade_score or 0.8
                        by_label[(method, score)].append(t.id)
                        flagged_trade_ids.add(t.id)
                    for (method, score), trade_ids in by_label.items():
                        await self.storage.update_trade_labels(
                            trade_ids,
                            is_wash_trade=True,
                            wash_trade_score=score,
                            detection_method=method,
                        )
                    detection_methods.extend(stats.keys())

            if use_ml:
                logger.info("Running ML detection...")
                ml_wash_trades = await ml_detector.detect_wash_trades(
                    params.chain_id, params.pool_address, threshold=0.8
                )
                if ml_wash_trades:
                    # detect_wash_trades persists per-trade probabilities itself.
                    flagged_trade_ids.update(t.id for t in ml_wash_trades)
                    detection_methods.append("ml")

        wash_trades_detected = len(flagged_trade_ids)

        # Re-fetch so volume metrics reflect the labels persisted by this run,
        # not the pre-detection snapshot.
        trades = await self.storage.get_pool_trades(params.chain_id, params.pool_address)
        total_volume = sum(t.volume_usd or 0 for t in trades)
        flagged_trades = [t for t in trades if t.is_wash_trade]
        wash_volume = sum(t.volume_usd or 0 for t in flagged_trades)
        wash_volume_by_method: dict[str, float] = defaultdict(float)
        for t in flagged_trades:
            wash_volume_by_method[t.detection_method or "unknown"] += t.volume_usd or 0
        wash_volume_ratio = wash_volume / max(total_volume, 1)

        risk_metrics = {
            "overall_risk_score": len(flagged_trades) / max(len(trades), 1),
            "wash_trade_volume_ratio": wash_volume_ratio,
            "severity": classify_severity(wash_volume_ratio),
            "wash_volume_by_method": dict(wash_volume_by_method),
            "total_trades_analyzed": len(trades),
            "total_volume_usd": total_volume,
            "wash_trade_volume_usd": wash_volume,
            "first_trade_timestamp": trades[-1].block_timestamp if trades else None,
        }

        duration = time.time() - start_time

        await self.storage.create_audit_log(
            chain_id=params.chain_id,
            pool_address=params.pool_address,
            detection_type="combined",
            start_block=params.start_block or 0,
            end_block=params.end_block or 0,
            trades_processed=len(trades),
            wash_trades_detected=wash_trades_detected,
            detection_duration_seconds=duration,
            parameters_used={
                "use_ml": use_ml,
                "use_heuristics": params.use_heuristics,
            },
            results_summary=risk_metrics,
        )

        self._print_results(
            params.pool_address,
            params.chain_id,
            len(trades),
            wash_trades_detected,
            risk_metrics,
            detection_methods,
            duration,
        )

        if export_format:
            await self._export_results(
                params,
                len(trades),
                wash_trades_detected,
                risk_metrics,
                detection_methods,
                duration,
                export_format,
                export_path,
            )

        return {
            "chain_id": params.chain_id,
            "pool_address": params.pool_address,
            "trades_processed": len(trades),
            "wash_trades_detected": wash_trades_detected,
            "risk_metrics": risk_metrics,
            "detection_methods": detection_methods,
            "duration": duration,
        }

    def _print_results(
        self,
        pool_address: str,
        chain_id: int,
        trades_analyzed: int,
        wash_trades: int,
        risk_metrics: dict[str, Any],
        detection_methods: list[str],
        duration: float,
    ) -> None:
        print("\n" + "=" * 60)
        print(f"Audit Results for Pool: {pool_address}")
        print(f"Chain ID: {chain_id}")
        print("-" * 60)
        print(f"Trades Analyzed: {trades_analyzed}")
        print(f"Wash Trades Detected: {wash_trades}")
        print(f"Wash Trade Ratio: {risk_metrics['overall_risk_score']:.2%}")
        print(f"Total Volume (USD): ${risk_metrics['total_volume_usd']:,.2f}")
        print(f"Wash Volume (USD): ${risk_metrics['wash_trade_volume_usd']:,.2f}")
        if "severity" in risk_metrics:
            print(f"Severity: {risk_metrics['severity']}")
        for method, volume in sorted(
            risk_metrics.get("wash_volume_by_method", {}).items(), key=lambda kv: -kv[1]
        ):
            print(f"  - {method}: ${volume:,.2f}")
        print(f"Detection Methods: {', '.join(detection_methods)}")
        print(f"Duration: {duration:.2f} seconds")
        print("=" * 60)

    async def _export_results(
        self,
        params: AuditParameters,
        trades_processed: int,
        wash_trades_detected: int,
        risk_metrics: dict[str, Any],
        detection_methods: list[str],
        duration: float,
        export_format: str,
        export_path: str | None,
    ) -> None:
        results = {
            "chain_id": params.chain_id,
            "pool_address": params.pool_address,
            "trades_processed": trades_processed,
            "wash_trades_detected": wash_trades_detected,
            "risk_metrics": risk_metrics,
            "detection_methods": detection_methods,
            "duration": duration,
            "timestamp": datetime.now(UTC).isoformat(),
        }

        if export_format == "json":
            export_file = export_path or f"audit_{params.chain_id}_{params.pool_address[:8]}.json"
            with open(export_file, "w") as f:
                json.dump(results, f, default=str, indent=2)
            logger.info(f"Results exported to {export_file}")

        elif export_format == "csv":
            export_file = export_path or f"audit_{params.chain_id}_{params.pool_address[:8]}.csv"
            with open(export_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["metric", "value"])
                for k, v in risk_metrics.items():
                    writer.writerow([k, v])
            logger.info(f"Results exported to {export_file}")


async def main() -> int:
    parser = argparse.ArgumentParser(description="Wash Trade Detection Audit")
    parser.add_argument("--chain-id", type=int, required=True, help="Blockchain chain ID")
    parser.add_argument("--pool", type=str, required=True, help="Pool address to audit")
    parser.add_argument("--start-block", type=int, help="Starting block number")
    parser.add_argument("--end-block", type=int, help="Ending block number")
    parser.add_argument("--no-ml", action="store_true", help="Disable ML detection")
    parser.add_argument("--no-heuristics", action="store_true", help="Disable heuristic detection")
    parser.add_argument("--export", choices=["json", "csv"], help="Export results to file")
    parser.add_argument("--export-path", type=str, help="Path for export file")
    args = parser.parse_args()

    try:
        params = AuditParameters(
            chain_id=args.chain_id,
            pool_address=args.pool,
            start_block=args.start_block,
            end_block=args.end_block,
            use_ml=not args.no_ml,
            use_heuristics=not args.no_heuristics,
        )
    except PydanticValidationError as exc:
        logger.error(f"Invalid parameters: {exc}")
        return 1

    runner = AuditRunner()
    runner.initialize_signal_handlers()

    try:
        await runner.initialize()
        await runner.run_audit(
            params=params,
            export_format=args.export,
            export_path=args.export_path,
        )
        return 0
    except WashTradeError as exc:
        logger.error(f"Audit failed: {exc}")
        return 1
    except Exception as exc:
        logger.exception(f"Unexpected error: {exc}")
        return 1
    finally:
        await runner.cleanup()


def cli() -> None:
    """Synchronous entry point for the `wash-audit` console script."""
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    cli()
