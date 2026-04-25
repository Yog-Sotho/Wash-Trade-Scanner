#!/usr/bin/env python3
"""
Main entry point for auditing a blockchain pool for wash trading.
"""

import asyncio
import argparse
import csv
import json
import logging
import os
import re
import signal
import time
from datetime import datetime
from typing import Optional

from core.ingestor import MultiChainIngestor
from core.storage import Storage
from core.feature_engineer import FeatureEngineer
from core.heuristics import HeuristicDetector
from core.ml_detector import MLDetector
from core.entity_clustering import EntityClusterer
from config.settings import settings

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

VALID_ADDRESS_RE = re.compile(r'^0x[a-fA-F0-9]{40}$')


def validate_address(addr: str) -> None:
    if not VALID_ADDRESS_RE.match(addr):
        raise ValueError(f"Invalid Ethereum address: {addr}")


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
):
    validate_address(pool_address)
    start_time = time.time()
    storage = Storage()
    await storage.initialize()
    feature_engineer = FeatureEngineer(storage)
    heuristic_detector = HeuristicDetector()
    ml_detector = MLDetector(storage, feature_engineer)
    entity_clusterer = EntityClusterer(storage)
    if use_ml:
        try:
            ml_detector.load_model()
        except FileNotFoundError:
            logger.warning("ML model not found. Skipping ML detection.")
            use_ml = False
    ingestor = MultiChainIngestor(storage)
    await ingestor.initialize()
    if sync_historical:
        logger.info(f"Syncing historical data for pool {pool_address} on chain {chain_id}")
        trades_synced = await ingestor.audit_pool(
            chain_id=chain_id,
            pool_address=pool_address,
            start_block=start_block,
            end_block=end_block,
        )
        logger.info(f"Synced {trades_synced} trades")
    trades = await storage.get_pool_trades(chain_id, pool_address)
    logger.info(f"Analyzing {len(trades)} trades")
    wash_trades_detected = 0
    detection_methods = []
    async with storage.get_session() as session:
        if use_heuristics:
            logger.info("Running heuristic detection...")
            heuristics_wash_trades, stats = await heuristic_detector.run_all_heuristics(
                chain_id, pool_address, session
            )
            if heuristics_wash_trades:
                trade_ids = [t.id for t in heuristics_wash_trades]
                await storage.update_trade_labels(
                    trade_ids,
                    is_wash_trade=True,
                    wash_trade_score=0.8,
                    detection_method="heuristic"
                )
                wash_trades_detected += len(heuristics_wash_trades)
                detection_methods.extend(stats.keys())
        if use_ml:
            logger.info("Running ML detection...")
            ml_wash_trades = await ml_detector.detect_wash_trades(
                chain_id, pool_address, threshold=0.8
            )
            if ml_wash_trades:
                trade_ids = [t.id for t in ml_wash_trades]
                await storage.update_trade_labels(
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
        await storage.update_token_risk_profile(
            chain_id=chain_id,
            pool_address=pool_address,
            token_address=pool_address,
            risk_metrics=risk_metrics,
        )
        duration = time.time() - start_time
        await storage.create_audit_log(
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
    if export_format:
        results = {
            "chain_id": chain_id,
            "pool_address": pool_address,
            "trades_processed": len(trades),
            "wash_trades_detected": wash_trades_detected,
            "risk_metrics": risk_metrics,
            "detection_methods": detection_methods,
            "duration": duration,
        }
        if export_format == "json":
            export_file = export_path or f"audit_{chain_id}_{pool_address[:8]}.json"
            with open(export_file, "w") as f:
                json.dump(results, f, default=str, indent=2)
            logger.info(f"Results exported to {export_file}")
        elif export_format == "csv":
            export_file = export_path or f"audit_{chain_id}_{pool_address[:8]}.csv"
            with open(export_file, "w", newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["metric", "value"])
                for k, v in risk_metrics.items():
                    writer.writerow([k, v])
            logger.info(f"Results exported to {export_file}")
    await storage.close()
    return {
        "chain_id": chain_id,
        "pool_address": pool_address,
        "trades_processed": len(trades),
        "wash_trades_detected": wash_trades_detected,
        "risk_metrics": risk_metrics,
        "detection_methods": detection_methods,
        "duration": duration,
    }


def handle_signal(signum, frame):
    logger.info("Shutdown signal received. Exiting.")
    raise SystemExit(0)


async def main():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    parser = argparse.ArgumentParser(description="Wash Trade Detection Audit")
    parser.add_argument("--chain-id", type=int, required=True, help="Blockchain chain ID")
    parser.add_argument("--pool", type=str, required=True, help="Pool address to audit")
    parser.add_argument("--start-block", type=int, help="Starting block number")
    parser.add_argument("--end-block", type=int, help="Ending block number")
    parser.add_argument("--no-ml", action="store_true", help="Disable ML detection")
    parser.add_argument("--no-heuristics", action="store_true", help="Disable heuristic detection")
    parser.add_argument("--no-sync", action="store_true", help="Skip historical sync (use existing data)")
    parser.add_argument("--export", choices=["json", "csv"], help="Export results to file")
    parser.add_argument("--export-path", type=str, help="Path for export file (optional)")
    args = parser.parse_args()
    await run_audit(
        chain_id=args.chain_id,
        pool_address=args.pool,
        start_block=args.start_block,
        end_block=args.end_block,
        use_ml=not args.no_ml,
        use_heuristics=not args.no_heuristics,
        sync_historical=not args.no_sync,
        export_format=args.export,
        export_path=args.export_path,
    )


if __name__ == "__main__":
    asyncio.run(main())
