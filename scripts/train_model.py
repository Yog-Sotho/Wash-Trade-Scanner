#!/usr/bin/env python3
"""
Train the ML model for wash trade detection.
With input validation and secure error handling.
"""

import asyncio
import argparse
import logging
import sys
import pydantic
from core.storage import Storage
from core.feature_engineer import FeatureEngineer
from core.ml_detector import MLDetector
from core.validators import TrainingParameters
from core.exceptions import WashTradeError
from config.settings import settings

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

async def train_model(params: TrainingParameters, save_model: bool = True):
    """Execute model training with validated parameters."""
    storage = Storage()
    try:
        await storage.initialize()
        ml_detector = MLDetector(storage, FeatureEngineer(storage))
        logger.info(f"Training model on {len(params.pool_addresses)} pools (chain_id={params.chain_id})")
        await ml_detector.train(
            chain_id=params.chain_id,
            pool_addresses=params.pool_addresses,
            use_heuristic_labels=params.use_heuristic_labels,
            contamination=params.contamination,
        )
        if save_model:
            ml_detector.save_model()
            logger.info(f"Model saved to {settings.ML_MODEL_PATH}")
    finally:
        await storage.close()

async def main() -> int:
    """CLI entry point with input validation."""
    parser = argparse.ArgumentParser(description="Train ML model for wash trade detection")
    parser.add_argument("--chain-id", type=int, required=True)
    parser.add_argument("--pools", nargs="+", required=True)
    parser.add_argument("--no-labels", action="store_true")
    parser.add_argument("--contamination", type=float)
    parser.add_argument("--no-save", action="store_true")
    args = parser.parse_args()

    try:
        params = TrainingParameters(
            chain_id=args.chain_id,
            pool_addresses=args.pools,
            use_heuristic_labels=not args.no_labels,
            contamination=args.contamination,
        )
    except (pydantic.ValidationError, ValueError):
        logger.error("Invalid training parameters provided.")
        return 1

    try:
        await train_model(params, save_model=not args.no_save)
        return 0
    except WashTradeError as exc:
        logger.error(f"Training failed: {exc}")
    except Exception:
        logger.error("Unexpected error during training.")
    return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
