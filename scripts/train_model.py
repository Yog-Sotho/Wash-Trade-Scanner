#!/usr/bin/env python3
"""
Train the ML model for wash trade detection.
"""

import asyncio
import argparse
import logging
import sys
from typing import List

import pydantic

from core.storage import Storage
from core.feature_engineer import FeatureEngineer
from core.ml_detector import MLDetector
from core.validators import TrainingParameters
from core.exceptions import WashTradeError
from config.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def train_model(
    chain_id: int,
    pool_addresses: List[str],
    use_heuristic_labels: bool = True,
    save_model: bool = True,
):
    storage = Storage()
    await storage.initialize()
    feature_engineer = FeatureEngineer(storage)
    ml_detector = MLDetector(storage, feature_engineer)
    logger.info(f"Training model on {len(pool_addresses)} pools")
    await ml_detector.train(
        chain_id=chain_id,
        pool_addresses=pool_addresses,
        use_heuristic_labels=use_heuristic_labels,
    )
    if save_model:
        ml_detector.save_model()
        logger.info(f"Model saved to {settings.ML_MODEL_PATH}")
    logger.info("Training complete")


async def main() -> int:
    parser = argparse.ArgumentParser(description="Train ML model for wash trade detection")
    parser.add_argument("--chain-id", type=int, required=True, help="Blockchain chain ID")
    parser.add_argument("--pools", nargs="+", required=True, help="Pool addresses for training")
    parser.add_argument("--no-labels", action="store_true", help="Train without heuristic labels")
    parser.add_argument("--no-save", action="store_true", help="Don't save the model")
    args = parser.parse_args()

    try:
        TrainingParameters(
            chain_id=args.chain_id,
            pool_addresses=args.pools,
            use_heuristic_labels=not args.no_labels,
        )
    except (pydantic.ValidationError, ValueError) as exc:
        logger.error(f"Invalid parameters: {exc}")
        return 1

    try:
        await train_model(
            chain_id=args.chain_id,
            pool_addresses=args.pools,
            use_heuristic_labels=not args.no_labels,
            save_model=not args.no_save,
        )
        return 0
    except WashTradeError as exc:
        logger.error(f"Training failed: {exc}")
        return 1
    except Exception as exc:
        logger.error(f"Unexpected error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))