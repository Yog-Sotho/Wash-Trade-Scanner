#!/usr/bin/env python3
"""
Train the ML model for wash trade detection, and measure it before saving.

The Isolation Forest is unsupervised, so a trained model is only worth saving
if it detects labelled wash trading. Training therefore:

1. cross-validates across the training pools (leave one pool out) when there
   are two or more, to show how much the model depends on which pools it saw;
2. fits the final model on the earliest 80% of each pool's trades and holds
   out the most recent 20% (a chronological split);
3. sets the detection threshold so that at most ML_TARGET_FPR of the honest
   trades in the evaluation harness's clean pools are flagged;
4. measures recall, false-positive rate and PR-AUC on the harness's labelled
   scenarios, and stores them in the saved artifact -- audits score the
   model's hits with these measured rates;
5. refuses to save a model whose PR-AUC is below ML_MIN_PR_AUC_LIFT times
   that of a random ranking, unless --allow-weak-model is given.
"""

import argparse
import asyncio
import logging
from typing import Any

import numpy as np

from config.settings import settings
from core.feature_engineer import FeatureEngineer
from core.ml_detector import MLDetector
from core.storage import Storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

HOLDOUT_FRACTION = 0.2
DEFAULT_EVAL_SEEDS = 5


async def _measure(ml_detector: MLDetector, eval_seeds: int) -> dict[str, Any]:
    from evaluation.dataset import fixture_names, load_fixture
    from evaluation.ml import measure, threshold_on

    clean = [f for f in (load_fixture(n) for n in fixture_names()) if f.kind == "clean"]
    threshold = await threshold_on(ml_detector, clean, settings.ML_TARGET_FPR)
    return await measure(ml_detector, threshold, clean, range(eval_seeds))


async def train_model(
    chain_id: int,
    pool_addresses: list[str],
    use_heuristic_labels: bool = True,
    save_model: bool = True,
    eval_seeds: int = DEFAULT_EVAL_SEEDS,
    allow_weak_model: bool = False,
) -> dict[str, Any]:
    """Train, evaluate and (if good enough) save. Returns the measured metrics
    with ``saved`` telling whether the model was written."""
    storage = Storage()
    await storage.initialize()
    try:
        feature_engineer = FeatureEngineer(storage)
        ml_detector = MLDetector(storage, feature_engineer)

        folds: list[dict[str, Any]] = []
        if len(pool_addresses) >= 2:
            for held_out in pool_addresses:
                subset = [p for p in pool_addresses if p != held_out]
                await ml_detector.train(chain_id, subset, use_heuristic_labels)
                folds.append({"held_out": held_out, **await _measure(ml_detector, eval_seeds)})

        logger.info(f"Training model on {len(pool_addresses)} pools")
        holdout = await ml_detector.train(
            chain_id, pool_addresses, use_heuristic_labels, holdout_fraction=HOLDOUT_FRACTION
        )
        metrics = await _measure(ml_detector, eval_seeds)
        if holdout:
            import pandas as pd

            probs = await ml_detector.predict(pd.concat(holdout, ignore_index=True))
            metrics["holdout_trades"] = len(probs)
            metrics["holdout_flagged_share"] = round(
                float(np.mean(probs >= metrics["threshold"])), 6
            )
        if folds:
            aucs = [fold["pr_auc"] for fold in folds]
            metrics["cv"] = {
                "folds": folds,
                "mean_pr_auc": round(float(np.mean(aucs)), 6),
                "std_pr_auc": round(float(np.std(aucs)), 6),
            }
        metrics["trained_on"] = {"chain_id": chain_id, "pools": pool_addresses}
        ml_detector.metrics = metrics
        logger.info(f"Model evaluation: {metrics}")

        floor = settings.ML_MIN_PR_AUC_LIFT * (metrics["prevalence"] or 0.0)
        weak = metrics["pr_auc"] < floor
        if weak:
            logger.error(
                f"Model PR-AUC {metrics['pr_auc']} is below {floor:.6f} "
                f"({settings.ML_MIN_PR_AUC_LIFT}x a random ranking); "
                + ("saving anyway (--allow-weak-model)" if allow_weak_model else "not saving")
            )
        saved = save_model and (not weak or allow_weak_model)
        if saved:
            ml_detector.save_model()
            logger.info(f"Model saved to {settings.ML_MODEL_PATH}")
        return {**metrics, "saved": saved}
    finally:
        await storage.close()


async def main() -> int:
    parser = argparse.ArgumentParser(description="Train ML model for wash trade detection")
    parser.add_argument("--chain-id", type=int, required=True, help="Blockchain chain ID")
    parser.add_argument("--pools", nargs="+", required=True, help="Pool addresses for training")
    parser.add_argument("--no-labels", action="store_true", help="Train without heuristic labels")
    parser.add_argument("--no-save", action="store_true", help="Don't save the model")
    parser.add_argument(
        "--eval-seeds", type=int, default=DEFAULT_EVAL_SEEDS, help="Labelled scenarios per pattern"
    )
    parser.add_argument(
        "--allow-weak-model",
        action="store_true",
        help="Save even if the model does not beat the PR-AUC floor",
    )
    args = parser.parse_args()
    if args.eval_seeds < 1:
        parser.error("--eval-seeds must be at least 1")
    result = await train_model(
        chain_id=args.chain_id,
        pool_addresses=[pool.lower() for pool in args.pools],  # stored form
        use_heuristic_labels=not args.no_labels,
        save_model=not args.no_save,
        eval_seeds=args.eval_seeds,
        allow_weak_model=args.allow_weak_model,
    )
    return 0 if result["saved"] or args.no_save else 1


def cli() -> None:
    """Synchronous entry point for the `wash-train` console script."""
    raise SystemExit(asyncio.run(main()))


if __name__ == "__main__":
    cli()
