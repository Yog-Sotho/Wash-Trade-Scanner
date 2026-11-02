"""
Evaluation of the Isolation Forest detector on the labelled harness.

The model is unsupervised, so its anomaly probability needs a threshold to
become a detector. The threshold is set on clean (honest) trades to a target
false-positive rate -- the calibration used by the network-based wash-trading
detector of Sirolly et al. (2026) as reproduced in LhyJason/WashTradingSimulation
-- and the model is then measured on labelled scenarios it was not fitted on.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, cast

import numpy as np
import pandas as pd

from config.settings import settings
from core.calibration import fpr_upper_bound
from core.feature_engineer import FeatureEngineer, features_from_trades
from core.ml_detector import MLDetector
from core.storage import Storage
from evaluation.dataset import Fixture, load_fixture, to_swap_trades
from evaluation.inject import PATTERNS, Market, inject
from evaluation.metrics import average_precision


@dataclass
class _Frame:
    features: pd.DataFrame
    positive: np.ndarray[Any, Any]  # bool per row


def _frame(rows: list[dict[str, Any]], positives: set[str]) -> _Frame:
    trades = to_swap_trades(rows)
    df = features_from_trades(trades)
    order = {t.id: t for t in trades}
    labels = np.array([order[int(i)].transaction_hash in positives for i in df["trade_id"]])
    return _Frame(df, labels)


def _matrix(model: MLDetector, df: pd.DataFrame) -> np.ndarray[Any, Any]:
    return df.reindex(columns=model.feature_columns).fillna(0).values


def threshold_for_fpr(clean_probabilities: np.ndarray[Any, Any], target_fpr: float) -> float:
    """The lowest threshold at which at most `target_fpr` of clean trades are flagged."""
    ordered = np.sort(clean_probabilities)[::-1]
    allowed = int(math.floor(target_fpr * len(ordered)))
    if allowed >= len(ordered):
        return 0.0
    # Strictly above the (allowed+1)-th highest clean score.
    return float(np.nextafter(ordered[allowed], np.inf))


def reference_model() -> MLDetector:
    """A detector used only to fit and score in memory (no database)."""
    return MLDetector(cast(Storage, None), cast(FeatureEngineer, None))


async def measure(
    model: MLDetector,
    threshold: float,
    fixtures: list[Fixture],
    seeds: range,
    patterns: tuple[str, ...] = PATTERNS,
) -> dict[str, Any]:
    """Recall, clean-pool FPR and PR-AUC of `model` at `threshold`."""
    scores: list[float] = []
    labels: list[bool] = []
    tp = fn = 0
    clean_fp = clean_n = 0
    for fixture in fixtures:
        market = Market(fixture.rows, int(fixture.meta["chain_id"]))
        clean = _frame(fixture.rows, set())
        probs = await model.predict(clean.features)
        clean_fp += int((probs >= threshold).sum())
        clean_n += len(probs)
        for pattern in patterns:
            for seed in seeds:
                campaign = inject(pattern, market, seed)
                positives = {row["transaction_hash"] for row in campaign.rows}
                frame = _frame(fixture.rows + campaign.rows, positives)
                probs = await model.predict(frame.features)
                scores.extend(float(p) for p in probs)
                labels.extend(bool(y) for y in frame.positive)
                hit = probs >= threshold
                tp += int((hit & frame.positive).sum())
                fn += int((~hit & frame.positive).sum())
    recall = tp / (tp + fn) if tp + fn else 0.0
    return {
        "threshold": threshold,
        "tpr": round(recall, 6),
        "fpr": round(clean_fp / clean_n, 6) if clean_n else None,
        "fpr_upper": round(fpr_upper_bound(clean_fp, clean_n), 6),
        "pr_auc": round(average_precision(scores, labels) or 0.0, 6),
        "prevalence": round(sum(labels) / len(labels), 6) if labels else None,
        "clean_trades": clean_n,
    }


async def threshold_on(model: MLDetector, fixtures: list[Fixture], target_fpr: float) -> float:
    probs = [await model.predict(_frame(f.rows, set()).features) for f in fixtures]
    return threshold_for_fpr(np.concatenate(probs), target_fpr)


async def leave_one_pool_out(seeds: int, fixture_names: list[str]) -> dict[str, Any]:
    """Cross-validate the reference Isolation Forest across clean pools.

    For each clean pool: fit on the other pools' trades, set the threshold on
    them, and measure on the held-out pool's labelled scenarios.
    """
    clean = [f for f in (load_fixture(n) for n in fixture_names) if f.kind == "clean"]
    folds: dict[str, Any] = {}
    for held_out in clean:
        train = [f for f in clean if f is not held_out]
        if not train:
            continue
        model = reference_model()
        model.fit_matrix(np.vstack([_matrix(model, _frame(f.rows, set()).features) for f in train]))
        threshold = await threshold_on(model, train, settings.ML_TARGET_FPR)
        folds[held_out.name] = await measure(model, threshold, [held_out], range(seeds))
    if not folds:
        return {"folds": {}}
    aucs = [fold["pr_auc"] for fold in folds.values()]
    return {
        "folds": folds,
        "mean_pr_auc": round(float(np.mean(aucs)), 6),
        "std_pr_auc": round(float(np.std(aucs)), 6),
        "mean_tpr": round(float(np.mean([f["tpr"] for f in folds.values()])), 6),
        "mean_fpr": round(float(np.mean([f["fpr"] for f in folds.values()])), 6),
    }
