"""
Turns detector hits into calibrated wash-trade probabilities.

Each detector's true-positive rate (recall on the labelled scenarios of the
evaluation harness) and false-positive rate (on real, liquid pools) are
measured by ``wash-eval`` and stored in ``config/detector_calibration.json``.
A hit from detector d is then worth, by Bayes' rule,

    P(wash | d fired) = TPR_d * prior / (TPR_d * prior + FPR_d * (1 - prior))

with the prior share of wash trades from settings (WASH_PRIOR). FPR_d is the
one-sided 95% Clopper-Pearson upper bound, so a detector that made no false
positive on N clean trades is still credited with up to ~3/N, not zero. When
several detectors fire on a trade, the most probative one decides: their hits
are correlated (several look at the same trades), so multiplying their
evidence as if independent would overstate it.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from functools import cache
from importlib import resources

from scipy.stats import beta

logger = logging.getLogger(__name__)

CALIBRATION_FILE = "detector_calibration.json"
CONFIDENCE = 0.95


@dataclass(frozen=True)
class DetectorCalibration:
    tpr: float
    fpr_upper: float

    def posterior(self, prior: float) -> float:
        hit = self.tpr * prior
        false_alarm = self.fpr_upper * (1.0 - prior)
        if hit + false_alarm == 0:
            return 0.0
        return hit / (hit + false_alarm)


def fpr_upper_bound(false_positives: int, negatives: int, confidence: float = CONFIDENCE) -> float:
    """One-sided Clopper-Pearson upper bound on a false-positive rate."""
    if negatives <= 0:
        return 1.0
    if false_positives >= negatives:
        return 1.0
    return float(beta.ppf(confidence, false_positives + 1, negatives - false_positives))


@cache
def load_calibration() -> dict[str, DetectorCalibration]:
    path = resources.files("config") / CALIBRATION_FILE
    payload = json.loads(path.read_text())
    return {
        name: DetectorCalibration(tpr=float(d["tpr"]), fpr_upper=float(d["fpr_upper"]))
        for name, d in payload["detectors"].items()
    }


_override: dict[str, DetectorCalibration] | None = None


def override_calibration(calibration: dict[str, DetectorCalibration] | None) -> None:
    """Score with `calibration` instead of the shipped file (None restores it).

    Used by the evaluation harness to score with freshly measured rates.
    """
    global _override
    _override = calibration


def wash_probability(method: str, prior: float) -> float:
    """P(wash | `method` fired). Uncalibrated detectors cannot label a trade."""
    table = _override if _override is not None else load_calibration()
    calibration = table.get(method)
    if calibration is None:
        logger.warning(f"No calibration for detector {method!r}; its hits are not scored")
        return 0.0
    return calibration.posterior(prior)
