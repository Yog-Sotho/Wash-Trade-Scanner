"""
Classification metrics for detector evaluation.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass
class Counts:
    tp: int = 0
    fp: int = 0
    fn: int = 0
    tn: int = 0
    tp_volume: float = 0.0
    fp_volume: float = 0.0
    negative_volume: float = 0.0

    def add(self, flagged: bool, positive: bool, volume: float) -> None:
        if positive:
            if flagged:
                self.tp += 1
                self.tp_volume += volume
            else:
                self.fn += 1
        else:
            self.negative_volume += volume
            if flagged:
                self.fp += 1
                self.fp_volume += volume
            else:
                self.tn += 1

    def merge(self, other: Counts) -> None:
        for name in self.__dataclass_fields__:
            setattr(self, name, getattr(self, name) + getattr(other, name))

    @property
    def precision(self) -> float | None:
        flagged = self.tp + self.fp
        return self.tp / flagged if flagged else None

    @property
    def recall(self) -> float | None:
        positives = self.tp + self.fn
        return self.tp / positives if positives else None

    @property
    def f1(self) -> float | None:
        p, r = self.precision, self.recall
        if p is None or r is None:
            return None
        return 2 * p * r / (p + r) if p + r else 0.0

    @property
    def fpr(self) -> float | None:
        negatives = self.fp + self.tn
        return self.fp / negatives if negatives else None

    @property
    def fp_volume_share(self) -> float | None:
        return self.fp_volume / self.negative_volume if self.negative_volume else None


def average_precision(scores: Sequence[float], labels: Sequence[bool]) -> float | None:
    """Area under the precision-recall curve as average precision (the
    step-wise sum used by scikit-learn): sum over thresholds of
    (R_n - R_{n-1}) * P_n. Tied scores are taken together, so a binary
    detector gets exactly one operating point."""
    positives = sum(labels)
    if positives == 0:
        return None
    pairs = sorted(zip(scores, labels, strict=True), key=lambda p: -p[0])
    ap = 0.0
    tp = fp = 0
    prev_recall = 0.0
    i = 0
    while i < len(pairs):
        score = pairs[i][0]
        while i < len(pairs) and pairs[i][0] == score:
            if pairs[i][1]:
                tp += 1
            else:
                fp += 1
            i += 1
        if score <= 0:
            break  # unflagged trades are not an operating point
        recall = tp / positives
        ap += (recall - prev_recall) * (tp / (tp + fp))
        prev_recall = recall
    return ap
