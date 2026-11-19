"""
Evidence for every wash-trade flag (roadmap Phase 6, explainability).

A score alone cannot be defended: "0.93" says how sure the audit is, not why.
Each detector therefore records a Finding when it fires: the trades it saw
together (a round trip's two legs, a window's trades, a ring's matched
pairs), the entity behind them, and the measurements that made it fire. When
the hits are scored, every flagged trade gets an evidence bundle: each
detector that fired on it, with its finding and the calibration figures that
turned the hit into a probability (core/calibration.py).

The bundle is stored with the trade (``swap_trades.wash_evidence``), served by
the API, sent with alerts and written by ``wash-audit --export json``.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from core.calibration import calibration_of, wash_probability
from models.schemas import SwapTrade

# Trades listed per finding; the finding's `trades_total` gives the full count.
MAX_LISTED_TRADES = 10


def trader_address(trade: SwapTrade) -> str:
    """The transaction sender, or the event's sender for rows without one
    (core.advanced_heuristics.trader_of, repeated to avoid an import cycle)."""
    return (trade.tx_from or trade.sender or "").lower()


def trade_ref(trade: SwapTrade) -> dict[str, Any]:
    """How a finding names a trade: enough to find it on a block explorer."""
    return {
        "tx": trade.transaction_hash,
        "log_index": trade.log_index,
        "block": trade.block_number,
        "time": trade.block_timestamp.isoformat() if trade.block_timestamp else None,
        "trader": trader_address(trade),
        "sold": (trade.token_in or "").lower(),
        "bought": (trade.token_out or "").lower(),
        "amount_in": trade.amount_in,
        "amount_out": trade.amount_out,
        "usd": round(trade.volume_usd, 2) if trade.volume_usd is not None else None,
    }


@dataclass
class Finding:
    """What one detector saw when it fired on a group of trades."""

    detector: str
    trades: list[SwapTrade]
    entity: str  # an address, "cluster:<id>", or a ring's addresses joined by ","
    summary: str  # one sentence a reviewer can check against the listed trades
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self, prior: float) -> dict[str, Any]:
        calibration = calibration_of(self.detector)
        ordered = sorted(self.trades, key=lambda t: (t.block_number, t.log_index))
        return {
            "detector": self.detector,
            "probability": round(wash_probability(self.detector, prior), 6),
            "tpr": calibration.tpr if calibration else None,
            "fpr_upper": calibration.fpr_upper if calibration else None,
            "entity": self.entity,
            "summary": self.summary,
            "details": self.details,
            "trades": [trade_ref(t) for t in ordered[:MAX_LISTED_TRADES]],
            "trades_total": len(ordered),
        }


class EvidenceLog:
    """The findings of one detection run, by trade."""

    def __init__(self) -> None:
        self._by_trade: dict[int, list[Finding]] = defaultdict(list)

    def add(self, finding: Finding) -> None:
        for trade in finding.trades:
            findings = self._by_trade[trade.id]
            if finding not in findings:
                findings.append(finding)

    def findings(self, trade: SwapTrade) -> list[Finding]:
        return list(self._by_trade.get(trade.id, ()))

    def bundle(
        self, trade: SwapTrade, methods: list[str], prior: float, threshold: float
    ) -> dict[str, Any]:
        """The evidence bundle of a trade the detectors in `methods` fired on:
        the most probative first, as the score is that of the most probative."""
        # One entry per detector: the first finding it recorded (e.g. the
        # smallest of the position-neutral windows a trade fell into).
        first: dict[str, Finding] = {}
        for finding in self.findings(trade):
            if finding.detector in methods:
                first.setdefault(finding.detector, finding)
        for method in methods:
            # A detector that fired without recording what it saw still counts,
            # so that the bundle's probability is always the trade's score.
            first.setdefault(
                method,
                Finding(method, [trade], trader_address(trade), "Fired; no details recorded"),
            )
        findings = list(first.values())
        detectors = sorted(
            (f.as_dict(prior) for f in findings), key=lambda d: (-d["probability"], d["detector"])
        )
        return {
            "probability": detectors[0]["probability"] if detectors else 0.0,
            "decided_by": detectors[0]["detector"] if detectors else None,
            "prior": prior,
            "threshold": threshold,
            "detectors": detectors,
        }


def merge_bundle(
    existing: dict[str, Any] | None, addition: dict[str, Any], prior: float, threshold: float
) -> dict[str, Any]:
    """Add one detector's entry (e.g. the ML model's, computed after the
    heuristics) to a stored bundle, keeping the most probative first."""
    bundle = dict(existing or {"prior": prior, "threshold": threshold})
    detectors = [d for d in bundle.get("detectors", []) if d["detector"] != addition["detector"]]
    detectors.append(addition)
    detectors.sort(key=lambda d: (-d["probability"], d["detector"]))
    bundle["detectors"] = detectors
    bundle["probability"] = detectors[0]["probability"]
    bundle["decided_by"] = detectors[0]["detector"]
    return bundle


def evidence_summary(bundle: dict[str, Any] | None) -> str | None:
    """The deciding detector's one-line summary, for alert texts."""
    if not bundle or not bundle.get("detectors"):
        return None
    first = bundle["detectors"][0]
    return f"{first['detector']}: {first['summary']}"
