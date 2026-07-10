"""
Shared risk-metric computation used by the CLI audit runner and the API.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from models.schemas import SwapTrade


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


def compute_risk_metrics(trades: list[SwapTrade]) -> dict[str, Any]:
    """Aggregate wash-trade risk metrics over a set of trades.

    Works on whatever label state the trades carry, so callers must pass
    post-detection data if they want this run's results reflected.
    """
    total_volume = sum(t.volume_usd or 0 for t in trades)
    flagged = [t for t in trades if t.is_wash_trade]
    wash_volume = sum(t.volume_usd or 0 for t in flagged)

    wash_volume_by_method: dict[str, float] = defaultdict(float)
    for t in flagged:
        wash_volume_by_method[t.detection_method or "unknown"] += t.volume_usd or 0

    wash_volume_ratio = wash_volume / max(total_volume, 1)
    timestamps = [t.block_timestamp for t in trades if t.block_timestamp is not None]

    return {
        "overall_risk_score": len(flagged) / max(len(trades), 1),
        "wash_trade_volume_ratio": wash_volume_ratio,
        "severity": classify_severity(wash_volume_ratio),
        "wash_volume_by_method": dict(wash_volume_by_method),
        "total_trades_analyzed": len(trades),
        "wash_trades_count": len(flagged),
        "total_volume_usd": total_volume,
        "wash_trade_volume_usd": wash_volume,
        "first_trade_timestamp": min(timestamps) if timestamps else None,
        "last_trade_timestamp": max(timestamps) if timestamps else None,
    }
