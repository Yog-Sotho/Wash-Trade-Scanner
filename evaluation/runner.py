"""
Run detectors over labelled scenarios and aggregate their metrics.

A scenario is a real background history plus, optionally, one injected
campaign. Background trades are treated as negatives: the backgrounds are the
most liquid pools on Ethereum, the kind Chainalysis excludes from wash-trading
analysis as unlikely to involve it. Any background trade a detector flags is
therefore counted as a false positive -- an upper bound on its true
false-positive rate, since a liquid pool is not guaranteed to be wash-free.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncSession

from core.advanced_heuristics import trader_of
from core.calibration import DetectorCalibration, fpr_upper_bound, override_calibration
from core.heuristics import HeuristicDetector
from evaluation.dataset import Fixture, load_fixture, to_swap_trades
from evaluation.inject import PATTERNS, Campaign, Market, inject
from evaluation.metrics import Counts, average_precision
from models.schemas import AddressCluster, SwapTrade

DetectorFn = Callable[[list[SwapTrade], list[AddressCluster]], Awaitable[list[SwapTrade]]]

# The session argument is part of the detector signature but unused by the
# detectors themselves (they operate on the trade list they are given).
_NO_SESSION = cast(AsyncSession, None)

# Documented wash-trading markets. There are no trade-level labels for them;
# the harness reports how much of each market every detector flags, with the
# evidence below (rendered into docs/benchmarks.md).
DOCUMENTED_CASES: dict[str, str] = {
    "nexf_weth": (
        "This pool is linked on chain to the FBI's NexFundAI sting (Operation Token "
        "Mirrors; charges announced October 2024, against market makers who wash-traded "
        "the token on Uniswap). Kaiko's analysis of the NexFundAI market identified the "
        "funding address `0x4aa6a6231630ad13ef52c06de3d3d3850fafcd70`; that address sent "
        "WETH 15 times (blocks 20,587,745-20,778,730) to contract "
        "`0x8a5d8700dd19488d964d202e87b0cc85704054db`, which swapped in this pool 58 "
        "times (blocks 20,598,086-20,727,020), and the pool traded from May to September "
        "2024, the sting's timeline. Those 58 swaps are all sells of NexF, in pairs from "
        "two operator accounts a few blocks apart: inventory distribution, not a wash "
        "pattern, so they are not used as positive labels. Kaiko describes the wash "
        "trading itself as multi-wallet (148 of 485 wallets first funded in the same "
        "block as at least five others), which only funding-link data can expose; the "
        "fixture records swaps, not funding transfers."
    ),
}


def detector_suite() -> dict[str, DetectorFn]:
    """Every production detector run on its own, by the method name it
    labels trades with, plus ``composite``: the audit's actual decision
    (all detectors, calibrated scoring, SUSPICIOUS_ACTIVITY_THRESHOLD)."""
    h = HeuristicDetector()

    def plain(
        method: Callable[[list[SwapTrade], AsyncSession], Awaitable[list[SwapTrade]]],
    ) -> DetectorFn:
        async def run(trades: list[SwapTrade], _clusters: list[AddressCluster]) -> list[SwapTrade]:
            return await method(trades, _NO_SESSION)

        return run

    async def position_neutral(
        trades: list[SwapTrade], clusters: list[AddressCluster]
    ) -> list[SwapTrade]:
        return await h.advanced.detect_position_neutral(trades, _NO_SESSION, clusters)

    async def wash_cluster(
        trades: list[SwapTrade], clusters: list[AddressCluster]
    ) -> list[SwapTrade]:
        return await h.detect_wash_clusters(trades, clusters, _NO_SESSION)

    async def composite(trades: list[SwapTrade], clusters: list[AddressCluster]) -> list[SwapTrade]:
        flagged, _ = await h.run_detectors_on_trades(trades, _NO_SESSION, clusters)
        return flagged

    return {
        "round_trip": plain(h.advanced.detect_round_trips),
        "position_neutral": position_neutral,
        "circular_trading": plain(h.detect_circular_trading),
        "closed_cluster": plain(h.advanced.detect_closed_cluster),
        "repeated_amounts": plain(h.advanced.detect_repeated_amounts),
        "high_frequency_bot": plain(h.detect_high_frequency_bot),
        "volume_anomaly": plain(h.detect_volume_anomaly),
        "wash_cluster": wash_cluster,
        "composite": composite,
    }


@dataclass
class DetectorResult:
    overall: Counts = field(default_factory=Counts)
    by_pattern: dict[str, Counts] = field(default_factory=dict)
    clean: dict[str, Counts] = field(default_factory=dict)
    # What the detector flags in each clean pool, per trader.
    clean_flags: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    documented: dict[str, dict[str, Any]] = field(default_factory=dict)
    scores: list[float] = field(default_factory=list)
    labels: list[bool] = field(default_factory=list)
    seconds: float = 0.0


def _clusters(campaign: Campaign | None) -> list[AddressCluster]:
    if campaign is None:
        return []
    return [
        AddressCluster(cluster_id=f"eval:{i}", addresses=list(members), confidence_score=0.8)
        for i, members in enumerate(campaign.clusters)
    ]


async def _run_one(
    detector: DetectorFn,
    rows: list[dict[str, Any]],
    positives: set[str],
    clusters: list[AddressCluster],
) -> list[tuple[SwapTrade, bool, float]]:
    """Run `detector` on fresh trade objects; return (trade, is_positive, score)."""
    trades = to_swap_trades(rows)
    flagged = {t.id for t in await detector(trades, clusters)}
    return [
        (
            trade,
            trade.transaction_hash in positives,
            float(trade.wash_trade_score or 0.0) if trade.id in flagged else 0.0,
        )
        for trade in trades
    ]


# Calibration is measured on scenarios built from these seeds and reported
# metrics on seeds 0..n-1, so the reported numbers are never the ones the
# calibration was fitted to.
DEV_SEED_OFFSET = 1_000


async def _run_scenarios(
    loaded: list[Fixture],
    suite: dict[str, DetectorFn],
    seeds: range,
    patterns: tuple[str, ...],
    include_documented: bool,
) -> tuple[dict[str, DetectorResult], dict[str, int]]:
    results = {name: DetectorResult() for name in suite}
    totals = {"scenarios": 0, "injected_trades": 0, "background_trades": 0}

    for fixture in loaded:
        if fixture.kind == "documented_wash":
            if not include_documented:
                continue
            for name, detector in suite.items():
                started = time.perf_counter()
                outcome = await _run_one(detector, fixture.rows, set(), [])
                results[name].seconds += time.perf_counter() - started
                total_volume = sum(t.volume_usd or 0.0 for t, _, _ in outcome)
                flagged = [t for t, _, score in outcome if score > 0]
                results[name].documented[fixture.name] = {
                    "trades": len(outcome),
                    "flagged": len(flagged),
                    "flagged_share": len(flagged) / len(outcome) if outcome else None,
                    "flagged_volume_share": (
                        sum(t.volume_usd or 0.0 for t in flagged) / total_volume
                        if total_volume
                        else None
                    ),
                }
            continue

        market = Market(fixture.rows, int(fixture.meta["chain_id"]))
        # Clean run: the false-positive rate on an honest market.
        for name, detector in suite.items():
            started = time.perf_counter()
            outcome = await _run_one(detector, fixture.rows, set(), [])
            results[name].seconds += time.perf_counter() - started
            counts = Counts()
            for trade, positive, score in outcome:
                counts.add(score > 0, positive, trade.volume_usd or 0.0)
            results[name].clean[fixture.name] = counts
            results[name].clean_flags[fixture.name] = _flags_by_trader(
                [t for t, _, score in outcome if score > 0]
            )

        for pattern in patterns:
            for seed in seeds:
                campaign = inject(pattern, market, seed)
                rows = fixture.rows + campaign.rows
                positives = {row["transaction_hash"] for row in campaign.rows}
                clusters = _clusters(campaign)
                totals["scenarios"] += 1
                totals["injected_trades"] += len(campaign.rows)
                totals["background_trades"] += len(fixture.rows)
                for name, detector in suite.items():
                    started = time.perf_counter()
                    outcome = await _run_one(detector, rows, positives, clusters)
                    results[name].seconds += time.perf_counter() - started
                    counts = Counts()
                    for trade, positive, score in outcome:
                        counts.add(score > 0, positive, trade.volume_usd or 0.0)
                        results[name].scores.append(score)
                        results[name].labels.append(positive)
                    results[name].overall.merge(counts)
                    results[name].by_pattern.setdefault(pattern, Counts()).merge(counts)
    return results, totals


def _flags_by_trader(flagged: list[SwapTrade]) -> list[dict[str, Any]]:
    by_trader: dict[str, list[SwapTrade]] = {}
    for trade in flagged:
        by_trader.setdefault(trader_of(trade), []).append(trade)
    summary = []
    for trader, trades in sorted(by_trader.items()):
        trades.sort(key=lambda t: (t.block_number, t.log_index))
        first_side = trades[0].token_in
        summary.append(
            {
                "trader": trader,
                "trades": len(trades),
                "volume_usd": round(sum(t.volume_usd or 0.0 for t in trades), 2),
                "legs": [
                    {
                        "block": t.block_number,
                        "side": "A" if t.token_in == first_side else "B",
                        "usd": round(t.volume_usd or 0.0, 2),
                    }
                    for t in trades
                ],
            }
        )
    return summary


def calibration_from(results: dict[str, DetectorResult]) -> dict[str, dict[str, Any]]:
    """Per-detector TPR (recall over all injected patterns) and clean-pool
    false-positive rate with its 95% upper bound."""
    table: dict[str, dict[str, Any]] = {}
    for name, result in results.items():
        clean = Counts()
        for counts in result.clean.values():
            clean.merge(counts)
        negatives = clean.fp + clean.tn
        table[name] = {
            "tpr": round(result.overall.recall or 0.0, 6),
            "fpr": round(clean.fpr or 0.0, 6),
            "fpr_upper": round(fpr_upper_bound(clean.fp, negatives), 6),
            "clean_trades": negatives,
            "clean_false_positives": clean.fp,
        }
    return table


async def evaluate(
    seeds: int,
    fixtures: list[str] | None = None,
    patterns: tuple[str, ...] = PATTERNS,
    calibrate: bool = True,
    ml: bool = True,
) -> dict[str, Any]:
    """Measure every detector, and the composite audit decision.

    With `calibrate`, detector rates are first measured on development seeds
    and the composite is scored with them; otherwise it uses the shipped
    config/detector_calibration.json.
    """
    from evaluation.dataset import fixture_names

    names = fixtures if fixtures is not None else fixture_names()
    loaded: list[Fixture] = [load_fixture(name) for name in names]
    suite = detector_suite()
    raw = {name: fn for name, fn in suite.items() if name != "composite"}

    calibration: dict[str, dict[str, Any]] | None = None
    if calibrate:
        dev, _ = await _run_scenarios(
            loaded, raw, range(DEV_SEED_OFFSET, DEV_SEED_OFFSET + seeds), patterns, False
        )
        calibration = calibration_from(dev)
        override_calibration(
            {
                name: DetectorCalibration(tpr=c["tpr"], fpr_upper=c["fpr_upper"])
                for name, c in calibration.items()
            }
        )
    try:
        results, totals = await _run_scenarios(loaded, suite, range(seeds), patterns, True)
    finally:
        override_calibration(None)

    ml_results: dict[str, Any] | None = None
    if ml:
        from evaluation.ml import leave_one_pool_out

        ml_results = await leave_one_pool_out(seeds, names)

    return {
        "seeds": seeds,
        "ml_isolation_forest": ml_results,
        "fixtures": {f.name: f.meta for f in loaded},
        "patterns": list(patterns),
        **totals,
        "calibration": calibration,
        "detectors": {name: _summarise(result) for name, result in results.items()},
    }


def _rate(value: float | None) -> float | None:
    return None if value is None else round(value, 4)


def _counts_summary(counts: Counts) -> dict[str, Any]:
    return {
        "tp": counts.tp,
        "fp": counts.fp,
        "fn": counts.fn,
        "tn": counts.tn,
        "precision": _rate(counts.precision),
        "recall": _rate(counts.recall),
        "f1": _rate(counts.f1),
        "fpr": _rate(counts.fpr),
        "fp_volume_share": _rate(counts.fp_volume_share),
    }


def _summarise(result: DetectorResult) -> dict[str, Any]:
    clean_total = Counts()
    for counts in result.clean.values():
        clean_total.merge(counts)
    return {
        "overall": {
            **_counts_summary(result.overall),
            "pr_auc": _rate(average_precision(result.scores, result.labels)),
        },
        "by_pattern": {p: _counts_summary(c) for p, c in result.by_pattern.items()},
        "clean_pools": {
            **{name: _counts_summary(c) for name, c in result.clean.items()},
            "all": _counts_summary(clean_total),
        },
        "clean_flags": result.clean_flags,
        "documented_wash": {
            name: {k: (_rate(v) if isinstance(v, float) else v) for k, v in d.items()}
            for name, d in result.documented.items()
        },
        "seconds": round(result.seconds, 2),
    }
