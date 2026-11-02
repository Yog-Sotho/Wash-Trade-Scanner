"""
Parameter selection for detectors, against the labelled harness.

For each candidate parameter set, a detector is run on the development
scenarios (seeds from DEV_SEED_OFFSET, never the reported ones) and on the
clean pools. A candidate is admissible when a hit is decisive on its own --
P(wash | hit) >= SUSPICIOUS_ACTIVITY_THRESHOLD at the configured WASH_PRIOR,
using the 95% upper bound of its clean-pool false-positive rate. Among
admissible candidates the one with the highest recall wins (ties: fewer false
positives). A detector whose best candidate is still not decisive is reported
as such; its hits then only inform the score, never the label.

    python -m evaluation.tune --seeds 10
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from config.settings import settings
from core.calibration import DetectorCalibration, fpr_upper_bound
from evaluation.dataset import fixture_names, load_fixture
from evaluation.inject import PATTERNS
from evaluation.runner import DEV_SEED_OFFSET, _run_scenarios, detector_suite

GRIDS: dict[str, dict[str, list[Any]]] = {
    "round_trip": {
        "ROUND_TRIP_MAX_BLOCKS": [25, 50, 100, 300],
        "ROUND_TRIP_VALUE_TOLERANCE": [0.005, 0.01, 0.02],
        "ROUND_TRIP_MIN_COUNT": [2, 3, 4],
    },
    "position_neutral": {
        "POSITION_NEUTRAL_MARGIN": [0.001, 0.0025, 0.005, 0.01],
        "POSITION_NEUTRAL_MIN_TRADES": [4, 6, 8],
        "POSITION_NEUTRAL_WINDOWS_HOURS": [[1], [1, 24], [1, 24, 168]],
    },
    "repeated_amounts": {
        "REPEATED_AMOUNT_MIN_COUNT": [5, 8, 12],
        "REPEATED_AMOUNT_SIG_FIGS": [3, 4, 6],
    },
}


@contextmanager
def overridden(values: dict[str, Any]) -> Iterator[None]:
    saved = {name: getattr(settings, name) for name in values}
    try:
        for name, value in values.items():
            setattr(settings, name, value)
        yield
    finally:
        for name, value in saved.items():
            setattr(settings, name, value)


def candidates(grid: dict[str, list[Any]]) -> Iterator[dict[str, Any]]:
    names = list(grid)
    for combo in itertools.product(*(grid[n] for n in names)):
        yield dict(zip(names, combo, strict=True))


async def tune(detector: str, seeds: int) -> dict[str, Any]:
    loaded = [load_fixture(n) for n in fixture_names()]
    suite = {detector: detector_suite()[detector]}
    rows: list[dict[str, Any]] = []
    for params in candidates(GRIDS[detector]):
        with overridden(params):
            results, _ = await _run_scenarios(
                loaded, suite, range(DEV_SEED_OFFSET, DEV_SEED_OFFSET + seeds), PATTERNS, False
            )
        result = results[detector]
        fp = sum(c.fp for c in result.clean.values())
        n = sum(c.fp + c.tn for c in result.clean.values())
        tpr = result.overall.recall or 0.0
        posterior = DetectorCalibration(tpr=tpr, fpr_upper=fpr_upper_bound(fp, n)).posterior(
            settings.WASH_PRIOR
        )
        rows.append(
            {
                "params": params,
                "tpr": round(tpr, 4),
                "clean_fp": fp,
                "clean_trades": n,
                "posterior": round(posterior, 4),
                "decisive": posterior >= settings.SUSPICIOUS_ACTIVITY_THRESHOLD,
            }
        )
    admissible = [r for r in rows if r["decisive"]]
    pool = admissible or rows
    best = max(pool, key=lambda r: (r["tpr"], -r["clean_fp"], r["posterior"]))
    return {"detector": detector, "best": best, "decisive": bool(admissible), "grid": rows}


async def main() -> int:
    parser = argparse.ArgumentParser(description="Select detector parameters on dev seeds")
    parser.add_argument("--seeds", type=int, default=10)
    parser.add_argument("--detector", choices=sorted(GRIDS), action="append")
    parser.add_argument("--json", metavar="PATH", help="write the selection (rendered in docs)")
    args = parser.parse_args()
    selection: dict[str, Any] = {}
    for detector in args.detector or sorted(GRIDS):
        outcome = await tune(detector, args.seeds)
        selection[detector] = {"best": outcome["best"], "decisive": outcome["decisive"]}
        print(f"\n{detector}: best {outcome['best']} (decisive: {outcome['decisive']})")
        for row in sorted(outcome["grid"], key=lambda r: (-r["decisive"], -r["tpr"])):
            print(
                f"  {row['params']} tpr={row['tpr']} clean_fp={row['clean_fp']}/"
                f"{row['clean_trades']} P(wash|hit)={row['posterior']}"
            )
    if args.json:
        Path(args.json).write_text(
            json.dumps({"seeds": args.seeds, "detectors": selection}, indent=2, sort_keys=True)
            + "\n"
        )
    return 0


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    raise SystemExit(asyncio.run(main()))
