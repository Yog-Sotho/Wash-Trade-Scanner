"""
``wash-eval``: measure every detector and publish the numbers.

    wash-eval                     # print the summary
    wash-eval --write-docs        # regenerate docs/benchmarks.md
    wash-eval --write-baseline    # accept current metrics as the regression baseline
    wash-eval --write-calibration # ship the measured detector rates for scoring
    wash-eval --check             # exit 1 on a regression or a stale calibration
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from importlib import resources
from pathlib import Path
from typing import Any

from core.calibration import CALIBRATION_FILE
from evaluation.report import render
from evaluation.runner import evaluate

DEFAULT_SEEDS = 10
BASELINE_FILE = "baseline.json"
# A detector regresses if, on the same deterministic scenarios, its F1 or
# recall drops, or its clean-pool false-positive rate rises, by more than this.
TOLERANCE = 0.005


PHASE1_FILE = "phase1_results.json"  # Phase-1 detectors on this harness (see the file)
TUNING_FILE = "tuning.json"  # python -m evaluation.tune --json evaluation/tuning.json


def _optional_json(name: str) -> dict[str, Any] | None:
    path = Path(str(resources.files("evaluation") / name))
    return json.loads(path.read_text()) if path.exists() else None


def _tuning() -> dict[str, Any] | None:
    payload = _optional_json(TUNING_FILE)
    return payload["detectors"] if payload else None


def baseline_path() -> Path:
    return Path(str(resources.files("evaluation") / BASELINE_FILE))


def calibration_path() -> Path:
    return Path(str(resources.files("config") / CALIBRATION_FILE))


def stale_calibration(measured: dict[str, Any], shipped: dict[str, Any]) -> list[str]:
    """Differences between freshly measured detector rates and the shipped ones.

    The shipped file must be what the current detectors measure; otherwise
    audits score detections with rates the code no longer has.
    """
    problems: list[str] = []
    for name in sorted(set(measured) | set(shipped)):
        if name not in shipped:
            problems.append(f"{name}: measured but missing from {CALIBRATION_FILE}")
        elif name not in measured:
            problems.append(f"{name}: in {CALIBRATION_FILE} but no longer measured")
        else:
            for key in ("tpr", "fpr_upper"):
                if abs(measured[name][key] - shipped[name][key]) > 1e-6:
                    problems.append(
                        f"{name}: {key} is {measured[name][key]} but "
                        f"{CALIBRATION_FILE} has {shipped[name][key]}"
                    )
    return problems


def regressions(current: dict[str, Any], baseline: dict[str, Any]) -> list[str]:
    """Human-readable regressions of `current` against `baseline`."""
    problems: list[str] = []
    if current.get("seeds") != baseline.get("seeds"):
        problems.append(
            f"seed count differs from the baseline ({current.get('seeds')} vs "
            f"{baseline.get('seeds')}); results are not comparable"
        )
        return problems
    for name, base in baseline["detectors"].items():
        now = current["detectors"].get(name)
        if now is None:
            problems.append(f"{name}: detector missing from the current run")
            continue
        for metric in ("f1", "recall"):
            before, after = base["overall"].get(metric), now["overall"].get(metric)
            if before is not None and (after is None or after < before - TOLERANCE):
                problems.append(f"{name}: {metric} fell from {before} to {after}")
        before = base["clean_pools"]["all"].get("fpr")
        after = now["clean_pools"]["all"].get("fpr")
        if before is not None and (after is None or after > before + TOLERANCE):
            problems.append(f"{name}: clean-pool FPR rose from {before} to {after}")
    return problems


def _baseline_view(results: dict[str, Any]) -> dict[str, Any]:
    """The subset of results the regression gate compares."""
    return {
        "seeds": results["seeds"],
        "detectors": {
            name: {
                "overall": {k: d["overall"][k] for k in ("precision", "recall", "f1", "pr_auc")},
                "clean_pools": {"all": {"fpr": d["clean_pools"]["all"]["fpr"]}},
            }
            for name, d in results["detectors"].items()
        },
    }


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Evaluate wash-trade detectors")
    parser.add_argument("--seeds", type=int, default=DEFAULT_SEEDS)
    parser.add_argument("--write-docs", metavar="PATH", nargs="?", const="docs/benchmarks.md")
    parser.add_argument("--write-baseline", action="store_true")
    parser.add_argument("--write-calibration", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--json", metavar="PATH", help="write the full results as JSON")
    args = parser.parse_args(argv)
    if args.seeds < 1:
        parser.error("--seeds must be at least 1")

    results = await evaluate(seeds=args.seeds)

    for name, d in results["detectors"].items():
        o, c = d["overall"], d["clean_pools"]["all"]
        print(
            f"{name:22s} precision={o['precision']} recall={o['recall']} f1={o['f1']} "
            f"pr_auc={o['pr_auc']} clean_fpr={c['fpr']} ({d['seconds']}s)"
        )

    if args.json:
        Path(args.json).write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    if args.write_docs:
        Path(args.write_docs).write_text(
            render(results, before=_optional_json(PHASE1_FILE), tuning=_tuning())
        )
        print(f"Wrote {args.write_docs}")
    if args.write_calibration:
        payload = {
            "generated_by": "wash-eval --write-calibration",
            "seeds": args.seeds,
            "detectors": results["calibration"],
        }
        calibration_path().write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        print(f"Wrote {calibration_path()}")
    if args.write_baseline:
        baseline_path().write_text(
            json.dumps(_baseline_view(results), indent=2, sort_keys=True) + "\n"
        )
        print(f"Wrote {baseline_path()}")
    if args.check:
        baseline = json.loads(baseline_path().read_text())
        problems = regressions(_baseline_view(results), baseline)
        shipped = json.loads(calibration_path().read_text())["detectors"]
        problems += stale_calibration(results["calibration"], shipped)
        for problem in problems:
            print(f"REGRESSION: {problem}", file=sys.stderr)
        if problems:
            return 1
        print("No regressions against the baseline.")
    return 0


def cli() -> None:
    logging.basicConfig(level=logging.WARNING)
    sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    cli()
