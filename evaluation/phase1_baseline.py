"""
Measure the Phase 1 detectors (commit b513973) on this harness, for the
"before" column of docs/benchmarks.md (evaluation/phase1_results.json).

The old detectors must run as they were, so this runs inside a worktree of
that commit, with the harness copied in:

    git worktree add --detach /tmp/phase1 b513973
    cp -r evaluation /tmp/phase1/ && cp core/calibration.py /tmp/phase1/core/
    cd /tmp/phase1 && python -m evaluation.phase1_baseline 10 /path/to/phase1_results.json

The old SwapTrade has no tx_from/tx_to columns, so the harness's EvalTrade
drops them and the old detectors see exactly the fields they knew.
"""

import asyncio
import json
import logging
import sys
from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncSession

from core.heuristics import HeuristicDetector
from evaluation.dataset import fixture_names, load_fixture
from evaluation.inject import PATTERNS
from evaluation.runner import DetectorFn, _run_scenarios, _summarise

_NO_SESSION = cast(AsyncSession, None)


def phase1_suite() -> dict[str, DetectorFn]:
    h: Any = HeuristicDetector()

    def plain(method: Any) -> DetectorFn:
        async def run(trades: Any, _clusters: Any) -> Any:
            return await method(trades, _NO_SESSION)

        return run

    async def wash_cluster(trades: Any, clusters: Any) -> Any:
        return await h.detect_wash_clusters(trades, clusters, _NO_SESSION)

    async def phase1_audit(trades: Any, clusters: Any) -> Any:
        flagged, _ = await h.run_detectors_on_trades(trades, _NO_SESSION, clusters)
        return flagged

    return {
        "self_trading": plain(h.detect_self_trading),
        "circular_trading": plain(h.detect_circular_trading),
        "position_neutral_scc": plain(h.advanced.detect_position_neutral_scc),
        "closed_cluster": plain(h.advanced.detect_closed_cluster),
        "repeated_amounts": plain(h.advanced.detect_repeated_amounts),
        "high_frequency_bot": plain(h.detect_high_frequency_bot),
        "volume_anomaly": plain(h.detect_volume_anomaly),
        "wash_cluster": wash_cluster,
        "phase1_audit": phase1_audit,
    }


async def main(seeds: int, out: str) -> None:
    loaded = [load_fixture(name) for name in fixture_names()]
    results, totals = await _run_scenarios(loaded, phase1_suite(), range(seeds), PATTERNS, True)
    payload = {
        "seeds": seeds,
        **totals,
        "commit": "b513973",
        "detectors": {
            # Run times vary between runs; everything else is deterministic.
            name: {k: v for k, v in _summarise(result).items() if k != "seconds"}
            for name, result in results.items()
        },
    }
    with open(out, "w") as fh:
        fh.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    logging.disable(logging.CRITICAL)
    asyncio.run(main(int(sys.argv[1]), sys.argv[2]))
