"""
The performance benchmark (evaluation/perf.py, docs/performance.md) on a
small slice of its real pool, so the harness cannot rot unnoticed. It also
pins the request pattern on real swaps: one block request per block, none per
transaction, and nothing but logs on a re-audit.
"""

import pytest

from evaluation.perf import run_benchmark

pytestmark = pytest.mark.integration


async def test_the_benchmark_runs_and_ingestion_is_one_request_per_block(pg_storage):
    await pg_storage.close()  # the harness empties the database itself
    results = await run_benchmark(trades=300, latency_ms=0.0)

    audit, re_audit = results["runs"]["audit"], results["runs"]["re_audit"]
    assert audit["trades_processed"] == re_audit["trades_processed"] == 300
    assert audit["wash_trades_detected"] == re_audit["wash_trades_detected"]

    methods = audit["rpc_by_method"]
    assert methods["eth_getBlockByNumber"] == results["blocks_with_swaps"]
    assert "eth_getTransactionByHash" not in methods
    assert set(audit["stages"]) >= {"ingest", "heuristics", "ml", "audit_log"}

    assert "eth_getBlockByNumber" not in re_audit["rpc_by_method"]
    assert re_audit["rpc_requests"] < 15
