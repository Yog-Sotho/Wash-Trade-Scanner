
import os

import pytest

from core.validators import AuditParameters
from scripts.run_audit import AuditRunner


@pytest.mark.asyncio
async def test_export_results_path_traversal_protection():
    runner = AuditRunner()
    params = AuditParameters(
        chain_id=1,
        pool_address="0x" + "0" * 40,
        use_ml=False,
        use_heuristics=False
    )

    risk_metrics = {
        "overall_risk_score": 0.1,
        "wash_trade_volume_ratio": 0.1,
        "total_trades_analyzed": 10,
        "total_volume_usd": 1000.0,
        "wash_trade_volume_usd": 100.0,
        "first_trade_timestamp": None
    }

    # Attempting to export to a "traversal" path
    traversal_path = "/tmp/sentinel_test_traversal.json"

    # We expect the file to be created in the CURRENT directory, not /tmp/
    expected_filename = "sentinel_test_traversal.json"

    if os.path.exists(traversal_path):
        os.remove(traversal_path)
    if os.path.exists(expected_filename):
        os.remove(expected_filename)

    await runner._export_results(
        params=params,
        trades_processed=10,
        wash_trades_detected=1,
        risk_metrics=risk_metrics,
        detection_methods=["heuristic"],
        duration=1.0,
        export_format="json",
        export_path=traversal_path
    )

    # Check that it DOES NOT exist in /tmp/
    assert not os.path.exists(traversal_path)

    # Check that it DOES exist in current directory (sanitized)
    assert os.path.exists(expected_filename)

    # Cleanup
    if os.path.exists(expected_filename):
        os.remove(expected_filename)
