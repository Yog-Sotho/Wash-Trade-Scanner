"""
Evidence bundles (roadmap Phase 6, explainability): every labelled trade says
which detectors fired, what each saw, and what its hit is worth.
"""

from datetime import datetime, timedelta

import pytest

from config.settings import settings
from core.calibration import DetectorCalibration, override_calibration
from core.evidence import EvidenceLog, Finding, evidence_summary, merge_bundle
from core.heuristics import HeuristicDetector
from evaluation.dataset import load_fixture, to_swap_trades
from evaluation.inject import Market, inject
from models.schemas import AddressCluster, SwapTrade

WETH, USDC = "0x" + "e" * 40, "0x" + "c" * 40
TRADER = "0x" + "a" * 40
START = datetime(2026, 1, 15, 12, 0)


@pytest.fixture(autouse=True)
def calibrated():
    names = (
        "round_trip",
        "position_neutral",
        "circular_trading",
        "closed_cluster",
        "repeated_amounts",
        "high_frequency_bot",
        "volume_anomaly",
        "wash_cluster",
    )
    override_calibration({n: DetectorCalibration(tpr=0.9, fpr_upper=0.0001) for n in names})
    yield
    override_calibration(None)


def swap(i: int, block: int, sold: str, bought: str, usd: float, trader: str = TRADER) -> SwapTrade:
    return SwapTrade(
        id=i,
        chain_id=1,
        pool_address="0xpool",
        token_in=sold,
        token_out=bought,
        amount_in=usd,
        amount_out=usd,
        sender="0x" + "7" * 40,
        recipient=trader,
        tx_from=trader,
        transaction_hash="0x" + f"{i:064x}",
        log_index=0,
        block_number=block,
        block_timestamp=START + timedelta(seconds=12 * (block - 100)),
        volume_usd=usd,
        is_wash_trade=False,
        wash_trade_score=0.0,
    )


def round_trips() -> list[SwapTrade]:
    """Three buy-and-sell-backs by one trader, legs 2 blocks and 0.5% apart."""
    trades = []
    for n in range(3):
        block = 100 + 50 * n
        trades.append(swap(2 * n + 1, block, USDC, WETH, 1000.0))
        trades.append(swap(2 * n + 2, block + 2, WETH, USDC, 995.0))
    return trades


async def test_a_round_trip_names_its_partner_leg_the_gap_and_the_count():
    trades = round_trips()
    wash, _ = await HeuristicDetector().run_detectors_on_trades(trades, None)

    assert {t.id for t in wash} == {1, 2, 3, 4, 5, 6}
    bundle = trades[0].wash_evidence
    entry = next(d for d in bundle["detectors"] if d["detector"] == "round_trip")
    assert [t["tx"] for t in entry["trades"]] == [
        trades[0].transaction_hash,
        trades[1].transaction_hash,
    ]
    assert entry["entity"] == TRADER
    assert entry["details"]["blocks_apart"] == 2
    assert entry["details"]["value_gap"] == pytest.approx(0.005)
    assert entry["details"]["round_trips_by_trader"] == 3
    assert entry["summary"] == (
        "Bought and sold back 2 blocks later, the two legs' USD values 0.50% apart; "
        "one of 3 such round trips by this trader in this pool"
    )
    assert (entry["tpr"], entry["fpr_upper"]) == (0.9, 0.0001)
    assert bundle["probability"] == pytest.approx(trades[0].wash_trade_score)


async def test_a_funding_cluster_window_lists_its_members_and_net_position():
    a, b = "0x" + "1" * 40, "0x" + "2" * 40
    trades = [
        swap(1, 100, USDC, WETH, 1000.0, a),
        swap(2, 101, WETH, USDC, 1000.0, b),
        swap(3, 102, USDC, WETH, 500.0, b),
        swap(4, 103, WETH, USDC, 500.0, a),
    ]
    cluster = AddressCluster(cluster_id="c1", addresses=[a, b], confidence_score=0.8)
    log = EvidenceLog()
    flagged = await HeuristicDetector().advanced.detect_position_neutral(
        trades, None, [cluster], evidence=log
    )

    assert len(flagged) == 4
    assert all(log.findings(t) for t in flagged)  # each flagged trade explained
    # One finding per window size it was neutral in; the smallest comes first.
    findings = log.findings(trades[0])
    assert [f.details["window_hours"] for f in findings] == sorted(
        settings.POSITION_NEUTRAL_WINDOWS_HOURS
    )
    finding = findings[0]
    assert finding.entity == "cluster:c1"
    assert finding.details["members"] == sorted([a, b])
    assert finding.details["trades_in_window"] == 4
    assert finding.details["net_to_gross"] == {USDC: 0.0, WETH: 0.0}
    assert finding.summary.startswith("Funding cluster of 2 addresses made 4 trades")


async def test_repeated_sizes_name_the_amount_and_how_often():
    trades = [swap(i, 100 + 10 * i, USDC, WETH, 1234.0) for i in range(1, 7)]
    log = EvidenceLog()
    await HeuristicDetector().advanced.detect_repeated_amounts(trades, None, evidence=log)

    (finding,) = log.findings(trades[0])
    assert finding.details["times"] == 6
    assert finding.details["amount"] == 1230.0
    assert (
        finding.summary
        == f"Sold 1.23e+03 raw units of {USDC} 6 times (equal to 3 significant digits)"
    )


async def test_every_detector_explains_every_trade_it_flags():
    """A detector that labels without a finding would leave a label nobody can
    check. Run each one over a real pool with every injected pattern."""
    fixture = load_fixture("univ2_usdc_weth")
    market = Market(fixture.rows, 1)
    h = HeuristicDetector()
    detectors = {
        "round_trip": h.advanced.detect_round_trips,
        "circular_trading": h.detect_circular_trading,
        "closed_cluster": h.advanced.detect_closed_cluster,
        "repeated_amounts": h.advanced.detect_repeated_amounts,
        "high_frequency_bot": h.detect_high_frequency_bot,
        "volume_anomaly": h.detect_volume_anomaly,
    }
    fired: set[str] = set()
    scenarios = [
        (p, seed)
        for p in ("aa_round_trip", "funded_ring", "repeated_size_bot")
        for seed in range(3)
    ]
    for pattern, seed in scenarios:
        campaign = inject(pattern, market, seed=seed)
        trades = to_swap_trades(fixture.rows + campaign.rows)
        clusters = [
            AddressCluster(cluster_id=f"e{i}", addresses=m, confidence_score=0.8)
            for i, m in enumerate(campaign.clusters)
        ]
        for name, fn in detectors.items():
            log = EvidenceLog()
            for trade in await fn(trades, None, evidence=log):
                fired.add(name)
                assert any(f.detector == name for f in log.findings(trade)), (name, trade.id)
        log = EvidenceLog()
        for trade in await h.advanced.detect_position_neutral(trades, None, clusters, evidence=log):
            fired.add("position_neutral")
            assert any(f.detector == "position_neutral" for f in log.findings(trade))
    # position_neutral is covered by the funding-cluster test above.
    assert {"round_trip", "repeated_amounts", "volume_anomaly"} <= fired


def test_the_bundle_lists_the_most_probative_first_and_one_entry_per_detector():
    override_calibration(
        {
            "volume_anomaly": DetectorCalibration(tpr=0.5, fpr_upper=0.5),
            "round_trip": DetectorCalibration(tpr=0.95, fpr_upper=0.0001),
        }
    )
    trade = swap(1, 100, USDC, WETH, 10.0)
    log = EvidenceLog()
    log.add(Finding("volume_anomaly", [trade], TRADER, "big"))
    log.add(Finding("round_trip", [trade], TRADER, "first window"))
    log.add(Finding("round_trip", [trade], TRADER, "second window"))

    bundle = log.bundle(trade, ["volume_anomaly", "round_trip", "high_frequency_bot"], 0.01, 0.5)

    assert [d["detector"] for d in bundle["detectors"]] == [
        "round_trip",
        "volume_anomaly",
        "high_frequency_bot",  # fired without a finding: still listed
    ]
    assert bundle["detectors"][0]["summary"] == "first window"
    assert bundle["detectors"][2]["summary"] == "Fired; no details recorded"
    assert bundle["decided_by"] == "round_trip"
    assert (bundle["prior"], bundle["threshold"]) == (0.01, 0.5)


def test_a_later_detector_merges_into_a_stored_bundle():
    stored = {
        "probability": 0.6,
        "decided_by": "round_trip",
        "prior": 0.01,
        "threshold": 0.5,
        "detectors": [{"detector": "round_trip", "probability": 0.6, "summary": "rt"}],
    }
    ml = {"detector": "ml_isolation_forest", "probability": 0.9, "summary": "anomalous"}

    merged = merge_bundle(stored, ml, prior=0.01, threshold=0.5)
    assert [d["detector"] for d in merged["detectors"]] == ["ml_isolation_forest", "round_trip"]
    assert (merged["decided_by"], merged["probability"]) == ("ml_isolation_forest", 0.9)
    assert merge_bundle(None, ml, prior=0.01, threshold=0.5)["prior"] == 0.01
    assert evidence_summary(merged) == "ml_isolation_forest: anomalous"
    assert evidence_summary(None) is None
