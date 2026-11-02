"""
The evaluation harness itself: metrics, campaign injection, fixtures and the
regression gate. A harness bug would silently corrupt every published number.
"""

import gzip
import json
import random
from datetime import datetime, timedelta

import pytest
from sklearn.metrics import average_precision_score

from evaluation.cli import regressions
from evaluation.dataset import to_swap_trades
from evaluation.inject import PATTERNS, Market, inject
from evaluation.metrics import Counts, average_precision
from evaluation.record import write_fixture

USDC = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WETH = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
POOL = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
ROUTER = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"


def _background(n: int = 400) -> list[dict]:
    """A synthetic honest USDC/WETH history at ETH = $2,000, 0.3% fee."""
    rng = random.Random(0)
    rows = []
    t0 = datetime(2026, 1, 1)
    for i in range(n):
        trader = "0x" + f"{rng.getrandbits(160):040x}"
        usd = rng.uniform(100, 20_000)
        sell_usdc = rng.random() < 0.5
        rows.append(
            {
                "chain_id": 1,
                "dex_name": "UniswapV2",
                "pool_address": POOL,
                "token_in": USDC if sell_usdc else WETH,
                "token_out": WETH if sell_usdc else USDC,
                "amount_in": usd * 1e6 if sell_usdc else usd / 2000 * 1e18,
                "amount_out": usd * 0.997 / 2000 * 1e18 if sell_usdc else usd * 0.997 * 1e6,
                "sender": ROUTER,
                "recipient": trader,
                "tx_from": trader,
                "tx_to": ROUTER,
                "transaction_hash": "0x" + f"{i:064x}",
                "block_number": 1_000 + 3 * i,
                "block_timestamp": t0 + timedelta(seconds=36 * i),
                "gas_price": 20.0,
                "gas_used": None,
                "log_index": 0,
                "amount_in_usd": usd,
                "amount_out_usd": usd * 0.997,
                "volume_usd": usd,
            }
        )
    return rows


# ------------------------------------------------------------------ metrics


def test_counts_rates():
    c = Counts()
    for flagged, positive in [(1, 1), (1, 1), (0, 1), (1, 0), (0, 0), (0, 0), (0, 0)]:
        c.add(bool(flagged), bool(positive), 10.0)
    assert (c.tp, c.fp, c.fn, c.tn) == (2, 1, 1, 3)
    assert c.precision == pytest.approx(2 / 3)
    assert c.recall == pytest.approx(2 / 3)
    assert c.f1 == pytest.approx(2 / 3)
    assert c.fpr == pytest.approx(1 / 4)
    assert c.fp_volume_share == pytest.approx(10 / 40)


def test_counts_undefined_rates_are_none():
    c = Counts()
    c.add(False, False, 1.0)
    assert c.precision is None and c.recall is None and c.f1 is None
    assert c.fpr == 0.0


@pytest.mark.parametrize("seed", range(5))
def test_average_precision_matches_scikit_learn(seed):
    rng = random.Random(seed)
    labels = [rng.random() < 0.3 for _ in range(200)]
    # Scores with ties, and a block of unflagged (zero) scores.
    scores = [
        0.0 if rng.random() < 0.4 else round(rng.random() + (0.5 if y else 0), 1) for y in labels
    ]
    ours = average_precision(scores, labels)
    # scikit-learn treats score 0 as the lowest threshold, which our metric
    # deliberately excludes (unflagged is not an operating point). Compare on
    # the flagged trades plus the missed positives at the bottom.
    expected = average_precision_score(labels, scores)
    flagged_tp = sum(1 for s, y in zip(scores, labels, strict=True) if y and s > 0)
    zero_tp = sum(labels) - flagged_tp
    zero_total = sum(1 for s in scores if s == 0)
    last_point = (zero_tp / sum(labels)) * (sum(labels) / len(labels)) if zero_total else 0
    assert ours == pytest.approx(expected - last_point, abs=1e-9)


def test_average_precision_of_a_perfect_binary_detector_is_one():
    assert average_precision([1, 1, 0, 0], [True, True, False, False]) == pytest.approx(1.0)
    assert average_precision([0, 0], [False, False]) is None


# ------------------------------------------------------------------ injection


@pytest.mark.parametrize("pattern", PATTERNS)
def test_injection_is_deterministic_and_well_formed(pattern):
    market = Market(_background(), chain_id=1)
    first = inject(pattern, market, seed=3)
    again = inject(pattern, market, seed=3)
    other = inject(pattern, market, seed=4)

    assert first.rows == again.rows
    assert first.rows != other.rows
    assert first.rows, "a campaign must inject trades"
    keys = set(_background()[0])
    for row in first.rows:
        assert set(row) == keys  # same columns the ingestor produces
        assert row["pool_address"] == POOL
        assert row["tx_from"] in first.wallets
        assert row["volume_usd"] and row["volume_usd"] > 0
        assert market.first_block <= row["block_number"]
    assert len({r["transaction_hash"] for r in first.rows}) == len(first.rows)


def test_round_trips_sell_back_what_they_bought_within_25_blocks():
    market = Market(_background(), chain_id=1)
    campaign = inject("aa_round_trip", market, seed=1)
    rows = campaign.rows
    assert len(rows) % 2 == 0 and len(rows) >= 6
    for buy, sell in zip(rows[::2], rows[1::2], strict=True):
        assert sell["token_in"] == buy["token_out"]
        assert sell["amount_in"] == buy["amount_out"]
        assert 1 <= sell["block_number"] - buy["block_number"] <= 25
        # Value matched within 1% (one pool fee apart).
        assert abs(sell["volume_usd"] - buy["volume_usd"]) / buy["volume_usd"] < 0.01


def test_funded_ring_exposes_its_funding_cluster_and_unlinked_does_not():
    market = Market(_background(), chain_id=1)
    linked = inject("funded_ring", market, seed=2)
    unlinked = inject("unlinked_ring", market, seed=2)
    assert linked.clusters == [linked.wallets] and len(linked.wallets) >= 5
    assert unlinked.clusters == []


def test_repeated_size_bot_repeats_one_size_through_a_bot_contract():
    market = Market(_background(), chain_id=1)
    campaign = inject("repeated_size_bot", market, seed=5)
    opening_sizes = {row["amount_in"] for row in campaign.rows[::2]}
    assert len(opening_sizes) == 1
    assert all(row["sender"] == row["recipient"] == row["tx_to"] for row in campaign.rows)


def test_market_reads_prices_and_fee_from_the_background():
    market = Market(_background(), chain_id=1)
    assert market.router == ROUTER
    assert market.out_in_ratio == pytest.approx(0.997)
    assert market.unit_usd(WETH, 1_000) * 1e18 == pytest.approx(2000.0)
    assert market.unit_usd(USDC, 1_000) * 1e6 == pytest.approx(1.0)
    assert market.timestamp(1_003) == datetime(2026, 1, 1) + timedelta(seconds=36)


# ------------------------------------------------------------------ fixtures


def test_fixture_round_trip(tmp_path):
    rows = _background(10)
    path = tmp_path / "f.json.gz"
    write_fixture(str(path), {"name": "f", "kind": "clean"}, rows)
    first = path.read_bytes()
    write_fixture(str(path), {"name": "f", "kind": "clean"}, rows)
    assert path.read_bytes() == first  # byte-identical re-recording

    payload = json.loads(gzip.decompress(first))
    assert payload["meta"]["kind"] == "clean"
    assert payload["trades"][0]["block_timestamp"] == rows[0]["block_timestamp"].isoformat()


def test_to_swap_trades_orders_and_numbers_trades():
    rows = list(reversed(_background(5)))
    trades = to_swap_trades(rows)
    assert [t.id for t in trades] == [1, 2, 3, 4, 5]
    assert [t.block_number for t in trades] == sorted(t.block_number for t in trades)
    assert all(t.is_wash_trade is False and t.wash_trade_score == 0.0 for t in trades)


# ------------------------------------------------------------------ gate


def _gate_results(f1=0.8, recall=0.7, fpr=0.01, seeds=10):
    return {
        "seeds": seeds,
        "detectors": {
            "d": {
                "overall": {"precision": 0.9, "recall": recall, "f1": f1, "pr_auc": 0.8},
                "clean_pools": {"all": {"fpr": fpr}},
            }
        },
    }


def test_gate_passes_identical_and_improved_results():
    base = _gate_results()
    assert regressions(base, base) == []
    assert regressions(_gate_results(f1=0.9, recall=0.8, fpr=0.0), base) == []


@pytest.mark.parametrize(
    "current",
    [_gate_results(f1=0.7), _gate_results(recall=0.6), _gate_results(fpr=0.02)],
)
def test_gate_flags_regressions(current):
    assert regressions(current, _gate_results())


def test_gate_refuses_to_compare_different_seed_counts():
    assert regressions(_gate_results(seeds=5), _gate_results())


def test_gate_flags_a_missing_detector():
    current = {"seeds": 10, "detectors": {}}
    assert regressions(current, _gate_results()) == ["d: detector missing from the current run"]


# ------------------------------------------------------------------ fidelity


async def test_detectors_see_the_same_trades_with_or_without_the_orm():
    """The harness runs detectors on lightweight EvalTrade records; they must
    produce exactly what the ORM objects an audit uses would produce."""
    from core.calibration import DetectorCalibration, override_calibration
    from evaluation.runner import _clusters, detector_suite

    background = _background(300)
    market = Market(background, chain_id=1)
    campaigns = [inject(p, market, seed=0) for p in PATTERNS]
    rows = background + [row for c in campaigns for row in c.rows]
    clusters = [cl for c in campaigns for cl in _clusters(c)]
    suite = detector_suite()
    override_calibration({n: DetectorCalibration(0.9, 0.0001) for n in suite})
    detectors_with_hits = set()
    try:
        for name, detector in suite.items():
            light = to_swap_trades(rows)
            orm = to_swap_trades(rows, orm=True)
            flagged_light = await detector(light, clusters)
            flagged_orm = await detector(orm, clusters)
            assert [t.id for t in flagged_light] == [t.id for t in flagged_orm], name
            assert [(t.wash_trade_score, t.detection_method) for t in light] == [
                (t.wash_trade_score, t.detection_method) for t in orm
            ], name
            if flagged_orm:
                detectors_with_hits.add(name)
    finally:
        override_calibration(None)
    # Not vacuous: the comparison covers real hits, including the decisive ones.
    assert {"round_trip", "repeated_amounts", "composite"} <= detectors_with_hits
    assert len(detectors_with_hits) >= 4


def test_shipped_defaults_are_the_selected_parameters():
    """config/settings.py defaults must be what python -m evaluation.tune chose
    (evaluation/tuning.json), so the benchmark describes the shipped detectors."""
    from importlib import resources

    from config.settings import Settings

    tuning = json.loads((resources.files("evaluation") / "tuning.json").read_text())
    defaults = {name: field.default for name, field in Settings.model_fields.items()}
    for detector, outcome in tuning["detectors"].items():
        for name, value in outcome["best"]["params"].items():
            assert defaults[name] == value, f"{detector}: {name}"


# ------------------------------------------------------------- runner/report


def _fixture(kind: str, name: str, rows):
    from evaluation.dataset import Fixture

    meta = {
        "name": name,
        "kind": kind,
        "description": f"{name} test fixture",
        "chain_id": 1,
        "pool_address": POOL,
        "from_block": rows[0]["block_number"],
        "to_block": rows[-1]["block_number"],
        "trades": len(rows),
        "with_tx_from": len(rows),
        "priced": len(rows),
    }
    return Fixture(meta=meta, rows=rows)


async def test_scenarios_calibration_and_report_on_a_synthetic_pool():
    from core.calibration import DetectorCalibration, override_calibration
    from evaluation.report import render
    from evaluation.runner import _run_scenarios, _summarise, calibration_from, detector_suite

    clean = _fixture("clean", "synthetic", _background(300))
    documented = _fixture("documented_wash", "nexf_weth", _background(50))
    suite = detector_suite()
    override_calibration({n: DetectorCalibration(0.9, 0.0001) for n in suite})
    try:
        results, totals = await _run_scenarios(
            [clean, documented], suite, range(2), PATTERNS, include_documented=True
        )
    finally:
        override_calibration(None)

    assert totals["scenarios"] == 2 * len(PATTERNS)
    assert totals["background_trades"] == 300 * totals["scenarios"]
    round_trip = results["round_trip"]
    assert round_trip.by_pattern["aa_round_trip"].recall == 1.0
    assert round_trip.clean["synthetic"].fp == 0  # honest synthetic traders never round-trip
    assert results["composite"].documented["nexf_weth"]["trades"] == 50

    calibration = calibration_from(results)
    assert calibration["round_trip"]["clean_trades"] == 300
    assert calibration["round_trip"]["fpr"] <= calibration["round_trip"]["fpr_upper"]

    summary = {
        "seeds": 2,
        "patterns": list(PATTERNS),
        **totals,
        "fixtures": {f.name: f.meta for f in (clean, documented)},
        "calibration": calibration,
        "detectors": {n: _summarise(r) for n, r in results.items()},
        "ml_isolation_forest": {
            "folds": {"synthetic": {"pr_auc": 0.1, "prevalence": 0.01, "tpr": 0.0, "fpr": 0.01}},
            "mean_pr_auc": 0.1,
            "std_pr_auc": 0.0,
            "mean_tpr": 0.0,
            "mean_fpr": 0.01,
        },
    }
    before = {"commit": "b513973", "detectors": {"phase1_audit": summary["detectors"]["composite"]}}
    tuning = {
        "round_trip": {
            "best": {
                "params": {"ROUND_TRIP_MAX_BLOCKS": 50},
                "tpr": 0.4,
                "clean_fp": 2,
                "clean_trades": 300,
                "posterior": 0.85,
            },
            "decisive": True,
        }
    }
    text = render(summary, before=before, tuning=tuning)
    for heading in (
        "## Headline",
        "## Data",
        "## Detectors",
        "### Recall by pattern",
        "## Calibration",
        "## Isolation Forest (leave-one-pool-out)",
        "## Documented wash-trading market",
        "## Parameter selection",
    ):
        assert heading in text
    assert "Phase 1 (`b513973`)" in text
    assert render(summary, before=before, tuning=tuning) == text  # deterministic


def test_threshold_for_fpr_caps_the_clean_flag_rate():
    import numpy as np

    from evaluation.ml import threshold_for_fpr

    clean = np.linspace(0.0, 1.0, 1000)
    threshold = threshold_for_fpr(clean, 0.01)
    assert (clean >= threshold).mean() <= 0.01
    assert (clean >= threshold).sum() == 10  # as many as allowed, no fewer
    assert threshold_for_fpr(np.array([0.5, 0.5]), 0.0) > 0.5


async def test_ml_measure_on_a_synthetic_pool():
    from evaluation.ml import _frame, _matrix, measure, reference_model, threshold_on

    pool = _fixture("clean", "synthetic", _background(300))
    model = reference_model()
    model.fit_matrix(_matrix(model, _frame(pool.rows, set()).features))
    threshold = await threshold_on(model, [pool], 0.05)

    metrics = await measure(model, threshold, [pool], range(1))

    assert metrics["clean_trades"] == 300
    assert metrics["fpr"] <= 0.05
    assert 0.0 <= metrics["pr_auc"] <= 1.0
    assert 0.0 < metrics["prevalence"] < 1.0


async def test_coinbase_source_values_each_timestamp_at_its_hour_open():
    from aiohttp import web

    from evaluation.record import CoinbaseHourlyPriceSource

    requests = []

    async def candles(request):
        requests.append(dict(request.query))
        # [time, low, high, open, close, volume], newest first
        return web.json_response([[7200, 1, 1, 2002.0, 1, 1], [3600, 1, 1, 2001.0, 1, 1]])

    app = web.Application()
    app.router.add_get("/products/{product}/candles", candles)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    host, port = runner.addresses[0][:2]
    try:
        source = CoinbaseHourlyPriceSource(f"http://{host}:{port}/products/{{product}}/candles")
        prices = await source.get_usd_prices("ethereum", [3600.0, 7199.0, 7200.0, 99_999.0])
        unknown = await source.get_usd_prices("not-a-coin", [1.0])
    finally:
        await runner.cleanup()

    assert prices == {3600.0: 2001.0, 7199.0: 2001.0, 7200.0: 2002.0, 99_999.0: None}
    assert unknown == {1.0: None}
    assert requests[0]["granularity"] == "3600"


def test_tune_candidates_and_overrides_restore_settings():
    from config.settings import settings
    from evaluation.tune import candidates, overridden

    grid = {"ROUND_TRIP_MAX_BLOCKS": [25, 50], "ROUND_TRIP_MIN_COUNT": [2, 3]}
    assert len(list(candidates(grid))) == 4
    before = settings.ROUND_TRIP_MAX_BLOCKS
    with overridden({"ROUND_TRIP_MAX_BLOCKS": 999}):
        assert settings.ROUND_TRIP_MAX_BLOCKS == 999
    assert before == settings.ROUND_TRIP_MAX_BLOCKS


async def test_cli_writes_and_then_checks_against_its_own_outputs(tmp_path, monkeypatch):
    """wash-eval --write-* followed by --check passes; a better baseline fails it."""
    import evaluation.cli as cli
    from core.calibration import DetectorCalibration, override_calibration
    from evaluation.runner import _run_scenarios, _summarise, calibration_from, detector_suite

    clean = _fixture("clean", "synthetic", _background(200))
    suite = detector_suite()
    override_calibration({n: DetectorCalibration(0.9, 0.0001) for n in suite})
    try:
        results, totals = await _run_scenarios([clean], suite, range(1), PATTERNS, False)
    finally:
        override_calibration(None)
    summary = {
        "seeds": 1,
        "patterns": list(PATTERNS),
        **totals,
        "fixtures": {"synthetic": clean.meta},
        "calibration": calibration_from({k: v for k, v in results.items() if k != "composite"}),
        "detectors": {n: _summarise(r) for n, r in results.items()},
        "ml_isolation_forest": None,
    }

    async def fake_evaluate(seeds):
        assert seeds == 1
        return summary

    monkeypatch.setattr(cli, "evaluate", fake_evaluate)
    monkeypatch.setattr(cli, "baseline_path", lambda: tmp_path / "baseline.json")
    monkeypatch.setattr(cli, "calibration_path", lambda: tmp_path / "calibration.json")

    args = ["--seeds", "1", "--write-calibration", "--write-baseline"]
    docs, out = tmp_path / "bench.md", tmp_path / "results.json"
    assert await cli.main([*args, "--write-docs", str(docs), "--json", str(out)]) == 0
    assert docs.read_text().startswith("# Detection Benchmarks")
    assert json.loads(out.read_text())["seeds"] == 1
    assert await cli.main(["--seeds", "1", "--check"]) == 0

    baseline = json.loads((tmp_path / "baseline.json").read_text())
    baseline["detectors"]["round_trip"]["overall"]["recall"] = 1.5  # unreachable
    (tmp_path / "baseline.json").write_text(json.dumps(baseline))
    assert await cli.main(["--seeds", "1", "--check"]) == 1


def test_stale_calibration_is_reported():
    from evaluation.cli import stale_calibration

    shipped = {"a": {"tpr": 0.5, "fpr_upper": 0.01}, "gone": {"tpr": 0.1, "fpr_upper": 0.1}}
    measured = {"a": {"tpr": 0.6, "fpr_upper": 0.01}, "new": {"tpr": 0.2, "fpr_upper": 0.1}}
    problems = stale_calibration(measured, shipped)
    assert any(p.startswith("a: tpr is 0.6") for p in problems)
    assert any(p.startswith("gone:") for p in problems)
    assert any(p.startswith("new:") for p in problems)
    assert stale_calibration(shipped, shipped) == []
