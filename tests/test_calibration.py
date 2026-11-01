"""
Composite scoring: measured detector rates -> P(wash | detector fired).
"""

import json

import pytest
from scipy.stats import beta

from core.calibration import (
    DetectorCalibration,
    fpr_upper_bound,
    load_calibration,
    override_calibration,
    wash_probability,
)


def test_posterior_is_bayes_rule():
    c = DetectorCalibration(tpr=0.8, fpr_upper=0.002)
    prior = 0.01
    expected = 0.8 * prior / (0.8 * prior + 0.002 * (1 - prior))
    assert c.posterior(prior) == pytest.approx(expected)


def test_posterior_edge_cases():
    assert DetectorCalibration(tpr=0.0, fpr_upper=0.0).posterior(0.01) == 0.0
    assert DetectorCalibration(tpr=0.0, fpr_upper=0.1).posterior(0.01) == 0.0
    assert DetectorCalibration(tpr=0.5, fpr_upper=0.0).posterior(0.01) == 1.0


def test_zero_false_positives_still_have_an_upper_bound():
    """No false positive in N trades does not mean a zero rate: the 95%
    one-sided bound is about 3/N (the rule of three)."""
    bound = fpr_upper_bound(0, 10_000)
    assert bound == pytest.approx(1 - 0.05 ** (1 / 10_000), rel=1e-6)
    assert bound == pytest.approx(3 / 10_000, rel=0.01)


def test_upper_bound_matches_clopper_pearson_and_exceeds_the_rate():
    for fp, n in [(1, 100), (17, 15_000), (106, 15_485)]:
        bound = fpr_upper_bound(fp, n)
        assert bound == pytest.approx(beta.ppf(0.95, fp + 1, n - fp))
        assert bound > fp / n


def test_upper_bound_degenerate_inputs():
    assert fpr_upper_bound(0, 0) == 1.0
    assert fpr_upper_bound(5, 5) == 1.0


def test_shipped_calibration_covers_every_audit_detector():
    """Every detector the audit runs must have measured rates; a missing one
    could never label a trade."""
    shipped = load_calibration()
    assert set(shipped) == {
        "round_trip",
        "position_neutral",
        "circular_trading",
        "closed_cluster",
        "repeated_amounts",
        "high_frequency_bot",
        "volume_anomaly",
        "wash_cluster",
    }
    for c in shipped.values():
        assert 0.0 <= c.tpr <= 1.0
        assert 0.0 < c.fpr_upper <= 1.0  # never zero: an upper bound


def test_shipped_calibration_file_is_well_formed():
    from importlib import resources

    payload = json.loads((resources.files("config") / "detector_calibration.json").read_text())
    assert payload["generated_by"] == "wash-eval --write-calibration"
    for d in payload["detectors"].values():
        assert d["fpr"] <= d["fpr_upper"]
        assert d["clean_false_positives"] <= d["clean_trades"]


def test_override_takes_precedence_and_restores():
    override_calibration({"x": DetectorCalibration(tpr=1.0, fpr_upper=0.0)})
    try:
        assert wash_probability("x", 0.01) == 1.0
    finally:
        override_calibration(None)
    assert wash_probability("x", 0.01) == 0.0  # unknown to the shipped file
