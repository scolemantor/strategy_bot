"""Tests for scanners/backtest/drift_detector.py.

Mix of unit tests (pure-function math against synthetic in-memory inputs)
and integration tests (against the extended tests/fixtures/ CSVs which have
deliberate drift injected into short_squeeze and breakout_52w).
"""
from __future__ import annotations

import math
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scanners.backtest import drift_detector as dd

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def report_dir(tmp_path: Path) -> Path:
    shutil.copy(FIXTURES / "sample_pipeline_report.csv", tmp_path / "pipeline_edge_report.csv")
    shutil.copy(FIXTURES / "sample_basket.csv", tmp_path / "basket.csv")
    shutil.copy(FIXTURES / "sample_picks_returns.csv", tmp_path / "picks_returns.csv")
    return tmp_path


# --- unit: _compute_z ----------------------------------------------------


def test_compute_z_basic_math():
    # baseline mean=0, std=1 (sample). recent mean=0.5, n_recent=4.
    # SE = 1 / sqrt(4) = 0.5. z = (0.5 - 0) / 0.5 = 1.0.
    baseline = np.array([-1.5, -0.5, 0.0, 0.5, 1.5], dtype=float)  # mean=0, std=1.118
    recent = np.array([0.5, 0.5, 0.5, 0.5], dtype=float)
    z = dd._compute_z(recent, baseline)
    expected = (0.5 - 0.0) / (baseline.std(ddof=1) / math.sqrt(4))
    assert abs(z - expected) < 1e-9


def test_compute_z_handles_short_baseline():
    assert math.isnan(dd._compute_z(np.array([1.0]), np.array([0.5])))
    assert math.isnan(dd._compute_z(np.array([1.0]), np.array([])))


def test_compute_z_handles_zero_variance():
    baseline = np.array([0.5, 0.5, 0.5, 0.5], dtype=float)
    recent = np.array([1.0, 1.0], dtype=float)
    assert math.isnan(dd._compute_z(recent, baseline))


def test_compute_z_skips_nan_in_inputs():
    baseline = np.array([0.0, np.nan, 1.0, 2.0], dtype=float)  # effective: [0, 1, 2]
    recent = np.array([np.nan, 3.0], dtype=float)              # effective: [3]
    z = dd._compute_z(recent, baseline)
    base_clean = np.array([0.0, 1.0, 2.0])
    expected = (3.0 - 1.0) / (base_clean.std(ddof=1) / math.sqrt(1))
    assert abs(z - expected) < 1e-9


# --- unit: severity ------------------------------------------------------


def test_severity_label_thresholds():
    t = -1.5
    assert dd._severity_label(-2.51, t) == "SEVERE"     # |z| >= 1.5 * 1.67 = 2.505
    assert dd._severity_label(-2.20, t) == "HIGH"       # |z| >= 1.5 * 1.33 = 1.995
    assert dd._severity_label(-1.60, t) == "MODERATE"   # |z| >= 1.5
    assert dd._severity_label(-1.40, t) == "below_threshold"
    assert dd._severity_label(+2.51, t) == "SEVERE"     # symmetric
    assert dd._severity_label(float("nan"), t) == "n/a"


# --- unit: _per_scanner_weekly_excess dedupes buckets --------------------


def test_per_scanner_weekly_excess_dedupes_buckets():
    # Same pick (AAPL, 2025-01-06) replicated across 3 buckets — must count once.
    basket = pd.DataFrame([
        ("AAPL", "2025-01-06", 1, 99.0, 1, "insider_buying", 5),
        ("AAPL", "2025-01-06", 1, 99.0, 1, "insider_buying", 10),
        ("AAPL", "2025-01-06", 1, 99.0, 1, "insider_buying", 20),
    ], columns=["ticker", "surface_date", "rank", "composite_score", "n_scanners", "scanners_hit", "top_n_bucket"])
    returns = pd.DataFrame([
        ("AAPL", "2025-01-06", 21, 0.05, 0.03),
    ], columns=["ticker", "surface_date", "horizon_days", "forward_return", "excess_return"])

    df = dd._per_scanner_weekly_excess(basket, returns, horizon=21)
    assert len(df) == 1
    assert df.iloc[0]["scanner"] == "insider_buying"
    assert df.iloc[0]["n_picks"] == 1
    assert abs(df.iloc[0]["mean_excess"] - 0.03) < 1e-9


def test_per_scanner_weekly_excess_explodes_multi_attribution():
    basket = pd.DataFrame([
        ("MSFT", "2025-01-06", 1, 99.0, 2, "insider_buying,short_squeeze", 5),
    ], columns=["ticker", "surface_date", "rank", "composite_score", "n_scanners", "scanners_hit", "top_n_bucket"])
    returns = pd.DataFrame([
        ("MSFT", "2025-01-06", 21, 0.05, 0.02),
    ], columns=["ticker", "surface_date", "horizon_days", "forward_return", "excess_return"])

    df = dd._per_scanner_weekly_excess(basket, returns, horizon=21)
    assert set(df["scanner"]) == {"insider_buying", "short_squeeze"}
    # Each scanner sees the pick once with the same excess
    for _, row in df.iterrows():
        assert row["n_picks"] == 1
        assert abs(row["mean_excess"] - 0.02) < 1e-9


# --- unit: _rolling_mean -------------------------------------------------


def test_rolling_mean_min_periods_enforced():
    s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    r = dd._rolling_mean(s, window=3)
    # First two values should be NaN (min_periods=window)
    assert pd.isna(r.iloc[0])
    assert pd.isna(r.iloc[1])
    assert abs(r.iloc[2] - 2.0) < 1e-9   # mean(1,2,3)
    assert abs(r.iloc[3] - 3.0) < 1e-9   # mean(2,3,4)
    assert abs(r.iloc[4] - 4.0) < 1e-9   # mean(3,4,5)


# --- integration: timeline shape -----------------------------------------


def test_compute_drift_timeline_shape(report_dir):
    basket = dd._load_basket(report_dir)
    returns = dd._load_returns(report_dir, [21])
    timeline = dd.compute_drift_timeline(basket, returns, window=8, horizon=21)

    for col in ("scanner", "surface_date", "n_picks", "mean_excess", "rolling_mean"):
        assert col in timeline.columns
    # Five distinct scanners present in fixture
    assert set(timeline["scanner"]) == {
        "insider_buying", "breakout_52w", "thirteen_f_changes",
        "earnings_drift", "short_squeeze",
    }
    # Each scanner should have rows for many weeks (most of the 30)
    for scanner, group in timeline.groupby("scanner"):
        assert len(group) >= 25
    # Rolling mean is NaN for first window-1 weeks and finite afterward
    one_scanner = timeline[timeline["scanner"] == "short_squeeze"].sort_values("surface_date")
    assert pd.isna(one_scanner["rolling_mean"].iloc[0])
    assert pd.notna(one_scanner["rolling_mean"].iloc[-1])


# --- integration: drift detection ----------------------------------------


def test_detect_drift_flags_short_squeeze_as_degrading(report_dir):
    basket = dd._load_basket(report_dir)
    returns = dd._load_returns(report_dir, [21])
    timeline = dd.compute_drift_timeline(basket, returns, window=8, horizon=21)
    degrading, _ = dd.detect_drift(timeline, window=8, z_threshold=-1.5)

    assert "short_squeeze" in set(degrading["scanner"])
    row = degrading[degrading["scanner"] == "short_squeeze"].iloc[0]
    assert row["z_score"] < -1.5
    assert row["severity"] in ("MODERATE", "HIGH", "SEVERE")
    assert row["recent_mean_excess_pct"] < row["baseline_mean_excess_pct"]


def test_detect_drift_flags_breakout_52w_as_improving(report_dir):
    basket = dd._load_basket(report_dir)
    returns = dd._load_returns(report_dir, [21])
    timeline = dd.compute_drift_timeline(basket, returns, window=8, horizon=21)
    _, improving = dd.detect_drift(timeline, window=8, z_threshold=-1.5)

    assert "breakout_52w" in set(improving["scanner"])
    row = improving[improving["scanner"] == "breakout_52w"].iloc[0]
    assert row["z_score"] > 1.5
    assert row["severity"] in ("MODERATE", "HIGH", "SEVERE")
    assert row["recent_mean_excess_pct"] > row["baseline_mean_excess_pct"]


def test_stable_scanners_not_flagged_at_high_severity(report_dir):
    """thirteen_f_changes and earnings_drift have no injected drift.
    They might cross MODERATE on noise, but should not show HIGH/SEVERE."""
    basket = dd._load_basket(report_dir)
    returns = dd._load_returns(report_dir, [21])
    timeline = dd.compute_drift_timeline(basket, returns, window=8, horizon=21)
    degrading, improving = dd.detect_drift(timeline, window=8, z_threshold=-1.5)

    for stable_scanner in ("thirteen_f_changes", "earnings_drift"):
        for df in (degrading, improving):
            sub = df[df["scanner"] == stable_scanner]
            if not sub.empty:
                assert sub.iloc[0]["severity"] not in ("HIGH", "SEVERE"), (
                    f"{stable_scanner} unexpectedly flagged at HIGH/SEVERE: "
                    f"{sub.to_dict('records')}"
                )


def test_baseline_365d_cap_is_noop_for_1y_data(report_dir):
    """Fixtures span ~30 weeks (~210 days) < 365d cap. Cap should not exclude
    any baseline weeks. Verify by checking n_baseline_weeks equals the count
    of weekly observations before recent."""
    basket = dd._load_basket(report_dir)
    returns = dd._load_returns(report_dir, [21])
    timeline = dd.compute_drift_timeline(basket, returns, window=8, horizon=21)
    degrading, improving = dd.detect_drift(timeline, window=8, z_threshold=-1.5)

    # short_squeeze appears in many weeks; check its baseline count
    all_alerts = pd.concat([degrading, improving], ignore_index=True)
    short_squeeze_weeks = timeline[
        (timeline["scanner"] == "short_squeeze") & timeline["mean_excess"].notna()
    ]
    expected_baseline = len(short_squeeze_weeks) - 8  # minus recent window
    row = all_alerts[all_alerts["scanner"] == "short_squeeze"].iloc[0]
    assert row["n_baseline_weeks"] == expected_baseline


# --- integration: I/O errors ---------------------------------------------


def test_load_returns_raises_when_missing(tmp_path):
    with pytest.raises(FileNotFoundError) as excinfo:
        dd._load_returns(tmp_path, [21])
    assert "analyze_pipeline" in str(excinfo.value)


def test_load_basket_raises_when_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        dd._load_basket(tmp_path)


# --- CLI -----------------------------------------------------------------


def test_cli_dispatch(report_dir, capsys):
    dd.main([
        "--report-dir", str(report_dir),
        "--window-weeks", "8",
        "--z-threshold", "-1.5",
        "--horizon", "21",
    ])
    captured = capsys.readouterr()
    assert "DEGRADING" in captured.out
    assert "IMPROVING" in captured.out
    assert "Drift timeline" in captured.out


def test_cli_writes_csv_when_out_dir_set(report_dir, tmp_path, capsys):
    out = tmp_path / "drift_out"
    dd.main([
        "--report-dir", str(report_dir),
        "--horizon", "21",
        "--out-dir", str(out),
    ])
    assert (out / "drift_timeline.csv").exists()
    assert (out / "drift_alerts_degrading.csv").exists()
    assert (out / "drift_alerts_improving.csv").exists()
