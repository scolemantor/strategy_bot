"""Tests for scanners/backtest/sharpe_variants.py.

Mix of unit tests (synthetic in-memory series with hand-computed values)
and integration tests against extended fixtures. No scanner-package
imports beyond what scanners.backtest.sharpe_variants triggers; tests
pass DataFrames directly so the public functions don't need
analyze_pipeline at runtime.
"""
from __future__ import annotations

import math
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scanners.backtest.sharpe_variants import (
    compute_sharpe_variants,
    compute_sharpe_variants_for_pipeline,
    compute_sharpe_variants_per_scanner,
    main,
    TRADING_DAYS_PER_YEAR,
)

FIXTURES = Path(__file__).parent / "fixtures"


# === unit tests with hand-computed values ==============================


def test_sharpe_zero_known_inputs():
    """series = [0.02, 0.0] * 126.
    mean = 0.01, sample std (ddof=1) = sqrt(252 * 0.0001 / 251) ≈ 0.010020.
    sharpe_per_period ≈ 0.998, annualized * sqrt(252) ≈ 15.84.
    """
    s = pd.Series([0.02, 0.0] * 126)
    out = compute_sharpe_variants(s, periods_per_year=252, tbill_rate=0.0)
    expected = (0.01 / s.std(ddof=1)) * math.sqrt(252)
    assert abs(out["sharpe_zero"] - expected) < 1e-3


def test_sharpe_zero_equals_excess_spy():
    """Documented design: sharpe_zero == sharpe_excess_spy by construction."""
    s = pd.Series([0.02, 0.0, 0.01, -0.01] * 50)
    out = compute_sharpe_variants(s, periods_per_year=252, tbill_rate=0.04)
    assert out["sharpe_zero"] == out["sharpe_excess_spy"]


def test_sharpe_excess_tbill_subtracts_tbill():
    """Higher tbill -> lower excess_tbill sharpe."""
    s = pd.Series([0.02, 0.0] * 126)
    out_zero_rf = compute_sharpe_variants(s, periods_per_year=252, tbill_rate=0.0)
    out_high_rf = compute_sharpe_variants(s, periods_per_year=252, tbill_rate=0.10)
    # With tbill=0%, excess_tbill == sharpe_zero
    assert abs(out_zero_rf["sharpe_excess_tbill"] - out_zero_rf["sharpe_zero"]) < 1e-9
    # With tbill=10%, sharpe drops
    assert out_high_rf["sharpe_excess_tbill"] < out_zero_rf["sharpe_excess_tbill"]


def test_sortino_only_penalizes_downside():
    """All-positive returns -> sortino = inf (no downside vol)."""
    s = pd.Series([0.01, 0.02, 0.03, 0.01, 0.02])
    out = compute_sharpe_variants(s, periods_per_year=252, tbill_rate=0.0)
    assert math.isinf(out["sortino_zero"])
    assert out["sortino_zero"] > 0


def test_sortino_normal_with_mixed_returns():
    s = pd.Series([0.02, -0.01, 0.03, -0.02, 0.04, -0.01])
    out = compute_sharpe_variants(s, periods_per_year=252, tbill_rate=0.0)
    assert not math.isnan(out["sortino_zero"])
    assert math.isfinite(out["sortino_zero"])
    assert out["sortino_zero"] > 0


def test_calmar_uses_max_drawdown():
    """returns = [0.10, -0.20, 0.10, 0.10, 0.10]
    equity:    [1.10, 0.88, 0.968, 1.0648, 1.17128]
    peak:      [1.10, 1.10, 1.10, 1.10, 1.17128]
    dd:        [0.0, -0.20, -0.12, -0.0320, 0.0]
    max_dd_abs = 0.20
    annual_return = mean(returns) * 252 = 0.04 * 252 = 10.08
    calmar = 10.08 / 0.20 = 50.4
    """
    s = pd.Series([0.10, -0.20, 0.10, 0.10, 0.10])
    out = compute_sharpe_variants(s, periods_per_year=252, tbill_rate=0.0)
    expected_calmar = (s.mean() * 252) / 0.20
    assert abs(out["calmar"] - expected_calmar) < 0.01
    assert out["mar_ratio"] == out["calmar"]


def test_negative_max_drawdown_handled():
    """All-positive returns -> no drawdown -> calmar = inf."""
    s = pd.Series([0.01, 0.02, 0.01, 0.03, 0.02])
    out = compute_sharpe_variants(s, periods_per_year=252, tbill_rate=0.0)
    assert math.isinf(out["calmar"])
    assert out["calmar"] > 0


def test_omega_above_one_for_positive_skew():
    """mean(upside) > mean(|downside|)."""
    s = pd.Series([0.10, 0.10, -0.01, -0.01, -0.01, -0.01])
    out = compute_sharpe_variants(s, periods_per_year=252)
    assert out["omega_ratio"] > 1.0
    assert abs(out["omega_ratio"] - 10.0) < 0.001  # 0.10 / 0.01


def test_omega_below_one_for_negative_skew():
    s = pd.Series([0.01, 0.01, 0.01, -0.10, -0.10])
    out = compute_sharpe_variants(s, periods_per_year=252)
    assert out["omega_ratio"] < 1.0
    assert abs(out["omega_ratio"] - 0.1) < 0.001  # 0.01 / 0.10


def test_omega_no_downside_returns_inf():
    s = pd.Series([0.01, 0.02, 0.03])
    out = compute_sharpe_variants(s, periods_per_year=252)
    assert math.isinf(out["omega_ratio"])


def test_empty_series_returns_all_nan():
    s = pd.Series([], dtype=float)
    out = compute_sharpe_variants(s, periods_per_year=252)
    for k, v in out.items():
        assert math.isnan(v), f"{k} = {v}, expected nan"


# === integration tests (extended fixtures) =============================


@pytest.fixture
def basket():
    return pd.read_csv(FIXTURES / "sample_basket.csv")


@pytest.fixture
def returns():
    return pd.read_csv(FIXTURES / "sample_picks_returns.csv")


def test_per_scanner_returns_all_columns(basket, returns):
    df = compute_sharpe_variants_per_scanner(basket, returns, horizon=21)
    expected_cols = [
        "scanner", "n_picks",
        "sharpe_zero", "sharpe_excess_spy", "sharpe_excess_tbill",
        "sortino_zero", "sortino_excess_tbill",
        "calmar", "omega_ratio", "mar_ratio",
    ]
    for col in expected_cols:
        assert col in df.columns, f"missing {col}"
    assert len(df) > 0


def test_per_scanner_sorted_by_excess_tbill_desc(basket, returns):
    df = compute_sharpe_variants_per_scanner(basket, returns, horizon=21)
    # Ignore NaN tail
    cleaned = df["sharpe_excess_tbill"].dropna().tolist()
    assert cleaned == sorted(cleaned, reverse=True)


def test_per_scanner_mar_equals_calmar(basket, returns):
    df = compute_sharpe_variants_per_scanner(basket, returns, horizon=21)
    for _, row in df.iterrows():
        if math.isnan(row["calmar"]) and math.isnan(row["mar_ratio"]):
            continue
        if math.isinf(row["calmar"]) and math.isinf(row["mar_ratio"]):
            continue
        assert row["calmar"] == row["mar_ratio"]


def test_pipeline_aggregation_returns_all_keys(basket, returns):
    out = compute_sharpe_variants_for_pipeline(
        basket, returns, top_n=10, horizon=5, tbill_rate=0.04,
    )
    expected_keys = {
        "sharpe_zero", "sharpe_excess_spy", "sharpe_excess_tbill",
        "sortino_zero", "sortino_excess_tbill",
        "calmar", "omega_ratio", "mar_ratio",
    }
    assert set(out.keys()) == expected_keys


def test_sharpe_zero_equals_excess_spy_in_pipeline(basket, returns):
    """When pipeline wrapper feeds excess_return, the two labels match."""
    out = compute_sharpe_variants_for_pipeline(
        basket, returns, top_n=10, horizon=5, tbill_rate=0.04,
    )
    if not math.isnan(out["sharpe_zero"]):
        assert out["sharpe_zero"] == out["sharpe_excess_spy"]


# === CLI ===============================================================


@pytest.fixture
def report_dir(tmp_path: Path) -> Path:
    shutil.copy(FIXTURES / "sample_basket.csv", tmp_path / "basket.csv")
    shutil.copy(FIXTURES / "sample_picks_returns.csv", tmp_path / "picks_returns.csv")
    shutil.copy(FIXTURES / "sample_pipeline_report.csv", tmp_path / "pipeline_edge_report.csv")
    return tmp_path


def test_cli_pipeline_subcommand(report_dir, capsys):
    main(["--report-dir", str(report_dir), "--horizon", "21", "--top-n", "10",
          "pipeline"])
    out = capsys.readouterr().out
    for key in ("sharpe_zero", "sharpe_excess_spy", "sharpe_excess_tbill",
                "sortino_zero", "sortino_excess_tbill",
                "calmar", "omega_ratio", "mar_ratio"):
        assert key in out


def test_cli_per_scanner_subcommand(report_dir, capsys):
    main(["--report-dir", str(report_dir), "--horizon", "21", "per-scanner"])
    out = capsys.readouterr().out
    assert "scanner" in out
    assert "sharpe_excess_tbill" in out


def test_cli_single_series_subcommand(tmp_path, capsys):
    csv_path = tmp_path / "ad_hoc.csv"
    pd.DataFrame({"my_returns": [0.01, -0.01, 0.02, 0.0, 0.03]}).to_csv(
        csv_path, index=False,
    )
    main([
        "single-series",
        "--csv-path", str(csv_path),
        "--column", "my_returns",
        "--periods-per-year", "252",
    ])
    out = capsys.readouterr().out
    for key in ("sharpe_zero", "sortino_zero", "calmar", "omega_ratio"):
        assert key in out
