"""Tests for scanners/backtest/edge_confidence.py.

Uses extended fixtures (sample_basket.csv 30 weeks, sample_picks_returns.csv
1801 rows). All tests work against synthetic data; no scanner state. The
two assertions about bootstrap behavior (high-n tighter; block wider than
simple) are mathematically not strict guarantees — see the plan's 'Known
statistical fragility' section. They use aggregate-mean-width comparisons
with a 5% tolerance.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from scanners.backtest.edge_confidence import (
    compute_scanner_edge_block_bootstrap,
    compute_scanner_edge_with_ci,
    compute_scanner_edge_both,
    main,
)

FIXTURES = Path(__file__).parent / "fixtures"

EXPECTED_COLUMNS = [
    "scanner",
    "n_picks",
    "mean_excess_pct",
    "ci_lower_pct",
    "ci_upper_pct",
    "median_excess_pct",
    "win_rate_pct",
    "std_excess_pct",
    "sharpe_estimate",
]


@pytest.fixture
def report_dir(tmp_path: Path) -> Path:
    shutil.copy(FIXTURES / "sample_basket.csv", tmp_path / "basket.csv")
    shutil.copy(FIXTURES / "sample_picks_returns.csv", tmp_path / "picks_returns.csv")
    shutil.copy(FIXTURES / "sample_pipeline_report.csv", tmp_path / "pipeline_edge_report.csv")
    return tmp_path


@pytest.fixture
def basket(report_dir):
    return pd.read_csv(report_dir / "basket.csv")


@pytest.fixture
def returns(report_dir):
    return pd.read_csv(report_dir / "picks_returns.csv")


# === simple bootstrap ===

def test_simple_bootstrap_returns_expected_columns(basket, returns):
    df = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=200, seed=42)
    assert list(df.columns) == EXPECTED_COLUMNS
    assert len(df) > 0


def test_simple_bootstrap_ci_contains_mean(basket, returns):
    df = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=500, seed=42)
    for _, row in df.iterrows():
        assert row["ci_lower_pct"] <= row["mean_excess_pct"] <= row["ci_upper_pct"], (
            f"{row['scanner']}: mean={row['mean_excess_pct']} not in "
            f"[{row['ci_lower_pct']}, {row['ci_upper_pct']}]"
        )


def test_simple_bootstrap_high_n_returns_tighter_ci(basket, returns):
    """Aggregate mean CI width across scanners should not get materially wider
    as n_bootstrap grows. Allow 5% slack — bootstrap percentile estimator
    converges in distribution but individual run widths are stochastic."""
    df_low = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=100, seed=42)
    df_high = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=10000, seed=42)
    width_low = float((df_low["ci_upper_pct"] - df_low["ci_lower_pct"]).mean())
    width_high = float((df_high["ci_upper_pct"] - df_high["ci_lower_pct"]).mean())
    assert width_high <= width_low * 1.05, (
        f"high-n CI width {width_high:.4f} unexpectedly much wider than "
        f"low-n {width_low:.4f}"
    )


# === block bootstrap ===

def test_block_bootstrap_returns_same_columns_as_simple(basket, returns):
    df_simple = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=200, seed=42)
    df_block = compute_scanner_edge_block_bootstrap(
        basket, returns, horizon=21, n_bootstrap=200, seed=42, block_size_weeks=4,
    )
    assert list(df_block.columns) == list(df_simple.columns)


def test_block_bootstrap_ci_wider_than_simple(basket, returns):
    """Aggregate mean CI width: block >= simple x 0.95 (block usually wider
    when picks have positive autocorrelation, but allow 5% slack for
    stochastic edge cases)."""
    df_simple = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=500, seed=42)
    df_block = compute_scanner_edge_block_bootstrap(
        basket, returns, horizon=21, n_bootstrap=500, seed=42, block_size_weeks=4,
    )
    width_simple = float((df_simple["ci_upper_pct"] - df_simple["ci_lower_pct"]).mean())
    width_block = float((df_block["ci_upper_pct"] - df_block["ci_lower_pct"]).mean())
    assert width_block >= width_simple * 0.95, (
        f"block CI width {width_block:.4f} unexpectedly much narrower than "
        f"simple {width_simple:.4f}"
    )


# === reproducibility ===

def test_seed_reproducibility(basket, returns):
    df1 = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=200, seed=42)
    df2 = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=200, seed=42)
    pd.testing.assert_frame_equal(df1, df2)


def test_seed_changes_results(basket, returns):
    df1 = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=200, seed=42)
    df2 = compute_scanner_edge_with_ci(basket, returns, horizon=21, n_bootstrap=200, seed=43)
    # CI bounds must differ for at least one scanner
    diffs = (df1["ci_lower_pct"] - df2["ci_lower_pct"]).abs() + \
            (df1["ci_upper_pct"] - df2["ci_upper_pct"]).abs()
    assert diffs.sum() > 0, "different seeds produced identical CIs (RNG was not exercised)"


# === CLI ===

def test_cli_simple_subcommand(report_dir, capsys):
    main(["--report-dir", str(report_dir), "--horizon", "21",
          "--n-bootstrap", "100", "--seed", "42", "simple"])
    captured = capsys.readouterr()
    assert "scanner" in captured.out
    assert "ci_lower_pct" in captured.out


def test_cli_both_subcommand(report_dir, capsys):
    main(["--report-dir", str(report_dir), "--horizon", "21",
          "--n-bootstrap", "100", "--seed", "42", "both"])
    captured = capsys.readouterr()
    # Side-by-side output should contain both label families and the inflation column
    assert "simple_ci" in captured.out
    assert "block_ci" in captured.out
    assert "ci_inflation_factor" in captured.out


# === extra: compute_scanner_edge_both shape ===

def test_compute_both_shape(basket, returns):
    df = compute_scanner_edge_both(basket, returns, horizon=21, n_bootstrap=100, seed=42)
    for col in ("scanner", "n_picks", "mean_excess_pct",
                "simple_ci_lo", "simple_ci_hi", "simple_width",
                "block_ci_lo", "block_ci_hi", "block_width",
                "ci_inflation_factor"):
        assert col in df.columns
    assert (df["simple_width"] >= 0).all()
    assert (df["block_width"] >= 0).all()
