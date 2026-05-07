"""Tests for scanners/backtest/analyze_pipeline.py CLI subcommands.

Tests use deterministic fixtures in tests/fixtures/, never touch data_cache,
never write outside tmp_path. Safe to run after the Phase 4e backtest finishes.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd
import pytest

from scanners.backtest import analyze_pipeline as ap

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def report_dir(tmp_path: Path) -> Path:
    """Copy fixture CSVs into a tmp report directory."""
    shutil.copy(FIXTURES / "sample_pipeline_report.csv", tmp_path / "pipeline_edge_report.csv")
    shutil.copy(FIXTURES / "sample_basket.csv", tmp_path / "basket.csv")
    shutil.copy(FIXTURES / "sample_picks_returns.csv", tmp_path / "picks_returns.csv")
    return tmp_path


def _ns(**kw) -> argparse.Namespace:
    return argparse.Namespace(**kw)


# --- summary -------------------------------------------------------------


def test_summary_loads_and_pivots_all_buckets(report_dir, capsys):
    text = ap.cmd_summary(_ns(), report_dir)
    assert "top_5" in text
    assert "top_10" in text
    assert "top_20" in text
    # 9 distinct (bucket, horizon) combinations -> 9 occurrences of the
    # bucket prefix "top_"
    assert text.count("top_") == 9
    captured = capsys.readouterr()
    assert "top_5" in captured.out
    # Hit rates formatted as percentages
    assert "%" in captured.out


# --- scanners ------------------------------------------------------------


def test_scanners_groups_and_ranks(report_dir):
    df = ap.cmd_scanners(_ns(top_n=10, horizon=21), report_dir)
    assert isinstance(df, pd.DataFrame)
    for col in ("scanner", "n_picks", "mean_excess_pct", "median_excess_pct", "win_rate"):
        assert col in df.columns
    assert len(df) > 0
    # Sorted descending by mean_excess_pct (NaN tolerant: dropna and check)
    cleaned = df["mean_excess_pct"].dropna().tolist()
    assert cleaned == sorted(cleaned, reverse=True)
    # Every scanner from the fixture appears
    expected = {"insider_buying", "breakout_52w", "thirteen_f_changes",
                "earnings_drift", "short_squeeze"}
    assert set(df["scanner"]) >= expected


# --- histogram -----------------------------------------------------------


def test_histogram_bins_match_requested_count(report_dir):
    bins = ap.cmd_histogram(
        _ns(top_n=10, horizon=21, bins=10, bin_low=-0.30, bin_high=0.30),
        report_dir,
    )
    assert len(bins) == 10
    assert sum(bins.values()) > 0


def test_histogram_total_equals_dropna_count(report_dir):
    """Sum of bin counts must equal the number of non-null returns."""
    basket = ap._load_basket(report_dir)
    basket_n = basket[basket["top_n_bucket"] == 10]
    returns = ap._load_or_compute_returns(report_dir, basket_n, [21])
    rs = returns[returns["horizon_days"] == 21]["forward_return"].dropna()
    bins = ap.cmd_histogram(
        _ns(top_n=10, horizon=21, bins=12, bin_low=-0.30, bin_high=0.30),
        report_dir,
    )
    assert sum(bins.values()) == len(rs)


# --- drawdown ------------------------------------------------------------


def test_drawdown_computes_max_dd(report_dir):
    summary = ap.cmd_drawdown(_ns(top_n=10, horizon=5), report_dir)
    assert set(summary) >= {"total_return", "max_drawdown", "max_dd_date", "n_weeks"}
    assert summary["max_drawdown"] <= 0.0
    assert summary["n_weeks"] >= 1
    assert isinstance(summary["total_return"], float)


# --- compare-spy ---------------------------------------------------------


def test_compare_spy_emits_alpha_beta_ir(report_dir):
    summary = ap.cmd_compare_spy(_ns(top_n=10, horizon=5), report_dir)
    expected = {"basket_total_return", "spy_total_return", "alpha_per_period",
                "alpha_annualized", "beta", "information_ratio", "n_weeks"}
    assert set(summary) >= expected
    assert isinstance(summary["basket_total_return"], float)
    assert isinstance(summary["spy_total_return"], float)


# --- report --------------------------------------------------------------


def test_report_writes_markdown_with_all_sections(report_dir):
    out_path = ap.cmd_report(_ns(), report_dir)
    assert out_path.exists()
    assert out_path.name == "ANALYSIS.md"
    content = out_path.read_text(encoding="utf-8")
    for header in (
        "# Pipeline analysis",
        "## Summary table",
        "## Per-scanner contribution",
        "## Distribution of forward returns",
        "## Drawdown",
        "## SPY comparison",
        "## Notes",
    ):
        assert header in content


# --- CLI dispatch --------------------------------------------------------


def test_cli_dispatch_summary(report_dir, capsys):
    ap.main(["--report-dir", str(report_dir), "summary"])
    captured = capsys.readouterr()
    assert "top_" in captured.out


def test_cli_dispatch_scanners(report_dir, capsys):
    ap.main(["--report-dir", str(report_dir), "scanners", "--top-n", "10", "--horizon", "21"])
    captured = capsys.readouterr()
    assert "Per-scanner contribution" in captured.out


# --- helpers -------------------------------------------------------------


def test_load_or_compute_returns_uses_cache_without_compute(report_dir, monkeypatch):
    """When picks_returns.csv exists, the helper must NOT call
    forward_returns.compute_returns_for_candidates (which would touch
    data_cache and possibly hit Alpaca)."""
    # Sentinel: if compute is invoked, raise
    def _explode(*a, **kw):
        raise RuntimeError("compute_returns_for_candidates was called")
    import scanners.backtest.forward_returns as fr
    monkeypatch.setattr(fr, "compute_returns_for_candidates", _explode)

    basket = ap._load_basket(report_dir)
    df = ap._load_or_compute_returns(report_dir, basket, [5, 21, 63])
    assert "forward_return" in df.columns
    assert "excess_return" in df.columns
    assert len(df) > 0


def test_split_scanners_hit_handles_separators():
    assert ap._split_scanners_hit("a,b,c") == ["a", "b", "c"]
    assert ap._split_scanners_hit("a|b|c") == ["a", "b", "c"]
    assert ap._split_scanners_hit("a;b;c") == ["a", "b", "c"]
    assert ap._split_scanners_hit("solo") == ["solo"]
    assert ap._split_scanners_hit("") == []
    assert ap._split_scanners_hit(None) == []
    assert ap._split_scanners_hit(float("nan")) == []
    assert ap._split_scanners_hit(" a , b ") == ["a", "b"]
