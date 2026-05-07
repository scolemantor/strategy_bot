"""Tests for scanners/backtest/regime_analysis.py.

Mix of unit tests (synthetic short DataFrames inline) and integration
tests (extended fixtures + sample_spy_bars). All tests use tmp_path or
in-memory data; no real data_cache or backtest_output reads.
"""
from __future__ import annotations

import shutil
from pathlib import Path
from typing import List

import pandas as pd
import pytest

from scanners.backtest.regime_analysis import (
    classify_basket_regimes,
    classify_regime,
    compute_regime_conditional_edge,
    main,
    regime_distribution,
    regime_pivot_table,
    _load_spy_bars,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _make_spy_series(prices: List[float], end_date: str = "2025-01-06") -> pd.DataFrame:
    """Build a SPY DataFrame with len(prices) trading days ending end_date."""
    dates = pd.bdate_range(end=end_date, periods=len(prices))
    return pd.DataFrame({"close": prices}, index=dates)


# === unit tests for classify_regime ===

def test_classify_regime_risk_on_above_200dma_with_buffer():
    # 200 days at 100 + 5 days at 110. 110 > 100 * 1.02 = 102 for all 5 days
    # (need only last 3 for trigger).
    spy = _make_spy_series([100.0] * 200 + [110.0] * 5)
    assert classify_regime("2025-01-06", spy) == "RISK_ON"


def test_classify_regime_risk_off_below_200dma_with_buffer():
    spy = _make_spy_series([100.0] * 200 + [90.0] * 5)
    assert classify_regime("2025-01-06", spy) == "RISK_OFF"


def test_classify_regime_uncertain_mid_band():
    # 101 within +/-2% of 100 (200dma)
    spy = _make_spy_series([100.0] * 200 + [101.0] * 5)
    assert classify_regime("2025-01-06", spy) == "UNCERTAIN"


def test_classify_regime_requires_consecutive_days():
    # 200dma at 100, last 3 days: 99 (in band), 99 (in band), 110 (above buffer).
    # Mixed -> UNCERTAIN since not ALL 3 above buffer.
    spy = _make_spy_series([100.0] * 200 + [99.0, 99.0, 110.0])
    assert classify_regime("2025-01-06", spy) == "UNCERTAIN"


def test_classify_regime_insufficient_history():
    spy = _make_spy_series([100.0] * 50)
    assert classify_regime("2025-01-06", spy) == "UNCERTAIN"


# === integration: classify_basket_regimes ===

@pytest.fixture
def spy_bars():
    return pd.read_csv(
        FIXTURES / "sample_spy_bars.csv",
        parse_dates=["timestamp"],
    ).set_index("timestamp")


@pytest.fixture
def basket():
    return pd.read_csv(FIXTURES / "sample_basket.csv")


@pytest.fixture
def returns():
    return pd.read_csv(FIXTURES / "sample_picks_returns.csv")


def test_classify_basket_regimes_assigns_one_per_date(basket, spy_bars):
    out = classify_basket_regimes(basket, spy_bars)
    # Every (surface_date) maps to exactly one regime
    per_date_regimes = out.groupby("surface_date")["regime"].nunique()
    assert (per_date_regimes == 1).all()


def test_classify_basket_regimes_produces_all_three_regimes(basket, spy_bars):
    out = classify_basket_regimes(basket, spy_bars)
    regime_set = set(out["regime"].unique())
    assert {"RISK_ON", "RISK_OFF", "UNCERTAIN"}.issubset(regime_set), (
        f"expected all 3 regimes; got {regime_set}"
    )


# === integration: compute_regime_conditional_edge ===

def test_compute_regime_conditional_edge_returns_per_regime_rows(basket, spy_bars, returns):
    basket_w_regime = classify_basket_regimes(basket, spy_bars)
    df = compute_regime_conditional_edge(
        basket_w_regime, returns, horizon=21, n_bootstrap=100, seed=42,
    )
    for col in ("scanner", "regime", "n_picks", "mean_excess_pct",
                "ci_lower_pct", "ci_upper_pct", "win_rate_pct"):
        assert col in df.columns
    assert len(df) > 0
    # Each (scanner, regime) combination appears at most once
    duplicates = df.groupby(["scanner", "regime"]).size()
    assert (duplicates == 1).all()


def test_compute_regime_conditional_edge_ci_contains_mean(basket, spy_bars, returns):
    basket_w_regime = classify_basket_regimes(basket, spy_bars)
    df = compute_regime_conditional_edge(
        basket_w_regime, returns, horizon=21, n_bootstrap=200, seed=42,
    )
    for _, row in df.iterrows():
        assert row["ci_lower_pct"] <= row["mean_excess_pct"] <= row["ci_upper_pct"], (
            f"({row['scanner']}, {row['regime']}): mean={row['mean_excess_pct']} "
            f"not in [{row['ci_lower_pct']}, {row['ci_upper_pct']}]"
        )


def test_compute_regime_conditional_edge_raises_without_regime_column(basket, returns):
    with pytest.raises(ValueError) as excinfo:
        compute_regime_conditional_edge(basket, returns, horizon=21)
    assert "regime" in str(excinfo.value).lower()


# === regime_pivot_table ===

def test_regime_pivot_table_columns(basket, spy_bars, returns):
    basket_w_regime = classify_basket_regimes(basket, spy_bars)
    edge = compute_regime_conditional_edge(
        basket_w_regime, returns, horizon=21, n_bootstrap=100, seed=42,
    )
    pivot = regime_pivot_table(edge)
    assert "scanner" in pivot.columns
    # At least one *_mean column for each regime that appeared
    regimes_in_edge = edge["regime"].unique()
    for reg in regimes_in_edge:
        assert f"{reg}_mean" in pivot.columns
        assert f"{reg}_ci" in pivot.columns
        assert f"{reg}_n" in pivot.columns


# === regime_distribution ===

def test_regime_distribution_sums_to_unique_picks(basket, spy_bars):
    basket_w_regime = classify_basket_regimes(basket, spy_bars)
    dist = regime_distribution(basket_w_regime)
    deduped = basket_w_regime.drop_duplicates(["ticker", "surface_date"])
    assert dist["n_picks"].sum() == len(deduped)
    # Percentages should sum to ~100
    assert abs(dist["pct"].sum() - 100.0) < 0.5


# === SPY loader ===

def test_missing_spy_data_clear_error(tmp_path):
    bogus = tmp_path / "nonexistent_SPY.parquet"
    with pytest.raises(FileNotFoundError) as excinfo:
        _load_spy_bars(bogus)
    msg = str(excinfo.value)
    assert "data_cache" in msg.lower() or str(bogus) in msg


# === CLI ===

def test_cli_edge_subcommand(tmp_path, basket, spy_bars, returns, capsys):
    # Stage report dir
    shutil.copy(FIXTURES / "sample_basket.csv", tmp_path / "basket.csv")
    shutil.copy(FIXTURES / "sample_picks_returns.csv", tmp_path / "picks_returns.csv")
    shutil.copy(FIXTURES / "sample_pipeline_report.csv", tmp_path / "pipeline_edge_report.csv")
    # Stage SPY parquet (CLI loader expects parquet)
    spy_pq = tmp_path / "SPY.parquet"
    spy_bars.to_parquet(spy_pq)

    main([
        "--report-dir", str(tmp_path),
        "--spy-path", str(spy_pq),
        "--horizon", "21",
        "--n-bootstrap", "100",
        "--seed", "42",
        "edge",
    ])
    out = capsys.readouterr().out
    assert "regime" in out
    assert "scanner" in out


def test_cli_distribution_subcommand(tmp_path, basket, spy_bars, returns, capsys):
    shutil.copy(FIXTURES / "sample_basket.csv", tmp_path / "basket.csv")
    shutil.copy(FIXTURES / "sample_picks_returns.csv", tmp_path / "picks_returns.csv")
    shutil.copy(FIXTURES / "sample_pipeline_report.csv", tmp_path / "pipeline_edge_report.csv")
    spy_pq = tmp_path / "SPY.parquet"
    spy_bars.to_parquet(spy_pq)

    main([
        "--report-dir", str(tmp_path),
        "--spy-path", str(spy_pq),
        "--horizon", "21",
        "distribution",
    ])
    out = capsys.readouterr().out
    assert "n_picks" in out
    assert "n_weeks" in out
