"""Regime-conditional edge analysis for Phase 4e backtest results.

Classifies each surface_date as RISK_ON / RISK_OFF / UNCERTAIN based on
SPY 200dma with 2% buffer and 3-consecutive-days confirmation, then
computes per-scanner edge with bootstrap CIs separately within each
regime.

DELIBERATE DEVIATION from production regime detector (src/strategy.py
evaluate_regime): production uses TWO-way classification with state
inheritance (inherits prior regime when neither trigger fires).
This module uses THREE-way with explicit UNCERTAIN bucket so mid-band
basket dates are analyzed separately rather than back-attributed to a
prior regime. Production behavior is unchanged.

Parameters match production: 200-day MA, 2% buffer, 3 consecutive days.

CLI:
  python -m scanners.backtest.regime_analysis edge          # full per-(scanner,regime) table
  python -m scanners.backtest.regime_analysis pivot         # wide-format scanner x regime
  python -m scanners.backtest.regime_analysis distribution  # picks per regime
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Dict, List, Literal, Optional

import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_REPORT_DIR = Path("backtest_output/_pipeline_report_2026-05-06")
DEFAULT_SPY_PATH = Path("data_cache/SPY.parquet")
DEFAULT_HORIZON = 21
DEFAULT_BUFFER = 0.02
DEFAULT_DAYS_REQUIRED = 3
DEFAULT_MA_WINDOW = 200

Regime = Literal["RISK_ON", "RISK_OFF", "UNCERTAIN"]


def classify_regime(
    date_iso: str,
    spy_bars: pd.DataFrame,
    buffer: float = DEFAULT_BUFFER,
    days_required: int = DEFAULT_DAYS_REQUIRED,
    ma_window: int = DEFAULT_MA_WINDOW,
) -> Regime:
    """Three-way regime classification for a single date.

    spy_bars: DatetimeIndex DataFrame with a 'close' column. Index need
              not include date_iso exactly -- looks up the latest row
              with index <= date_iso.

    Logic:
      1. Truncate spy_bars to rows with index <= date_iso.
      2. If fewer than ma_window + days_required rows -> UNCERTAIN.
      3. Compute rolling mean of close over ma_window.
      4. Take last days_required rows. If close > ma * (1+buffer) for ALL
         -> RISK_ON. If close < ma * (1-buffer) for ALL -> RISK_OFF.
         Else -> UNCERTAIN.
    """
    as_of_ts = pd.Timestamp(date_iso)
    sub = spy_bars[spy_bars.index <= as_of_ts]
    if len(sub) < ma_window + days_required:
        return "UNCERTAIN"
    closes = sub["close"]
    ma = closes.rolling(ma_window, min_periods=ma_window).mean()
    last_close = closes.tail(days_required)
    last_ma = ma.tail(days_required)
    if last_ma.isna().any():
        return "UNCERTAIN"
    if (last_close > last_ma * (1 + buffer)).all():
        return "RISK_ON"
    if (last_close < last_ma * (1 - buffer)).all():
        return "RISK_OFF"
    return "UNCERTAIN"


def classify_basket_regimes(
    basket: pd.DataFrame,
    spy_bars: pd.DataFrame,
    buffer: float = DEFAULT_BUFFER,
    days_required: int = DEFAULT_DAYS_REQUIRED,
    ma_window: int = DEFAULT_MA_WINDOW,
) -> pd.DataFrame:
    """Add a 'regime' column to basket. Caches per-date so we don't
    re-compute regime once per pick (a date can have many basket rows
    when the same pick appears in multiple top-N buckets)."""
    out = basket.copy()
    cache: Dict[str, str] = {}

    def lookup(d: str) -> str:
        if d not in cache:
            cache[d] = classify_regime(d, spy_bars, buffer, days_required, ma_window)
        return cache[d]

    out["regime"] = out["surface_date"].map(lookup)
    return out


def compute_regime_conditional_edge(
    basket: pd.DataFrame,
    returns: pd.DataFrame,
    horizon: int,
    n_bootstrap: int = 1000,
    ci_level: float = 0.95,
    seed: int = 42,
    regime_col: str = "regime",
) -> pd.DataFrame:
    """Per-(scanner, regime) edge with bootstrap CIs.

    Requires basket to already have regime_col (call classify_basket_regimes
    first). For each regime, splits the basket and calls
    edge_confidence.compute_scanner_edge_with_ci.

    Returns long-format: scanner, regime, n_picks, mean_excess_pct,
    ci_lower_pct, ci_upper_pct, win_rate_pct.
    """
    if regime_col not in basket.columns:
        raise ValueError(
            f"basket missing '{regime_col}' column; call classify_basket_regimes first"
        )
    from .edge_confidence import compute_scanner_edge_with_ci

    rows = []
    for regime, sub_basket in basket.groupby(regime_col):
        if sub_basket.empty:
            continue
        edge = compute_scanner_edge_with_ci(
            sub_basket, returns, horizon, n_bootstrap, ci_level, seed,
        )
        for _, r in edge.iterrows():
            rows.append({
                "scanner": r["scanner"],
                "regime": regime,
                "n_picks": int(r["n_picks"]),
                "mean_excess_pct": float(r["mean_excess_pct"]),
                "ci_lower_pct": float(r["ci_lower_pct"]),
                "ci_upper_pct": float(r["ci_upper_pct"]),
                "win_rate_pct": float(r["win_rate_pct"]),
            })
    if not rows:
        return pd.DataFrame(columns=[
            "scanner", "regime", "n_picks", "mean_excess_pct",
            "ci_lower_pct", "ci_upper_pct", "win_rate_pct",
        ])
    return pd.DataFrame(rows).sort_values(
        ["scanner", "regime"]
    ).reset_index(drop=True)


def regime_pivot_table(regime_edge: pd.DataFrame) -> pd.DataFrame:
    """Wide-format scanner x regime view.
    Columns per regime: {regime}_mean, {regime}_ci, {regime}_n.
    Cells for regimes a scanner never appeared in are absent (sparse)."""
    rows = []
    for scanner, group in regime_edge.groupby("scanner"):
        row: Dict[str, object] = {"scanner": scanner}
        for _, r in group.iterrows():
            reg = r["regime"]
            row[f"{reg}_mean"] = round(float(r["mean_excess_pct"]), 3)
            row[f"{reg}_ci"] = (
                f"[{float(r['ci_lower_pct']):+.2f}, {float(r['ci_upper_pct']):+.2f}]"
            )
            row[f"{reg}_n"] = int(r["n_picks"])
        rows.append(row)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("scanner").reset_index(drop=True)


def regime_distribution(basket: pd.DataFrame, regime_col: str = "regime") -> pd.DataFrame:
    """Count picks (after dedup on (ticker, surface_date)) per regime,
    plus number of distinct weeks in each regime."""
    if regime_col not in basket.columns:
        raise ValueError(f"basket missing '{regime_col}' column")
    deduped = basket.drop_duplicates(["ticker", "surface_date"])
    counts = deduped.groupby(regime_col).size().reset_index(name="n_picks")
    total = counts["n_picks"].sum()
    counts["pct"] = (counts["n_picks"] / total * 100).round(2) if total else 0.0
    weeks_per_regime = (
        deduped.drop_duplicates("surface_date")
        .groupby(regime_col).size().reset_index(name="n_weeks")
    )
    return counts.merge(weeks_per_regime, on=regime_col, how="left")


# === SPY loader ===

def _load_spy_bars(spy_path: Path = DEFAULT_SPY_PATH) -> pd.DataFrame:
    if not spy_path.exists():
        raise FileNotFoundError(
            f"SPY bars not found at {spy_path}. Phase 4e backtest writes "
            f"SPY data to data_cache/. Pass --spy-path to override or "
            f"populate the cache via a scanner run."
        )
    df = pd.read_parquet(spy_path)
    if not isinstance(df.index, pd.DatetimeIndex):
        if "timestamp" in df.columns:
            df = df.set_index("timestamp")
        elif "date" in df.columns:
            df = df.set_index("date")
        df.index = pd.to_datetime(df.index)
    if "close" not in df.columns:
        raise ValueError(f"SPY bars at {spy_path} missing 'close' column")
    return df.sort_index()


# === CLI ===

def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Phase 4e regime-conditional edge analysis",
    )
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--spy-path", type=Path, default=DEFAULT_SPY_PATH)
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON)
    parser.add_argument("--buffer", type=float, default=DEFAULT_BUFFER)
    parser.add_argument("--days-required", type=int, default=DEFAULT_DAYS_REQUIRED)
    parser.add_argument("--ma-window", type=int, default=DEFAULT_MA_WINDOW)
    parser.add_argument("--n-bootstrap", type=int, default=1000)
    parser.add_argument("--ci-level", type=float, default=0.95)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--log-level", default="WARNING")

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("edge")
    sub.add_parser("pivot")
    sub.add_parser("distribution")

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Lazy import: avoid eager analyze_pipeline coupling at module load
    from .analyze_pipeline import _load_basket, _load_or_compute_returns

    spy_bars = _load_spy_bars(args.spy_path)
    basket = _load_basket(args.report_dir)
    returns = _load_or_compute_returns(args.report_dir, basket, [args.horizon])

    basket_with_regime = classify_basket_regimes(
        basket, spy_bars, args.buffer, args.days_required, args.ma_window,
    )

    if args.cmd == "edge":
        df = compute_regime_conditional_edge(
            basket_with_regime, returns, args.horizon,
            args.n_bootstrap, args.ci_level, args.seed,
        )
        print(df.to_string(index=False))
    elif args.cmd == "pivot":
        edge = compute_regime_conditional_edge(
            basket_with_regime, returns, args.horizon,
            args.n_bootstrap, args.ci_level, args.seed,
        )
        pivot = regime_pivot_table(edge)
        print(pivot.to_string(index=False))
    else:  # distribution
        df = regime_distribution(basket_with_regime)
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
