"""Multiple Sharpe-style risk-adjusted return variants.

Computes 8 ratios from a single returns series so analysis isn't tied to
one assumption set. Different ratios reveal different risk profiles:
  - sharpe_*    : reward per unit of total volatility
  - sortino_*   : reward per unit of DOWNSIDE volatility (rewards upside vol)
  - calmar/mar  : reward per unit of worst-case drawdown
  - omega_ratio : non-parametric upside vs downside mass

NOTE on sharpe_zero vs sharpe_excess_spy: these hold the same numeric
value (the basic mean/std annualized formula). Labels disambiguate based
on what the caller passed in:
  - if input is total returns: sharpe_zero is meaningful
  - if input is excess-over-SPY: sharpe_excess_spy is meaningful
  - if input is excess-over-tbill: that's sharpe_excess_tbill (different
    key, computed separately by subtracting tbill from input)

Wrapper functions in this module pass excess_return so both labels apply
as 'Sharpe vs SPY'.

CLI:
  python -m scanners.backtest.sharpe_variants pipeline       # weekly top-N basket
  python -m scanners.backtest.sharpe_variants per-scanner    # scanner-level table
  python -m scanners.backtest.sharpe_variants single-series  # ad-hoc CSV column
"""
from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_REPORT_DIR = Path("backtest_output/_pipeline_report_2026-05-06")
DEFAULT_HORIZON = 21
DEFAULT_TOP_N = 10
DEFAULT_TBILL_RATE = 0.04
TRADING_DAYS_PER_YEAR = 252


# === parsing helper (mirrors analyze_pipeline._split_scanners_hit) ===

def _split_scanners_hit(s) -> List[str]:
    if s is None:
        return []
    if isinstance(s, float) and pd.isna(s):
        return []
    s = str(s).strip()
    if not s:
        return []
    for sep in (",", "|", ";"):
        if sep in s:
            return [t.strip() for t in s.split(sep) if t.strip()]
    return [s]


# === private numerical helpers ===

def _annualized_sharpe(
    returns_per_period: pd.Series,
    periods_per_year: int,
    rf_per_period: float = 0.0,
) -> float:
    s = returns_per_period.dropna()
    if len(s) < 2:
        return float("nan")
    std = float(s.std(ddof=1))
    if std == 0:
        return float("nan")
    mean = float(s.mean()) - rf_per_period
    return (mean / std) * math.sqrt(periods_per_year)


def _downside_deviation(returns: pd.Series, threshold: float = 0.0) -> float:
    s = returns.dropna()
    if len(s) == 0:
        return float("nan")
    deviations = (s - threshold).clip(upper=0.0)
    return float(np.sqrt((deviations ** 2).mean()))


def _annualized_sortino(
    returns_per_period: pd.Series,
    periods_per_year: int,
    rf_per_period: float = 0.0,
    threshold: float = 0.0,
) -> float:
    s = returns_per_period.dropna()
    if len(s) == 0:
        return float("nan")
    dd = _downside_deviation(s, threshold)
    if math.isnan(dd):
        return float("nan")
    mean = float(s.mean()) - rf_per_period
    if dd == 0:
        # No downside; reward is infinite if mean > 0, nan if mean == 0,
        # negative-inf if mean < 0
        if mean > 0:
            return float("inf")
        if mean < 0:
            return float("-inf")
        return float("nan")
    return (mean / dd) * math.sqrt(periods_per_year)


def _max_drawdown(returns: pd.Series) -> float:
    """Returns positive magnitude of worst peak-to-trough loss; 0.0 if
    none. nan if input empty."""
    s = returns.dropna()
    if len(s) == 0:
        return float("nan")
    eq = (1 + s).cumprod()
    peak = eq.cummax()
    dd = (eq - peak) / peak
    worst = float(dd.min())
    return abs(worst) if worst < 0 else 0.0


def _calmar(returns: pd.Series, periods_per_year: int) -> float:
    s = returns.dropna()
    if len(s) == 0:
        return float("nan")
    mdd = _max_drawdown(s)
    if math.isnan(mdd):
        return float("nan")
    annual_return = float(s.mean()) * periods_per_year
    if mdd == 0:
        if annual_return > 0:
            return float("inf")
        if annual_return < 0:
            return float("-inf")
        return float("nan")
    return annual_return / mdd


def _omega_ratio(returns: pd.Series, threshold: float = 0.0) -> float:
    s = returns.dropna()
    if len(s) == 0:
        return float("nan")
    upside = s[s > threshold]
    downside = s[s < threshold]
    if len(upside) == 0 and len(downside) == 0:
        return float("nan")
    if len(downside) == 0:
        return float("inf")
    if len(upside) == 0:
        return 0.0
    return float(upside.mean()) / abs(float(downside.mean()))


# === public API ===

def compute_sharpe_variants(
    returns: pd.Series,
    periods_per_year: int = TRADING_DAYS_PER_YEAR,
    tbill_rate: float = DEFAULT_TBILL_RATE,
) -> Dict[str, float]:
    """Compute 8 risk-adjusted return ratios from a single returns series.

    See module docstring for sharpe_zero vs sharpe_excess_spy semantics.
    """
    s = returns.dropna()
    if len(s) == 0:
        nan = float("nan")
        return {
            "sharpe_zero": nan, "sharpe_excess_spy": nan,
            "sharpe_excess_tbill": nan, "sortino_zero": nan,
            "sortino_excess_tbill": nan, "calmar": nan,
            "omega_ratio": nan, "mar_ratio": nan,
        }

    rf_per_period = tbill_rate / periods_per_year if periods_per_year else 0.0

    sharpe_zero = _annualized_sharpe(s, periods_per_year, rf_per_period=0.0)
    sharpe_excess_tbill = _annualized_sharpe(s, periods_per_year, rf_per_period=rf_per_period)
    sortino_zero = _annualized_sortino(s, periods_per_year, rf_per_period=0.0)
    sortino_excess_tbill = _annualized_sortino(s, periods_per_year, rf_per_period=rf_per_period)
    calmar = _calmar(s, periods_per_year)
    omega = _omega_ratio(s, threshold=0.0)

    return {
        "sharpe_zero":          round(sharpe_zero, 6) if not math.isnan(sharpe_zero) else float("nan"),
        "sharpe_excess_spy":    round(sharpe_zero, 6) if not math.isnan(sharpe_zero) else float("nan"),
        "sharpe_excess_tbill":  round(sharpe_excess_tbill, 6) if not math.isnan(sharpe_excess_tbill) else float("nan"),
        "sortino_zero":         sortino_zero if math.isinf(sortino_zero) or math.isnan(sortino_zero) else round(sortino_zero, 6),
        "sortino_excess_tbill": sortino_excess_tbill if math.isinf(sortino_excess_tbill) or math.isnan(sortino_excess_tbill) else round(sortino_excess_tbill, 6),
        "calmar":               calmar if math.isinf(calmar) or math.isnan(calmar) else round(calmar, 6),
        "omega_ratio":          omega if math.isinf(omega) or math.isnan(omega) else round(omega, 6),
        "mar_ratio":            calmar if math.isinf(calmar) or math.isnan(calmar) else round(calmar, 6),
    }


def compute_sharpe_variants_for_pipeline(
    basket: pd.DataFrame,
    returns: pd.DataFrame,
    top_n: int = DEFAULT_TOP_N,
    horizon: int = 5,
    tbill_rate: float = DEFAULT_TBILL_RATE,
) -> Dict[str, float]:
    """Aggregate basket picks to weekly basket excess returns then compute
    all 8 variants on the resulting weekly series."""
    basket_n = basket[basket["top_n_bucket"] == top_n].copy()
    rh = returns[returns["horizon_days"] == horizon][
        ["ticker", "surface_date", "excess_return"]
    ]
    joined = basket_n.merge(rh, on=["ticker", "surface_date"], how="left")
    weekly = (
        joined.groupby("surface_date")["excess_return"]
        .apply(lambda s: float(s.dropna().mean()) if s.dropna().any() else float("nan"))
        .dropna()
        .sort_index()
    )
    ppy = max(1, int(round(TRADING_DAYS_PER_YEAR / horizon)))
    return compute_sharpe_variants(weekly, periods_per_year=ppy, tbill_rate=tbill_rate)


def compute_sharpe_variants_per_scanner(
    basket: pd.DataFrame,
    returns: pd.DataFrame,
    horizon: int = DEFAULT_HORIZON,
    tbill_rate: float = DEFAULT_TBILL_RATE,
) -> pd.DataFrame:
    """Per-scanner: dedupe, explode, merge, compute 8 variants per scanner.
    Sorted by sharpe_excess_tbill descending."""
    unique_picks = basket.drop_duplicates(["ticker", "surface_date"]).copy()
    rh = returns[returns["horizon_days"] == horizon][
        ["ticker", "surface_date", "excess_return"]
    ]
    joined = unique_picks.merge(rh, on=["ticker", "surface_date"], how="left")
    joined["scanners_list"] = joined["scanners_hit"].apply(_split_scanners_hit)
    exploded = joined.explode("scanners_list").rename(columns={"scanners_list": "scanner"})
    exploded = exploded[exploded["scanner"].notna() & (exploded["scanner"] != "")]
    exploded = exploded.dropna(subset=["excess_return"])

    ppy = max(1, int(round(TRADING_DAYS_PER_YEAR / horizon)))
    rows = []
    for scanner, group in exploded.groupby("scanner"):
        series = group["excess_return"].astype(float)
        variants = compute_sharpe_variants(series, periods_per_year=ppy, tbill_rate=tbill_rate)
        rows.append({
            "scanner": scanner,
            "n_picks": int(len(series)),
            **variants,
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df.sort_values(
        "sharpe_excess_tbill", ascending=False, na_position="last",
    ).reset_index(drop=True)


# === CLI ===

def _format_value(v: float) -> str:
    if math.isnan(v):
        return "    nan"
    if math.isinf(v):
        return "    inf" if v > 0 else "   -inf"
    return f"{v:+8.4f}"


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Phase 4e Sharpe variants")
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON)
    parser.add_argument("--tbill-rate", type=float, default=DEFAULT_TBILL_RATE)
    parser.add_argument(
        "--periods-per-year", type=int, default=None,
        help="Override; auto-derived from horizon if unset",
    )
    parser.add_argument("--log-level", default="WARNING")

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("pipeline")
    sub.add_parser("per-scanner")
    p_single = sub.add_parser("single-series")
    p_single.add_argument("--csv-path", type=Path, required=True)
    p_single.add_argument("--column", type=str, required=True)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.cmd == "single-series":
        df = pd.read_csv(args.csv_path)
        if args.column not in df.columns:
            raise ValueError(f"column {args.column!r} not in {args.csv_path}")
        series = df[args.column].dropna().astype(float)
        ppy = args.periods_per_year or TRADING_DAYS_PER_YEAR
        out = compute_sharpe_variants(series, ppy, args.tbill_rate)
        print(f"single-series: {args.csv_path}::{args.column}  (n={len(series)}, ppy={ppy})")
        for k, v in out.items():
            print(f"  {k:25}  {_format_value(v)}")
        return

    from .analyze_pipeline import _load_basket, _load_or_compute_returns
    basket = _load_basket(args.report_dir)
    returns = _load_or_compute_returns(args.report_dir, basket, [args.horizon])

    if args.cmd == "pipeline":
        out = compute_sharpe_variants_for_pipeline(
            basket, returns, args.top_n, args.horizon, args.tbill_rate,
        )
        print(
            f"Pipeline (top_{args.top_n}, horizon={args.horizon}d, "
            f"tbill={args.tbill_rate:.2%})"
        )
        for k, v in out.items():
            print(f"  {k:25}  {_format_value(v)}")
    else:  # per-scanner
        df = compute_sharpe_variants_per_scanner(
            basket, returns, args.horizon, args.tbill_rate,
        )
        print(
            f"Per-scanner (horizon={args.horizon}d, tbill={args.tbill_rate:.2%})"
        )
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
