"""Drift detection for Phase 4e scanner edge measurements.

Detects when a scanner's recent edge has decayed (or improved) materially
relative to its trailing baseline. Edge decay matters: a signal that worked
in 2024 may not work in 2025 because markets adapt, signal sources get
crowded, or the underlying inefficiency closes. The pipeline output gives us
mean excess across the full year, but that average can hide a scanner that
delivered alpha in Q1 and zero in Q4. We flag those before they cost
production money.

Inputs (from a pipeline_report directory):
  - basket.csv          per-pick scanner attribution + surface_date
  - picks_returns.csv   per-(ticker, surface_date, horizon) excess return

Outputs:
  - timeline DataFrame: per-(scanner, week) mean excess + rolling N-week mean
  - alerts DataFrames:  degrading + improving scanners with z, severity,
                        sample sizes
  - prints summary tables; optional CSV writes via --out-dir

Statistical approach:
  - Per-week aggregation: for each (scanner, surface_date), mean excess of
    all picks attributed to that scanner that week (deduplicated to unique
    (ticker, surface_date) pairs to avoid bucket double-counting since the
    same pick appears in top_5/top_10/top_20 rows).
  - Recent window: last `window_weeks` weekly means.
  - Baseline: all weekly means BEFORE the recent window, capped at 365 days
    back from the recent window's start. The cap is a safety rail for
    future multi-year data and a no-op for the current 1-year backtest.
  - z = (mean(recent) - mean(baseline)) / (std(baseline) / sqrt(n_recent))
    where std(baseline) is the sample std (ddof=1) used as the noise yardstick.
  - Flag degrading if z < z_threshold (default -1.5).
  - Flag improving if z > -z_threshold (symmetric).

CLI:
  python -m scanners.backtest.drift_detector --report-dir D
    [--window-weeks 8] [--z-threshold -1.5] [--horizon 21] [--out-dir D]
"""
from __future__ import annotations

import argparse
import logging
import math
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_WINDOW_WEEKS = 8
DEFAULT_Z_THRESHOLD = -1.5
DEFAULT_HORIZON = 21
BASELINE_LOOKBACK_DAYS_CAP = 365


def _load_basket(report_dir: Path) -> pd.DataFrame:
    p = report_dir / "basket.csv"
    if not p.exists():
        raise FileNotFoundError(f"basket.csv not found in {report_dir}")
    return pd.read_csv(p)


def _load_returns(report_dir: Path, horizons: List[int]) -> pd.DataFrame:
    p = report_dir / "picks_returns.csv"
    if not p.exists():
        raise FileNotFoundError(
            f"picks_returns.csv not found in {report_dir}. "
            "Run analyze_pipeline first (any returns-using subcommand) to "
            "auto-generate it from data_cache."
        )
    df = pd.read_csv(p)
    return df[df["horizon_days"].isin(horizons)].copy()


def _split_scanners_hit(s) -> List[str]:
    """Tolerate comma/pipe/semicolon. None/NaN/empty -> []."""
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


def _per_scanner_weekly_excess(
    basket: pd.DataFrame,
    returns: pd.DataFrame,
    horizon: int,
) -> pd.DataFrame:
    """Long-format DataFrame: scanner, surface_date, n_picks, mean_excess.

    Dedupes basket on (ticker, surface_date) before exploding scanners_hit
    so each pick contributes once per scanner regardless of how many top_n
    buckets it landed in.
    """
    unique_picks = basket.drop_duplicates(["ticker", "surface_date"]).copy()
    rh = returns[returns["horizon_days"] == horizon][
        ["ticker", "surface_date", "excess_return"]
    ]
    joined = unique_picks.merge(rh, on=["ticker", "surface_date"], how="left")

    joined["scanners_list"] = joined["scanners_hit"].apply(_split_scanners_hit)
    exploded = joined.explode("scanners_list").rename(
        columns={"scanners_list": "scanner"}
    )
    exploded = exploded[exploded["scanner"].notna() & (exploded["scanner"] != "")]

    g = (
        exploded.groupby(["scanner", "surface_date"])
        .agg(
            n_picks=("ticker", "count"),
            mean_excess=("excess_return", lambda s: float(s.dropna().mean()) if s.dropna().any() else float("nan")),
        )
        .reset_index()
    )
    return g.sort_values(["scanner", "surface_date"]).reset_index(drop=True)


def _rolling_mean(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()


def compute_drift_timeline(
    basket: pd.DataFrame,
    returns: pd.DataFrame,
    window: int,
    horizon: int,
) -> pd.DataFrame:
    """One row per (scanner, surface_date) with raw weekly mean_excess plus
    a trailing N-week rolling_mean. Sorted by scanner then date."""
    weekly = _per_scanner_weekly_excess(basket, returns, horizon)
    if weekly.empty:
        return pd.DataFrame(columns=["scanner", "surface_date", "n_picks", "mean_excess", "rolling_mean"])

    parts = []
    for scanner, group in weekly.groupby("scanner"):
        g = group.sort_values("surface_date").copy()
        g["rolling_mean"] = _rolling_mean(g["mean_excess"], window)
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def _compute_z(recent_means: np.ndarray, baseline_means: np.ndarray) -> float:
    recent_clean = recent_means[~np.isnan(recent_means)]
    baseline_clean = baseline_means[~np.isnan(baseline_means)]
    if len(baseline_clean) < 2 or len(recent_clean) == 0:
        return float("nan")
    base_mean = float(baseline_clean.mean())
    base_std = float(baseline_clean.std(ddof=1))
    if base_std == 0:
        return float("nan")
    rec_mean = float(recent_clean.mean())
    se = base_std / math.sqrt(len(recent_clean))
    return (rec_mean - base_mean) / se


def _severity_label(z: float, threshold: float) -> str:
    """Severity tiers, symmetric around zero. Threshold is negative for
    degradation; we flip sign and tier on absolute distance."""
    if math.isnan(z):
        return "n/a"
    abs_z = abs(z)
    abs_thresh = abs(threshold)
    if abs_z >= abs_thresh * 1.67:
        return "SEVERE"
    if abs_z >= abs_thresh * 1.33:
        return "HIGH"
    if abs_z >= abs_thresh:
        return "MODERATE"
    return "below_threshold"


def detect_drift(
    timeline: pd.DataFrame,
    window: int,
    z_threshold: float,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (degrading, improving) DataFrames sorted by abs(z) descending."""
    if timeline.empty:
        empty = pd.DataFrame(columns=[
            "scanner", "recent_mean_excess_pct", "baseline_mean_excess_pct",
            "baseline_std_pct", "n_recent_weeks", "n_baseline_weeks",
            "z_score", "severity",
        ])
        return empty, empty.copy()

    rows = []
    for scanner, group in timeline.groupby("scanner"):
        g = group.sort_values("surface_date").copy()
        g["surface_date_dt"] = pd.to_datetime(g["surface_date"])
        weekly = g[["surface_date_dt", "mean_excess"]].dropna()
        if len(weekly) < window + 2:
            continue

        recent = weekly.tail(window)
        recent_start = recent["surface_date_dt"].min()
        cap_floor = recent_start - timedelta(days=BASELINE_LOOKBACK_DAYS_CAP)

        baseline = weekly[
            (weekly["surface_date_dt"] < recent_start)
            & (weekly["surface_date_dt"] >= cap_floor)
        ]
        if len(baseline) < 2:
            continue

        rec_arr = recent["mean_excess"].to_numpy(dtype=float)
        base_arr = baseline["mean_excess"].to_numpy(dtype=float)
        z = _compute_z(rec_arr, base_arr)

        rows.append({
            "scanner": scanner,
            "recent_mean_excess_pct": round(float(rec_arr.mean()) * 100, 3),
            "baseline_mean_excess_pct": round(float(base_arr.mean()) * 100, 3),
            "baseline_std_pct": round(float(np.std(base_arr, ddof=1)) * 100, 3),
            "n_recent_weeks": int(len(rec_arr)),
            "n_baseline_weeks": int(len(base_arr)),
            "z_score": round(z, 3) if not math.isnan(z) else float("nan"),
            "severity": _severity_label(z, z_threshold),
        })

    df = pd.DataFrame(rows)
    if df.empty:
        empty = pd.DataFrame(columns=[
            "scanner", "recent_mean_excess_pct", "baseline_mean_excess_pct",
            "baseline_std_pct", "n_recent_weeks", "n_baseline_weeks",
            "z_score", "severity",
        ])
        return empty, empty.copy()

    deg = df[df["z_score"] < z_threshold].copy()
    imp = df[df["z_score"] > -z_threshold].copy()
    deg = deg.assign(_abs_z=deg["z_score"].abs()).sort_values("_abs_z", ascending=False).drop(columns="_abs_z").reset_index(drop=True)
    imp = imp.assign(_abs_z=imp["z_score"].abs()).sort_values("_abs_z", ascending=False).drop(columns="_abs_z").reset_index(drop=True)
    return deg, imp


def _print_timeline_summary(timeline: pd.DataFrame) -> None:
    if timeline.empty:
        print("Timeline empty.")
        return
    n_scanners = timeline["scanner"].nunique()
    weeks_per = timeline.groupby("scanner")["surface_date"].nunique()
    print(f"Drift timeline: {n_scanners} scanners across {timeline['surface_date'].nunique()} weeks")
    print(f"  Weeks per scanner: min={weeks_per.min()}, max={weeks_per.max()}, median={int(weeks_per.median())}")


def _print_alerts(degrading: pd.DataFrame, improving: pd.DataFrame, threshold: float) -> None:
    print(f"\n=== DEGRADING (z < {threshold}) ===")
    if degrading.empty:
        print("  none")
    else:
        print(degrading.to_string(index=False))

    print(f"\n=== IMPROVING (z > {-threshold}) ===")
    if improving.empty:
        print("  none")
    else:
        print(improving.to_string(index=False))


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Phase 4e scanner edge drift detector")
    parser.add_argument(
        "--report-dir", type=Path,
        default=Path("backtest_output/_pipeline_report_2026-05-06"),
    )
    parser.add_argument("--window-weeks", type=int, default=DEFAULT_WINDOW_WEEKS)
    parser.add_argument("--z-threshold", type=float, default=DEFAULT_Z_THRESHOLD)
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON)
    parser.add_argument(
        "--out-dir", type=Path, default=None,
        help="Optional: write drift_timeline.csv + drift_alerts_*.csv here",
    )
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    basket = _load_basket(args.report_dir)
    returns = _load_returns(args.report_dir, [args.horizon])
    timeline = compute_drift_timeline(basket, returns, args.window_weeks, args.horizon)
    degrading, improving = detect_drift(timeline, args.window_weeks, args.z_threshold)

    _print_timeline_summary(timeline)
    _print_alerts(degrading, improving, args.z_threshold)

    if args.out_dir is not None:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        timeline.to_csv(args.out_dir / "drift_timeline.csv", index=False)
        degrading.to_csv(args.out_dir / "drift_alerts_degrading.csv", index=False)
        improving.to_csv(args.out_dir / "drift_alerts_improving.csv", index=False)
        print(f"\nWrote drift CSVs to {args.out_dir}")


if __name__ == "__main__":
    main()
