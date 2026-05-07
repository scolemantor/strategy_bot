"""Bootstrap-resampled confidence intervals for per-scanner edge.

Phase 4e v2 enhancement of the analyze_pipeline 'scanners' subcommand.
Adds 95% CIs (configurable) so post-backtest scanner_weights recalibration
is evidence-based rather than reading point estimates.

Two flavors:

  compute_scanner_edge_with_ci  -- simple bootstrap. Resample pick-level
    excess returns with replacement. Assumes picks are IID. Standard
    textbook CI; baseline.

  compute_scanner_edge_block_bootstrap -- moving-block bootstrap.
    Resample blocks of B consecutive weeks (default B=4). Honors
    within-week and short-window cross-pick correlation. Wider,
    more conservative CIs.

CLI:
  python -m scanners.backtest.edge_confidence simple [--horizon 21] ...
  python -m scanners.backtest.edge_confidence block  [--horizon 21] [--block-size-weeks 4] ...
  python -m scanners.backtest.edge_confidence both   [--horizon 21] [--block-size-weeks 4] ...

Common flags: --report-dir, --n-bootstrap (default 1000), --ci-level (0.95),
--seed (default 42).

Reproducibility: each compute function takes a `seed` parameter and
constructs an isolated np.random.default_rng(seed) generator. No global
RNG mutation, so concurrent calls with different seeds don't interfere.
"""
from __future__ import annotations

import argparse
import logging
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_REPORT_DIR = Path("backtest_output/_pipeline_report_2026-05-06")
DEFAULT_N_BOOTSTRAP = 1000
DEFAULT_CI_LEVEL = 0.95
DEFAULT_BLOCK_SIZE_WEEKS = 4
DEFAULT_HORIZON = 21
DEFAULT_SEED = 42


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


def _explode_per_scanner(
    basket: pd.DataFrame,
    returns: pd.DataFrame,
    horizon: int,
) -> pd.DataFrame:
    """Dedupe basket on (ticker, surface_date), explode scanners_hit, merge
    excess_return at given horizon, drop NaN.

    Returns long-format: scanner, surface_date, ticker, excess_return.
    """
    unique_picks = basket.drop_duplicates(["ticker", "surface_date"]).copy()
    rh = returns[returns["horizon_days"] == horizon][
        ["ticker", "surface_date", "excess_return"]
    ]
    joined = unique_picks.merge(rh, on=["ticker", "surface_date"], how="left")
    joined["scanners_list"] = joined["scanners_hit"].apply(_split_scanners_hit)
    exploded = joined.explode("scanners_list").rename(columns={"scanners_list": "scanner"})
    exploded = exploded[exploded["scanner"].notna() & (exploded["scanner"] != "")]
    exploded = exploded.dropna(subset=["excess_return"])
    return exploded[["scanner", "surface_date", "ticker", "excess_return"]].reset_index(drop=True)


# === simple bootstrap ===

def _simple_bootstrap_ci(
    returns: np.ndarray,
    n_bootstrap: int,
    ci_level: float,
    rng: np.random.Generator,
) -> Tuple[float, float]:
    """Vectorized: sample (n_bootstrap, n) matrix, mean across columns,
    take percentiles."""
    n = len(returns)
    if n == 0:
        return (float("nan"), float("nan"))
    samples = rng.choice(returns, size=(n_bootstrap, n), replace=True)
    means = samples.mean(axis=1)
    alpha = 1.0 - ci_level
    return (
        float(np.percentile(means, 100 * alpha / 2)),
        float(np.percentile(means, 100 * (1 - alpha / 2))),
    )


# === moving-block bootstrap ===

def _block_bootstrap_ci(
    weekly_groups: List[np.ndarray],
    n_bootstrap: int,
    ci_level: float,
    block_size: int,
    rng: np.random.Generator,
) -> Tuple[float, float]:
    """Moving-block bootstrap on weekly groups.

    n_possible_blocks = n_weeks - block_size + 1 (overlapping starts).
    Per resample: draw ceil(n_weeks / block_size) block starts uniformly,
    flatten picks across drawn blocks, take mean. Repeat n_bootstrap times.

    Falls back to simple bootstrap if n_weeks < block_size + 1.
    """
    n_weeks = len(weekly_groups)
    if n_weeks < block_size + 1:
        flat = np.concatenate(weekly_groups) if weekly_groups else np.array([])
        return _simple_bootstrap_ci(flat, n_bootstrap, ci_level, rng)

    n_possible = n_weeks - block_size + 1
    n_blocks_to_draw = max(1, int(math.ceil(n_weeks / block_size)))

    means = np.empty(n_bootstrap)
    for i in range(n_bootstrap):
        starts = rng.integers(0, n_possible, size=n_blocks_to_draw)
        chunks = []
        for s in starts:
            for w in range(s, s + block_size):
                if weekly_groups[w].size > 0:
                    chunks.append(weekly_groups[w])
        if chunks:
            means[i] = float(np.concatenate(chunks).mean())
        else:
            means[i] = np.nan

    means = means[~np.isnan(means)]
    if len(means) == 0:
        return (float("nan"), float("nan"))
    alpha = 1.0 - ci_level
    return (
        float(np.percentile(means, 100 * alpha / 2)),
        float(np.percentile(means, 100 * (1 - alpha / 2))),
    )


# === per-scanner metrics ===

def _scanner_summary(
    returns: np.ndarray, horizon: int,
) -> Dict[str, float]:
    n = len(returns)
    if n == 0:
        return {
            "n_picks": 0,
            "mean_excess_pct": float("nan"),
            "median_excess_pct": float("nan"),
            "win_rate_pct": float("nan"),
            "std_excess_pct": float("nan"),
            "sharpe_estimate": float("nan"),
        }
    mean = float(returns.mean())
    median = float(np.median(returns))
    win_rate = float((returns > 0).mean())
    std = float(returns.std(ddof=1)) if n > 1 else 0.0
    if std > 0 and horizon > 0:
        sharpe = (mean / std) * math.sqrt(252.0 / horizon)
    else:
        sharpe = float("nan")
    return {
        "n_picks": n,
        "mean_excess_pct": round(mean * 100, 3),
        "median_excess_pct": round(median * 100, 3),
        "win_rate_pct": round(win_rate * 100, 2),
        "std_excess_pct": round(std * 100, 3),
        "sharpe_estimate": round(sharpe, 3) if not math.isnan(sharpe) else float("nan"),
    }


# === public API ===

def compute_scanner_edge_with_ci(
    basket: pd.DataFrame,
    returns: pd.DataFrame,
    horizon: int,
    n_bootstrap: int = DEFAULT_N_BOOTSTRAP,
    ci_level: float = DEFAULT_CI_LEVEL,
    seed: int = DEFAULT_SEED,
) -> pd.DataFrame:
    """Per-scanner edge with simple-bootstrap percentile CIs."""
    exploded = _explode_per_scanner(basket, returns, horizon)
    rng = np.random.default_rng(seed)
    rows = []
    for scanner, group in exploded.groupby("scanner"):
        excess = group["excess_return"].to_numpy(dtype=float)
        summary = _scanner_summary(excess, horizon)
        ci_lo, ci_hi = _simple_bootstrap_ci(excess, n_bootstrap, ci_level, rng)
        rows.append({
            "scanner": scanner,
            "n_picks": summary["n_picks"],
            "mean_excess_pct": summary["mean_excess_pct"],
            "ci_lower_pct": round(ci_lo * 100, 3),
            "ci_upper_pct": round(ci_hi * 100, 3),
            "median_excess_pct": summary["median_excess_pct"],
            "win_rate_pct": summary["win_rate_pct"],
            "std_excess_pct": summary["std_excess_pct"],
            "sharpe_estimate": summary["sharpe_estimate"],
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values("mean_excess_pct", ascending=False).reset_index(drop=True)


def compute_scanner_edge_block_bootstrap(
    basket: pd.DataFrame,
    returns: pd.DataFrame,
    horizon: int,
    n_bootstrap: int = DEFAULT_N_BOOTSTRAP,
    ci_level: float = DEFAULT_CI_LEVEL,
    block_size_weeks: int = DEFAULT_BLOCK_SIZE_WEEKS,
    seed: int = DEFAULT_SEED,
) -> pd.DataFrame:
    """Per-scanner edge with moving-block-bootstrap CIs."""
    exploded = _explode_per_scanner(basket, returns, horizon)
    rng = np.random.default_rng(seed)
    rows = []
    for scanner, group in exploded.groupby("scanner"):
        excess = group["excess_return"].to_numpy(dtype=float)
        summary = _scanner_summary(excess, horizon)

        # Build weekly groups for this scanner: sorted by surface_date
        weekly_groups: List[np.ndarray] = []
        for _, week_group in group.sort_values("surface_date").groupby("surface_date", sort=True):
            weekly_groups.append(week_group["excess_return"].to_numpy(dtype=float))

        ci_lo, ci_hi = _block_bootstrap_ci(
            weekly_groups, n_bootstrap, ci_level, block_size_weeks, rng,
        )
        rows.append({
            "scanner": scanner,
            "n_picks": summary["n_picks"],
            "mean_excess_pct": summary["mean_excess_pct"],
            "ci_lower_pct": round(ci_lo * 100, 3),
            "ci_upper_pct": round(ci_hi * 100, 3),
            "median_excess_pct": summary["median_excess_pct"],
            "win_rate_pct": summary["win_rate_pct"],
            "std_excess_pct": summary["std_excess_pct"],
            "sharpe_estimate": summary["sharpe_estimate"],
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values("mean_excess_pct", ascending=False).reset_index(drop=True)


def compute_scanner_edge_both(
    basket: pd.DataFrame,
    returns: pd.DataFrame,
    horizon: int,
    n_bootstrap: int = DEFAULT_N_BOOTSTRAP,
    ci_level: float = DEFAULT_CI_LEVEL,
    block_size_weeks: int = DEFAULT_BLOCK_SIZE_WEEKS,
    seed: int = DEFAULT_SEED,
) -> pd.DataFrame:
    """Side-by-side simple + block CIs with ci_inflation_factor (block_width
    / simple_width). Joined on scanner + n_picks + mean_excess_pct."""
    df_simple = compute_scanner_edge_with_ci(
        basket, returns, horizon, n_bootstrap, ci_level, seed,
    )
    df_block = compute_scanner_edge_block_bootstrap(
        basket, returns, horizon, n_bootstrap, ci_level, block_size_weeks, seed,
    )
    if df_simple.empty or df_block.empty:
        return pd.DataFrame()

    s = df_simple[["scanner", "n_picks", "mean_excess_pct", "ci_lower_pct", "ci_upper_pct"]].rename(
        columns={"ci_lower_pct": "simple_ci_lo", "ci_upper_pct": "simple_ci_hi"}
    )
    b = df_block[["scanner", "ci_lower_pct", "ci_upper_pct"]].rename(
        columns={"ci_lower_pct": "block_ci_lo", "ci_upper_pct": "block_ci_hi"}
    )
    out = s.merge(b, on="scanner", how="inner")
    out["simple_width"] = (out["simple_ci_hi"] - out["simple_ci_lo"]).round(3)
    out["block_width"] = (out["block_ci_hi"] - out["block_ci_lo"]).round(3)
    out["ci_inflation_factor"] = (out["block_width"] / out["simple_width"]).round(3)
    return out.sort_values("mean_excess_pct", ascending=False).reset_index(drop=True)


# === CLI ===

def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Phase 4e v2 per-scanner edge with bootstrap CIs",
    )
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--horizon", type=int, default=DEFAULT_HORIZON)
    parser.add_argument("--n-bootstrap", type=int, default=DEFAULT_N_BOOTSTRAP)
    parser.add_argument("--ci-level", type=float, default=DEFAULT_CI_LEVEL)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--log-level", default="WARNING")

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("simple")
    p_block = sub.add_parser("block")
    p_block.add_argument("--block-size-weeks", type=int, default=DEFAULT_BLOCK_SIZE_WEEKS)
    p_both = sub.add_parser("both")
    p_both.add_argument("--block-size-weeks", type=int, default=DEFAULT_BLOCK_SIZE_WEEKS)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Lazy import to avoid coupling at module load
    from .analyze_pipeline import _load_basket, _load_or_compute_returns
    basket = _load_basket(args.report_dir)
    returns = _load_or_compute_returns(args.report_dir, basket, [args.horizon])

    if args.cmd == "simple":
        df = compute_scanner_edge_with_ci(
            basket, returns, args.horizon, args.n_bootstrap, args.ci_level, args.seed,
        )
        print(f"Simple bootstrap CIs (n={args.n_bootstrap}, ci_level={args.ci_level}, seed={args.seed})")
        print(df.to_string(index=False))
    elif args.cmd == "block":
        df = compute_scanner_edge_block_bootstrap(
            basket, returns, args.horizon, args.n_bootstrap, args.ci_level,
            args.block_size_weeks, args.seed,
        )
        print(
            f"Block bootstrap CIs (block_size={args.block_size_weeks}w, "
            f"n={args.n_bootstrap}, ci_level={args.ci_level}, seed={args.seed})"
        )
        print(df.to_string(index=False))
    else:  # both
        df = compute_scanner_edge_both(
            basket, returns, args.horizon, args.n_bootstrap, args.ci_level,
            args.block_size_weeks, args.seed,
        )
        print(
            f"Simple vs block bootstrap (block_size={args.block_size_weeks}w, "
            f"n={args.n_bootstrap}, ci_level={args.ci_level}, seed={args.seed})"
        )
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
