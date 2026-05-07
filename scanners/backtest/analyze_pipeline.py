"""Pipeline output analysis CLI for Phase 4e backtest results.

Reads the artifacts produced by `python -m scanners.backtest.pipeline_replay`
and answers six questions:

  summary     -- pivot pipeline_edge_report.csv into a readable table
                 (3 buckets x 3 horizons = 9 rows, hit rates under 5 win
                 definitions, mean excess, annualized Sharpe).
  scanners    -- group basket picks by which scanners flagged them, compute
                 mean excess return per scanner. This is the answer that
                 recalibrates scanner_weights.yaml.
  histogram   -- ASCII distribution of forward returns for the top_10 bucket
                 at 21d horizon.
  drawdown    -- month-by-month equity curve assuming you traded the top_10
                 basket each week, max drawdown over the period.
  compare-spy -- top_10 vs SPY equity curves, total return, alpha, beta,
                 information ratio.
  report      -- combine all of the above into ANALYSIS.md in the report dir.

Inputs (in --report-dir, default backtest_output/_pipeline_report_2026-05-06/):
  - pipeline_edge_report.csv  (required)
  - basket.csv                (required for everything except `summary`)
  - picks_returns.csv         (auto-created on first need, then cached)

The picks_returns.csv auto-compute uses
scanners.backtest.forward_returns.compute_returns_for_candidates against
data_cache. That call can take minutes for ~1000 unique (ticker, date) pairs
and requires SPY + ticker bars to be cached locally.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_REPORT_DIR = Path("backtest_output/_pipeline_report_2026-05-06")
DEFAULT_TOP_N = 10
DEFAULT_HORIZON_WEEKLY = 5
DEFAULT_HORIZON_HISTOGRAM = 21


def _load_edge_report(report_dir: Path) -> pd.DataFrame:
    p = report_dir / "pipeline_edge_report.csv"
    if not p.exists():
        raise FileNotFoundError(f"pipeline_edge_report.csv not found in {report_dir}")
    return pd.read_csv(p)


def _load_basket(report_dir: Path) -> pd.DataFrame:
    p = report_dir / "basket.csv"
    if not p.exists():
        raise FileNotFoundError(f"basket.csv not found in {report_dir}")
    return pd.read_csv(p)


def _load_or_compute_returns(
    report_dir: Path,
    basket: pd.DataFrame,
    horizons: List[int],
) -> pd.DataFrame:
    """Return per-(ticker, surface_date, horizon) returns DataFrame.

    If picks_returns.csv exists, load and filter to requested horizons.
    Otherwise compute via forward_returns.compute_returns_for_candidates
    against data_cache and cache as picks_returns.csv.
    """
    cache_path = report_dir / "picks_returns.csv"
    if cache_path.exists():
        log.debug(f"Loading cached picks_returns from {cache_path}")
        df = pd.read_csv(cache_path)
        return df[df["horizon_days"].isin(horizons)].copy()

    log.info(f"picks_returns.csv not found in {report_dir}; computing from data_cache")
    from .forward_returns import compute_returns_for_candidates

    pairs = (
        basket[["ticker", "surface_date"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    candidates = [(t, date.fromisoformat(d)) for t, d in pairs]
    log.info(f"Computing forward returns for {len(candidates)} unique (ticker, date) pairs")
    df = compute_returns_for_candidates(candidates, horizons=horizons)
    df.to_csv(cache_path, index=False)
    log.info(f"Cached {len(df)} return rows to {cache_path}")
    return df


def _split_scanners_hit(s) -> List[str]:
    """meta_ranker emits comma-separated; tolerate pipe/semicolon. None/NaN -> []."""
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


def cmd_summary(args, report_dir: Path) -> str:
    edge = _load_edge_report(report_dir)
    cols = [
        "scanner", "horizon_days",
        "hit_any_beat", "hit_after_costs", "hit_material_2pct",
        "hit_strong_5pct", "hit_absolute_pos",
        "mean_excess_pct", "sharpe_annualized",
    ]
    out = edge[cols].copy()
    for c in ("hit_any_beat", "hit_after_costs", "hit_material_2pct",
              "hit_strong_5pct", "hit_absolute_pos"):
        out[c] = out[c].apply(lambda v: f"{v*100:5.1f}%" if pd.notna(v) else "  n/a")
    out["mean_excess_pct"] = out["mean_excess_pct"].apply(
        lambda v: f"{v:+5.2f}%" if pd.notna(v) else "  n/a"
    )
    out["sharpe_annualized"] = out["sharpe_annualized"].apply(
        lambda v: f"{v:+5.2f}" if pd.notna(v) else "  n/a"
    )
    text = out.to_string(index=False)
    print(text)
    return text


def cmd_scanners(args, report_dir: Path) -> pd.DataFrame:
    basket = _load_basket(report_dir)
    basket_n = basket[basket["top_n_bucket"] == args.top_n].copy()
    if basket_n.empty:
        print(f"No basket entries for top_n={args.top_n}")
        return pd.DataFrame()

    returns = _load_or_compute_returns(report_dir, basket_n, [args.horizon])
    rh = returns[returns["horizon_days"] == args.horizon][
        ["ticker", "surface_date", "forward_return", "excess_return"]
    ]

    joined = basket_n.merge(rh, on=["ticker", "surface_date"], how="left")
    joined["scanners_list"] = joined["scanners_hit"].apply(_split_scanners_hit)
    exploded = joined.explode("scanners_list").rename(columns={"scanners_list": "scanner"})
    exploded = exploded[exploded["scanner"].notna() & (exploded["scanner"] != "")]

    g = exploded.groupby("scanner").agg(
        n_picks=("ticker", "count"),
        mean_excess_pct=("excess_return", lambda s: float(s.dropna().mean() * 100) if s.dropna().any() else float("nan")),
        median_excess_pct=("excess_return", lambda s: float(s.dropna().median() * 100) if s.dropna().any() else float("nan")),
        win_rate=("excess_return", lambda s: float((s.dropna() > 0).mean()) if s.dropna().any() else float("nan")),
    ).reset_index().sort_values("mean_excess_pct", ascending=False).reset_index(drop=True)

    print(f"Per-scanner contribution at top_{args.top_n}, horizon {args.horizon}d")
    print(g.to_string(index=False))
    return g


def _bucket_returns(returns: pd.Series, n_bins: int, low: float, high: float) -> Dict[str, int]:
    edges = np.linspace(low, high, n_bins + 1)
    out: Dict[str, int] = {}
    for i in range(n_bins):
        lo, hi = edges[i], edges[i + 1]
        if i == 0:
            label = f"<= {hi*100:+.0f}%"
            mask = returns < hi
        elif i == n_bins - 1:
            label = f"> {lo*100:+.0f}%"
            mask = returns >= lo
        else:
            label = f"{lo*100:+.0f}..{hi*100:+.0f}%"
            mask = (returns >= lo) & (returns < hi)
        out[label] = int(mask.sum())
    return out


def cmd_histogram(args, report_dir: Path) -> Dict[str, int]:
    basket = _load_basket(report_dir)
    basket_n = basket[basket["top_n_bucket"] == args.top_n]
    returns = _load_or_compute_returns(report_dir, basket_n, [args.horizon])
    rh = returns[returns["horizon_days"] == args.horizon]
    rs = rh["forward_return"].dropna()

    bins = _bucket_returns(rs, args.bins, args.bin_low, args.bin_high)
    max_count = max(bins.values()) if bins and max(bins.values()) > 0 else 1
    print(f"Forward-return distribution: top_{args.top_n} @ {args.horizon}d  (n={len(rs)})")
    for label, count in bins.items():
        bar_width = int(40 * count / max_count) if max_count else 0
        print(f"  {label:>14}  | {'#' * bar_width}{' ' * (40 - bar_width)} {count:4d}")
    if len(rs) > 0:
        print(f"  mean={rs.mean()*100:+.2f}%, median={rs.median()*100:+.2f}%, std={rs.std()*100:.2f}%")
    return bins


def _weekly_basket_returns(
    basket: pd.DataFrame,
    top_n: int,
    horizon: int,
    report_dir: Path,
) -> pd.Series:
    basket_n = basket[basket["top_n_bucket"] == top_n]
    returns = _load_or_compute_returns(report_dir, basket_n, [horizon])
    rh = returns[returns["horizon_days"] == horizon][["ticker", "surface_date", "forward_return"]]
    joined = basket_n.merge(rh, on=["ticker", "surface_date"], how="left")
    weekly = joined.groupby("surface_date")["forward_return"].apply(
        lambda s: float(s.dropna().mean()) if s.dropna().any() else float("nan")
    )
    weekly.index = pd.to_datetime(weekly.index)
    return weekly.sort_index().dropna()


def cmd_drawdown(args, report_dir: Path) -> Dict:
    basket = _load_basket(report_dir)
    weekly = _weekly_basket_returns(basket, args.top_n, args.horizon, report_dir)
    if weekly.empty:
        print("No weekly returns available; aborting drawdown analysis")
        return {"total_return": 0.0, "max_drawdown": 0.0, "max_dd_date": "", "n_weeks": 0}

    eq = (1 + weekly).cumprod()
    peak = eq.cummax()
    dd = (eq - peak) / peak
    monthly = eq.resample("ME").last().ffill().pct_change()
    if not monthly.empty:
        monthly.iloc[0] = float(eq.iloc[0]) - 1.0

    summary = {
        "total_return": float(eq.iloc[-1] - 1),
        "max_drawdown": float(dd.min()),
        "max_dd_date": dd.idxmin().date().isoformat(),
        "n_weeks": int(len(weekly)),
    }
    print(f"Weekly basket: top_{args.top_n}, hold {args.horizon}d  ({summary['n_weeks']} weeks)")
    print(f"Total return:  {summary['total_return']*100:+.2f}%")
    print(f"Max drawdown:  {summary['max_drawdown']*100:.2f}% on {summary['max_dd_date']}")
    print("Month-by-month:")
    for ts, r in monthly.items():
        print(f"  {ts.strftime('%Y-%m')}  {r*100:+6.2f}%")
    return summary


def cmd_compare_spy(args, report_dir: Path) -> Dict:
    basket = _load_basket(report_dir)
    basket_n = basket[basket["top_n_bucket"] == args.top_n]
    returns = _load_or_compute_returns(report_dir, basket_n, [args.horizon])
    rh = returns[returns["horizon_days"] == args.horizon][
        ["ticker", "surface_date", "forward_return", "excess_return"]
    ].copy()
    rh["spy_return"] = rh["forward_return"] - rh["excess_return"]

    joined = basket_n.merge(rh, on=["ticker", "surface_date"], how="left")
    weekly = joined.groupby("surface_date").agg(
        basket_ret=("forward_return", lambda s: float(s.dropna().mean()) if s.dropna().any() else float("nan")),
        spy_ret=("spy_return", lambda s: float(s.dropna().mean()) if s.dropna().any() else float("nan")),
    )
    weekly.index = pd.to_datetime(weekly.index)
    weekly = weekly.sort_index().dropna()

    if weekly.empty:
        print("No paired weekly returns; aborting compare-spy")
        return {
            "basket_total_return": 0.0, "spy_total_return": 0.0,
            "alpha_per_period": 0.0, "alpha_annualized": 0.0,
            "beta": float("nan"), "information_ratio": float("nan"), "n_weeks": 0,
        }

    eq_basket = (1 + weekly["basket_ret"]).cumprod()
    eq_spy = (1 + weekly["spy_ret"]).cumprod()
    excess = weekly["basket_ret"] - weekly["spy_ret"]

    if len(weekly) >= 2 and weekly["spy_ret"].var(ddof=1) > 0:
        cov = float(np.cov(weekly["basket_ret"], weekly["spy_ret"], ddof=1)[0, 1])
        var_spy = float(weekly["spy_ret"].var(ddof=1))
        beta = cov / var_spy
    else:
        beta = float("nan")

    alpha_per_period = float(weekly["basket_ret"].mean() - (beta if not np.isnan(beta) else 0.0) * weekly["spy_ret"].mean())
    periods_per_year = 52 if args.horizon == 5 else (252.0 / args.horizon)
    alpha_annualized = alpha_per_period * periods_per_year
    info_ratio = float(excess.mean() / excess.std()) if excess.std() and len(excess) > 1 else float("nan")

    summary = {
        "basket_total_return": float(eq_basket.iloc[-1] - 1),
        "spy_total_return": float(eq_spy.iloc[-1] - 1),
        "alpha_per_period": alpha_per_period,
        "alpha_annualized": alpha_annualized,
        "beta": float(beta) if not np.isnan(beta) else float("nan"),
        "information_ratio": info_ratio,
        "n_weeks": int(len(weekly)),
    }
    print(f"Top-{args.top_n} vs SPY  ({summary['n_weeks']} weeks @ {args.horizon}d hold)")
    print(f"  Basket total: {summary['basket_total_return']*100:+.2f}%")
    print(f"  SPY total:    {summary['spy_total_return']*100:+.2f}%")
    print(f"  Alpha (ann):  {summary['alpha_annualized']*100:+.2f}%")
    print(f"  Beta:         {summary['beta']:.3f}")
    print(f"  Info ratio:   {summary['information_ratio']:.3f}")
    return summary


def cmd_report(args, report_dir: Path) -> Path:
    sections: List[str] = []
    sections.append(f"# Pipeline analysis - {date.today().isoformat()}\n")
    sections.append(f"**Report directory:** `{report_dir}`\n")

    sections.append("## Summary table\n")
    sections.append("```\n" + cmd_summary(argparse.Namespace(), report_dir) + "\n```\n")

    sections.append("## Per-scanner contribution (top_10 @ 21d)\n")
    sca = cmd_scanners(argparse.Namespace(top_n=10, horizon=21), report_dir)
    sections.append("```\n" + sca.to_string(index=False) + "\n```\n")

    sections.append("## Distribution of forward returns (top_10 @ 21d)\n")
    bins = cmd_histogram(
        argparse.Namespace(top_n=10, horizon=21, bins=12, bin_low=-0.30, bin_high=0.30),
        report_dir,
    )
    sections.append("```\n" + "\n".join(f"  {k:>14}  {v:4d}" for k, v in bins.items()) + "\n```\n")

    sections.append("## Drawdown (top_10 weekly)\n")
    dd = cmd_drawdown(argparse.Namespace(top_n=10, horizon=5), report_dir)
    sections.append(
        "```\n"
        f"Total return:  {dd['total_return']*100:+.2f}%\n"
        f"Max drawdown:  {dd['max_drawdown']*100:.2f}% on {dd['max_dd_date']}\n"
        f"Weeks traded:  {dd['n_weeks']}\n"
        "```\n"
    )

    sections.append("## SPY comparison (top_10 weekly)\n")
    cs = cmd_compare_spy(argparse.Namespace(top_n=10, horizon=5), report_dir)
    sections.append(
        "```\n"
        f"Basket total: {cs['basket_total_return']*100:+.2f}%\n"
        f"SPY total:    {cs['spy_total_return']*100:+.2f}%\n"
        f"Alpha (ann):  {cs['alpha_annualized']*100:+.2f}%\n"
        f"Beta:         {cs['beta']:.3f}\n"
        f"Info ratio:   {cs['information_ratio']:.3f}\n"
        "```\n"
    )

    sections.append(
        "## Notes\n\n"
        "- Forward returns assume next-day-open entry, close N trading days later.\n"
        "- Weekly equity curve assumes equal-weight basket, no transaction costs, "
        "no slippage, no rebalancing within the holding window.\n"
        "- Per-scanner contribution attributes a pick's full excess return to "
        "every scanner that flagged it; double-attribution by design (a pick "
        "hit by 3 scanners contributes to all 3 averages).\n"
    )

    out_path = report_dir / "ANALYSIS.md"
    out_path.write_text("\n".join(sections), encoding="utf-8")
    print(f"Wrote {out_path}")
    return out_path


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Phase 4e pipeline output analysis")
    parser.add_argument("--report-dir", type=Path, default=DEFAULT_REPORT_DIR)
    parser.add_argument("--log-level", default="WARNING")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_sum = sub.add_parser("summary", help="formatted edge_report table")
    p_sum.set_defaults(func=cmd_summary)

    p_sca = sub.add_parser("scanners", help="per-scanner mean excess return")
    p_sca.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    p_sca.add_argument("--horizon", type=int, default=DEFAULT_HORIZON_HISTOGRAM)
    p_sca.set_defaults(func=cmd_scanners)

    p_hi = sub.add_parser("histogram", help="ASCII forward-return distribution")
    p_hi.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    p_hi.add_argument("--horizon", type=int, default=DEFAULT_HORIZON_HISTOGRAM)
    p_hi.add_argument("--bins", type=int, default=12)
    p_hi.add_argument("--bin-low", type=float, default=-0.30)
    p_hi.add_argument("--bin-high", type=float, default=0.30)
    p_hi.set_defaults(func=cmd_histogram)

    p_dd = sub.add_parser("drawdown", help="weekly basket equity curve + max DD")
    p_dd.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    p_dd.add_argument("--horizon", type=int, default=DEFAULT_HORIZON_WEEKLY)
    p_dd.set_defaults(func=cmd_drawdown)

    p_cmp = sub.add_parser("compare-spy", help="basket vs SPY: total/alpha/beta/IR")
    p_cmp.add_argument("--top-n", type=int, default=DEFAULT_TOP_N)
    p_cmp.add_argument("--horizon", type=int, default=DEFAULT_HORIZON_WEEKLY)
    p_cmp.set_defaults(func=cmd_compare_spy)

    p_rep = sub.add_parser("report", help="full markdown analysis -> ANALYSIS.md")
    p_rep.set_defaults(func=cmd_report)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args.func(args, args.report_dir)


if __name__ == "__main__":
    main()
