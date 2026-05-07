"""Pipeline-level backtest replay (Phase 4e v2).

Replays the FULL system (scanners -> meta_ranker) across historical dates and
measures portfolio-level forward returns of the top-N basket from master_ranked.csv.

This is what we actually trade. Individual scanner edge measurement is not the
goal — the goal is "if I ran my system every week last year and bought the
top-N from master_ranked, would I have made money."

Pipeline per replay date:
  1. Call each available scanner's backtest_mode(D) — writes <scanner>.csv
     to backtest_output/<D>/.
  2. Run meta_ranker.aggregate(D, output_dir=backtest_output/) — produces
     master_ranked.csv at backtest_output/<D>/.
  3. Read top N (default 5/10/20) from master_ranked.csv.
  4. Record each top-N pick as a basket entry with surface_date.

After all replay dates:
  5. For each basket size and each horizon (5/21/63 trading days), compute
     forward excess return vs SPY for every entry.
  6. Aggregate to portfolio metrics: hit rates under multiple definitions,
     mean/median excess, win/loss ratio, Sharpe.
  7. Output pipeline_edge_report.csv.

CLI: python -m scanners.backtest.pipeline_replay [--start D] [--end D]
                                                  [--cadence weekly]
                                                  [--top-ns 5,10,20]
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Tuple

import pandas as pd

from .forward_returns import compute_returns_for_candidates
from .edge_metrics import compute_edge_report
from .. import meta_ranker

log = logging.getLogger(__name__)

BACKTEST_OUTPUT_DIR = Path("backtest_output")
DEFAULT_TOP_NS = [5, 10, 20]
DEFAULT_HORIZONS = [5, 21, 63]


def _generate_replay_dates(start: date, end: date, cadence: str = "weekly") -> List[date]:
    if cadence == "weekly":
        delta = timedelta(days=7)
    elif cadence == "monthly":
        delta = timedelta(days=30)
    elif cadence == "daily":
        delta = timedelta(days=1)
    else:
        raise ValueError(f"Unknown cadence: {cadence}")

    current = start
    while current.weekday() >= 5:
        current = current + timedelta(days=1)

    dates = []
    while current <= end:
        if current.weekday() < 5:
            dates.append(current)
        current = current + delta
    return dates


def _get_registered_backtest_scanners() -> Dict[str, Callable]:
    """Return {scanner_name: backtest_mode_fn} for scanners that have it."""
    registered: Dict[str, Callable] = {}

    try:
        from ..breakout_52w import backtest_mode as bt_b52
        registered["breakout_52w"] = bt_b52
    except (ImportError, AttributeError):
        log.debug("  breakout_52w backtest_mode not available")

    try:
        from ..insider_buying import backtest_mode as bt_ib
        registered["insider_buying"] = bt_ib
    except (ImportError, AttributeError):
        log.debug("  insider_buying backtest_mode not available")

    try:
        from ..insider_selling_clusters import backtest_mode as bt_isc
        registered["insider_selling_clusters"] = bt_isc
    except (ImportError, AttributeError):
        log.debug("  insider_selling_clusters backtest_mode not available")

    try:
        from ..earnings_drift import backtest_mode as bt_ed
        registered["earnings_drift"] = bt_ed
    except (ImportError, AttributeError):
        log.debug("  earnings_drift backtest_mode not available")

    try:
        from ..spinoff_tracker import backtest_mode as bt_sp
        registered["spinoff_tracker"] = bt_sp
    except (ImportError, AttributeError):
        log.debug("  spinoff_tracker backtest_mode not available")

    try:
        from ..thirteen_f_changes import backtest_mode as bt_13f
        registered["thirteen_f_changes"] = bt_13f
    except (ImportError, AttributeError):
        log.debug("  thirteen_f_changes backtest_mode not available")

    try:
        from ..short_squeeze import backtest_mode as bt_ss
        registered["short_squeeze"] = bt_ss
    except (ImportError, AttributeError):
        log.debug("  short_squeeze backtest_mode not available")

    try:
        from ..small_cap_value import backtest_mode as bt_scv
        registered["small_cap_value"] = bt_scv
    except (ImportError, AttributeError):
        log.debug("  small_cap_value backtest_mode not available")

    try:
        from ..sector_rotation import backtest_mode as bt_sr
        registered["sector_rotation"] = bt_sr
    except (ImportError, AttributeError):
        log.debug("  sector_rotation backtest_mode not available")

    return registered


def replay_pipeline(
    replay_dates: List[date],
    top_ns: List[int],
) -> pd.DataFrame:
    """Run all scanners and meta-ranker for each replay date, collect top-N picks."""
    scanners = _get_registered_backtest_scanners()
    if not scanners:
        log.error("No scanners have backtest_mode registered; cannot run pipeline replay")
        return pd.DataFrame()

    log.info(f"Pipeline replay across {len(replay_dates)} dates")
    log.info(f"  Available scanners: {sorted(scanners.keys())}")

    basket_rows: List[Dict] = []
    max_top_n = max(top_ns)

    for d_idx, d in enumerate(replay_dates):
        log.info(f"  [{d_idx+1}/{len(replay_dates)}] Replay date: {d}")

        scanner_counts = {}
        for sc_name, sc_fn in scanners.items():
            try:
                count = sc_fn(d, output_dir=BACKTEST_OUTPUT_DIR)
                scanner_counts[sc_name] = count
            except Exception as e:
                log.warning(f"    {sc_name} failed on {d}: {e}")
                scanner_counts[sc_name] = 0

        log.info(f"    Scanner counts: {scanner_counts}")

        try:
            master_df, _conflicts, _summary = meta_ranker.aggregate(
                d, output_dir=BACKTEST_OUTPUT_DIR
            )
        except Exception as e:
            log.warning(f"    meta_ranker failed on {d}: {e}")
            continue

        if master_df.empty:
            log.info(f"    master_ranked empty for {d}; skipping basket")
            continue

        top_picks = master_df.head(max_top_n).reset_index(drop=True)
        for rank, row in top_picks.iterrows():
            in_buckets = [n for n in top_ns if rank < n]
            for bucket in in_buckets:
                basket_rows.append({
                    "ticker": row["ticker"],
                    "surface_date": d.isoformat(),
                    "rank": rank + 1,
                    "composite_score": float(row["composite_score"]),
                    "n_scanners": int(row["n_scanners"]),
                    "scanners_hit": row["scanners_hit"],
                    "top_n_bucket": bucket,
                })

    if not basket_rows:
        log.warning("  No basket entries collected across any replay date")
        return pd.DataFrame()

    return pd.DataFrame(basket_rows)


def compute_pipeline_edge(
    basket_df: pd.DataFrame,
    top_ns: List[int],
    horizons: List[int],
) -> pd.DataFrame:
    """For each (top_n_bucket, horizon) combination, compute forward returns
    and edge metrics."""
    if basket_df.empty:
        return pd.DataFrame()

    candidates: List[Tuple[str, date]] = []
    for _, row in basket_df.drop_duplicates(["ticker", "surface_date"]).iterrows():
        candidates.append((
            row["ticker"],
            date.fromisoformat(row["surface_date"]),
        ))

    log.info(f"Computing forward returns for {len(candidates)} unique (ticker, date) pairs")
    returns_df = compute_returns_for_candidates(candidates, horizons=horizons)

    all_reports = []
    for n in top_ns:
        bucket_df = basket_df[basket_df["top_n_bucket"] == n]
        if bucket_df.empty:
            continue
        bucket_keys = set(zip(
            bucket_df["ticker"],
            bucket_df["surface_date"],
        ))

        returns_df_filtered = returns_df[
            returns_df.apply(
                lambda r: (r["ticker"], r["surface_date"]) in bucket_keys,
                axis=1,
            )
        ]

        edge = compute_edge_report(returns_df_filtered, scanner_name=f"top_{n}", horizons=horizons)
        if not edge.empty:
            all_reports.append(edge)

    if not all_reports:
        return pd.DataFrame()

    return pd.concat(all_reports, ignore_index=True)


def cli():
    parser = argparse.ArgumentParser(description="Phase 4e pipeline-level backtest replay")
    parser.add_argument(
        "--start",
        type=lambda s: date.fromisoformat(s),
        default=date.today() - timedelta(days=730),
    )
    parser.add_argument(
        "--end",
        type=lambda s: date.fromisoformat(s),
        default=date.today() - timedelta(days=180),
    )
    parser.add_argument("--cadence", choices=["daily", "weekly", "monthly"], default="weekly")
    parser.add_argument(
        "--top-ns",
        default="5,10,20",
        help="Comma-separated top-N values to evaluate (default: 5,10,20)",
    )
    parser.add_argument(
        "--horizons",
        default="5,21,63",
        help="Comma-separated forward-return horizons in trading days (default: 5,21,63)",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    top_ns = [int(x) for x in args.top_ns.split(",")]
    horizons = [int(x) for x in args.horizons.split(",")]

    replay_dates = _generate_replay_dates(args.start, args.end, args.cadence)
    log.info(f"Generated {len(replay_dates)} replay dates ({args.cadence}, {args.start} -> {args.end})")
    log.info(f"Top-N buckets: {top_ns}")
    log.info(f"Forward horizons: {horizons} trading days")

    basket_df = replay_pipeline(replay_dates, top_ns)

    if basket_df.empty:
        log.warning("Pipeline replay produced no basket entries; aborting")
        return

    today = date.today()
    out_dir = BACKTEST_OUTPUT_DIR / f"_pipeline_report_{today.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    basket_path = out_dir / "basket.csv"
    basket_df.to_csv(basket_path, index=False)
    log.info(f"Wrote basket to {basket_path} ({len(basket_df)} entries)")

    edge_df = compute_pipeline_edge(basket_df, top_ns, horizons)
    if edge_df.empty:
        log.warning("No edge metrics produced")
        return

    edge_path = out_dir / "pipeline_edge_report.csv"
    edge_df.to_csv(edge_path, index=False)
    log.info(f"Wrote {edge_path}")

    print(f"\n=== Pipeline edge report ({today}) ===")
    print(edge_df.to_string(index=False))
    print()


if __name__ == "__main__":
    cli()