"""Backtest replay orchestrator (Phase 4e).

Replays scanners against historical dates to measure their predictive edge.
Each scanner registers a `backtest_mode(as_of_date)` function that returns
the candidates it WOULD have surfaced if run on that date (using only data
available as of that date — no look-ahead).

This module:
  1. Picks a set of historical replay dates (e.g. one per week over 2y)
  2. For each scanner, calls backtest_mode for each replay date
  3. Aggregates all surfaced candidates into a (ticker, surface_date) list
  4. Computes forward returns at multiple horizons via forward_returns.py
  5. Computes per-scanner edge metrics via edge_metrics.py
  6. Outputs backtest_output/<run_date>/edge_report.csv

Scanner registration: each scanner that supports backtesting exposes a
backtest_mode() function in its own module. We import them here directly
to avoid a registry indirection — explicit imports make the supported set
visible at a glance.

CLI: python -m scanners.backtest.replay [--scanner NAME] [--start YYYY-MM-DD]
                                          [--end YYYY-MM-DD] [--cadence weekly]
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

log = logging.getLogger(__name__)

OUTPUT_DIR = Path("backtest_output")

# Default horizons: 1w / 1mo / 3mo / 6mo trading days
DEFAULT_HORIZONS = [5, 21, 63, 126]


def _generate_replay_dates(start: date, end: date, cadence: str = "weekly") -> List[date]:
    """Generate a list of historical replay dates from start to end.

    For weekly cadence: snap start to the next Monday if it falls on a weekend,
    then step in 7-day increments (always lands on a weekday).
    For daily/monthly cadence: skip weekends individually.
    """
    if cadence == "weekly":
        delta = timedelta(days=7)
    elif cadence == "monthly":
        delta = timedelta(days=30)
    elif cadence == "daily":
        delta = timedelta(days=1)
    else:
        raise ValueError(f"Unknown cadence: {cadence}")

    # Snap start forward to the next weekday if it lands on a weekend.
    # Saturday (5) -> +2 days, Sunday (6) -> +1 day.
    current = start
    while current.weekday() >= 5:
        current = current + timedelta(days=1)

    dates = []
    while current <= end:
        if current.weekday() < 5:
            dates.append(current)
        current = current + delta
    return dates


def replay_scanner(
    scanner_name: str,
    backtest_fn: Callable[[date], List[Tuple[str, date]]],
    replay_dates: List[date],
    horizons: List[int] = None,
) -> pd.DataFrame:
    """Run a single scanner across all replay dates, compute forward returns,
    return its edge report.

    backtest_fn signature: (as_of_date: date) -> List[(ticker, surface_date)]
    """
    horizons = horizons or DEFAULT_HORIZONS
    log.info(f"Replaying {scanner_name} across {len(replay_dates)} dates")

    all_candidates: List[Tuple[str, date]] = []
    for d in replay_dates:
        try:
            cands = backtest_fn(d)
            log.debug(f"  {d}: {len(cands)} candidates")
            all_candidates.extend(cands)
        except Exception as e:
            log.warning(f"  {scanner_name} failed on {d}: {e}")

    log.info(f"  Total candidates surfaced: {len(all_candidates)}")
    if not all_candidates:
        log.info(f"  No candidates; skipping edge report")
        return pd.DataFrame()

    returns_df = compute_returns_for_candidates(all_candidates, horizons=horizons)
    edge_df = compute_edge_report(returns_df, scanner_name=scanner_name, horizons=horizons)
    return edge_df


def _get_registered_scanners() -> Dict[str, Callable]:
    """Import and return the dict of scanner_name -> backtest_fn for all
    scanners that have registered a backtest_mode."""
    registered: Dict[str, Callable] = {}

    # Each scanner self-registers by exposing a backtest_mode function.
    # We import them here in a try/except so the orchestrator works as
    # scanners are added incrementally.
    try:
        from ..breakout_52w import backtest_mode as bt_breakout
        registered["breakout_52w"] = bt_breakout
    except (ImportError, AttributeError):
        log.debug("  breakout_52w backtest_mode not yet available")

    try:
        from ..insider_buying import backtest_mode as bt_ib
        registered["insider_buying"] = bt_ib
    except (ImportError, AttributeError):
        log.debug("  insider_buying backtest_mode not yet available")

    try:
        from ..insider_selling_clusters import backtest_mode as bt_isc
        registered["insider_selling_clusters"] = bt_isc
    except (ImportError, AttributeError):
        log.debug("  insider_selling_clusters backtest_mode not yet available")

    try:
        from ..earnings_drift import backtest_mode as bt_ed
        registered["earnings_drift"] = bt_ed
    except (ImportError, AttributeError):
        log.debug("  earnings_drift backtest_mode not yet available")

    try:
        from ..spinoff_tracker import backtest_mode as bt_sp
        registered["spinoff_tracker"] = bt_sp
    except (ImportError, AttributeError):
        log.debug("  spinoff_tracker backtest_mode not yet available")

    try:
        from ..thirteen_f_changes import backtest_mode as bt_13f
        registered["thirteen_f_changes"] = bt_13f
    except (ImportError, AttributeError):
        log.debug("  thirteen_f_changes backtest_mode not yet available")

    try:
        from ..short_squeeze import backtest_mode as bt_ss
        registered["short_squeeze"] = bt_ss
    except (ImportError, AttributeError):
        log.debug("  short_squeeze backtest_mode not yet available")

    try:
        from ..small_cap_value import backtest_mode as bt_scv
        registered["small_cap_value"] = bt_scv
    except (ImportError, AttributeError):
        log.debug("  small_cap_value backtest_mode not yet available")

    try:
        from ..sector_rotation import backtest_mode as bt_sr
        registered["sector_rotation"] = bt_sr
    except (ImportError, AttributeError):
        log.debug("  sector_rotation backtest_mode not yet available")

    return registered


def cli():
    parser = argparse.ArgumentParser(description="Phase 4e backtest replay")
    parser.add_argument("--scanner", help="Run only this scanner")
    parser.add_argument(
        "--start",
        type=lambda s: date.fromisoformat(s),
        default=date.today() - timedelta(days=730),
        help="Replay start date. Default: 2 years ago.",
    )
    parser.add_argument(
        "--end",
        type=lambda s: date.fromisoformat(s),
        default=date.today() - timedelta(days=180),
        help="Replay end date. Default: 6 months ago (so all horizons fit).",
    )
    parser.add_argument(
        "--cadence",
        choices=["daily", "weekly", "monthly"],
        default="weekly",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    replay_dates = _generate_replay_dates(args.start, args.end, args.cadence)
    log.info(f"Generated {len(replay_dates)} replay dates ({args.cadence}, {args.start} to {args.end})")

    registered = _get_registered_scanners()
    log.info(f"Registered scanners with backtest_mode: {sorted(registered.keys()) or 'NONE YET'}")

    if not registered:
        log.warning("No scanners have backtest_mode yet. Add backtest_mode() to scanner modules to enable.")
        return

    if args.scanner:
        if args.scanner not in registered:
            log.error(f"Scanner {args.scanner} not registered for backtesting")
            return
        registered = {args.scanner: registered[args.scanner]}

    all_reports = []
    for scanner_name, fn in registered.items():
        log.info("=" * 60)
        log.info(f"Backtesting: {scanner_name}")
        log.info("=" * 60)
        report = replay_scanner(scanner_name, fn, replay_dates)
        if not report.empty:
            all_reports.append(report)

    if not all_reports:
        log.warning("No edge reports generated")
        return

    master_report = pd.concat(all_reports, ignore_index=True)

    today = date.today()
    out_dir = OUTPUT_DIR / today.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "edge_report.csv"
    master_report.to_csv(report_path, index=False)
    log.info(f"Wrote {report_path}")

    print(f"\n=== Edge report ({today}) ===")
    print(master_report.to_string(index=False))
    print()


if __name__ == "__main__":
    cli()