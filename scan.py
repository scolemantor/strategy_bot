"""CLI entry point for the acorns scanner.

Examples:
  python scan.py list                              # list all available scanners
  python scan.py run insider_buying                # run a single scanner
  python scan.py all                               # run every working scanner
  python scan.py run insider_buying --no-filter    # bypass investability filter
  python scan.py watch add CRWV --reason "..."     # add ticker to watchlist
  python scan.py watch list                        # show watchlist
  python scan.py watch digest --date 2026-05-03    # generate daily digest
"""
from __future__ import annotations

import argparse
import logging
import socket
import sys
import time
import traceback
from datetime import date
from pathlib import Path

import pandas as pd

from scanners import DISABLED_IN_SCAN_ALL, SCANNERS, get_scanner, list_scanners
from scanners.base import save_result
from scanners.investability import filter_candidates
from scanners import watchlist as wl
from src.alerting import bridge, events
from src.alerting.setup import init_default_bridge

APP_VERSION = "0.4e"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_list() -> None:
    print("\nAvailable scanners:")
    for name, scanner in list_scanners().items():
        paid = " [PAID DATA]" if scanner.requires_paid_data else ""
        print(f"  {name:<25} {scanner.description}{paid}")
    print()


def cmd_run(name: str, run_date: date, output_dir: Path, apply_filter: bool = True) -> dict:
    """Run a single scanner.

    Returns {count, errors, runtime_seconds} for cmd_all aggregation. The
    `python scan.py run NAME` CLI path discards the return value.
    """
    log = logging.getLogger("scan")
    scanner = get_scanner(name)
    log.info(f"Running scanner: {scanner}")

    start_t = time.perf_counter()
    try:
        result = scanner.run(run_date)
    except Exception as e:
        runtime = time.perf_counter() - start_t
        bridge.alert(events.scanner_exception(
            scanner_name=name,
            exception_class=type(e).__name__,
            exception_message=str(e),
            traceback=traceback.format_exc(),
        ))
        log.exception(f"{name} raised {type(e).__name__}; continuing")
        return {"count": 0, "errors": 1, "runtime_seconds": runtime}
    runtime = time.perf_counter() - start_t

    errors = 1 if result.error else 0
    bridge.alert(events.scanner_complete(
        scanner_name=name,
        candidates_count=result.count,
        runtime_seconds=runtime,
        errors_count=errors,
    ))

    if result.error:
        log.error(f"{name} failed: {result.error}")
        return {"count": 0, "errors": 1, "runtime_seconds": runtime}

    if result.notes:
        for n in result.notes:
            log.info(f"  - {n}")
    log.info(f"{name}: {result.count} candidate(s) raw")

    if result.count == 0:
        return {"count": 0, "errors": 0, "runtime_seconds": runtime}

    rejection_dfs = []

    # Scanner-level rejections (e.g. ESPP filter on insider_buying)
    if result.rejected_candidates is not None and not result.rejected_candidates.empty:
        scanner_rej = result.rejected_candidates.copy()
        scanner_rej["source"] = "scanner"
        rejection_dfs.append(scanner_rej)

    if apply_filter:
        try:
            approved_df, rejected_df = filter_candidates(
                result.candidates,
                scanner_name=name,
            )
            log.info(f"  Investability filter: {len(approved_df)} approved, {len(rejected_df)} rejected")
            result.candidates = approved_df
            if not rejected_df.empty:
                inv_rej = rejected_df.copy()
                inv_rej["source"] = "investability"
                rejection_dfs.append(inv_rej)
        except Exception as e:
            log.exception(f"  Investability filter failed: {e}; passing through unfiltered")

    if rejection_dfs:
        combined_rej = pd.concat(rejection_dfs, ignore_index=True, sort=False)
        _save_rejected(combined_rej, name, run_date, output_dir)

    if not result.candidates.empty:
        path = save_result(result, output_dir)
        if path:
            log.info(f"Wrote {path}")
        print(f"\n=== Top candidates ({name}) ===")
        with_preview = result.candidates.head(20)
        if "reason" in with_preview.columns:
            with_preview = with_preview.copy()
            with_preview["reason"] = with_preview["reason"].str.slice(0, 70)
        print(with_preview.to_string(index=False))
        print()
    else:
        log.info(f"{name}: 0 candidates after investability filter")

    return {"count": result.count, "errors": errors, "runtime_seconds": runtime}


def _save_rejected(rejected_df, scanner_name: str, run_date: date, output_dir: Path) -> None:
    """Save rejected candidates to <scanner>_rejected.csv for audit trail."""
    log = logging.getLogger("scan")
    date_dir = output_dir / run_date.isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    rejected_path = date_dir / f"{scanner_name}_rejected.csv"
    try:
        rejected_df.to_csv(rejected_path, index=False)
        log.info(f"  Wrote {rejected_path} ({len(rejected_df)} rejected)")
    except Exception as e:
        log.warning(f"  Failed to write rejected.csv: {e}")


def cmd_all(run_date: date, output_dir: Path, apply_filter: bool = True) -> None:
    log = logging.getLogger("scan")
    bridge.alert(events.scan_started(scanner_count=len(SCANNERS)))

    total_candidates = 0
    total_errors = 0
    started_at = time.perf_counter()

    for name in SCANNERS:
        if name in DISABLED_IN_SCAN_ALL:
            log.info(f"Skipping {name} (disabled in scan_all; run via 'scan.py run {name}')")
            continue
        log.info("=" * 60)
        log.info(f"Running: {name}")
        log.info("=" * 60)
        result = cmd_run(name, run_date, output_dir, apply_filter=apply_filter)
        if result is not None:
            total_candidates += result.get("count", 0)
            total_errors += result.get("errors", 0)

    elapsed = time.perf_counter() - started_at

    # Logger-only suite-complete event (no Pushover; meta_ranker fires daily_summary)
    _log_suite_complete(run_date, total_candidates, total_errors, elapsed)


def _log_suite_complete(run_date: date, total_candidates: int, total_errors: int, elapsed: float) -> None:
    """Log a 'scan_suite_complete' event via the bridge's logger if present.
    No-op if bridge or logger not initialized. Never raises."""
    try:
        from src.alerting.bridge import _bridge
        if _bridge is None or _bridge._logger is None:
            return
        _bridge._logger.log(
            "scan_suite_complete",
            f"All {len(SCANNERS)} scanners run for {run_date}",
            level="INFO",
            payload={
                "scan_count": len(SCANNERS),
                "total_candidates_raw": total_candidates,
                "total_errors": total_errors,
                "runtime_seconds": elapsed,
                "run_date": run_date.isoformat(),
            },
        )
    except Exception:
        pass


def cmd_watch(args, output_dir: Path) -> None:
    """Handle the watch subcommand (Phase 4d)."""
    sub = args.watch_command

    if sub == "add":
        wl.add_ticker(args.ticker, reason=args.reason, category=args.category)
    elif sub == "remove":
        wl.remove_ticker(args.ticker)
    elif sub == "list":
        tickers = wl.list_tickers()
        if not tickers:
            print("\nWatchlist is empty.\n")
            return
        print(f"\n=== Watchlist ({len(tickers)} ticker(s)) ===")
        for t in tickers:
            print(f"  {t['ticker']:<8} added {t['added_date']:<12} "
                  f"category={t['category']:<15} reason={t['reason']}")
        print()
    elif sub == "digest":
        digest = wl.run_digest(args.date, output_dir)
        if digest.empty:
            print("\nNo watchlist data to display (watchlist may be empty).\n")
            return
        print(f"\n=== Watchlist digest ({args.date}) ===")
        display_cols = ["ticker", "scanner", "score", "delta_flag", "stale_flag", "scanner_reason"]
        cols_present = [c for c in display_cols if c in digest.columns]
        print(digest[cols_present].to_string(index=False))
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Acorns idea-generation scanner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # List subcommand
    subparsers.add_parser("list", help="List available scanners")

    # Run subcommand
    p_run = subparsers.add_parser("run", help="Run a single scanner")
    p_run.add_argument("name", help="Scanner name (see 'list')")
    p_run.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=date.today(),
        help="Run date (YYYY-MM-DD). Default: today.",
    )
    p_run.add_argument(
        "--no-filter",
        action="store_true",
        help="Bypass the investability filter and write raw scanner output.",
    )

    # All subcommand
    p_all = subparsers.add_parser("all", help="Run all available scanners")
    p_all.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=date.today(),
    )
    p_all.add_argument(
        "--no-filter",
        action="store_true",
        help="Bypass the investability filter and write raw scanner output.",
    )

    # Watch subcommand (Phase 4d)
    p_watch = subparsers.add_parser("watch", help="Manage and run watchlist tracking")
    watch_subs = p_watch.add_subparsers(dest="watch_command", required=True)

    p_watch_add = watch_subs.add_parser("add", help="Add ticker to watchlist")
    p_watch_add.add_argument("ticker")
    p_watch_add.add_argument("--reason", default="", help="Reason for tracking")
    p_watch_add.add_argument("--category", default="general", help="Category tag")

    p_watch_remove = watch_subs.add_parser("remove", help="Remove ticker from watchlist")
    p_watch_remove.add_argument("ticker")

    watch_subs.add_parser("list", help="Show current watchlist")

    p_watch_digest = watch_subs.add_parser("digest", help="Run watchlist digest for a date")
    p_watch_digest.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=date.today(),
    )

    parser.add_argument(
        "--output-dir",
        default="scan_output",
        help="Directory for CSV output. Default: scan_output/",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)
    output_dir = Path(args.output_dir)

    init_default_bridge()
    bridge.alert(events.system_startup(version=APP_VERSION, hostname=socket.gethostname()))

    if args.command == "list":
        cmd_list()
    elif args.command == "run":
        apply_filter = not getattr(args, "no_filter", False)
        cmd_run(args.name, args.date, output_dir, apply_filter=apply_filter)
    elif args.command == "all":
        apply_filter = not getattr(args, "no_filter", False)
        cmd_all(args.date, output_dir, apply_filter=apply_filter)
    elif args.command == "watch":
        cmd_watch(args, output_dir)


if __name__ == "__main__":
    main()