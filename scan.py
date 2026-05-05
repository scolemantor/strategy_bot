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
import sys
from datetime import date
from pathlib import Path

from scanners import SCANNERS, get_scanner, list_scanners
from scanners.base import save_result
from scanners.investability import filter_candidates
from scanners import watchlist as wl


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


def cmd_run(name: str, run_date: date, output_dir: Path, apply_filter: bool = True) -> None:
    log = logging.getLogger("scan")
    scanner = get_scanner(name)
    log.info(f"Running scanner: {scanner}")

    result = scanner.run(run_date)
    if result.error:
        log.error(f"{name} failed: {result.error}")
        return

    if result.notes:
        for n in result.notes:
            log.info(f"  - {n}")
    log.info(f"{name}: {result.count} candidate(s) raw")

    if result.count == 0:
        return

    if apply_filter:
        try:
            approved_df, rejected_df = filter_candidates(
                result.candidates,
                scanner_name=name,
            )
            log.info(f"  Investability filter: {len(approved_df)} approved, {len(rejected_df)} rejected")
            result.candidates = approved_df
            if not rejected_df.empty:
                _save_rejected(rejected_df, name, run_date, output_dir)
        except Exception as e:
            log.exception(f"  Investability filter failed: {e}; passing through unfiltered")

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
    for name in SCANNERS:
        log.info("=" * 60)
        log.info(f"Running: {name}")
        log.info("=" * 60)
        cmd_run(name, run_date, output_dir, apply_filter=apply_filter)


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