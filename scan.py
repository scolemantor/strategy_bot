"""CLI entry point for the acorns scanner.

Examples:
  python scan.py list                       # list all available scanners
  python scan.py run insider_buying         # run a single scanner
  python scan.py all                        # run every working scanner
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from scanners import SCANNERS, get_scanner, list_scanners
from scanners.base import save_result


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


def cmd_run(name: str, run_date: date, output_dir: Path) -> None:
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

    log.info(f"{name}: {result.count} candidate(s)")

    if result.count > 0:
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


def cmd_all(run_date: date, output_dir: Path) -> None:
    log = logging.getLogger("scan")
    for name in SCANNERS:
        log.info("=" * 60)
        log.info(f"Running: {name}")
        log.info("=" * 60)
        cmd_run(name, run_date, output_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Acorns idea-generation scanner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List available scanners")

    p_run = subparsers.add_parser("run", help="Run a single scanner")
    p_run.add_argument("name", help="Scanner name (see 'list')")
    p_run.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=date.today(),
        help="Run date (YYYY-MM-DD). Default: today.",
    )

    p_all = subparsers.add_parser("all", help="Run all available scanners")
    p_all.add_argument(
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
        cmd_run(args.name, args.date, output_dir)
    elif args.command == "all":
        cmd_all(args.date, output_dir)


if __name__ == "__main__":
    main()