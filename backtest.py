"""CLI entry point for backtesting the oak strategy.

Examples:
  python backtest.py                            # default: 5y, $100k, monthly
  python backtest.py --start 2020-01-01
  python backtest.py --capital 200000 --frequency Q
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

from src.backtest import (
    buy_and_hold_benchmark,
    print_report,
    run_backtest,
    save_csvs,
)
from src.config import load_credentials, load_strategy
from src.data import aligned_close_prices, fetch_bars


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    today = date.today()
    default_start = today - timedelta(days=365 * 5)

    parser = argparse.ArgumentParser(description="Backtest the oak rebalancer")
    parser.add_argument("--config", default="config/strategy.yaml")
    parser.add_argument(
        "--start",
        type=lambda s: date.fromisoformat(s),
        default=default_start,
        help="Start date (YYYY-MM-DD). Default: 5 years ago.",
    )
    parser.add_argument(
        "--end",
        type=lambda s: date.fromisoformat(s),
        default=today,
        help="End date (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=100_000.0,
        help="Starting capital in dollars. Default: 100000.",
    )
    parser.add_argument(
        "--frequency",
        choices=["D", "W", "M", "Q"],
        default="M",
        help="Rebalance frequency: D=daily, W=weekly, M=monthly, Q=quarterly. Default: M.",
    )
    parser.add_argument(
        "--benchmark",
        default="SPY",
        help="Symbol to use as buy-and-hold benchmark. Default: SPY.",
    )
    parser.add_argument(
        "--output-dir",
        default="backtest_output",
        help="Directory for CSV output. Default: backtest_output/",
    )
    parser.add_argument("--no-cache", action="store_true", help="Bypass on-disk cache")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)
    log = logging.getLogger("backtest")

    log.info(f"Loading config and credentials")
    cfg = load_strategy(args.config)
    creds = load_credentials()

    symbols = cfg.all_tracked_symbols() + [args.benchmark]
    log.info(f"Backtest period: {args.start} to {args.end}")
    log.info(f"Capital: ${args.capital:,.0f}, frequency: {args.frequency}")
    log.info(f"Fetching bars for {len(symbols)} symbols")

    bars = fetch_bars(symbols, args.start, args.end, creds, use_cache=not args.no_cache)
    closes = aligned_close_prices(bars)

    if closes.empty:
        log.error("No data returned. Check date range and symbols.")
        return

    # Trim closes to symbols in the strategy (drop benchmark before passing to backtest)
    strategy_closes = closes[cfg.all_tracked_symbols()].dropna()
    if strategy_closes.empty:
        log.error("No overlapping dates for all strategy symbols. Try a later start date.")
        return

    log.info(f"Running backtest on {len(strategy_closes)} trading days")
    result = run_backtest(
        cfg,
        strategy_closes,
        initial_capital=args.capital,
        rebalance_frequency=args.frequency,
    )

    benchmark_series = None
    if args.benchmark in closes.columns:
        bench_closes = closes[args.benchmark].reindex(strategy_closes.index).dropna()
        if not bench_closes.empty:
            benchmark_series = buy_and_hold_benchmark(bench_closes, args.capital)

    print_report(result, benchmark=benchmark_series, benchmark_name=args.benchmark)

    save_csvs(result, Path(args.output_dir))
    print(f"  CSVs written to: {Path(args.output_dir).resolve()}")
    print()


if __name__ == "__main__":
    main()
