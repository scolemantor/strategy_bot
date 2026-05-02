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
        help="Symbol to use as buy-and-hold benchmark for the report. Default: SPY.",
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

    # Build the fetch list: tracked symbols, plus the report benchmark, plus
    # the regime benchmark (which may differ from the report benchmark and
    # which must be available for evaluate_regime to work in-loop).
    fetch_symbols = set(cfg.all_tracked_symbols())
    fetch_symbols.add(args.benchmark)
    if cfg.regime.enabled:
        fetch_symbols.add(cfg.regime.benchmark)
    fetch_symbols = sorted(fetch_symbols)

    log.info(f"Backtest period: {args.start} to {args.end}")
    log.info(f"Capital: ${args.capital:,.0f}, frequency: {args.frequency}")
    log.info(f"Fetching bars for {len(fetch_symbols)} symbols")

    bars = fetch_bars(fetch_symbols, args.start, args.end, creds, use_cache=not args.no_cache)
    closes = aligned_close_prices(bars)

    if closes.empty:
        log.error("No data returned. Check date range and symbols.")
        return

    # Build the column set passed into the strategy: tracked symbols (required
    # for trading) plus the regime benchmark (required for evaluate_regime to
    # actually evaluate). Drop rows where any TRACKED symbol is NaN — those
    # are days we can't trade. The benchmark column is allowed to have gaps;
    # evaluate_regime handles missing benchmark data with documented defaults.
    strategy_columns = list(cfg.all_tracked_symbols())
    if cfg.regime.enabled and cfg.regime.benchmark not in strategy_columns:
        if cfg.regime.benchmark in closes.columns:
            strategy_columns.append(cfg.regime.benchmark)
        else:
            log.warning(
                f"Regime is enabled but benchmark {cfg.regime.benchmark} not in fetched "
                f"data. Regime overlay will default ON for the entire backtest."
            )

    strategy_closes = closes[strategy_columns].dropna(subset=cfg.all_tracked_symbols())
    if strategy_closes.empty:
        log.error("No overlapping dates for all strategy symbols. Try a later start date.")
        return

    log.info(f"Running backtest on {len(strategy_closes)} trading days")
    if cfg.regime.enabled and cfg.regime.benchmark in strategy_closes.columns:
        log.info(f"Regime overlay active using {cfg.regime.benchmark}")

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