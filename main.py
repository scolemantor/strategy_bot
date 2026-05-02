"""CLI entry point for the oak strategy trading bot.

Commands:
  status      Show current portfolio vs. target allocations.
  rebalance   Compute (and optionally execute) rebalance orders.

Defaults to dry-run. Live execution requires --execute AND non-paper credentials
AND an explicit typed confirmation.

Tax-aware lot ledger (Phase 3):
  When `ledger.enabled: true` is set in strategy.yaml, the bot maintains a
  SQLite database of every fill to track per-lot cost basis. Before each
  rebalance run that will execute, the bot:
    1. Auto-seeds (idempotent) any broker-held symbols not yet in the ledger.
    2. Reconciles ledger total qty against broker-reported qty per symbol.
    3. Halts the rebalance if reconciliation finds any mismatch.

  Dry runs do not touch the ledger.
  When `ledger.enabled: false` (default), the bot runs exactly as pre-Phase-3.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Optional

from src.broker import AlpacaBroker
from src.config import StrategyConfig, load_credentials, load_strategy
from src.executor import execute_orders
from src.lot_ledger import LotLedger
from src.lot_migration import reconcile_with_broker, seed_from_broker
from src.risk import check_orders
from src.strategy import (
    compute_holding_status,
    compute_rebalance_orders,
    compute_target_values,
)


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_status(broker: AlpacaBroker, cfg: StrategyConfig) -> None:
    account = broker.get_account()
    positions = broker.get_positions()

    mode = "PAPER" if broker.paper else "LIVE"
    print(f"\n=== Portfolio status ({mode}) ===")
    print(f"  Cash:            ${account.cash:>14,.2f}")
    print(f"  Portfolio value: ${account.portfolio_value:>14,.2f}")
    print(f"  Buying power:    ${account.buying_power:>14,.2f}")

    targets = compute_target_values(account.portfolio_value, cfg)
    statuses = compute_holding_status(positions, targets, cfg)

    print(f"\n  {'Symbol':<8} {'Sleeve':<10} {'Target':>14} {'Current':>14} {'Drift':>9}")
    print("  " + "-" * 60)
    by_sleeve = sorted(statuses, key=lambda s: (s.sleeve, s.symbol))
    for s in by_sleeve:
        drift_str = f"{s.drift_pct:+.1%}" if s.target_value > 0 else "n/a"
        print(
            f"  {s.symbol:<8} {s.sleeve:<10} ${s.target_value:>12,.0f} "
            f"${s.current_value:>12,.0f} {drift_str:>9}"
        )

    # Untracked symbols held in the account
    tracked = set(targets.keys())
    untracked = [p for sym, p in positions.items() if sym not in tracked]
    if untracked:
        print("\n  Untracked positions (not part of trunk/branches):")
        for p in untracked:
            print(f"    {p.symbol:<8} ${p.market_value:>12,.0f}")
    print()


def cmd_rebalance(
    broker: AlpacaBroker,
    cfg: StrategyConfig,
    dry_run: bool,
    seeding_mode: bool = False,
    ledger: Optional[LotLedger] = None,
) -> None:
    log = logging.getLogger("rebalance")

    log.info("Fetching account state and positions")
    account = broker.get_account()
    positions = broker.get_positions()
    log.info(f"Portfolio value: ${account.portfolio_value:,.2f}")

    # Ledger seeding + reconciliation (only when actually executing)
    if ledger is not None and not dry_run:
        seed_result = seed_from_broker(
            ledger, positions, date.today(), only_missing=True
        )
        if seed_result.seeded_symbols:
            log.info(
                f"Seeded ledger with {len(seed_result.seeded_symbols)} new symbol(s): "
                f"{', '.join(seed_result.seeded_symbols)}"
            )

        recon = reconcile_with_broker(ledger, positions)
        if not recon.is_clean:
            log.error("Ledger/broker reconciliation FAILED — refusing to trade")
            for line in recon.summary().split("\n"):
                log.error(f"  {line}")
            print("\n" + "=" * 60)
            print("  LEDGER MISMATCH — REFUSING TO TRADE")
            print("=" * 60)
            print(recon.summary())
            print()
            print("Possible causes: manual trades outside the bot, dividend")
            print("reinvestment, stock splits, or a corrupted ledger.")
            print()
            print("Fix manually before retrying. To bypass the ledger temporarily,")
            print("set ledger.enabled: false in config/strategy.yaml.")
            print()
            sys.exit(2)

    tracked = cfg.all_tracked_symbols()
    log.info(f"Fetching quotes for {len(tracked)} symbols")
    quotes = broker.get_quotes(tracked)

    missing_quotes = [s for s in tracked if quotes.get(s, 0) <= 0]
    if missing_quotes:
        log.warning(f"Missing quotes for: {missing_quotes} - those holdings will be skipped")

    orders = compute_rebalance_orders(positions, account.portfolio_value, quotes, cfg)
    log.info(f"Strategy proposes {len(orders)} order(s)")

    if not orders:
        print("\nNo rebalance needed - all holdings within drift threshold.\n")
        return

    market_open = broker.is_market_open()
    if not market_open:
        log.info("Market is currently closed")

    if seeding_mode:
        log.warning("SEEDING MODE: per-order size limit bypassed")

    risk_result = check_orders(
        orders, account, cfg,
        market_open=market_open,
        seeding_mode=seeding_mode,
    )

    if risk_result.halt:
        log.warning(f"HALT triggered: {risk_result.halt_reason}")
        print(f"\nKill switch triggered: {risk_result.halt_reason}")
        for order, reason in risk_result.rejected:
            print(f"  REJECTED {order.side} {order.symbol}: {reason}")
        print()
        return

    if risk_result.rejected:
        log.warning(f"{len(risk_result.rejected)} order(s) rejected by risk manager")
        for order, reason in risk_result.rejected:
            print(f"  REJECTED {order.side:<4} {order.symbol}: {reason}")

    mode = "DRY RUN" if dry_run else "LIVE EXECUTION"
    print(f"\n=== {mode}: {len(risk_result.approved)} order(s) approved ===")
    results = execute_orders(
        risk_result.approved,
        broker,
        dry_run=dry_run,
        ledger=ledger,
    )

    print("\n=== Summary ===")
    ledger_failures = 0
    for r in results:
        line = f"  {r.status.upper():<24} {r.side:<5} {r.qty:>10.4f} {r.symbol}"
        print(line)
        if r.error:
            print(f"             error: {r.error}")
        if "_LEDGER_FAILED" in r.status:
            ledger_failures += 1

    if ledger_failures:
        print()
        print(f"  WARNING: {ledger_failures} order(s) had LEDGER UPDATE FAILURES.")
        print("  Run reconciliation manually before next rebalance.")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Oak strategy trading bot")
    parser.add_argument("command", choices=["status", "rebalance"])
    parser.add_argument("--config", default="config/strategy.yaml")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually place orders (default is dry run)",
    )
    parser.add_argument(
        "--seeding",
        action="store_true",
        help="Bypass the per-order size limit (initial seeding only - kill switches still apply)",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)
    log = logging.getLogger("main")

    log.info("Loading configuration")
    cfg = load_strategy(args.config)
    creds = load_credentials()

    # Confirmation for live trading
    if args.execute and not creds.paper:
        print("\n" + "=" * 60)
        print("  LIVE TRADING MODE - real money")
        print("=" * 60)
        confirm = input("Type 'I UNDERSTAND' to proceed: ")
        if confirm.strip() != "I UNDERSTAND":
            print("Aborted.")
            sys.exit(1)

    # Confirmation for seeding mode (extra fence even on paper)
    if args.execute and args.seeding:
        print("\n" + "=" * 60)
        print("  SEEDING MODE - per-order size limit bypassed")
        print("  Use only for initial portfolio seeding.")
        print("=" * 60)
        confirm = input("Type 'SEED' to proceed: ")
        if confirm.strip() != "SEED":
            print("Aborted.")
            sys.exit(1)

    log.info(f"Connecting to Alpaca ({'paper' if creds.paper else 'LIVE'})")
    broker = AlpacaBroker(creds)

    # Open lot ledger if enabled in config
    ledger: Optional[LotLedger] = None
    if cfg.ledger.enabled:
        db_path = Path(cfg.ledger.db_path).expanduser()
        log.info(f"Tax-aware ledger enabled, opening at {db_path}")
        ledger = LotLedger(db_path)

    if args.command == "status":
        cmd_status(broker, cfg)
    elif args.command == "rebalance":
        cmd_rebalance(
            broker, cfg,
            dry_run=not args.execute,
            seeding_mode=args.seeding,
            ledger=ledger,
        )


if __name__ == "__main__":
    main()
