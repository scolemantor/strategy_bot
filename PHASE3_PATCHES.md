# Phase 3 — main.py and backtest.py patches

The other two changes are partial — surgical edits to existing files, not full rewrites. Here's what to change.

---

## main.py — `cmd_rebalance` function

Replace the body of `cmd_rebalance` with this version. The change: it now fetches historical prices when the config requires them (vol weighting or regime detection enabled), and passes them through to `compute_rebalance_orders`.

```python
def cmd_rebalance(broker: AlpacaBroker, cfg: StrategyConfig, dry_run: bool, seeding_mode: bool = False) -> None:
    log = logging.getLogger("rebalance")

    log.info("Fetching account state and positions")
    account = broker.get_account()
    positions = broker.get_positions()
    log.info(f"Portfolio value: ${account.portfolio_value:,.2f}")

    tracked = cfg.all_tracked_symbols()
    log.info(f"Fetching quotes for {len(tracked)} symbols")
    quotes = broker.get_quotes(tracked)

    missing_quotes = [s for s in tracked if quotes.get(s, 0) <= 0]
    if missing_quotes:
        log.warning(f"Missing quotes for: {missing_quotes} - those holdings will be skipped")

    historical_prices = None
    if cfg.needs_history():
        from datetime import date, timedelta
        from src.data import fetch_bars, aligned_close_prices
        from src.config import load_credentials
        creds = load_credentials()

        history_symbols = list(tracked)
        if cfg.regime.enabled and cfg.regime.benchmark not in history_symbols:
            history_symbols.append(cfg.regime.benchmark)

        history_days_needed = max(cfg.regime.ma_window if cfg.regime.enabled else 0, 90) + 30
        start = date.today() - timedelta(days=int(history_days_needed * 1.6))
        log.info(f"Fetching {history_days_needed}+ days of history for vol/regime")
        bars = fetch_bars(history_symbols, start, date.today(), creds, use_cache=True)
        historical_prices = aligned_close_prices(bars)
        if historical_prices.empty:
            log.warning("No historical prices fetched; vol/regime fall back to defaults")

    orders = compute_rebalance_orders(
        positions, account.portfolio_value, quotes, cfg,
        historical_prices=historical_prices,
    )
    log.info(f"Strategy proposes {len(orders)} order(s)")
    
    # ...rest of function continues unchanged (the part that prints orders, runs risk checks, executes)
```

The "rest of function" comment refers to the existing logic that prints proposed orders, runs them through the risk manager, and either executes or dry-runs. That part doesn't change.

---

## main.py — `cmd_status` function

Replace the body of `cmd_status` so it also fetches history and prints regime status:

```python
def cmd_status(broker: AlpacaBroker, cfg: StrategyConfig) -> None:
    account = broker.get_account()
    positions = broker.get_positions()

    mode = "PAPER" if broker.paper else "LIVE"
    print(f"
=== Portfolio status ({mode}) ===")
    print(f"  Cash:            ${account.cash:>14,.2f}")
    print(f"  Portfolio value: ${account.portfolio_value:>14,.2f}")
    print(f"  Buying power:    ${account.buying_power:>14,.2f}")

    historical_prices = None
    if cfg.needs_history():
        from datetime import date, timedelta
        from src.data import fetch_bars, aligned_close_prices
        from src.config import load_credentials
        creds = load_credentials()
        history_symbols = list(cfg.all_tracked_symbols())
        if cfg.regime.enabled and cfg.regime.benchmark not in history_symbols:
            history_symbols.append(cfg.regime.benchmark)
        history_days_needed = max(cfg.regime.ma_window if cfg.regime.enabled else 0, 90) + 30
        start = date.today() - timedelta(days=int(history_days_needed * 1.6))
        bars = fetch_bars(history_symbols, start, date.today(), creds, use_cache=True)
        historical_prices = aligned_close_prices(bars)

    if cfg.regime.enabled:
        from src.strategy import evaluate_regime
        regime = evaluate_regime(cfg, historical_prices)
        if regime.benchmark_price > 0:
            signal = "RISK ON" if not regime.is_offsignal else "RISK OFF"
            print(f"  Regime ({regime.benchmark}): {signal} - price ${regime.benchmark_price:.2f} "
                  f"vs {cfg.regime.ma_window}d MA ${regime.moving_average:.2f} "
                  f"(equity multiplier {regime.risk_multiplier:.2f})")

    targets = compute_target_values(account.portfolio_value, cfg, historical_prices=historical_prices)
    statuses = compute_holding_status(positions, targets, cfg)

    print(f"
  {'Symbol':<8} {'Sleeve':<10} {'Target':>14} {'Current':>14} {'Drift':>9}")
    print("  " + "-" * 60)
    by_sleeve = sorted(statuses, key=lambda s: (s.sleeve, s.symbol))
    for s in by_sleeve:
        drift_str = f"{s.drift_pct:+.1%}" if s.target_value > 0 else "n/a"
        print(
            f"  {s.symbol:<8} {s.sleeve:<10} ${s.target_value:>12,.0f} "
            f"${s.current_value:>12,.0f} {drift_str:>9}"
        )

    tracked = set(targets.keys())
    untracked = [p for sym, p in positions.items() if sym not in tracked]
    if untracked:
        print("
  Untracked positions (not part of trunk/branches):")
        for p in untracked:
            print(f"    {p.symbol:<8} ${p.market_value:>12,.0f}")
    print()
```

---

## backtest.py (root) — `main` function

Find the section in `main()` of `backtest.py` that builds the symbols list and fetches bars. Replace it with this version, which ensures the regime benchmark gets pulled even if it's not a tracked holding:

```python
    symbols = cfg.all_tracked_symbols() + [args.benchmark]
    if cfg.regime.enabled and cfg.regime.benchmark not in symbols:
        symbols.append(cfg.regime.benchmark)
    log.info(f"Backtest period: {args.start} to {args.end}")
    log.info(f"Capital: ${args.capital:,.0f}, frequency: {args.frequency}")
    log.info(f"Fetching bars for {len(symbols)} symbols")

    bars = fetch_bars(symbols, args.start, args.end, creds, use_cache=not args.no_cache)
    closes = aligned_close_prices(bars)

    if closes.empty:
        log.error("No data returned. Check date range and symbols.")
        return

    # Strategy gets tracked symbols PLUS regime benchmark (so regime check has data)
    strategy_cols = cfg.all_tracked_symbols()
    if cfg.regime.enabled and cfg.regime.benchmark in closes.columns:
        if cfg.regime.benchmark not in strategy_cols:
            strategy_cols = strategy_cols + [cfg.regime.benchmark]
    strategy_closes = closes[strategy_cols].dropna()
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
```

---

## How to deploy this safely

1. **Branch it.** Before you change any of these files, create a git branch:
   ```
   git checkout -b phase3
   ```
   That way if anything goes wrong you can `git checkout main` and you're back to the working V3 code.

2. **Replace the three full files** (`config/strategy.yaml`, `src/config.py`, `src/strategy.py`) with the versions in this folder.

3. **Patch `main.py` and `backtest.py`** with the surgical edits above.

4. **Validate before any rebalance:**
   ```
   python -c "from src.config import load_strategy; cfg = load_strategy(); print('Config OK, needs_history:', cfg.needs_history())"
   python -c "from src.strategy import evaluate_regime, compute_sleeve_weights; print('Imports OK')"
   ```

5. **Run a backtest** to compare against V3:
   ```
   python backtest.py
   ```
   Expected: lower CAGR than V3 (regime drags during recoveries) but lower max drawdown (regime exits in selloffs). Vol weighting should reduce COPX/SMH exposure and increase XLU/IHI.

6. **Run status to see the regime signal:**
   ```
   python main.py status
   ```
   You should see a new "Regime (SPY): RISK ON/OFF" line.

7. **Only merge to main and rebalance with --execute** after the backtest looks reasonable and status output makes sense.

## What was validated in the original Phase 3 build

In synthetic-data backtest, the engine produced:
- Branches inverse-vol weights: COPX 7.8%, SMH 9.1%, PAVE 12.6%, INDA 13.6%, ITA 15.6%, IHI 19.4%, XLU 22.0%
- Regime correctly tripped to RISK OFF when SPY was below 200dma
- Backtest: 9.5% CAGR, 5.4% volatility, -6.5% max drawdown, Sharpe 1.75 (synthetic, not real)

Real-data results will differ — that's the point of running the backtest after deploying.
