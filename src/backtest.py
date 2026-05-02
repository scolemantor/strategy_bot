"""Backtest engine for the oak rebalancer."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .broker import Position
from .config import StrategyConfig
from .strategy import compute_rebalance_orders

log = logging.getLogger(__name__)

SLIPPAGE_BPS = 5


@dataclass
class Trade:
    date: pd.Timestamp
    symbol: str
    side: str
    qty: float
    price: float
    value: float


@dataclass
class BacktestResult:
    equity_curve: pd.Series
    trades: List[Trade] = field(default_factory=list)
    benchmark: pd.Series | None = None
    start_value: float = 0.0
    end_value: float = 0.0


def _rebalance_dates(closes_index: pd.DatetimeIndex, frequency: str) -> List[pd.Timestamp]:
    if frequency == "D":
        return list(closes_index)

    df = pd.DataFrame(index=closes_index)
    df["date"] = closes_index

    if frequency == "W":
        groups = df.groupby([df.index.isocalendar().year, df.index.isocalendar().week])
    elif frequency == "M":
        groups = df.groupby([df.index.year, df.index.month])
    elif frequency == "Q":
        groups = df.groupby([df.index.year, df.index.quarter])
    else:
        raise ValueError(f"Unsupported frequency: {frequency}")

    return [grp["date"].iloc[0] for _, grp in groups]


def run_backtest(
    cfg: StrategyConfig,
    closes: pd.DataFrame,
    initial_capital: float = 100_000.0,
    rebalance_frequency: str = "M",
) -> BacktestResult:
    """Run a paper backtest of the rebalancer over `closes`.

    `closes` must contain every tracked symbol (else ValueError). It MAY
    contain additional columns — typically the regime benchmark (SPY) — which
    are not traded but are passed through to the strategy as historical
    context. evaluate_regime and compute_sleeve_weights use whatever columns
    are present; missing ones trigger the documented fallbacks.

    On each rebalance bar, the slice of price history up through that bar
    (no look-ahead) is passed to compute_rebalance_orders so vol-weighting,
    regime detection, and any other lookback signals can use real history.
    """
    tracked = cfg.all_tracked_symbols()
    missing = [s for s in tracked if s not in closes.columns]
    if missing:
        raise ValueError(f"Closes DataFrame missing required symbols: {missing}")

    cash = initial_capital
    qty: Dict[str, float] = {sym: 0.0 for sym in tracked}
    equity: Dict[pd.Timestamp, float] = {}
    trades: List[Trade] = []

    rebalance_set = set(_rebalance_dates(closes.index, rebalance_frequency))

    for ts, row in closes.iterrows():
        prices = row.to_dict()

        positions_value = sum(qty[s] * prices[s] for s in tracked if pd.notna(prices.get(s)))
        total_value = cash + positions_value
        equity[ts] = total_value

        if ts not in rebalance_set:
            continue

        position_objs = {}
        for sym in tracked:
            if qty[sym] > 0 and pd.notna(prices.get(sym)):
                position_objs[sym] = Position(
                    symbol=sym, qty=qty[sym],
                    market_value=qty[sym] * prices[sym],
                    avg_entry_price=0.0,
                )

        if any(pd.isna(prices.get(s)) for s in tracked):
            continue

        # Slice history up through and including the current bar. This is the
        # critical line that was missing before — without it, the strategy
        # never saw historical prices, regime stayed permanently ON, and
        # vol-weighting silently fell back to equal weight.
        history_so_far = closes.loc[:ts]

        proposed_orders = compute_rebalance_orders(
            position_objs, total_value,
            {s: float(prices[s]) for s in tracked}, cfg,
            historical_prices=history_so_far,
        )

        slip = SLIPPAGE_BPS / 10_000
        sells = [o for o in proposed_orders if o.side == "sell"]
        buys = [o for o in proposed_orders if o.side == "buy"]

        for order in sells:
            held = qty.get(order.symbol, 0)
            actual_qty = min(order.estimated_qty, held)
            if actual_qty <= 0:
                continue
            exec_price = prices[order.symbol] * (1 - slip)
            proceeds = actual_qty * exec_price
            cash += proceeds
            qty[order.symbol] -= actual_qty
            trades.append(Trade(date=ts, symbol=order.symbol, side="sell",
                qty=actual_qty, price=exec_price, value=proceeds))

        for order in buys:
            exec_price = prices[order.symbol] * (1 + slip)
            cost = order.estimated_qty * exec_price
            if cost > cash:
                actual_qty = cash / exec_price
                if actual_qty <= 0:
                    continue
                cost = actual_qty * exec_price
            else:
                actual_qty = order.estimated_qty
            cash -= cost
            qty[order.symbol] = qty.get(order.symbol, 0) + actual_qty
            trades.append(Trade(date=ts, symbol=order.symbol, side="buy",
                qty=actual_qty, price=exec_price, value=cost))

    equity_series = pd.Series(equity).sort_index()
    return BacktestResult(
        equity_curve=equity_series,
        trades=trades,
        start_value=initial_capital,
        end_value=float(equity_series.iloc[-1]) if not equity_series.empty else 0.0,
    )


def buy_and_hold_benchmark(closes: pd.Series, initial_capital: float = 100_000.0) -> pd.Series:
    if closes.empty:
        return closes
    initial_qty = initial_capital / closes.iloc[0]
    return closes * initial_qty


def compute_stats(equity: pd.Series) -> Dict[str, float]:
    if len(equity) < 2:
        return {}
    daily_returns = equity.pct_change().dropna()
    days = len(equity)
    years = days / 252.0
    total_return = (equity.iloc[-1] / equity.iloc[0]) - 1
    cagr = (equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1 if years > 0 else 0
    volatility = daily_returns.std() * (252 ** 0.5)
    sharpe = (cagr - 0.0) / volatility if volatility > 0 else 0
    running_max = equity.cummax()
    drawdown = (equity - running_max) / running_max
    max_dd = drawdown.min()
    return {
        "total_return": total_return, "cagr": cagr,
        "volatility": volatility, "sharpe": sharpe,
        "max_drawdown": max_dd, "trading_days": days,
    }


def print_report(result: BacktestResult, benchmark: pd.Series | None = None,
                 benchmark_name: str = "SPY") -> None:
    stats = compute_stats(result.equity_curve)
    print("\n" + "=" * 60)
    print("  BACKTEST RESULTS")
    print("=" * 60)
    print(f"  Period:        {result.equity_curve.index[0].date()} to {result.equity_curve.index[-1].date()}")
    print(f"  Trading days:  {stats.get('trading_days', 0)}")
    print(f"  Trades:        {len(result.trades)}")
    print()
    print(f"  Starting:      ${result.start_value:>14,.2f}")
    print(f"  Ending:        ${result.end_value:>14,.2f}")
    print(f"  Total return:  {stats.get('total_return', 0):>14.1%}")
    print(f"  CAGR:          {stats.get('cagr', 0):>14.1%}")
    print(f"  Volatility:    {stats.get('volatility', 0):>14.1%}  (annualized)")
    print(f"  Sharpe:        {stats.get('sharpe', 0):>14.2f}")
    print(f"  Max drawdown:  {stats.get('max_drawdown', 0):>14.1%}")

    if benchmark is not None and not benchmark.empty:
        bench_aligned = benchmark.reindex(result.equity_curve.index).ffill()
        bench_stats = compute_stats(bench_aligned)
        print()
        print(f"  vs {benchmark_name} buy-and-hold:")
        print(f"    Ending:      ${bench_aligned.iloc[-1]:>14,.2f}")
        print(f"    Total ret:   {bench_stats.get('total_return', 0):>14.1%}")
        print(f"    CAGR:        {bench_stats.get('cagr', 0):>14.1%}")
        print(f"    Max DD:      {bench_stats.get('max_drawdown', 0):>14.1%}")
        delta = stats.get('total_return', 0) - bench_stats.get('total_return', 0)
        print(f"    Strategy outperforms by:  {delta:+.1%}")
    print()


def save_csvs(result: BacktestResult, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    equity_df = result.equity_curve.to_frame(name="portfolio_value")
    equity_df.index.name = "date"
    equity_df.to_csv(output_dir / "equity_curve.csv")
    trades_rows = [{
        "date": t.date.date(), "symbol": t.symbol, "side": t.side,
        "qty": t.qty, "price": t.price, "value": t.value,
    } for t in result.trades]
    pd.DataFrame(trades_rows).to_csv(output_dir / "trades.csv", index=False)
    log.info(f"Saved equity_curve.csv and trades.csv to {output_dir}")