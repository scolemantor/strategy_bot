"""Forward return calculator for backtest evaluation.

Given a ticker and a "surface date" (the date a scanner would have flagged it),
compute the N-day forward return vs SPY benchmark.

Key design decisions to avoid look-ahead bias:
  - Entry price: NEXT-day open (not surface-day close — we couldn't have known
    the close price during the trading day, and after-hours filings often
    can't be acted on until next-day open)
  - Exit price: close N trading days later
  - Excess return: (ticker_return - SPY_return) over same window
  - Returns None if any required price data is missing (delisted, recent IPO
    pre-history, etc) — caller decides how to handle missing data

Bar data comes from cached Alpaca parquet files in data_cache/.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")


def _load_bars(ticker: str) -> Optional[pd.DataFrame]:
    """Load cached Alpaca bars for a ticker. Returns None if not cached."""
    p = CACHE_DIR / f"{ticker}.parquet"
    if not p.exists():
        return None
    try:
        df = pd.read_parquet(p)
        if df.empty:
            return None
        # Ensure index is sorted DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            if "timestamp" in df.columns:
                df = df.set_index("timestamp")
            elif "date" in df.columns:
                df = df.set_index("date")
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        return df
    except Exception as e:
        log.debug(f"  Failed to load bars for {ticker}: {e}")
        return None


def _next_trading_day_open(bars: pd.DataFrame, surface_date: date) -> Optional[Tuple[pd.Timestamp, float]]:
    """Find the first trading day strictly AFTER surface_date and return (date, open).
    Returns None if no future bars exist."""
    surface_ts = pd.Timestamp(surface_date)
    future = bars[bars.index > surface_ts]
    if future.empty:
        return None
    first = future.iloc[0]
    return (future.index[0], float(first["open"]))


def _close_at_n_trading_days_forward(
    bars: pd.DataFrame, entry_ts: pd.Timestamp, n_days: int
) -> Optional[Tuple[pd.Timestamp, float]]:
    """From the entry timestamp, find the close N trading days later. Returns (date, close)."""
    forward = bars[bars.index >= entry_ts]
    if len(forward) <= n_days:
        return None  # not enough forward data
    target = forward.iloc[n_days]
    return (forward.index[n_days], float(target["close"]))


def compute_forward_return(
    ticker: str, surface_date: date, n_trading_days: int
) -> Optional[float]:
    """Compute N-day forward return for a single ticker, surfaced on surface_date.
    Returns the simple return (e.g. 0.05 = +5%) or None if data unavailable.

    Uses next-day-open as entry, close N trading days later as exit.
    """
    bars = _load_bars(ticker)
    if bars is None:
        return None

    entry = _next_trading_day_open(bars, surface_date)
    if entry is None:
        return None
    entry_ts, entry_price = entry
    if entry_price <= 0:
        return None

    exit_ = _close_at_n_trading_days_forward(bars, entry_ts, n_trading_days)
    if exit_ is None:
        return None
    _, exit_price = exit_
    if exit_price <= 0:
        return None

    return (exit_price / entry_price) - 1.0


def compute_excess_return(
    ticker: str, surface_date: date, n_trading_days: int, benchmark: str = "SPY"
) -> Optional[float]:
    """Compute N-day forward return MINUS benchmark return over same window.
    Returns excess return (e.g. 0.03 = ticker beat SPY by 3%) or None.
    """
    ticker_ret = compute_forward_return(ticker, surface_date, n_trading_days)
    if ticker_ret is None:
        return None

    benchmark_ret = compute_forward_return(benchmark, surface_date, n_trading_days)
    if benchmark_ret is None:
        return None

    return ticker_ret - benchmark_ret


def compute_returns_for_candidates(
    candidates: List[Tuple[str, date]],
    horizons: List[int],
    benchmark: str = "SPY",
) -> pd.DataFrame:
    """Compute forward + excess returns for a list of (ticker, surface_date) pairs
    at multiple horizons. Returns a DataFrame with one row per (ticker, surface_date,
    horizon) combination plus columns for forward_return and excess_return.

    Missing data results in NaN values in those columns (rows not dropped — caller
    can decide).
    """
    rows = []
    for ticker, surface_date in candidates:
        for h in horizons:
            forward = compute_forward_return(ticker, surface_date, h)
            excess = compute_excess_return(ticker, surface_date, h, benchmark=benchmark)
            rows.append({
                "ticker": ticker,
                "surface_date": surface_date.isoformat() if isinstance(surface_date, date) else str(surface_date),
                "horizon_days": h,
                "forward_return": forward,
                "excess_return": excess,
            })
    return pd.DataFrame(rows)