"""Historical bar data fetcher.

Pulls daily OHLC bars from Alpaca for a list of symbols and a date range.
Caches results to disk as parquet so repeated backtests are fast.

This module is only used for backtesting, never for live trading.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from .config import BrokerCredentials

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")


def _cache_path(symbol: str) -> Path:
    return CACHE_DIR / f"{symbol}.parquet"


def _load_cached(symbol: str) -> pd.DataFrame | None:
    path = _cache_path(symbol)
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return None
        return df
    except Exception as e:
        log.warning(f"Failed to load cache for {symbol}: {e}")
        return None


def _save_cached(symbol: str, df: pd.DataFrame) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(_cache_path(symbol))


def fetch_bars(
    symbols: List[str],
    start: date,
    end: date,
    creds: BrokerCredentials,
    use_cache: bool = True,
) -> Dict[str, pd.DataFrame]:
    """Return a DataFrame of daily bars for each symbol, indexed by date."""
    client = StockHistoricalDataClient(
        api_key=creds.api_key,
        secret_key=creds.secret_key,
    )

    result: Dict[str, pd.DataFrame] = {}
    to_fetch: List[str] = []

    for symbol in symbols:
        if use_cache:
            cached = _load_cached(symbol)
            if cached is not None:
                cached_start = cached.index.min().date()
                cached_end = cached.index.max().date()
                if cached_start <= start and cached_end >= end:
                    result[symbol] = cached.loc[
                        (cached.index >= pd.Timestamp(start)) &
                        (cached.index <= pd.Timestamp(end))
                    ]
                    log.info(f"Using cached bars for {symbol} ({len(result[symbol])} days)")
                    continue
        to_fetch.append(symbol)

    if not to_fetch:
        return result

    # Alpaca rejects requests with 414 URI-too-long when too many symbols
    # are passed in a single call. Chunk into batches of 100 to stay safe.
    BATCH_SIZE = 100
    log.info(f"Fetching {len(to_fetch)} symbols from Alpaca in batches of {BATCH_SIZE}")

    bars_data = {}
    total_batches = (len(to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_idx in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = (batch_idx // BATCH_SIZE) + 1
        log.info(f"  Batch {batch_num}/{total_batches}: fetching {len(batch)} symbols")
        try:
            req = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                start=datetime.combine(start, datetime.min.time()),
                end=datetime.combine(end, datetime.min.time()),
            )
            batch_bars = client.get_stock_bars(req)
            bars_data.update(batch_bars.data)
        except Exception as e:
            log.warning(f"Batch {batch_num} failed: {e}; continuing with next batch")

    # Wrap in an object that mimics the original .data attribute access
    class _BarsContainer:
        def __init__(self, data):
            self.data = data
    bars = _BarsContainer(bars_data)

    for symbol in to_fetch:
        if symbol not in bars.data:
            log.warning(f"No bars returned for {symbol}")
            result[symbol] = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
            continue

        rows = []
        for bar in bars.data[symbol]:
            rows.append({
                "timestamp": bar.timestamp,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
            })
        df = pd.DataFrame(rows)
        if df.empty:
            result[symbol] = df
            continue
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
        df = df.set_index("timestamp").sort_index()
        result[symbol] = df
        if use_cache:
            _save_cached(symbol, df)
            log.info(f"Cached {len(df)} bars for {symbol}")

    return result


def aligned_close_prices(bars: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Combine per-symbol bar DataFrames into a single DataFrame of close prices."""
    series = {}
    for symbol, df in bars.items():
        if df.empty:
            continue
        series[symbol] = df["close"]

    if not series:
        return pd.DataFrame()

    closes = pd.DataFrame(series)
    closes = closes.ffill()
    return closes