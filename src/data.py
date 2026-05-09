"""Historical bar data fetcher.

Pulls daily OHLC bars from Alpaca for a list of symbols and a date range.
Caches results to disk as parquet so repeated backtests are fast.

Cache strategy: merge fetched bars with existing cache rather than overwrite.
This is critical for Phase 4e backtest replay which calls fetch_bars with
many overlapping date windows — the cache must accumulate full history,
not just the most-recently-requested window.

This module is only used for backtesting, never for live trading.
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from .config import BrokerCredentials
from .http_utils import apply_default_timeout, with_deadline

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
BATCH_DELAY_SEC = 0.5  # throttle Alpaca batch fetches; ~2 req/sec


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
    """Save bars to cache. Merges with existing cached data so we accumulate
    full history across multiple fetches with different date windows."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    existing = _load_cached(symbol)
    if existing is not None and not existing.empty:
        # Merge: combine, dedupe by index, sort
        combined = pd.concat([existing, df])
        combined = combined[~combined.index.duplicated(keep="last")]
        combined = combined.sort_index()
        combined.to_parquet(_cache_path(symbol))
    else:
        df.to_parquet(_cache_path(symbol))


def fetch_bars(
    symbols: List[str],
    start: date,
    end: date,
    creds: BrokerCredentials,
    use_cache: bool = True,
    batch_size: Optional[int] = None,
) -> Dict[str, pd.DataFrame]:
    """Return a DataFrame of daily bars for each symbol, indexed by date.

    Cache is consulted first. A symbol is re-fetched only if cached data does
    not cover the requested [start, end] window. Fetched bars are merged into
    the cache rather than overwriting it.
    """
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
                    continue
        to_fetch.append(symbol)

    if not to_fetch:
        log.info(f"All {len(symbols)} symbols served from cache")
        return result

    BATCH_SIZE = batch_size if batch_size is not None else int(os.environ.get("ALPACA_BATCH_SIZE", "100"))
    total_batches = (len(to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE
    est_minutes = (total_batches * (BATCH_DELAY_SEC + 2)) / 60  # 0.5s throttle + ~2s/batch
    log.info(
        f"Fetching {len(to_fetch)} symbols from Alpaca in batches of {BATCH_SIZE} "
        f"with {BATCH_DELAY_SEC}s throttle (~{est_minutes:.1f} min); "
        f"{len(symbols) - len(to_fetch)} from cache"
    )

    bars_data = {}
    cumulative_rows = 0
    loop_t0 = time.monotonic()
    for batch_idx in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = (batch_idx // BATCH_SIZE) + 1
        if batch_num > 1:
            time.sleep(BATCH_DELAY_SEC)

        # Phase 7.5 hotfix: fresh Alpaca client per batch. The previous
        # implementation created one client outside the loop and reused it
        # across every batch. urllib3's connection pool would accumulate
        # state from earlier batches; after ~9-10 successful reuses the
        # pool would hand back a TCP connection that Alpaca's load balancer
        # had silently cycled, causing the next batch to hang ~30s on a
        # dead socket (TCP RTO). Confirmed positional: batch 10 in isolation
        # ran in 3.9s; the same symbols hung when reached after 9 prior
        # batches. Fresh client = fresh pool = no stale state. Cost is one
        # TLS handshake per batch (~100-300ms), negligible vs the 0.5s
        # throttle and 30s deadline.
        client = StockHistoricalDataClient(
            api_key=creds.api_key,
            secret_key=creds.secret_key,
        )
        apply_default_timeout(client._session, 60)

        log.info(f"  Batch {batch_num}/{total_batches}: fetching {len(batch)} symbols")
        batch_t0 = time.monotonic()
        try:
            req = StockBarsRequest(
                symbol_or_symbols=batch,
                timeframe=TimeFrame.Day,
                start=datetime.combine(start, datetime.min.time()),
                end=datetime.combine(end, datetime.min.time()),
            )
            batch_bars = with_deadline(lambda: client.get_stock_bars(req), timeout=30, default=None)
            batch_dt = time.monotonic() - batch_t0
            if batch_bars is None:
                log.warning(f"  Batch {batch_num} hit 30s deadline after {batch_dt:.1f}s; skipping")
                continue
            bars_data.update(batch_bars.data)
            batch_rows = sum(len(v) for v in batch_bars.data.values())
            cumulative_rows += batch_rows
            total_elapsed = time.monotonic() - loop_t0
            log.info(
                f"  Batch {batch_num}/{total_batches} done in {batch_dt:.1f}s: "
                f"{len(batch_bars.data)} symbols, {batch_rows:,} bars "
                f"(cum: {cumulative_rows:,} bars, {total_elapsed:.0f}s)"
            )
        except Exception as e:
            batch_dt = time.monotonic() - batch_t0
            log.warning(f"  Batch {batch_num} failed after {batch_dt:.1f}s: {e}; continuing with next batch")

    total_elapsed = time.monotonic() - loop_t0
    log.info(
        f"Fetch complete: {len(bars_data)} symbols, {cumulative_rows:,} total bars in {total_elapsed:.0f}s"
    )

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

        if use_cache:
            _save_cached(symbol, df)
            # After cache merge, return the FULL cache for downstream use,
            # not just the freshly-fetched portion. This way the caller
            # gets all available history including data fetched in earlier
            # calls with different windows.
            full_cached = _load_cached(symbol)
            if full_cached is not None:
                result[symbol] = full_cached.loc[
                    (full_cached.index >= pd.Timestamp(start)) &
                    (full_cached.index <= pd.Timestamp(end))
                ]
            else:
                result[symbol] = df
        else:
            result[symbol] = df

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