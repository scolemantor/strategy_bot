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

import gc
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from alpaca.data.enums import Adjustment
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from .config import BrokerCredentials
from .http_utils import apply_default_timeout, with_deadline

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
BATCH_DELAY_SEC = 0.5  # throttle Alpaca batch fetches; ~2 req/sec

# Phase 8a: Alpaca's bar API defaults to RAW (unadjusted) prices. NOW (ServiceNow)
# 5-for-1 split on 2025-12-18 was returning unadjusted historical bars,
# producing nonsense indicators (200dma=$515 vs current=$91). Switch to ALL
# (split + dividend adjusted) so technical indicators + 52w highs are correct.
# This breaks cache compat — old cached parquets contain unadjusted bars and
# would mix with new adjusted bars. CACHE_VERSION bump invalidates old caches
# on first fetch_bars() call after deploy.
BAR_ADJUSTMENT = Adjustment.ALL
CACHE_VERSION_FILE = CACHE_DIR / ".cache_version"
CACHE_VERSION = "2"  # v1 = pre-split-adjust (raw), v2 = post-split-adjust (all)


def _rss_kb() -> int:
    """Process resident set size in KB. -1 on platforms that don't have
    resource.getrusage (Windows). Used for diagnostic logging during
    the batch-volume-correlated hang investigation."""
    try:
        import resource  # POSIX-only
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return -1


def _check_cache_version() -> None:
    """First-call-after-deploy cache invalidation. If CACHE_VERSION_FILE
    is missing or contains a different version string, wipe all per-symbol
    bar parquets at the top level of CACHE_DIR. Sub-directories (yfinance
    fundamentals, sec_form4_parsed, etc) are left untouched.

    Called at the start of fetch_bars() — not at module import time so
    test runs that don't fetch don't trigger wipes."""
    current = None
    try:
        if CACHE_VERSION_FILE.exists():
            current = CACHE_VERSION_FILE.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    if current == CACHE_VERSION:
        return  # already current

    if CACHE_DIR.exists():
        wiped = 0
        for p in CACHE_DIR.glob("*.parquet"):
            try:
                p.unlink()
                wiped += 1
            except Exception:
                pass
        if wiped > 0:
            log.info(
                f"Cache version mismatch (had={current!r}, want={CACHE_VERSION!r}); "
                f"wiped {wiped} per-symbol bar parquet(s) — Alpaca refetch on next call"
            )
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        CACHE_VERSION_FILE.write_text(CACHE_VERSION, encoding="utf-8")
    except Exception as e:
        log.warning(f"Failed to write cache version sentinel: {e}")


def _to_alpaca_symbol(s: str) -> str:
    """Canonical (yfinance/Wikipedia) -> Alpaca form. BRK-B -> BRK.B etc.
    Alpaca's data API rejects the dash form for share-class tickers with
    `{"message":"invalid symbol: BF-B"}`. Other tickers are returned unchanged."""
    return s.replace("-", ".")


def _from_alpaca_symbol(s: str) -> str:
    """Alpaca form -> canonical. BRK.B -> BRK-B etc. Used to translate
    response keys back so the rest of the codebase (cache, downstream
    scanners) sees only canonical symbols."""
    return s.replace(".", "-")


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

    Per-batch client lifecycle (after several rounds of misdiagnosis — see
    git log for hotfix #1-#5 history):

      - Fresh StockHistoricalDataClient per batch → fresh urllib3 pool,
        no carryover connection state. Single shared client (hotfix #3)
        failed at batch 10 — some accumulated state inside urllib3 or
        alpaca-py we never fully isolated.
      - client._retry = 0 → disables alpaca-py SDK silent retry-on-429
        (defaults to 3 retries × 3s wait, surfaces as a 30s "hang").
      - try/finally with client._session.close() → best-effort socket
        release. urllib3 2.x's PoolManager.clear() only empties the
        pool dict (actual connection close depends on GC), so this is
        not a hard guarantee but doesn't hurt.

    Hotfix #4 also tried mounting a fresh HTTPAdapter with pool_maxsize=100;
    that REGRESSED — alpaca-py mounts its own adapter with custom request
    handling, replacing it with a vanilla one introduced "invalid symbol"
    errors for dotted tickers (BF-B, CWEN-A, MOG-A) and shifted the hang
    forward to batch 10. Don't re-mount adapters; let alpaca-py keep its.
    """
    # Invalidate any unadjusted-bar cache files on first call after deploy.
    # No-op if cache version sentinel is already current.
    _check_cache_version()

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

    cumulative_rows = 0
    loop_t0 = time.monotonic()
    for batch_idx in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = (batch_idx // BATCH_SIZE) + 1
        if batch_num > 1:
            time.sleep(BATCH_DELAY_SEC)

        client = StockHistoricalDataClient(
            api_key=creds.api_key,
            secret_key=creds.secret_key,
        )
        # Disable SDK's internal retry-on-429 (defaults are 3 retries × 3s
        # wait, surfaces as a 30s hang). Fail fast instead.
        client._retry = 0
        apply_default_timeout(client._session, 60)

        log.info(
            f"  Batch {batch_num}/{total_batches}: fetching {len(batch)} symbols "
            f"[rss_kb={_rss_kb()} cum_bars={cumulative_rows:,}]"
        )
        batch_t0 = time.monotonic()
        try:
            # Translate canonical (BRK-B) to Alpaca form (BRK.B) before request;
            # response keys translate back so cache + downstream code stays
            # canonical-only.
            batch_alpaca = [_to_alpaca_symbol(s) for s in batch]
            req = StockBarsRequest(
                symbol_or_symbols=batch_alpaca,
                timeframe=TimeFrame.Day,
                start=datetime.combine(start, datetime.min.time()),
                end=datetime.combine(end, datetime.min.time()),
                adjustment=BAR_ADJUSTMENT,
            )
            batch_bars = with_deadline(lambda: client.get_stock_bars(req), timeout=30, default=None)
            batch_dt = time.monotonic() - batch_t0
            if batch_bars is None:
                log.warning(f"  Batch {batch_num} hit 30s deadline after {batch_dt:.1f}s; skipping")
                continue

            # Stream this batch's bars into DataFrames + cache RIGHT NOW.
            # Older code accumulated all batches' Pydantic Bar objects in a
            # bars_data dict, then post-processed at the end. With ~41K bars
            # per batch × ~400B per Bar ≈ 16 MB per batch, and the container
            # has ~440MB available headroom (1GB total minus Postgres +
            # uvicorn + cron); accumulating 6+ batches pushed past the limit
            # and manifested as the urllib3-timeout-disguised-as-DNS-error
            # hang. Streaming per batch caps live memory at one batch's
            # worth regardless of universe size.
            batch_rows = _process_batch_bars(
                batch_bars.data, result, start, end, use_cache,
            )
            del batch_bars  # release alpaca-py's BarSet wrapper immediately
            cumulative_rows += batch_rows
            total_elapsed = time.monotonic() - loop_t0
            log.info(
                f"  Batch {batch_num}/{total_batches} done in {batch_dt:.1f}s: "
                f"{batch_rows:,} bars "
                f"(cum: {cumulative_rows:,} bars, {total_elapsed:.0f}s)"
            )
        except Exception as e:
            batch_dt = time.monotonic() - batch_t0
            log.warning(f"  Batch {batch_num} failed after {batch_dt:.1f}s: {e}; continuing with next batch")
        finally:
            try:
                client._session.close()
            except Exception:
                pass
            # Force GC so the alpaca-py response wrappers + per-batch
            # DataFrames go away promptly instead of waiting for a
            # threshold-triggered cycle.
            gc.collect()

    # Symbols that Alpaca didn't return data for: emit an empty DataFrame
    # so the caller's `for sym in result` loops still see them.
    for symbol in to_fetch:
        if symbol not in result:
            log.warning(f"No bars returned for {symbol}")
            result[symbol] = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    total_elapsed = time.monotonic() - loop_t0
    non_empty = sum(1 for df in result.values() if not df.empty)
    log.info(
        f"Fetch complete: {non_empty} symbols with bars, {cumulative_rows:,} "
        f"total bars in {total_elapsed:.0f}s"
    )

    return result


def _process_batch_bars(
    batch_data: Dict[str, list],
    result: Dict[str, pd.DataFrame],
    start: date,
    end: date,
    use_cache: bool,
) -> int:
    """Convert one batch's per-symbol Bar lists into DataFrames, save to
    cache, and write into `result`. Returns total bar count for the batch.

    Called from inside the fetch_bars batch loop so that each batch's raw
    response objects can be released immediately rather than accumulated.
    """
    batch_rows = 0
    for sym_alpaca, bars_list in batch_data.items():
        sym = _from_alpaca_symbol(sym_alpaca)
        rows = [
            {
                "timestamp": bar.timestamp,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
            }
            for bar in bars_list
        ]
        batch_rows += len(rows)
        if not rows:
            continue
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
        df = df.set_index("timestamp").sort_index()

        if use_cache:
            _save_cached(sym, df)
            # After cache merge, return the FULL cache window for downstream
            # use, not just the freshly-fetched portion. This way the caller
            # gets all available history including data fetched in earlier
            # calls with different windows.
            full_cached = _load_cached(sym)
            if full_cached is not None:
                result[sym] = full_cached.loc[
                    (full_cached.index >= pd.Timestamp(start)) &
                    (full_cached.index <= pd.Timestamp(end))
                ]
            else:
                result[sym] = df
        else:
            result[sym] = df
    return batch_rows


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