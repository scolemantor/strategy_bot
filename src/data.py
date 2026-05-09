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

import faulthandler
import logging
import os
import socket
import sys
import threading
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
ALPACA_DATA_HOST = "data.alpaca.markets"


# --- DEBUG INSTRUMENTATION (Phase 7.5 batch-11 investigation) ---
# This block is temporary diagnostics. Remove after root cause identified.
# Captures per-batch process state and dumps all thread stacks if any
# batch hangs >25s, so we can see where the call is actually blocked.
# faulthandler.dump_traceback_later was unreliable due to module path
# issues; threading.Timer + faulthandler.dump_traceback (in-process call)
# works around that.

def _read_socket_count() -> Dict[str, int]:
    """Read /proc/self/net/tcp + tcp6 line counts (each line = one socket)."""
    out = {"tcp": -1, "tcp6": -1}
    for proto in ("tcp", "tcp6"):
        try:
            with open(f"/proc/self/net/{proto}", "r") as f:
                out[proto] = sum(1 for _ in f) - 1  # subtract header
        except Exception:
            pass
    return out


def _read_max_rss_kb() -> int:
    """Best-effort process max RSS in KB. -1 on failure (e.g. Windows)."""
    try:
        import resource  # POSIX-only
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return -1


def _inspect_session_pools(session) -> str:
    """Stringify urllib3 pool state from a requests.Session."""
    try:
        adapter = session.get_adapter(f"https://{ALPACA_DATA_HOST}")
        pm = adapter.poolmanager
        pools = list(pm.pools.keys()) if hasattr(pm, "pools") else []
        return (
            f"adapter={type(adapter).__name__} "
            f"pool_connections={getattr(adapter, '_pool_connections', '?')} "
            f"pool_maxsize={getattr(adapter, '_pool_maxsize', '?')} "
            f"active_pools={len(pools)}"
        )
    except Exception as e:
        return f"inspect_failed:{type(e).__name__}:{e}"


def _dns_probe(host: str) -> str:
    """Quick DNS sanity check at hang-time. Returns IP or error string."""
    try:
        t0 = time.monotonic()
        ip = socket.gethostbyname(host)
        dt = (time.monotonic() - t0) * 1000
        return f"{ip} in {dt:.1f}ms"
    except Exception as e:
        return f"FAILED: {type(e).__name__}: {e}"


def _dump_diagnostic_state(batch_num: int, batch_size: int) -> None:
    """Fires from a threading.Timer at 25s into a hung batch. Dumps
    everything we can reach: memory, sockets, DNS at-this-moment, and
    all Python thread stacks (which should show the worker thread blocked
    in whatever call is actually hanging)."""
    print("", file=sys.stderr, flush=True)
    print(
        f"=== [BATCH {batch_num}] HANG WARNING — 25s without completion "
        f"(symbols={batch_size}) ===",
        file=sys.stderr, flush=True,
    )
    socks = _read_socket_count()
    print(
        f"  sockets: tcp={socks['tcp']} tcp6={socks['tcp6']}  "
        f"max_rss_kb={_read_max_rss_kb()}",
        file=sys.stderr, flush=True,
    )
    print(
        f"  dns probe (live, separate from alpaca-py): {ALPACA_DATA_HOST} -> "
        f"{_dns_probe(ALPACA_DATA_HOST)}",
        file=sys.stderr, flush=True,
    )
    # getaddrinfo specifically (urllib3 uses this, not gethostbyname)
    try:
        t0 = time.monotonic()
        infos = socket.getaddrinfo(ALPACA_DATA_HOST, 443, type=socket.SOCK_STREAM)
        dt = (time.monotonic() - t0) * 1000
        print(
            f"  getaddrinfo (live, urllib3-style): {len(infos)} results in {dt:.1f}ms; "
            f"families={sorted({i[0].name for i in infos})}",
            file=sys.stderr, flush=True,
        )
    except Exception as e:
        print(
            f"  getaddrinfo (live, urllib3-style): FAILED {type(e).__name__}: {e}",
            file=sys.stderr, flush=True,
        )
    print(f"  python threads: {threading.active_count()} active", file=sys.stderr, flush=True)
    print("=== ALL THREAD STACKS ===", file=sys.stderr, flush=True)
    faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
    print("=== END DIAGNOSTIC DUMP ===", file=sys.stderr, flush=True)
    print("", file=sys.stderr, flush=True)


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

        # Fresh Alpaca client per batch. NOTE: this was originally added in
        # commit e35a99b under the (incorrect) hypothesis that urllib3
        # connection-pool reuse was causing the batch-10 hang. That diagnosis
        # was wrong — the real cause was alpaca-py's silent retry-on-429
        # (see `client._retry = 0` below). Per-batch client is kept as
        # defensive hygiene (negligible cost, isolates each batch's pool
        # state) but is NOT what fixes the hang. Cost is one TLS handshake
        # per batch (~100-300ms), trivial vs the 0.5s throttle.
        client = StockHistoricalDataClient(
            api_key=creds.api_key,
            secret_key=creds.secret_key,
        )
        # Disable SDK's internal retry-on-429 — defaults are 3 retries × ~3s wait
        # which appears as a 30s hang. With Algo Trader Plus (10K req/min) we should
        # rarely hit 429; if we do, failing fast is better than silent retry. Our
        # with_deadline wrapper provides hard wall-clock cap as backup.
        client._retry = 0
        apply_default_timeout(client._session, 60)

        # DEBUG (batch-11 investigation): per-batch state snapshot
        socks_before = _read_socket_count()
        pool_state = _inspect_session_pools(client._session)
        log.info(
            f"  Batch {batch_num}/{total_batches}: fetching {len(batch)} symbols "
            f"[sockets tcp={socks_before['tcp']} tcp6={socks_before['tcp6']}; {pool_state}]"
        )
        batch_t0 = time.monotonic()

        # DEBUG (batch-11 investigation): if this batch takes >25s, dump
        # all thread stacks + memory/socket/DNS state to stderr. The timer
        # is cancelled cleanly on success, so successful batches produce
        # no extra output.
        dump_timer = threading.Timer(
            25.0, _dump_diagnostic_state, args=(batch_num, len(batch)),
        )
        dump_timer.daemon = True
        dump_timer.start()

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
            # DEBUG: capture FULL exception chain (tells us what error class is
            # actually being raised — Sean's report mentioned the "DNS error" is
            # a misleading wrapper. We want the unwrapped __cause__ chain.)
            chain = []
            cur = e
            while cur is not None and len(chain) < 10:
                chain.append(f"{type(cur).__name__}: {cur}")
                cur = getattr(cur, "__cause__", None) or getattr(cur, "__context__", None)
                if cur is e:
                    break
            log.warning(
                f"  Batch {batch_num} failed after {batch_dt:.1f}s; continuing.\n"
                f"    exception chain ({len(chain)} levels):\n    "
                + "\n    ".join(chain)
            )
        finally:
            dump_timer.cancel()

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