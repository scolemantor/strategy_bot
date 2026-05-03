"""Universe loader — get the list of tradeable US equities from Alpaca.

Caches the list to disk for 24h since the universe of listed stocks doesn't
change much day-to-day. Filters to actively-traded common stock by default.
"""
from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import List

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass, AssetExchange, AssetStatus
from alpaca.trading.requests import GetAssetsRequest

from src.config import load_credentials

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
CACHE_TTL_HOURS = 24


def _cache_path() -> Path:
    return CACHE_DIR / "alpaca_universe.json"


def _is_cache_fresh() -> bool:
    p = _cache_path()
    if not p.exists():
        return False
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    return age_hours < CACHE_TTL_HOURS


def get_us_equity_universe(force_refresh: bool = False) -> List[str]:
    """Return the list of tradeable US equity tickers."""
    cache = _cache_path()
    if not force_refresh and _is_cache_fresh():
        log.debug("Using cached Alpaca universe")
        return json.loads(cache.read_text())

    log.info("Fetching tradeable US equity universe from Alpaca")
    creds = load_credentials()
    client = TradingClient(
        api_key=creds.api_key,
        secret_key=creds.secret_key,
        paper=creds.paper,
    )

    req = GetAssetsRequest(status=AssetStatus.ACTIVE, asset_class=AssetClass.US_EQUITY)
    assets = client.get_all_assets(req)

    keep_exchanges = {AssetExchange.NYSE, AssetExchange.NASDAQ, AssetExchange.ARCA, AssetExchange.BATS}
    tickers = []
    for a in assets:
        if not a.tradable:
            continue
        if a.exchange not in keep_exchanges:
            continue
        sym = a.symbol
        if not sym or "/" in sym or "." in sym:
            continue
        tickers.append(sym)

    tickers = sorted(set(tickers))
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(tickers))
    log.info(f"Universe: {len(tickers)} tradeable US equity tickers (cached for 24h)")
    return tickers
SP500_CACHE_TTL_HOURS = 168  # weekly refresh — S&P 500 membership changes rarely


def _sp500_cache_path() -> Path:
    return CACHE_DIR / "sp500_universe.json"


def _is_sp500_cache_fresh() -> bool:
    p = _sp500_cache_path()
    if not p.exists():
        return False
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    return age_hours < SP500_CACHE_TTL_HOURS


def get_sp500_universe(force_refresh: bool = False) -> List[str]:
    """Return current S&P 500 constituent tickers from Wikipedia.

    Used by earnings-driven scanners where we need real operating companies
    that actually report quarterly earnings. ETFs, warrants, units, and
    leveraged products in the broader Alpaca universe have no earnings data
    and pollute results.

    Cached weekly (membership changes rarely).
    """
    import pandas as pd

    cache = _sp500_cache_path()
    if not force_refresh and _is_sp500_cache_fresh():
        log.debug("Using cached S&P 500 universe")
        return json.loads(cache.read_text())

    log.info("Fetching S&P 500 constituents from Wikipedia")
    import requests as _requests
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        # Wikipedia rejects the default urllib UA. Fetch with requests (real UA),
        # then hand the raw HTML to pandas.
        resp = _requests.get(
            url,
            headers={
                "User-Agent": "strategy_bot/1.0 (+https://github.com/scolemantor/strategy_bot)"
            },
            timeout=30,
        )
        resp.raise_for_status()
        from io import StringIO
        tables = pd.read_html(StringIO(resp.text))
        df = tables[0]  # first table is the constituents list
    except Exception as e:
        log.exception("Failed to fetch S&P 500 list")
        # Fallback: if cache exists at all (even stale), use it
        if cache.exists():
            log.warning("Using stale S&P 500 cache after fetch failure")
            return json.loads(cache.read_text())
        raise RuntimeError(f"Could not fetch S&P 500 universe: {e}")

    # Wikipedia uses 'Symbol' column. Sometimes capitalization varies.
    symbol_col = None
    for c in df.columns:
        if str(c).lower() == "symbol":
            symbol_col = c
            break
    if symbol_col is None:
        raise RuntimeError(f"No Symbol column in S&P 500 table; columns were: {list(df.columns)}")

    tickers = df[symbol_col].astype(str).tolist()

    # Wikipedia uses BRK.B for class B shares; Alpaca/yfinance use BRK-B.
    # Same for BF.B -> BF-B. Standardize on the dash form (yfinance convention).
    tickers = [t.replace(".", "-").strip().upper() for t in tickers]
    tickers = [t for t in tickers if t and not t.startswith("NAN")]

    tickers = sorted(set(tickers))
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(tickers))
    log.info(f"S&P 500 universe: {len(tickers)} tickers (cached for {SP500_CACHE_TTL_HOURS}h)")
    return tickers
SP1500_CACHE_TTL_HOURS = 168  # weekly, same as S&P 500


def _sp1500_cache_path() -> Path:
    return CACHE_DIR / "sp1500_universe.json"


def _is_sp1500_cache_fresh() -> bool:
    p = _sp1500_cache_path()
    if not p.exists():
        return False
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    return age_hours < SP1500_CACHE_TTL_HOURS


def get_sp1500_universe(force_refresh: bool = False) -> List[str]:
    """Return current S&P 1500 constituent tickers (S&P 500 + S&P 400 MidCap + S&P 600 SmallCap).

    Used by scanners that want broad US equity coverage without the noise of
    OTC / warrants / units / leveraged products in the full Alpaca universe.

    Source: Wikipedia. We pull all three constituent lists and dedupe.
    Cached weekly.
    """
    import pandas as pd
    import requests as _requests
    from io import StringIO

    cache = _sp1500_cache_path()
    if not force_refresh and _is_sp1500_cache_fresh():
        log.debug("Using cached S&P 1500 universe")
        return json.loads(cache.read_text())

    log.info("Fetching S&P 1500 constituents from Wikipedia (3 indices)")

    sources = {
        "S&P 500":     "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        "S&P 400":     "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies",
        "S&P 600":     "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies",
    }

    all_tickers: set = set()

    for name, url in sources.items():
        try:
            resp = _requests.get(
                url,
                headers={
                    "User-Agent": "strategy_bot/1.0 (+https://github.com/scolemantor/strategy_bot)"
                },
                timeout=30,
            )
            resp.raise_for_status()
            tables = pd.read_html(StringIO(resp.text))
        except Exception as e:
            log.warning(f"  Failed to fetch {name}: {e}")
            continue

        # First table is the constituents list for all three index pages
        df = tables[0]
        symbol_col = None
        for c in df.columns:
            if str(c).lower() == "symbol":
                symbol_col = c
                break
        if symbol_col is None:
            log.warning(f"  No Symbol column in {name}; columns: {list(df.columns)}")
            continue

        tickers = df[symbol_col].astype(str).tolist()
        # Wikipedia uses BRK.B; yfinance/Alpaca use BRK-B. Normalize.
        tickers = [t.replace(".", "-").strip().upper() for t in tickers]
        tickers = [t for t in tickers if t and not t.startswith("NAN")]

        log.info(f"  {name}: {len(tickers)} tickers")
        all_tickers.update(tickers)

    if not all_tickers:
        # Fallback to stale cache if exists
        if cache.exists():
            log.warning("All Wikipedia fetches failed; using stale cache")
            return json.loads(cache.read_text())
        raise RuntimeError("Could not fetch S&P 1500 universe and no cache available")

    sorted_tickers = sorted(all_tickers)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(sorted_tickers))
    log.info(f"S&P 1500 universe: {len(sorted_tickers)} tickers (cached for {SP1500_CACHE_TTL_HOURS}h)")
    return sorted_tickers