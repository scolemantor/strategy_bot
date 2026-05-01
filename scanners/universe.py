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