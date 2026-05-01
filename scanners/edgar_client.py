"""SEC EDGAR HTTP client with rate limiting and CIK->ticker resolution.

SEC requires a User-Agent identifying who is making requests, and asks that
clients stay under 10 requests per second. We back off to 5/s to be polite.

The company_tickers.json file maps CIK -> ticker. We cache it locally for 24h
so we don't re-download a 5MB file on every scan.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import requests

log = logging.getLogger(__name__)

EDGAR_BASE = "https://www.sec.gov"
EDGAR_DATA_BASE = "https://data.sec.gov"
CACHE_DIR = Path("data_cache")
TICKER_MAP_TTL_HOURS = 24
RATE_LIMIT_DELAY = 0.21  # seconds between requests, ~5 req/s


def _user_agent() -> str:
    """Return a User-Agent string. Reads from env or uses a default."""
    contact = os.getenv("SEC_USER_AGENT_CONTACT", "research@example.com")
    return f"OakStrategyBot {contact}"


def _headers() -> Dict[str, str]:
    return {
        "User-Agent": _user_agent(),
        "Accept-Encoding": "gzip, deflate",
    }


_last_request_time = 0.0


def _rate_limit():
    """Sleep if needed to stay under the rate limit."""
    global _last_request_time
    now = time.time()
    delta = now - _last_request_time
    if delta < RATE_LIMIT_DELAY:
        time.sleep(RATE_LIMIT_DELAY - delta)
    _last_request_time = time.time()


def edgar_get(url: str, timeout: int = 30) -> requests.Response:
    """Rate-limited GET to an EDGAR URL. Raises on HTTP errors."""
    _rate_limit()
    log.debug(f"EDGAR GET {url}")
    resp = requests.get(url, headers=_headers(), timeout=timeout)
    resp.raise_for_status()
    return resp


def _ticker_cache_path() -> Path:
    return CACHE_DIR / "sec_company_tickers.json"


def _is_ticker_cache_fresh() -> bool:
    p = _ticker_cache_path()
    if not p.exists():
        return False
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    return age_hours < TICKER_MAP_TTL_HOURS


def load_cik_to_ticker() -> Dict[str, str]:
    """Return a CIK (zero-padded 10-digit) -> ticker mapping.

    Refreshes the local cache if older than 24h. The SEC file maps each row
    by integer index, with cik_str/ticker/title fields.
    """
    cache_path = _ticker_cache_path()

    if not _is_ticker_cache_fresh():
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        log.info("Refreshing SEC company_tickers.json cache")
        url = f"{EDGAR_BASE}/files/company_tickers.json"
        resp = edgar_get(url)
        cache_path.write_text(resp.text)
    else:
        log.debug("Using cached SEC company_tickers.json")

    raw = json.loads(cache_path.read_text())
    mapping: Dict[str, str] = {}
    for _, row in raw.items():
        cik = str(row["cik_str"]).zfill(10)
        ticker = row["ticker"]
        mapping[cik] = ticker
    log.info(f"Loaded {len(mapping)} CIK->ticker mappings")
    return mapping


def cik_to_ticker(cik: str, mapping: Optional[Dict[str, str]] = None) -> Optional[str]:
    """Convert a CIK (any format) to a ticker, or None if not found."""
    if mapping is None:
        mapping = load_cik_to_ticker()
    cik_padded = str(cik).strip().zfill(10)
    return mapping.get(cik_padded)