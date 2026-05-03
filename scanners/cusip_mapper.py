"""CUSIP to ticker resolution via OpenFIGI API.

Used by scanner #6 (thirteen_f_changes) and any future scanner that needs to
translate 13F-style CUSIP identifiers into tradeable tickers.

OpenFIGI is free, no auth required. Without an API key the rate limit is
~25 requests/minute and 10 mapping jobs per request. With a free API key
those go up to ~250/minute and 100 per request.

Cache strategy:
  - Hit OpenFIGI once per CUSIP, then cache forever.
  - CUSIPs are immutable per security. If a security changes (merger, delist,
    rename), it gets a NEW CUSIP. So cache invalidation is a non-problem.

Failure handling:
  - If OpenFIGI returns no match, cache the negative result so we don't retry.
  - If OpenFIGI errors out, return None and don't cache (will retry next run).
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
CUSIP_CACHE_DIR = CACHE_DIR / "cusip_to_ticker"

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
JOBS_PER_REQUEST = 10  # without API key
RATE_LIMIT_DELAY_SEC = 2.5  # ~25 req/min, safe under the 25/min limit

_last_request_time = 0.0


def _rate_limit():
    global _last_request_time
    now = time.time()
    delta = now - _last_request_time
    if delta < RATE_LIMIT_DELAY_SEC:
        time.sleep(RATE_LIMIT_DELAY_SEC - delta)
    _last_request_time = time.time()


def _cache_path(cusip: str) -> Path:
    return CUSIP_CACHE_DIR / f"{cusip}.json"


def load_cached(cusip: str) -> Optional[Dict]:
    """Return cached mapping for a CUSIP. Returns dict or None.

    Cached entries can be either:
      - {"cusip": ..., "ticker": "AAPL", "name": "...", ...} — successful match
      - {"cusip": ..., "ticker": None, "reason": "no_match"} — known negative
    """
    p = _cache_path(cusip)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception as e:
        log.debug(f"Failed to load CUSIP cache for {cusip}: {e}")
        return None


def save_cached(cusip: str, data: Dict) -> None:
    CUSIP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        _cache_path(cusip).write_text(json.dumps(data))
    except Exception as e:
        log.debug(f"Failed to save CUSIP cache for {cusip}: {e}")


def _user_agent() -> str:
    contact = os.getenv("SEC_USER_AGENT_CONTACT", "seanpcoloeman1@gmail.com")
    return f"OakStrategyBot {contact}"


def _resolve_batch(cusips: List[str]) -> Dict[str, Dict]:
    """Send one batch request to OpenFIGI. Returns dict mapping cusip -> result."""
    if not cusips:
        return {}

    api_key = os.getenv("OPENFIGI_API_KEY")
    headers = {
        "Content-Type": "application/json",
        "User-Agent": _user_agent(),
    }
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    jobs = [
        {"idType": "ID_CUSIP", "idValue": c, "exchCode": "US"}
        for c in cusips
    ]

    try:
        _rate_limit()
        r = requests.post(OPENFIGI_URL, json=jobs, headers=headers, timeout=30)
        if r.status_code == 429:
            log.warning("OpenFIGI rate limit hit, sleeping 60s")
            time.sleep(60)
            return _resolve_batch(cusips)  # retry once
        if r.status_code != 200:
            log.warning(f"OpenFIGI returned HTTP {r.status_code}: {r.text[:200]}")
            return {}
        results = r.json()
    except Exception as e:
        log.warning(f"OpenFIGI request failed: {e}")
        return {}

    # Results are positionally aligned with the request jobs
    out: Dict[str, Dict] = {}
    for cusip, result in zip(cusips, results):
        if "data" in result and result["data"]:
            # Take the first US common-stock result
            best = None
            for entry in result["data"]:
                if (entry.get("exchCode") == "US" and
                    entry.get("securityType") == "Common Stock"):
                    best = entry
                    break
            if best is None:
                # Fallback: take the first US entry of any kind
                for entry in result["data"]:
                    if entry.get("exchCode") == "US":
                        best = entry
                        break
            if best is None:
                # Last-resort: take whatever the first entry is
                best = result["data"][0]

            out[cusip] = {
                "cusip": cusip,
                "ticker": best.get("ticker"),
                "name": best.get("name"),
                "exch_code": best.get("exchCode"),
                "security_type": best.get("securityType"),
                "fetched_at": datetime.now().isoformat(),
            }
        else:
            # No match found — cache as negative
            warning = result.get("warning") or result.get("error") or "no_match"
            out[cusip] = {
                "cusip": cusip,
                "ticker": None,
                "reason": warning,
                "fetched_at": datetime.now().isoformat(),
            }

    return out


def resolve_cusips(cusips: List[str], use_cache: bool = True) -> Dict[str, Optional[str]]:
    """Resolve a list of CUSIPs to tickers.

    Returns dict of {cusip: ticker_or_None}. Hits cache first, then OpenFIGI
    in batches for any cache misses. Saves all results to cache.
    """
    cusips = list(set(c for c in cusips if c and len(c) >= 8))  # dedupe + sanity-filter

    result: Dict[str, Optional[str]] = {}
    misses: List[str] = []

    if use_cache:
        for c in cusips:
            cached = load_cached(c)
            if cached is not None:
                result[c] = cached.get("ticker")
            else:
                misses.append(c)
    else:
        misses = cusips

    if not misses:
        return result

    log.info(
        f"CUSIP resolution: {len(cusips) - len(misses)} cached, "
        f"{len(misses)} need OpenFIGI lookup "
        f"(~{len(misses) * RATE_LIMIT_DELAY_SEC / 60:.1f} min)"
    )

    # Batch the misses
    for i in range(0, len(misses), JOBS_PER_REQUEST):
        batch = misses[i:i + JOBS_PER_REQUEST]
        batch_results = _resolve_batch(batch)

        for cusip in batch:
            data = batch_results.get(cusip)
            if data is None:
                # OpenFIGI errored — don't cache, leave for next run
                result[cusip] = None
            else:
                save_cached(cusip, data)
                result[cusip] = data.get("ticker")

        if (i + JOBS_PER_REQUEST) < len(misses):
            log.debug(f"  Resolved {i + JOBS_PER_REQUEST}/{len(misses)} so far")

    return result