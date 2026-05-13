"""Unusual Whales API client (Phase 4g.1).

First paid-data integration. Wraps the UW REST API with:
  - Bearer token auth read from UNUSUAL_WHALES_API_TOKEN env var
  - Module-level rate limiting (matches the scanners/edgar_client.py pattern;
    sync requests, no async refactor needed for parity with existing scanners)
  - Disk cache keyed by (endpoint, params, run_date) so re-running a scan
    on the same calendar day hits cache and doesn't burn rate budget
  - Exponential backoff on 429 responses (1s, 2s, 4s, 8s, then fail)
  - Helpful error messages on 401/404 that NEVER leak the token

Token security: the token is read from os.environ at every request; never
bound to a long-lived module-level variable, never logged at any level,
never included in exception messages. Verify with:
  grep -r 'UNUSUAL_WHALES_API_TOKEN' src/ scanners/ tests/
The only matches should be os.environ.get() calls and the env var name
inside string literals (constants/error messages — the var NAME is fine
to log, the var VALUE is not).

Endpoints implemented in this commit:
  - get_flow_alerts() — used by scanners/options_unusual.py

Endpoints stubbed (raise NotImplementedError with phase reference):
  - get_ticker_flow      → Phase 4g.5 (ma_rumors scanner)
  - get_ticker_gex       → Phase 8d (dashboard gamma exposure widget)
  - get_congressional_trades → Phase 4g.1b (Quiver replacement)
  - get_dark_pool_prints → Phase 4g.5 (ma_rumors scanner)
  - get_market_tide      → future macro indicator
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger(__name__)

API_TOKEN_ENV = "UNUSUAL_WHALES_API_TOKEN"
BASE_URL = "https://api.unusualwhales.com"
USER_AGENT = "OakStrategyBot research"
REQUEST_TIMEOUT = 30
CACHE_DIR = Path("data_cache/unusual_whales")

# Conservative rate limit until UW publishes the basic-tier limit. Assume
# 100 req/min and back off to 50 req/min (1.2s between requests) so a
# burst of scanner activity stays well under any sensible cap.
RATE_LIMIT_DELAY = 1.2

# 429 retry backoff schedule (seconds between attempts). After the last
# value, give up and raise.
RETRY_BACKOFF_SCHEDULE = [1, 2, 4, 8]

# Per-endpoint cache TTLs (seconds). Flow alerts move fast; stock-info
# endpoints are stable for the trading day.
TTL_FLOW_ALERTS_SEC = 60
TTL_HISTORICAL_SEC = 300
TTL_STOCK_INFO_SEC = 3600

# Module-level rate limit clock. Mirrors scanners/edgar_client.py pattern.
_last_request_time = 0.0


def _token() -> str:
    """Read the API token from env. Raise with a helpful message if unset.

    The error message names the env var (safe — that's its identifier) but
    NEVER includes the value (would be empty here anyway, but be explicit
    about the convention)."""
    token = os.environ.get(API_TOKEN_ENV)
    if not token:
        raise RuntimeError(
            f"{API_TOKEN_ENV} env var not set. Sign up at "
            f"https://unusualwhales.com/api and add the key to .env."
        )
    return token


def _headers() -> Dict[str, str]:
    """Build request headers fresh for each call. Token is constructed at
    request time and lives only in the dict scope — no long-lived binding."""
    return {
        "Authorization": f"Bearer {_token()}",
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }


def _rate_limit() -> None:
    """Sleep if needed to maintain RATE_LIMIT_DELAY between requests."""
    global _last_request_time
    now = time.time()
    delta = now - _last_request_time
    if delta < RATE_LIMIT_DELAY:
        time.sleep(RATE_LIMIT_DELAY - delta)
    _last_request_time = time.time()


def _params_hash(params: Dict[str, Any]) -> str:
    """Stable short hash of params dict for the cache filename."""
    canonical = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]


def _cache_path(
    endpoint_slug: str,
    params: Dict[str, Any],
    run_date: Optional[date],
) -> Path:
    """Cache file path. run_date is part of the key so the day rollover
    cleanly invalidates yesterday's cache without needing a TTL sweep."""
    date_key = (run_date or datetime.now(timezone.utc).date()).isoformat()
    h = _params_hash(params)
    return CACHE_DIR / f"{endpoint_slug}_{date_key}_{h}.json"


def _read_cache(path: Path, ttl_sec: int) -> Optional[Any]:
    """Return cached value if file exists and is younger than ttl_sec."""
    if not path.exists():
        return None
    age_sec = time.time() - path.stat().st_mtime
    if age_sec >= ttl_sec:
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"UW cache read failed at {path}: {e}; ignoring cache")
        return None


def _write_cache(path: Path, data: Any) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data), encoding="utf-8")
    except Exception as e:
        log.warning(f"UW cache write failed at {path}: {e}; continuing")


def _get(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    ttl_sec: int = TTL_HISTORICAL_SEC,
    run_date: Optional[date] = None,
) -> Any:
    """Rate-limited GET to a UW endpoint with caching + 429 backoff.

    `endpoint` is the path segment after BASE_URL, with leading slash.
    Returns the parsed JSON body. Raises requests.HTTPError on persistent
    non-2xx (after backoff for 429). Error messages NEVER include the
    token."""
    params = dict(params or {})
    endpoint_slug = endpoint.strip("/").replace("/", "__")
    cache_path = _cache_path(endpoint_slug, params, run_date)

    cached = _read_cache(cache_path, ttl_sec)
    if cached is not None:
        log.debug(f"UW cache hit: {endpoint} ({cache_path.name})")
        return cached

    url = f"{BASE_URL}{endpoint}"
    last_exc: Optional[Exception] = None
    for attempt, backoff_after in enumerate(RETRY_BACKOFF_SCHEDULE + [None]):
        _rate_limit()
        log.info(f"UW GET {endpoint} params={params} (attempt {attempt + 1})")
        try:
            resp = requests.get(
                url,
                params=params,
                headers=_headers(),
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as e:
            log.warning(f"UW network error on {endpoint}: {e}")
            last_exc = e
            if backoff_after is None:
                raise
            time.sleep(backoff_after)
            continue

        # Surface response rate-limit headers so we can tune
        # RATE_LIMIT_DELAY once we have real data on UW's actual cap.
        rl_remaining = resp.headers.get("X-RateLimit-Remaining")
        rl_limit = resp.headers.get("X-RateLimit-Limit")
        if rl_remaining or rl_limit:
            log.debug(
                f"UW rate-limit headers: remaining={rl_remaining} limit={rl_limit}"
            )

        if resp.status_code == 429:
            if backoff_after is None:
                log.error(
                    f"UW rate limit on {endpoint} after "
                    f"{len(RETRY_BACKOFF_SCHEDULE)} retries; giving up"
                )
                resp.raise_for_status()
            retry_after = resp.headers.get("Retry-After")
            wait = int(retry_after) if retry_after and retry_after.isdigit() else backoff_after
            log.warning(
                f"UW 429 on {endpoint}; backing off {wait}s "
                f"(attempt {attempt + 1}/{len(RETRY_BACKOFF_SCHEDULE)})"
            )
            time.sleep(wait)
            continue

        if resp.status_code == 401:
            # Don't echo the response body — UW could include the
            # token-prefix or other auth-context detail we don't want
            # in logs. Just say auth failed.
            raise RuntimeError(
                f"UW auth failed on {endpoint} (HTTP 401). Check that "
                f"{API_TOKEN_ENV} is set correctly and the subscription "
                f"covers this endpoint."
            )

        if resp.status_code == 404:
            log.warning(f"UW 404 on {endpoint}; returning empty result")
            return [] if endpoint.endswith("recent") or "alerts" in endpoint else {}

        resp.raise_for_status()
        data = resp.json()
        _write_cache(cache_path, data)
        return data

    # Unreachable in practice; keeps mypy/static-checkers happy
    if last_exc:
        raise last_exc
    raise RuntimeError(f"UW request failed for {endpoint} (no exception)")


# --- Public endpoint wrappers ---

def get_flow_alerts(
    *,
    limit: int = 100,
    min_premium: int = 100_000,
    lookback_hours: int = 24,
    run_date: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Recent unusual options flow above premium threshold (global feed).

    Endpoint: GET /api/option-trades/flow-alerts

    Returns a list of alert dicts. Expected per-alert fields (verified
    against UW's published response shape — caller should treat any
    individual field as optional and degrade gracefully):
      - ticker (str)
      - option_symbol / option_chain (str, OCC format)
      - option_type / type ("call" | "put")
      - strike (number)
      - expiry / expires (date string YYYY-MM-DD)
      - premium / total_premium (number, dollars)
      - volume (number)
      - open_interest (number)
      - underlying_price / spot (number, underlying spot at trade time)
      - trade_id / id (str, for client-side dedup)
      - created_at (ISO timestamp)

    Cached for TTL_FLOW_ALERTS_SEC (60s). Re-running within 60s hits
    cache; longer than that re-fetches even within the same run_date."""
    params = {
        "limit": limit,
        "min_premium": min_premium,
        "lookback_hours": lookback_hours,
    }
    data = _get(
        "/api/option-trades/flow-alerts",
        params=params,
        ttl_sec=TTL_FLOW_ALERTS_SEC,
        run_date=run_date,
    )
    # UW typically returns either a bare list or {"data": [...]} envelope;
    # normalize to a list to keep callers simple.
    if isinstance(data, dict):
        for k in ("data", "alerts", "results"):
            if k in data and isinstance(data[k], list):
                return data[k]
        log.warning(
            f"UW flow-alerts returned unexpected dict shape "
            f"(keys={list(data.keys())[:5]}); returning empty list"
        )
        return []
    if isinstance(data, list):
        return data
    log.warning(
        f"UW flow-alerts returned unexpected type {type(data).__name__}; "
        f"returning empty list"
    )
    return []


# --- Stubs for endpoints whose consumer scanners aren't built yet ---
# Implement on demand when the consuming scanner is designed. Premature
# implementation risks guessing wrong on UW's response shape and shipping
# untested code.

def get_ticker_flow(ticker: str, lookback_hours: int = 24) -> List[Dict[str, Any]]:
    """Recent options flow for a specific ticker.

    Will be implemented in Phase 4g.5 (ma_rumors scanner).
    See PROJECT_STATE.md.
    """
    raise NotImplementedError(
        "get_ticker_flow not yet implemented. Scheduled for Phase 4g.5 "
        "(ma_rumors scanner). For global options flow, call "
        "get_flow_alerts() instead."
    )


def get_ticker_gex(ticker: str) -> Dict[str, Any]:
    """Gamma exposure per strike for a specific ticker.

    Will be implemented in Phase 8d (dashboard gamma exposure widget).
    """
    raise NotImplementedError(
        "get_ticker_gex not yet implemented. Scheduled for Phase 8d "
        "(dashboard gamma exposure widget)."
    )


def get_congressional_trades(lookback_days: int = 30) -> List[Dict[str, Any]]:
    """Recent congressional stock trades.

    Will be implemented in Phase 4g.1b (replaces Quiver-backed
    scanners/congressional_trades.py with UW-backed version).
    """
    raise NotImplementedError(
        "get_congressional_trades not yet implemented. Scheduled for "
        "Phase 4g.1b (Quiver-replacement congressional_trades scanner)."
    )


def get_dark_pool_prints(
    limit: int = 50, min_premium: int = 1_000_000,
) -> List[Dict[str, Any]]:
    """Recent dark pool prints above premium threshold.

    Will be implemented in Phase 4g.5 (ma_rumors scanner).
    """
    raise NotImplementedError(
        "get_dark_pool_prints not yet implemented. Scheduled for "
        "Phase 4g.5 (ma_rumors scanner)."
    )


def get_market_tide() -> Dict[str, Any]:
    """Broad market sentiment indicator.

    Future macro indicator — no consuming scanner planned yet.
    """
    raise NotImplementedError(
        "get_market_tide not yet implemented. Future macro indicator; "
        "no consuming scanner planned in current phase roadmap."
    )
