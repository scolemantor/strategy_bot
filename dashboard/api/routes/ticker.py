"""Ticker detail routes.

GET /api/ticker/{symbol}     — cached read: yfinance fundamentals + reverse
                                index + recent scanner signals
POST /api/ticker/{symbol}/refresh — re-fetches yfinance fundamentals,
                                    overwrites cache, returns same shape
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from fastapi import APIRouter, Depends

from dashboard.api.data_loader import (
    load_ticker_index, scanner_csvs_with_ticker,
)
from dashboard.api.deps import current_user
from dashboard.api.models import User

router = APIRouter(prefix="/api/ticker", tags=["ticker"])

FUNDAMENTALS_CACHE_DIR = Path("data_cache/yfinance_fundamentals")

log = logging.getLogger(__name__)


def _read_fundamentals(symbol: str) -> dict:
    path = FUNDAMENTALS_CACHE_DIR / f"{symbol.upper()}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _build_response(symbol: str) -> dict:
    sym = symbol.upper()
    fund = _read_fundamentals(sym)

    meta = {
        "symbol": sym,
        "name": fund.get("name"),
        "sector": fund.get("sector"),
        "industry": fund.get("industry"),
        "market_cap": fund.get("market_cap"),
        "last_updated": fund.get("fetched_at"),
    }
    fundamentals = {
        k: fund.get(k) for k in (
            "pe_trailing", "pb", "ev_ebitda", "debt_equity", "fcf", "last_close",
        )
    }

    # Scanner history from reverse index (commit 3 wrote this)
    idx = load_ticker_index(sym) or {}
    scanner_history: List[dict] = []
    for h in idx.get("history", []):
        scanner_history.append({
            "date": h.get("date", ""),
            "scanners": h.get("scanners", []),
            "composite_score": h.get("composite_score"),
        })

    # Recent signals: per-scanner CSV rows from last 7 days. The reverse
    # index doesn't carry per-scanner reason text, so we still need the
    # CSVs for human-readable summaries.
    recent_signals: List[dict] = []
    for d, scanner_name, row in scanner_csvs_with_ticker(sym, lookback_days=7):
        reason = row.get("reason") or row.get("scanner_reason") or ""
        recent_signals.append({
            "date": d.isoformat(),
            "scanner": scanner_name,
            "summary": str(reason)[:240],
        })

    return {
        "meta": meta,
        "fundamentals": fundamentals,
        "scanner_history": scanner_history,
        "recent_signals": recent_signals,
        "cached_at": fund.get("fetched_at"),
    }


@router.get("/{symbol}")
def get_ticker(symbol: str, _: User = Depends(current_user)) -> dict:
    return _build_response(symbol)


@router.post("/{symbol}/refresh")
def refresh_ticker(symbol: str, _: User = Depends(current_user)) -> dict:
    """Hit live yfinance and overwrite the fundamentals cache for this ticker."""
    sym = symbol.upper()
    try:
        import yfinance as yf
        info = yf.Ticker(sym).info or {}
    except Exception as e:
        log.warning(f"refresh: yfinance fetch failed for {sym}: {e}")
        return _build_response(sym)

    if info:
        fundamentals = {
            "name": info.get("longName") or info.get("shortName"),
            "market_cap": info.get("marketCap"),
            "pe_trailing": info.get("trailingPE"),
            "pb": info.get("priceToBook"),
            "ev_ebitda": info.get("enterpriseToEbitda"),
            "debt_equity": info.get("debtToEquity"),
            "fcf": info.get("freeCashflow"),
            "last_close": info.get("regularMarketPrice") or info.get("previousClose"),
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        FUNDAMENTALS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = FUNDAMENTALS_CACHE_DIR / f"{sym}.json"
        path.write_text(json.dumps(fundamentals), encoding="utf-8")

    return _build_response(sym)
