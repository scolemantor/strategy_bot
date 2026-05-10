"""Watchlist routes: legacy digest + Phase 8a CRUD on entries.

Endpoint summary:

  GET /api/watchlist
      Legacy — returns watchlist_for_date(latest) using the digest CSV.
      Frontend Watchlist.tsx still consumes this shape until 8c lands.

  GET /api/watchlist/digest?date=YYYY-MM-DD
      Legacy — historical digest for a specific date.

  GET /api/watchlist/entries
      Phase 8a NEW — returns full entries with extended schema (tier,
      position_size, entry_price, stop_loss, target_price, notes,
      auto_added, added_at, last_modified) plus latest_technicals
      payload per ticker if a technical_overlay scan has produced data.

  POST /api/watchlist/entries
      Phase 8a NEW — add ticker. 409 if already exists.

  DELETE /api/watchlist/entries/{ticker}
      Phase 8a NEW — remove ticker. 404 if not found.

  PUT /api/watchlist/entries/{ticker}
      Phase 8a NEW — partial update of fields. 404 if not found.

All Phase 8a write endpoints:
  - hold scanners.watchlist_lock (POSIX fcntl) for the read-modify-write
  - append one JSONL line to logs/watchlist_changes.log via
    scanners.watchlist_audit (with user_agent header from request)
"""
from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from dashboard.api.data_loader import latest_scan_date, watchlist_for_date
from dashboard.api.deps import current_user
from dashboard.api.models import User
from dashboard.api.schemas import (
    WatchlistAddRequest, WatchlistEntriesResponse, WatchlistEntry,
    WatchlistUpdateRequest,
)
from scanners.watchlist import (
    add_entry, read_all_entries, remove_entry, update_entry,
)

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])

TECHNICAL_DETAIL_DIR = Path("data_cache/technical")

log = logging.getLogger(__name__)


# --- Legacy (Phase 4d/7.5) endpoints — preserved until 8c frontend cuts over ---

@router.get("")
def get_watchlist(_: User = Depends(current_user)) -> dict:
    target = latest_scan_date()
    if target is None:
        return {"date": None, "members": []}
    return watchlist_for_date(target)


@router.get("/digest")
def get_watchlist_digest(
    target_date: date = Query(..., alias="date"),
    _: User = Depends(current_user),
) -> dict:
    return watchlist_for_date(target_date)


# --- Phase 8a entries endpoints ---

def _load_latest_technicals(ticker: str) -> Optional[dict]:
    """Best-effort load of the per-ticker technical breakdown JSON.
    Returns None if the file doesn't exist or fails to parse — never raises."""
    path = TECHNICAL_DETAIL_DIR / f"{ticker}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.debug(f"failed to load {path}: {e}")
        return None


def _scan_freshness() -> Optional[str]:
    """Mtime of the most recently-modified per-ticker JSON. Used as a
    'last technical scan' freshness indicator in the entries response."""
    if not TECHNICAL_DETAIL_DIR.exists():
        return None
    try:
        from datetime import datetime, timezone
        latest = max(
            (p.stat().st_mtime for p in TECHNICAL_DETAIL_DIR.glob("*.json")),
            default=None,
        )
        if latest is None:
            return None
        return datetime.fromtimestamp(latest, tz=timezone.utc).isoformat()
    except Exception:
        return None


@router.get("/entries", response_model=WatchlistEntriesResponse)
def get_watchlist_entries(_: User = Depends(current_user)) -> WatchlistEntriesResponse:
    raw = read_all_entries()
    entries = []
    for r in raw:
        r = dict(r)
        r["latest_technicals"] = _load_latest_technicals(r["ticker"])
        entries.append(WatchlistEntry(**r))
    return WatchlistEntriesResponse(
        entries=entries,
        last_technical_scan=_scan_freshness(),
    )


@router.post(
    "/entries",
    response_model=WatchlistEntry,
    status_code=status.HTTP_201_CREATED,
)
def post_watchlist_entry(
    payload: WatchlistAddRequest,
    request: Request,
    _: User = Depends(current_user),
) -> WatchlistEntry:
    user_agent = request.headers.get("user-agent")
    success, before, after = add_entry(
        ticker=payload.ticker,
        source=payload.source,
        user_agent=user_agent,
        reason=payload.reason,
        tier=payload.tier,
        notes=payload.notes,
        category=payload.category,
        position_size=payload.position_size,
        entry_price=payload.entry_price,
        stop_loss=payload.stop_loss,
        target_price=payload.target_price,
    )
    if not success:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"{payload.ticker.upper()} already on watchlist",
        )
    # Hydrate response with full read shape (defaults applied + technicals)
    return _entry_to_response(after, payload.ticker.upper())


@router.delete("/entries/{ticker}", status_code=status.HTTP_200_OK)
def delete_watchlist_entry(
    ticker: str,
    request: Request,
    source: str = Query("dashboard"),
    _: User = Depends(current_user),
) -> dict:
    user_agent = request.headers.get("user-agent")
    success, before = remove_entry(
        ticker=ticker, source=source, user_agent=user_agent,
    )
    if not success:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"{ticker.upper()} not on watchlist",
        )
    return {"removed": True, "ticker": ticker.upper(), "before_state": before}


@router.put("/entries/{ticker}", response_model=WatchlistEntry)
def put_watchlist_entry(
    ticker: str,
    payload: WatchlistUpdateRequest,
    request: Request,
    source: str = Query("dashboard"),
    _: User = Depends(current_user),
) -> WatchlistEntry:
    user_agent = request.headers.get("user-agent")
    fields = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not fields:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "no updatable fields in request body",
        )
    success, before, after = update_entry(
        ticker=ticker, fields=fields, source=source, user_agent=user_agent,
    )
    if not success:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"{ticker.upper()} not on watchlist",
        )
    return _entry_to_response(after, ticker.upper())


def _entry_to_response(after: dict, ticker: str) -> WatchlistEntry:
    """Hydrate a raw `after` dict from add_entry/update_entry with the
    full read shape (defaults applied, latest_technicals attached)."""
    # Find the ticker in the freshly-read entries list to get defaults
    # applied via PHASE_8A_DEFAULTS and date-field fallbacks.
    for r in read_all_entries():
        if r["ticker"] == ticker:
            r = dict(r)
            r["latest_technicals"] = _load_latest_technicals(ticker)
            return WatchlistEntry(**r)
    # Fallback: build from raw `after` (shouldn't happen since we just wrote)
    return WatchlistEntry(ticker=ticker, **{
        k: v for k, v in after.items()
        if k in WatchlistEntry.model_fields
    })
