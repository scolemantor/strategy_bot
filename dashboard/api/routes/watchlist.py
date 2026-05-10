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
import subprocess
import sys
from datetime import date, datetime, timezone
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


AUTO_SCAN_LOG_PATH = Path("logs/auto_scan.log")


def _fire_background_technical_scan(ticker: str) -> bool:
    """Phase 8c Issue 2 + four-defense fix: fire a non-blocking background
    subprocess that runs technical_overlay --tickers <ticker>. Frontend
    polls every 60s and the technical breakdown appears on the next
    poll (typically within 30s, allowing for the Anthropic narrator API
    call from Issue 3 which adds 2-10s).

    Four defenses applied:
      1. sys.executable — guarantees the same Python that's running
         uvicorn, no PATH lookup risk.
      2. cwd="/app" — explicit, doesn't rely on inheritance from the
         entrypoint's `cd ${APP_DIR}`.
      3. start_new_session=True — detaches the child into its own
         POSIX process group so SIGTERM to uvicorn (e.g. on container
         restart) doesn't propagate to the in-flight scan.
      4. stdout/stderr -> logs/auto_scan.log — append-mode log gives
         forward visibility on every auto-scan firing. `tail -f` to
         monitor live; previously DEVNULL was hiding silent failures.

    Returns True if subprocess.Popen succeeded (process exists with a PID),
    False if Popen itself raised. Failure does NOT propagate — the
    watchlist add already succeeded; failing the API response over a
    backgroundable scan would be wrong."""
    try:
        AUTO_SCAN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        log_fh = open(AUTO_SCAN_LOG_PATH, "a", encoding="utf-8")
        log_fh.write(
            f"\n=== {datetime.now(timezone.utc).isoformat()} START "
            f"ticker={ticker} ===\n"
        )
        log_fh.flush()

        proc = subprocess.Popen(
            [sys.executable, "scan.py", "run", "technical_overlay", "--tickers", ticker],
            stdout=log_fh,
            stderr=log_fh,
            cwd="/app",
            start_new_session=True,
        )
        log.warning(  # WARNING level so it shows in default-INFO log filters
            f"Auto-scan spawned: ticker={ticker} pid={proc.pid} "
            f"log={AUTO_SCAN_LOG_PATH}"
        )
        return True
    except Exception as e:
        log.warning(
            f"Background technical_overlay --tickers {ticker} failed to spawn: "
            f"{e} (next */15 cron will pick it up)"
        )
        return False


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
    # Phase 8c Issue 2: fire background technical scan for the just-added
    # ticker so its data appears in the dashboard within ~30s instead of
    # waiting up to 15 min for the next */15 cron tick.
    scan_triggered = _fire_background_technical_scan(payload.ticker.upper())

    # Hydrate response with full read shape (defaults applied + technicals)
    response = _entry_to_response(after, payload.ticker.upper())
    # Transient hint to the frontend: True if a scan was kicked off
    # successfully; the dashboard can show a "Scanning..." spinner on
    # the new ticker for ~30s until the next poll picks up the technical
    # data. Always None on GET — only set on POST response.
    response.scan_triggered = scan_triggered
    return response


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
