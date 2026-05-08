"""Watchlist routes: latest digest + historical."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query

from dashboard.api.data_loader import latest_scan_date, watchlist_for_date
from dashboard.api.deps import current_user
from dashboard.api.models import User

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


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
