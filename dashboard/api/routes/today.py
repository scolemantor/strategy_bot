"""Today routes: latest master_ranked + conflicts + category_summary."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from dashboard.api.data_loader import (
    category_summary_for_date, conflicts_for_date,
    latest_scan_date, master_ranked_to_response,
)
from dashboard.api.deps import current_user
from dashboard.api.models import User

router = APIRouter(prefix="/api/today", tags=["today"])


@router.get("")
def get_today(_: User = Depends(current_user)) -> dict:
    target = latest_scan_date()
    if target is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no scan output available")
    return master_ranked_to_response(target)


@router.get("/conflicts")
def get_today_conflicts(_: User = Depends(current_user)) -> dict:
    target = latest_scan_date()
    if target is None:
        return {"date": None, "conflicts": []}
    return {"date": target.isoformat(), "conflicts": conflicts_for_date(target)}


@router.get("/category-summary")
def get_today_category_summary(_: User = Depends(current_user)) -> dict:
    target = latest_scan_date()
    if target is None:
        return {"date": None, "summary": []}
    return {"date": target.isoformat(), "summary": category_summary_for_date(target)}
