"""History routes: last 30 days of scan dates + per-date master_ranked."""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, status

from dashboard.api.data_loader import history_summary, master_ranked_to_response
from dashboard.api.deps import current_user
from dashboard.api.models import User

router = APIRouter(prefix="/api/history", tags=["history"])


@router.get("")
def list_history(_: User = Depends(current_user)) -> list[dict]:
    return history_summary(limit=30)


@router.get("/{target_date}")
def get_history_for_date(
    target_date: date, _: User = Depends(current_user),
) -> dict:
    response = master_ranked_to_response(target_date)
    if response["total_count"] == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"no scan output for {target_date.isoformat()}",
        )
    return response
