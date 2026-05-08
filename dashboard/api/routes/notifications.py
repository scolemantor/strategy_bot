"""Notifications routes."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from dashboard.api.db import get_session
from dashboard.api.deps import current_user
from dashboard.api.jsonl_backfill import incremental_sync
from dashboard.api.models import Notification, User
from dashboard.api.schemas import NotificationOut, NotificationsResponse

router = APIRouter(prefix="/api/notifications", tags=["notifications"])


@router.get("", response_model=NotificationsResponse)
def list_notifications(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    event_type: str | None = Query(None),
    outcome: str | None = Query(None),
    _: User = Depends(current_user),
    db: Session = Depends(get_session),
) -> NotificationsResponse:
    # Sync any new JSONL events into DB before returning. Cheap when nothing new.
    try:
        incremental_sync(db)
    except Exception:
        pass

    stmt = select(Notification).order_by(Notification.sent_at.desc())
    count_stmt = select(func.count(Notification.id))

    if event_type:
        stmt = stmt.where(Notification.event_type == event_type)
        count_stmt = count_stmt.where(Notification.event_type == event_type)
    if outcome:
        stmt = stmt.where(Notification.outcome == outcome)
        count_stmt = count_stmt.where(Notification.outcome == outcome)

    total = db.execute(count_stmt).scalar() or 0
    rows = db.execute(stmt.limit(limit).offset(offset)).scalars().all()
    return NotificationsResponse(
        total=int(total),
        items=[NotificationOut.model_validate(r) for r in rows],
    )
