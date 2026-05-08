"""In-process callback that PushoverDispatcher invokes for every alert
event (dispatched, failed, suppressed, test_mode). Translates the alerting
event payload into a Notification row and inserts it via the dashboard's
SQLAlchemy session.

Cron jobs don't register this callback (their PushoverDispatcher is
instantiated in src/alerting/setup.py without notifications_writer set);
their alerts reach the DB via the JSONL backfill / incremental sync in
jsonl_backfill.py instead. Keeps the cron pipeline DB-free.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from dashboard.api.db import get_sessionmaker
from dashboard.api.models import Notification

log = logging.getLogger(__name__)


# Map alerting event_type → notifications.outcome enum
EVENT_TO_OUTCOME = {
    "alert_dispatched": "dispatched",
    "alert_dispatch_failed": "failed",
    "alert_suppressed": "suppressed",
    "alert_test_mode": "test_mode",
}


def write_notification(event_type: str, message: str, payload: dict) -> None:
    """Insert a single notifications row. Never raises — alerting must not
    break on DB problems. Unknown event_types are dropped silently."""
    outcome = EVENT_TO_OUTCOME.get(event_type)
    if outcome is None:
        return  # not an alerting event we track

    alert = (payload or {}).get("alert", {}) or {}
    suppression_reason = (payload or {}).get("reason") if outcome == "suppressed" else None

    SessionLocal = get_sessionmaker()
    db = SessionLocal()
    try:
        # Parse timestamp — alerts.to_dict() emits ISO8601; fall back to now.
        sent_at = _parse_ts(alert.get("timestamp")) or datetime.now(timezone.utc)
        row = Notification(
            sent_at=sent_at,
            event_type=event_type,
            title=(alert.get("title") or "")[:255] or None,
            message=alert.get("body") or message,
            priority=int(alert.get("priority", 0) or 0),
            outcome=outcome,
            suppression_reason=suppression_reason,
        )
        db.add(row)
        db.commit()
    except Exception as e:
        log.warning(f"notifications_writer: insert failed: {e}")
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        db.close()


def _parse_ts(s) -> datetime | None:
    if not s or not isinstance(s, str):
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None
