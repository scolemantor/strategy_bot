"""Backfill notifications table from logs/strategy_bot_*.jsonl files.

Two entry points:
  - backfill_if_empty(db) — called at FastAPI startup. If notifications
    table has zero rows, walks every JSONL file in chronological order and
    inserts a row for each alert_dispatched / alert_dispatch_failed /
    alert_suppressed / alert_test_mode event.
  - incremental_sync(db) — called at the top of each /api/notifications
    request. Finds the latest sent_at in DB and inserts any newer JSONL
    events. Cheap when there's nothing new (tail-only scan).

JSONL format (from src/logging_v2):
  {"timestamp": "...", "event_type": "...", "level": "...", "message": "...",
   "payload": {"alert": {...}, "reason": "..."}}

Cron jobs run as separate processes from the dashboard. They write to JSONL
via JsonLinesLogger; they do NOT touch the DB. The dashboard mirrors JSONL
into the DB so the UI can query/filter quickly.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from dashboard.api.models import Notification
from dashboard.api.notifications_writer import EVENT_TO_OUTCOME, _parse_ts

log = logging.getLogger(__name__)

LOG_DIR = Path("logs")


def _iter_jsonl_events(after: Optional[datetime] = None) -> Iterable[dict]:
    """Yield dicts from logs/strategy_bot_*.jsonl in chronological order.
    Skips entries with timestamp <= `after` if provided."""
    if not LOG_DIR.exists():
        return
    files = sorted(LOG_DIR.glob("strategy_bot_*.jsonl"))
    for path in files:
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if after is not None:
                    ts = _parse_ts(entry.get("timestamp"))
                    if ts is None or ts <= after:
                        continue
                yield entry
        except Exception as e:
            log.warning(f"jsonl_backfill: failed to read {path}: {e}")


def _entry_to_row(entry: dict) -> Optional[Notification]:
    event_type = entry.get("event_type")
    outcome = EVENT_TO_OUTCOME.get(event_type)
    if outcome is None:
        return None
    payload = entry.get("payload") or {}
    alert = payload.get("alert", {}) or {}
    sent_at = _parse_ts(entry.get("timestamp")) or datetime.now(timezone.utc)
    return Notification(
        sent_at=sent_at,
        event_type=event_type,
        title=(alert.get("title") or "")[:255] or None,
        message=alert.get("body") or entry.get("message") or "",
        priority=int(alert.get("priority", 0) or 0),
        outcome=outcome,
        suppression_reason=payload.get("reason") if outcome == "suppressed" else None,
    )


def backfill_if_empty(db: Session) -> int:
    """If notifications table is empty, populate it from JSONL. Returns rows inserted."""
    if db.query(Notification).count() > 0:
        return 0
    rows: List[Notification] = []
    for entry in _iter_jsonl_events():
        row = _entry_to_row(entry)
        if row is not None:
            rows.append(row)
    if not rows:
        log.info("jsonl_backfill: no events to backfill")
        return 0
    db.add_all(rows)
    db.commit()
    log.info(f"jsonl_backfill: inserted {len(rows)} historical notifications")
    return len(rows)


def incremental_sync(db: Session) -> int:
    """Insert any JSONL events newer than the latest sent_at in the DB.
    Cheap: if nothing new, no inserts. Returns rows inserted."""
    latest = db.execute(
        select(Notification.sent_at).order_by(Notification.sent_at.desc()).limit(1),
    ).scalar()
    rows: List[Notification] = []
    for entry in _iter_jsonl_events(after=latest):
        row = _entry_to_row(entry)
        if row is not None:
            rows.append(row)
    if not rows:
        return 0
    db.add_all(rows)
    db.commit()
    return len(rows)
