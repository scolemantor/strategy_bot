"""JSONL audit log for watchlist mutations.

Every state change (add/remove/update) appends one line to
logs/watchlist_changes.log. Schema:

  {
    "timestamp": "2026-05-10T15:30:00+00:00",
    "action": "add" | "remove" | "update",
    "ticker": "CEG",
    "source": "dashboard" | "cli" | "auto",
    "before_state": <dict or null>,
    "after_state": <dict or null>,
    "user": null,                                 # placeholder (single-user)
    "user_agent": "Mozilla/5.0 ..." | null        # from API request header
  }

Never raises — audit logging must not break the actual mutation.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

AUDIT_LOG_PATH = Path("logs/watchlist_changes.log")


def append_audit_log(
    action: str,
    ticker: str,
    source: str,
    before_state: Optional[dict] = None,
    after_state: Optional[dict] = None,
    user_agent: Optional[str] = None,
) -> None:
    """Append one JSONL line. Best-effort — failures logged at WARNING."""
    try:
        AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "ticker": ticker,
            "source": source,
            "before_state": before_state,
            "after_state": after_state,
            "user": None,
            "user_agent": user_agent,
        }, default=str)
        with AUDIT_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        log.warning(f"watchlist audit log append failed: {e}")
