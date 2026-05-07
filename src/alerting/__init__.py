"""Alerting package — dispatcher-agnostic Alert type + concrete dispatchers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Literal, Optional

Severity = Literal["CRITICAL", "OPERATIONAL", "INFO"]


@dataclass
class Alert:
    """A single alert event. Truncation to Pushover's API limits happens at
    dispatch time, not in this dataclass — keeps the dataclass policy-free.

    timestamp: should be timezone-aware. Naive datetimes are treated as UTC
    by the Pushover dispatcher when serializing to Unix epoch.
    """
    severity: Severity
    title: str
    body: str
    timestamp: datetime
    source: str
    payload: Optional[Dict[str, Any]] = None
    dedup_key: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "severity": self.severity,
            "title": self.title,
            "body": self.body,
            "timestamp": self.timestamp.isoformat() if self.timestamp is not None else None,
            "source": self.source,
            "payload": self.payload,
            "dedup_key": self.dedup_key,
        }


from .pushover import PushoverDispatcher  # noqa: E402  re-export for callers

__all__ = ["Alert", "Severity", "PushoverDispatcher"]
