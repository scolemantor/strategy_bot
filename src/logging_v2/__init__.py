"""Structured JSON Lines logging for strategy_bot.

Additive to stdlib logging — does not replace it. Scanners and other modules
will continue using `logging.getLogger(__name__)` for human-readable runtime
logs; logging_v2 captures structured events for audit, query, and downstream
consumption (Phase 7.5 API, Phase 13 iOS app).

Each entry is a single line of JSON with this schema:
    {
      "timestamp":  "2026-05-09T14:32:11.234567+00:00",  # ISO 8601 UTC
      "level":      "INFO" | "WARNING" | "ERROR" | "DEBUG",
      "source":     "scanners.short_squeeze",            # caller module
      "event_type": "scanner_complete",                  # short identifier
      "message":    "short_squeeze produced 12 candidates",
      "payload":    {...}                                # event-specific dict
    }

Daily rotation by UTC date: logs/strategy_bot_YYYY-MM-DD.jsonl.
Critical events (event_type contains any of order/rebalance/alert/error,
case-insensitive substring) are ALSO appended to logs/critical/<same name>
which is retained forever.

Retention behavior is in rotation.py. Logger __init__ runs a rotation pass
when auto_rotate=True (default). Pass auto_rotate=False for cron-driven
production setups (Phase 7).

Redaction: payload is walked recursively before write. Any dict key whose
lowercased name contains one of the redact_keys substrings (default:
api_key, password, secret, token, alpaca_key, alpaca_secret) has its value
replaced with "***REDACTED***". Original payload object is not mutated.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from .rotation import rotation_pass

REDACTED = "***REDACTED***"


class JsonLinesLogger:
    DEFAULT_REDACT_KEYS: Tuple[str, ...] = (
        "api_key", "password", "secret", "token",
        "alpaca_key", "alpaca_secret",
    )
    DEFAULT_CRITICAL_KEYWORDS: Tuple[str, ...] = (
        "order", "rebalance", "alert", "error",
    )

    def __init__(
        self,
        log_dir: Path,
        auto_rotate: bool = True,
        grace_days: int = 7,
        delete_after_days: int = 90,
        critical_keywords: Tuple[str, ...] = DEFAULT_CRITICAL_KEYWORDS,
        redact_keys: Tuple[str, ...] = DEFAULT_REDACT_KEYS,
        clock: Optional[Callable[[], datetime]] = None,
    ):
        self.log_dir = Path(log_dir)
        self.critical_dir = self.log_dir / "critical"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.critical_dir.mkdir(parents=True, exist_ok=True)
        self.grace_days = grace_days
        self.delete_after_days = delete_after_days
        self.critical_keywords = tuple(k.lower() for k in critical_keywords)
        self.redact_keys = tuple(k.lower() for k in redact_keys)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

        if auto_rotate:
            rotation_pass(
                self.log_dir,
                grace_days=self.grace_days,
                delete_after_days=self.delete_after_days,
                now=self._clock(),
            )

    def log(
        self,
        event_type: str,
        message: str,
        *,
        level: str = "INFO",
        payload: Optional[Dict[str, Any]] = None,
        source: Optional[str] = None,
    ) -> None:
        if source is None:
            source = _infer_source_from_caller(skip_frames=2)
        ts = self._clock()
        entry = {
            "timestamp": ts.isoformat(),
            "level": level,
            "source": source,
            "event_type": event_type,
            "message": message,
            "payload": _redact(payload, self.redact_keys) if payload is not None else {},
        }
        self._write(entry, ts.date().isoformat())

    def info(self, event_type, message, *, payload=None, source=None) -> None:
        if source is None:
            source = _infer_source_from_caller(skip_frames=2)
        self.log(event_type, message, level="INFO", payload=payload, source=source)

    def warning(self, event_type, message, *, payload=None, source=None) -> None:
        if source is None:
            source = _infer_source_from_caller(skip_frames=2)
        self.log(event_type, message, level="WARNING", payload=payload, source=source)

    def error(self, event_type, message, *, payload=None, source=None) -> None:
        if source is None:
            source = _infer_source_from_caller(skip_frames=2)
        self.log(event_type, message, level="ERROR", payload=payload, source=source)

    def debug(self, event_type, message, *, payload=None, source=None) -> None:
        if source is None:
            source = _infer_source_from_caller(skip_frames=2)
        self.log(event_type, message, level="DEBUG", payload=payload, source=source)

    def _is_critical(self, event_type: str) -> bool:
        low = event_type.lower()
        return any(kw in low for kw in self.critical_keywords)

    def _today_path(self, date_str: str) -> Path:
        return self.log_dir / f"strategy_bot_{date_str}.jsonl"

    def _today_critical_path(self, date_str: str) -> Path:
        return self.critical_dir / f"strategy_bot_{date_str}.jsonl"

    def _write(self, entry: dict, date_str: str) -> None:
        line = json.dumps(entry, default=str) + "\n"
        with open(self._today_path(date_str), "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
        if self._is_critical(entry["event_type"]):
            with open(self._today_critical_path(date_str), "a", encoding="utf-8") as f:
                f.write(line)
                f.flush()


def _key_matches_redact(key: str, redact_keys: Iterable[str]) -> bool:
    low = str(key).lower()
    return any(rk in low for rk in redact_keys)


def _redact(payload: Any, redact_keys: Tuple[str, ...]) -> Any:
    """Recursive walk: dict values whose key matches → REDACTED.
    Otherwise recurse. Lists/tuples handled element-wise. Original not mutated."""
    if isinstance(payload, dict):
        return {
            k: (REDACTED if _key_matches_redact(k, redact_keys) else _redact(v, redact_keys))
            for k, v in payload.items()
        }
    if isinstance(payload, list):
        return [_redact(item, redact_keys) for item in payload]
    if isinstance(payload, tuple):
        return tuple(_redact(item, redact_keys) for item in payload)
    return payload


def _infer_source_from_caller(skip_frames: int = 2) -> str:
    try:
        frame = sys._getframe(skip_frames)
        return frame.f_globals.get("__name__", "unknown") or "unknown"
    except (ValueError, AttributeError):
        return "unknown"
