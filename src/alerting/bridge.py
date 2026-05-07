"""Unified alert dispatch + structured logging bridge.

Composes PushoverDispatcher (network alert) and JsonLinesLogger (audit
record) so callers raise alerts with a single call. Both dependencies are
optional and duck-typed — bridge.py does NOT hard-import either, so it
stays usable in tests with mocks and never blocks construction when one
sink is unavailable.

Order of operations: log first, then dispatch. The audit trail survives
Pushover network failures. If logger.log itself raises, the exception is
swallowed and a stderr warning is emitted — logging failures should never
cascade into the caller.

Severity -> Python log level mapping (used when calling logger.log):
  CRITICAL    -> ERROR
  OPERATIONAL -> INFO
  INFO        -> DEBUG

The event_type passed to logger.log is always 'alert_dispatched'. This
substring matches Phase 5's critical-retention keyword 'alert', so every
bridged alert lands in logs/critical/ regardless of severity.

Module-level singleton:
  init(dispatcher, logger)  -- set the singleton once at startup
  alert(a)                  -- fire an alert via the singleton
  is_initialized()          -- bool check
  _reset()                  -- test-only, clears the singleton
"""
from __future__ import annotations

import sys
from typing import Any, Optional

from . import Alert

SEVERITY_TO_LOG_LEVEL = {
    "CRITICAL":    "ERROR",
    "OPERATIONAL": "INFO",
    "INFO":        "DEBUG",
}
LOG_EVENT_TYPE = "alert_dispatched"


class AlertBridge:
    def __init__(
        self,
        dispatcher: Optional[Any] = None,   # forward-typed PushoverDispatcher
        logger: Optional[Any] = None,       # forward-typed JsonLinesLogger
    ):
        self._dispatcher = dispatcher
        self._logger = logger

    def raise_alert(self, alert: Alert) -> bool:
        if self._dispatcher is None and self._logger is None:
            sys.stderr.write(
                f"AlertBridge: no dispatcher or logger configured, "
                f"alert dropped: {alert.title}\n"
            )
            return False

        # Log first so the audit record exists even if dispatch fails.
        log_ok = self._log_alert(alert)

        if self._dispatcher is not None:
            return self._dispatcher.dispatch(alert)

        # Logger-only mode: True iff logger.log didn't raise.
        return log_ok

    def _log_alert(self, alert: Alert) -> bool:
        if self._logger is None:
            return False
        level = SEVERITY_TO_LOG_LEVEL.get(alert.severity, "INFO")
        try:
            self._logger.log(
                LOG_EVENT_TYPE,
                f"[{alert.severity}] {alert.title}",
                level=level,
                payload=alert.to_dict(),
                source=alert.source,
            )
            return True
        except Exception as e:
            sys.stderr.write(
                f"AlertBridge: logger.log raised {type(e).__name__}: {e}; "
                f"alert audit dropped: {alert.title}\n"
            )
            return False


# === module-level singleton ==============================================

_bridge: Optional[AlertBridge] = None


def init(
    dispatcher: Optional[Any] = None,
    logger: Optional[Any] = None,
) -> None:
    """Set up the module-level singleton. Call once at startup."""
    global _bridge
    _bridge = AlertBridge(dispatcher=dispatcher, logger=logger)


def alert(a: Alert) -> bool:
    """Fire an alert via the module-level singleton.
    Returns False with a stderr warning if init() has not been called."""
    if _bridge is None:
        sys.stderr.write(
            f"AlertBridge: not initialized (call init() first), "
            f"alert dropped: {a.title}\n"
        )
        return False
    return _bridge.raise_alert(a)


def is_initialized() -> bool:
    return _bridge is not None


def _reset() -> None:
    """Test-only: clear the singleton."""
    global _bridge
    _bridge = None
