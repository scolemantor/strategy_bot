"""Pushover dispatcher for strategy_bot alerts.

Reads config from config/alerting.yaml at instantiation. env: prefixed values
in the config (env:PUSHOVER_USER_KEY, env:PUSHOVER_APP_TOKEN) are resolved
from os.environ at load time — missing required env vars raise immediately.

Severity → Pushover priority + sound mapping is config-driven so production
can tune without code changes. Emergency priority (2) requires retry/expire
parameters; the dispatcher pulls those from severity_routing config.

Quiet hours, rate limiting, and dedup all run BEFORE the network call.
Suppression reasons are logged via the optional JsonLinesLogger but no
exception is raised — callers always get a bool back.

Concurrency: rate limiter and dedup state are in-memory dicts. Single-process
assumption. Multi-process scenarios (Phase 7 cron + daemon writing
simultaneously) would need shared state — out of scope for this commit.
"""
from __future__ import annotations

import argparse
import os
import socket
import sys
from collections import defaultdict, deque
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
import yaml

from . import Alert

PUSHOVER_API_URL = "https://api.pushover.net/1/messages.json"
PUSHOVER_TITLE_MAX = 250
PUSHOVER_BODY_MAX = 1024
DEFAULT_TIMEOUT_SECONDS = 10

EVENT_TYPE_SOURCE_PREFIX = "src.alerting.events."


def _event_type_from_source(source: str) -> str:
    """Extract the event_type from canonical source.

    Alerts built via events.py have source of the form
    'src.alerting.events.<func_name>'; the func_name is the event_type.
    Fall back to the full source string for non-canonical alerts.
    """
    if source and source.startswith(EVENT_TYPE_SOURCE_PREFIX):
        return source[len(EVENT_TYPE_SOURCE_PREFIX):]
    return source or ""


class PushoverDispatcher:
    def __init__(
        self,
        config_path: Path = Path("config/alerting.yaml"),
        logger: Optional[Any] = None,
        clock: Optional[Callable[[], datetime]] = None,
        notifications_writer: Optional[Callable[[str, str, dict], None]] = None,
    ):
        self._config_path = Path(config_path)
        self._logger = logger
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        # Phase 7.5: optional in-process callback. Dashboard registers this
        # at FastAPI startup so dashboard-initiated alerts go straight to
        # the notifications table. Cron-only callers leave it as None and
        # stay DB-free; their alerts land in DB via the JSONL backfill +
        # incremental sync paths in dashboard/api/jsonl_backfill.py.
        self._notifications_writer = notifications_writer

        raw_config = self._load_config(self._config_path)
        config = self._resolve_env_refs(raw_config)

        pushover_cfg = config.get("pushover", {})
        self._user_key = pushover_cfg.get("user_key")
        self._app_token = pushover_cfg.get("app_token")
        self._test_mode = bool(pushover_cfg.get("test_mode", False))
        self._skip_event_types = set(pushover_cfg.get("skip_for_event_types") or [])
        if not self._user_key or not self._app_token:
            raise ValueError(
                f"pushover.user_key and pushover.app_token are required in {config_path}"
            )

        self.severity_routing: Dict[str, dict] = config.get("severity_routing", {})
        self.quiet_hours_cfg: dict = config.get("quiet_hours", {"enabled": False})
        self.dedup_window_minutes: int = int(config.get("dedup_window_minutes", 15))

        self._rate_history: Dict[str, Deque[datetime]] = defaultdict(deque)
        self._dedup: Dict[str, datetime] = {}

    # === public API ===

    def dispatch(self, alert: Alert) -> bool:
        # Gate 0: event_type skip list (email-only events skip Pushover).
        # Alert has no explicit event_type field; the function name embedded
        # in alert.source ("src.alerting.events.<name>") IS the event_type.
        if _event_type_from_source(alert.source) in self._skip_event_types:
            self._log_suppressed(alert, "event_type_excluded")
            return False

        # Gate 1: quiet hours
        reason = self._check_quiet_hours(alert)
        if reason:
            self._log_suppressed(alert, reason)
            return False

        # Gate 2: dedup
        reason = self._check_dedup(alert)
        if reason:
            self._log_suppressed(alert, reason)
            return False

        # Gate 3: rate limit
        reason = self._check_rate_limit(alert)
        if reason:
            self._log_suppressed(alert, reason)
            return False

        # Gate 4: test mode
        if self._test_mode:
            self._record_dispatch(alert)
            self._log_event(
                "alert_test_mode",
                f"test_mode: {alert.severity} alert not sent to Pushover",
                {"alert": alert.to_dict()},
            )
            return True

        # Gates passed: build + send
        payload = self._build_payload(alert)
        ok = self._send(payload, alert)
        if ok:
            self._record_dispatch(alert)
            self._log_event(
                "alert_dispatched",
                f"{alert.severity} alert sent: {alert.title}",
                {"alert": alert.to_dict()},
            )
        return ok

    # === gates ===

    def _check_quiet_hours(self, alert: Alert) -> Optional[str]:
        sev_cfg = self.severity_routing.get(alert.severity, {})
        if sev_cfg.get("bypasses_quiet_hours", False):
            return None
        qh = self.quiet_hours_cfg or {}
        if not qh.get("enabled", False):
            return None
        try:
            tz = ZoneInfo(qh["timezone"])
        except KeyError:
            return None
        local = self._clock().astimezone(tz).time()
        start = time.fromisoformat(qh["start"])
        end = time.fromisoformat(qh["end"])
        if start <= end:
            in_window = start <= local < end
        else:
            in_window = local >= start or local < end
        return "quiet_hours" if in_window else None

    def _check_dedup(self, alert: Alert) -> Optional[str]:
        if not alert.dedup_key:
            return None
        last = self._dedup.get(alert.dedup_key)
        if last is None:
            return None
        if self._clock() - last < timedelta(minutes=self.dedup_window_minutes):
            return "dedup"
        return None

    def _check_rate_limit(self, alert: Alert) -> Optional[str]:
        sev_cfg = self.severity_routing.get(alert.severity, {})
        limit = sev_cfg.get("rate_limit_per_hour")
        if limit is None:
            return None
        now = self._clock()
        history = self._rate_history[alert.severity]
        # Prune entries older than 2h to bound memory
        cutoff = now - timedelta(hours=2)
        while history and history[0] < cutoff:
            history.popleft()
        current_bucket = now.replace(minute=0, second=0, microsecond=0)
        in_bucket = sum(
            1 for ts in history
            if ts.replace(minute=0, second=0, microsecond=0) == current_bucket
        )
        if in_bucket >= limit:
            return "rate_limited"
        return None

    # === state mutators ===

    def _record_dispatch(self, alert: Alert) -> None:
        now = self._clock()
        self._rate_history[alert.severity].append(now)
        if alert.dedup_key:
            self._dedup[alert.dedup_key] = now

    # === HTTP ===

    def _build_payload(self, alert: Alert) -> dict:
        sev_cfg = self.severity_routing.get(alert.severity, {})
        title = (alert.title or "")[:PUSHOVER_TITLE_MAX]
        body = (alert.body or "")[:PUSHOVER_BODY_MAX]
        ts = alert.timestamp
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        payload = {
            "token": self._app_token,
            "user": self._user_key,
            "title": title,
            "message": body,
            "priority": sev_cfg.get("priority", 0),
            "sound": sev_cfg.get("sound", "pushover"),
            "timestamp": int(ts.timestamp()),
        }
        if payload["priority"] == 2:
            payload["retry"] = sev_cfg.get("retry_seconds", 60)
            payload["expire"] = sev_cfg.get("expire_seconds", 1800)
        return payload

    def _send(self, payload: dict, alert: Alert) -> bool:
        try:
            resp = requests.post(
                PUSHOVER_API_URL, data=payload, timeout=DEFAULT_TIMEOUT_SECONDS,
            )
            resp.raise_for_status()
            return True
        except requests.RequestException as e:
            self._log_event(
                "alert_dispatch_failed",
                f"{alert.severity} alert failed to dispatch: {e}",
                {"alert": alert.to_dict(), "error": str(e)},
            )
            return False

    # === logging helpers ===

    def _log_event(self, event_type: str, message: str, payload: dict) -> None:
        if self._logger is not None:
            try:
                self._logger.log(event_type, message, level="INFO", payload=payload)
            except Exception:
                pass
        if self._notifications_writer is not None:
            try:
                self._notifications_writer(event_type, message, payload)
            except Exception:
                pass  # never let DB write break alerting

    def _log_suppressed(self, alert: Alert, reason: str) -> None:
        self._log_event(
            "alert_suppressed",
            f"{alert.severity} alert suppressed: {reason}",
            {"alert": alert.to_dict(), "reason": reason},
        )

    # === config loading ===

    @staticmethod
    def _load_config(path: Path) -> dict:
        if not path.exists():
            raise FileNotFoundError(f"alerting config not found: {path}")
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    @staticmethod
    def _resolve_env_refs(config: Any) -> Any:
        """Recursively replace any string of the form 'env:VARNAME' with
        the value of os.environ[VARNAME]. Raise ValueError if missing."""
        if isinstance(config, dict):
            return {k: PushoverDispatcher._resolve_env_refs(v) for k, v in config.items()}
        if isinstance(config, list):
            return [PushoverDispatcher._resolve_env_refs(v) for v in config]
        if isinstance(config, str) and config.startswith("env:"):
            var = config[4:]
            if var not in os.environ:
                raise ValueError(f"alerting config references missing env var: {var}")
            return os.environ[var]
        return config


def _hostname() -> str:
    try:
        return socket.gethostname()
    except Exception:
        return "unknown"


# === CLI ===

def cmd_test(args) -> int:
    dispatcher = PushoverDispatcher(config_path=args.config)
    alert = Alert(
        severity="INFO",
        title="strategy_bot test",
        body=f"Pushover dispatcher test alert from {_hostname()}",
        timestamp=datetime.now(timezone.utc),
        source="src.alerting.pushover.cli",
    )
    ok = dispatcher.dispatch(alert)
    return 0 if ok else 1


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Pushover alerting CLI")
    parser.add_argument("--config", type=Path, default=Path("config/alerting.yaml"))
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_test = sub.add_parser("test", help="send a test INFO alert end-to-end")
    p_test.set_defaults(func=cmd_test)
    args = parser.parse_args(argv)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
