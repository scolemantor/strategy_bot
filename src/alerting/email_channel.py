"""Resend HTTP API email channel for the daily summary email.

Uses httpx.post to https://api.resend.com/emails (port 443, unblocked
on cloud VMs). Auth via Bearer token from RESEND_API_KEY env var.

DigitalOcean and most cloud hosts block outbound port 587, so the
earlier Gmail SMTP implementation silently failed in production.
Resend's free tier is 100 emails/day -- well above strategy_bot's
once-per-morning daily summary cadence.

Filters by severity AND event_type per config (Sean wants ONE email per
day, only for daily_summary_email).

test_mode=true writes the rendered HTML to logs/email_test_<date>.html
instead of dialing the API -- useful during development without
consuming the free tier quota.

Composes with the bridge: bridge.raise_alert() invokes
EmailChannel.dispatch(alert, attachments=...) after PushoverDispatcher.
EmailChannel returns False silently for alerts that don't match its
filter, so the bridge stays event-agnostic.

CLI:
  python -m src.alerting.email_channel test
    Sends a synthetic daily_summary_email through the channel.
"""
from __future__ import annotations

import argparse
import base64
import logging
import mimetypes
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, List, Optional

import httpx
import yaml

from . import Alert
from .email_templates import render_daily_summary_html, render_daily_summary_text

log = logging.getLogger(__name__)

RESEND_API_URL = "https://api.resend.com/emails"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_RESEND_FROM = "onboarding@resend.dev"        # sandbox sender (no DNS needed)
DEFAULT_RESEND_TO = "seanpcoleman1@gmail.com"        # per CLAUDE.md user email

EVENT_TYPE_SOURCE_PREFIX = "src.alerting.events."


def _event_type_from_source(source: str) -> str:
    """Extract event_type from canonical 'src.alerting.events.<name>' source.
    Falls back to full source for non-canonical alerts."""
    if source and source.startswith(EVENT_TYPE_SOURCE_PREFIX):
        return source[len(EVENT_TYPE_SOURCE_PREFIX):]
    return source or ""


def _safe_json_or_text(resp) -> Any:
    """Extract Resend response body for logging without crashing on non-JSON."""
    try:
        return resp.json()
    except Exception:
        try:
            return resp.text[:500]
        except Exception:
            return None


class EmailChannel:
    def __init__(
        self,
        config_path: Path = Path("config/alerting.yaml"),
        logger: Optional[Any] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ):
        self._config_path = Path(config_path)
        self._logger = logger
        self._clock = clock or (lambda: datetime.now(timezone.utc))

        raw = self._load_config(self._config_path)
        config = self._resolve_env_refs(raw)

        email_cfg = config.get("email", {})
        if not email_cfg:
            raise ValueError(
                f"alerting config has no 'email' section: {config_path}"
            )

        provider = email_cfg.get("provider", "resend")
        if provider != "resend":
            raise ValueError(
                f"unsupported email provider: {provider!r} (only 'resend' is supported)"
            )

        self._api_key = email_cfg.get("resend_api_key")
        if not self._api_key:
            raise ValueError(
                f"email.resend_api_key required (set RESEND_API_KEY env): {config_path}"
            )

        # from: explicit config wins; else env RESEND_FROM; else sandbox default
        self._from = (
            email_cfg.get("resend_from")
            or os.environ.get("RESEND_FROM")
            or DEFAULT_RESEND_FROM
        )
        # to: explicit config wins; else env RESEND_TO; else legacy SMTP_TO; else default
        self._to = (
            email_cfg.get("resend_to")
            or os.environ.get("RESEND_TO")
            or os.environ.get("SMTP_TO")  # backward compat from Gmail-SMTP era
            or DEFAULT_RESEND_TO
        )

        self._test_mode = bool(email_cfg.get("test_mode", False))
        self._send_for_severities = set(email_cfg.get("send_for_severities") or [])
        self._send_only_for_event_types = set(
            email_cfg.get("send_only_for_event_types") or []
        )

    # === public API ===

    def dispatch(
        self,
        alert: Alert,
        attachments: Optional[List[Path]] = None,
    ) -> bool:
        # Gate 1: severity + event_type filter
        reason = self._should_dispatch(alert)
        if reason:
            self._log_event(
                "alert_email_suppressed",
                f"email suppressed for {alert.title!r}: {reason}",
                {"alert": alert.to_dict(), "reason": reason},
            )
            return False

        # Gate 2: test_mode short-circuit
        if self._test_mode:
            html = render_daily_summary_html(alert)
            path = self._write_test_mode_artifact(alert, html)
            self._log_event(
                "alert_email_test_mode",
                f"test_mode: email written to {path}",
                {"alert": alert.to_dict(), "html_path": str(path)},
            )
            return True

        # Build + send
        body = self._build_resend_body(alert, list(attachments or []))

        try:
            resp = httpx.post(
                RESEND_API_URL,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=DEFAULT_TIMEOUT_SECONDS,
            )
        except (httpx.HTTPError, OSError) as e:
            self._log_event(
                "alert_email_failed",
                f"resend http error: {type(e).__name__}: {e}",
                {"alert": alert.to_dict(), "error": str(e)},
            )
            return False

        status = getattr(resp, "status_code", None)
        if status in (200, 202):
            self._log_event(
                "alert_email_sent",
                f"resend ok ({status}) -> {self._to}",
                {
                    "alert": alert.to_dict(),
                    "to": self._to,
                    "response": _safe_json_or_text(resp),
                },
            )
            return True

        self._log_event(
            "alert_email_failed",
            f"resend non-2xx: {status}",
            {
                "alert": alert.to_dict(),
                "status_code": status,
                "response": _safe_json_or_text(resp),
            },
        )
        return False

    # === gates ===

    def _should_dispatch(self, alert: Alert) -> Optional[str]:
        if (
            self._send_for_severities
            and alert.severity not in self._send_for_severities
        ):
            return f"severity {alert.severity!r} not in send_for_severities"
        event_type = _event_type_from_source(alert.source)
        if (
            self._send_only_for_event_types
            and event_type not in self._send_only_for_event_types
        ):
            return f"event_type {event_type!r} not in send_only_for_event_types"
        return None

    # === Resend body construction ===

    def _build_resend_body(self, alert: Alert, attachments: List[Path]) -> dict:
        date_str = alert.timestamp.strftime("%Y-%m-%d")
        body: dict = {
            "from": self._from,
            "to": [self._to],
            "subject": f"strategy_bot daily summary — {date_str}",
            "html": render_daily_summary_html(alert),
            "text": render_daily_summary_text(alert),
        }

        att_list = []
        for path in attachments:
            if not path.exists():
                log.warning(f"email attachment not found, skipping: {path}")
                continue
            try:
                data = path.read_bytes()
            except Exception as e:
                log.warning(f"email attachment read failed for {path}: {e}")
                continue
            mime, _ = mimetypes.guess_type(path.name)
            att_list.append({
                "filename": path.name,
                "content": base64.b64encode(data).decode("ascii"),
                "content_type": mime or "application/octet-stream",
            })
        if att_list:
            body["attachments"] = att_list
        return body

    # === test_mode helper ===

    def _write_test_mode_artifact(self, alert: Alert, html: str) -> Path:
        out_dir = Path(os.environ.get("LOG_DIR", "logs"))
        out_dir.mkdir(parents=True, exist_ok=True)
        date_str = alert.timestamp.strftime("%Y-%m-%d")
        path = out_dir / f"email_test_{date_str}.html"
        path.write_text(html, encoding="utf-8")
        return path

    # === logging helper ===

    def _log_event(self, event_type: str, message: str, payload: dict) -> None:
        if self._logger is None:
            return
        try:
            self._logger.log(event_type, message, level="INFO", payload=payload)
        except Exception:
            pass

    # === config loading (mirrors PushoverDispatcher) ===

    @staticmethod
    def _load_config(path: Path) -> dict:
        if not path.exists():
            raise FileNotFoundError(f"alerting config not found: {path}")
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    @staticmethod
    def _resolve_env_refs(config: Any) -> Any:
        if isinstance(config, dict):
            return {k: EmailChannel._resolve_env_refs(v) for k, v in config.items()}
        if isinstance(config, list):
            return [EmailChannel._resolve_env_refs(v) for v in config]
        if isinstance(config, str) and config.startswith("env:"):
            var = config[4:]
            if var not in os.environ:
                raise ValueError(f"alerting config references missing env var: {var}")
            return os.environ[var]
        return config


# === CLI ===

def cmd_test(args) -> int:
    """Send a test daily_summary_email via the channel."""
    from .events import daily_summary_email
    channel = EmailChannel(config_path=args.config)
    sample_picks = [
        {"ticker": "AAPL", "composite_score": 92.5, "scanners_hit": "insider_buying, breakout_52w"},
        {"ticker": "MSFT", "composite_score": 75.0, "scanners_hit": "thirteen_f_changes"},
    ]
    sample_conflicts = [
        {"ticker": "TSLA", "directions": "bullish, bearish",
         "scanners_hit": "insider_buying, insider_selling_clusters"},
    ]
    sample_deltas = [
        {"ticker": "NVDA", "signal_type": "NEW", "scanner": "insider_buying",
         "change": "fresh insider buys"},
    ]
    alert = daily_summary_email(
        scan_count=13, candidates_count=42, conflicts_count=1,
        watchlist_signals_count=1,
        top_picks=sample_picks, conflicts=sample_conflicts,
        watchlist_deltas=sample_deltas,
    )
    ok = channel.dispatch(alert)
    print(f"dispatch result: {ok}")
    return 0 if ok else 1


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Email channel CLI (Resend)")
    parser.add_argument("--config", type=Path, default=Path("config/alerting.yaml"))
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_test = sub.add_parser("test", help="send a test daily_summary_email via Resend")
    p_test.set_defaults(func=cmd_test)
    args = parser.parse_args(argv)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
