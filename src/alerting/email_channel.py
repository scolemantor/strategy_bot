"""SMTP email channel for the daily summary email.

Filters by severity AND event_type per config (Sean wants ONE email per
day, only for daily_summary_email; everything else stays Pushover-only).
Reads SMTP creds from env via config 'env:VARNAME' resolution.

test_mode=true writes the rendered HTML to logs/email_test_<date>.html
instead of dialing SMTP -- useful during development without spamming
the inbox.

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
import logging
import os
import smtplib
import socket
import sys
from datetime import datetime, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Callable, List, Optional

import yaml

from . import Alert
from .email_templates import render_daily_summary_html, render_daily_summary_text

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 30

EVENT_TYPE_SOURCE_PREFIX = "src.alerting.events."


def _event_type_from_source(source: str) -> str:
    """Extract event_type from canonical 'src.alerting.events.<name>' source.
    Falls back to full source for non-canonical alerts."""
    if source and source.startswith(EVENT_TYPE_SOURCE_PREFIX):
        return source[len(EVENT_TYPE_SOURCE_PREFIX):]
    return source or ""


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

        self._smtp_host = email_cfg.get("smtp_host", "smtp.gmail.com")
        self._smtp_port = int(email_cfg.get("smtp_port", 587))
        self._smtp_user = email_cfg.get("smtp_user")
        self._smtp_password = email_cfg.get("smtp_password")
        self._test_mode = bool(email_cfg.get("test_mode", False))
        self._send_for_severities = set(email_cfg.get("send_for_severities") or [])
        self._send_only_for_event_types = set(email_cfg.get("send_only_for_event_types") or [])

        # smtp_to: explicit config wins, else env SMTP_TO, else fall back to smtp_user
        self._smtp_to = (
            email_cfg.get("smtp_to")
            or os.environ.get("SMTP_TO")
            or self._smtp_user
        )

        if not self._smtp_user or not self._smtp_password:
            raise ValueError(
                f"email.smtp_user and email.smtp_password are required in {config_path}"
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
                f"email suppressed for {alert.event_type}: {reason}",
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
        attachments = list(attachments or [])
        msg = self._build_message(alert, attachments)

        try:
            with smtplib.SMTP(
                self._smtp_host, self._smtp_port, timeout=DEFAULT_TIMEOUT_SECONDS,
            ) as server:
                server.starttls()
                server.login(self._smtp_user, self._smtp_password)
                server.send_message(msg)
            self._log_event(
                "alert_email_sent",
                f"email sent to {self._smtp_to}",
                {"alert": alert.to_dict(), "to": self._smtp_to},
            )
            return True
        except (smtplib.SMTPException, socket.gaierror, ConnectionError, OSError) as e:
            self._log_event(
                "alert_email_failed",
                f"email send failed: {type(e).__name__}: {e}",
                {"alert": alert.to_dict(), "error": str(e)},
            )
            return False

    # === gates ===

    def _should_dispatch(self, alert: Alert) -> Optional[str]:
        if self._send_for_severities and alert.severity not in self._send_for_severities:
            return f"severity {alert.severity!r} not in send_for_severities"
        event_type = _event_type_from_source(alert.source)
        if (
            self._send_only_for_event_types
            and event_type not in self._send_only_for_event_types
        ):
            return f"event_type {event_type!r} not in send_only_for_event_types"
        return None

    # === message construction ===

    def _build_message(self, alert: Alert, attachments: List[Path]) -> MIMEMultipart:
        date_str = alert.timestamp.strftime("%Y-%m-%d")
        msg = MIMEMultipart("mixed")
        msg["Subject"] = f"strategy_bot daily summary — {date_str}"
        msg["From"] = self._smtp_user
        msg["To"] = self._smtp_to

        # multipart/alternative carries text + html parts
        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(render_daily_summary_text(alert), "plain", "utf-8"))
        alt.attach(MIMEText(render_daily_summary_html(alert), "html", "utf-8"))
        msg.attach(alt)

        for path in attachments:
            if not path.exists():
                log.warning(f"email attachment not found, skipping: {path}")
                continue
            try:
                data = path.read_bytes()
            except Exception as e:
                log.warning(f"email attachment read failed for {path}: {e}")
                continue
            part = MIMEApplication(data, Name=path.name)
            part["Content-Disposition"] = f'attachment; filename="{path.name}"'
            msg.attach(part)

        return msg

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
    parser = argparse.ArgumentParser(description="Email channel CLI")
    parser.add_argument("--config", type=Path, default=Path("config/alerting.yaml"))
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_test = sub.add_parser("test", help="send a test daily_summary_email")
    p_test.set_defaults(func=cmd_test)
    args = parser.parse_args(argv)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
