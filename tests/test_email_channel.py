"""Tests for src/alerting/email_channel.py.

All tests mock smtplib.SMTP. No real Gmail hits.
"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.alerting import Alert
from src.alerting import email_channel as ec


CANONICAL_CONFIG = {
    "pushover": {
        "user_key": "env:PUSHOVER_USER_KEY",
        "app_token": "env:PUSHOVER_APP_TOKEN",
        "test_mode": False,
    },
    "severity_routing": {
        "OPERATIONAL": {"priority": 0, "sound": "pushover", "rate_limit_per_hour": 20,
                        "bypasses_quiet_hours": False},
    },
    "quiet_hours": {"enabled": False, "start": "22:00", "end": "06:00",
                    "timezone": "America/New_York"},
    "dedup_window_minutes": 15,
    "email": {
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
        "smtp_user": "env:SMTP_USER",
        "smtp_password": "env:SMTP_PASSWORD",
        "smtp_to": None,
        "test_mode": False,
        "send_for_severities": ["OPERATIONAL"],
        "send_only_for_event_types": ["daily_summary_email"],
        "attach_master_ranked": True,
    },
}


@pytest.fixture(autouse=True)
def env_creds(monkeypatch):
    monkeypatch.setenv("PUSHOVER_USER_KEY", "px_user")
    monkeypatch.setenv("PUSHOVER_APP_TOKEN", "px_token")
    monkeypatch.setenv("SMTP_USER", "test@example.com")
    monkeypatch.setenv("SMTP_PASSWORD", "test_password")


@pytest.fixture
def config_factory(tmp_path):
    def _factory(**email_overrides) -> Path:
        cfg = copy.deepcopy(CANONICAL_CONFIG)
        if email_overrides:
            cfg["email"].update(email_overrides)
        path = tmp_path / "alerting.yaml"
        path.write_text(yaml.safe_dump(cfg))
        return path
    return _factory


def _make_alert(
    severity="OPERATIONAL",
    event_type="daily_summary_email",
    payload=None,
):
    """event_type is encoded in source per the canonical events.py convention."""
    return Alert(
        severity=severity,
        title="Daily summary 2026-05-09",
        body="preview",
        timestamp=datetime(2026, 5, 9, 14, 32, tzinfo=timezone.utc),
        source=f"src.alerting.events.{event_type}",
        payload=payload or {
            "scan_count": 13, "candidates_count": 42,
            "conflicts_count": 1, "watchlist_signals_count": 1,
            "top_picks": [{"ticker": "AAPL", "composite_score": 87.4, "scanners_hit": "ib"}],
            "conflicts": [],
            "watchlist_deltas": [],
        },
    )


# === core dispatch behavior ===

def test_dispatch_renders_html_and_plaintext(config_factory):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    captured_msg = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout=None):
            captured_msg["host"] = host
            captured_msg["port"] = port
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def starttls(self): pass
        def login(self, u, p):
            captured_msg["login"] = (u, p)
        def send_message(self, msg):
            captured_msg["msg"] = msg

    # Patch smtplib by monkeypatching module attribute on email_channel
    with patch.object(ec.smtplib, "SMTP", FakeSMTP):
        ok = channel.dispatch(alert)

    assert ok is True
    msg = captured_msg["msg"]
    # walk the multipart looking for plain + html parts
    types = [p.get_content_type() for p in msg.walk()]
    assert "text/plain" in types
    assert "text/html" in types


def test_dispatch_attaches_files(config_factory, tmp_path):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    attachment = tmp_path / "master_ranked.csv"
    attachment.write_text("ticker,composite_score\nAAPL,90.0\n")

    captured = {}
    class FakeSMTP:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def starttls(self): pass
        def login(self, *a): pass
        def send_message(self, msg):
            captured["msg"] = msg

    with patch.object(ec.smtplib, "SMTP", FakeSMTP):
        ok = channel.dispatch(alert, attachments=[attachment])

    assert ok is True
    msg = captured["msg"]
    # Find the attachment part by Content-Disposition
    found = False
    for p in msg.walk():
        cd = p.get("Content-Disposition", "")
        if "attachment" in cd and "master_ranked.csv" in cd:
            found = True
            break
    assert found, f"attachment not found in MIME parts; CDs: {[p.get('Content-Disposition') for p in msg.walk()]}"


def test_dispatch_handles_smtp_failure(config_factory):
    logger = MagicMock()
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg, logger=logger)
    alert = _make_alert()

    class FakeSMTP:
        def __init__(self, *a, **kw): pass
        def __enter__(self): raise ConnectionError("network down")
        def __exit__(self, *a): pass

    with patch.object(ec.smtplib, "SMTP", FakeSMTP):
        ok = channel.dispatch(alert)

    assert ok is False
    failure_calls = [c for c in logger.log.call_args_list if c.args[0] == "alert_email_failed"]
    assert len(failure_calls) == 1


def test_test_mode_writes_html_to_disk(config_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    cfg = config_factory(test_mode=True)
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    with patch.object(ec.smtplib, "SMTP", side_effect=AssertionError("must not call SMTP")):
        ok = channel.dispatch(alert)

    assert ok is True
    expected = tmp_path / "logs" / "email_test_2026-05-09.html"
    assert expected.exists()


def test_loads_credentials_from_env(config_factory):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    assert channel._smtp_user == "test@example.com"
    assert channel._smtp_password == "test_password"
    # smtp_to falls back to smtp_user when config null and SMTP_TO env unset
    assert channel._smtp_to == "test@example.com"


def test_only_sends_for_configured_events(config_factory):
    """An OPERATIONAL alert with a non-matching event_type must be suppressed."""
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    # event_type is derived from source; "scanner_complete" source != daily_summary_email
    bad = Alert(
        severity="OPERATIONAL", title="Scanner done", body="x",
        timestamp=datetime(2026, 5, 9, 14, 32, tzinfo=timezone.utc),
        source="src.alerting.events.scanner_complete",
        payload={},
    )

    with patch.object(ec.smtplib, "SMTP", side_effect=AssertionError("must not call SMTP")):
        ok = channel.dispatch(bad)

    assert ok is False  # filtered out

    # Severity mismatch also filters
    info_alert = Alert(
        severity="INFO", title="System startup", body="x",
        timestamp=datetime(2026, 5, 9, 14, 32, tzinfo=timezone.utc),
        source="src.alerting.events.daily_summary_email",  # event_type matches
        payload={},
    )
    with patch.object(ec.smtplib, "SMTP", side_effect=AssertionError("must not call SMTP")):
        ok = channel.dispatch(info_alert)
    assert ok is False  # severity INFO not in send_for_severities=[OPERATIONAL]


def test_subject_includes_date(config_factory):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()
    msg = channel._build_message(alert, attachments=[])
    assert "2026-05-09" in msg["Subject"]
    assert "strategy_bot daily summary" in msg["Subject"]


def test_missing_attachment_logged_and_continues(config_factory, tmp_path):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    class FakeSMTP:
        def __init__(self, *a, **kw): pass
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def starttls(self): pass
        def login(self, *a): pass
        def send_message(self, msg): pass

    nonexistent = tmp_path / "ghost.csv"
    with patch.object(ec.smtplib, "SMTP", FakeSMTP):
        ok = channel.dispatch(alert, attachments=[nonexistent])

    assert ok is True   # missing attachment doesn't fail dispatch


def test_cli_test_subcommand(config_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    cfg = config_factory(test_mode=True)
    with patch.object(ec.smtplib, "SMTP", side_effect=AssertionError("must not call API")):
        with pytest.raises(SystemExit) as excinfo:
            ec.main(["--config", str(cfg), "test"])
    assert excinfo.value.code == 0
