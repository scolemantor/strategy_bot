"""Tests for src/alerting/email_channel.py.

All tests mock httpx.post. No real Resend API hits.
"""
from __future__ import annotations

import base64
import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
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
        "provider": "resend",
        "resend_api_key": "env:RESEND_API_KEY",
        "resend_from": None,
        "resend_to": None,
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
    monkeypatch.setenv("RESEND_API_KEY", "re_test_key")
    # SMTP_TO / RESEND_TO / RESEND_FROM unset by default; tests opt in


@pytest.fixture
def config_factory(tmp_path):
    def _factory(**overrides) -> Path:
        cfg = copy.deepcopy(CANONICAL_CONFIG)
        if overrides:
            cfg["email"].update(overrides)
        path = tmp_path / "alerting.yaml"
        path.write_text(yaml.safe_dump(cfg))
        return path
    return _factory


def _make_alert(
    severity="OPERATIONAL",
    event_type="daily_summary_email",
    payload=None,
):
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


def _ok_response(status_code=200):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = {"id": "re_123abc"}
    r.text = '{"id":"re_123abc"}'
    return r


def _err_response(status_code=403):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = {"error": "forbidden"}
    r.text = '{"error":"forbidden"}'
    return r


# === API call shape ===

def test_dispatch_calls_resend_api_with_correct_shape(config_factory):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    with patch.object(ec.httpx, "post", return_value=_ok_response()) as mp:
        ok = channel.dispatch(alert)

    assert ok is True
    mp.assert_called_once()
    args, kwargs = mp.call_args
    assert args[0] == ec.RESEND_API_URL
    headers = kwargs["headers"]
    assert headers["Authorization"] == "Bearer re_test_key"
    assert headers["Content-Type"] == "application/json"
    body = kwargs["json"]
    for key in ("from", "to", "subject", "html", "text"):
        assert key in body
    assert body["from"] == ec.DEFAULT_RESEND_FROM
    assert body["to"] == [ec.DEFAULT_RESEND_TO]


def test_dispatch_subject_includes_date(config_factory):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()
    with patch.object(ec.httpx, "post", return_value=_ok_response()) as mp:
        channel.dispatch(alert)
    body = mp.call_args.kwargs["json"]
    assert body["subject"] == "strategy_bot daily summary — 2026-05-09"


def test_dispatch_attaches_files(config_factory, tmp_path):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    attachment = tmp_path / "master_ranked.csv"
    attachment.write_text("ticker,composite_score\nAAPL,90.0\n")

    with patch.object(ec.httpx, "post", return_value=_ok_response()) as mp:
        ok = channel.dispatch(alert, attachments=[attachment])

    assert ok is True
    body = mp.call_args.kwargs["json"]
    assert "attachments" in body
    assert len(body["attachments"]) == 1
    att = body["attachments"][0]
    assert att["filename"] == "master_ranked.csv"
    assert att["content_type"] == "text/csv"
    decoded = base64.b64decode(att["content"])
    assert b"AAPL,90.0" in decoded


def test_csv_attachment_uses_text_csv_content_type(config_factory, tmp_path):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    csv_path = tmp_path / "report.csv"
    csv_path.write_text("a,b\n1,2\n")

    with patch.object(ec.httpx, "post", return_value=_ok_response()) as mp:
        channel.dispatch(alert, attachments=[csv_path])

    att = mp.call_args.kwargs["json"]["attachments"][0]
    assert att["content_type"] == "text/csv"


def test_unknown_extension_uses_octet_stream(config_factory, tmp_path):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    weird = tmp_path / "report.xyzbinary"
    weird.write_bytes(b"\x00\x01\x02")

    with patch.object(ec.httpx, "post", return_value=_ok_response()) as mp:
        channel.dispatch(alert, attachments=[weird])

    att = mp.call_args.kwargs["json"]["attachments"][0]
    assert att["content_type"] == "application/octet-stream"


# === HTTP error / status handling ===

def test_dispatch_handles_http_error(config_factory):
    logger = MagicMock()
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg, logger=logger)
    alert = _make_alert()

    with patch.object(ec.httpx, "post", side_effect=httpx.ConnectError("network down")):
        ok = channel.dispatch(alert)

    assert ok is False
    failure_calls = [c for c in logger.log.call_args_list if c.args[0] == "alert_email_failed"]
    assert len(failure_calls) == 1


def test_dispatch_handles_non_2xx_response(config_factory):
    logger = MagicMock()
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg, logger=logger)
    alert = _make_alert()

    with patch.object(ec.httpx, "post", return_value=_err_response(403)):
        ok = channel.dispatch(alert)

    assert ok is False
    failure_calls = [c for c in logger.log.call_args_list if c.args[0] == "alert_email_failed"]
    assert len(failure_calls) == 1
    payload = failure_calls[0].kwargs["payload"]
    assert payload["status_code"] == 403


def test_dispatch_accepts_202_as_success(config_factory):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()
    with patch.object(ec.httpx, "post", return_value=_ok_response(202)):
        ok = channel.dispatch(alert)
    assert ok is True


# === test_mode ===

def test_test_mode_writes_html_to_disk(config_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    cfg = config_factory(test_mode=True)
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    with patch.object(ec.httpx, "post", side_effect=AssertionError("must not call API")):
        ok = channel.dispatch(alert)

    assert ok is True
    expected = tmp_path / "logs" / "email_test_2026-05-09.html"
    assert expected.exists()


# === credentials / env fallback ===

def test_loads_credentials_from_env(config_factory):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    assert channel._api_key == "re_test_key"


def test_missing_resend_api_key_raises(config_factory, monkeypatch):
    monkeypatch.delenv("RESEND_API_KEY", raising=False)
    cfg = config_factory()
    with pytest.raises(ValueError) as excinfo:
        ec.EmailChannel(config_path=cfg)
    assert "RESEND_API_KEY" in str(excinfo.value)


def test_resend_to_falls_back_to_smtp_to(config_factory, monkeypatch):
    monkeypatch.delenv("RESEND_TO", raising=False)
    monkeypatch.setenv("SMTP_TO", "legacy@example.com")
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    assert channel._to == "legacy@example.com"


def test_resend_to_falls_back_to_default_when_no_env(config_factory, monkeypatch):
    monkeypatch.delenv("RESEND_TO", raising=False)
    monkeypatch.delenv("SMTP_TO", raising=False)
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    assert channel._to == ec.DEFAULT_RESEND_TO


def test_resend_from_defaults_to_sandbox_when_no_env(config_factory, monkeypatch):
    monkeypatch.delenv("RESEND_FROM", raising=False)
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    assert channel._from == ec.DEFAULT_RESEND_FROM


def test_resend_from_uses_env_when_set(config_factory, monkeypatch):
    monkeypatch.setenv("RESEND_FROM", "alerts@mydomain.com")
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    assert channel._from == "alerts@mydomain.com"


# === filter behavior ===

def test_only_sends_for_configured_events(config_factory):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    bad = _make_alert(event_type="scanner_complete")

    with patch.object(ec.httpx, "post", side_effect=AssertionError("must not call API")):
        ok = channel.dispatch(bad)

    assert ok is False


def test_severity_mismatch_filters_out(config_factory):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    bad = _make_alert(severity="INFO")  # event_type matches but severity doesn't

    with patch.object(ec.httpx, "post", side_effect=AssertionError("must not call API")):
        ok = channel.dispatch(bad)

    assert ok is False


# === attachment edge cases ===

def test_missing_attachment_logged_and_continues(config_factory, tmp_path):
    cfg = config_factory()
    channel = ec.EmailChannel(config_path=cfg)
    alert = _make_alert()

    nonexistent = tmp_path / "ghost.csv"
    with patch.object(ec.httpx, "post", return_value=_ok_response()) as mp:
        ok = channel.dispatch(alert, attachments=[nonexistent])

    assert ok is True
    body = mp.call_args.kwargs["json"]
    assert "attachments" not in body  # missing file is silently skipped


# === provider gate ===

def test_provider_must_be_resend(config_factory):
    cfg = config_factory(provider="ses")
    with pytest.raises(ValueError) as excinfo:
        ec.EmailChannel(config_path=cfg)
    assert "resend" in str(excinfo.value).lower()


# === CLI ===

def test_cli_test_subcommand(config_factory, tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    cfg = config_factory(test_mode=True)
    with patch.object(ec.httpx, "post", side_effect=AssertionError("must not call API")):
        with pytest.raises(SystemExit) as excinfo:
            ec.main(["--config", str(cfg), "test"])
    assert excinfo.value.code == 0
