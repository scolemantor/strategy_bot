"""Tests for src/alerting/pushover.py.

All tests use tmp_path for config files and mock requests.post.
No scanner imports. No real network calls. No real Pushover hits.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
import copy

import pytest
import requests
import yaml

from src.alerting import Alert
from src.alerting import pushover as pv


CANONICAL_CONFIG = {
    "pushover": {
        "user_key": "env:PUSHOVER_USER_KEY",
        "app_token": "env:PUSHOVER_APP_TOKEN",
        "test_mode": False,
    },
    "severity_routing": {
        "CRITICAL": {
            "priority": 2, "sound": "siren",
            "retry_seconds": 60, "expire_seconds": 1800,
            "rate_limit_per_hour": 10, "bypasses_quiet_hours": True,
        },
        "OPERATIONAL": {
            "priority": 0, "sound": "pushover",
            "rate_limit_per_hour": 20, "bypasses_quiet_hours": False,
        },
        "INFO": {
            "priority": -1, "sound": "none",
            "rate_limit_per_hour": 5, "bypasses_quiet_hours": False,
        },
    },
    "quiet_hours": {
        "enabled": True, "start": "22:00", "end": "06:00",
        "timezone": "America/New_York",
    },
    "dedup_window_minutes": 15,
}


# === fixtures ===

@pytest.fixture(autouse=True)
def env_creds(monkeypatch):
    monkeypatch.setenv("PUSHOVER_USER_KEY", "test_user_key")
    monkeypatch.setenv("PUSHOVER_APP_TOKEN", "test_app_token")


@pytest.fixture
def config_factory(tmp_path):
    """Returns a callable: config_factory(**overrides) -> Path.
    Top-level overrides merge into a deep copy of CANONICAL_CONFIG."""
    def _factory(**overrides) -> Path:
        cfg = copy.deepcopy(CANONICAL_CONFIG)
        for k, v in overrides.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k] = {**cfg[k], **v}
            else:
                cfg[k] = v
        path = tmp_path / "alerting.yaml"
        path.write_text(yaml.safe_dump(cfg))
        return path
    return _factory


@pytest.fixture
def fixed_clock_noon():
    """14:30 UTC on a weekday (10:30 EDT, well outside quiet hours)."""
    fixed = datetime(2026, 5, 6, 14, 30, 0, tzinfo=timezone.utc)
    return lambda: fixed


@pytest.fixture
def fixed_clock_quiet():
    """03:00 UTC = 23:00 EDT (within 22:00-06:00 NYC quiet hours)."""
    fixed = datetime(2026, 5, 6, 3, 0, 0, tzinfo=timezone.utc)
    return lambda: fixed


def _make_alert(severity="INFO", dedup_key=None, ts=None) -> Alert:
    return Alert(
        severity=severity,
        title=f"{severity} test",
        body=f"{severity} body",
        timestamp=ts or datetime(2026, 5, 6, 14, 30, 0, tzinfo=timezone.utc),
        source="tests.test_pushover",
        dedup_key=dedup_key,
    )


def _mock_response(status_code=200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.raise_for_status = MagicMock()
    return mock


# === API call shape ===

def test_dispatch_calls_pushover_api(config_factory, fixed_clock_noon):
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_noon)
    with patch.object(pv.requests, "post", return_value=_mock_response()) as mp:
        ok = d.dispatch(_make_alert("INFO"))
    assert ok is True
    mp.assert_called_once()
    args, kwargs = mp.call_args
    assert args[0] == pv.PUSHOVER_API_URL
    posted = kwargs["data"]
    for key in ("token", "user", "title", "message", "priority", "sound", "timestamp"):
        assert key in posted


def test_severity_priority_mapping(config_factory, fixed_clock_noon):
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_noon)
    posts = []
    def capture(*a, **kw):
        posts.append(kw["data"])
        return _mock_response()
    with patch.object(pv.requests, "post", side_effect=capture):
        d.dispatch(_make_alert("CRITICAL"))
        d.dispatch(_make_alert("OPERATIONAL"))
        d.dispatch(_make_alert("INFO"))
    assert posts[0]["priority"] == 2
    assert posts[1]["priority"] == 0
    assert posts[2]["priority"] == -1


def test_critical_includes_retry_expire(config_factory, fixed_clock_noon):
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_noon)
    captured = {}
    def capture(*a, **kw):
        captured.update(kw["data"])
        return _mock_response()
    with patch.object(pv.requests, "post", side_effect=capture):
        d.dispatch(_make_alert("CRITICAL"))
    assert captured["retry"] == 60
    assert captured["expire"] == 1800


def test_title_and_body_truncated_to_api_limits(config_factory, fixed_clock_noon):
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_noon)
    long_title = "A" * 300
    long_body = "B" * 1500
    alert = Alert(
        severity="INFO", title=long_title, body=long_body,
        timestamp=datetime(2026, 5, 6, 14, 30, tzinfo=timezone.utc),
        source="t",
    )
    captured = {}
    def capture(*a, **kw):
        captured.update(kw["data"])
        return _mock_response()
    with patch.object(pv.requests, "post", side_effect=capture):
        d.dispatch(alert)
    assert len(captured["title"]) == pv.PUSHOVER_TITLE_MAX
    assert len(captured["message"]) == pv.PUSHOVER_BODY_MAX


# === quiet hours ===

def test_quiet_hours_blocks_operational(config_factory, fixed_clock_quiet):
    logger = MagicMock()
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, logger=logger, clock=fixed_clock_quiet)
    with patch.object(pv.requests, "post") as mp:
        ok = d.dispatch(_make_alert("OPERATIONAL"))
    assert ok is False
    mp.assert_not_called()
    # Logger called with event_type alert_suppressed
    assert any(
        call.args[0] == "alert_suppressed" or call.kwargs.get("event_type") == "alert_suppressed"
        for call in logger.log.call_args_list
    )


def test_quiet_hours_allows_critical(config_factory, fixed_clock_quiet):
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_quiet)
    with patch.object(pv.requests, "post", return_value=_mock_response()) as mp:
        ok = d.dispatch(_make_alert("CRITICAL"))
    assert ok is True
    mp.assert_called_once()


# === rate limiting ===

def test_rate_limit_drops_excess(config_factory, fixed_clock_noon):
    logger = MagicMock()
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, logger=logger, clock=fixed_clock_noon)
    with patch.object(pv.requests, "post", return_value=_mock_response()):
        results = [d.dispatch(_make_alert("CRITICAL")) for _ in range(11)]
    assert results[:10] == [True] * 10
    assert results[10] is False
    # The suppression log call must reference rate_limited
    suppressed_calls = [
        c for c in logger.log.call_args_list
        if c.args[0] == "alert_suppressed"
    ]
    assert any(
        c.kwargs.get("payload", {}).get("reason") == "rate_limited"
        for c in suppressed_calls
    )


def test_rate_limit_resets_hourly(config_factory):
    cfg = config_factory()
    clock_state = {"now": datetime(2026, 5, 6, 14, 30, 0, tzinfo=timezone.utc)}
    d = pv.PushoverDispatcher(
        config_path=cfg, clock=lambda: clock_state["now"],
    )
    with patch.object(pv.requests, "post", return_value=_mock_response()):
        for _ in range(10):
            assert d.dispatch(_make_alert("CRITICAL", ts=clock_state["now"])) is True
        # Advance to next hour bucket
        clock_state["now"] = datetime(2026, 5, 6, 15, 0, 0, tzinfo=timezone.utc)
        assert d.dispatch(_make_alert("CRITICAL", ts=clock_state["now"])) is True


# === dedup ===

def test_dedup_suppresses_repeats(config_factory, fixed_clock_noon):
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_noon)
    with patch.object(pv.requests, "post", return_value=_mock_response()):
        first = d.dispatch(_make_alert("CRITICAL", dedup_key="K1"))
        second = d.dispatch(_make_alert("CRITICAL", dedup_key="K1"))
    assert first is True
    assert second is False


def test_dedup_different_keys_pass(config_factory, fixed_clock_noon):
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_noon)
    with patch.object(pv.requests, "post", return_value=_mock_response()):
        a = d.dispatch(_make_alert("CRITICAL", dedup_key="K1"))
        b = d.dispatch(_make_alert("CRITICAL", dedup_key="K2"))
    assert a is True
    assert b is True


def test_dedup_window_expires(config_factory):
    cfg = config_factory()
    clock_state = {"now": datetime(2026, 5, 6, 14, 30, 0, tzinfo=timezone.utc)}
    d = pv.PushoverDispatcher(
        config_path=cfg, clock=lambda: clock_state["now"],
    )
    with patch.object(pv.requests, "post", return_value=_mock_response()):
        first = d.dispatch(_make_alert("CRITICAL", dedup_key="K1", ts=clock_state["now"]))
        clock_state["now"] = datetime(2026, 5, 6, 14, 46, 0, tzinfo=timezone.utc)  # 16 min later
        second = d.dispatch(_make_alert("CRITICAL", dedup_key="K1", ts=clock_state["now"]))
    assert first is True
    assert second is True


# === test mode ===

def test_test_mode_skips_api(config_factory, fixed_clock_noon):
    logger = MagicMock()
    cfg = config_factory(pushover={
        "user_key": "env:PUSHOVER_USER_KEY",
        "app_token": "env:PUSHOVER_APP_TOKEN",
        "test_mode": True,
    })
    d = pv.PushoverDispatcher(config_path=cfg, logger=logger, clock=fixed_clock_noon)
    with patch.object(pv.requests, "post") as mp:
        ok = d.dispatch(_make_alert("INFO"))
    assert ok is True
    mp.assert_not_called()
    assert any(
        c.args[0] == "alert_test_mode" for c in logger.log.call_args_list
    )


# === credential handling ===

def test_loads_credentials_from_env(config_factory, fixed_clock_noon):
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_noon)
    assert d._user_key == "test_user_key"
    assert d._app_token == "test_app_token"


def test_missing_env_credential_raises_at_init(config_factory, monkeypatch, fixed_clock_noon):
    monkeypatch.delenv("PUSHOVER_USER_KEY", raising=False)
    cfg = config_factory()
    with pytest.raises(ValueError) as excinfo:
        pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_noon)
    assert "PUSHOVER_USER_KEY" in str(excinfo.value)


# === network failure ===

def test_network_failure_logged_and_returns_false(config_factory, fixed_clock_noon):
    logger = MagicMock()
    cfg = config_factory()
    d = pv.PushoverDispatcher(config_path=cfg, logger=logger, clock=fixed_clock_noon)
    with patch.object(pv.requests, "post", side_effect=requests.ConnectionError("boom")):
        ok = d.dispatch(_make_alert("CRITICAL"))
    assert ok is False
    failure_calls = [
        c for c in logger.log.call_args_list
        if c.args[0] == "alert_dispatch_failed"
    ]
    assert len(failure_calls) == 1
    assert "alert" in failure_calls[0].kwargs["payload"]


# === CLI ===

def test_skip_for_event_types_suppresses_dispatch(config_factory, fixed_clock_noon):
    """Alerts with source matching skip_for_event_types are suppressed early."""
    logger = MagicMock()
    cfg = config_factory(pushover={
        "user_key": "env:PUSHOVER_USER_KEY",
        "app_token": "env:PUSHOVER_APP_TOKEN",
        "test_mode": False,
        "skip_for_event_types": ["daily_summary_email"],
    })
    d = pv.PushoverDispatcher(config_path=cfg, logger=logger, clock=fixed_clock_noon)

    # event_type derived from source: "src.alerting.events.daily_summary_email"
    skipped = Alert(
        severity="OPERATIONAL", title="Daily summary", body="x",
        timestamp=datetime(2026, 5, 6, 14, 30, tzinfo=timezone.utc),
        source="src.alerting.events.daily_summary_email",
    )

    with patch.object(pv.requests, "post") as mp:
        ok = d.dispatch(skipped)

    assert ok is False
    mp.assert_not_called()
    suppressed_calls = [
        c for c in logger.log.call_args_list
        if c.args[0] == "alert_suppressed"
    ]
    assert any(
        c.kwargs.get("payload", {}).get("reason") == "event_type_excluded"
        for c in suppressed_calls
    )


def test_skip_for_event_types_does_not_affect_other_events(config_factory, fixed_clock_noon):
    """Non-matching event_type still dispatches normally."""
    cfg = config_factory(pushover={
        "user_key": "env:PUSHOVER_USER_KEY",
        "app_token": "env:PUSHOVER_APP_TOKEN",
        "test_mode": False,
        "skip_for_event_types": ["daily_summary_email"],
    })
    d = pv.PushoverDispatcher(config_path=cfg, clock=fixed_clock_noon)

    normal = Alert(
        severity="OPERATIONAL", title="Scanner done", body="x",
        timestamp=datetime(2026, 5, 6, 14, 30, tzinfo=timezone.utc),
        source="src.alerting.events.scanner_complete",
    )
    with patch.object(pv.requests, "post", return_value=_mock_response()) as mp:
        ok = d.dispatch(normal)
    assert ok is True
    mp.assert_called_once()


def test_cli_test_subcommand(config_factory):
    cfg = config_factory(pushover={
        "user_key": "env:PUSHOVER_USER_KEY",
        "app_token": "env:PUSHOVER_APP_TOKEN",
        "test_mode": True,
    })
    with patch.object(pv.requests, "post", side_effect=AssertionError("must not call API")):
        with pytest.raises(SystemExit) as excinfo:
            pv.main(["--config", str(cfg), "test"])
    assert excinfo.value.code == 0
