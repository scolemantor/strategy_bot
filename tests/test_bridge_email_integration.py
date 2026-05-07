"""Tests for AlertBridge + EmailChannel integration in src/alerting/bridge.py."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.alerting import Alert
from src.alerting import bridge


@pytest.fixture(autouse=True)
def reset_singleton():
    bridge._reset()
    yield
    bridge._reset()


def _make_alert(event_type="daily_summary_email", payload=None):
    return Alert(
        severity="OPERATIONAL",
        title="Daily summary 2026-05-09",
        body="preview",
        timestamp=datetime(2026, 5, 9, 14, 32, tzinfo=timezone.utc),
        source=f"src.alerting.events.{event_type}",
        payload=payload or {},
    )


def _mock_dispatcher(return_value=True):
    d = MagicMock()
    d.dispatch.return_value = return_value
    return d


def _mock_email():
    e = MagicMock()
    e.dispatch.return_value = True
    return e


def test_bridge_dispatches_email_for_daily_summary_email():
    dispatcher = _mock_dispatcher(return_value=True)
    email_channel = _mock_email()
    b = bridge.AlertBridge(
        dispatcher=dispatcher, logger=None, email_channel=email_channel,
    )
    alert = _make_alert(event_type="daily_summary_email")
    result = b.raise_alert(alert)

    assert result is True
    email_channel.dispatch.assert_called_once()
    # Pushover should also be called (it self-skips by event_type internally;
    # the bridge doesn't filter)
    dispatcher.dispatch.assert_called_once()


def test_bridge_skips_email_for_non_matching_event_type_via_internal_filter():
    """Bridge calls email_channel.dispatch unconditionally; the channel itself
    filters by event_type. Mock returns False for non-matching events."""
    dispatcher = _mock_dispatcher(return_value=True)
    email_channel = MagicMock()
    email_channel.dispatch.return_value = False  # simulates channel-side filter
    b = bridge.AlertBridge(
        dispatcher=dispatcher, logger=None, email_channel=email_channel,
    )
    alert = _make_alert(event_type="scanner_complete")
    result = b.raise_alert(alert)

    # Bridge does call email_channel.dispatch (it's the channel's job to filter).
    email_channel.dispatch.assert_called_once()
    # Bridge's return reflects Pushover (the primary channel), not email.
    assert result is True


def test_bridge_email_failure_doesnt_block_pushover(capsys):
    """If email_channel.dispatch raises, Pushover still dispatches and its
    return value is what the bridge returns."""
    dispatcher = _mock_dispatcher(return_value=True)
    email_channel = MagicMock()
    email_channel.dispatch.side_effect = ConnectionError("smtp dead")
    b = bridge.AlertBridge(
        dispatcher=dispatcher, logger=None, email_channel=email_channel,
    )
    alert = _make_alert()
    result = b.raise_alert(alert)

    dispatcher.dispatch.assert_called_once()
    assert result is True  # Pushover's True wins
    captured = capsys.readouterr()
    assert "email_channel.dispatch raised" in captured.err


def test_bridge_passes_attachments_from_payload():
    dispatcher = _mock_dispatcher(return_value=True)
    email_channel = _mock_email()
    b = bridge.AlertBridge(
        dispatcher=dispatcher, logger=None, email_channel=email_channel,
    )
    alert = _make_alert(payload={"attachments": ["a.csv", "b.csv"]})
    b.raise_alert(alert)

    args, kwargs = email_channel.dispatch.call_args
    # First positional arg is the alert; attachments passed as kwarg
    assert "attachments" in kwargs
    paths = kwargs["attachments"]
    assert all(isinstance(p, Path) for p in paths)
    assert [str(p) for p in paths] == ["a.csv", "b.csv"]


def test_bridge_no_attachments_when_payload_omits_them():
    dispatcher = _mock_dispatcher(return_value=True)
    email_channel = _mock_email()
    b = bridge.AlertBridge(
        dispatcher=dispatcher, logger=None, email_channel=email_channel,
    )
    alert = _make_alert(payload={})  # no attachments key
    b.raise_alert(alert)

    args, kwargs = email_channel.dispatch.call_args
    assert kwargs.get("attachments") == []


def test_bridge_handles_missing_email_channel_gracefully():
    """Bridge with email_channel=None still dispatches Pushover."""
    dispatcher = _mock_dispatcher(return_value=True)
    b = bridge.AlertBridge(
        dispatcher=dispatcher, logger=None, email_channel=None,
    )
    alert = _make_alert()
    result = b.raise_alert(alert)
    assert result is True
    dispatcher.dispatch.assert_called_once()
