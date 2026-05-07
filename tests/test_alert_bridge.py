"""Tests for src/alerting/bridge.py.

All tests use mocks. No scanner imports. No real Pushover hits. No real
JsonLinesLogger writes. The autouse fixture resets the module singleton
between tests so order doesn't matter.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.alerting import Alert
from src.alerting import bridge
from src.alerting.bridge import AlertBridge


@pytest.fixture(autouse=True)
def reset_singleton():
    bridge._reset()
    yield
    bridge._reset()


def _make_alert(severity="CRITICAL"):
    return Alert(
        severity=severity,
        title=f"{severity} test",
        body=f"{severity} body",
        timestamp=datetime(2026, 5, 9, 14, 32, tzinfo=timezone.utc),
        source="tests.test_alert_bridge",
    )


def _mock_dispatcher(return_value=True):
    d = MagicMock()
    d.dispatch.return_value = return_value
    return d


def _mock_logger():
    return MagicMock()


# === core dispatch + log behavior ========================================


def test_raise_alert_dispatches_and_logs():
    """Both sinks called once each, log BEFORE dispatch."""
    parent = MagicMock()
    logger = _mock_logger()
    dispatcher = _mock_dispatcher(return_value=True)
    parent.attach_mock(logger, "logger")
    parent.attach_mock(dispatcher, "dispatcher")

    b = AlertBridge(dispatcher=dispatcher, logger=logger)
    result = b.raise_alert(_make_alert())

    assert result is True
    logger.log.assert_called_once()
    dispatcher.dispatch.assert_called_once()
    # Ordering: log call appears before dispatch call in the merged trace
    method_names = [name for name, *_ in parent.method_calls]
    assert method_names.index("logger.log") < method_names.index("dispatcher.dispatch")


def test_raise_alert_logger_only():
    logger = _mock_logger()
    b = AlertBridge(dispatcher=None, logger=logger)
    result = b.raise_alert(_make_alert())
    assert result is True
    logger.log.assert_called_once()


def test_raise_alert_dispatcher_only():
    dispatcher = _mock_dispatcher(return_value=False)
    b = AlertBridge(dispatcher=dispatcher, logger=None)
    result = b.raise_alert(_make_alert())
    assert result is False
    dispatcher.dispatch.assert_called_once()


def test_raise_alert_both_none_warns(capsys):
    b = AlertBridge(dispatcher=None, logger=None)
    result = b.raise_alert(_make_alert())
    assert result is False
    captured = capsys.readouterr()
    assert "no dispatcher or logger" in captured.err
    assert "CRITICAL test" in captured.err


def test_logger_failure_is_swallowed(capsys):
    """logger.log raises -> stderr warning, dispatcher still called,
    return value is dispatcher's result."""
    logger = _mock_logger()
    logger.log.side_effect = ConnectionError("logger boom")
    dispatcher = _mock_dispatcher(return_value=True)

    b = AlertBridge(dispatcher=dispatcher, logger=logger)
    result = b.raise_alert(_make_alert())

    assert result is True
    dispatcher.dispatch.assert_called_once()
    captured = capsys.readouterr()
    assert "logger.log raised" in captured.err
    assert "ConnectionError" in captured.err


# === module-level singleton ==============================================


def test_module_init_then_alert():
    logger = _mock_logger()
    dispatcher = _mock_dispatcher(return_value=True)
    bridge.init(dispatcher=dispatcher, logger=logger)

    result = bridge.alert(_make_alert())
    assert result is True
    logger.log.assert_called_once()
    dispatcher.dispatch.assert_called_once()


def test_module_alert_uninitialized(capsys):
    bridge._reset()
    result = bridge.alert(_make_alert())
    assert result is False
    captured = capsys.readouterr()
    assert "not initialized" in captured.err


def test_module_is_initialized():
    bridge._reset()
    assert bridge.is_initialized() is False
    bridge.init(dispatcher=_mock_dispatcher(), logger=_mock_logger())
    assert bridge.is_initialized() is True


def test_module_reset():
    bridge.init(dispatcher=_mock_dispatcher(), logger=_mock_logger())
    assert bridge.is_initialized() is True
    bridge._reset()
    assert bridge.is_initialized() is False


# === logger call shape ===================================================


def test_log_event_type_is_alert_dispatched():
    logger = _mock_logger()
    b = AlertBridge(dispatcher=None, logger=logger)
    b.raise_alert(_make_alert())
    args, kwargs = logger.log.call_args
    assert args[0] == "alert_dispatched"


def test_log_payload_includes_full_alert():
    logger = _mock_logger()
    b = AlertBridge(dispatcher=None, logger=logger)
    a = _make_alert()
    b.raise_alert(a)
    _, kwargs = logger.log.call_args
    payload = kwargs["payload"]
    for key in ("severity", "title", "body", "source", "dedup_key", "payload", "timestamp"):
        assert key in payload, f"missing {key} in payload: {payload}"
    assert payload["severity"] == a.severity
    assert payload["title"] == a.title


def test_log_severity_passed_to_logger():
    """CRITICAL -> ERROR, OPERATIONAL -> INFO, INFO -> DEBUG."""
    cases = [
        ("CRITICAL", "ERROR"),
        ("OPERATIONAL", "INFO"),
        ("INFO", "DEBUG"),
    ]
    for severity, expected_level in cases:
        logger = _mock_logger()
        b = AlertBridge(dispatcher=None, logger=logger)
        b.raise_alert(_make_alert(severity=severity))
        _, kwargs = logger.log.call_args
        assert kwargs["level"] == expected_level, (
            f"{severity} -> level={kwargs['level']!r}, expected {expected_level!r}"
        )
