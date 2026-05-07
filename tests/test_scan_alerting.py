"""Tests for scan.py alerting/logging activation.

All tests mock the network and the actual scanners; nothing here ever
calls a real scanner.run(), opens a real Pushover socket, or writes to
the real logs/ directory.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import scan
from src.alerting import bridge as alert_bridge
from src.alerting.setup import init_default_bridge


# === fixtures ===

@pytest.fixture(autouse=True)
def reset_bridge():
    """Each test starts with no singleton bridge."""
    alert_bridge._reset()
    yield
    alert_bridge._reset()


@pytest.fixture
def mock_bridge_init(monkeypatch):
    """Replace init_default_bridge in scan.py with a no-op that installs
    mocked logger + dispatcher. Tests can inspect the resulting bridge."""
    def _install():
        if alert_bridge.is_initialized():
            return
        logger = MagicMock()
        dispatcher = MagicMock()
        dispatcher.dispatch.return_value = True
        alert_bridge.init(dispatcher=dispatcher, logger=logger)
    monkeypatch.setattr(scan, "init_default_bridge", _install)
    return _install


# === setup.init_default_bridge ===

def test_init_default_bridge_idempotent(monkeypatch, tmp_path):
    monkeypatch.setenv("PUSHOVER_USER_KEY", "test_user")
    monkeypatch.setenv("PUSHOVER_APP_TOKEN", "test_token")
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))

    init_default_bridge()
    first = alert_bridge._bridge
    init_default_bridge()
    second = alert_bridge._bridge

    assert first is second  # second call did not replace


def test_init_default_bridge_missing_pushover_falls_back_to_logger_only(
    monkeypatch, tmp_path, capsys,
):
    monkeypatch.delenv("PUSHOVER_USER_KEY", raising=False)
    monkeypatch.delenv("PUSHOVER_APP_TOKEN", raising=False)
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))

    init_default_bridge()
    captured = capsys.readouterr()
    assert "PushoverDispatcher init failed" in captured.err
    assert alert_bridge.is_initialized()
    # dispatcher should be None, logger present
    assert alert_bridge._bridge._dispatcher is None
    assert alert_bridge._bridge._logger is not None


# === scan.main system_startup ===

def test_main_fires_system_startup(monkeypatch, mock_bridge_init):
    """Dispatch the 'list' subcommand; assert system_startup alert fired."""
    monkeypatch.setattr("sys.argv", ["scan.py", "list"])
    captured = []
    real_alert = alert_bridge.alert
    def capture(a):
        captured.append(a)
        return real_alert(a)
    monkeypatch.setattr(scan.bridge, "alert", capture)
    scan.main()
    titles = [a.title for a in captured]
    assert any("strategy_bot startup" in t for t in titles), titles


# === cmd_run ===

def _fake_scan_result(count=3, error=None):
    from scanners.base import ScanResult
    df = pd.DataFrame([
        {"ticker": "AAPL", "score": 90.0, "reason": "test"},
        {"ticker": "MSFT", "score": 80.0, "reason": "test"},
        {"ticker": "NVDA", "score": 70.0, "reason": "test"},
    ][:count])
    return ScanResult(
        scanner_name="fake",
        run_date=date(2026, 5, 7),
        candidates=df,
        error=error,
    )


def test_cmd_run_fires_scanner_complete_on_success(
    mock_bridge_init, monkeypatch, tmp_path,
):
    mock_bridge_init()
    fake_scanner = MagicMock()
    fake_scanner.run.return_value = _fake_scan_result(count=3)
    fake_scanner.__str__ = lambda self: "fake_scanner: testing"
    monkeypatch.setattr(scan, "get_scanner", lambda name: fake_scanner)
    # Bypass the investability filter (it would try to read enrichment data)
    monkeypatch.setattr(scan, "filter_candidates", lambda df, scanner_name: (df, df.iloc[0:0]))

    captured = []
    monkeypatch.setattr(scan.bridge, "alert", lambda a: captured.append(a) or True)

    result = scan.cmd_run("fake", date(2026, 5, 7), tmp_path, apply_filter=True)

    assert result["count"] == 3
    assert result["errors"] == 0
    assert "runtime_seconds" in result
    assert any("Scanner done" in a.title for a in captured), [a.title for a in captured]


def test_cmd_run_fires_scanner_exception_on_failure(
    mock_bridge_init, monkeypatch, tmp_path,
):
    mock_bridge_init()
    fake_scanner = MagicMock()
    fake_scanner.run.side_effect = ValueError("boom")
    fake_scanner.__str__ = lambda self: "fake_scanner: testing"
    monkeypatch.setattr(scan, "get_scanner", lambda name: fake_scanner)

    captured = []
    monkeypatch.setattr(scan.bridge, "alert", lambda a: captured.append(a) or True)

    result = scan.cmd_run("fake", date(2026, 5, 7), tmp_path)

    assert result["count"] == 0
    assert result["errors"] == 1
    titles = [a.title for a in captured]
    assert any("Scanner exception" in t for t in titles), titles


# === cmd_all ===

def test_cmd_all_fires_scan_started_then_logs_suite_complete(
    mock_bridge_init, monkeypatch, tmp_path,
):
    mock_bridge_init()
    fake_scanner = MagicMock()
    fake_scanner.run.return_value = _fake_scan_result(count=1)
    fake_scanner.__str__ = lambda self: "fake_scanner"
    monkeypatch.setattr(scan, "get_scanner", lambda name: fake_scanner)
    monkeypatch.setattr(scan, "SCANNERS", {"a": None, "b": None})
    monkeypatch.setattr(scan, "filter_candidates", lambda df, scanner_name: (df, df.iloc[0:0]))

    captured = []
    monkeypatch.setattr(scan.bridge, "alert", lambda a: captured.append(a) or True)

    scan.cmd_all(date(2026, 5, 7), tmp_path, apply_filter=True)

    titles = [a.title for a in captured]
    assert any("Scan started" in t for t in titles)
    # logger.log called with scan_suite_complete
    log_calls = alert_bridge._bridge._logger.log.call_args_list
    assert any(c.args[0] == "scan_suite_complete" for c in log_calls), log_calls


def test_cmd_all_continues_after_scanner_exception(
    mock_bridge_init, monkeypatch, tmp_path,
):
    mock_bridge_init()
    call_order = []

    def fake_get_scanner(name):
        m = MagicMock()
        m.__str__ = lambda self: name
        if name == "boom":
            m.run.side_effect = ValueError("kaboom")
        else:
            m.run.return_value = _fake_scan_result(count=1)
        call_order.append(name)
        return m

    monkeypatch.setattr(scan, "get_scanner", fake_get_scanner)
    monkeypatch.setattr(scan, "SCANNERS", {"first": None, "boom": None, "third": None})
    monkeypatch.setattr(scan, "filter_candidates", lambda df, scanner_name: (df, df.iloc[0:0]))

    monkeypatch.setattr(scan.bridge, "alert", lambda a: True)

    scan.cmd_all(date(2026, 5, 7), tmp_path)
    # All three scanners attempted
    assert call_order == ["first", "boom", "third"]


# === CLI flag preservation ===

def test_existing_cli_flags_still_parse(monkeypatch, mock_bridge_init):
    """All existing scan.py CLI surfaces must still accept their args."""
    test_cases = [
        ["scan.py", "list"],
        ["scan.py", "all", "--date", "2026-05-07"],
        ["scan.py", "all", "--no-filter"],
        ["scan.py", "watch", "list"],
        ["scan.py", "--log-level", "DEBUG", "list"],
        ["scan.py", "--output-dir", "/tmp/out", "list"],
    ]
    for argv in test_cases:
        monkeypatch.setattr("sys.argv", argv)
        # Patch the actual scanner work so we don't run anything; we only verify
        # argparse + dispatch don't crash.
        with patch.object(scan, "cmd_list"), \
             patch.object(scan, "cmd_all"), \
             patch.object(scan, "cmd_watch"), \
             patch.object(scan.bridge, "alert", return_value=True):
            scan.main()


def test_cmd_run_returns_dict_with_count_errors_runtime(
    mock_bridge_init, monkeypatch, tmp_path,
):
    mock_bridge_init()
    fake_scanner = MagicMock()
    fake_scanner.run.return_value = _fake_scan_result(count=2)
    fake_scanner.__str__ = lambda self: "fake"
    monkeypatch.setattr(scan, "get_scanner", lambda name: fake_scanner)
    monkeypatch.setattr(scan, "filter_candidates", lambda df, scanner_name: (df, df.iloc[0:0]))
    monkeypatch.setattr(scan.bridge, "alert", lambda a: True)

    result = scan.cmd_run("fake", date(2026, 5, 7), tmp_path)
    assert set(result.keys()) >= {"count", "errors", "runtime_seconds"}
    assert isinstance(result["count"], int)
    assert isinstance(result["errors"], int)
    assert isinstance(result["runtime_seconds"], float)
