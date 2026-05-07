"""Tests for meta_ranker.py alerting hook.

Mocks bridge to verify alert/log calls without firing real Pushover or
writing real log files. Constructs synthetic per-scanner CSVs in tmp_path.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.alerting import bridge as alert_bridge
from scanners import meta_ranker


@pytest.fixture(autouse=True)
def reset_bridge():
    alert_bridge._reset()
    yield
    alert_bridge._reset()


@pytest.fixture
def stub_bridge():
    """Install a mock dispatcher + logger so meta_ranker's _emit_alerts
    doesn't try to construct a real PushoverDispatcher."""
    logger = MagicMock()
    dispatcher = MagicMock()
    dispatcher.dispatch.return_value = True
    alert_bridge.init(dispatcher=dispatcher, logger=logger)
    return dispatcher, logger


def _write_scanner_csv(date_dir: Path, name: str, rows: list) -> None:
    date_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(date_dir / f"{name}.csv", index=False)


def _write_minimal_weights_yaml(tmp_path: Path) -> None:
    """meta_ranker reads config/scanner_weights.yaml; we patch its module-level
    constant via monkeypatch in the test instead of writing a real file here."""


def test_meta_ranker_fires_daily_summary(monkeypatch, tmp_path, stub_bridge):
    dispatcher, logger = stub_bridge
    # Stub the config loader to avoid needing a real YAML
    monkeypatch.setattr(meta_ranker, "_load_config", lambda: {
        "multi_scanner_bonus": {1: 1.0, 2: 1.5, 3: 2.0, 4: 2.5, 5: 3.0},
        "category_diversity_bonus": {1: 1.0, 2: 1.2, 3: 1.4, 4: 1.5},
        "scanners": {
            "ib": {"direction": "bullish", "category": "conviction", "weight": 1.0},
            "br": {"direction": "bullish", "category": "technical", "weight": 1.0},
        },
    })

    run_date = date(2026, 5, 7)
    date_dir = tmp_path / run_date.isoformat()
    _write_scanner_csv(date_dir, "ib", [
        {"ticker": "AAPL", "score": 100, "reason": "insiders"},
        {"ticker": "MSFT", "score": 90, "reason": "insiders"},
    ])
    _write_scanner_csv(date_dir, "br", [
        {"ticker": "AAPL", "score": 80, "reason": "breakout"},
    ])

    captured = []
    monkeypatch.setattr(alert_bridge, "alert", lambda a: captured.append(a) or True)

    master_df, conflicts_df, summary_df = meta_ranker.aggregate(run_date, tmp_path)

    titles = [a.title for a in captured]
    assert any("Daily summary" in t for t in titles), titles


def test_meta_ranker_logs_meta_ranker_complete(monkeypatch, tmp_path, stub_bridge):
    dispatcher, logger = stub_bridge
    monkeypatch.setattr(meta_ranker, "_load_config", lambda: {
        "multi_scanner_bonus": {1: 1.0, 2: 1.5},
        "category_diversity_bonus": {1: 1.0, 2: 1.2},
        "scanners": {
            "ib": {"direction": "bullish", "category": "conviction", "weight": 1.0},
        },
    })

    run_date = date(2026, 5, 7)
    date_dir = tmp_path / run_date.isoformat()
    _write_scanner_csv(date_dir, "ib", [
        {"ticker": "AAPL", "score": 50, "reason": "x"},
    ])

    meta_ranker.aggregate(run_date, tmp_path)

    log_calls = logger.log.call_args_list
    event_types = [c.args[0] for c in log_calls if c.args]
    assert "meta_ranker_complete" in event_types, event_types


def test_meta_ranker_handles_alerting_failure_gracefully(monkeypatch, tmp_path, stub_bridge):
    """Force bridge.alert to raise; aggregate() must still return its tuple."""
    monkeypatch.setattr(meta_ranker, "_load_config", lambda: {
        "multi_scanner_bonus": {1: 1.0},
        "category_diversity_bonus": {1: 1.0},
        "scanners": {
            "ib": {"direction": "bullish", "category": "conviction", "weight": 1.0},
        },
    })

    run_date = date(2026, 5, 7)
    date_dir = tmp_path / run_date.isoformat()
    _write_scanner_csv(date_dir, "ib", [
        {"ticker": "AAPL", "score": 50, "reason": "x"},
    ])

    monkeypatch.setattr(
        alert_bridge, "alert",
        lambda a: (_ for _ in ()).throw(RuntimeError("alert exploded")),
    )

    result = meta_ranker.aggregate(run_date, tmp_path)
    # Tuple of 3 DataFrames; not None
    assert isinstance(result, tuple)
    assert len(result) == 3
