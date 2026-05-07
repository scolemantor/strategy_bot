"""Tests for watchlist.py alerting hook.

Mocks bridge to verify watchlist_signal alerts fire only for NEW/STRONGER
deltas. Constructs synthetic per-day scanner CSVs in tmp_path so the
digest computation has real data to chew on.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.alerting import bridge as alert_bridge
from scanners import watchlist


@pytest.fixture(autouse=True)
def reset_bridge():
    alert_bridge._reset()
    yield
    alert_bridge._reset()


@pytest.fixture
def stub_bridge():
    logger = MagicMock()
    dispatcher = MagicMock()
    dispatcher.dispatch.return_value = True
    alert_bridge.init(dispatcher=dispatcher, logger=logger)
    return dispatcher, logger


@pytest.fixture
def fake_watchlist(monkeypatch):
    """Stub _load_watchlist to return a 1-ticker watchlist (NVDA)."""
    monkeypatch.setattr(watchlist, "_load_watchlist", lambda: {
        "settings": {"stale_days": 14, "delta_threshold_pct": 0.10},
        "tickers": {
            "NVDA": {
                "added_date": "2026-01-01",
                "reason": "test",
                "category": "test",
            },
        },
    })


def _write_scanner_csv(date_dir: Path, scanner_name: str, rows: list) -> None:
    date_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(date_dir / f"{scanner_name}.csv", index=False)


def test_watchlist_fires_signal_for_new_delta(monkeypatch, tmp_path, stub_bridge, fake_watchlist):
    """NVDA appears in today's insider_buying but not yesterday's = NEW."""
    run_date = date(2026, 5, 7)
    today_dir = tmp_path / run_date.isoformat()
    yesterday_dir = tmp_path / (run_date - timedelta(days=1)).isoformat()

    _write_scanner_csv(today_dir, "insider_buying", [
        {"ticker": "NVDA", "score": 80.0, "reason": "fresh insider buys"},
    ])
    yesterday_dir.mkdir(parents=True, exist_ok=True)  # empty

    captured = []
    monkeypatch.setattr(alert_bridge, "alert", lambda a: captured.append(a) or True)

    watchlist.run_digest(run_date, output_dir=tmp_path)

    new_alerts = [
        a for a in captured
        if "Watchlist" in a.title and "NEW" in a.title
    ]
    assert len(new_alerts) == 1, [a.title for a in captured]
    assert "NVDA" in new_alerts[0].title


def test_watchlist_fires_signal_for_stronger_delta(monkeypatch, tmp_path, stub_bridge, fake_watchlist):
    """NVDA score went 50 -> 80 (60% gain, well above 10% threshold) = STRONGER."""
    run_date = date(2026, 5, 7)
    today_dir = tmp_path / run_date.isoformat()
    yesterday_dir = tmp_path / (run_date - timedelta(days=1)).isoformat()

    _write_scanner_csv(today_dir, "insider_buying", [
        {"ticker": "NVDA", "score": 80.0, "reason": "more buys"},
    ])
    _write_scanner_csv(yesterday_dir, "insider_buying", [
        {"ticker": "NVDA", "score": 50.0, "reason": "some buys"},
    ])

    captured = []
    monkeypatch.setattr(alert_bridge, "alert", lambda a: captured.append(a) or True)

    watchlist.run_digest(run_date, output_dir=tmp_path)

    stronger_alerts = [
        a for a in captured
        if "Watchlist" in a.title and "STRONGER" in a.title
    ]
    assert len(stronger_alerts) == 1, [a.title for a in captured]


def test_watchlist_no_signal_for_same_or_weaker(monkeypatch, tmp_path, stub_bridge, fake_watchlist):
    """NVDA same score yesterday + today (no delta) = SAME, no alert."""
    run_date = date(2026, 5, 7)
    today_dir = tmp_path / run_date.isoformat()
    yesterday_dir = tmp_path / (run_date - timedelta(days=1)).isoformat()

    _write_scanner_csv(today_dir, "insider_buying", [
        {"ticker": "NVDA", "score": 50.0, "reason": "x"},
    ])
    _write_scanner_csv(yesterday_dir, "insider_buying", [
        {"ticker": "NVDA", "score": 50.0, "reason": "x"},
    ])

    captured = []
    monkeypatch.setattr(alert_bridge, "alert", lambda a: captured.append(a) or True)

    watchlist.run_digest(run_date, output_dir=tmp_path)

    watchlist_alerts = [a for a in captured if "Watchlist" in a.title]
    assert len(watchlist_alerts) == 0, [a.title for a in watchlist_alerts]


def test_watchlist_handles_alerting_failure_gracefully(
    monkeypatch, tmp_path, stub_bridge, fake_watchlist,
):
    """Force bridge.alert to raise; run_digest must still return digest_df."""
    run_date = date(2026, 5, 7)
    today_dir = tmp_path / run_date.isoformat()
    _write_scanner_csv(today_dir, "insider_buying", [
        {"ticker": "NVDA", "score": 80.0, "reason": "x"},
    ])

    monkeypatch.setattr(
        alert_bridge, "alert",
        lambda a: (_ for _ in ()).throw(RuntimeError("alert exploded")),
    )

    digest_df = watchlist.run_digest(run_date, output_dir=tmp_path)
    assert isinstance(digest_df, pd.DataFrame)
    assert not digest_df.empty
