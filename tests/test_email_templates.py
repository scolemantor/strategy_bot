"""Tests for src/alerting/email_templates.py."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from src.alerting import Alert
from src.alerting.email_templates import (
    ACCOUNT_PLACEHOLDER, EMPTY_SECTION_PLACEHOLDER, WATCHLIST_EMPTY_PLACEHOLDER,
    render_daily_summary_html, render_daily_summary_text,
)

FIXED_TS = datetime(2026, 5, 9, 14, 32, tzinfo=timezone.utc)


def _alert_with_payload(**payload: Any) -> Alert:
    base = {
        "scan_count": 13, "candidates_count": 42,
        "conflicts_count": 1, "watchlist_signals_count": 2,
        "top_picks": [], "conflicts": [], "watchlist_deltas": [],
    }
    base.update(payload)
    return Alert(
        severity="OPERATIONAL",
        title="Daily summary 2026-05-09",
        body="preview",
        timestamp=FIXED_TS,
        source="tests.test_email_templates",
        payload=base,
    )


def test_render_html_includes_all_sections():
    a = _alert_with_payload(
        top_picks=[{"ticker": "AAPL", "composite_score": 87.4, "scanners_hit": "x"}],
        conflicts=[{"ticker": "TSLA", "directions": "bullish, bearish", "scanners_hit": "y"}],
        watchlist_deltas=[{"ticker": "NVDA", "signal_type": "NEW", "scanner": "ib", "change": "fresh"}],
    )
    html = render_daily_summary_html(a)
    for section in ("Top picks", "Conflicts", "Watchlist deltas", "Account state", "automated"):
        assert section in html, f"missing section: {section}"


def test_render_text_includes_all_sections():
    a = _alert_with_payload(
        top_picks=[{"ticker": "AAPL", "composite_score": 87.4, "scanners_hit": "x"}],
        conflicts=[{"ticker": "TSLA", "directions": "bullish, bearish", "scanners_hit": "y"}],
        watchlist_deltas=[{"ticker": "NVDA", "signal_type": "NEW", "scanner": "ib", "change": "fresh"}],
    )
    text = render_daily_summary_text(a)
    for section in ("Top picks", "Conflicts", "Watchlist deltas", "Account state", "Automated"):
        assert section in text, f"missing section: {section}"


def test_render_html_color_codes_conflicts():
    a = _alert_with_payload(
        top_picks=[
            {"ticker": "AAPL", "composite_score": 87.4, "scanners_hit": "x"},  # green-positive
            {"ticker": "TSLA", "composite_score": -1.2, "scanners_hit": "y"},  # red-negative
        ],
        conflicts=[{"ticker": "ZZZ", "directions": "bullish, bearish", "scanners_hit": "z"}],
    )
    html = render_daily_summary_html(a)
    # green for positive scores
    assert "#0a7a2f" in html
    # red used in conflict row OR negative-score color
    assert ("#a60e0e" in html) or ("fbebeb" in html)


def test_render_with_no_master_ranked_uses_payload(tmp_path: Path):
    a = _alert_with_payload(
        top_picks=[{"ticker": "MSFT", "composite_score": 75.0, "scanners_hit": "ib"}],
    )
    nonexistent = tmp_path / "does_not_exist.csv"
    html = render_daily_summary_html(a, master_ranked_path=nonexistent)
    assert "MSFT" in html
    assert "75.00" in html


def test_render_uses_master_ranked_csv_when_present(tmp_path: Path):
    csv = tmp_path / "master_ranked.csv"
    pd.DataFrame([
        {"ticker": "FROMCSV", "composite_score": 99.9, "scanners_hit": "csv"},
    ]).to_csv(csv, index=False)
    a = _alert_with_payload(
        top_picks=[{"ticker": "FROMPAYLOAD", "composite_score": 1.0, "scanners_hit": "p"}],
    )
    html = render_daily_summary_html(a, master_ranked_path=csv)
    assert "FROMCSV" in html
    assert "FROMPAYLOAD" not in html  # CSV wins over payload


def test_render_html_renders_empty_sections_gracefully():
    a = _alert_with_payload()  # all empty lists
    html = render_daily_summary_html(a)
    assert EMPTY_SECTION_PLACEHOLDER in html
    assert WATCHLIST_EMPTY_PLACEHOLDER in html
    assert ACCOUNT_PLACEHOLDER in html
    assert "<html>" in html or "<!DOCTYPE" in html


def test_render_text_renders_empty_sections_gracefully():
    a = _alert_with_payload()
    text = render_daily_summary_text(a)
    assert EMPTY_SECTION_PLACEHOLDER in text
    assert WATCHLIST_EMPTY_PLACEHOLDER in text
    assert ACCOUNT_PLACEHOLDER in text
