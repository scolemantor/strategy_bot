"""Tests for src/alerting/events.py.

Two layers:
  1. Per-constructor structural tests (parametrized) — verify each Alert's
     severity / title / body substrings / dedup_key / source / payload keys.
  2. Cross-cutting invariants — every CRITICAL has a dedup_key, every INFO
     has none, no title exceeds 250 chars, no body exceeds 1024 chars.

No scanner imports. No real I/O. No real Pushover hits.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.alerting import events
from src.alerting.events import (
    auth_failure,
    backtest_complete,
    daily_summary,
    daily_summary_email,
    drawdown_breach,
    kill_switch_triggered,
    new_candidate,
    order_failure,
    rebalance_executed,
    regime_flip,
    scan_started,
    scanner_complete,
    scanner_exception,
    system_startup,
    watchlist_signal,
)

FIXED_TS = datetime(2026, 5, 9, 14, 32, 11, tzinfo=timezone.utc)
FIXED_CLOCK = lambda: FIXED_TS  # noqa: E731


# (func, kwargs, severity, expected_dedup, title_substr, body_substrs, payload_keys)
ALL_CONSTRUCTORS = [
    pytest.param(
        kill_switch_triggered,
        {"reason": "dd_kill", "current_drawdown": 0.30, "hwm_drawdown": 0.40},
        "CRITICAL",
        "kill_switch:dd_kill",
        "KILL SWITCH",
        ["dd_kill", "30.00%", "40.00%"],
        ["reason", "current_drawdown", "hwm_drawdown"],
        id="kill_switch_triggered",
    ),
    pytest.param(
        drawdown_breach,
        {"current_drawdown": 0.15, "threshold": 0.10, "account_value": 180_000.0},
        "CRITICAL",
        "drawdown:2026-05-09",
        "Drawdown breach",
        ["15.00%", "10.00%", "180,000"],
        ["current_drawdown", "threshold", "account_value"],
        id="drawdown_breach",
    ),
    pytest.param(
        order_failure,
        {"symbol": "AAPL", "side": "buy", "qty": 100, "error_message": "rejected"},
        "CRITICAL",
        "order_fail:AAPL:buy:100",
        "Order failed",
        ["AAPL", "buy", "100", "rejected"],
        ["symbol", "side", "qty", "error_message", "alpaca_order_id"],
        id="order_failure",
    ),
    pytest.param(
        auth_failure,
        {"service": "alpaca", "error_message": "401 Unauthorized"},
        "CRITICAL",
        "auth_fail:alpaca",
        "Auth failure",
        ["alpaca", "401 Unauthorized"],
        ["service", "error_message"],
        id="auth_failure",
    ),
    pytest.param(
        scanner_exception,
        {
            "scanner_name": "short_squeeze",
            "exception_class": "ValueError",
            "exception_message": "boom",
            "traceback": "Traceback (most recent call last):\n  File 'x.py'\n    line\nValueError: boom",
        },
        "CRITICAL",
        "scanner_exc:short_squeeze:ValueError",
        "Scanner exception",
        ["short_squeeze", "ValueError", "boom"],
        ["scanner_name", "exception_class", "exception_message", "traceback"],
        id="scanner_exception",
    ),
    pytest.param(
        regime_flip,
        {"from_regime": "risk_on", "to_regime": "risk_off",
         "spy_price": 440.50, "spy_200dma": 445.20},
        "CRITICAL",
        "regime:risk_on:risk_off:2026-05-09",
        "Regime flip",
        ["risk_on", "risk_off", "440.50", "445.20"],
        ["from_regime", "to_regime", "spy_price", "spy_200dma"],
        id="regime_flip",
    ),
    pytest.param(
        daily_summary,
        {"scan_count": 13, "candidates_count": 47, "conflicts_count": 3,
         "watchlist_signals_count": 5, "account_value": 200_000.0,
         "daily_pnl": 1234.56},
        "OPERATIONAL",
        "daily_summary:2026-05-09",
        "Daily summary",
        ["13", "47", "3", "5", "200,000", "1,234.56"],
        ["scan_count", "candidates_count", "conflicts_count",
         "watchlist_signals_count", "account_value", "daily_pnl"],
        id="daily_summary",
    ),
    pytest.param(
        rebalance_executed,
        {"orders_placed": 4, "total_value": 50_000.0,
         "drift_before": 0.08, "drift_after": 0.01},
        "OPERATIONAL",
        "rebalance:2026-05-09",
        "Rebalance executed",
        ["4", "50,000", "8.00%", "1.00%"],
        ["orders_placed", "total_value", "drift_before", "drift_after"],
        id="rebalance_executed",
    ),
    pytest.param(
        scanner_complete,
        {"scanner_name": "short_squeeze", "candidates_count": 12,
         "runtime_seconds": 14.3, "errors_count": 0},
        "OPERATIONAL",
        "scanner_done:short_squeeze:2026-05-09",
        "Scanner done",
        ["short_squeeze", "12", "14.3"],
        ["scanner_name", "candidates_count", "runtime_seconds", "errors_count"],
        id="scanner_complete",
    ),
    pytest.param(
        watchlist_signal,
        {"ticker": "NVDA", "signal_type": "NEW", "scanner": "insider_buying",
         "change_description": "First flag in 30 days"},
        "OPERATIONAL",
        "watchlist:NVDA:NEW:2026-05-09",
        "Watchlist",
        ["NVDA", "NEW", "insider_buying", "First flag"],
        ["ticker", "signal_type", "scanner", "change_description"],
        id="watchlist_signal",
    ),
    pytest.param(
        events.daily_summary_email,
        {
            "scan_count": 13, "candidates_count": 42,
            "conflicts_count": 1, "watchlist_signals_count": 2,
            "top_picks": [{"ticker": "AAPL", "composite_score": 87.4, "scanners_hit": "ib"}],
            "conflicts": [{"ticker": "TSLA", "directions": "bullish, bearish",
                           "scanners_hit": "ib, isc"}],
            "watchlist_deltas": [{"ticker": "NVDA", "signal_type": "NEW",
                                  "scanner": "ib", "change": "fresh"}],
        },
        "OPERATIONAL",
        "daily_summary_email:2026-05-09",
        "Daily summary",
        ["13", "42", "1", "2"],
        ["scan_count", "top_picks", "conflicts", "watchlist_deltas", "attachments"],
        id="daily_summary_email",
    ),
    pytest.param(
        backtest_complete,
        {
            "start_date": date(2024, 5, 1),
            "end_date": date(2025, 4, 30),
            "final_metrics_summary": {"sharpe": 1.21, "cagr": 0.074, "max_dd": -0.172},
        },
        "OPERATIONAL",
        "backtest_done:2025-04-30",
        "Backtest complete",
        ["2024-05-01", "2025-04-30", "sharpe", "1.21"],
        ["start_date", "end_date", "final_metrics_summary"],
        id="backtest_complete",
    ),
    pytest.param(
        system_startup,
        {"version": "1.0.0", "hostname": "test-host"},
        "INFO",
        None,
        "strategy_bot startup",
        ["1.0.0", "test-host"],
        ["version", "hostname"],
        id="system_startup",
    ),
    pytest.param(
        scan_started,
        {"scanner_count": 13},
        "INFO",
        None,
        "Scan started",
        ["13"],
        ["scanner_count"],
        id="scan_started",
    ),
    pytest.param(
        new_candidate,
        {"ticker": "MSFT", "scanner": "breakout_52w", "score": 87.4,
         "reason": "Closed $415, broke 412 52w high"},
        "INFO",
        None,
        "New candidate",
        ["MSFT", "breakout_52w", "87.40", "broke 412"],
        ["ticker", "scanner", "score", "reason"],
        id="new_candidate",
    ),
]


# === per-constructor structural tests ====================================


@pytest.mark.parametrize(
    "func, kwargs, severity, dedup, title_sub, body_subs, payload_keys",
    ALL_CONSTRUCTORS,
)
def test_constructor_returns_alert_with_expected_shape(
    func, kwargs, severity, dedup, title_sub, body_subs, payload_keys,
):
    alert = func(**kwargs, clock=FIXED_CLOCK)
    assert alert.severity == severity
    assert alert.dedup_key == dedup
    assert title_sub in alert.title
    for sub in body_subs:
        assert sub in alert.body, f"missing {sub!r} in body: {alert.body!r}"
    for key in payload_keys:
        assert key in alert.payload, f"missing {key!r} in payload: {alert.payload!r}"
    assert alert.source == f"src.alerting.events.{func.__name__}"
    assert alert.timestamp == FIXED_TS


# === cross-cutting invariants ============================================


def test_all_critical_alerts_have_dedup_key():
    for param in ALL_CONSTRUCTORS:
        func, kwargs, severity, *_ = param.values
        if severity == "CRITICAL":
            alert = func(**kwargs, clock=FIXED_CLOCK)
            assert alert.dedup_key is not None, (
                f"{func.__name__} returned alert without dedup_key"
            )


def test_all_info_alerts_have_no_dedup_key():
    for param in ALL_CONSTRUCTORS:
        func, kwargs, severity, *_ = param.values
        if severity == "INFO":
            alert = func(**kwargs, clock=FIXED_CLOCK)
            assert alert.dedup_key is None, (
                f"{func.__name__} (INFO) unexpectedly has dedup_key={alert.dedup_key}"
            )


def test_titles_under_pushover_limit():
    for param in ALL_CONSTRUCTORS:
        func, kwargs, *_ = param.values
        alert = func(**kwargs, clock=FIXED_CLOCK)
        assert len(alert.title) <= 250, (
            f"{func.__name__} title too long: {len(alert.title)} chars"
        )


def test_bodies_under_pushover_limit():
    for param in ALL_CONSTRUCTORS:
        func, kwargs, *_ = param.values
        alert = func(**kwargs, clock=FIXED_CLOCK)
        assert len(alert.body) <= 1024, (
            f"{func.__name__} body too long: {len(alert.body)} chars"
        )


# === edge cases ==========================================================


def test_scanner_exception_truncates_long_traceback():
    """500-line traceback must produce body <= 1024 chars; full traceback
    survives in payload."""
    long_tb = "\n".join(f"  line {i}" for i in range(500))
    alert = scanner_exception(
        scanner_name="x",
        exception_class="ValueError",
        exception_message="boom",
        traceback=long_tb,
        clock=FIXED_CLOCK,
    )
    assert len(alert.body) <= 1024
    assert alert.payload["traceback"] == long_tb


def test_clock_default_is_now_utc():
    before = datetime.now(timezone.utc)
    alert = system_startup("v1", "host")
    after = datetime.now(timezone.utc)
    assert before <= alert.timestamp <= after
    assert alert.timestamp.tzinfo is not None


def test_order_failure_with_alpaca_order_id():
    alert = order_failure(
        symbol="NVDA", side="sell", qty=50, error_message="filled_partial",
        alpaca_order_id="abc-123", clock=FIXED_CLOCK,
    )
    assert "abc-123" in alert.body
    assert alert.payload["alpaca_order_id"] == "abc-123"


def test_order_failure_without_alpaca_order_id():
    alert = order_failure(
        symbol="NVDA", side="sell", qty=50, error_message="rejected",
        clock=FIXED_CLOCK,
    )
    assert alert.payload["alpaca_order_id"] is None
