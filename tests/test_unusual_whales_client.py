"""Tests for src/unusual_whales_client.py.

Coverage focus: the wrapper logic (auth header injection, 429 backoff,
cache hit/miss, run_date in cache key, NotImplementedError messaging
for stubbed endpoints) — not UW's response shape, which we treat as
real and document in the client docstring.

Network is fully mocked via unittest.mock.patch on requests.get; no
real HTTP, no real disk reads (cache dir is overridden via
monkeypatch on the module attribute).
"""
from __future__ import annotations

import json
import time
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from src import unusual_whales_client as uwc


# --- Fixtures -----------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolate_module_state(tmp_path, monkeypatch):
    """Each test gets a fresh cache dir + reset rate-limit clock."""
    monkeypatch.setattr(uwc, "CACHE_DIR", tmp_path / "uw_cache")
    monkeypatch.setattr(uwc, "_last_request_time", 0.0)
    # Disable the rate-limit sleep so tests don't crawl
    monkeypatch.setattr(uwc, "RATE_LIMIT_DELAY", 0.0)
    monkeypatch.setenv("UNUSUAL_WHALES_API_TOKEN", "test-token-secret-do-not-log")
    yield


def _mock_response(
    status_code: int = 200,
    body=None,
    headers: dict | None = None,
):
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.json.return_value = body if body is not None else []
    if status_code >= 400:
        err = requests.HTTPError(f"HTTP {status_code}")
        err.response = resp
        resp.raise_for_status.side_effect = err
    else:
        resp.raise_for_status.return_value = None
    return resp


# --- Auth header injection ----------------------------------------------

def test_auth_header_uses_env_token():
    captured = {}

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["headers"] = kwargs.get("headers", {})
        captured["params"] = kwargs.get("params", {})
        return _mock_response(200, [])

    with patch("src.unusual_whales_client.requests.get", side_effect=fake_get):
        uwc.get_flow_alerts(run_date=date(2026, 5, 12))

    assert captured["url"] == "https://api.unusualwhales.com/api/option-trades/flow-alerts"
    assert captured["headers"]["Authorization"] == "Bearer test-token-secret-do-not-log"
    assert captured["headers"]["User-Agent"] == "OakStrategyBot research"
    assert captured["params"]["min_premium"] == 100_000


def test_missing_token_raises_helpful_message(monkeypatch):
    monkeypatch.delenv("UNUSUAL_WHALES_API_TOKEN", raising=False)
    with pytest.raises(RuntimeError) as exc_info:
        uwc.get_flow_alerts(run_date=date(2026, 5, 12))
    assert "UNUSUAL_WHALES_API_TOKEN" in str(exc_info.value)
    # Message should NOT include the literal token (which is empty here
    # anyway, but verify the convention holds)
    assert "Bearer" not in str(exc_info.value)


# --- 429 backoff --------------------------------------------------------

def test_429_retries_with_backoff_then_succeeds(monkeypatch):
    # Fast-forward sleep so test isn't slow
    sleep_calls = []
    monkeypatch.setattr(uwc.time, "sleep", lambda s: sleep_calls.append(s))

    # First two 429s, then success
    responses_iter = iter([
        _mock_response(429, headers={"Retry-After": "2"}),
        _mock_response(429),
        _mock_response(200, [{"ticker": "AAPL", "premium": 250_000, "type": "call"}]),
    ])

    with patch(
        "src.unusual_whales_client.requests.get",
        side_effect=lambda *a, **kw: next(responses_iter),
    ):
        result = uwc.get_flow_alerts(run_date=date(2026, 5, 12))

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["ticker"] == "AAPL"
    # First retry honored Retry-After=2; second used the backoff schedule
    assert 2 in sleep_calls
    assert any(s in sleep_calls for s in (1, 2, 4, 8))


def test_429_exhaust_retries_raises(monkeypatch):
    monkeypatch.setattr(uwc.time, "sleep", lambda s: None)
    responses = [_mock_response(429) for _ in range(20)]
    responses_iter = iter(responses)

    with patch(
        "src.unusual_whales_client.requests.get",
        side_effect=lambda *a, **kw: next(responses_iter),
    ):
        with pytest.raises(requests.HTTPError):
            uwc.get_flow_alerts(run_date=date(2026, 5, 12))


# --- Cache hit / miss ---------------------------------------------------

def test_cache_hit_skips_second_http_call():
    call_count = {"n": 0}

    def fake_get(url, **kwargs):
        call_count["n"] += 1
        return _mock_response(200, [{"ticker": "MSFT", "premium": 500_000}])

    with patch("src.unusual_whales_client.requests.get", side_effect=fake_get):
        first = uwc.get_flow_alerts(run_date=date(2026, 5, 12))
        second = uwc.get_flow_alerts(run_date=date(2026, 5, 12))

    assert call_count["n"] == 1, "second call should hit cache, not HTTP"
    assert first == second


def test_cache_key_includes_run_date():
    # Different run_dates should produce different cache files = 2 HTTP calls
    call_count = {"n": 0}

    def fake_get(url, **kwargs):
        call_count["n"] += 1
        return _mock_response(200, [])

    with patch("src.unusual_whales_client.requests.get", side_effect=fake_get):
        uwc.get_flow_alerts(run_date=date(2026, 5, 12))
        uwc.get_flow_alerts(run_date=date(2026, 5, 13))

    assert call_count["n"] == 2, "different run_date must invalidate cache"


def test_cache_expires_after_ttl(monkeypatch):
    """flow-alerts TTL is 60s; advance mtime past it and verify re-fetch."""
    call_count = {"n": 0}

    def fake_get(url, **kwargs):
        call_count["n"] += 1
        return _mock_response(200, [])

    with patch("src.unusual_whales_client.requests.get", side_effect=fake_get):
        uwc.get_flow_alerts(run_date=date(2026, 5, 12))
        # Backdate the cache file's mtime to 65s ago
        for cache_file in uwc.CACHE_DIR.glob("*.json"):
            old = time.time() - 65
            import os
            os.utime(cache_file, (old, old))
        uwc.get_flow_alerts(run_date=date(2026, 5, 12))

    assert call_count["n"] == 2, "cache should expire after TTL"


# --- Response shape normalization --------------------------------------

def test_dict_envelope_unwrapped_to_list():
    """UW sometimes wraps the array in {'data': [...]}; normalize to list."""
    with patch(
        "src.unusual_whales_client.requests.get",
        return_value=_mock_response(200, {"data": [{"ticker": "NVDA"}]}),
    ):
        result = uwc.get_flow_alerts(run_date=date(2026, 5, 12))
    assert result == [{"ticker": "NVDA"}]


def test_unexpected_response_shape_returns_empty_list():
    with patch(
        "src.unusual_whales_client.requests.get",
        return_value=_mock_response(200, "not a list or dict-with-data"),
    ):
        result = uwc.get_flow_alerts(run_date=date(2026, 5, 12))
    assert result == []


# --- 401 handling -------------------------------------------------------

def test_401_raises_clear_message_without_token_leak():
    with patch(
        "src.unusual_whales_client.requests.get",
        return_value=_mock_response(401),
    ):
        with pytest.raises(RuntimeError) as exc_info:
            uwc.get_flow_alerts(run_date=date(2026, 5, 12))
    msg = str(exc_info.value)
    assert "auth failed" in msg.lower() or "401" in msg
    assert "test-token-secret-do-not-log" not in msg
    assert "Bearer" not in msg


# --- Stubbed endpoints --------------------------------------------------

@pytest.mark.parametrize("fn,phase_marker", [
    (lambda: uwc.get_ticker_flow("AAPL"), "Phase 4g.5"),
    (lambda: uwc.get_ticker_gex("AAPL"), "Phase 8d"),
    (lambda: uwc.get_congressional_trades(), "Phase 4g.1b"),
    (lambda: uwc.get_dark_pool_prints(), "Phase 4g.5"),
    (lambda: uwc.get_market_tide(), "no consuming scanner"),
])
def test_stub_endpoints_raise_with_phase_reference(fn, phase_marker):
    with pytest.raises(NotImplementedError) as exc_info:
        fn()
    assert phase_marker in str(exc_info.value)
