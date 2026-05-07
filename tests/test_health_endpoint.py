"""Tests for src/api/health.py FastAPI endpoints."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.api import health as health_mod


@pytest.fixture
def client(monkeypatch, tmp_path: Path):
    """Pin LOG_DIR to a tmp directory and ESSENTIAL_CONFIGS to fake paths
    so each test gets a clean environment."""
    log_dir = tmp_path / "logs"
    monkeypatch.setattr(health_mod, "LOG_DIR", log_dir)
    fake_cfg_a = tmp_path / "portfolio_v3.yaml"
    fake_cfg_b = tmp_path / "scanner_weights.yaml"
    monkeypatch.setattr(health_mod, "ESSENTIAL_CONFIGS", (fake_cfg_a, fake_cfg_b))
    return TestClient(health_mod.app), log_dir, (fake_cfg_a, fake_cfg_b)


def _write_log(log_dir: Path, date_str: str, entries: list) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    p = log_dir / f"strategy_bot_{date_str}.jsonl"
    p.write_text("\n".join(json.dumps(e) for e in entries) + "\n", encoding="utf-8")


# === /api/health ===

def test_health_returns_alive_status(client):
    c, log_dir, _ = client
    resp = c.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "alive"
    assert "uptime_seconds" in body
    assert "checked_at" in body


def test_health_includes_uptime(client):
    c, _, _ = client
    body = c.get("/api/health").json()
    assert isinstance(body["uptime_seconds"], (int, float))
    assert body["uptime_seconds"] >= 0


def test_health_finds_last_scan_from_logs(client):
    c, log_dir, _ = client
    _write_log(log_dir, "2026-05-07", [
        {"timestamp": "2026-05-07T12:00:00+00:00", "event_type": "scanner_complete",
         "level": "INFO", "source": "scan", "message": "x", "payload": {}},
        {"timestamp": "2026-05-07T12:30:00+00:00", "event_type": "scanner_complete",
         "level": "INFO", "source": "scan", "message": "y", "payload": {}},
    ])
    body = c.get("/api/health").json()
    assert body["last_scan_at"] == "2026-05-07T12:30:00+00:00"


def test_health_finds_last_alert_from_logs(client):
    c, log_dir, _ = client
    _write_log(log_dir, "2026-05-07", [
        {"timestamp": "2026-05-07T11:00:00+00:00", "event_type": "alert_dispatched",
         "level": "INFO", "source": "alert", "message": "x", "payload": {}},
    ])
    body = c.get("/api/health").json()
    assert body["last_alert_at"] == "2026-05-07T11:00:00+00:00"


def test_health_returns_none_when_no_logs(client):
    c, _, _ = client
    body = c.get("/api/health").json()
    assert body["last_scan_at"] is None
    assert body["last_alert_at"] is None


def test_health_walks_files_in_reverse_date_order(client):
    """Most recent scan event should win even if it's in a different day's file."""
    c, log_dir, _ = client
    _write_log(log_dir, "2026-05-05", [
        {"timestamp": "2026-05-05T09:00:00+00:00", "event_type": "scanner_complete",
         "level": "INFO", "source": "scan", "message": "old", "payload": {}},
    ])
    _write_log(log_dir, "2026-05-07", [
        {"timestamp": "2026-05-07T09:00:00+00:00", "event_type": "scanner_complete",
         "level": "INFO", "source": "scan", "message": "new", "payload": {}},
    ])
    body = c.get("/api/health").json()
    assert body["last_scan_at"] == "2026-05-07T09:00:00+00:00"


# === /api/health/ready ===

def test_ready_returns_200_when_configs_present(client):
    c, _, configs = client
    for p in configs:
        p.write_text("dummy: true\n")
    resp = c.get("/api/health/ready")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert len(body["configs_present"]) == 2


def test_ready_returns_503_when_configs_missing(client):
    c, _, _ = client
    resp = c.get("/api/health/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"


def test_ready_lists_missing_configs(client):
    c, _, configs = client
    # Create only the second config
    configs[1].write_text("dummy: true\n")
    resp = c.get("/api/health/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert len(body["missing_configs"]) == 1
    assert str(configs[0]) in body["missing_configs"]
