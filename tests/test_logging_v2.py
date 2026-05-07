"""Tests for src/logging_v2/.

All tests use tmp_path so no real disk side effects. No scanner imports.
"""
from __future__ import annotations

import gzip
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.logging_v2 import (
    JsonLinesLogger, REDACTED, _redact, _key_matches_redact,
)
from src.logging_v2 import query as q
from src.logging_v2.rotation import rotation_pass


# === fixtures ===

@pytest.fixture
def fixed_clock():
    fixed = datetime(2026, 5, 9, 14, 32, 11, 234567, tzinfo=timezone.utc)
    return lambda: fixed


@pytest.fixture
def logger(tmp_path, fixed_clock):
    return JsonLinesLogger(tmp_path, auto_rotate=False, clock=fixed_clock)


def _read_lines(path: Path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_test_file(p: Path, content: str = "{}\n"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


# === logger unit tests ===

def test_log_writes_valid_json_line(logger, tmp_path):
    logger.log("scanner_complete", "ok")
    p = tmp_path / "strategy_bot_2026-05-09.jsonl"
    assert p.exists()
    lines = _read_lines(p)
    assert len(lines) == 1
    assert isinstance(lines[0], dict)


def test_log_includes_required_fields(logger, tmp_path):
    logger.log("scanner_complete", "hi", payload={"k": "v"})
    e = _read_lines(tmp_path / "strategy_bot_2026-05-09.jsonl")[0]
    for field in ("timestamp", "level", "source", "event_type", "message", "payload"):
        assert field in e
    assert e["event_type"] == "scanner_complete"
    assert e["message"] == "hi"
    assert e["level"] == "INFO"
    assert e["payload"] == {"k": "v"}


def test_log_creates_log_dir_if_missing(tmp_path, fixed_clock):
    new_dir = tmp_path / "nested" / "logs"
    JsonLinesLogger(new_dir, auto_rotate=False, clock=fixed_clock)
    assert new_dir.exists()


def test_log_creates_critical_subdir(logger, tmp_path):
    assert (tmp_path / "critical").exists()


def test_clock_injection_pins_timestamp(logger, tmp_path):
    logger.log("test", "hi")
    e = _read_lines(tmp_path / "strategy_bot_2026-05-09.jsonl")[0]
    assert e["timestamp"].startswith("2026-05-09T14:32:11")


def test_log_level_convenience_methods(logger, tmp_path):
    logger.info("a", "msg-a")
    logger.warning("b", "msg-b")
    logger.error("c-err", "msg-c")    # critical (contains "err")
    logger.debug("d", "msg-d")
    main_lines = _read_lines(tmp_path / "strategy_bot_2026-05-09.jsonl")
    levels = [l["level"] for l in main_lines]
    assert levels == ["INFO", "WARNING", "ERROR", "DEBUG"]


def test_source_explicit_overrides_inference(logger, tmp_path):
    logger.log("test", "hi", source="explicit_source")
    e = _read_lines(tmp_path / "strategy_bot_2026-05-09.jsonl")[0]
    assert e["source"] == "explicit_source"


def test_source_inferred_from_caller(logger, tmp_path):
    logger.log("test", "hi")
    e = _read_lines(tmp_path / "strategy_bot_2026-05-09.jsonl")[0]
    # Pytest discovers tests as either "test_logging_v2" or "tests.test_logging_v2"
    assert "test_logging_v2" in e["source"]


# === redaction unit tests ===

def test_redact_basic_keys():
    out = _redact({"api_key": "x"}, ("api_key",))
    assert out == {"api_key": REDACTED}


def test_redact_case_insensitive():
    for k in ("API_KEY", "ApiKey", "api_key", "apiKey"):
        out = _redact({k: "x"}, ("api_key",))
        assert out[k] == REDACTED


def test_redact_substring_match():
    out = _redact(
        {"alpaca_secret_v2": "x", "ALPACA_SECRET_KEY": "y"},
        ("alpaca_secret",),
    )
    assert out["alpaca_secret_v2"] == REDACTED
    assert out["ALPACA_SECRET_KEY"] == REDACTED


def test_redact_nested_dict():
    payload = {"creds": {"password": "x", "user": "u"}}
    out = _redact(payload, ("password",))
    assert out["creds"]["password"] == REDACTED
    assert out["creds"]["user"] == "u"


def test_redact_inside_list():
    payload = [{"token": "x"}, {"normal": 1}]
    out = _redact(payload, ("token",))
    assert out[0]["token"] == REDACTED
    assert out[1]["normal"] == 1


def test_redact_does_not_mutate_input():
    original = {"api_key": "x", "nested": {"secret": "y"}}
    snapshot = json.loads(json.dumps(original))
    _redact(original, ("api_key", "secret"))
    assert original == snapshot


def test_redact_preserves_non_sensitive_fields():
    out = _redact({"ticker": "AAPL", "qty": 100}, ("api_key",))
    assert out == {"ticker": "AAPL", "qty": 100}


def test_redact_handles_none_and_empty():
    assert _redact(None, ("a",)) is None
    assert _redact({}, ("a",)) == {}
    assert _redact([], ("a",)) == []


def test_key_matches_redact():
    assert _key_matches_redact("api_key", ("api_key",))
    assert _key_matches_redact("API_KEY", ("api_key",))
    assert _key_matches_redact("my_secret_thing", ("secret",))
    assert not _key_matches_redact("ticker", ("api_key", "secret"))


# === critical-event tests ===

def test_critical_event_writes_to_both_files(logger, tmp_path):
    logger.log("order_placed", "buy 100 AAPL")
    main = tmp_path / "strategy_bot_2026-05-09.jsonl"
    crit = tmp_path / "critical" / "strategy_bot_2026-05-09.jsonl"
    assert main.exists()
    assert crit.exists()
    assert _read_lines(main) == _read_lines(crit)


def test_non_critical_event_writes_only_main(logger, tmp_path):
    logger.log("scanner_complete", "ok")
    main = tmp_path / "strategy_bot_2026-05-09.jsonl"
    crit = tmp_path / "critical" / "strategy_bot_2026-05-09.jsonl"
    assert main.exists()
    assert not crit.exists()


def test_critical_keywords_case_insensitive(logger, tmp_path):
    logger.log("REBALANCE_DONE", "x")
    crit = tmp_path / "critical" / "strategy_bot_2026-05-09.jsonl"
    assert crit.exists()


# === rotation tests ===

def test_rotation_gzips_files_past_grace_days(tmp_path):
    today = date(2026, 5, 9)
    now = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    old_date = today - timedelta(days=8)
    old_path = tmp_path / f"strategy_bot_{old_date.isoformat()}.jsonl"
    _write_test_file(old_path)
    recent_date = today - timedelta(days=5)
    recent_path = tmp_path / f"strategy_bot_{recent_date.isoformat()}.jsonl"
    _write_test_file(recent_path)

    out = rotation_pass(tmp_path, grace_days=7, delete_after_days=90, now=now)
    assert out["gzipped"] == 1
    assert (tmp_path / f"strategy_bot_{old_date.isoformat()}.jsonl.gz").exists()
    assert not old_path.exists()
    assert recent_path.exists()


def test_rotation_skips_critical_dir(tmp_path):
    today = date(2026, 5, 9)
    now = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    crit_dir = tmp_path / "critical"
    crit_dir.mkdir(parents=True)
    old_crit = crit_dir / f"strategy_bot_{(today - timedelta(days=200)).isoformat()}.jsonl"
    _write_test_file(old_crit)
    rotation_pass(tmp_path, grace_days=7, delete_after_days=90, now=now)
    assert old_crit.exists(), "critical files must never be touched"


def test_rotation_deletes_gz_past_delete_after_days(tmp_path):
    today = date(2026, 5, 9)
    now = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    old_date = today - timedelta(days=100)
    gz_path = tmp_path / f"strategy_bot_{old_date.isoformat()}.jsonl.gz"
    gz_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(gz_path, "wb") as f:
        f.write(b"{}\n")

    out = rotation_pass(tmp_path, grace_days=7, delete_after_days=90, now=now)
    assert out["deleted"] == 1
    assert not gz_path.exists()


def test_rotation_idempotent(tmp_path):
    today = date(2026, 5, 9)
    now = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    old_date = today - timedelta(days=8)
    _write_test_file(tmp_path / f"strategy_bot_{old_date.isoformat()}.jsonl")
    first = rotation_pass(tmp_path, grace_days=7, delete_after_days=90, now=now)
    second = rotation_pass(tmp_path, grace_days=7, delete_after_days=90, now=now)
    assert first["gzipped"] == 1
    assert second["gzipped"] == 0


def test_auto_rotate_runs_at_init(tmp_path, fixed_clock):
    old_date = fixed_clock().date() - timedelta(days=8)
    p = tmp_path / f"strategy_bot_{old_date.isoformat()}.jsonl"
    _write_test_file(p)
    JsonLinesLogger(tmp_path, auto_rotate=True, clock=fixed_clock)
    assert (tmp_path / f"strategy_bot_{old_date.isoformat()}.jsonl.gz").exists()
    assert not p.exists()


def test_auto_rotate_false_skips_init_rotation(tmp_path, fixed_clock):
    old_date = fixed_clock().date() - timedelta(days=8)
    p = tmp_path / f"strategy_bot_{old_date.isoformat()}.jsonl"
    _write_test_file(p)
    JsonLinesLogger(tmp_path, auto_rotate=False, clock=fixed_clock)
    assert p.exists()


# === CLI tests ===

@pytest.fixture
def populated_log_dir(tmp_path, fixed_clock):
    log = JsonLinesLogger(tmp_path, auto_rotate=False, clock=fixed_clock)
    log.info("scanner_complete", "msg1", payload={"ticker": "AAPL"})
    log.info("scanner_complete", "msg2", payload={"ticker": "MSFT"})
    log.warning("scanner_warning", "warn", payload={"scanner": "short_squeeze"})
    log.error("scan_failure", "boom", payload={"err": "x"})       # critical (contains "error")
    log.log("order_placed", "buy", payload={"ticker": "NVDA", "qty": 10})  # critical
    return tmp_path


def test_cli_search_filters_by_event_type_substring(populated_log_dir, capsys):
    q.main(["--log-dir", str(populated_log_dir), "search", "--event-type", "scanner"])
    captured = capsys.readouterr()
    lines = [ln for ln in captured.out.splitlines() if ln.strip()]
    # 3 events match: scanner_complete x2, scanner_warning
    assert len(lines) == 3


def test_cli_search_filters_by_payload_field(populated_log_dir, capsys):
    q.main(["--log-dir", str(populated_log_dir), "search", "--payload", "ticker=AAPL"])
    captured = capsys.readouterr()
    lines = [json.loads(ln) for ln in captured.out.splitlines() if ln.strip()]
    assert len(lines) == 1
    assert lines[0]["payload"]["ticker"] == "AAPL"


def test_cli_search_filters_by_level(populated_log_dir, capsys):
    q.main(["--log-dir", str(populated_log_dir), "search", "--level", "ERROR"])
    captured = capsys.readouterr()
    lines = [ln for ln in captured.out.splitlines() if ln.strip()]
    assert len(lines) == 1


def test_cli_search_respects_limit(populated_log_dir, capsys):
    q.main(["--log-dir", str(populated_log_dir), "search", "--limit", "2"])
    captured = capsys.readouterr()
    lines = [ln for ln in captured.out.splitlines() if ln.strip()]
    assert len(lines) == 2


def test_cli_search_include_critical_dedupes(populated_log_dir, capsys):
    """order_placed lands in both main and critical/. Without flag: 1 row.
    With --include-critical: still 1 row (deduped on timestamp+event+message)."""
    q.main(["--log-dir", str(populated_log_dir), "search", "--event-type", "order"])
    n_no = len([ln for ln in capsys.readouterr().out.splitlines() if ln.strip()])

    q.main([
        "--log-dir", str(populated_log_dir), "search",
        "--event-type", "order", "--include-critical",
    ])
    n_yes = len([ln for ln in capsys.readouterr().out.splitlines() if ln.strip()])

    assert n_no == 1
    assert n_yes == 1


def test_cli_summary_counts_by_event_type(populated_log_dir, capsys):
    q.main(["--log-dir", str(populated_log_dir), "summary"])
    out = capsys.readouterr().out
    assert "scanner_complete" in out
    assert "order_placed" in out


def test_cli_maintain_runs_rotation_pass(tmp_path, capsys, fixed_clock):
    old_date = fixed_clock().date() - timedelta(days=8)
    p = tmp_path / f"strategy_bot_{old_date.isoformat()}.jsonl"
    _write_test_file(p)
    q.main(["--log-dir", str(tmp_path), "maintain"])
    assert (tmp_path / f"strategy_bot_{old_date.isoformat()}.jsonl.gz").exists()
