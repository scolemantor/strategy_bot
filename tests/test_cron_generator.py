"""Tests for src/deploy/cron_generator.py."""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from src.deploy.cron_generator import (
    load_schedule, validate_schedule, render_crontab, main,
)


VALID_YAML = dedent("""
    timezone: America/New_York
    jobs:
      - name: scan_all
        schedule: "30 8 * * 1-5"
        command: "python scan.py all"
        timeout_minutes: 90
        description: "morning scan"
      - name: meta_ranker
        schedule: "30 9 * * 1-5"
        command: "python -m scanners.meta_ranker --date $(date +%F)"
        timeout_minutes: 5
""").strip()


def _write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "sched.yaml"
    p.write_text(content)
    return p


def test_load_schedule_parses_yaml(tmp_path):
    p = _write_yaml(tmp_path, VALID_YAML)
    sched = load_schedule(p)
    assert sched["timezone"] == "America/New_York"
    assert len(sched["jobs"]) == 2


def test_validate_schedule_passes_for_valid_config(tmp_path):
    sched = load_schedule(_write_yaml(tmp_path, VALID_YAML))
    assert validate_schedule(sched) == []


def test_validate_schedule_catches_missing_timezone(tmp_path):
    bad = dedent("""
        jobs:
          - name: x
            schedule: "0 0 * * *"
            command: "echo"
            timeout_minutes: 1
    """).strip()
    sched = load_schedule(_write_yaml(tmp_path, bad))
    errors = validate_schedule(sched)
    assert any("timezone" in e for e in errors)


def test_validate_schedule_catches_duplicate_job_names(tmp_path):
    bad = dedent("""
        timezone: UTC
        jobs:
          - name: dup
            schedule: "0 0 * * *"
            command: "a"
            timeout_minutes: 1
          - name: dup
            schedule: "0 1 * * *"
            command: "b"
            timeout_minutes: 1
    """).strip()
    sched = load_schedule(_write_yaml(tmp_path, bad))
    errors = validate_schedule(sched)
    assert any("duplicate" in e for e in errors)


def test_validate_schedule_catches_bad_schedule_format(tmp_path):
    bad = dedent("""
        timezone: UTC
        jobs:
          - name: bad
            schedule: "0 0 * *"
            command: "a"
            timeout_minutes: 1
    """).strip()
    sched = load_schedule(_write_yaml(tmp_path, bad))
    errors = validate_schedule(sched)
    assert any("5 fields" in e for e in errors)


def test_validate_schedule_catches_invalid_timeout(tmp_path):
    bad = dedent("""
        timezone: UTC
        jobs:
          - name: bad
            schedule: "0 0 * * *"
            command: "a"
            timeout_minutes: -5
    """).strip()
    sched = load_schedule(_write_yaml(tmp_path, bad))
    errors = validate_schedule(sched)
    assert any("timeout_minutes" in e for e in errors)


def test_render_crontab_includes_all_jobs(tmp_path):
    sched = load_schedule(_write_yaml(tmp_path, VALID_YAML))
    text = render_crontab(sched)
    # One 'timeout' line per job
    assert text.count("timeout 90m") == 1
    assert text.count("timeout 5m") == 1


def test_render_crontab_wraps_with_timeout_and_log_redirect(tmp_path):
    sched = load_schedule(_write_yaml(tmp_path, VALID_YAML))
    text = render_crontab(sched)
    assert "logs/cron/scan_all.log 2>&1" in text
    assert "logs/cron/meta_ranker.log 2>&1" in text
    assert "timeout 90m bash -c" in text


def test_render_crontab_sets_tz_and_path(tmp_path):
    sched = load_schedule(_write_yaml(tmp_path, VALID_YAML))
    text = render_crontab(sched)
    assert "TZ=America/New_York" in text
    assert "PATH=/usr/local/sbin:/usr/local/bin" in text
    assert "SHELL=/bin/bash" in text


def test_main_writes_to_output_file(tmp_path, capsys):
    p = _write_yaml(tmp_path, VALID_YAML)
    out = tmp_path / "crontab.txt"
    rc = main(["--schedule", str(p), "--output", str(out)])
    assert rc == 0
    assert out.exists()
    content = out.read_text()
    assert "scan_all" in content
    assert "meta_ranker" in content


def test_main_validate_only_prints_summary(tmp_path, capsys):
    p = _write_yaml(tmp_path, VALID_YAML)
    rc = main(["--schedule", str(p), "--validate"])
    captured = capsys.readouterr()
    assert rc == 0
    assert "Schedule OK" in captured.err


def test_main_returns_nonzero_on_validation_failure(tmp_path, capsys):
    bad = "timezone: UTC\njobs: []\n"
    p = _write_yaml(tmp_path, bad)
    rc = main(["--schedule", str(p), "--validate"])
    assert rc != 0


def test_main_returns_one_when_schedule_missing(tmp_path, capsys):
    rc = main(["--schedule", str(tmp_path / "nope.yaml")])
    assert rc == 1


def test_canonical_schedule_validates():
    """The shipped config/cron_schedule.yaml must always validate cleanly."""
    sched = load_schedule(Path("config/cron_schedule.yaml"))
    assert validate_schedule(sched) == []
