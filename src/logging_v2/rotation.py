"""Retention pass for structured logs.

Gzip strategy_bot_YYYY-MM-DD.jsonl files older than grace_days.
Delete strategy_bot_YYYY-MM-DD.jsonl.gz files older than delete_after_days.
Skip log_dir/critical/ entirely (retained forever per Phase 5 spec).
Idempotent: safe to run repeatedly.
"""
from __future__ import annotations

import gzip
import re
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Optional

JSONL_NAME_RE = re.compile(r"^strategy_bot_(\d{4}-\d{2}-\d{2})\.jsonl$")
JSONL_GZ_NAME_RE = re.compile(r"^strategy_bot_(\d{4}-\d{2}-\d{2})\.jsonl\.gz$")


def rotation_pass(
    log_dir: Path,
    grace_days: int = 7,
    delete_after_days: int = 90,
    now: Optional[datetime] = None,
) -> Dict[str, int]:
    """Returns {"gzipped": N, "deleted": N, "skipped_critical": N}."""
    log_dir = Path(log_dir)
    if not log_dir.exists():
        return {"gzipped": 0, "deleted": 0, "skipped_critical": 0}

    now = now or datetime.now(timezone.utc)
    today = now.date()

    out = {"gzipped": 0, "deleted": 0, "skipped_critical": 0}

    critical_dir = log_dir / "critical"
    if critical_dir.exists():
        out["skipped_critical"] = sum(1 for p in critical_dir.iterdir() if p.is_file())

    for p in log_dir.iterdir():
        if not p.is_file():
            continue

        m = JSONL_NAME_RE.match(p.name)
        if m:
            try:
                file_date = date.fromisoformat(m.group(1))
            except ValueError:
                continue
            age_days = (today - file_date).days
            if age_days > grace_days:
                gz_path = p.with_name(p.name + ".gz")
                with open(p, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                p.unlink()
                out["gzipped"] += 1
            continue

        m = JSONL_GZ_NAME_RE.match(p.name)
        if m:
            try:
                file_date = date.fromisoformat(m.group(1))
            except ValueError:
                continue
            age_days = (today - file_date).days
            if age_days > delete_after_days:
                p.unlink()
                out["deleted"] += 1

    return out
