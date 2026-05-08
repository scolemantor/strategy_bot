"""Settings routes — read/write config/scanner_weights.yaml.

GET /api/settings              — list all scanners with weight/direction/
                                  category/enabled/last_updated
PUT /api/settings/scanner/{name} — body {weight?, enabled?}; writes back
                                  to YAML atomically (temp + rename) and
                                  records each changed field in
                                  settings_audit.

Per Q3: scan pipeline never reads from DB. The YAML stays the single
source of truth; the dashboard just gives you a nicer way to edit it.
'enabled' field is honored by meta_ranker (false -> treated as weight=0).

YAML edits use ruamel.yaml round-trip mode so inline comments — including
the dated tuning comments like "raised 2026-05-08 from 1.2 — ..." —
survive the rewrite.
"""
from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from dashboard.api.db import get_session
from dashboard.api.deps import current_user
from dashboard.api.models import SettingsAudit, User
from dashboard.api.schemas import (
    ScannerSetting, ScannerSettingUpdate, SettingsResponse,
)

YAML_PATH = Path("config/scanner_weights.yaml")

router = APIRouter(prefix="/api/settings", tags=["settings"])


def _load_yaml():
    from ruamel.yaml import YAML
    yaml = YAML()
    yaml.preserve_quotes = True
    with open(YAML_PATH, "r", encoding="utf-8") as f:
        return yaml.load(f), yaml


def _atomic_dump(data, yaml) -> None:
    """Write to a temp file alongside the target, then rename. Atomic on
    POSIX; on Windows the rename overwrites cleanly with os.replace."""
    YAML_PATH.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(YAML_PATH.parent), prefix=".scanner_weights.", suffix=".yaml.tmp",
    )
    os.close(fd)
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            yaml.dump(data, f)
        os.replace(tmp_path, YAML_PATH)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _last_audit_for_scanner(db: Session, scanner_name: str) -> Optional[SettingsAudit]:
    return db.execute(
        select(SettingsAudit)
        .where(SettingsAudit.scanner_name == scanner_name)
        .order_by(SettingsAudit.changed_at.desc())
        .limit(1),
    ).scalar()


@router.get("", response_model=SettingsResponse)
def get_settings(
    _: User = Depends(current_user),
    db: Session = Depends(get_session),
) -> SettingsResponse:
    if not YAML_PATH.exists():
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "scanner_weights.yaml missing")
    data, _yaml = _load_yaml()
    scanners_cfg = data.get("scanners", {}) or {}

    out: list[ScannerSetting] = []
    for name, cfg in scanners_cfg.items():
        last = _last_audit_for_scanner(db, name)
        last_user_username = None
        if last is not None and last.changed_by is not None:
            user = db.execute(select(User).where(User.id == last.changed_by)).scalar()
            if user is not None:
                last_user_username = user.username
        out.append(ScannerSetting(
            name=name,
            enabled=bool(cfg.get("enabled", True)),
            weight=float(cfg.get("weight", 0.0)),
            direction=str(cfg.get("direction", "neutral")),
            category=str(cfg.get("category", "other")),
            last_updated=last.changed_at if last is not None else None,
            last_updated_by=last_user_username,
        ))
    return SettingsResponse(scanners=out)


@router.put("/scanner/{name}", response_model=ScannerSetting)
def update_scanner(
    name: str,
    payload: ScannerSettingUpdate,
    user: User = Depends(current_user),
    db: Session = Depends(get_session),
) -> ScannerSetting:
    data, yaml = _load_yaml()
    scanners_cfg = data.get("scanners", {}) or {}
    if name not in scanners_cfg:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"unknown scanner: {name}")
    cfg = scanners_cfg[name]

    audit_rows: list[SettingsAudit] = []
    now = datetime.now(timezone.utc)

    if payload.weight is not None and float(payload.weight) != float(cfg.get("weight", 0.0)):
        old = cfg.get("weight", 0.0)
        cfg["weight"] = float(payload.weight)
        audit_rows.append(SettingsAudit(
            changed_at=now, changed_by=user.id, scanner_name=name,
            field_name="weight", old_value=str(old), new_value=str(payload.weight),
        ))

    if payload.enabled is not None and bool(payload.enabled) != bool(cfg.get("enabled", True)):
        old = cfg.get("enabled", True)
        cfg["enabled"] = bool(payload.enabled)
        audit_rows.append(SettingsAudit(
            changed_at=now, changed_by=user.id, scanner_name=name,
            field_name="enabled", old_value=str(old), new_value=str(payload.enabled),
        ))

    if not audit_rows:
        # No-op update; return current state
        return ScannerSetting(
            name=name, enabled=bool(cfg.get("enabled", True)),
            weight=float(cfg.get("weight", 0.0)),
            direction=str(cfg.get("direction", "neutral")),
            category=str(cfg.get("category", "other")),
            last_updated=None, last_updated_by=None,
        )

    _atomic_dump(data, yaml)
    db.add_all(audit_rows)
    db.commit()

    return ScannerSetting(
        name=name, enabled=bool(cfg.get("enabled", True)),
        weight=float(cfg.get("weight", 0.0)),
        direction=str(cfg.get("direction", "neutral")),
        category=str(cfg.get("category", "other")),
        last_updated=now, last_updated_by=user.username,
    )


@router.get("/audit")
def get_audit_log(
    _: User = Depends(current_user),
    db: Session = Depends(get_session),
) -> dict:
    rows = db.execute(
        select(SettingsAudit).order_by(SettingsAudit.changed_at.desc()).limit(50),
    ).scalars().all()
    items = []
    for r in rows:
        username = None
        if r.changed_by is not None:
            user = db.execute(select(User).where(User.id == r.changed_by)).scalar()
            if user is not None:
                username = user.username
        items.append({
            "changed_at": r.changed_at.isoformat(),
            "changed_by": username,
            "scanner_name": r.scanner_name,
            "field_name": r.field_name,
            "old_value": r.old_value,
            "new_value": r.new_value,
        })
    return {"items": items}
