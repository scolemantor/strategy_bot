"""First-run seeding for the dashboard DB.

Called from FastAPI startup event after alembic upgrade has applied schema.
Idempotent: if users table already has rows, does nothing.

Reads DASHBOARD_SEED_USERNAME / DASHBOARD_SEED_EMAIL / DASHBOARD_SEED_PASSWORD
from env. If any of those are missing AND the table is empty, logs a
warning and skips — the dashboard will simply have no usable login
until env is fixed and container restarted.
"""
from __future__ import annotations

import logging
import os

from sqlalchemy.orm import Session

from dashboard.api.auth import hash_password
from dashboard.api.models import User

log = logging.getLogger(__name__)


def seed_user_if_empty(db: Session) -> None:
    if db.query(User).count() > 0:
        log.info("seed: users table non-empty; skipping")
        return

    username = os.environ.get("DASHBOARD_SEED_USERNAME")
    password = os.environ.get("DASHBOARD_SEED_PASSWORD")
    email = os.environ.get("DASHBOARD_SEED_EMAIL")

    if not (username and password):
        log.warning(
            "seed: DASHBOARD_SEED_USERNAME / DASHBOARD_SEED_PASSWORD not set; "
            "skipping user seed (dashboard will have no usable login)"
        )
        return

    if not email:
        email = f"{username}@strategy.local"

    user = User(
        username=username,
        email=email,
        password_hash=hash_password(password),
    )
    db.add(user)
    db.commit()
    log.info(f"seed: created user '{username}' ({email})")
