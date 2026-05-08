"""ORM models for dashboard tables.

Three tables (Phase 7.5):
  - users               — single seeded user, no registration
  - notifications       — every Pushover send + suppression
  - settings_audit      — record of YAML edits via Settings page

Per Sean's Q2/Q3 clarifications: NO sessions table (JWT-only auth) and
NO scanner_config table (Settings page writes back to YAML, not DB).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    BigInteger, CheckConstraint, DateTime, ForeignKey, Index, Integer,
    String, Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dashboard.api.db import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow,
    )
    last_login_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # outcome: 'dispatched' | 'failed' | 'suppressed' | 'test_mode'
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    # suppression_reason: 'dedup' | 'rate_limit' | 'quiet_hours' | 'event_type_excluded' | NULL
    suppression_reason: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    pushover_response_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    pushover_response_body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "outcome IN ('dispatched', 'failed', 'suppressed', 'test_mode')",
            name="ck_notifications_outcome",
        ),
        Index("idx_notifications_sent_at", "sent_at"),
        Index("idx_notifications_event_type", "event_type", "sent_at"),
        Index("idx_notifications_outcome", "outcome", "sent_at"),
    )


class SettingsAudit(Base):
    __tablename__ = "settings_audit"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utcnow,
    )
    changed_by: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.id"), nullable=True,
    )
    scanner_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    field_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    old_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    new_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
