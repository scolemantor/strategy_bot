"""Initial dashboard schema: users, notifications, settings_audit.

Per Q2/Q3 clarifications: NO sessions table (JWT-only) and NO scanner_config
table (Settings page writes back to YAML, not DB).

Revision ID: 001_initial
Revises:
Create Date: 2026-05-08
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("username", sa.String(64), nullable=False, unique=True),
        sa.Column("email", sa.String(255), nullable=False, unique=True),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.func.now(),
        ),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "notifications",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "sent_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.func.now(),
        ),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("title", sa.String(255), nullable=True),
        sa.Column("message", sa.Text, nullable=False),
        sa.Column("priority", sa.Integer, nullable=False, server_default="0"),
        sa.Column("outcome", sa.String(32), nullable=False),
        sa.Column("suppression_reason", sa.String(64), nullable=True),
        sa.Column("pushover_response_status", sa.Integer, nullable=True),
        sa.Column("pushover_response_body", sa.Text, nullable=True),
        sa.CheckConstraint(
            "outcome IN ('dispatched', 'failed', 'suppressed', 'test_mode')",
            name="ck_notifications_outcome",
        ),
    )
    op.create_index("idx_notifications_sent_at", "notifications", ["sent_at"])
    op.create_index(
        "idx_notifications_event_type", "notifications", ["event_type", "sent_at"],
    )
    op.create_index(
        "idx_notifications_outcome", "notifications", ["outcome", "sent_at"],
    )

    op.create_table(
        "settings_audit",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "changed_at", sa.DateTime(timezone=True),
            nullable=False, server_default=sa.func.now(),
        ),
        sa.Column(
            "changed_by", sa.Integer,
            sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
        ),
        sa.Column("scanner_name", sa.String(64), nullable=True),
        sa.Column("field_name", sa.String(64), nullable=True),
        sa.Column("old_value", sa.Text, nullable=True),
        sa.Column("new_value", sa.Text, nullable=True),
    )
    op.create_index(
        "idx_settings_audit_changed_at", "settings_audit", ["changed_at"],
    )


def downgrade() -> None:
    op.drop_index("idx_settings_audit_changed_at", table_name="settings_audit")
    op.drop_table("settings_audit")
    op.drop_index("idx_notifications_outcome", table_name="notifications")
    op.drop_index("idx_notifications_event_type", table_name="notifications")
    op.drop_index("idx_notifications_sent_at", table_name="notifications")
    op.drop_table("notifications")
    op.drop_table("users")
