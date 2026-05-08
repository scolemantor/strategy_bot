"""SQLAlchemy engine + session factory for the dashboard.

The DATABASE_URL env var is the single source of truth. Constructed in
docker-compose.yml as postgresql://strategy_bot:$POSTGRES_PASSWORD@postgres:5432/strategy_bot
for production, can be overridden for local dev with sqlite:////tmp/dev.db
or similar.

Engine + sessionmaker are module-level singletons (FastAPI dependency
injection takes care of per-request session lifetime via get_session).
"""
from __future__ import annotations

import os
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    """Single declarative base for all dashboard ORM models."""


def _engine_kwargs(url: str) -> dict:
    if url.startswith("sqlite"):
        return {"connect_args": {"check_same_thread": False}}
    return {"pool_pre_ping": True, "pool_size": 5, "max_overflow": 10}


DATABASE_URL = os.environ.get("DATABASE_URL", "")
_engine = None
_SessionLocal = None


def get_engine():
    global _engine
    if _engine is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL not set")
        _engine = create_engine(DATABASE_URL, **_engine_kwargs(DATABASE_URL))
    return _engine


def get_sessionmaker():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=get_engine(), autocommit=False, autoflush=False, expire_on_commit=False,
        )
    return _SessionLocal


def get_session() -> Iterator[Session]:
    """FastAPI dependency: yields a request-scoped session."""
    SessionLocal = get_sessionmaker()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
