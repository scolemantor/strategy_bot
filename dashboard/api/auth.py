"""Auth helpers: bcrypt password hashing + HS256 JWT cookie minting/parsing.

The JWT_SECRET env var is required at import time. Missing secret = hard
crash on startup, which is the right behavior — silent fallback to a
default would let the dashboard launch with a guessable signing key.

Cookie attributes:
  - HttpOnly: yes (prevents JS access)
  - SameSite: Lax (correct for same-origin SPA)
  - Secure: gated by DASHBOARD_COOKIE_SECURE env (default off for plain HTTP
    droplet; flip to 1 once Caddy/nginx + LE is in front).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
from jose import JWTError, jwt

JWT_SECRET = os.environ.get("JWT_SECRET")
JWT_ALGORITHM = "HS256"
COOKIE_NAME = "strategy_bot_session"
COOKIE_MAX_AGE_SECONDS = 7 * 24 * 60 * 60
COOKIE_SECURE = os.getenv("DASHBOARD_COOKIE_SECURE", "0") == "1"


def require_jwt_secret() -> str:
    """Called at app startup. Crashes loudly if JWT_SECRET unset."""
    if not JWT_SECRET:
        raise RuntimeError(
            "JWT_SECRET env var not set. Generate one with `openssl rand -hex 32` "
            "and add to .env before starting the dashboard."
        )
    return JWT_SECRET


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError):
        return False


def mint_token(user_id: int, username: str) -> str:
    secret = require_jwt_secret()
    payload = {
        "sub": str(user_id),
        "username": username,
        "exp": datetime.now(timezone.utc) + timedelta(seconds=COOKIE_MAX_AGE_SECONDS),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, secret, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    """Returns the decoded payload or None if invalid/expired."""
    secret = require_jwt_secret()
    try:
        return jwt.decode(token, secret, algorithms=[JWT_ALGORITHM])
    except JWTError:
        return None
