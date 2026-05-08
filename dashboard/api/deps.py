"""FastAPI dependencies for dashboard routes."""
from __future__ import annotations

from typing import Optional

from fastapi import Cookie, Depends, HTTPException, status
from sqlalchemy.orm import Session

from dashboard.api.auth import COOKIE_NAME, decode_token
from dashboard.api.db import get_session
from dashboard.api.models import User


def current_user(
    session_cookie: Optional[str] = Cookie(default=None, alias=COOKIE_NAME),
    db: Session = Depends(get_session),
) -> User:
    """Auth gate. Raises 401 if cookie missing/invalid or user not found."""
    if not session_cookie:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "not authenticated")
    payload = decode_token(session_cookie)
    if not payload or "sub" not in payload:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid session")
    try:
        user_id = int(payload["sub"])
    except (TypeError, ValueError):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "malformed session") from None
    user = db.query(User).filter(User.id == user_id).one_or_none()
    if user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "user not found")
    return user
