"""Authentication routes: login, logout, current-user.

Login flow: POST /api/auth/login with {username, password}. If bcrypt verifies,
mint an HS256 JWT (7d expiry) and set as HTTP-only cookie. Logout clears the
cookie. /me decodes the cookie and returns the user record.
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from dashboard.api.auth import (
    COOKIE_MAX_AGE_SECONDS, COOKIE_NAME, COOKIE_SECURE,
    mint_token, verify_password,
)
from dashboard.api.db import get_session
from dashboard.api.deps import current_user
from dashboard.api.models import User
from dashboard.api.schemas import LoginRequest, UserOut

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.post("/login", response_model=UserOut)
def login(
    payload: LoginRequest,
    response: Response,
    db: Session = Depends(get_session),
) -> User:
    user = db.query(User).filter(User.username == payload.username).one_or_none()
    if user is None or not verify_password(payload.password, user.password_hash):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials")

    user.last_login_at = datetime.now(timezone.utc)
    db.commit()

    token = mint_token(user.id, user.username)
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=COOKIE_MAX_AGE_SECONDS,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite="lax",
        path="/",
    )
    return user


@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
def logout(response: Response) -> None:
    response.delete_cookie(
        COOKIE_NAME, httponly=True, secure=COOKIE_SECURE, samesite="lax", path="/",
    )


@router.get("/me", response_model=UserOut)
def me(user: User = Depends(current_user)) -> User:
    return user
