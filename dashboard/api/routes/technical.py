"""Technical detail route — Phase 8a.

GET /api/technical/{ticker}
  Read the per-ticker breakdown JSON written by scanners/technical_overlay
  to data_cache/technical/<TICKER>.json. 404 if no scan has produced
  data for this ticker yet (e.g. just added to the watchlist; next */15
  cron fire will populate it).
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status

from dashboard.api.deps import current_user
from dashboard.api.models import User

router = APIRouter(prefix="/api/technical", tags=["technical"])

TECHNICAL_DETAIL_DIR = Path("data_cache/technical")

log = logging.getLogger(__name__)


@router.get("/{ticker}")
def get_technical(ticker: str, _: User = Depends(current_user)) -> dict:
    sym = ticker.upper().strip()
    if not sym:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "ticker required")
    path = TECHNICAL_DETAIL_DIR / f"{sym}.json"
    if not path.exists():
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"no technical scan yet for {sym} — wait for next */15 cron fire",
        )
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.exception(f"failed to read {path}")
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR, f"read failed: {e}",
        ) from None
