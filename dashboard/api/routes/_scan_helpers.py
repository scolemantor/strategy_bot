"""Shared subprocess helper for firing background technical_overlay scans.

Used by both:
  - POST /api/watchlist/entries  (auto-fire on watchlist add)
  - POST /api/ticker/{symbol}/rescan  (manual fire from ticker detail page)

Single source of truth for the four-defense subprocess detachment
applied in commit e0cb0c9 (Phase 8c Issue 2 fix).
"""
from __future__ import annotations

import logging
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

AUTO_SCAN_LOG_PATH = Path("logs/auto_scan.log")


def fire_background_technical_scan(ticker: str) -> bool:
    """Fire a non-blocking background subprocess that runs
    `technical_overlay --tickers <ticker>`.

    Four defenses applied:
      1. sys.executable — guarantees the same Python that's running
         uvicorn, no PATH lookup risk.
      2. cwd="/app" — explicit, doesn't rely on inheritance from the
         entrypoint's `cd ${APP_DIR}`.
      3. start_new_session=True — detaches into POSIX process group
         so SIGTERM to uvicorn doesn't kill the in-flight scan.
      4. stdout/stderr -> logs/auto_scan.log — append-mode log gives
         forward visibility on every spawn.
    Plus PID logged at WARNING level.

    Returns True if Popen succeeded, False if it raised. Failure does
    NOT propagate — callers treat the scan as best-effort."""
    try:
        AUTO_SCAN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        log_fh = open(AUTO_SCAN_LOG_PATH, "a", encoding="utf-8")
        log_fh.write(
            f"\n=== {datetime.now(timezone.utc).isoformat()} START "
            f"ticker={ticker} ===\n"
        )
        log_fh.flush()

        proc = subprocess.Popen(
            [sys.executable, "scan.py", "run", "technical_overlay", "--tickers", ticker],
            stdout=log_fh,
            stderr=log_fh,
            cwd="/app",
            start_new_session=True,
        )
        log.warning(
            f"Auto-scan spawned: ticker={ticker} pid={proc.pid} "
            f"log={AUTO_SCAN_LOG_PATH}"
        )
        return True
    except Exception as e:
        log.warning(
            f"Background technical_overlay --tickers {ticker} failed to spawn: "
            f"{e} (next */15 cron will pick it up)"
        )
        return False
