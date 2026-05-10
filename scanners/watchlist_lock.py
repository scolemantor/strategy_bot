"""File-lock context manager for watchlist.yaml writes.

POSIX: uses fcntl.flock(LOCK_EX) on a sentinel file in data_cache/.
Windows: no-op fallback (single-user dev only). The lock prevents two
concurrent writers (e.g. dashboard POST /add + cron-driven add_ticker)
from racing on read-modify-write of the YAML.

Usage:
    from scanners.watchlist_lock import watchlist_lock
    with watchlist_lock():
        data = _load_watchlist()
        ...mutate...
        _save_watchlist(data)
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path

log = logging.getLogger(__name__)

LOCK_DIR = Path("data_cache")
LOCK_PATH = LOCK_DIR / ".watchlist.lock"


@contextmanager
def watchlist_lock(timeout_seconds: float = 5.0):
    """Acquire exclusive lock on the watchlist file. Best-effort on
    Windows (no fcntl)."""
    try:
        import fcntl  # POSIX only
    except ImportError:
        log.debug("fcntl unavailable (Windows); proceeding without lock")
        yield
        return

    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    fd = None
    try:
        fd = os.open(str(LOCK_PATH), os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        if fd is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                os.close(fd)
            except Exception:
                pass
