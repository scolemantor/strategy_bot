"""Phase 7 dead-man-switch / health endpoint.

Two routes:
  GET /api/health        -- live status: alive flag, last_scan_at,
                            last_alert_at, uptime_seconds, checked_at
  GET /api/health/ready  -- 200 if essential configs present,
                            503 otherwise (with missing list)

Reads logs/strategy_bot_*.jsonl to find last_scan_at and last_alert_at by
walking files in reverse-date order and matching event_type prefixes.

Run: python -m src.api.health  (binds 0.0.0.0:8000)
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from fastapi import FastAPI
from fastapi.responses import JSONResponse

log = logging.getLogger(__name__)

START_TIME = time.time()

LOG_DIR = Path("logs")
ESSENTIAL_CONFIGS: Tuple[Path, ...] = (
    Path("config/portfolio_v3.yaml"),
    Path("config/scanner_weights.yaml"),
)
SCAN_EVENT_PREFIXES = ("scanner_complete", "scan_started", "scan_suite_complete")
ALERT_EVENT_PREFIXES = ("alert_dispatched", "alert_test_mode")
SEARCH_LOOKBACK_DAYS = 7

app = FastAPI(title="strategy_bot health", version="0.7.0")


def _find_last_event_timestamp(prefixes: Tuple[str, ...]) -> Optional[str]:
    """Walk recent log files in reverse-date order; return ISO timestamp of
    most recent entry whose event_type starts with any prefix.
    Returns None if no match found in the lookback window."""
    if not LOG_DIR.exists():
        return None
    files = sorted(LOG_DIR.glob("strategy_bot_*.jsonl"), reverse=True)[:SEARCH_LOOKBACK_DAYS]
    for path in files:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        for line in reversed(lines):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            et = str(entry.get("event_type", ""))
            if any(et.startswith(p) for p in prefixes):
                ts = entry.get("timestamp")
                if isinstance(ts, str):
                    return ts
    return None


@app.get("/api/health")
def health() -> dict:
    return {
        "status": "alive",
        "last_scan_at": _find_last_event_timestamp(SCAN_EVENT_PREFIXES),
        "last_alert_at": _find_last_event_timestamp(ALERT_EVENT_PREFIXES),
        "uptime_seconds": round(time.time() - START_TIME, 3),
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/health/ready")
def ready():
    missing = [str(p) for p in ESSENTIAL_CONFIGS if not p.exists()]
    if missing:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "missing_configs": missing},
        )
    return {"status": "ready", "configs_present": [str(p) for p in ESSENTIAL_CONFIGS]}


def main() -> None:
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    main()
