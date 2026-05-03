"""On-disk cache for parsed SEC filings and daily indices.

Why two caches:
  - Daily index cache: avoids re-fetching the form.idx for dates we already walked.
    Files are small (~1MB) and SEC publishes them once. Cache forever.
  - Parsed filing cache: each Form 4 takes ~2 SEC requests + XML parse. Cache the
    extracted transactions as JSON keyed by accession number. Once we've parsed a
    filing, we never need to fetch it again.

The cache is local-only and gitignored. Safe to delete to force a full rebuild.
"""
from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
INDEX_CACHE = CACHE_DIR / "sec_daily_index"
FILING_CACHE = CACHE_DIR / "sec_form4_parsed"
SPINOFF_CACHE = CACHE_DIR / "sec_spinoff_parsed"


def _index_cache_path(day: date) -> Path:
    return INDEX_CACHE / f"{day.isoformat()}.json"


def load_cached_index(day: date) -> Optional[List[Dict]]:
    p = _index_cache_path(day)
    if not p.exists():
        return None
    try:
        rows = json.loads(p.read_text())
        for r in rows:
            if isinstance(r.get("filing_date"), str):
                r["filing_date"] = date.fromisoformat(r["filing_date"])
        return rows
    except Exception as e:
        log.warning(f"Failed to load index cache for {day}: {e}")
        return None


def save_cached_index(day: date, rows: List[Dict]) -> None:
    INDEX_CACHE.mkdir(parents=True, exist_ok=True)
    serializable = []
    for r in rows:
        copy = dict(r)
        if isinstance(copy.get("filing_date"), date):
            copy["filing_date"] = copy["filing_date"].isoformat()
        serializable.append(copy)
    _index_cache_path(day).write_text(json.dumps(serializable))


def _filing_cache_path(accession: str) -> Path:
    return FILING_CACHE / f"{accession}.json"


def is_filing_cached(accession: str) -> bool:
    if not accession:
        return False
    return _filing_cache_path(accession).exists()


def load_cached_filing(accession: str) -> Optional[List[Dict]]:
    p = _filing_cache_path(accession)
    if not p.exists():
        return None
    try:
        rows = json.loads(p.read_text())
        for r in rows:
            for fld in ("filing_date", "transaction_date"):
                v = r.get(fld)
                if isinstance(v, str):
                    try:
                        r[fld] = date.fromisoformat(v)
                    except ValueError:
                        r[fld] = None
        return rows
    except Exception as e:
        log.warning(f"Failed to load filing cache for {accession}: {e}")
        return None


def save_cached_filing(accession: str, transactions: List[Dict]) -> None:
    if not accession:
        return
    FILING_CACHE.mkdir(parents=True, exist_ok=True)
    serializable = []
    for r in transactions:
        copy = dict(r)
        for fld in ("filing_date", "transaction_date"):
            v = copy.get(fld)
            if isinstance(v, date):
                copy[fld] = v.isoformat()
        serializable.append(copy)
    _filing_cache_path(accession).write_text(json.dumps(serializable))


def cache_stats() -> Dict[str, int]:
    return {
        "indices_cached": len(list(INDEX_CACHE.glob("*.json"))) if INDEX_CACHE.exists() else 0,
        "filings_cached": len(list(FILING_CACHE.glob("*.json"))) if FILING_CACHE.exists() else 0,
    }
# --- Spinoff cache (scanner #4) ---
# Separate cache so Form 4 and spinoff parses don't collide.

def _spinoff_cache_path(accession: str) -> Path:
    return SPINOFF_CACHE / f"{accession}.json"


def is_spinoff_cached(accession: str) -> bool:
    if not accession:
        return False
    return _spinoff_cache_path(accession).exists()


def load_cached_spinoff(accession: str) -> Optional[Dict]:
    p = _spinoff_cache_path(accession)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
        # Restore date types
        for fld in ("filing_date", "spinoff_date", "first_trade_date"):
            v = data.get(fld)
            if isinstance(v, str):
                try:
                    data[fld] = date.fromisoformat(v)
                except ValueError:
                    data[fld] = None
        return data
    except Exception as e:
        log.warning(f"Failed to load spinoff cache for {accession}: {e}")
        return None


def save_cached_spinoff(accession: str, data: Dict) -> None:
    if not accession:
        return
    SPINOFF_CACHE.mkdir(parents=True, exist_ok=True)
    copy = dict(data)
    for fld in ("filing_date", "spinoff_date", "first_trade_date"):
        v = copy.get(fld)
        if isinstance(v, date):
            copy[fld] = v.isoformat()
    _spinoff_cache_path(accession).write_text(json.dumps(copy))