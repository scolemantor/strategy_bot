"""SEC fundamentals fetcher for investability filter gates 3 + 4.

Provides two functions:
  - get_dilution_data(tickers): % share count change in last 90 days from 10-Q filings
  - get_going_concern_data(tickers): boolean flag from latest 10-K filing text

Both rely on SEC EDGAR submissions API + filing text. Heavy caching is essential
because 10-K text can be 100-500KB per filing. We:
  - Cache list of filings per company forever (filings are immutable once filed)
  - Cache parsed dilution % per company for 30 days (refreshes when new 10-Q lands)
  - Cache going-concern flag per company for 365 days (10-K is annual)

For tickers without recent SEC filings (very recent IPOs, foreign issuers, etc),
both functions return None / False — meaning "no data, don't reject on this gate".
"""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests

from .edgar_client import EDGAR_BASE, EDGAR_DATA_BASE, edgar_get, load_cik_to_ticker

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
DILUTION_CACHE_DIR = CACHE_DIR / "sec_dilution"
GOING_CONCERN_CACHE_DIR = CACHE_DIR / "sec_going_concern"
SHARES_OUTSTANDING_CACHE_DIR = CACHE_DIR / "sec_shares_outstanding"

DILUTION_TTL_DAYS = 30  # 10-Q is quarterly; 30-day cache catches new filings
GOING_CONCERN_TTL_DAYS = 365  # 10-K is annual

# Going-concern language patterns. SEC requires specific language when material
# doubt exists, so we look for variations of these phrases.
GOING_CONCERN_PATTERNS = [
    r"substantial doubt.{0,40}(ability|continue).{0,40}going concern",
    r"going concern.{0,40}substantial doubt",
    r"raise substantial doubt about.{0,80}going concern",
    r"may not be able to continue as a going concern",
    r"raises substantial doubt.{0,40}going concern",
    r"unable to continue as a going concern",
]
GOING_CONCERN_REGEX = re.compile("|".join(GOING_CONCERN_PATTERNS), re.IGNORECASE | re.DOTALL)


# Build ticker → CIK reverse mapping (called once, cached in memory)
_ticker_to_cik_cache: Optional[Dict[str, str]] = None


def _ticker_to_cik(ticker: str) -> Optional[str]:
    """Return zero-padded 10-digit CIK for a ticker, or None if not found."""
    global _ticker_to_cik_cache
    if _ticker_to_cik_cache is None:
        cik_to_ticker_map = load_cik_to_ticker()
        _ticker_to_cik_cache = {v.upper(): k for k, v in cik_to_ticker_map.items()}
    return _ticker_to_cik_cache.get(ticker.upper())


def _fetch_filings_index(cik: str) -> Optional[List[Dict]]:
    """Get the list of recent 10-K and 10-Q filings for a CIK from SEC submissions API."""
    cik_padded = cik.zfill(10)
    url = f"{EDGAR_DATA_BASE}/submissions/CIK{cik_padded}.json"
    try:
        resp = edgar_get(url)
        data = resp.json()
    except Exception as e:
        log.debug(f"  Failed to fetch submissions for CIK {cik}: {e}")
        return None

    filings_section = data.get("filings", {}).get("recent", {})
    forms = filings_section.get("form", [])
    accessions = filings_section.get("accessionNumber", [])
    filing_dates = filings_section.get("filingDate", [])
    primary_docs = filings_section.get("primaryDocument", [])

    out = []
    for i, form in enumerate(forms):
        if form not in ("10-K", "10-K/A", "10-Q", "10-Q/A"):
            continue
        try:
            f_date = date.fromisoformat(filing_dates[i]) if filing_dates[i] else None
        except (ValueError, IndexError):
            f_date = None
        out.append({
            "form_type": form,
            "accession": accessions[i],
            "filing_date": f_date,
            "primary_document": primary_docs[i] if i < len(primary_docs) else None,
        })
    return out


def _fetch_filing_text(cik: str, accession: str, primary_document: str) -> Optional[str]:
    """Download the primary document text for a filing. Returns text or None."""
    cik_clean = str(cik).strip().lstrip("0") or "0"
    accession_no_dashes = accession.replace("-", "")
    url = (
        f"{EDGAR_BASE}/Archives/edgar/data/{cik_clean}/"
        f"{accession_no_dashes}/{primary_document}"
    )
    try:
        resp = edgar_get(url)
        return resp.text
    except Exception as e:
        log.debug(f"  Failed to fetch filing text {accession}: {e}")
        return None


# --- Going concern ---

def _going_concern_cache_path(ticker: str) -> Path:
    return GOING_CONCERN_CACHE_DIR / f"{ticker.upper()}.json"


def _load_cached_going_concern(ticker: str) -> Optional[Dict]:
    p = _going_concern_cache_path(ticker)
    if not p.exists():
        return None
    age_days = (time.time() - p.stat().st_mtime) / 86400
    if age_days > GOING_CONCERN_TTL_DAYS:
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _save_cached_going_concern(ticker: str, data: Dict) -> None:
    GOING_CONCERN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        _going_concern_cache_path(ticker).write_text(json.dumps(data))
    except Exception as e:
        log.debug(f"  Failed to cache going concern for {ticker}: {e}")


def _check_going_concern_for_ticker(ticker: str) -> bool:
    """Return True if ticker's latest 10-K mentions going-concern doubt."""
    cached = _load_cached_going_concern(ticker)
    if cached is not None:
        return cached.get("flag", False)

    cik = _ticker_to_cik(ticker)
    if not cik:
        _save_cached_going_concern(ticker, {"flag": False, "reason": "no CIK mapping"})
        return False

    filings = _fetch_filings_index(cik)
    if not filings:
        _save_cached_going_concern(ticker, {"flag": False, "reason": "no filings index"})
        return False

    # Find latest 10-K
    ten_ks = [f for f in filings if f["form_type"] in ("10-K", "10-K/A")]
    if not ten_ks:
        _save_cached_going_concern(ticker, {"flag": False, "reason": "no 10-K filings"})
        return False

    ten_ks_sorted = sorted(ten_ks, key=lambda f: f.get("filing_date") or date.min, reverse=True)
    latest = ten_ks_sorted[0]
    accession = latest["accession"]
    primary_doc = latest.get("primary_document")
    if not primary_doc:
        _save_cached_going_concern(ticker, {"flag": False, "reason": "no primary document"})
        return False

    text = _fetch_filing_text(cik, accession, primary_doc)
    if text is None:
        _save_cached_going_concern(ticker, {"flag": False, "reason": "fetch failed"})
        return False

    # Strip HTML tags for cleaner regex matching (10-Ks often filed as HTML)
    text_clean = re.sub(r"<[^>]+>", " ", text)
    text_clean = re.sub(r"\s+", " ", text_clean)

    flag = bool(GOING_CONCERN_REGEX.search(text_clean))
    _save_cached_going_concern(ticker, {
        "flag": flag,
        "accession": accession,
        "filing_date": latest["filing_date"].isoformat() if latest["filing_date"] else None,
        "checked_at": datetime.now().isoformat(),
    })
    return flag


def get_going_concern_data(tickers: List[str]) -> Dict[str, bool]:
    """Return {ticker: True if going concern flagged in latest 10-K, False otherwise}."""
    out: Dict[str, bool] = {}
    cached_count = sum(1 for t in tickers if _load_cached_going_concern(t) is not None)
    if len(tickers) - cached_count > 50:
        log.info(f"  Going-concern fetch: {cached_count}/{len(tickers)} cached, "
                 f"{len(tickers) - cached_count} need SEC fetch (~{(len(tickers) - cached_count) * 0.5:.0f}s)")

    for ticker in tickers:
        try:
            out[ticker] = _check_going_concern_for_ticker(ticker)
        except Exception as e:
            log.debug(f"  Going-concern check failed for {ticker}: {e}")
            out[ticker] = False
    return out


# --- Dilution (shares outstanding change over 90 days) ---

def _dilution_cache_path(ticker: str) -> Path:
    return DILUTION_CACHE_DIR / f"{ticker.upper()}.json"


def _load_cached_dilution(ticker: str) -> Optional[Dict]:
    p = _dilution_cache_path(ticker)
    if not p.exists():
        return None
    age_days = (time.time() - p.stat().st_mtime) / 86400
    if age_days > DILUTION_TTL_DAYS:
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _save_cached_dilution(ticker: str, data: Dict) -> None:
    DILUTION_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        _dilution_cache_path(ticker).write_text(json.dumps(data))
    except Exception:
        pass


def _extract_shares_outstanding(text: str) -> Optional[int]:
    """Extract shares-outstanding count from filing text. Returns int or None.

    10-K and 10-Q cover pages typically state shares outstanding near the start.
    Common pattern: 'X,XXX,XXX shares' or 'X,XXX,XXX,XXX shares of common stock'
    appearing within the first 5000 characters of plain text.
    """
    # Strip HTML, normalize whitespace
    text_clean = re.sub(r"<[^>]+>", " ", text)
    text_clean = re.sub(r"\s+", " ", text_clean)

    # Look only in first 8000 chars (cover page region)
    head = text_clean[:8000]

    # Pattern: number with commas + "shares" within ~50 chars + "common stock" or "outstanding"
    patterns = [
        r"([\d,]{8,18})\s+shares.{0,100}common stock.{0,40}outstanding",
        r"([\d,]{8,18})\s+shares.{0,40}outstanding.{0,100}common",
        r"outstanding.{0,80}([\d,]{8,18})\s+shares",
        r"common stock.{0,80}outstanding.{0,40}([\d,]{8,18})",
    ]

    for pattern in patterns:
        m = re.search(pattern, head, re.IGNORECASE)
        if m:
            try:
                shares = int(m.group(1).replace(",", ""))
                if 100_000 < shares < 500_000_000_000:  # sanity range
                    return shares
            except ValueError:
                continue
    return None


def _check_dilution_for_ticker(ticker: str) -> Optional[float]:
    """Return dilution % (e.g. 0.15 = 15% more shares vs 90 days ago) or None."""
    cached = _load_cached_dilution(ticker)
    if cached is not None:
        return cached.get("dilution_pct")

    cik = _ticker_to_cik(ticker)
    if not cik:
        _save_cached_dilution(ticker, {"dilution_pct": None, "reason": "no CIK"})
        return None

    filings = _fetch_filings_index(cik)
    if not filings:
        _save_cached_dilution(ticker, {"dilution_pct": None, "reason": "no filings"})
        return None

    # Get last 4 quarterly-class filings (10-Q + 10-K combined; either has shares-outstanding)
    quarterly = [f for f in filings if f["form_type"] in ("10-Q", "10-K")]
    quarterly_sorted = sorted(quarterly, key=lambda f: f.get("filing_date") or date.min, reverse=True)

    if len(quarterly_sorted) < 2:
        _save_cached_dilution(ticker, {"dilution_pct": None, "reason": "need 2+ filings"})
        return None

    # Compare latest filing to one filed at least 60 days earlier
    latest = quarterly_sorted[0]
    latest_date = latest.get("filing_date")
    if not latest_date:
        _save_cached_dilution(ticker, {"dilution_pct": None, "reason": "no latest date"})
        return None

    # Find a comparison filing at least 60 days older
    comparison = None
    for f in quarterly_sorted[1:]:
        if f.get("filing_date") and (latest_date - f["filing_date"]).days >= 60:
            comparison = f
            break

    if comparison is None:
        _save_cached_dilution(ticker, {"dilution_pct": None, "reason": "no comparison filing 60+ days back"})
        return None

    # Fetch both filings, extract shares outstanding
    latest_text = _fetch_filing_text(cik, latest["accession"], latest.get("primary_document", ""))
    comparison_text = _fetch_filing_text(cik, comparison["accession"], comparison.get("primary_document", ""))

    if not latest_text or not comparison_text:
        _save_cached_dilution(ticker, {"dilution_pct": None, "reason": "filing fetch failed"})
        return None

    latest_shares = _extract_shares_outstanding(latest_text)
    comparison_shares = _extract_shares_outstanding(comparison_text)

    if not latest_shares or not comparison_shares or comparison_shares == 0:
        _save_cached_dilution(ticker, {"dilution_pct": None, "reason": "shares extraction failed"})
        return None

    dilution_pct = (latest_shares - comparison_shares) / comparison_shares
    _save_cached_dilution(ticker, {
        "dilution_pct": dilution_pct,
        "latest_shares": latest_shares,
        "latest_date": latest_date.isoformat(),
        "comparison_shares": comparison_shares,
        "comparison_date": comparison["filing_date"].isoformat(),
        "checked_at": datetime.now().isoformat(),
    })
    return dilution_pct


def get_dilution_data(tickers: List[str]) -> Dict[str, Optional[float]]:
    """Return {ticker: dilution_pct or None}."""
    out: Dict[str, Optional[float]] = {}
    cached_count = sum(1 for t in tickers if _load_cached_dilution(t) is not None)
    if len(tickers) - cached_count > 50:
        log.info(f"  Dilution fetch: {cached_count}/{len(tickers)} cached, "
                 f"{len(tickers) - cached_count} need SEC fetch (~{(len(tickers) - cached_count) * 1.0:.0f}s)")

    for ticker in tickers:
        try:
            out[ticker] = _check_dilution_for_ticker(ticker)
        except Exception as e:
            log.debug(f"  Dilution check failed for {ticker}: {e}")
            out[ticker] = None
    return out