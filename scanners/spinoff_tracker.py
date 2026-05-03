"""Spinoff tracker scanner.

Surfaces companies that recently spun off from a parent (last 6 months).
The Greenblatt thesis (You Can Be A Stock Market Genius, 1997): spinoffs are
typically small relative to the parent, get distributed to shareholders who
didn't ask for them and don't want them, get sold indiscriminately, creating
forced-selling pressure that pushes price below intrinsic value.

The "selling pressure window" is typically the first 1-3 months post-distribution.
After that, the forced sellers are out and price reflects fundamentals.

Source: SEC Form 10 filings (and variants 10-12B, 10-12G). Form 10 is what a
new spinoff entity files to register itself as a separate publicly-traded
company. We treat the Form 10 filing date as a proxy for "spinoff event" —
actual distribution is usually within 30-60 days of filing.

Three filters narrow the noisy raw Form 10 stream to real corporate spinoffs:
  1. Name patterns: drop LPs, funds, REITs, partnerships
  2. CIK age: drop ancient OTC shells repurposed via reverse merger
  3. Parent cross-reference: real spinoffs have a public parent filing 8-Ks
     about them. If no parent 8-K exists, it's a BDC / private fund / shell.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests

from .base import Scanner, ScanResult, empty_result
from .edgar_client import (
    EDGAR_BASE,
    _headers,
    _rate_limit,
    cik_to_ticker,
    edgar_get,
    load_cik_to_ticker,
)
from .sec_cache import (
    cache_stats,
    is_spinoff_cached,
    load_cached_index,
    load_cached_spinoff,
    save_cached_index,
    save_cached_spinoff,
)

log = logging.getLogger(__name__)

# Same regex as insider_buying — daily form.idx is shared format
ROW_REGEX = re.compile(
    r"^(?P<form>[\w\-/]+(?:\s+\d{1,3}[\w\-/]*)?)"
    r"\s{2,}"
    r"(?P<company>.+?)"
    r"\s{2,}"
    r"(?P<cik>\d{1,10})"
    r"\s+"
    r"(?P<date>\d{8})"
    r"\s+"
    r"(?P<filename>edgar/\S+)"
    r"\s*$"
)

# Form 10 = standard registration of a class of securities (new spinoff entity)
# Form 10-12B = registration under section 12(b) (NYSE/AMEX listings)
# Form 10-12G = registration under section 12(g) (Nasdaq/OTC listings)
SPINOFF_FORM_TYPES = {"10", "10-12B", "10-12G", "10/A", "10-12B/A", "10-12G/A"}


class SpinoffTrackerScanner(Scanner):
    name = "spinoff_tracker"
    description = "Recently spun-off companies (Greenblatt strategy) via SEC Form 10"
    cadence = "daily"

    DEFAULT_LOOKBACK_DAYS = 180
    SELLING_PRESSURE_WINDOW_DAYS = 90

    # Filter 1: drop fund/LP/REIT/partnership names
    EXCLUDE_NAME_PATTERNS = [
        r"\bL\.?P\.?\b",
        r"\bFUND\b",
        r"\bREIT\b",
        r"\bTRUST\b",
        r"\bPARTNERS\b",
        r"\bCAPITAL\b",
        r"\bMASTER\b",
        r"\bFEEDER\b",
    ]

    # Filter 2: minimum CIK (drops shell companies registered in the 1980s-90s)
    MIN_CIK_FOR_SPINOFF = 1_500_000

    # Filter 3: parent cross-reference via EDGAR full-text search
    PARENT_SEARCH_LOOKBACK_DAYS = 365
    EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

    def __init__(self, lookback_days: Optional[int] = None):
        super().__init__()
        self.lookback_days = lookback_days or self.DEFAULT_LOOKBACK_DAYS
        self._current_candidate_cik: Optional[str] = None

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Lookback window: {self.lookback_days} days")
        log.info(f"Cache state at start: {cache_stats()}")

        try:
            ticker_map = load_cik_to_ticker()
        except Exception as e:
            log.exception("Failed to load CIK->ticker mapping")
            return empty_result(self.name, run_date, error=f"ticker map: {e}")

        try:
            filings = self._collect_spinoff_filings(run_date)
        except Exception as e:
            log.exception("Failed to collect spinoff filings")
            return empty_result(self.name, run_date, error=f"index fetch: {e}")

        log.info(f"Found {len(filings)} spinoff-relevant filings in lookback window")

        if not filings:
            return empty_result(self.name, run_date)

        # Filter 1: name patterns
        compiled_excludes = [re.compile(p, re.IGNORECASE) for p in self.EXCLUDE_NAME_PATTERNS]
        before = len(filings)
        filings = [
            f for f in filings
            if not any(pat.search(f["company"]) for pat in compiled_excludes)
        ]
        log.info(
            f"Name filter: {before} -> {len(filings)} "
            f"(removed {before - len(filings)} fund/LP/REIT entities)"
        )

        # Filter 2: CIK age
        before = len(filings)
        filings = [f for f in filings if int(f["cik"]) >= self.MIN_CIK_FOR_SPINOFF]
        log.info(
            f"CIK age filter: {before} -> {len(filings)} "
            f"(removed {before - len(filings)} entities with CIK < {self.MIN_CIK_FOR_SPINOFF:,})"
        )

        # Dedupe BEFORE parent search to save API calls
        by_cik: Dict[str, Dict] = {}
        for f in filings:
            cik = f["cik"]
            existing = by_cik.get(cik)
            if existing is None or f["filing_date"] > existing["filing_date"]:
                by_cik[cik] = f
        log.info(f"Deduped to {len(by_cik)} distinct companies")

        # Filter 3: parent 8-K cross-reference
        log.info(f"Searching EDGAR for parent 8-Ks for {len(by_cik)} candidates...")
        before = len(by_cik)
        survivors: Dict[str, Dict] = {}
        for cik, filing in by_cik.items():
            self._current_candidate_cik = cik
            parent_cik = self._has_parent_announcement(filing["company"], run_date)
            if parent_cik is not None:
                filing["parent_cik"] = parent_cik
                survivors[cik] = filing
                log.debug(f"  KEPT: {filing['company']} (parent CIK {parent_cik})")
            else:
                log.debug(f"  Dropped: {filing['company']} (no parent 8-K found)")
        by_cik = survivors
        log.info(
            f"Parent filter: {before} -> {len(by_cik)} "
            f"(removed {before - len(by_cik)} entities with no public parent)"
        )

        if not by_cik:
            return empty_result(self.name, run_date)

        rows = []
        for cik, filing in by_cik.items():
            ticker = cik_to_ticker(cik, ticker_map)
            parent_cik = filing.get("parent_cik")
            parent_ticker = cik_to_ticker(parent_cik, ticker_map) if parent_cik else None

            days_since = (run_date - filing["filing_date"]).days
            in_pressure_window = days_since <= self.SELLING_PRESSURE_WINDOW_DAYS

            recency_score = max(0, 100 - (days_since / self.lookback_days * 100))
            window_bonus = 25 if in_pressure_window else 0
            ticker_known_bonus = 15 if ticker is not None else 0
            parent_known_bonus = 10 if parent_ticker else 0
            score = recency_score + window_bonus + ticker_known_bonus + parent_known_bonus

            rows.append({
                "ticker": ticker if ticker else "?",
                "company": filing["company"],
                "cik": cik,
                "parent_ticker": parent_ticker or "",
                "parent_cik": parent_cik or "",
                "form_type": filing["form_type"],
                "filing_date": filing["filing_date"].isoformat(),
                "days_since_filing": days_since,
                "in_pressure_window": in_pressure_window,
                "ticker_known": ticker is not None,
                "score": round(score, 2),
                "reason": (
                    f"Form {filing['form_type']} filed {days_since}d ago"
                    + (" (in pressure window)" if in_pressure_window else "")
                    + (f", parent: {parent_ticker}" if parent_ticker
                       else (f", parent CIK {parent_cik}" if parent_cik else ""))
                    + ("" if ticker else " — ticker not yet in SEC map")
                ),
            })

        if not rows:
            return empty_result(self.name, run_date)

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Lookback: {self.lookback_days} days",
                f"Distinct companies after all filters: {len(by_cik)}",
                f"In selling-pressure window: {sum(1 for r in rows if r['in_pressure_window'])}",
                f"Spinoff ticker known: {sum(1 for r in rows if r['ticker_known'])}",
                f"Parent ticker known: {sum(1 for r in rows if r['parent_ticker'])}",
            ],
        )

    def _has_parent_announcement(self, company_name: str, run_date: date) -> Optional[str]:
        """Find a public-company 8-K mentioning this spinoff entity by name.

        Returns the parent's CIK if found, None otherwise. Returns None on any
        error (treated as "no parent found" — conservative).
        """
        # Strip common corporate suffixes that confuse exact-phrase search
        cleaned = re.sub(
            r",?\s*(Inc\.?|Corp\.?|Corporation|Company|Co\.?|Ltd\.?|LLC|Holdings?|Group|plc)\s*$",
            "",
            company_name,
            flags=re.IGNORECASE,
        ).strip().rstrip(",")

        if not cleaned or len(cleaned) < 4:
            return None

        date_to = run_date.strftime("%Y-%m-%d")
        date_from = (run_date - timedelta(days=self.PARENT_SEARCH_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        params = {
            "q": f'"{cleaned}"',
            "forms": "8-K",
            "dateRange": "custom",
            "startdt": date_from,
            "enddt": date_to,
        }

        try:
            _rate_limit()
            resp = requests.get(
                self.EDGAR_SEARCH_URL,
                params=params,
                headers=_headers(),
                timeout=15,
            )
            if resp.status_code != 200:
                log.debug(f"EDGAR search returned {resp.status_code} for '{cleaned}'")
                return None
            data = resp.json()
        except Exception as e:
            log.debug(f"EDGAR search failed for '{cleaned}': {e}")
            return None

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            return None

        # First hit's filer CIK is our best guess for the parent.
        # Skip any hit whose filer is the spinoff entity itself.
        for hit in hits:
            source = hit.get("_source", {})
            ciks = source.get("ciks", [])
            for cik in ciks:
                cik_padded = str(cik).zfill(10)
                if cik_padded != self._current_candidate_cik:
                    return cik_padded

        return None

    def _collect_spinoff_filings(self, run_date: date) -> List[Dict]:
        end = run_date
        start = run_date - timedelta(days=self.lookback_days)
        all_filings: List[Dict] = []

        cur = start
        while cur <= end:
            if cur.weekday() >= 5:
                cur += timedelta(days=1)
                continue

            cached = load_cached_index(cur)
            if cached is not None:
                spinoffs = [f for f in cached if f["form_type"].upper() in SPINOFF_FORM_TYPES]
                all_filings.extend(spinoffs)
                if spinoffs:
                    log.info(f"  {cur}: {len(cached)} total, {len(spinoffs)} spinoff-relevant [cached]")
                cur += timedelta(days=1)
                continue

            try:
                day_filings = self._fetch_daily_index(cur)
                save_cached_index(cur, day_filings)
                spinoffs = [f for f in day_filings if f["form_type"].upper() in SPINOFF_FORM_TYPES]
                all_filings.extend(spinoffs)
                if spinoffs:
                    log.info(f"  {cur}: {len(day_filings)} total, {len(spinoffs)} spinoff-relevant")
            except requests.HTTPError as e:
                log.warning(f"Daily index for {cur} not available ({e}); skipping")
            cur += timedelta(days=1)

        return all_filings

    def _fetch_daily_index(self, day: date) -> List[Dict]:
        quarter = (day.month - 1) // 3 + 1
        url = (
            f"{EDGAR_BASE}/Archives/edgar/daily-index/"
            f"{day.year}/QTR{quarter}/form.{day.strftime('%Y%m%d')}.idx"
        )
        resp = edgar_get(url)
        text = resp.text

        lines = text.splitlines()
        sep_idx = None
        for i, line in enumerate(lines):
            if line.startswith("---"):
                sep_idx = i
                break
        if sep_idx is None:
            return []

        filings: List[Dict] = []
        for line in lines[sep_idx + 1:]:
            if not line.strip():
                continue
            parsed = self._parse_idx_row(line, day)
            if parsed:
                filings.append(parsed)
        return filings

    def _parse_idx_row(self, line: str, day: date) -> Optional[Dict]:
        m = ROW_REGEX.match(line)
        if not m:
            return None

        form = m.group("form").strip()
        company = m.group("company").strip()
        cik = m.group("cik")
        filename = m.group("filename")

        accession_match = re.search(r"(\d{10}-\d{2}-\d{6})", filename)
        accession = accession_match.group(1) if accession_match else ""

        if accession:
            cik_clean = cik.lstrip("0") or "0"
            accession_no_dashes = accession.replace("-", "")
            filing_index_url = (
                f"{EDGAR_BASE}/Archives/edgar/data/{cik_clean}/"
                f"{accession_no_dashes}/{accession}-index.htm"
            )
        else:
            filing_index_url = f"{EDGAR_BASE}/{filename.lstrip('/')}"

        return {
            "form_type": form,
            "company": company,
            "cik": cik.zfill(10),
            "filing_date": day,
            "accession": accession,
            "filing_index_url": filing_index_url,
        }