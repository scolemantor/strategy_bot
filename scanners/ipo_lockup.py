"""IPO lockup expiration scanner.

Surfaces recent IPOs whose 180-day lockup period is expiring in the next
30-60 days. The thesis: lockup expirations create supply-side pressure as
insiders and pre-IPO investors finally get to sell. The day-of-expiration
is often the *bottom* (selling pressure already priced in), making it a
potential bottom-fishing setup AFTER stabilization.

ACORNS SLEEVE ONLY. Most lockup bottom-fishing setups keep falling. The
thesis is "lockup is sometimes the bottom", not "always".

Source: stockanalysis.com IPO calendar pages (2025 and 2026), free,
publicly accessible HTML, no JS required. Filters out SPACs (different
lockup mechanics) and obvious micro-cap garbage tier.

Filters:
  1. Drop SPACs by name pattern (Acquisition Corp, Capital Corp, etc.)
  2. Drop names with current price < $5 (penny stock territory)
  3. Lockup expiration in next 30-60 days (forward-looking window)
  4. Return-since-IPO between -80pct and +50pct (filters total collapses
     and explosive winners both — neither is a "bottom" candidate)

Lockup math: simple IPO_date + 180 days. Real lockup terms vary (90/180/365
day flavors, with some structures using "earnings + N days" triggers), but
180 is the dominant convention and good enough for a screening tool.

Honest limits:
  - Lockup terms are NOT always 180 days; some are 90, some 365, some
    earnings-triggered
  - Doesn't read S-1 filings to verify actual lockup language
  - Doesn't check for early-release agreements (underwriter waivers)
  - Recent IPO data quality is often poor — float/insider ownership
    estimates from yfinance are unreliable for IPOs <12 months old
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from .base import Scanner, ScanResult, empty_result

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
IPO_CACHE_DIR = CACHE_DIR / "stockanalysis_ipos"
IPO_CACHE_TTL_HOURS = 24

# IPO calendar URLs (year-by-year listings)
IPO_URLS = {
    2025: "https://stockanalysis.com/ipos/2025/",
    2026: "https://stockanalysis.com/ipos/2026/",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; strategy_bot/1.0; +https://github.com/scolemantor/strategy_bot)",
    "Accept": "text/html",
}

# SPAC name patterns — these dominate the IPO list and have different lockup mechanics
SPAC_PATTERNS = [
    # Direct "Acquisition" patterns
    r"\bAcquisition\b",          # broad — catches "Acquisition Corp", "Acquisitions", "Acquisition Co.", etc.
    r"\bAcquisitions\b",
    # SPAC sponsor name patterns
    r"\bCantor Equity Partners\b",
    r"\bChurchill Capital\b",
    r"\bSilverBox Corp\b",
    # Roman numeral suffix on Corp/Inc — strong SPAC signal
    r"\bCorp(?:oration)?\s+(?:I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV)$",
    r"\bInc\.?\s+(?:I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV)$",
    r"\bCorp\.?\s+(?:I|II|III|IV|V|VI|VII|VIII|IX|X|XI|XII|XIII|XIV|XV)\.?$",
    # Catch-all SPAC-like phrases
    r"\bSPAC\b",
    r"\bMerger Corp\b",
    r"\bMerger Corporation\b",
    r"\bInvestment Corp\b",
    r"\bInvestment Company\b",
    r"\bInvestment Corporation\b",
    r"\bGrowth Corp\b",
    r"\bCapital Corp\b",
    r"\bCapital Partners Corp\b",
    r"\bPartners Corp\b",
    r"\bHoldings Corp\b",
    # Specific tells
    r"\bBlueport\b",
    r"\bTwelve Seas\b",
    r"\bVine Hill\b",
    r"\bTGE Value Creative\b",
    r"\bSocial Commerce Partners\b",
    # Common foreign micro-cap pattern
    r"\bHoldings Limited$",
]
SPAC_REGEX = re.compile("|".join(SPAC_PATTERNS), re.IGNORECASE)


def _is_spac(name: str) -> bool:
    return bool(SPAC_REGEX.search(name or ""))


def _ipo_cache_path(year: int) -> Path:
    return IPO_CACHE_DIR / f"ipos_{year}.json"


def _load_cached_ipos(year: int) -> Optional[List[Dict]]:
    p = _ipo_cache_path(year)
    if not p.exists():
        return None
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    if age_hours > IPO_CACHE_TTL_HOURS:
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _save_cached_ipos(year: int, ipos: List[Dict]) -> None:
    IPO_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        _ipo_cache_path(year).write_text(json.dumps(ipos))
    except Exception as e:
        log.debug(f"Failed to cache IPOs for {year}: {e}")


def _fetch_ipos_for_year(year: int) -> List[Dict]:
    """Scrape stockanalysis.com IPO calendar for a year. Returns list of dicts."""
    cached = _load_cached_ipos(year)
    if cached is not None:
        log.debug(f"Using cached IPO list for {year}")
        return cached

    url = IPO_URLS.get(year)
    if not url:
        log.warning(f"No IPO URL configured for year {year}")
        return []

    log.info(f"Fetching IPO list for {year} from {url}")
    try:
        time.sleep(0.5)
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        log.warning(f"  Failed to fetch IPO list for {year}: {e}")
        return []

    # The IPO table is the first table on the page
    try:
        tables = pd.read_html(StringIO(r.text))
    except Exception as e:
        log.warning(f"  Failed to parse IPO HTML for {year}: {e}")
        return []

    if not tables:
        return []

    df = tables[0]

    # Expected columns: IPO Date, Symbol, Company Name, IPO Price, Current, Return
    # Normalize column names
    df.columns = [str(c).strip() for c in df.columns]
    log.debug(f"  IPO table columns: {list(df.columns)}")

    out = []
    for _, row in df.iterrows():
        try:
            ipo_date_str = str(row.get("IPO Date", ""))
            symbol = str(row.get("Symbol", "")).strip().upper()
            name = str(row.get("Company Name", "")).strip()
            ipo_price_str = str(row.get("IPO Price", ""))
            current_str = str(row.get("Current", ""))

            if not symbol or symbol == "NAN":
                continue

            # Parse IPO date — format is "Dec 23, 2025" style
            try:
                ipo_date = datetime.strptime(ipo_date_str, "%b %d, %Y").date()
            except ValueError:
                try:
                    ipo_date = datetime.strptime(ipo_date_str, "%B %d, %Y").date()
                except ValueError:
                    continue

            # Parse prices
            ipo_price = None
            if ipo_price_str and ipo_price_str not in ("nan", "-", ""):
                try:
                    ipo_price = float(ipo_price_str.replace("$", "").replace(",", ""))
                except ValueError:
                    pass

            current_price = None
            if current_str and current_str not in ("nan", "-", ""):
                try:
                    current_price = float(current_str.replace("$", "").replace(",", ""))
                except ValueError:
                    pass

            out.append({
                "symbol": symbol,
                "name": name,
                "ipo_date": ipo_date.isoformat(),
                "ipo_price": ipo_price,
                "current_price": current_price,
            })
        except Exception as e:
            log.debug(f"  Skipping malformed IPO row: {e}")
            continue

    log.info(f"  Parsed {len(out)} IPOs for {year}")
    _save_cached_ipos(year, out)
    return out


class IpoLockupScanner(Scanner):
    name = "ipo_lockup"
    description = "Recent IPOs with 180-day lockup expiring in next 30-60 days (acorns sleeve)"
    cadence = "weekly"  # IPO data updates daily but lockup windows are slow-moving

    LOCKUP_DAYS = 180
    MIN_DAYS_UNTIL = 0  # include events expiring today
    MAX_DAYS_UNTIL = 60  # forward-looking window
    MIN_CURRENT_PRICE = 5.0
    MIN_RETURN_PCT = -80.0  # below this, name is too broken to be a recovery candidate
    MAX_RETURN_PCT = 50.0  # above this, supply-side pressure thesis doesn't apply

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Lockup convention: {self.LOCKUP_DAYS} days post-IPO")
        log.info(f"Window: {self.MIN_DAYS_UNTIL} to {self.MAX_DAYS_UNTIL} days until expiry")
        log.info(f"Min current price: ${self.MIN_CURRENT_PRICE}")
        log.info(f"Return-since-IPO range: {self.MIN_RETURN_PCT}% to {self.MAX_RETURN_PCT}%")

        # Step 1: load IPO universe (current year + previous year)
        all_ipos: List[Dict] = []
        for year in (run_date.year - 1, run_date.year):
            ipos = _fetch_ipos_for_year(year)
            all_ipos.extend(ipos)

        log.info(f"Total IPOs fetched: {len(all_ipos)}")

        if not all_ipos:
            return empty_result(self.name, run_date, error="no IPO data")

        # Step 2: filter out SPACs
        before = len(all_ipos)
        all_ipos = [ipo for ipo in all_ipos if not _is_spac(ipo["name"])]
        log.info(f"SPAC filter: {before} -> {len(all_ipos)}")

        # Step 3: compute lockup expiration date and days-until
        for ipo in all_ipos:
            ipo_date = date.fromisoformat(ipo["ipo_date"])
            lockup_expiry = ipo_date + timedelta(days=self.LOCKUP_DAYS)
            ipo["lockup_expiry"] = lockup_expiry
            ipo["days_until_expiry"] = (lockup_expiry - run_date).days

        # Step 4: filter to lockup window
        before = len(all_ipos)
        all_ipos = [
            ipo for ipo in all_ipos
            if self.MIN_DAYS_UNTIL <= ipo["days_until_expiry"] <= self.MAX_DAYS_UNTIL
        ]
        log.info(f"Lockup-window filter ({self.MIN_DAYS_UNTIL}-{self.MAX_DAYS_UNTIL} days): {before} -> {len(all_ipos)}")

        # Step 5: filter by current price (drop penny stocks)
        before = len(all_ipos)
        all_ipos = [
            ipo for ipo in all_ipos
            if ipo.get("current_price") and ipo["current_price"] >= self.MIN_CURRENT_PRICE
        ]
        log.info(f"Min-price filter (>= ${self.MIN_CURRENT_PRICE}): {before} -> {len(all_ipos)}")

        # Step 6: compute return since IPO and filter
        for ipo in all_ipos:
            if ipo.get("ipo_price") and ipo["ipo_price"] > 0 and ipo.get("current_price"):
                ipo["return_since_ipo_pct"] = ((ipo["current_price"] - ipo["ipo_price"]) / ipo["ipo_price"]) * 100
            else:
                ipo["return_since_ipo_pct"] = None

        before = len(all_ipos)
        all_ipos = [
            ipo for ipo in all_ipos
            if ipo.get("return_since_ipo_pct") is not None
            and self.MIN_RETURN_PCT <= ipo["return_since_ipo_pct"] <= self.MAX_RETURN_PCT
        ]
        log.info(f"Return-since-IPO filter ({self.MIN_RETURN_PCT}% to {self.MAX_RETURN_PCT}%): {before} -> {len(all_ipos)}")

        if not all_ipos:
            return empty_result(self.name, run_date)

        # Step 7: build output rows
        rows = []
        for ipo in all_ipos:
            # Score: imminent expiry (sooner = higher), more decline (more bottom potential),
            # current price above $10 (real-cap company)
            urgency_bonus = max(0, 30 - ipo["days_until_expiry"] * 0.5)  # 30 for today, 0 at 60 days
            decline_bonus = abs(min(0, ipo["return_since_ipo_pct"])) * 0.5  # heavily-down = more bottom potential
            cap_bonus = min(20, ipo["current_price"])  # higher price = more meaningful company
            score = urgency_bonus + decline_bonus + cap_bonus

            rows.append({
                "ticker": ipo["symbol"],
                "name": ipo["name"],
                "ipo_date": ipo["ipo_date"],
                "ipo_price": round(float(ipo["ipo_price"]), 2) if ipo.get("ipo_price") else None,
                "current_price": round(float(ipo["current_price"]), 2),
                "return_since_ipo_pct": round(float(ipo["return_since_ipo_pct"]), 1),
                "lockup_expiry_date": ipo["lockup_expiry"].isoformat(),
                "days_until_expiry": ipo["days_until_expiry"],
                "score": round(score, 2),
                "reason": (
                    f"{ipo['name'][:45]}: IPO'd {ipo['ipo_date']} at ${ipo['ipo_price']}, "
                    f"now ${ipo['current_price']:.2f} ({ipo['return_since_ipo_pct']:+.0f}%), "
                    f"180-day lockup expires {ipo['lockup_expiry']} (in {ipo['days_until_expiry']} days)"
                ),
            })

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Lockup convention: {self.LOCKUP_DAYS} days post-IPO",
                f"Window: {self.MIN_DAYS_UNTIL}-{self.MAX_DAYS_UNTIL} days until expiry",
                f"Universe: stockanalysis.com IPO calendars {run_date.year - 1} + {run_date.year}",
                f"Final candidates: {len(rows)}",
                "ACORNS SLEEVE ONLY — lockup bottom-fishing has high failure rate.",
                "Lockup terms vary (90/180/365); we assume 180-day convention.",
            ],
        )