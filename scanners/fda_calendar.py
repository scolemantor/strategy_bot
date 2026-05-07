"""FDA PDUFA calendar scanner.

Surfaces small/mid-cap biotech companies with PDUFA decisions in the next
30-90 days. PDUFA = Prescription Drug User Fee Act = the legally-mandated
deadline by which the FDA must respond to a New Drug Application or Biologics
License Application. These are binary catalysts: approval often = stock 50-200%
gain; rejection (Complete Response Letter) often = 30-70% loss overnight.

The asymmetry makes small-cap biotech one of the few areas where individual
investors can outperform via concentrated catalyst trades — IF they have edge
in reading the underlying science (clinical trial data, FDA briefing docs,
advisory committee history).

This scanner only SURFACES candidates. It does not predict outcomes. Use this
to populate your research queue, not to trade blind.

Source: RTTNews FDA Calendar (rttnews.com/corpinfo/fdacalendar.aspx).
HTML scraped + parsed with BeautifulSoup. Multi-page walk.

Filters:
  1. Status = Pending (drop already-decided events)
  2. PDUFA date is 30-90 days out (configurable)
  3. Has US-listed ticker (drop foreign-only listings)
  4. Market cap < $5B (the asymmetry shrinks at large-cap)

Honest limits:
  - RTTNews calendar is not exhaustive; smaller biotechs may be missing
  - Free version is paginated; we walk all visible pages
  - Market cap from yfinance is occasionally stale by a day
  - We don't predict outcomes — the scanner is a research-queue populator
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.http_utils import with_deadline, yfinance_session

from .base import Scanner, ScanResult, empty_result

log = logging.getLogger(__name__)

try:
    _YF_SESSION = yfinance_session(30)
except Exception:
    _YF_SESSION = None

CACHE_DIR = Path("data_cache")
RTT_CACHE = CACHE_DIR / "rttnews_fda_calendar"
MARKET_CAP_CACHE = CACHE_DIR / "yfinance_market_cap"

PAGE_CACHE_TTL_HOURS = 6
MARKET_CAP_CACHE_TTL_HOURS = 24

RTT_BASE_URL = "https://www.rttnews.com/corpinfo/fdacalendar.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; strategy_bot/1.0; +https://github.com/scolemantor/strategy_bot)",
    "Accept": "text/html,application/xhtml+xml",
}

DATE_RE = re.compile(r"(\d{2})/(\d{2})/(\d{4})")


def _page_cache_path(page_num: int) -> Path:
    return RTT_CACHE / f"page_{page_num}.html"


def _is_page_cache_fresh(page_num: int) -> bool:
    p = _page_cache_path(page_num)
    if not p.exists():
        return False
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    return age_hours < PAGE_CACHE_TTL_HOURS


def _market_cap_cache_path(symbol: str) -> Path:
    return MARKET_CAP_CACHE / f"{symbol}.json"


def _load_cached_market_cap(symbol: str) -> Optional[float]:
    p = _market_cap_cache_path(symbol)
    if not p.exists():
        return None
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    if age_hours > MARKET_CAP_CACHE_TTL_HOURS:
        return None
    try:
        data = json.loads(p.read_text())
        return data.get("market_cap")
    except Exception as e:
        log.debug(f"Failed to load market cap cache for {symbol}: {e}")
        return None


def _save_cached_market_cap(symbol: str, market_cap: Optional[float]) -> None:
    MARKET_CAP_CACHE.mkdir(parents=True, exist_ok=True)
    try:
        _market_cap_cache_path(symbol).write_text(json.dumps({
            "symbol": symbol,
            "market_cap": market_cap,
            "fetched_at": datetime.now().isoformat(),
        }))
    except Exception as e:
        log.debug(f"Failed to save market cap cache for {symbol}: {e}")


class FdaCalendarScanner(Scanner):
    name = "fda_calendar"
    description = "Small/mid-cap biotech with PDUFA decisions in 30-90 day window"
    cadence = "daily"

    DEFAULT_DAYS_MIN = 30
    DEFAULT_DAYS_MAX = 90
    MAX_MARKET_CAP = 5_000_000_000  # $5B
    MAX_PAGES = 10  # safety cap on pagination walk
    REQUEST_DELAY_SEC = 1.0  # courtesy delay between page fetches

    def __init__(self,
                 days_min: int = None,
                 days_max: int = None,
                 max_market_cap: float = None):
        super().__init__()
        self.days_min = days_min if days_min is not None else self.DEFAULT_DAYS_MIN
        self.days_max = days_max if days_max is not None else self.DEFAULT_DAYS_MAX
        self.max_market_cap = max_market_cap if max_market_cap is not None else self.MAX_MARKET_CAP

    def run(self, run_date: date) -> ScanResult:
        log.info(f"PDUFA window: {self.days_min}-{self.days_max} days from {run_date}")
        log.info(f"Max market cap: ${self.max_market_cap:,.0f}")

        # Step 1: scrape all pages
        try:
            events = self._scrape_all_pages()
        except Exception as e:
            log.exception("Failed to scrape RTTNews FDA calendar")
            return empty_result(self.name, run_date, error=f"scrape: {e}")

        log.info(f"Scraped {len(events)} raw PDUFA events from RTTNews")

        if not events:
            return empty_result(self.name, run_date)

        # Step 2: filter to Pending only
        before = len(events)
        events = [e for e in events if e["is_pending"]]
        log.info(f"Pending filter: {before} -> {len(events)} (removed already-decided events)")

        # Step 3: filter to date window
        before = len(events)
        date_min = run_date + timedelta(days=self.days_min)
        date_max = run_date + timedelta(days=self.days_max)
        events = [e for e in events if e["pdufa_date"] is not None
                  and date_min <= e["pdufa_date"] <= date_max]
        log.info(
            f"Date window filter ({self.days_min}-{self.days_max}d): {before} -> {len(events)} "
            f"({date_min} to {date_max})"
        )

        # Step 4: filter to US-listed ticker present
        before = len(events)
        events = [e for e in events if e["ticker"]]
        log.info(f"US-ticker filter: {before} -> {len(events)} (removed foreign-only listings)")

        if not events:
            log.info("No events survived filters")
            return empty_result(self.name, run_date)

        # Step 5: enrich with market cap, then filter to small/mid cap
        log.info(f"Fetching market caps for {len(events)} candidates...")
        try:
            import yfinance as yf
        except ImportError:
            return empty_result(self.name, run_date,
                error="yfinance not installed - run: python -m pip install yfinance")

        for e in events:
            e["market_cap"] = self._get_market_cap(yf, e["ticker"])

        before = len(events)
        events = [e for e in events
                  if e["market_cap"] is not None and e["market_cap"] <= self.max_market_cap]
        log.info(
            f"Market cap filter (<= ${self.max_market_cap:,.0f}): {before} -> {len(events)} "
            f"(dropped large-caps and tickers without cap data)"
        )

        if not events:
            return empty_result(self.name, run_date)

        # Step 6: build output rows
        rows = []
        for e in events:
            days_until = (e["pdufa_date"] - run_date).days
            # Score: closer events score higher (more imminent catalyst).
            # Boost smaller caps slightly (more asymmetry per dollar invested).
            recency_score = max(0, 100 - days_until)
            cap_bonus = max(0, 30 - (e["market_cap"] / 1e9) * 6)  # full 30 at $0, 0 at $5B
            score = recency_score + cap_bonus

            rows.append({
                "ticker": e["ticker"],
                "company": e["company"],
                "drug_name": e["drug_name"],
                "indication": e["indication"],
                "pdufa_date": e["pdufa_date"].isoformat(),
                "days_until": days_until,
                "market_cap": int(e["market_cap"]),
                "score": round(score, 2),
                "reason": (
                    f"PDUFA {e['pdufa_date'].isoformat()} ({days_until}d), "
                    f"mcap ${e['market_cap']/1e9:.2f}B, "
                    f"{e['drug_name']}: {e['indication'][:80]}"
                ),
            })

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Window: {self.days_min}-{self.days_max} days",
                f"Max market cap: ${self.max_market_cap/1e9:.1f}B",
                f"Source: RTTNews FDA Calendar",
                f"Final candidates: {len(rows)}",
            ],
        )

    # ---------- scraping ----------

    def _scrape_all_pages(self) -> List[Dict]:
        all_events: List[Dict] = []
        for page_num in range(1, self.MAX_PAGES + 1):
            html = self._fetch_page(page_num)
            if html is None:
                log.info(f"Page {page_num}: not available, stopping pagination")
                break
            page_events = self._parse_page(html)
            log.info(f"Page {page_num}: extracted {len(page_events)} events")
            if not page_events:
                # If we get an empty page, we've walked past the end
                log.info(f"Page {page_num} returned 0 events, stopping pagination")
                break
            all_events.extend(page_events)
        return all_events

    def _fetch_page(self, page_num: int) -> Optional[str]:
        if _is_page_cache_fresh(page_num):
            log.debug(f"Page {page_num}: using cached HTML")
            return _page_cache_path(page_num).read_text(encoding="utf-8")

        url = RTT_BASE_URL if page_num == 1 else f"{RTT_BASE_URL}?PageNum={page_num}"
        try:
            time.sleep(self.REQUEST_DELAY_SEC)
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                log.warning(f"Page {page_num}: HTTP {r.status_code}")
                return None
            text = r.text
            RTT_CACHE.mkdir(parents=True, exist_ok=True)
            _page_cache_path(page_num).write_text(text, encoding="utf-8")
            return text
        except Exception as e:
            log.warning(f"Page {page_num}: fetch failed: {e}")
            return None

    def _parse_page(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, "html.parser")

        # Each event row contains 4 sibling divs with classes tblcontent1/2/3/4.
        # Find every tblcontent1 div, then walk forward to grab its companions.
        events: List[Dict] = []
        company_divs = soup.find_all("div", class_="tblcontent1")

        for company_div in company_divs:
            # Walk forward to find the next tblcontent2 / 3 / 4 in the same row group.
            drug_div = company_div.find_next("div", class_="tblcontent2")
            event_div = company_div.find_next("div", class_="tblcontent3")
            outcome_div = company_div.find_next("div", class_="tblcontent4")

            if not (drug_div and event_div):
                continue

            company_name, ticker = self._extract_company_and_ticker(company_div)
            drug_name = drug_div.get_text(strip=True)
            pdufa_date, indication = self._extract_event(event_div)
            is_pending = self._extract_pending(outcome_div) if outcome_div else True

            events.append({
                "company": company_name,
                "ticker": ticker,
                "drug_name": drug_name,
                "pdufa_date": pdufa_date,
                "indication": indication,
                "is_pending": is_pending,
            })

        return events

    def _extract_company_and_ticker(self, div) -> tuple:
        """Extract company name and US-listed ticker from a tblcontent1 div.

        The HTML looks like:
          Axsome Therapeutics, Inc. <br/> (<a href=".../symbolsearch.aspx?symbol=AXSM">AXSM</a>)
        Or for foreign-listed:
          AstraZeneca PLC <br/> (<a href=".../symbolsearch.aspx?symbol=AZN">AZN</a>, AZN.L, ZEG.DE)

        We want the symbolsearch.aspx ticker (always US-listed) and the company name.
        Returns (company_name, ticker) where ticker is None if no US listing found.
        """
        # Get the company name as the text before the parens
        text = div.get_text(separator=" ", strip=True)
        # Remove ticker section (everything from first "(" onward)
        company = re.sub(r"\s*\(.*$", "", text).strip()

        # Find the first symbolsearch.aspx link — that's our US ticker
        link = div.find("a", href=re.compile(r"symbolsearch\.aspx\?symbol="))
        ticker = link.get_text(strip=True) if link else None

        return company, ticker

    def _extract_event(self, div) -> tuple:
        """Extract PDUFA date and indication text from a tblcontent3 div.

        Format:
          <span class="bg-purple">04/30/2026</span> <br/> FDA decision on AXS-05 for the treatment of...
        """
        date_span = div.find("span", class_="bg-purple")
        pdufa_date = None
        if date_span:
            m = DATE_RE.search(date_span.get_text(strip=True))
            if m:
                month, day, year = m.groups()
                try:
                    pdufa_date = date(int(year), int(month), int(day))
                except ValueError:
                    pdufa_date = None

        # The indication is everything else in the div, with the date stripped
        full_text = div.get_text(separator=" ", strip=True)
        if date_span:
            full_text = full_text.replace(date_span.get_text(strip=True), "", 1).strip()
        # Clean up leading "FDA decision on" / "FDA panel to review" prefixes for brevity
        indication = re.sub(r"^(FDA decision on|FDA panel to review)\s+", "", full_text)

        return pdufa_date, indication

    def _extract_pending(self, div) -> bool:
        """Outcome div contains either 'Pending' or an actual outcome text."""
        text = div.get_text(strip=True).lower()
        return "pending" in text and "fda approved" not in text and "fda panel" not in text

    # ---------- market cap ----------

    def _get_market_cap(self, yf, symbol: str) -> Optional[float]:
        cached = _load_cached_market_cap(symbol)
        if cached is not None:
            return cached

        try:
            ticker = yf.Ticker(symbol, session=_YF_SESSION)
            # Try fast_info first (faster), fall back to info
            try:
                fi = with_deadline(lambda: ticker.fast_info, timeout=15, default=None)
                cap = fi.get("market_cap") if fi is not None else None
            except Exception:
                cap = None
            if cap is None or cap == 0:
                try:
                    info = with_deadline(lambda: ticker.info, timeout=30, default=None)
                    cap = info.get("marketCap") if info is not None else None
                except Exception:
                    cap = None
            cap = float(cap) if cap else None
        except Exception as e:
            log.debug(f"Market cap fetch failed for {symbol}: {e}")
            cap = None

        _save_cached_market_cap(symbol, cap)
        return cap