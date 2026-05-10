"""Congressional trades scanner via Financial Modeling Prep (FMP) API.

Surfaces tickers being bought by Members of Congress under STOCK Act
disclosures. The signal is two-pronged:

  1. Cluster buys: the same ticker bought by 2+ distinct members within the
     30-day lookback window.
  2. High-signal solo buys: any ticker bought by a member on the configurable
     HIGH_SIGNAL_MEMBERS list with a disclosed midpoint amount >= $50k.

Academic basis: Ziobrowski et al. (2004, 2011) documented Senate and House
trading edge over the market.

DATA SOURCE (Phase 7 hotfix, 2026-05-09): the original community S3 feeds
(house-stock-watcher-data / senate-stock-watcher-data) both went 403
Forbidden in early 2026 and the GitHub mirror is stale (last commit 2021).
We migrated to Financial Modeling Prep's congressional trading endpoints:

  - House:  /api/v4/senate-disclosure-rss-feed (FMP confusingly names the
            House firehose 'senate-disclosure'; this is intentional, not a
            typo — verified against FMP's docs, May 2026)
  - Senate: /api/v4/senate-trading-rss-feed

Requires an FMP_API_KEY env var. Free tier is 250 calls/day + 500 MB/30-day
bandwidth, plenty for one daily scan that fetches ~5-10 pages per chamber.
Sign up at https://site.financialmodelingprep.com/developer/docs/pricing
and add `FMP_API_KEY=...` to .env. Without the env var set, the scanner
returns an empty ScanResult with a clear "API key not configured" note —
it does NOT raise, so scan.py all keeps working.

Look-ahead protection (backtest mode): we filter by `disclosure_date <=
as_of_date`, NEVER `transaction_date`. The 45-day legal disclosure window
means a transaction_date filter would surface trades that weren't yet public
on the historical date being replayed.

Honest limits:
  - 45-day disclosure lag means signals are stale by definition.
  - Members file late or amend after deadlines.
  - Spouse/family member trades (`owner` field = Spouse/Joint) are disclosed
    but harder to attribute; we count them under the member's name.
  - FMP tickers come straight from the filing — sometimes blank, sometimes
    `"--"`, sometimes class-B variants. We normalize as best we can.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from .base import Scanner, ScanResult, empty_result

log = logging.getLogger(__name__)

# FMP (Financial Modeling Prep) congressional-trading firehose endpoints.
# Both return JSON arrays of trade records, newest disclosure first,
# paginated via ?page=N (0-indexed) and optional ?limit=N (max 1000,
# default ~100). FMP confusingly names the House firehose
# 'senate-disclosure-rss-feed' — verified, not a typo.
FMP_BASE = "https://financialmodelingprep.com/api/v4"
FMP_HOUSE_FEED = f"{FMP_BASE}/senate-disclosure-rss-feed"
FMP_SENATE_FEED = f"{FMP_BASE}/senate-trading-rss-feed"
FMP_API_KEY_ENV = "FMP_API_KEY"
FMP_PAGE_LIMIT = 100  # default page size; FMP allows up to 1000
FMP_MAX_PAGES = 30    # safety cap; with 30-day lookback we typically need 5-10

USER_AGENT = "OakStrategyBot research"
REQUEST_TIMEOUT = 60
CACHE_DIR = Path("data_cache/congressional_trades")
CACHE_TTL_HOURS = 6

HIGH_SIGNAL_MEMBERS: List[str] = []

AMOUNT_BRACKETS: Dict[str, Tuple[float, float]] = {
    "$1,001 - $15,000":           (1_001,        15_000),
    "$15,001 - $50,000":          (15_001,       50_000),
    "$50,001 - $100,000":         (50_001,      100_000),
    "$100,001 - $250,000":        (100_001,     250_000),
    "$250,001 - $500,000":        (250_001,     500_000),
    "$500,001 - $1,000,000":      (500_001,    1_000_000),
    "$1,000,001 - $5,000,000":    (1_000_001,  5_000_000),
    "$5,000,001 - $25,000,000":   (5_000_001, 25_000_000),
    "$25,000,001 - $50,000,000":  (25_000_001, 50_000_000),
    "$50,000,001 +":              (50_000_001, 100_000_000),
}

# Substrings that, when present in asset_description, mark the asset as
# something other than common stock and disqualify the trade.
NON_COMMON_STOCK_HINTS = (
    "option", "call ", "put ",
    "bond", "treasury", "muni", "municipal",
    "etf", "mutual fund", "fund -", "fund (",
    "warrant", "preferred", "convertible note",
    "cd ", "certificate of deposit",
    "cryptocurrency", "crypto",
)


@dataclass
class CongressionalTrade:
    ticker: str
    member_name: str
    chamber: str
    transaction_date: Optional[date]
    disclosure_date: Optional[date]
    transaction_type: str
    amount_min: float
    amount_max: float
    asset_description: str
    is_purchase: bool
    is_high_signal_member: bool
    raw_amount: str

    @property
    def amount_midpoint(self) -> float:
        return (self.amount_min + self.amount_max) / 2.0

    def to_dict(self) -> dict:
        d = asdict(self)
        for k in ("transaction_date", "disclosure_date"):
            if isinstance(d[k], date):
                d[k] = d[k].isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "CongressionalTrade":
        d2 = dict(d)
        for k in ("transaction_date", "disclosure_date"):
            v = d2.get(k)
            if isinstance(v, str):
                try:
                    d2[k] = date.fromisoformat(v)
                except ValueError:
                    d2[k] = None
        return cls(**d2)


class CongressionalTradesScanner(Scanner):
    name = "congressional_trades"
    description = "STOCK Act disclosures: cluster buys (2+ members) or high-signal individual buys"
    cadence = "daily"

    DEFAULT_LOOKBACK_DAYS = 30
    MIN_CLUSTER_MEMBERS = 2
    MIN_HIGH_SIGNAL_AMOUNT = 50_000
    HIGH_SIGNAL_BONUS = 50

    def __init__(self, lookback_days: Optional[int] = None):
        super().__init__()
        self.lookback_days = lookback_days if lookback_days is not None else self.DEFAULT_LOOKBACK_DAYS

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Lookback window: {self.lookback_days} days (by disclosure_date)")
        log.info(f"High-signal members configured: {len(HIGH_SIGNAL_MEMBERS)}")

        api_key = os.environ.get(FMP_API_KEY_ENV)
        if not api_key:
            log.warning(
                f"{FMP_API_KEY_ENV} env var not set; congressional_trades scanner is "
                "disabled. Sign up at https://site.financialmodelingprep.com/developer/docs/pricing "
                "(free tier: 250 calls/day) and add the key to .env."
            )
            return ScanResult(
                scanner_name=self.name,
                run_date=run_date,
                candidates=pd.DataFrame(columns=["ticker", "score", "reason"]),
                notes=[f"{FMP_API_KEY_ENV} not configured; scanner disabled (no error)."],
            )

        cutoff = run_date - timedelta(days=self.lookback_days)

        try:
            house_raw = self._fetch_fmp_feed(
                FMP_HOUSE_FEED, "fmp_house.json", api_key, cutoff,
            )
        except Exception as e:
            log.exception("Failed to fetch FMP House feed")
            house_raw = []
            log.warning("Continuing with Senate-only data (House fetch failed)")

        try:
            senate_raw = self._fetch_fmp_feed(
                FMP_SENATE_FEED, "fmp_senate.json", api_key, cutoff,
            )
        except Exception as e:
            log.exception("Failed to fetch FMP Senate feed")
            return empty_result(self.name, run_date, error=f"senate feed: {e}")

        log.info(f"Loaded {len(house_raw)} house records, {len(senate_raw)} senate records")

        trades: List[CongressionalTrade] = []
        for r in house_raw:
            t = self._parse_fmp_record(r, "house")
            if t is not None:
                trades.append(t)
        for r in senate_raw:
            t = self._parse_fmp_record(r, "senate")
            if t is not None:
                trades.append(t)
        log.info(f"Parsed {len(trades)} trades total")

        in_window = [
            t for t in trades
            if t.disclosure_date is not None
            and cutoff <= t.disclosure_date <= run_date
        ]
        log.info(f"Disclosure-date filter ({cutoff} to {run_date}): {len(trades)} -> {len(in_window)}")

        purchases = [
            t for t in in_window
            if t.is_purchase
            and t.ticker
            and self._is_common_stock(t.asset_description)
        ]
        log.info(f"Purchase + common-stock filter: {len(in_window)} -> {len(purchases)}")

        if not purchases:
            return empty_result(self.name, run_date)

        by_ticker: Dict[str, List[CongressionalTrade]] = defaultdict(list)
        for t in purchases:
            by_ticker[t.ticker.upper()].append(t)

        rows = []
        for ticker, group in by_ticker.items():
            distinct_members = {t.member_name.strip().lower() for t in group if t.member_name}
            n_members = len(distinct_members)
            has_high_signal = any(
                t.is_high_signal_member and t.amount_midpoint >= self.MIN_HIGH_SIGNAL_AMOUNT
                for t in group
            )

            if n_members < self.MIN_CLUSTER_MEMBERS and not has_high_signal:
                continue

            total_min = sum(t.amount_min for t in group)
            total_max = sum(t.amount_max for t in group)
            total_mid = sum(t.amount_midpoint for t in group)
            disclosure_dates = [t.disclosure_date for t in group if t.disclosure_date]
            earliest = min(disclosure_dates) if disclosure_dates else None
            latest = max(disclosure_dates) if disclosure_dates else None

            chambers_set = {t.chamber for t in group}
            if chambers_set == {"house"}:
                chambers = "h"
            elif chambers_set == {"senate"}:
                chambers = "s"
            else:
                chambers = "both"

            value_bonus = min(total_mid / 10_000, 50)
            high_signal_bonus = self.HIGH_SIGNAL_BONUS if has_high_signal else 0
            score = n_members * 100 + value_bonus + high_signal_bonus

            members_list = "; ".join(sorted({t.member_name for t in group if t.member_name}))

            if has_high_signal and n_members >= self.MIN_CLUSTER_MEMBERS:
                reason = (
                    f"{n_members} members + high-signal buyer, "
                    f"~${total_mid:,.0f} midpoint disclosed"
                )
            elif has_high_signal:
                reason = f"high-signal solo buy, ~${total_mid:,.0f} midpoint disclosed"
            else:
                reason = f"{n_members} members, ~${total_mid:,.0f} midpoint disclosed"

            rows.append({
                "ticker": ticker,
                "member_count": len(group),
                "distinct_members": n_members,
                "total_value_min": int(total_min),
                "total_value_max": int(total_max),
                "total_value_midpoint": int(total_mid),
                "earliest_disclosure": earliest.isoformat() if earliest else "",
                "latest_disclosure": latest.isoformat() if latest else "",
                "has_high_signal_member": has_high_signal,
                "members_list": members_list,
                "chambers": chambers,
                "score": round(score, 2),
                "reason": reason,
            })

        if not rows:
            return empty_result(self.name, run_date)

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Lookback (disclosure_date): {self.lookback_days} days",
                f"House records: {len(house_raw)}, Senate: {len(senate_raw)}",
                f"Trades parsed: {len(trades)}, in-window purchases: {len(purchases)}",
                f"Distinct tickers w/ purchases: {len(by_ticker)}",
                f"Flagged (cluster or high-signal): {len(rows)}",
                "Amounts are STOCK Act bracket midpoints — approximate by design.",
            ],
        )

    def _fetch_fmp_feed(
        self, url: str, cache_name: str, api_key: str, cutoff: date,
    ) -> List[Dict]:
        """Fetch FMP firehose endpoint, paginated newest-first. Stops as
        soon as the oldest disclosure_date in a page falls before `cutoff`,
        so we don't waste API budget pulling years of history. Cached for
        CACHE_TTL_HOURS as the merged-pages JSON.

        Returns the unparsed list of raw FMP records covering at least the
        disclosure window [cutoff, today]."""
        cache_path = CACHE_DIR / cache_name
        if cache_path.exists():
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if age_hours < CACHE_TTL_HOURS:
                log.debug(f"Using cached {cache_name} ({age_hours:.1f}h old)")
                return json.loads(cache_path.read_text())

        all_records: List[Dict] = []
        for page in range(FMP_MAX_PAGES):
            params = {"page": page, "limit": FMP_PAGE_LIMIT, "apikey": api_key}
            log.info(f"Fetching {cache_name} page {page} from {url}")
            resp = requests.get(
                url,
                params=params,
                headers={"User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            page_data = resp.json()
            if not isinstance(page_data, list):
                log.warning(
                    f"  Unexpected response shape from {url}: "
                    f"{type(page_data).__name__}; stopping pagination"
                )
                break
            if not page_data:
                log.debug(f"  Page {page} empty; end of feed")
                break
            all_records.extend(page_data)

            # Newest-first: check if oldest in this page is already past cutoff
            page_dates = [
                self._parse_date(r.get("disclosureDate")) for r in page_data
            ]
            page_dates = [d for d in page_dates if d is not None]
            if page_dates and min(page_dates) < cutoff:
                log.debug(
                    f"  Page {page} oldest disclosure {min(page_dates)} < "
                    f"cutoff {cutoff}; stopping pagination "
                    f"(have {len(all_records)} records)"
                )
                break

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(all_records))
        return all_records

    def _parse_fmp_record(self, raw: Dict, chamber: str) -> Optional[CongressionalTrade]:
        """Parse one FMP congressional trade record. Schema is identical
        across chambers (House and Senate firehoses return the same fields).

        Field reference (verified 2026-05-09):
          symbol, disclosureDate, transactionDate, firstName, lastName,
          office, district, owner, assetDescription, assetType, type,
          amount, comment, link
        """
        try:
            ticker = (raw.get("symbol") or "").strip().upper()
            first = (raw.get("firstName") or "").strip()
            last = (raw.get("lastName") or "").strip()
            member = f"{first} {last}".strip()
            txn_type_raw = (raw.get("type") or "").strip().lower()
            asset_desc = (raw.get("assetDescription") or "").strip()
            amount_raw = (raw.get("amount") or "").strip()
            txn_date = self._parse_date(raw.get("transactionDate"))
            disc_date = self._parse_date(raw.get("disclosureDate"))
        except (AttributeError, TypeError):
            return None

        if not member:
            return None
        if ticker in ("--", "N/A", ""):
            ticker = ""

        amount_min, amount_max = self._parse_amount(amount_raw)
        return CongressionalTrade(
            ticker=ticker,
            member_name=member,
            chamber=chamber,
            transaction_date=txn_date,
            disclosure_date=disc_date,
            transaction_type=txn_type_raw,
            amount_min=amount_min,
            amount_max=amount_max,
            asset_description=asset_desc,
            # FMP `type` values: "Purchase", "Sale (Full)", "Sale (Partial)",
            # "Exchange". We treat anything containing "purchase" as a buy.
            is_purchase=("purchase" in txn_type_raw),
            is_high_signal_member=self._is_high_signal(member),
            raw_amount=amount_raw,
        )

    @staticmethod
    def _parse_date(s) -> Optional[date]:
        if not s or not isinstance(s, str):
            return None
        s = s.strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%m-%d-%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_amount(amount_str: str) -> Tuple[float, float]:
        if not amount_str:
            return (0.0, 0.0)
        normalized = re.sub(r"\s+", " ", amount_str).strip()
        normalized = re.sub(r"[–—−]", "-", normalized)
        if normalized in AMOUNT_BRACKETS:
            return AMOUNT_BRACKETS[normalized]
        log.debug(
            f"  _parse_amount: unparseable bracket — raw={amount_str!r} "
            f"normalized={normalized!r}"
        )
        return (0.0, 0.0)

    @staticmethod
    def _is_common_stock(asset_description: str) -> bool:
        if not asset_description:
            return False
        low = asset_description.lower()
        return not any(hint in low for hint in NON_COMMON_STOCK_HINTS)

    @staticmethod
    def _is_high_signal(member_name: str) -> bool:
        if not HIGH_SIGNAL_MEMBERS or not member_name:
            return False
        low = member_name.lower()
        return any(m.lower() in low for m in HIGH_SIGNAL_MEMBERS)


def backtest_mode(as_of_date: date, output_dir=None) -> int:
    """Run congressional_trades scanner as-of a historical date.

    Look-ahead protection: trades are filtered by disclosure_date <= as_of_date
    (inside CongressionalTradesScanner.run via the lookback window), NOT by
    transaction_date. STOCK Act allows up to 45 days between transaction and
    disclosure, so a transaction-date filter would surface trades that were
    not yet public on as_of_date.

    The community API returns ALL historical disclosures (immutable once
    published, modulo amendments). So this is the live scanner pointed at a
    historical date with the disclosure-date filter applied.

    Output goes to <output_dir>/<as_of_date>/congressional_trades.csv where
    output_dir defaults to backtest_output/.
    """
    output_dir = Path(output_dir) if output_dir else Path("backtest_output")
    scanner = CongressionalTradesScanner()

    try:
        result = scanner.run(as_of_date)
    except Exception as e:
        log.warning(f"congressional_trades backtest_mode failed for {as_of_date}: {e}")
        return 0

    if result.error or result.candidates.empty:
        return 0

    date_dir = output_dir / as_of_date.isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    out_path = date_dir / "congressional_trades.csv"
    result.candidates.to_csv(out_path, index=False)
    log.debug(
        f"  congressional_trades {as_of_date}: wrote {len(result.candidates)} candidates to {out_path}"
    )

    return len(result.candidates)
