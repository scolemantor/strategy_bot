"""Congressional trades scanner via Quiver Quantitative API.

Surfaces tickers being bought by Members of Congress under STOCK Act
disclosures. The signal is two-pronged:

  1. Cluster buys: the same ticker bought by 2+ distinct members within the
     30-day lookback window.
  2. High-signal solo buys: any ticker bought by a member on the configurable
     HIGH_SIGNAL_MEMBERS list with a disclosed midpoint amount >= $50k.

Academic basis: Ziobrowski et al. (2004, 2011) documented Senate and House
trading edge over the market.

DATA SOURCE (Phase 7 hotfix history):
  - Original: housestockwatcher.com / senatestockwatcher.com S3 feeds.
    Both went 403 Forbidden in early 2026; GitHub mirror stale since 2021.
  - First replacement attempt: FMP `/api/v4/senate-disclosure-rss-feed` —
    turned out to be paid-tier-locked despite docs implying free tier.
  - Second replacement attempt: Finnhub `/stock/congressional-trading` —
    also premium-only.
  - Current: Quiver Quantitative — confirmed-paid but transparent
    ($30/mo Hobbyist tier covers Congress Trading dataset). One unified
    endpoint covers both chambers.

Endpoint: `https://api.quiverquant.com/beta/bulk/congresstrading`
Auth:     `Authorization: Bearer <QUIVER_API_KEY>` header (per Quiver's
          OpenAPI spec; their official Python wrapper uses the legacy
          `Token` form which is also accepted)
Schema:   one JSON array. Each record has `Representative`, `BioGuideID`,
          `Ticker`, `Transaction` (Purchase/Sale/...), `Range` (STOCK Act
          bracket string), `Amount`, `District`, `House` (chamber
          discriminator: "Representatives" or "Senate"), `Party`,
          `TickerType` ("CS" or "ST" for common stock; other values for
          non-equity), `ReportDate`, `TransactionDate`, `last_modified`,
          plus performance fields `ExcessReturn`, `PriceChange`,
          `SPYChange` (Quiver's per-trade alpha-vs-SPY tracking).
Filters:  defensive `date_from` + `date_to` + `page=1` + `page_size=10000`
          query params — if Quiver honors them we save bandwidth, if not
          we still get the full array and client-side cutoff filter applies.
TickerType: hard filter to CS/ST common stock only. Empty/missing
          TickerType drops the record (V2 schema is well-populated; a
          blank field signals non-equity).

Sign up at https://api.quiverquant.com/pricing and add
`QUIVER_API_KEY=...` to .env. Without the env var set the scanner returns
an empty ScanResult with a clear "API key not configured" note — does NOT
raise, so scan.py all keeps working.

Look-ahead protection (backtest mode): we filter by `disclosure_date <=
as_of_date` (Quiver's `ReportDate`), NEVER `transaction_date`. The 45-day
legal disclosure window means a transaction_date filter would surface
trades that weren't yet public on the historical date being replayed.

Honest limits:
  - 45-day disclosure lag means signals are stale by definition.
  - Spouse/family member trades are disclosed but harder to attribute;
    we count them under the member's name as listed.
  - Quiver's `Ticker` field is normalized but occasionally blank for
    non-equity assets — those rows get dropped at the common-stock filter.
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

# Quiver Quantitative bulk congressional-trading endpoint. Date-filterable
# and paginated per Quiver's OpenAPI spec. Returns both chambers in one
# response; the `House` field discriminates ("Senate" or "Representatives").
# Auth via `Authorization: Bearer <KEY>` header.
QUIVER_FEED_URL = "https://api.quiverquant.com/beta/bulk/congresstrading"
QUIVER_API_KEY_ENV = "QUIVER_API_KEY"
# Defensive per-page request size. If the server honors pagination, we get
# pages of this size; if it ignores `page_size`, we get the full array. A
# response of EXACTLY this length triggers the truncation warning.
QUIVER_PAGE_SIZE = 10000
# TickerType values that the scanner accepts as common stock. Anything
# else (options, ETFs, bonds, crypto, blank) is dropped before clustering.
COMMON_STOCK_TICKER_TYPES = frozenset({"CS", "ST"})

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


def _coerce_float(v) -> Optional[float]:
    """Defensive float-coerce for Quiver's optional numeric fields.
    Returns None for None / "" / non-numeric; preserves explicit 0.0."""
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


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
    # Phase 7 hotfix (Quiver migration): new fields with defaults so any
    # cached pre-migration JSON deserializes cleanly via from_dict.
    ticker_type: str = ""
    excess_return: Optional[float] = None
    price_change: Optional[float] = None

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

        api_key = os.environ.get(QUIVER_API_KEY_ENV)
        if not api_key:
            log.warning(
                f"{QUIVER_API_KEY_ENV} env var not set; congressional_trades scanner "
                "is disabled. Sign up at https://api.quiverquant.com/pricing "
                "(Hobbyist tier $30/mo includes Congress Trading) and add the key to .env."
            )
            return ScanResult(
                scanner_name=self.name,
                run_date=run_date,
                candidates=pd.DataFrame(columns=["ticker", "score", "reason"]),
                notes=[f"{QUIVER_API_KEY_ENV} not configured; scanner disabled (no error)."],
            )

        cutoff = run_date - timedelta(days=self.lookback_days)

        try:
            raw_records = self._fetch_quiver_feed(api_key, cutoff, run_date)
        except Exception as e:
            log.exception("Failed to fetch Quiver congressional trading feed")
            return empty_result(self.name, run_date, error=f"quiver feed: {e}")

        # Quiver returns one combined array; split on the `House` field.
        # V2 schema values are "Representatives" / "Senate" (V1 was "House" /
        # "Senate"); _parse_quiver_record normalizes both to "house"/"senate".
        house_count = sum(
            1 for r in raw_records
            if str(r.get("House", "")).lower() in ("house", "representatives")
        )
        senate_count = sum(1 for r in raw_records if str(r.get("House", "")).lower() == "senate")
        log.info(
            f"Loaded {len(raw_records)} total records "
            f"(house={house_count}, senate={senate_count})"
        )

        trades: List[CongressionalTrade] = []
        ticker_type_dropped = 0
        for r in raw_records:
            t = self._parse_quiver_record(r)
            if t is None:
                continue
            # Hard TickerType filter: V2 schema populates this consistently;
            # missing/non-equity values mean the trade is not in our universe.
            if t.ticker_type not in COMMON_STOCK_TICKER_TYPES:
                ticker_type_dropped += 1
                continue
            trades.append(t)
        log.info(
            f"Parsed {len(trades)} common-stock trades "
            f"(dropped {ticker_type_dropped} non-CS/ST records)"
        )

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

            # Phase 7 hotfix: Quiver returns per-trade alpha vs SPY
            # (ExcessReturn) and absolute price change since the trade
            # (PriceChange). Aggregate across the cluster as a confidence
            # signal — DATA only for now, not folded into the score
            # formula until a separate meta_ranker commit weights it.
            er_values = [t.excess_return for t in group if t.excess_return is not None]
            pc_values = [t.price_change for t in group if t.price_change is not None]
            avg_excess_return = (
                round(sum(er_values) / len(er_values), 4) if er_values else None
            )
            avg_price_change = (
                round(sum(pc_values) / len(pc_values), 4) if pc_values else None
            )

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
                "avg_excess_return": avg_excess_return,
                "avg_price_change": avg_price_change,
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

    def _fetch_quiver_feed(
        self, api_key: str, cutoff: date, run_date: date,
    ) -> List[Dict]:
        """Fetch Quiver's bulk congresstrading endpoint. Sends defensive
        date_from/date_to + page/page_size params per the OpenAPI spec —
        if the server honors them, we save bandwidth; if not, we still
        get the full array and the caller's client-side cutoff filter
        applies. Cached for CACHE_TTL_HOURS.

        Pagination: not looped in this version. The official Python
        wrapper hits this endpoint with no params and parses a flat array,
        suggesting one-shot returns the full set. If the truncation
        diagnostic (response length == QUIVER_PAGE_SIZE) fires AND the
        latest TransactionDate seen falls before our window, follow-up
        commit needs a real pagination loop.

        Auth: `Authorization: Bearer <key>` (Quiver also accepts the
        legacy `Token` form for back-compat)."""
        cache_path = CACHE_DIR / "quiver_bulk_congresstrading.json"
        if cache_path.exists():
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if age_hours < CACHE_TTL_HOURS:
                log.debug(f"Using cached Quiver bulk feed ({age_hours:.1f}h old)")
                return json.loads(cache_path.read_text())

        params = {
            "date_from": cutoff.isoformat(),
            "date_to": run_date.isoformat(),
            "page": 1,
            "page_size": QUIVER_PAGE_SIZE,
        }
        log.info(f"Fetching Quiver bulk feed from {QUIVER_FEED_URL} params={params}")
        resp = requests.get(
            QUIVER_FEED_URL,
            params=params,
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError(
                f"Unexpected Quiver response shape: {type(data).__name__}; "
                f"expected list. Body head: {str(data)[:200]}"
            )

        # Truncation diagnostic: if the response is exactly page_size,
        # we may have hit a server-side cap. Check the latest TransactionDate
        # to tell if we got the full window or only the most recent slice.
        if len(data) == QUIVER_PAGE_SIZE:
            txn_dates = [self._parse_date(r.get("TransactionDate")) for r in data]
            txn_dates = [d for d in txn_dates if d is not None]
            latest_txn = max(txn_dates) if txn_dates else None
            oldest_txn = min(txn_dates) if txn_dates else None
            within_window = oldest_txn is not None and oldest_txn <= cutoff
            log.warning(
                f"Quiver returned exactly {QUIVER_PAGE_SIZE} records — "
                f"may be truncated. latest_txn={latest_txn} oldest_txn={oldest_txn} "
                f"cutoff={cutoff} within_window={within_window}. "
                f"If within_window=False, pagination loop is required."
            )

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(data))
        return data

    def _parse_quiver_record(self, raw: Dict) -> Optional[CongressionalTrade]:
        """Parse one Quiver bulk congresstrading record (V2 schema).

        Field reference (verified against Quiver OpenAPI spec May 2026):
          Representative, BioGuideID, ReportDate, TransactionDate, Ticker,
          Transaction (Purchase / Sale (Partial) / Sale (Full) / Exchange),
          Range (STOCK Act bracket string), Amount, District,
          House (Representatives / Senate), Party (R / D),
          TickerType (CS = common stock, ST = stock, others non-equity),
          last_modified, ExcessReturn, PriceChange, SPYChange.
        """
        try:
            ticker = (raw.get("Ticker") or "").strip().upper()
            member = (raw.get("Representative") or "").strip()
            txn_type_raw = (raw.get("Transaction") or "").strip().lower()
            asset_desc = (raw.get("Description") or "").strip()
            amount_raw = (raw.get("Range") or raw.get("Amount") or "").strip()
            txn_date = self._parse_date(raw.get("TransactionDate"))
            disc_date = self._parse_date(raw.get("ReportDate"))
            chamber_raw = str(raw.get("House", "")).strip().lower()
            ticker_type = (raw.get("TickerType") or "").strip().upper()
        except (AttributeError, TypeError):
            return None

        if not member:
            return None
        if ticker in ("--", "N/A", ""):
            ticker = ""

        # `House` field is "Representatives" or "Senate" in V2 schema
        # ("House" in V1). Normalize both forms.
        if chamber_raw == "senate":
            chamber = "senate"
        elif chamber_raw in ("house", "representatives"):
            chamber = "house"
        else:
            chamber = "unknown"

        amount_min, amount_max = self._parse_amount(amount_raw)

        # Performance fields — Quiver's per-trade alpha tracking.
        # Float-coerce defensively; missing/null → None.
        excess_return = _coerce_float(raw.get("ExcessReturn"))
        price_change = _coerce_float(raw.get("PriceChange"))

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
            # Quiver `Transaction` values: "Purchase", "Sale (Full)",
            # "Sale (Partial)", "Exchange". Treat anything containing
            # "purchase" as a buy.
            is_purchase=("purchase" in txn_type_raw),
            is_high_signal_member=self._is_high_signal(member),
            raw_amount=amount_raw,
            ticker_type=ticker_type,
            excess_return=excess_return,
            price_change=price_change,
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
