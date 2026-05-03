"""FINRA short interest data client.

FINRA publishes bi-monthly short interest reports as CSV files at predictable
URLs based on settlement date:
  https://cdn.finra.org/equity/otcmarket/biweekly/shrt{YYYYMMDD}.csv

Settlement dates are the 15th and last business day of each month. Reports
are published 7 business days after settlement.

Despite "otcmarket" in the URL path, post-June-2021 files include all
exchange-listed equities, not just OTC. The path naming is historical.

The CSV is pipe-delimited (despite the .csv extension) with a header row.
Columns (from FINRA docs):
  symbolCode | settlementDate | name | marketCategory | currentShortShares
  | previousShortShares | changePercent | percentChange | averageDailyShares
  | daysToCover | marketCenter

Cache forever per settlement date — published files are immutable.
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from io import StringIO
from pathlib import Path
from typing import List, Optional

import pandas as pd
import requests

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache") / "finra_short_interest"
FINRA_URL_TEMPLATE = "https://cdn.finra.org/equity/otcmarket/biweekly/shrt{date_str}.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; strategy_bot/1.0; +https://github.com/scolemantor/strategy_bot)",
    "Accept": "text/csv,application/octet-stream",
}


def _cache_path(settlement_date: date) -> Path:
    return CACHE_DIR / f"shrt{settlement_date.strftime('%Y%m%d')}.csv"


def _load_cached(settlement_date: date) -> Optional[pd.DataFrame]:
    p = _cache_path(settlement_date)
    if not p.exists():
        return None
    try:
        return pd.read_csv(p, sep="|", dtype=str)
    except Exception as e:
        log.debug(f"Failed to load FINRA cache for {settlement_date}: {e}")
        return None


def _save_cached(settlement_date: date, df: pd.DataFrame) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(_cache_path(settlement_date), sep="|", index=False)


def fetch_short_interest(settlement_date: date) -> Optional[pd.DataFrame]:
    """Fetch FINRA short interest data for a specific settlement date.

    Returns DataFrame with columns: symbol, name, current_short_shares,
    avg_daily_shares, days_to_cover, change_pct.

    Returns None if the file isn't available (e.g. date doesn't have a published file).
    """
    cached = _load_cached(settlement_date)
    if cached is not None:
        log.debug(f"Using cached FINRA data for {settlement_date}")
        return _normalize_df(cached)

    date_str = settlement_date.strftime("%Y%m%d")
    url = FINRA_URL_TEMPLATE.format(date_str=date_str)
    log.info(f"Fetching FINRA short interest for settlement {settlement_date}")

    try:
        time.sleep(0.5)  # courtesy delay
        r = requests.get(url, headers=HEADERS, timeout=60)
        if r.status_code == 404:
            log.info(f"  FINRA file not yet published for {settlement_date}")
            return None
        if r.status_code != 200:
            log.warning(f"  FINRA returned HTTP {r.status_code}")
            return None

        # FINRA returns pipe-delimited despite the .csv extension
        csv_text = r.text
        if not csv_text or len(csv_text) < 100:
            log.warning(f"  FINRA file for {settlement_date} appears empty ({len(csv_text)} chars)")
            return None

        df = pd.read_csv(StringIO(csv_text), sep="|", dtype=str)
        _save_cached(settlement_date, df)
        log.info(f"  Got {len(df)} short interest records")
        return _normalize_df(df)
    except Exception as e:
        log.warning(f"  Failed to fetch FINRA data: {e}")
        return None


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column names and types for downstream use.

    FINRA's column names vary slightly between historical formats. We map
    common variants to canonical names.
    """
    # Map FINRA column names (which can vary) to our standard
    col_map = {
        # Modern FINRA names (current as of 2026)
        "symbolCode": "symbol",
        "issueName": "name",
        "currentShortPositionQuantity": "current_short_shares",
        "previousShortPositionQuantity": "previous_short_shares",
        "averageDailyVolumeQuantity": "avg_daily_shares",
        "daysToCoverQuantity": "days_to_cover",
        "changePercent": "change_pct",
        "changePreviousNumber": "change_previous",
        "marketClassCode": "market_category",
        "issuerServicesGroupExchangeCode": "exchange_code",
        "settlementDate": "settlement_date",
        "stockSplitFlag": "stock_split_flag",
        "revisionFlag": "revision_flag",

        # Legacy / alternate names (kept for backward compatibility with older cached files)
        "issueSymbolIdentifier": "symbol",
        "currentShortShareNumber": "current_short_shares",
        "previousShortShareNumber": "previous_short_shares",
        "averageShortShareNumber": "avg_daily_shares",
        "averageDailyShareVolume": "avg_daily_shares",
        "marketCategoryCode": "market_category",
        "marketCenter": "market_center",
        "percentageChangefromPreviousShort": "change_pct",
    }

    rename_map = {old: new for old, new in col_map.items() if old in df.columns}
    df = df.rename(columns=rename_map)

    # Coerce numeric columns
    numeric_cols = ["current_short_shares", "previous_short_shares", "avg_daily_shares", "days_to_cover", "change_pct"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def find_latest_published(run_date: date, max_lookback_days: int = 30) -> Optional[date]:
    """Walk back from run_date through likely settlement dates to find the most
    recent published FINRA file.

    Settlement dates are 15th and last business day of each month. We try those
    candidates in reverse-chronological order until we find one that returns data.
    Files are published ~7 business days after settlement, so we skip recent dates.
    """
    candidates: List[date] = []

    # Walk back month-by-month, generating both 15th and end-of-month candidates
    cursor = run_date
    for _ in range(3):  # 3 months back covers ~6 settlement dates
        # End of cursor's month
        next_month = cursor.replace(day=28) + timedelta(days=4)
        eom = next_month - timedelta(days=next_month.day)
        # Adjust for weekend
        while eom.weekday() >= 5:
            eom -= timedelta(days=1)
        candidates.append(eom)

        # 15th of cursor's month
        mid = cursor.replace(day=15)
        while mid.weekday() >= 5:
            mid -= timedelta(days=1)
        candidates.append(mid)

        # Step back a month
        cursor = (cursor.replace(day=1) - timedelta(days=1))

    # Filter out dates that are too recent (publication lag is 7 business days)
    earliest_publishable = run_date - timedelta(days=10)
    candidates = [d for d in candidates if d <= earliest_publishable and d >= run_date - timedelta(days=max_lookback_days)]

    # Sort newest first
    candidates = sorted(set(candidates), reverse=True)

    log.info(f"Trying FINRA settlement dates: {[d.isoformat() for d in candidates]}")

    for candidate in candidates:
        df = fetch_short_interest(candidate)
        if df is not None and not df.empty:
            log.info(f"  Latest published: {candidate}")
            return candidate

    log.warning("  No recent published FINRA file found")
    return None