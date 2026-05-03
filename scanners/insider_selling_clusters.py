"""Insider selling clusters scanner.

Mirror image of insider_buying (#1). Surfaces companies where 2+ different
insiders have sold open-market shares within the lookback window, with
aggregate value >= $1M. Reuses the entire SEC EDGAR Form 4 cache and parser
infrastructure built for #1 — zero new HTTP calls, zero new parsing.

Theory: when multiple insiders independently decide to sell substantial
amounts of stock simultaneously, they often share information the market
hasn't yet absorbed. Cluster sells are a bearish signal — though weaker
than cluster buys, since:
  - Many insider sales are routine (vesting, tax, diversification, 10b5-1 plans)
  - Insider buys are almost always discretionary; sells often aren't
  - Famous Peter Lynch quote: "There are many reasons to sell, but only one
    reason to buy"

The $1M aggregate threshold + 2+ insider cluster requirement filter most
routine vesting noise. What survives is concentrated bearish positioning.

Honest limits:
  - Cannot distinguish 10b5-1 plan sales from discretionary sales (the
    parsed Form 4 data doesn't include the plan flag)
  - Doesn't subtract the seller's remaining holdings — a CEO selling $5M
    while still holding $500M is different from one selling $5M while
    holding $5M total. Future enhancement.
  - Counter-signal: insiders sometimes sell INTO strength to rebalance,
    which can be a sign of confidence, not weakness
"""
from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests

from .base import Scanner, ScanResult, empty_result
from .edgar_client import (
    EDGAR_BASE,
    cik_to_ticker,
    edgar_get,
    load_cik_to_ticker,
)
from .insider_buying import (
    Form4Transaction,
    InsiderBuyingScanner,  # We'll subclass to reuse the heavy machinery
)
from .sec_cache import (
    cache_stats,
    is_filing_cached,
    load_cached_filing,
    load_cached_index,
    save_cached_filing,
    save_cached_index,
)

log = logging.getLogger(__name__)


class InsiderSellingClustersScanner(InsiderBuyingScanner):
    """Inverts InsiderBuyingScanner's purchase filter into a sale filter.

    Reuses every method of the parent: the daily index fetcher, the Form 4
    XML parser, the cache logic, the CIK-to-ticker resolution. The ONLY
    difference is the `run` method's filter step which selects 'S' (sale)
    transactions instead of 'P' (purchase) transactions.
    """

    name = "insider_selling_clusters"
    description = "Cluster sells (2+ insiders, $1M+ aggregate) via SEC Form 4"
    cadence = "daily"

    DEFAULT_LOOKBACK_DAYS = 10  # slightly wider than buys (sells trickle in over time)
    MIN_CLUSTER_INSIDERS = 2
    MIN_AGGREGATE_VALUE_USD = 1_000_000  # $1M aggregate threshold across cluster

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Lookback window: {self.lookback_days} days")
        log.info(f"Min cluster size: {self.MIN_CLUSTER_INSIDERS} insiders")
        log.info(f"Min aggregate value: ${self.MIN_AGGREGATE_VALUE_USD:,}")
        log.info(f"Cache state at start: {cache_stats()}")

        # Reuse parent's machinery for ticker map, filings collection, parsing
        try:
            ticker_map = load_cik_to_ticker()
        except Exception as e:
            log.exception("Failed to load CIK->ticker mapping")
            return empty_result(self.name, run_date, error=f"ticker map: {e}")

        try:
            filings = self._collect_form4_filings(run_date)
        except Exception as e:
            log.exception("Failed to collect Form 4 filings")
            return empty_result(self.name, run_date, error=f"index fetch: {e}")

        log.info(f"Found {len(filings)} Form 4 filings in lookback window")

        cached_count = sum(1 for f in filings if is_filing_cached(f["accession"]))
        log.info(f"  {cached_count} already cached, {len(filings) - cached_count} need fetching")

        transactions: List[Form4Transaction] = []
        for i, f in enumerate(filings):
            try:
                txns = self._parse_filing_cached(f, ticker_map)
                transactions.extend(txns)
            except Exception as e:
                log.debug(f"Skipping filing {f.get('accession')}: {e}")

            if (i + 1) % 100 == 0:
                log.info(f"Processed {i + 1}/{len(filings)} filings")

        log.info(f"Extracted {len(transactions)} transactions total")
        log.info(f"Cache state at end: {cache_stats()}")

        # FILTER FOR SALES (this is the key difference from #1):
        #   - is_acquisition False → disposition (sell side)
        #   - transaction_code 'S' → open-market sale (NOT 'M' option exercise, NOT 'F' tax withholding)
        #   - shares > 0 and price > 0 → real transaction with valid pricing
        #   - per-transaction value > $50k → drops fractional vesting noise
        sales = [
            t for t in transactions
            if not t.is_acquisition
            and t.transaction_code == "S"
            and t.shares > 0
            and t.price_per_share > 0
            and t.value_usd >= 50_000  # per-transaction floor
        ]
        log.info(f"Filtered to {len(sales)} open-market sales (transaction_code='S', value>=$50k)")

        if not sales:
            return empty_result(self.name, run_date)

        # Group sales by issuer
        by_issuer: Dict[str, List[Form4Transaction]] = defaultdict(list)
        for t in sales:
            by_issuer[t.issuer_cik].append(t)

        rows = []
        for issuer_cik, txns in by_issuer.items():
            distinct_insiders = {t.insider_cik for t in txns}
            if len(distinct_insiders) < self.MIN_CLUSTER_INSIDERS:
                continue

            total_value = sum(t.value_usd for t in txns)
            if total_value < self.MIN_AGGREGATE_VALUE_USD:
                continue

            ticker = cik_to_ticker(issuer_cik, ticker_map)
            if ticker is None:
                continue

            issuer_name = txns[0].issuer_name
            sell_count = len(txns)
            insider_count = len(distinct_insiders)
            total_shares = sum(t.shares for t in txns)
            valid_dates = [t.transaction_date or t.filing_date for t in txns if t.transaction_date or t.filing_date]
            earliest = min(valid_dates) if valid_dates else None
            latest = max(valid_dates) if valid_dates else None

            # Build seller list (deduplicated names)
            sellers = sorted(set(t.insider_name for t in txns))
            sellers_str = ", ".join(sellers[:5])
            if len(sellers) > 5:
                sellers_str += f" +{len(sellers) - 5} more"

            # Score: insider count weighted highest, value second, recency third
            value_millions = total_value / 1_000_000
            recency_bonus = max(0, 10 - (run_date - latest).days) if latest else 0
            score = insider_count * 50 + min(value_millions * 2, 50) + recency_bonus

            rows.append({
                "ticker": ticker,
                "issuer_name": issuer_name,
                "issuer_cik": issuer_cik,
                "insider_count": insider_count,
                "sell_count": sell_count,
                "total_shares": int(total_shares),
                "total_value_usd": round(total_value, 2),
                "earliest_sell": earliest.isoformat() if earliest else "",
                "latest_sell": latest.isoformat() if latest else "",
                "sellers": sellers_str,
                "score": round(score, 2),
                "reason": (
                    f"{insider_count} insiders sold ${total_value/1e6:.1f}M "
                    f"({sell_count} txns, {int(total_shares):,} sh) "
                    f"between {earliest} and {latest}"
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
                f"Filings parsed: {len(filings)}",
                f"Sales extracted: {len(sales)}",
                f"Distinct issuers w/ sells: {len(by_issuer)}",
                f"Clusters (>= {self.MIN_CLUSTER_INSIDERS} insiders, $1M+ aggregate): {len(rows)}",
                "Sells are weaker signal than buys — many are routine. Cluster requirement filters most noise.",
            ],
        )