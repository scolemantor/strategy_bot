"""13F changes scanner.

Surfaces equities where curated "smart money" funds either opened a new
position or substantially added to an existing position in the most recent
quarter. The signal is qualitative — 13F filings lag by 45 days so the data
is stale by design — but it tells us *who* is holding *what* with conviction.

Source: SEC Form 13F-HR filings from 25 hardcoded funds (Berkshire, Baupost,
Greenlight, Pershing Square, Third Point, Trian, Starboard, ValueAct, Coatue,
Tiger Global, Lone Pine, Viking, Maverick, Bridgewater, Renaissance, Citadel,
Millennium, Two Sigma, ARK, Whale Rock, Light Street, Elliott, Marathon,
Duquesne, Miller).

Architecture:
  1. For each fund, fetch the latest 2 13F-HR filings via SEC submissions API.
  2. Parse the information table XML from each filing.
  3. Resolve CUSIPs to tickers via OpenFIGI (cached forever per CUSIP).
  4. Diff current vs prior quarter holdings.
  5. Flag NEW positions OR position adds >50% in shares, where new dollar value > $50M.
  6. Output ranked list.

Caching:
  - Per-fund filings list: 24h (refreshed daily; new 13Fs appear on 45th day after quarter)
  - Per-filing parsed holdings: forever (13Fs are immutable post-filing)
  - CUSIP→ticker: forever (CUSIPs don't change)

Honest limits:
  - 13Fs are 45+ days old when filed. Not actionable timing.
  - Long-only — 13Fs don't show short positions.
  - Doesn't capture options, rights, warrants — only common stock.
  - First run is slow (~30-60 min) due to OpenFIGI rate limits. Subsequent
    runs are near-instant for the same CUSIPs.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests

from .base import Scanner, ScanResult, empty_result
from .cusip_mapper import resolve_cusips
from .edgar_client import (
    fetch_13f_filings_list,
    fetch_13f_holdings,
)
from .sec_cache import (
    is_13f_filing_cached,
    load_cached_13f_filing,
    load_cached_13f_filings_list,
    save_cached_13f_filing,
    save_cached_13f_filings_list,
)

log = logging.getLogger(__name__)


# 25 verified funds (resolved via SEC EDGAR company browse 2026-05-03).
# Format: display_name -> CIK (10-digit padded).
TRACKED_FUNDS: Dict[str, str] = {
    "Berkshire Hathaway":       "0001067983",
    "Baupost Group":            "0001054420",
    "Greenlight Capital":       "0001079114",
    "Pershing Square":          "0001336528",
    "Third Point":              "0001040273",
    "Trian Fund Management":    "0001345471",
    "Starboard Value":          "0001517137",
    "ValueAct Holdings":        "0001418814",
    "Coatue Management":        "0001135730",
    "Tiger Global":             "0001167483",
    "Lone Pine Capital":        "0001061165",
    "Viking Global":            "0001103804",
    "Maverick Capital":         "0000928617",
    "Bridgewater Associates":   "0001350694",
    "Renaissance Technologies": "0001037389",
    "ARK Investment Mgmt":      "0001697748",
    "Whale Rock Capital":       "0001387322",
    "Light Street Capital":     "0001569049",
    "Elliott Management":       "0001048445",
    "Marathon Asset Mgmt":      "0001279913",
    "Duquesne Family Office":   "0001536411",
    "Miller Value Partners":    "0001135778",
}


class ThirteenFChangesScanner(Scanner):
    name = "thirteen_f_changes"
    description = "New positions or significant adds (>50%) by tracked smart-money funds"
    cadence = "weekly"  # 13F data updates quarterly; weekly run keeps freshness without burning resources

    MIN_NEW_POSITION_VALUE = 50_000_000  # $50M minimum to surface
    SIGNIFICANT_ADD_PCT = 0.50  # 50%+ share increase counts as "significant"
    MAX_REASONABLE_ADD_PCT = 5.00  # 500%+ is almost always position re-establishment after sell

    # Staleness filter (added 2026-05-10): 13F alpha decays sharply after
    # 2-3 weeks per academic literature on 13F frontrunning. Filings
    # older than MAX_FILING_AGE_DAYS are filtered out entirely; survivors
    # within the window get a multiplier applied to their final score.
    # Override via env var THIRTEEN_F_MAX_AGE_DAYS for testing/tuning.
    DEFAULT_MAX_FILING_AGE_DAYS = 21

    @property
    def max_filing_age_days(self) -> int:
        try:
            return int(os.environ.get(
                "THIRTEEN_F_MAX_AGE_DAYS",
                self.DEFAULT_MAX_FILING_AGE_DAYS,
            ))
        except ValueError:
            return self.DEFAULT_MAX_FILING_AGE_DAYS

    @staticmethod
    def _staleness_multiplier(days_since_filing: int) -> float:
        """Score decay by filing age. Returns 0 when past the cutoff —
        caller should filter those rows out, not just zero the score
        (otherwise stale rows still appear in candidates with score=0).

        Buckets:
          0-7 days   -> 1.00x  (full score, post-filing reaction window)
          8-14 days  -> 0.75x  (decayed but still actionable)
          15-21 days -> 0.50x  (residual; tail of the window)
          >21 days   -> 0.00x  (filtered out at run/backtest time)
        Negative values (filing_date in the future, e.g. timezone edge
        cases) are treated as fresh."""
        if days_since_filing < 0:
            return 1.0
        if days_since_filing <= 7:
            return 1.0
        if days_since_filing <= 14:
            return 0.75
        if days_since_filing <= 21:
            return 0.50
        return 0.0

    def _apply_staleness_filter(
        self,
        changes: List[Dict],
        reference_date: date,
    ) -> List[Dict]:
        """Drop changes whose filing_date is past max_filing_age_days from
        reference_date. Annotate survivors with `days_since_filing` and
        `staleness_multiplier` for downstream score scaling."""
        cutoff = self.max_filing_age_days
        kept: List[Dict] = []
        for c in changes:
            fd = c.get("filing_date")
            if fd is None:
                # No filing date is suspicious for a 13F change; drop it.
                continue
            days_old = (reference_date - fd).days
            mult = self._staleness_multiplier(days_old)
            if mult == 0.0 or days_old > cutoff:
                continue
            c["days_since_filing"] = days_old
            c["staleness_multiplier"] = mult
            kept.append(c)
        return kept

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Tracking {len(TRACKED_FUNDS)} smart-money funds")
        log.info(f"Min new-position value: ${self.MIN_NEW_POSITION_VALUE:,}")
        log.info(f"Significant-add threshold: {self.SIGNIFICANT_ADD_PCT:.0%}")

        # Step 1: For each fund, get the latest 2 filings (current quarter + prior)
        all_changes: List[Dict] = []
        funds_with_changes = 0
        funds_with_no_recent_filings = 0
        funds_with_errors = 0

        for fund_name, cik in TRACKED_FUNDS.items():
            log.info(f"Processing fund: {fund_name} (CIK {cik})")
            try:
                changes = self._process_fund(fund_name, cik)
            except Exception as e:
                log.exception(f"  Failed to process {fund_name}: {e}")
                funds_with_errors += 1
                continue

            if changes is None:
                funds_with_no_recent_filings += 1
                log.info(f"  Insufficient filings to compute diff")
                continue

            if changes:
                funds_with_changes += 1
                all_changes.extend(changes)
                log.info(f"  Found {len(changes)} qualifying changes")
            else:
                log.info(f"  No qualifying changes (all positions trimmed or below thresholds)")

        log.info(
            f"Summary: {funds_with_changes} funds with changes, "
            f"{funds_with_no_recent_filings} insufficient data, "
            f"{funds_with_errors} errors"
        )

        if not all_changes:
            return empty_result(self.name, run_date)

        # Step 1.5: staleness filter. 13F alpha decays sharply after
        # 2-3 weeks; drop filings older than max_filing_age_days from
        # run_date. See _staleness_multiplier docstring for buckets.
        before_staleness = len(all_changes)
        all_changes = self._apply_staleness_filter(all_changes, run_date)
        log.info(
            f"Staleness filter (max {self.max_filing_age_days}d): "
            f"{len(all_changes)} kept, {before_staleness - len(all_changes)} dropped"
        )

        if not all_changes:
            log.info(
                "All qualifying changes were past the staleness cutoff. "
                "Next quarter's 13Fs typically file 45 days after quarter end."
            )
            return empty_result(self.name, run_date)

        # Step 2: Resolve all unique CUSIPs to tickers
        unique_cusips = list(set(c["cusip"] for c in all_changes if c["cusip"]))
        log.info(f"Resolving {len(unique_cusips)} unique CUSIPs to tickers...")
        cusip_to_ticker = resolve_cusips(unique_cusips)
        resolved = sum(1 for t in cusip_to_ticker.values() if t)
        log.info(f"  Resolved {resolved}/{len(unique_cusips)} CUSIPs")

        # Step 3: Build output rows, dropping any without a resolvable ticker
        rows = []
        for c in all_changes:
            ticker = cusip_to_ticker.get(c["cusip"])
            if not ticker:
                continue

            # Score: dollar value (in millions) + bonus for "new" vs "add",
            # decayed by filing age (staleness_multiplier set by
            # _apply_staleness_filter above).
            value_score = min(100, c["new_value"] / 10_000_000)  # $1B = 100, $100M = 10
            new_bonus = 30 if c["action"] == "new" else 0
            pct_bonus = min(20, c.get("pct_increase", 0) * 10) if c["action"] == "add" else 0
            raw_score = value_score + new_bonus + pct_bonus
            score = raw_score * c["staleness_multiplier"]

            rows.append({
                "ticker": ticker,
                "fund_name": c["fund_name"],
                "action": c["action"],
                "name_of_issuer": c["name_of_issuer"],
                "cusip": c["cusip"],
                "new_value": c["new_value"],
                "new_shares": c["new_shares"],
                "prior_shares": c.get("prior_shares", 0),
                "pct_increase": round(c.get("pct_increase", 0) * 100, 1) if c["action"] == "add" else None,
                "filing_date": c["filing_date"].isoformat() if c.get("filing_date") else "",
                "period_of_report": c["period_of_report"].isoformat() if c.get("period_of_report") else "",
                "days_since_filing": c["days_since_filing"],
                "staleness_multiplier": c["staleness_multiplier"],
                "score": round(score, 2),
                "reason": self._build_reason(c, ticker),
            })

        if not rows:
            log.info("No changes survived ticker resolution")
            return empty_result(self.name, run_date)

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Funds tracked: {len(TRACKED_FUNDS)}",
                f"Funds with qualifying changes: {funds_with_changes}",
                f"Min new-position value: ${self.MIN_NEW_POSITION_VALUE:,}",
                f"Significant-add threshold: {self.SIGNIFICANT_ADD_PCT:.0%}",
                f"Staleness window: {self.max_filing_age_days} days from run_date",
                f"Final candidates after ticker resolution: {len(rows)}",
            ],
        )

    def _build_reason(self, change: Dict, ticker: str) -> str:
        age_suffix = ""
        days_old = change.get("days_since_filing")
        if days_old is not None:
            age_suffix = f" [filed {days_old}d ago]"
        if change["action"] == "new":
            return (
                f"{change['fund_name']} opened NEW position in {ticker} "
                f"(${change['new_value']/1e6:.1f}M, {change['new_shares']:,} sh) "
                f"in Q ending {change['period_of_report']}{age_suffix}"
            )
        else:
            pct = change.get("pct_increase", 0) * 100
            return (
                f"{change['fund_name']} INCREASED {ticker} by {pct:.0f}% "
                f"({change.get('prior_shares', 0):,} -> {change['new_shares']:,} sh, "
                f"${change['new_value']/1e6:.1f}M) in Q ending {change['period_of_report']}{age_suffix}"
            )

    def _process_fund(self, fund_name: str, cik: str) -> Optional[List[Dict]]:
        """Get the latest two 13F-HR filings for this fund and diff them.

        Returns:
          - List of qualifying changes (could be empty)
          - None if there aren't enough filings to compute a diff
        """
        # Get filings list (cached 24h)
        filings = load_cached_13f_filings_list(cik)
        if filings is None:
            try:
                filings = fetch_13f_filings_list(cik, limit=4)
                save_cached_13f_filings_list(cik, filings)
            except requests.HTTPError as e:
                log.warning(f"  HTTP error fetching filings list: {e}")
                return None
            except Exception as e:
                log.warning(f"  Error fetching filings list: {e}")
                return None

        # Filter to 13F-HR (drop amendments — they would double-count)
        filings = [f for f in filings if f["form_type"] == "13F-HR"]
        if len(filings) < 2:
            return None

        # Sort by period_of_report descending
        filings = sorted(filings, key=lambda f: f.get("period_of_report") or date.min, reverse=True)
        current_filing = filings[0]
        prior_filing = filings[1]

        # Get holdings for both
        current_holdings = self._get_holdings(cik, current_filing)
        prior_holdings = self._get_holdings(cik, prior_filing)

        if not current_holdings:
            log.warning(f"  Current filing {current_filing['accession']} has no holdings")
            return None

        # Aggregate holdings by CUSIP (some funds report same security in multiple
        # rows — different lots, share classes, etc. — but we want one position per security).
        def aggregate_by_cusip(holdings_list):
            by_cusip = {}
            for h in holdings_list:
                c = h["cusip"]
                if not c:
                    continue
                if c in by_cusip:
                    by_cusip[c]["shares"] += h["shares"]
                    by_cusip[c]["value_dollars"] += h["value_dollars"]
                else:
                    by_cusip[c] = dict(h)
            return list(by_cusip.values())

        current_holdings = aggregate_by_cusip(current_holdings)
        prior_holdings = aggregate_by_cusip(prior_holdings)

        # Build a {cusip: shares} map for prior
        prior_map = {h["cusip"]: h["shares"] for h in prior_holdings}

        changes = []
        for h in current_holdings:
            cusip = h["cusip"]
            new_shares = h["shares"]
            new_value = h["value_dollars"]

            if not cusip:
                continue
            if new_value < self.MIN_NEW_POSITION_VALUE:
                continue

            prior_shares = prior_map.get(cusip, 0)

            if prior_shares == 0:
                # NEW position
                changes.append({
                    "fund_name": fund_name,
                    "action": "new",
                    "cusip": cusip,
                    "name_of_issuer": h["name_of_issuer"],
                    "new_shares": new_shares,
                    "prior_shares": 0,
                    "new_value": new_value,
                    "filing_date": current_filing["filing_date"],
                    "period_of_report": current_filing["period_of_report"],
                })
            elif new_shares > prior_shares:
                pct_increase = (new_shares - prior_shares) / prior_shares
                if pct_increase >= self.SIGNIFICANT_ADD_PCT and pct_increase <= self.MAX_REASONABLE_ADD_PCT:
                    changes.append({
                        "fund_name": fund_name,
                        "action": "add",
                        "cusip": cusip,
                        "name_of_issuer": h["name_of_issuer"],
                        "new_shares": new_shares,
                        "prior_shares": prior_shares,
                        "new_value": new_value,
                        "pct_increase": pct_increase,
                        "filing_date": current_filing["filing_date"],
                        "period_of_report": current_filing["period_of_report"],
                    })

        return changes

    def _get_holdings(self, cik: str, filing: Dict) -> List[Dict]:
        """Fetch holdings for a single filing, with caching."""
        accession = filing["accession"]
        cached = load_cached_13f_filing(accession)
        if cached is not None:
            return cached

        try:
            holdings = fetch_13f_holdings(
                cik=cik,
                accession=accession,
                primary_document=filing.get("primary_document"),
            )
        except Exception as e:
            log.warning(f"  Error parsing holdings for {accession}: {e}")
            return []

        save_cached_13f_filing(accession, holdings)
        return holdings
# --- Phase 4e backtest support ---

def backtest_mode(as_of_date: date, output_dir=None) -> int:
    """Run thirteen_f_changes scanner as-of a historical date.

    Look-ahead protection: 13F filings are fetched via SEC submissions API
    which returns ALL historical filings. We filter to filings with
    filing_date <= as_of_date so we only see what was actually visible on
    that historical date.

    Filings have a 45-day SEC reporting delay. So as-of 2024-09-15, the most
    recent 13F we'd see is for the quarter ending 2024-06-30 (filed Aug 14,
    2024), and the prior is the quarter ending 2024-03-31 (filed May 15).

    Output goes to <output_dir>/<as_of_date>/thirteen_f_changes.csv.
    """
    from pathlib import Path

    output_dir = Path(output_dir) if output_dir else Path("backtest_output")
    scanner = ThirteenFChangesScanner()

    log.info(f"thirteen_f_changes backtest as-of {as_of_date}: tracking {len(TRACKED_FUNDS)} funds")

    all_changes: List[Dict] = []

    for fund_name, cik in TRACKED_FUNDS.items():
        try:
            changes = _process_fund_for_backtest(fund_name, cik, as_of_date, scanner)
        except Exception as e:
            log.debug(f"  {fund_name}: error {e}")
            continue

        if changes:
            all_changes.extend(changes)

    if not all_changes:
        return 0

    # Staleness filter: same buckets as production run(), but reference
    # date is as_of_date so historical replays decay correctly relative
    # to when the signal would have been visible in real-time.
    before_staleness = len(all_changes)
    all_changes = scanner._apply_staleness_filter(all_changes, as_of_date)
    log.debug(
        f"  thirteen_f_changes {as_of_date}: staleness filter "
        f"(max {scanner.max_filing_age_days}d) kept "
        f"{len(all_changes)}/{before_staleness}"
    )
    if not all_changes:
        return 0

    # Resolve CUSIPs to tickers (cached forever)
    unique_cusips = list(set(c["cusip"] for c in all_changes if c["cusip"]))
    cusip_to_ticker = resolve_cusips(unique_cusips)

    rows = []
    for c in all_changes:
        ticker = cusip_to_ticker.get(c["cusip"])
        if not ticker:
            continue

        value_score = min(100, c["new_value"] / 10_000_000)
        new_bonus = 30 if c["action"] == "new" else 0
        pct_bonus = min(20, c.get("pct_increase", 0) * 10) if c["action"] == "add" else 0
        raw_score = value_score + new_bonus + pct_bonus
        score = raw_score * c["staleness_multiplier"]

        rows.append({
            "ticker": ticker,
            "fund_name": c["fund_name"],
            "action": c["action"],
            "name_of_issuer": c["name_of_issuer"],
            "cusip": c["cusip"],
            "new_value": c["new_value"],
            "new_shares": c["new_shares"],
            "prior_shares": c.get("prior_shares", 0),
            "pct_increase": round(c.get("pct_increase", 0) * 100, 1) if c["action"] == "add" else None,
            "filing_date": c["filing_date"].isoformat() if c.get("filing_date") else "",
            "period_of_report": c["period_of_report"].isoformat() if c.get("period_of_report") else "",
            "days_since_filing": c["days_since_filing"],
            "staleness_multiplier": c["staleness_multiplier"],
            "score": round(score, 2),
            "reason": scanner._build_reason(c, ticker),
        })

    if not rows:
        return 0

    df_out = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)

    date_dir = output_dir / as_of_date.isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    out_path = date_dir / "thirteen_f_changes.csv"
    df_out.to_csv(out_path, index=False)
    log.debug(f"  thirteen_f_changes {as_of_date}: wrote {len(df_out)} candidates to {out_path}")

    return len(df_out)


def _process_fund_for_backtest(fund_name: str, cik: str, as_of_date: date, scanner) -> Optional[List[Dict]]:
    """Backtest variant of _process_fund. Filters filings to those filed before as_of_date."""
    filings = load_cached_13f_filings_list(cik)
    if filings is None:
        try:
            filings = fetch_13f_filings_list(cik, limit=20)  # extra limit for historical filtering
            save_cached_13f_filings_list(cik, filings)
        except Exception:
            return None

    # CRITICAL: filter to filings actually visible as-of the historical date
    filings = [
        f for f in filings
        if f["form_type"] == "13F-HR"
        and f.get("filing_date") is not None
        and f["filing_date"] <= as_of_date
    ]
    if len(filings) < 2:
        return None

    filings = sorted(filings, key=lambda f: f.get("period_of_report") or date.min, reverse=True)
    current_filing = filings[0]
    prior_filing = filings[1]

    current_holdings = scanner._get_holdings(cik, current_filing)
    prior_holdings = scanner._get_holdings(cik, prior_filing)

    if not current_holdings:
        return None

    def aggregate_by_cusip(holdings_list):
        by_cusip = {}
        for h in holdings_list:
            c = h["cusip"]
            if not c:
                continue
            if c in by_cusip:
                by_cusip[c]["shares"] += h["shares"]
                by_cusip[c]["value_dollars"] += h["value_dollars"]
            else:
                by_cusip[c] = dict(h)
        return list(by_cusip.values())

    current_holdings = aggregate_by_cusip(current_holdings)
    prior_holdings = aggregate_by_cusip(prior_holdings)

    prior_map = {h["cusip"]: h["shares"] for h in prior_holdings}

    changes = []
    for h in current_holdings:
        cusip = h["cusip"]
        new_shares = h["shares"]
        new_value = h["value_dollars"]

        if not cusip or new_value < scanner.MIN_NEW_POSITION_VALUE:
            continue

        prior_shares = prior_map.get(cusip, 0)

        if prior_shares == 0:
            changes.append({
                "fund_name": fund_name,
                "action": "new",
                "cusip": cusip,
                "name_of_issuer": h["name_of_issuer"],
                "new_shares": new_shares,
                "prior_shares": 0,
                "new_value": new_value,
                "filing_date": current_filing["filing_date"],
                "period_of_report": current_filing["period_of_report"],
            })
        elif new_shares > prior_shares:
            pct_increase = (new_shares - prior_shares) / prior_shares
            if pct_increase >= scanner.SIGNIFICANT_ADD_PCT and pct_increase <= scanner.MAX_REASONABLE_ADD_PCT:
                changes.append({
                    "fund_name": fund_name,
                    "action": "add",
                    "cusip": cusip,
                    "name_of_issuer": h["name_of_issuer"],
                    "new_shares": new_shares,
                    "prior_shares": prior_shares,
                    "new_value": new_value,
                    "pct_increase": pct_increase,
                    "filing_date": current_filing["filing_date"],
                    "period_of_report": current_filing["period_of_report"],
                })

    return changes