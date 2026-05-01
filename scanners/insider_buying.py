"""Insider buying scanner via SEC EDGAR Form 4 filings."""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import requests
from lxml import etree

from .base import Scanner, ScanResult, empty_result
from .edgar_client import (
    EDGAR_BASE,
    cik_to_ticker,
    edgar_get,
    load_cik_to_ticker,
)

log = logging.getLogger(__name__)

# Match a daily-index row.
# Form types are short codes like "4", "4/A", "10-K", "S-1", "SC 13G/A".
# We accept letters/digits/hyphens/slashes plus optional " 13" / " 13G" etc.
ROW_REGEX = re.compile(
    r"^(?P<form>[\w\-/]+(?:\s+\d{1,3}[\w\-/]*)?)"  # form type
    r"\s{2,}"
    r"(?P<company>.+?)"                             # company name
    r"\s{2,}"
    r"(?P<cik>\d{1,10})"                            # CIK
    r"\s+"
    r"(?P<date>\d{8})"                              # filing date YYYYMMDD
    r"\s+"
    r"(?P<filename>edgar/\S+)"                      # path
    r"\s*$"
)


@dataclass
class Form4Transaction:
    issuer_cik: str
    issuer_name: str
    insider_cik: str
    insider_name: str
    filing_date: date
    transaction_date: Optional[date]
    transaction_code: str
    is_acquisition: bool
    is_purchase: bool
    shares: float
    price_per_share: float
    accession: str

    @property
    def value_usd(self) -> float:
        return self.shares * self.price_per_share


class InsiderBuyingScanner(Scanner):
    name = "insider_buying"
    description = "Cluster buys (2+ insiders, 30-day window) via SEC Form 4 parsing"
    cadence = "daily"

    LOOKBACK_DAYS = 30
    MIN_CLUSTER_INSIDERS = 2
    MIN_TRANSACTION_VALUE_USD = 5_000

    def run(self, run_date: date) -> ScanResult:
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

        transactions: List[Form4Transaction] = []
        for i, f in enumerate(filings):
            try:
                txns = self._parse_filing(f, ticker_map)
                transactions.extend(txns)
            except Exception as e:
                log.debug(f"Skipping filing {f.get('accession')}: {e}")
            if (i + 1) % 100 == 0:
                log.info(f"Parsed {i + 1}/{len(filings)} filings")

        log.info(f"Extracted {len(transactions)} transactions")

        purchases = [
            t for t in transactions
            if t.is_acquisition
            and t.is_purchase
            and t.shares > 0
            and t.price_per_share > 0
            and t.value_usd >= self.MIN_TRANSACTION_VALUE_USD
        ]
        log.info(f"Filtered to {len(purchases)} open-market purchases")

        if not purchases:
            return empty_result(self.name, run_date)

        by_issuer: Dict[str, List[Form4Transaction]] = defaultdict(list)
        for t in purchases:
            by_issuer[t.issuer_cik].append(t)

        rows = []
        for issuer_cik, txns in by_issuer.items():
            distinct_insiders = {t.insider_cik for t in txns}
            if len(distinct_insiders) < self.MIN_CLUSTER_INSIDERS:
                continue

            ticker = cik_to_ticker(issuer_cik, ticker_map)
            if ticker is None:
                continue

            issuer_name = txns[0].issuer_name
            total_value = sum(t.value_usd for t in txns)
            buy_count = len(txns)
            insider_count = len(distinct_insiders)
            earliest = min(t.transaction_date or t.filing_date for t in txns)
            latest = max(t.transaction_date or t.filing_date for t in txns)

            rows.append({
                "ticker": ticker,
                "issuer_name": issuer_name,
                "issuer_cik": issuer_cik,
                "insider_count": insider_count,
                "buy_count": buy_count,
                "total_value_usd": round(total_value, 2),
                "earliest_buy": earliest.isoformat(),
                "latest_buy": latest.isoformat(),
                "score": insider_count * 100 + min(buy_count * 5, 50),
                "reason": (
                    f"{insider_count} insiders, {buy_count} buys, "
                    f"${total_value:,.0f} total, {earliest} to {latest}"
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
                f"Lookback: {self.LOOKBACK_DAYS} days",
                f"Filings parsed: {len(filings)}",
                f"Purchases extracted: {len(purchases)}",
                f"Distinct issuers w/ buys: {len(by_issuer)}",
                f"Clusters (>= {self.MIN_CLUSTER_INSIDERS} insiders): {len(rows)}",
            ],
        )

    def _collect_form4_filings(self, run_date: date) -> List[Dict]:
        end = run_date
        start = run_date - timedelta(days=self.LOOKBACK_DAYS)
        all_filings: List[Dict] = []

        cur = start
        while cur <= end:
            if cur.weekday() >= 5:
                cur += timedelta(days=1)
                continue
            try:
                day_filings = self._fetch_daily_index(cur)
                form4 = [f for f in day_filings if f["form_type"].startswith("4")]
                all_filings.extend(form4)
                if day_filings:
                    log.info(f"  {cur}: {len(day_filings)} total filings, {len(form4)} Form 4s")
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

    def _parse_filing(
        self, filing: Dict, ticker_map: Dict[str, str],
    ) -> List[Form4Transaction]:
        if not filing["accession"]:
            return []

        try:
            resp = edgar_get(filing["filing_index_url"])
        except Exception as e:
            log.debug(f"Failed to fetch filing index {filing['filing_index_url']}: {e}")
            return []

        xml_match = re.search(
            r'href="(/Archives/edgar/data/\d+/\d+/[^"]+\.xml)"',
            resp.text,
        )
        if not xml_match:
            return []
        xml_url = EDGAR_BASE + xml_match.group(1)

        try:
            xml_resp = edgar_get(xml_url)
        except Exception as e:
            log.debug(f"Failed to fetch Form 4 XML {xml_url}: {e}")
            return []

        return self._parse_form4_xml(xml_resp.content, filing)

    def _parse_form4_xml(
        self, xml_bytes: bytes, filing_meta: Dict,
    ) -> List[Form4Transaction]:
        try:
            root = etree.fromstring(xml_bytes)
        except etree.XMLSyntaxError as e:
            log.debug(f"XML parse error in {filing_meta.get('accession')}: {e}")
            return []

        def find_text(elem, path: str) -> Optional[str]:
            found = elem.find(path)
            if found is not None and found.text is not None:
                return found.text.strip()
            return None

        issuer_cik = find_text(root, "issuer/issuerCik")
        issuer_name = find_text(root, "issuer/issuerName") or ""
        if not issuer_cik:
            return []
        issuer_cik = issuer_cik.zfill(10)

        insider_cik = find_text(root, "reportingOwner/reportingOwnerId/rptOwnerCik")
        insider_name = find_text(root, "reportingOwner/reportingOwnerId/rptOwnerName") or ""
        if not insider_cik:
            return []
        insider_cik = insider_cik.zfill(10)

        out: List[Form4Transaction] = []
        for txn in root.findall("nonDerivativeTable/nonDerivativeTransaction"):
            txn_code = find_text(txn, "transactionCoding/transactionCode") or ""
            ad_code = find_text(txn, "transactionAmounts/transactionAcquiredDisposedCode/value") or ""
            shares_str = find_text(txn, "transactionAmounts/transactionShares/value") or "0"
            price_str = find_text(txn, "transactionAmounts/transactionPricePerShare/value") or "0"
            txn_date_str = find_text(txn, "transactionDate/value")

            try:
                shares = float(shares_str)
                price = float(price_str)
            except ValueError:
                continue

            txn_date = None
            if txn_date_str:
                try:
                    txn_date = datetime.strptime(txn_date_str, "%Y-%m-%d").date()
                except ValueError:
                    pass

            out.append(Form4Transaction(
                issuer_cik=issuer_cik,
                issuer_name=issuer_name,
                insider_cik=insider_cik,
                insider_name=insider_name,
                filing_date=filing_meta["filing_date"],
                transaction_date=txn_date,
                transaction_code=txn_code,
                is_acquisition=(ad_code == "A"),
                is_purchase=(txn_code == "P"),
                shares=shares,
                price_per_share=price,
                accession=filing_meta.get("accession", ""),
            ))

        return out