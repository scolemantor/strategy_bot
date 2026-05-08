"""Insider buying scanner via SEC EDGAR Form 4 filings, with caching.

Defaults to 7-day lookback. Cache makes subsequent runs fast.

Phase 4e backtest support: backtest_mode(as_of_date) replays the same logic
on a historical date. SEC Form 4 filings are permanent records keyed by
accession number, so this is just the live scanner pointed at a past date.

ESPP filter (Phase 7): three layers exclude routine Employee Stock Purchase
Plan transactions which would otherwise look like cluster buys.
  - Layer 1 (per-txn floor): drop transactions below
    `min_per_txn_value_usd` (default $10k; configurable via
    config/insider_buying.yaml or env INSIDER_MIN_PER_TXN_VALUE).
  - Layer 2 (footnote ESPP detection): drop transactions whose Form 4
    footnote text matches ESPP_FOOTNOTE_PATTERN.
  - Layer 3 (cluster heuristic): drop clusters where all transactions
    occur on a single date AND >=50% are sub-$25k.
"""
from __future__ import annotations

import logging
import os
import re
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
import yaml
from lxml import etree

from .base import Scanner, ScanResult, empty_result
from .edgar_client import (
    EDGAR_BASE,
    cik_to_ticker,
    edgar_get,
    load_cik_to_ticker,
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

# Form 4 footnote keywords that indicate an ESPP / employee stock purchase
# plan transaction (not real insider conviction). Case-insensitive. Used by
# Layer 2 of the ESPP filter.
ESPP_FOOTNOTE_PATTERN = re.compile(
    r"(ESPP|Employee Stock Purchase Plan|stock purchase plan|"
    r"purchased pursuant to a stock purchase plan|ESPP transaction)",
    re.IGNORECASE,
)

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "insider_buying.yaml"


def _load_config() -> dict:
    """Load config/insider_buying.yaml. Missing file = empty dict (use class defaults)."""
    if not CONFIG_PATH.exists():
        return {}
    try:
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        log.warning(f"Failed to load {CONFIG_PATH}: {e}; using class defaults")
        return {}


def _build_rejection_rows(
    rejected_txns: List["Form4Transaction"],
    reason: str,
    ticker_map: Dict[str, str],
    min_cluster_insiders: int,
) -> List[dict]:
    """Aggregate rejected transactions by issuer; emit a rejection row per
    issuer that *would* have qualified as a cluster (>= min_cluster_insiders
    distinct insiders). Quiet on small dribbles to keep rejected.csv signal-rich."""
    by_issuer: Dict[str, List["Form4Transaction"]] = defaultdict(list)
    for t in rejected_txns:
        by_issuer[t.issuer_cik].append(t)

    rows = []
    for cik, ts in by_issuer.items():
        distinct = {t.insider_cik for t in ts}
        if len(distinct) < min_cluster_insiders:
            continue
        ticker = cik_to_ticker(cik, ticker_map)
        if ticker is None:
            continue
        rows.append({
            "ticker": ticker,
            "issuer_name": ts[0].issuer_name,
            "issuer_cik": cik,
            "rejection_reason": reason,
            "insider_count": len(distinct),
            "buy_count": len(ts),
            "total_value_usd": round(sum(t.value_usd for t in ts), 2),
        })
    return rows


@dataclass
class Form4Transaction:
    issuer_cik: str
    issuer_name: str
    insider_cik: str
    insider_name: str
    filing_date: Optional[date]
    transaction_date: Optional[date]
    transaction_code: str
    is_acquisition: bool
    is_purchase: bool
    shares: float
    price_per_share: float
    accession: str
    # Concatenated text from Form 4 <footnote> elements referenced by this
    # transaction. Default "" so old cache entries (pre-Phase-7) deserialize
    # cleanly without footnote data; ESPP Layer 2 simply won't fire on them.
    footnote_text: str = ""

    @property
    def value_usd(self) -> float:
        return self.shares * self.price_per_share

    def to_dict(self) -> dict:
        d = asdict(self)
        for k in ("filing_date", "transaction_date"):
            if isinstance(d[k], date):
                d[k] = d[k].isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Form4Transaction":
        d2 = dict(d)
        for k in ("filing_date", "transaction_date"):
            v = d2.get(k)
            if isinstance(v, str):
                try:
                    d2[k] = date.fromisoformat(v)
                except ValueError:
                    d2[k] = None
        return cls(**d2)


class InsiderBuyingScanner(Scanner):
    name = "insider_buying"
    description = "Cluster buys (2+ insiders) via SEC Form 4 - cached"
    cadence = "daily"

    DEFAULT_LOOKBACK_DAYS = 7
    MIN_CLUSTER_INSIDERS = 2
    MIN_TRANSACTION_VALUE_USD = 5_000             # legacy hard floor for any purchase
    MIN_PER_TXN_VALUE_USD = 10_000                # ESPP Layer 1 floor; YAML/env overridable
    ESPP_HEURISTIC_VALUE_THRESHOLD = 25_000       # ESPP Layer 3 sub-threshold
    ESPP_HEURISTIC_RATIO = 0.5                    # ESPP Layer 3 ratio

    def __init__(self, lookback_days: Optional[int] = None):
        super().__init__()
        env_override = os.getenv("INSIDER_LOOKBACK_DAYS")
        if lookback_days is not None:
            self.lookback_days = lookback_days
        elif env_override:
            try:
                self.lookback_days = int(env_override)
            except ValueError:
                self.lookback_days = self.DEFAULT_LOOKBACK_DAYS
        else:
            self.lookback_days = self.DEFAULT_LOOKBACK_DAYS

        # Layer 1 floor: env > yaml > class default
        config = _load_config()
        env_floor = os.getenv("INSIDER_MIN_PER_TXN_VALUE")
        if env_floor is not None:
            try:
                self.min_per_txn_value_usd = int(env_floor)
            except ValueError:
                self.min_per_txn_value_usd = int(
                    config.get("min_per_txn_value_usd", self.MIN_PER_TXN_VALUE_USD)
                )
        else:
            self.min_per_txn_value_usd = int(
                config.get("min_per_txn_value_usd", self.MIN_PER_TXN_VALUE_USD)
            )

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Lookback window: {self.lookback_days} days")
        log.info(f"Cache state at start: {cache_stats()}")

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

        purchases = [
            t for t in transactions
            if t.is_acquisition
            and t.is_purchase
            and t.shares > 0
            and t.price_per_share > 0
            and t.value_usd >= self.MIN_TRANSACTION_VALUE_USD
        ]
        log.info(f"Filtered to {len(purchases)} open-market purchases (>=${self.MIN_TRANSACTION_VALUE_USD:,})")

        rejected_rows: List[dict] = []

        # ESPP Layer 2: footnote pattern. Drop transactions whose Form 4
        # footnote text matches the ESPP regex.
        espp_footnote_rejected = [
            t for t in purchases if t.footnote_text and ESPP_FOOTNOTE_PATTERN.search(t.footnote_text)
        ]
        purchases = [
            t for t in purchases if not (t.footnote_text and ESPP_FOOTNOTE_PATTERN.search(t.footnote_text))
        ]
        if espp_footnote_rejected:
            log.info(f"  Layer 2 (ESPP footnote): {len(espp_footnote_rejected)} txns dropped")
        rejected_rows.extend(
            _build_rejection_rows(
                espp_footnote_rejected, "ESPP footnote", ticker_map, self.MIN_CLUSTER_INSIDERS,
            )
        )

        # ESPP Layer 1: per-txn floor (configurable via YAML / env).
        sub_floor_rejected = [t for t in purchases if t.value_usd < self.min_per_txn_value_usd]
        purchases = [t for t in purchases if t.value_usd >= self.min_per_txn_value_usd]
        if sub_floor_rejected:
            log.info(f"  Layer 1 (per-txn floor ${self.min_per_txn_value_usd:,}): {len(sub_floor_rejected)} txns dropped")
        rejected_rows.extend(
            _build_rejection_rows(
                sub_floor_rejected,
                f"sub-${self.min_per_txn_value_usd // 1000}k transactions",
                ticker_map,
                self.MIN_CLUSTER_INSIDERS,
            )
        )

        log.info(f"  After ESPP Layers 1+2: {len(purchases)} purchases remain")

        if not purchases:
            return self._finalize_empty(run_date, filings, rejected_rows)

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

            # ESPP Layer 3: cluster heuristic. All same date + >=50% sub-$25k = ESPP-shaped.
            distinct_dates = {t.transaction_date for t in txns if t.transaction_date is not None}
            n_under_threshold = sum(1 for t in txns if t.value_usd < self.ESPP_HEURISTIC_VALUE_THRESHOLD)
            ratio_under = n_under_threshold / len(txns) if txns else 0
            if len(distinct_dates) == 1 and ratio_under >= self.ESPP_HEURISTIC_RATIO:
                cluster_date = next(iter(distinct_dates))
                log.info(
                    f"  Layer 3: dropping {ticker} cluster ({len(txns)} txns, all on {cluster_date}, "
                    f"{n_under_threshold} sub-${self.ESPP_HEURISTIC_VALUE_THRESHOLD//1000}k) — likely ESPP"
                )
                rejected_rows.append({
                    "ticker": ticker,
                    "issuer_name": txns[0].issuer_name,
                    "issuer_cik": issuer_cik,
                    "rejection_reason": "ESPP heuristic (same-date + sub-$25k)",
                    "insider_count": len(distinct_insiders),
                    "buy_count": len(txns),
                    "total_value_usd": round(sum(t.value_usd for t in txns), 2),
                    "cluster_date": cluster_date.isoformat(),
                })
                continue

            issuer_name = txns[0].issuer_name
            total_value = sum(t.value_usd for t in txns)
            buy_count = len(txns)
            insider_count = len(distinct_insiders)
            valid_dates = [t.transaction_date or t.filing_date for t in txns if t.transaction_date or t.filing_date]
            earliest = min(valid_dates) if valid_dates else None
            latest = max(valid_dates) if valid_dates else None

            rows.append({
                "ticker": ticker,
                "issuer_name": issuer_name,
                "issuer_cik": issuer_cik,
                "insider_count": insider_count,
                "buy_count": buy_count,
                "total_value_usd": round(total_value, 2),
                "earliest_buy": earliest.isoformat() if earliest else "",
                "latest_buy": latest.isoformat() if latest else "",
                "score": insider_count * 100 + min(buy_count * 5, 50),
                "reason": (
                    f"{insider_count} insiders, {buy_count} buys, "
                    f"${total_value:,.0f} total"
                ),
            })

        rejected_df = pd.DataFrame(rejected_rows) if rejected_rows else None

        if not rows:
            return ScanResult(
                scanner_name=self.name,
                run_date=run_date,
                candidates=pd.DataFrame(columns=["ticker", "score", "reason"]),
                rejected_candidates=rejected_df,
                notes=[
                    f"Lookback: {self.lookback_days} days",
                    f"Filings parsed: {len(filings)}",
                    f"Purchases after ESPP filter: {len(purchases)}",
                    f"ESPP-rejected rows: {len(rejected_rows)}",
                ],
            )

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            rejected_candidates=rejected_df,
            notes=[
                f"Lookback: {self.lookback_days} days",
                f"Filings parsed: {len(filings)}",
                f"Purchases after ESPP filter: {len(purchases)}",
                f"Distinct issuers w/ buys: {len(by_issuer)}",
                f"Clusters (>= {self.MIN_CLUSTER_INSIDERS} insiders): {len(rows)}",
                f"ESPP-rejected rows: {len(rejected_rows)}",
            ],
        )

    def _finalize_empty(self, run_date: date, filings: list, rejected_rows: list) -> ScanResult:
        rejected_df = pd.DataFrame(rejected_rows) if rejected_rows else None
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=pd.DataFrame(columns=["ticker", "score", "reason"]),
            rejected_candidates=rejected_df,
            notes=[
                f"Lookback: {self.lookback_days} days",
                f"Filings parsed: {len(filings)}",
                f"ESPP-rejected rows: {len(rejected_rows)}",
            ],
        )

    def _collect_form4_filings(self, run_date: date) -> List[Dict]:
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
                form4 = [f for f in cached if f["form_type"].startswith("4")]
                all_filings.extend(form4)
                log.info(f"  {cur}: {len(cached)} total filings, {len(form4)} Form 4s [cached]")
                cur += timedelta(days=1)
                continue

            try:
                day_filings = self._fetch_daily_index(cur)
                save_cached_index(cur, day_filings)
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

    def _parse_filing_cached(
        self, filing: Dict, ticker_map: Dict[str, str],
    ) -> List[Form4Transaction]:
        accession = filing.get("accession", "")

        cached = load_cached_filing(accession)
        if cached is not None:
            return [Form4Transaction.from_dict(d) for d in cached]

        if not accession:
            return []

        try:
            resp = edgar_get(filing["filing_index_url"])
        except Exception as e:
            log.debug(f"Failed to fetch filing index {filing['filing_index_url']}: {e}")
            save_cached_filing(accession, [])
            return []

        # Find ALL .xml links on the filing index page, then pick the actual Form 4.
        # The previous version grabbed the first .xml match, which on EDGAR is
        # almost always the index metadata XML — it has no nonDerivativeTransaction
        # elements, so every filing parsed to 0 transactions.
        # Strategy: prefer files whose name contains "form4" (the standard SEC
        # naming convention), then fall back to any .xml that isn't the
        # accession-index metadata file.
        xml_candidates = re.findall(
            r'href="([^"]+\.xml)"',
            resp.text,
            flags=re.IGNORECASE,
        )

        if not xml_candidates:
            save_cached_filing(accession, [])
            return []

        # Strip XSL transformation prefix (xslF345X06/) from every candidate so
        # we always fetch raw XML, never the HTML-rendered view. Then dedupe.
        # Without this, we'd fetch the .xml URL but get an HTML page back,
        # which fails to parse as XML and returns 0 transactions.
        raw_candidates = []
        seen = set()
        for c in xml_candidates:
            stripped = re.sub(r'/xslF\d+X\d+/', '/', c, flags=re.IGNORECASE)
            if stripped not in seen:
                seen.add(stripped)
                raw_candidates.append(stripped)

        non_index = [c for c in raw_candidates if "-index.xml" not in c.lower()]
        if not non_index:
            non_index = raw_candidates

        form4_named = [c for c in non_index if "form4" in c.lower()]
        xml_path = form4_named[0] if form4_named else non_index[0]

        if xml_path.startswith("http"):
            xml_url = xml_path
        elif xml_path.startswith("/"):
            xml_url = EDGAR_BASE + xml_path
        else:
            base = filing["filing_index_url"].rsplit("/", 1)[0]
            xml_url = base + "/" + xml_path

        try:
            xml_resp = edgar_get(xml_url)
        except Exception as e:
            log.debug(f"Failed to fetch Form 4 XML {xml_url}: {e}")
            save_cached_filing(accession, [])
            return []

        txns = self._parse_form4_xml(xml_resp.content, filing)
        save_cached_filing(accession, [t.to_dict() for t in txns])
        return txns

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

        # Build footnote_id -> text map. <footnotes><footnote id="F1">...</footnote></footnotes>
        # Per-transaction <footnoteId id="F1"/> refs resolve to this text via Layer 2.
        footnote_map: Dict[str, str] = {}
        for fn in root.findall("footnotes/footnote"):
            fid = fn.get("id", "")
            if fid:
                footnote_map[fid] = (fn.text or "").strip()

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

            # Resolve footnoteId refs anywhere in the transaction subtree.
            fn_refs: List[str] = []
            for el in txn.iter():
                local = etree.QName(el).localname
                if local == "footnoteId":
                    fid = el.get("id", "")
                    if fid:
                        fn_refs.append(fid)
            footnote_text = " | ".join(
                footnote_map.get(f, "") for f in fn_refs if f in footnote_map
            ).strip()

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
                footnote_text=footnote_text,
            ))

        return out


# --- Phase 4e backtest support ---

def backtest_mode(as_of_date: date, output_dir=None) -> int:
    """Run insider_buying scanner as-of a historical date.

    SEC Form 4 filings are permanent records keyed by accession number, and
    the scanner only looks back N days from as_of_date. So this is just the
    live scanner pointed at a historical date — no look-ahead concerns.

    Output goes to <output_dir>/<as_of_date>/insider_buying.csv where
    output_dir defaults to backtest_output/.
    """
    from pathlib import Path

    output_dir = Path(output_dir) if output_dir else Path("backtest_output")
    scanner = InsiderBuyingScanner()

    try:
        result = scanner.run(as_of_date)
    except Exception as e:
        log.warning(f"insider_buying backtest_mode failed for {as_of_date}: {e}")
        return 0

    if result.error or result.candidates.empty:
        return 0

    date_dir = output_dir / as_of_date.isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    out_path = date_dir / "insider_buying.csv"
    result.candidates.to_csv(out_path, index=False)
    log.debug(f"  insider_buying {as_of_date}: wrote {len(result.candidates)} candidates to {out_path}")

    return len(result.candidates)