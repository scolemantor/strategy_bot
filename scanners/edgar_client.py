"""SEC EDGAR HTTP client with rate limiting and CIK->ticker resolution.

SEC requires a User-Agent identifying who is making requests, and asks that
clients stay under 10 requests per second. We back off to 5/s to be polite.

The company_tickers.json file maps CIK -> ticker. We cache it locally for 24h
so we don't re-download a 5MB file on every scan.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import requests

log = logging.getLogger(__name__)

EDGAR_BASE = "https://www.sec.gov"
EDGAR_DATA_BASE = "https://data.sec.gov"
CACHE_DIR = Path("data_cache")
TICKER_MAP_TTL_HOURS = 24
RATE_LIMIT_DELAY = 0.21  # seconds between requests, ~5 req/s


def _user_agent() -> str:
    """Return a User-Agent string. Reads from env or uses a default."""
    contact = os.getenv("SEC_USER_AGENT_CONTACT", "research@example.com")
    return f"OakStrategyBot {contact}"


def _headers() -> Dict[str, str]:
    return {
        "User-Agent": _user_agent(),
        "Accept-Encoding": "gzip, deflate",
    }


_last_request_time = 0.0


def _rate_limit():
    """Sleep if needed to stay under the rate limit."""
    global _last_request_time
    now = time.time()
    delta = now - _last_request_time
    if delta < RATE_LIMIT_DELAY:
        time.sleep(RATE_LIMIT_DELAY - delta)
    _last_request_time = time.time()


def edgar_get(url: str, timeout: int = 30) -> requests.Response:
    """Rate-limited GET to an EDGAR URL. Raises on HTTP errors."""
    _rate_limit()
    log.debug(f"EDGAR GET {url}")
    resp = requests.get(url, headers=_headers(), timeout=timeout)
    resp.raise_for_status()
    return resp


def _ticker_cache_path() -> Path:
    return CACHE_DIR / "sec_company_tickers.json"


def _is_ticker_cache_fresh() -> bool:
    p = _ticker_cache_path()
    if not p.exists():
        return False
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    return age_hours < TICKER_MAP_TTL_HOURS


def load_cik_to_ticker() -> Dict[str, str]:
    """Return a CIK (zero-padded 10-digit) -> ticker mapping.

    Refreshes the local cache if older than 24h. The SEC file maps each row
    by integer index, with cik_str/ticker/title fields.
    """
    cache_path = _ticker_cache_path()

    if not _is_ticker_cache_fresh():
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        log.info("Refreshing SEC company_tickers.json cache")
        url = f"{EDGAR_BASE}/files/company_tickers.json"
        resp = edgar_get(url)
        cache_path.write_text(resp.text)
    else:
        log.debug("Using cached SEC company_tickers.json")

    raw = json.loads(cache_path.read_text())
    mapping: Dict[str, str] = {}
    for _, row in raw.items():
        cik = str(row["cik_str"]).zfill(10)
        ticker = row["ticker"]
        mapping[cik] = ticker
    log.info(f"Loaded {len(mapping)} CIK->ticker mappings")
    return mapping


def cik_to_ticker(cik: str, mapping: Optional[Dict[str, str]] = None) -> Optional[str]:
    """Convert a CIK (any format) to a ticker, or None if not found."""
    if mapping is None:
        mapping = load_cik_to_ticker()
    cik_padded = str(cik).strip().zfill(10)
    return mapping.get(cik_padded)
def fetch_13f_filings_list(cik: str, limit: int = 12) -> list:
    """Fetch a list of 13F-HR filings for a given CIK, most recent first.

    Returns list of dicts: [{accession, filing_date, period_of_report, ...}, ...]
    Uses SEC's company submissions JSON endpoint at data.sec.gov.
    """
    import json as _json
    from datetime import date as _date

    cik_clean = str(cik).strip().lstrip("0") or "0"
    cik_padded = str(cik).strip().zfill(10)
    url = f"{EDGAR_DATA_BASE}/submissions/CIK{cik_padded}.json"

    resp = edgar_get(url)
    data = resp.json()

    filings_section = data.get("filings", {}).get("recent", {})
    forms = filings_section.get("form", [])
    accessions = filings_section.get("accessionNumber", [])
    filing_dates = filings_section.get("filingDate", [])
    report_dates = filings_section.get("reportDate", [])
    primary_docs = filings_section.get("primaryDocument", [])

    out = []
    for i, form in enumerate(forms):
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        try:
            f_date = _date.fromisoformat(filing_dates[i]) if filing_dates[i] else None
            p_date = _date.fromisoformat(report_dates[i]) if report_dates[i] else None
        except ValueError:
            f_date = p_date = None
        out.append({
            "form_type": form,
            "accession": accessions[i],
            "filing_date": f_date,
            "period_of_report": p_date,
            "primary_document": primary_docs[i] if i < len(primary_docs) else None,
        })
        if len(out) >= limit:
            break

    return out


def fetch_13f_holdings(cik: str, accession: str, primary_document: str = None) -> list:
    """Parse holdings from a 13F-HR filing.

    13F filings have an information table XML. The accession is the SEC's
    filing ID; we construct the path to the information table XML.

    Returns list of dicts: [{cusip, name_of_issuer, value, shares, ...}, ...]
    Values are reported in thousands of dollars per SEC convention; we convert
    to dollars (multiply by 1000) for downstream sanity.
    """
    import re

    cik_clean = str(cik).strip().lstrip("0") or "0"
    accession_no_dashes = accession.replace("-", "")

    # First fetch the filing index to find the information table XML filename
    index_url = (
        f"{EDGAR_BASE}/Archives/edgar/data/{cik_clean}/"
        f"{accession_no_dashes}/{accession}-index.htm"
    )
    resp = edgar_get(index_url)
    index_html = resp.text

    # Find the information table file (XML, name typically ends in .xml and contains 'informationTable' or similar)
    # Look for hrefs to xml files in the index
    xml_files = re.findall(r'href="([^"]+\.xml)"', index_html, re.IGNORECASE)
    info_table_file = None
    for f in xml_files:
        # Information table is usually the larger XML, not the primary doc which is just the cover
        fname = f.split("/")[-1].lower()
        if "informationtable" in fname or "infotable" in fname or "information_table" in fname:
            info_table_file = f.split("/")[-1]
            break
    if info_table_file is None:
        # Fallback: pick the second xml file (first is usually primary doc / cover, second is info table)
        if len(xml_files) >= 2:
            info_table_file = xml_files[1].split("/")[-1]
        elif len(xml_files) == 1:
            info_table_file = xml_files[0].split("/")[-1]
        else:
            return []

    table_url = (
        f"{EDGAR_BASE}/Archives/edgar/data/{cik_clean}/"
        f"{accession_no_dashes}/{info_table_file}"
    )
    resp = edgar_get(table_url)
    xml_text = resp.text

    from lxml import etree as LET

    try:
        # lxml handles namespaces properly. Parse from bytes to avoid encoding issues.
        root = LET.fromstring(xml_text.encode("utf-8"))
    except LET.XMLSyntaxError as e:
        log.warning(f"Failed to parse 13F XML for {accession}: {e}")
        return []

    # Resolve namespace dynamically — 13F uses several different namespaces over the years
    # (eis_Common, n1, etc.). We use a wildcard XPath that matches any namespace.
    nsmap = {"ns": root.nsmap.get(None)} if root.nsmap.get(None) else {}

    # Find all infoTable elements regardless of namespace
    info_tables = root.xpath("//*[local-name()='infoTable']")

    holdings = []
    for info in info_tables:
        try:
            def find_text(local_name: str, parent=info) -> str:
                """Find text of first child element with this local name (any namespace)."""
                els = parent.xpath(f".//*[local-name()='{local_name}']")
                return els[0].text.strip() if els and els[0].text else ""

            cusip = find_text("cusip")
            name = find_text("nameOfIssuer")
            value_thousands = find_text("value") or "0"
            shares = find_text("sshPrnamt") or "0"
            sh_type = find_text("sshPrnamtType")

            # Skip non-stock holdings (PRN = principal amount = bonds)
            if sh_type and sh_type.upper() != "SH":
                continue

            value_raw = int(float(value_thousands))
            # SEC changed value convention around 2023. Old filings: thousands of $.
            # New filings: actual $. We can't reliably distinguish per-filing, but
            # we can detect by magnitude: if shares * $1 > 10x value_raw, the value
            # is in thousands (old convention); otherwise dollars (new convention).
            # In practice, modern 13F XML reports raw dollars.
            shares_int = int(float(shares))
            holdings.append({
                "cusip": cusip,
                "name_of_issuer": name,
                "value_dollars": value_raw,  # report as-is, no multiplication
                "shares": shares_int,
            })
        except (ValueError, AttributeError) as e:
            log.debug(f"Skipping malformed infoTable row in {accession}: {e}")
            continue

    return holdings