"""Investability filter — universal quality gate for all scanner outputs.

Every scanner's candidate list passes through this filter before being written
to scan_output. The filter applies a configurable set of gates to each ticker:

  Gate 1: Market cap floor — must be tradeable size
  Gate 2: Avg daily dollar volume — must be tradeable liquidity
  Gate 3: Recent dilution — flag if >10pct new shares in 90 days
  Gate 4: Going concern — flag if latest 10-K mentions substantial doubt
  Gate 5: Listing exchange — drop OTC by default
  Gate 6: Hard exclusions — manually maintained never-trade list
  Gate 7: Audit trail — write rejected.csv per scanner with reasons

Per-scanner configuration in config/investability.yaml controls:
  - Which gates apply to that scanner
  - What threshold each gate uses
  - Override defaults for scanners that operate on different universes
    (e.g. small_cap_value disables mcap floor since it filters mcap internally)

Tier defaults:
  - strict: 300M mcap, 5M ADV, exchange listed only
  - loose (default): 50M mcap, 1M ADV, exchange listed only
  - permissive: 10M mcap, 250K ADV, allow OTC
  - off: no filtering (calendar/macro scanners that operate on events not equities)

Loose is default because strict would zero out half the scanners
(fda_calendar surfaces $50-300M biotechs by design; ipo_lockup surfaces
recent IPOs that may have low ADV; etc).
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from src.http_utils import with_deadline, yfinance_session

log = logging.getLogger(__name__)

try:
    _YF_SESSION = yfinance_session(30)
except Exception:
    _YF_SESSION = None

CACHE_DIR = Path("data_cache")
CONFIG_DIR = Path("config")
INVESTABILITY_CONFIG_PATH = CONFIG_DIR / "investability.yaml"
EXCLUSIONS_CONFIG_PATH = CONFIG_DIR / "exclusions.yaml"


# Tier presets — referenced from per-scanner config in YAML
TIER_PRESETS = {
    "strict": {
        "min_market_cap": 300_000_000,
        "min_avg_dollar_volume": 5_000_000,
        "allow_otc": False,
        "check_dilution": True,
        "check_going_concern": True,
        "check_exclusions": True,
        "max_dilution_pct": 0.10,
    },
    "loose": {
        "min_market_cap": 50_000_000,
        "min_avg_dollar_volume": 1_000_000,
        "allow_otc": False,
        "check_dilution": True,
        "check_going_concern": True,
        "check_exclusions": True,
        "max_dilution_pct": 0.10,
    },
    "permissive": {
        "min_market_cap": 10_000_000,
        "min_avg_dollar_volume": 250_000,
        "allow_otc": True,
        "check_dilution": False,
        "check_going_concern": False,
        "check_exclusions": True,
        "max_dilution_pct": 0.20,
    },
    "off": {
        # Bypass entirely — for calendar/macro scanners
        "min_market_cap": 0,
        "min_avg_dollar_volume": 0,
        "allow_otc": True,
        "check_dilution": False,
        "check_going_concern": False,
        "check_exclusions": False,
        "max_dilution_pct": 1.0,
    },
}


@dataclass
class FilterConfig:
    """Per-scanner filter configuration. Built from tier preset + per-scanner overrides."""
    tier: str = "loose"
    min_market_cap: float = 50_000_000
    min_avg_dollar_volume: float = 1_000_000
    allow_otc: bool = False
    check_dilution: bool = True
    check_going_concern: bool = True
    check_exclusions: bool = True
    max_dilution_pct: float = 0.10
    # Scanners that don't have ticker-level results bypass entirely
    bypass: bool = False


@dataclass
class FilterResult:
    """Outcome for a single ticker through the filter."""
    ticker: str
    passed: bool
    reasons: List[str] = field(default_factory=list)  # list of failure reasons (empty if passed)


def _load_config_yaml(path: Path) -> Dict:
    """Load a YAML config file. Returns empty dict if file doesn't exist."""
    if not path.exists():
        return {}
    try:
        import yaml
        return yaml.safe_load(path.read_text()) or {}
    except ImportError:
        log.warning("PyYAML not installed; config files cannot be loaded. pip install pyyaml")
        return {}
    except Exception as e:
        log.warning(f"Failed to load {path}: {e}")
        return {}


def _load_exclusions() -> Set[str]:
    """Load the hard-exclusions ticker list. Returns set of uppercase tickers."""
    cfg = _load_config_yaml(EXCLUSIONS_CONFIG_PATH)
    if not isinstance(cfg, dict):
        return set()
    excl = cfg.get("exclusions") or []  # handle None case from empty YAML key
    return set(t.upper().strip() for t in excl if t)


def get_filter_config(scanner_name: str) -> FilterConfig:
    """Build the FilterConfig for a given scanner from YAML + tier presets."""
    cfg = _load_config_yaml(INVESTABILITY_CONFIG_PATH)
    if not cfg:
        # No config file; return loose default
        log.debug(f"No investability config found; using loose defaults for {scanner_name}")
        preset = TIER_PRESETS["loose"]
        return FilterConfig(tier="loose", **preset)

    scanners_cfg = cfg.get("scanners", {}) if isinstance(cfg, dict) else {}
    scanner_cfg = scanners_cfg.get(scanner_name, {})

    # Look up tier (default to loose)
    tier = scanner_cfg.get("tier", "loose") if isinstance(scanner_cfg, dict) else "loose"
    if tier not in TIER_PRESETS:
        log.warning(f"Unknown tier '{tier}' for {scanner_name}; falling back to loose")
        tier = "loose"

    base = dict(TIER_PRESETS[tier])

    # Apply per-scanner overrides
    if isinstance(scanner_cfg, dict):
        for key in ("min_market_cap", "min_avg_dollar_volume", "allow_otc",
                    "check_dilution", "check_going_concern", "check_exclusions",
                    "max_dilution_pct", "bypass"):
            if key in scanner_cfg:
                base[key] = scanner_cfg[key]

    return FilterConfig(tier=tier, **{k: v for k, v in base.items() if k in FilterConfig.__dataclass_fields__})


# Importable from scanners that want to declare their own preferred tier
def declare_tier_for_scanner(scanner_name: str, default_tier: str = "loose") -> str:
    """Helper for scanners to declare what their default tier should be.
    Read from config if set, otherwise return default."""
    cfg = _load_config_yaml(INVESTABILITY_CONFIG_PATH)
    if not cfg:
        return default_tier
    scanners_cfg = cfg.get("scanners", {}) if isinstance(cfg, dict) else {}
    return scanners_cfg.get(scanner_name, {}).get("tier", default_tier)


# --- The filter machinery ---

def filter_candidates(
    candidates_df: pd.DataFrame,
    scanner_name: str,
    enrichment_data: Optional[Dict[str, Dict]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the investability filter to a candidates DataFrame.

    Args:
        candidates_df: scanner's candidate output (must have 'ticker' column)
        scanner_name: name of the scanner producing these candidates
        enrichment_data: optional pre-fetched dict of {ticker: {market_cap, adv, ...}}
                         If not provided, will fetch lazily.

    Returns:
        (approved_df, rejected_df) — same schema as input, with rejected_df having
        an additional 'rejection_reason' column.
    """
    config = get_filter_config(scanner_name)
    log.info(f"Investability filter for {scanner_name}: tier={config.tier}")

    if config.bypass:
        log.info(f"  Filter bypassed for {scanner_name}")
        empty_rejected = candidates_df.iloc[0:0].copy()
        empty_rejected["rejection_reason"] = pd.Series(dtype=str)
        return candidates_df.copy(), empty_rejected

    if "ticker" not in candidates_df.columns:
        log.warning(f"  Candidates have no 'ticker' column; bypassing")
        empty_rejected = candidates_df.iloc[0:0].copy()
        empty_rejected["rejection_reason"] = pd.Series(dtype=str)
        return candidates_df.copy(), empty_rejected

    if candidates_df.empty:
        empty_rejected = candidates_df.iloc[0:0].copy()
        empty_rejected["rejection_reason"] = pd.Series(dtype=str)
        return candidates_df.copy(), empty_rejected

    # Get unique tickers; we may evaluate them once and apply to all rows
    unique_tickers = candidates_df["ticker"].dropna().unique().tolist()

    # Evaluate each ticker
    exclusions = _load_exclusions() if config.check_exclusions else set()

    if enrichment_data is None:
        enrichment_data = enrich_tickers(unique_tickers)

    results: Dict[str, FilterResult] = {}
    for ticker in unique_tickers:
        result = _evaluate_ticker(ticker, config, enrichment_data.get(ticker, {}), exclusions)
        results[ticker] = result

    # Apply results to the original DataFrame
    approved_mask = candidates_df["ticker"].map(lambda t: results.get(t, FilterResult(t, True)).passed)
    approved = candidates_df[approved_mask].copy()
    rejected = candidates_df[~approved_mask].copy()
    rejected["rejection_reason"] = rejected["ticker"].map(
        lambda t: "; ".join(results.get(t, FilterResult(t, False)).reasons)
    )

    log.info(f"  Filter cascade: {len(candidates_df)} -> {len(approved)} approved ({len(rejected)} rejected)")
    return approved, rejected


def _evaluate_ticker(
    ticker: str,
    config: FilterConfig,
    enrichment: Dict,
    exclusions: Set[str],
) -> FilterResult:
    """Evaluate a single ticker through all enabled gates. Returns FilterResult."""
    reasons: List[str] = []

    # Gate 6: Hard exclusions (cheapest check first)
    if config.check_exclusions and ticker.upper() in exclusions:
        reasons.append("on hard-exclusion list")
        return FilterResult(ticker, passed=False, reasons=reasons)

    # Gate 5: Exchange filter
    if not config.allow_otc:
        exchange = (enrichment.get("exchange") or "").upper()
        # OTC-style exchange codes: PNK (Pink Sheets), OTC, OTCBB, OTCM
        otc_codes = ("PNK", "OTC", "OTCBB", "OTCM", "OTCMKTS", "OTCQX", "OTCQB")
        if exchange and any(otc in exchange for otc in otc_codes):
            reasons.append(f"OTC exchange ({exchange})")

    # Gate 1: Market cap floor
    mcap = enrichment.get("market_cap")
    if config.min_market_cap > 0:
        if mcap is None:
            # Conservative: if we can't determine mcap and floor is set, reject
            reasons.append("market cap unavailable")
        elif mcap < config.min_market_cap:
            reasons.append(f"mcap ${mcap/1e6:.1f}M < ${config.min_market_cap/1e6:.0f}M floor")

    # Gate 2: Average daily dollar volume
    adv = enrichment.get("avg_dollar_volume")
    if config.min_avg_dollar_volume > 0:
        if adv is None:
            # Don't reject solely on missing ADV (some recent IPOs may not have it cached)
            log.debug(f"  {ticker}: ADV unavailable, skipping volume check")
        elif adv < config.min_avg_dollar_volume:
            reasons.append(f"ADV ${adv/1e6:.2f}M < ${config.min_avg_dollar_volume/1e6:.1f}M floor")

    # Gate 3: Recent dilution
    if config.check_dilution:
        dilution_pct = enrichment.get("dilution_pct_90d")
        if dilution_pct is not None and dilution_pct > config.max_dilution_pct:
            reasons.append(f"recent dilution {dilution_pct*100:.1f}% > {config.max_dilution_pct*100:.0f}% threshold")

    # Gate 4: Going concern
    if config.check_going_concern:
        going_concern = enrichment.get("going_concern_flag", False)
        if going_concern:
            reasons.append("10-K flagged 'going concern'")

    return FilterResult(ticker, passed=(not reasons), reasons=reasons)


# --- Enrichment: fetch the data needed for the gates ---

def enrich_tickers(tickers: List[str]) -> Dict[str, Dict]:
    """Fetch all data needed by the filter for a list of tickers.

    Returns dict of {ticker: {market_cap, avg_dollar_volume, exchange,
                              dilution_pct_90d, going_concern_flag}}

    Pulls from existing yfinance fundamentals cache (#8 small_cap_value populated this),
    Alpaca bars cache (every scanner populates this), and SEC filings (lazy fetch).
    """
    out: Dict[str, Dict] = {}

    # Step 1: yfinance market cap + exchange (cached by #8 small_cap_value already)
    yfinance_data = _load_yfinance_fundamentals_batch(tickers)
    for ticker in tickers:
        d = yfinance_data.get(ticker, {})
        out[ticker] = {
            "market_cap": d.get("market_cap"),
            "exchange": d.get("exchange"),
            "last_close": d.get("last_close"),
        }

    # Step 2: avg dollar volume from Alpaca bars (compute from cached bars)
    adv_data = _compute_avg_dollar_volume(tickers)
    for ticker, adv in adv_data.items():
        if ticker in out:
            out[ticker]["avg_dollar_volume"] = adv

    # Step 3 + 4 (dilution + going concern) — populated lazily by sec_fundamentals module
    # if check_dilution / check_going_concern enabled. Imported only when needed.
    try:
        from .sec_fundamentals import get_dilution_data, get_going_concern_data
        dilution = get_dilution_data(tickers)
        going_concern = get_going_concern_data(tickers)
        for ticker in tickers:
            if ticker in out:
                out[ticker]["dilution_pct_90d"] = dilution.get(ticker)
                out[ticker]["going_concern_flag"] = going_concern.get(ticker, False)
    except ImportError:
        log.debug("  sec_fundamentals not yet available; dilution/going-concern checks will be skipped")

    return out


def _load_yfinance_fundamentals_batch(tickers: List[str]) -> Dict[str, Dict]:
    """Load fundamentals from existing yfinance_fundamentals cache.

    For tickers without a cache hit, lazy-fetch via yfinance and populate
    the cache. This makes the filter universe-agnostic: any scanner that
    surfaces a ticker will get its fundamentals checked.

    Cache is shared with #8 small_cap_value (24h TTL) so first-pass for new
    tickers is slow; subsequent runs are instant.

    Returns dict of {ticker: {market_cap, exchange, last_close}}.
    """
    cache_dir = CACHE_DIR / "yfinance_fundamentals"
    cache_dir.mkdir(parents=True, exist_ok=True)
    out: Dict[str, Dict] = {}

    cache_ttl_hours = 24
    misses: List[str] = []

    for ticker in tickers:
        p = cache_dir / f"{ticker}.json"
        if p.exists():
            age_hours = (time.time() - p.stat().st_mtime) / 3600
            if age_hours <= cache_ttl_hours:
                try:
                    data = json.loads(p.read_text())
                    out[ticker] = {
                        "market_cap": data.get("market_cap"),
                        "last_close": data.get("last_close"),
                        "exchange": data.get("exchange"),
                    }
                    continue
                except Exception:
                    pass
        misses.append(ticker)

    if not misses:
        return out

    # Lazy-fetch missing tickers via yfinance
    log.info(f"  yfinance fundamentals fetch: {len(out)} cached, {len(misses)} need fetch (~{len(misses) * 0.7 / 60:.1f} min)")
    try:
        import yfinance as yf
    except ImportError:
        log.warning("  yfinance not installed; cannot lazy-fetch fundamentals")
        for ticker in misses:
            out[ticker] = {}
        return out

    from datetime import datetime as _dt

    for i, ticker in enumerate(misses):
        try:
            time.sleep(0.2)  # rate limit
            t = yf.Ticker(ticker, session=_YF_SESSION)
            info = with_deadline(lambda: t.info, timeout=30, default=None)
            if info is None:
                log.debug(f"  yfinance fundamentals deadline for {ticker}")
                out[ticker] = {}
                continue
            data = {
                "name": info.get("longName") or info.get("shortName"),
                "market_cap": info.get("marketCap"),
                "pe_trailing": info.get("trailingPE"),
                "pb": info.get("priceToBook"),
                "ev_ebitda": info.get("enterpriseToEbitda"),
                "debt_equity": (info.get("debtToEquity") / 100.0) if info.get("debtToEquity") else None,
                "fcf": info.get("freeCashflow"),
                "last_close": info.get("currentPrice") or info.get("regularMarketPrice"),
                "sector": info.get("sector"),
                "exchange": info.get("exchange"),
                "fetched_at": _dt.now().isoformat(),
            }
            # Save to shared cache (compatible with #8's format)
            try:
                (cache_dir / f"{ticker}.json").write_text(json.dumps(data))
            except Exception:
                pass
            out[ticker] = {
                "market_cap": data.get("market_cap"),
                "last_close": data.get("last_close"),
                "exchange": data.get("exchange"),
            }
        except Exception as e:
            log.debug(f"  yfinance fundamentals fetch failed for {ticker}: {e}")
            out[ticker] = {}

        if (i + 1) % 50 == 0:
            log.info(f"    Fetched {i + 1}/{len(misses)} fundamentals")

    return out


def _compute_avg_dollar_volume(tickers: List[str], lookback_days: int = 30) -> Dict[str, float]:
    """Compute 30-day average dollar volume from cached Alpaca bars."""
    out: Dict[str, float] = {}
    bars_cache_dir = CACHE_DIR  # bars are cached at the root of data_cache as <TICKER>.parquet

    for ticker in tickers:
        p = bars_cache_dir / f"{ticker}.parquet"
        if not p.exists():
            continue
        try:
            df = pd.read_parquet(p)
            if df.empty or "close" not in df.columns or "volume" not in df.columns:
                continue
            # Take last N rows = approximate N trading days
            recent = df.iloc[-lookback_days:]
            if recent.empty:
                continue
            dollar_vol = (recent["close"] * recent["volume"]).mean()
            out[ticker] = float(dollar_vol)
        except Exception as e:
            log.debug(f"  Failed to compute ADV for {ticker}: {e}")
            continue

    return out