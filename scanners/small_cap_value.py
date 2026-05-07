"""Small/mid-cap value scanner.

Surfaces stocks that pass a classic deep-value screen across multiple
fundamental metrics. The Fama-French value premium is one of the most
documented anomalies in academic finance — but it's been brutal vs growth
for ~15 years. Treat output as research candidates, not buy signals.

Source: yfinance fundamentals + Alpaca for current price (already cached
by other scanners).

Universe: S&P 1500 (S&P 500 + S&P 400 MidCap + S&P 600 SmallCap). Filtered
downstream by market cap to small/mid range.

Filters (all must pass):
  - Market cap $300M – $3B (the small/mid cap value zone)
  - P/E < 12 (cheap on earnings)
  - P/B < 1.5 (cheap on book value)
  - EV/EBITDA < 8 (cheap on enterprise value, accounts for debt)
  - Debt/Equity < 1.0 (not over-leveraged)
  - Free cash flow > 0 (actually generating cash, not just accounting profits)
  - Last close >= $5 (filter penny-stock artifacts)

Honest limits:
  - yfinance fundamentals are sometimes stale (last reported quarter, not real-time)
  - Trailing P/E vs forward P/E — we use trailing, which can mislead in turnaround situations
  - Value traps: cheap stocks are often cheap for a reason (declining revenue, structural decline)
  - First-time run is slow (~10-15 min for 1500 yfinance fetches), cached after
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .base import Scanner, ScanResult, empty_result
from .universe import get_sp1500_universe

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
FUNDAMENTALS_CACHE = CACHE_DIR / "yfinance_fundamentals"
FUNDAMENTALS_CACHE_TTL_HOURS = 24  # Refresh daily; fundamentals don't change intraday


def _fundamentals_cache_path(symbol: str) -> Path:
    return FUNDAMENTALS_CACHE / f"{symbol}.json"


def _load_cached_fundamentals(symbol: str) -> Optional[Dict]:
    p = _fundamentals_cache_path(symbol)
    if not p.exists():
        return None
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    if age_hours > FUNDAMENTALS_CACHE_TTL_HOURS:
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _save_cached_fundamentals(symbol: str, data: Dict) -> None:
    FUNDAMENTALS_CACHE.mkdir(parents=True, exist_ok=True)
    try:
        _fundamentals_cache_path(symbol).write_text(json.dumps(data))
    except Exception:
        pass


class SmallCapValueScanner(Scanner):
    name = "small_cap_value"
    description = "Deep-value screen: P/E<12, P/B<1.5, EV/EBITDA<8, D/E<1, FCF>0, mcap $300M-$3B"
    cadence = "weekly"  # Fundamentals only update quarterly; weekly is plenty

    MIN_MARKET_CAP = 300_000_000
    MAX_MARKET_CAP = 3_000_000_000
    MAX_PE = 12.0
    MAX_PB = 1.5
    MAX_EV_EBITDA = 8.0
    MAX_DEBT_EQUITY = 1.0
    MIN_FCF = 0  # Strictly positive
    MIN_PRICE = 5.0
    REQUEST_DELAY_SEC = 0.2

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Market cap range: ${self.MIN_MARKET_CAP/1e6:.0f}M - ${self.MAX_MARKET_CAP/1e9:.1f}B")
        log.info(f"Max P/E: {self.MAX_PE}, Max P/B: {self.MAX_PB}, Max EV/EBITDA: {self.MAX_EV_EBITDA}")
        log.info(f"Max D/E: {self.MAX_DEBT_EQUITY}, Min FCF: ${self.MIN_FCF:,}")

        try:
            import yfinance as yf
        except ImportError:
            return empty_result(self.name, run_date,
                error="yfinance not installed - run: python -m pip install yfinance")

        # Step 1: load universe
        try:
            universe = get_sp1500_universe()
        except Exception as e:
            log.exception("Failed to load S&P 1500 universe")
            return empty_result(self.name, run_date, error=f"universe: {e}")

        log.info(f"Universe: {len(universe)} S&P 1500 tickers")

        # Step 2: fetch fundamentals for each (cached 24h)
        cached_count = sum(1 for s in universe if _load_cached_fundamentals(s) is not None)
        log.info(f"  {cached_count} cached, {len(universe) - cached_count} need yfinance fetch")
        uncached = len(universe) - cached_count
        if uncached > 100:
            log.info(f"  Estimated time: ~{uncached * 0.7 / 60:.1f} min")

        all_fundamentals: List[Dict] = []
        for i, symbol in enumerate(universe):
            data = _load_cached_fundamentals(symbol)
            if data is None:
                data = self._fetch_fundamentals(yf, symbol)
                _save_cached_fundamentals(symbol, data)

            if data is None or not data.get("market_cap"):
                continue

            data["symbol"] = symbol
            all_fundamentals.append(data)

            if (i + 1) % 100 == 0:
                log.info(f"  Processed {i + 1}/{len(universe)}, {len(all_fundamentals)} with valid data")

        log.info(f"Fundamentals available for {len(all_fundamentals)}/{len(universe)} tickers")

        if not all_fundamentals:
            return empty_result(self.name, run_date)

        # Step 3: filter cascade
        df = pd.DataFrame(all_fundamentals)

        before = len(df)
        df = df[(df["market_cap"] >= self.MIN_MARKET_CAP) & (df["market_cap"] <= self.MAX_MARKET_CAP)]
        log.info(f"Market cap filter (${self.MIN_MARKET_CAP/1e6:.0f}M-${self.MAX_MARKET_CAP/1e9:.1f}B): {before} -> {len(df)}")

        before = len(df)
        df = df[(df["pe_trailing"].notna()) & (df["pe_trailing"] > 0) & (df["pe_trailing"] < self.MAX_PE)]
        log.info(f"P/E filter (>0, <{self.MAX_PE}): {before} -> {len(df)}")

        before = len(df)
        df = df[(df["pb"].notna()) & (df["pb"] > 0) & (df["pb"] < self.MAX_PB)]
        log.info(f"P/B filter (>0, <{self.MAX_PB}): {before} -> {len(df)}")

        before = len(df)
        df = df[(df["ev_ebitda"].notna()) & (df["ev_ebitda"] > 0) & (df["ev_ebitda"] < self.MAX_EV_EBITDA)]
        log.info(f"EV/EBITDA filter (>0, <{self.MAX_EV_EBITDA}): {before} -> {len(df)}")

        before = len(df)
        df = df[(df["debt_equity"].notna()) & (df["debt_equity"] >= 0) & (df["debt_equity"] < self.MAX_DEBT_EQUITY)]
        log.info(f"D/E filter (>=0, <{self.MAX_DEBT_EQUITY}): {before} -> {len(df)}")

        before = len(df)
        df = df[(df["fcf"].notna()) & (df["fcf"] > self.MIN_FCF)]
        log.info(f"FCF filter (>${self.MIN_FCF}): {before} -> {len(df)}")

        before = len(df)
        df = df[(df["last_close"].notna()) & (df["last_close"] >= self.MIN_PRICE)]
        log.info(f"Min-price filter (>=${self.MIN_PRICE}): {before} -> {len(df)}")

        if df.empty:
            return empty_result(self.name, run_date)

        # Step 4: composite value score
        # Each metric scores 0-25; lower = better, so we invert and normalize
        df = df.copy()
        df["pe_score"] = (1 - df["pe_trailing"] / self.MAX_PE).clip(0, 1) * 25
        df["pb_score"] = (1 - df["pb"] / self.MAX_PB).clip(0, 1) * 25
        df["ev_score"] = (1 - df["ev_ebitda"] / self.MAX_EV_EBITDA).clip(0, 1) * 25
        df["fcf_yield_score"] = ((df["fcf"] / df["market_cap"]) * 100).clip(0, 25)  # FCF yield % capped at 25
        df["score"] = df["pe_score"] + df["pb_score"] + df["ev_score"] + df["fcf_yield_score"]

        rows = []
        for _, r in df.iterrows():
            fcf_yield = (r["fcf"] / r["market_cap"]) * 100
            rows.append({
                "ticker": r["symbol"],
                "name": r.get("name", ""),
                "market_cap": int(r["market_cap"]),
                "pe_trailing": round(float(r["pe_trailing"]), 2),
                "pb": round(float(r["pb"]), 2),
                "ev_ebitda": round(float(r["ev_ebitda"]), 2),
                "debt_equity": round(float(r["debt_equity"]), 2),
                "fcf": int(r["fcf"]),
                "fcf_yield_pct": round(fcf_yield, 2),
                "last_close": round(float(r["last_close"]), 2),
                "sector": r.get("sector", ""),
                "score": round(float(r["score"]), 2),
                "reason": (
                    f"P/E {r['pe_trailing']:.1f}, P/B {r['pb']:.2f}, EV/EBITDA {r['ev_ebitda']:.1f}, "
                    f"D/E {r['debt_equity']:.2f}, FCF yield {fcf_yield:.1f}%, mcap ${r['market_cap']/1e9:.2f}B"
                ),
            })

        out_df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=out_df,
            notes=[
                f"Universe: S&P 1500",
                f"Market cap: ${self.MIN_MARKET_CAP/1e6:.0f}M-${self.MAX_MARKET_CAP/1e9:.1f}B",
                f"Max P/E: {self.MAX_PE}, P/B: {self.MAX_PB}, EV/EBITDA: {self.MAX_EV_EBITDA}",
                f"Max D/E: {self.MAX_DEBT_EQUITY}, Min FCF: positive",
                f"Final candidates: {len(rows)}",
                "Value trap risk: cheap stocks are often cheap for a reason. Research before buying.",
            ],
        )

    def _fetch_fundamentals(self, yf, symbol: str) -> Optional[Dict]:
        time.sleep(self.REQUEST_DELAY_SEC)
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            if not info:
                return None

            return {
                "name": info.get("longName") or info.get("shortName"),
                "market_cap": info.get("marketCap"),
                "pe_trailing": info.get("trailingPE"),
                "pb": info.get("priceToBook"),
                "ev_ebitda": info.get("enterpriseToEbitda"),
                "debt_equity": (info.get("debtToEquity") / 100.0) if info.get("debtToEquity") else None,
                # yfinance returns debtToEquity as a percentage (e.g. 75.5 = 75.5%); we want 0.755
                "fcf": info.get("freeCashflow"),
                "last_close": info.get("currentPrice") or info.get("regularMarketPrice"),
                "sector": info.get("sector"),
                "fetched_at": datetime.now().isoformat(),
            }
        except Exception as e:
            log.debug(f"  yfinance fundamentals fetch failed for {symbol}: {e}")
            return None
# --- Phase 4e backtest support ---

def backtest_mode(as_of_date: date, output_dir=None) -> int:
    """STUB: small_cap_value cannot be cleanly backtested.

    yfinance only returns CURRENT fundamentals (P/E, P/B, EV/EBITDA, FCF,
    market cap). There is no way to get those values as-of a historical
    date without a paid fundamentals database (Compustat, etc).

    Using today's fundamentals would produce look-ahead bias — we'd surface
    tickers that are cheap NOW, not tickers that were cheap on as_of_date.

    For Phase 4e v1 we skip this scanner during backtest. Its weight in
    scanner_weights.yaml remains judgment-based until we have historical
    fundamentals access.
    """
    log.info(
        f"small_cap_value backtest_mode: SKIPPED for {as_of_date} "
        f"(no historical fundamentals available — see scanner docstring)"
    )
    return 0