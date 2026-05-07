"""Short squeeze scanner.

Surfaces equities setting up for a potential short squeeze: high short interest
relative to float, high days-to-cover (= low liquidity for shorts to exit),
AND positive 20-day price momentum (= the squeeze trigger may be live).

This is a momentum/speculation signal, NOT a value signal. Most squeeze setups
fizzle. Once-in-a-generation events (GME 2021) make the math look attractive
but selection bias is severe.

ACORNS SLEEVE ONLY. Do not size into branches/trunk based on this scanner.

Source:
  - FINRA bi-monthly short interest CSV (cdn.finra.org)
  - yfinance for share float (needed to compute SI as % of float)
  - Alpaca bars for 20-day momentum (already cached by src/data.py)

Filters:
  1. Days to cover > 5
  2. Short interest >= 20% of float
  3. 20-day return > +10%
  4. Last close >= $5 (filter sub-penny noise)
  5. Avg daily volume >= 100k shares (need liquidity to actually trade)

Honest limits:
  - FINRA data is bi-monthly — current short interest is up to 2 weeks stale.
  - "Float" from yfinance is occasionally wrong. Edge cases (recent IPOs,
    splits, secondary offerings) can produce nonsense SI percentages.
  - Doesn't predict timing. A squeeze setup can sit for months before triggering.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .base import Scanner, ScanResult, empty_result
from .finra_client import fetch_short_interest, find_latest_published

log = logging.getLogger(__name__)

CACHE_DIR = Path("data_cache")
FLOAT_CACHE_DIR = CACHE_DIR / "yfinance_float"
FLOAT_CACHE_TTL_HOURS = 24


def _float_cache_path(symbol: str) -> Path:
    return FLOAT_CACHE_DIR / f"{symbol}.json"


def _load_cached_float(symbol: str) -> Optional[float]:
    p = _float_cache_path(symbol)
    if not p.exists():
        return None
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    if age_hours > FLOAT_CACHE_TTL_HOURS:
        return None
    try:
        data = json.loads(p.read_text())
        return data.get("float_shares")
    except Exception:
        return None


def _save_cached_float(symbol: str, float_shares: Optional[float]) -> None:
    FLOAT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        _float_cache_path(symbol).write_text(json.dumps({
            "symbol": symbol,
            "float_shares": float_shares,
        }))
    except Exception:
        pass


class ShortSqueezeScanner(Scanner):
    name = "short_squeeze"
    description = "High SI + high days-to-cover + positive momentum (acorns sleeve only)"
    cadence = "weekly"  # FINRA publishes bi-monthly; weekly check catches new releases

    MIN_DAYS_TO_COVER = 5.0
    MIN_SHORT_PCT_FLOAT = 0.20  # 20%+ short interest
    MIN_TWENTY_DAY_RETURN = 0.10  # 10%+ in last 20 days
    MIN_PRICE = 5.0
    MIN_AVG_VOLUME = 100_000
    REQUEST_DELAY_SEC = 0.2  # courtesy delay for yfinance fetches

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Min days-to-cover: {self.MIN_DAYS_TO_COVER}")
        log.info(f"Min SI as % of float: {self.MIN_SHORT_PCT_FLOAT:.0%}")
        log.info(f"Min 20-day return: {self.MIN_TWENTY_DAY_RETURN:.0%}")

        # Step 1: find the latest available FINRA short interest file
        try:
            latest_settlement = find_latest_published(run_date)
        except Exception as e:
            log.exception("Failed to find latest FINRA file")
            return empty_result(self.name, run_date, error=f"finra discovery: {e}")

        if latest_settlement is None:
            return empty_result(self.name, run_date, error="No recent FINRA short interest file available")

        # Step 2: load the data
        si_df = fetch_short_interest(latest_settlement)
        if si_df is None or si_df.empty:
            return empty_result(self.name, run_date, error=f"empty data for {latest_settlement}")

        log.info(f"Loaded {len(si_df)} short interest records from {latest_settlement}")

        # Step 3: filter by days-to-cover (cheap pre-filter, no API calls)
        before = len(si_df)
        si_df = si_df[si_df["days_to_cover"] >= self.MIN_DAYS_TO_COVER].copy()
        log.info(f"Days-to-cover filter (>= {self.MIN_DAYS_TO_COVER}): {before} -> {len(si_df)}")

        if si_df.empty:
            return empty_result(self.name, run_date)

        # Step 4: filter to listed exchanges only (drop OTC)
        # Modern FINRA uses exchange names: NYSE, NASDAQ, ARCA, BATS, etc.
        # OTC entries have things like "OTC", "OTCBB", "u" depending on era.
        if "market_category" in si_df.columns:
            before = len(si_df)
            si_df = si_df[
                si_df["market_category"].str.upper().isin(
                    ["NYSE", "NASDAQ", "ARCA", "BATS", "BX", "AMEX", "IEXG", "EDGX", "EDGA"]
                )
            ].copy()
            log.info(f"Exchange filter (listed only): {before} -> {len(si_df)}")

        # Step 5: enrich with float data from yfinance
        try:
            import yfinance as yf
        except ImportError:
            return empty_result(self.name, run_date,
                error="yfinance not installed - run: python -m pip install yfinance")

        log.info(f"Fetching float data for {len(si_df)} candidates (this is the slow part)...")
        floats: Dict[str, Optional[float]] = {}

        symbols = si_df["symbol"].tolist()
        cached_count = sum(1 for s in symbols if _load_cached_float(s) is not None)
        log.info(f"  {cached_count} cached, {len(symbols) - cached_count} need yfinance fetch")
        if (len(symbols) - cached_count) > 100:
            log.info(f"  Estimated time: ~{(len(symbols) - cached_count) * 0.7 / 60:.1f} min")

        for i, symbol in enumerate(symbols):
            cached = _load_cached_float(symbol)
            if cached is not None:
                floats[symbol] = cached
                continue

            try:
                time.sleep(self.REQUEST_DELAY_SEC)
                ticker = yf.Ticker(symbol)
                info = ticker.info
                float_shares = info.get("floatShares") or info.get("sharesOutstanding")
                if float_shares:
                    float_shares = float(float_shares)
            except Exception as e:
                log.debug(f"  yfinance float fetch failed for {symbol}: {e}")
                float_shares = None

            floats[symbol] = float_shares
            _save_cached_float(symbol, float_shares)

            if (i + 1) % 100 == 0:
                log.info(f"  Processed {i + 1}/{len(symbols)} float fetches")

        si_df["float_shares"] = si_df["symbol"].map(floats)

        # Step 6: compute SI as % of float, filter
        before = len(si_df)
        si_df = si_df[si_df["float_shares"].notna() & (si_df["float_shares"] > 0)].copy()
        log.info(f"Float-available filter: {before} -> {len(si_df)}")

        si_df["short_pct_float"] = si_df["current_short_shares"] / si_df["float_shares"]

        before = len(si_df)
        si_df = si_df[si_df["short_pct_float"] >= self.MIN_SHORT_PCT_FLOAT].copy()
        # Cap at 100% — anything over is data quality issue
        si_df = si_df[si_df["short_pct_float"] <= 1.0].copy()
        log.info(f"SI%-of-float filter ({self.MIN_SHORT_PCT_FLOAT:.0%}-100%): {before} -> {len(si_df)}")

        if si_df.empty:
            return empty_result(self.name, run_date)

        # Step 7: enrich with 20-day momentum from Alpaca via existing data layer
        log.info(f"Fetching 20-day momentum for {len(si_df)} candidates...")
        momentum_data = self._fetch_momentum(si_df["symbol"].tolist(), run_date)

        si_df["last_close"] = si_df["symbol"].map(lambda s: momentum_data.get(s, {}).get("last_close"))
        si_df["twenty_day_return"] = si_df["symbol"].map(lambda s: momentum_data.get(s, {}).get("return_20d"))

        before = len(si_df)
        si_df = si_df[si_df["last_close"].notna() & (si_df["last_close"] >= self.MIN_PRICE)].copy()
        log.info(f"Min-price filter (>= ${self.MIN_PRICE}): {before} -> {len(si_df)}")

        before = len(si_df)
        si_df = si_df[si_df["twenty_day_return"].notna() & (si_df["twenty_day_return"] >= self.MIN_TWENTY_DAY_RETURN)].copy()
        log.info(f"20-day-momentum filter (>= {self.MIN_TWENTY_DAY_RETURN:.0%}): {before} -> {len(si_df)}")

        before = len(si_df)
        si_df = si_df[si_df["avg_daily_shares"] >= self.MIN_AVG_VOLUME].copy()
        log.info(f"Min-volume filter (>= {self.MIN_AVG_VOLUME:,}): {before} -> {len(si_df)}")

        if si_df.empty:
            return empty_result(self.name, run_date)

        # Step 8: build output rows
        rows = []
        for _, r in si_df.iterrows():
            si_pct = r["short_pct_float"] * 100
            ret_pct = r["twenty_day_return"] * 100

            # Score: SI% (0-100) + days_to_cover bonus (capped) + momentum bonus
            si_score = min(100, si_pct)
            dtc_bonus = min(20, r["days_to_cover"] * 2)
            momentum_bonus = min(30, ret_pct)
            score = si_score + dtc_bonus + momentum_bonus

            rows.append({
                "ticker": r["symbol"],
                "name": r.get("name", ""),
                "short_pct_float": round(si_pct, 1),
                "days_to_cover": round(float(r["days_to_cover"]), 1),
                "twenty_day_return": round(ret_pct, 1),
                "last_close": round(float(r["last_close"]), 2),
                "current_short_shares": int(r["current_short_shares"]),
                "float_shares": int(r["float_shares"]),
                "avg_daily_shares": int(r["avg_daily_shares"]),
                "settlement_date": latest_settlement.isoformat(),
                "score": round(score, 2),
                "reason": (
                    f"SI {si_pct:.0f}% of float, {r['days_to_cover']:.1f} days-to-cover, "
                    f"+{ret_pct:.0f}% in last 20d at ${r['last_close']:.2f}"
                ),
            })

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Settlement date: {latest_settlement}",
                f"Min SI%: {self.MIN_SHORT_PCT_FLOAT:.0%}",
                f"Min days-to-cover: {self.MIN_DAYS_TO_COVER}",
                f"Min 20d return: {self.MIN_TWENTY_DAY_RETURN:.0%}",
                f"Min price: ${self.MIN_PRICE}",
                f"Final candidates: {len(rows)}",
                "ACORNS SLEEVE ONLY — squeezes are speculation, not value",
            ],
        )

    def _fetch_momentum(self, symbols: list, run_date: date) -> Dict[str, Dict]:
        """Fetch 20-day return and last close for a list of symbols via Alpaca.

        Returns {symbol: {last_close, return_20d}}. Reuses src/data.py batching.
        """
        from src.data import fetch_bars
        from src.config import load_credentials

        try:
            creds = load_credentials()
            # Fetch 35 calendar days to ensure we have 20+ trading days available
            start = run_date - timedelta(days=35)
            end = run_date
            bars_dict = fetch_bars(
                symbols=symbols,
                start=start,
                end=end,
                creds=creds,
                use_cache=True,
            )
        except Exception as e:
            log.warning(f"  Failed to fetch bars: {e}")
            return {}

        out: Dict[str, Dict] = {}
        for symbol in symbols:
            try:
                df = bars_dict.get(symbol)
                if df is None or df.empty or len(df) < 21:
                    continue
                # df is already sorted by index (timestamp). Sort defensively anyway.
                df = df.sort_index()
                last_close = float(df["close"].iloc[-1])
                # 20 trading days back from the last row
                ref_close = float(df["close"].iloc[-21])
                ret = (last_close - ref_close) / ref_close if ref_close else None
                out[symbol] = {
                    "last_close": last_close,
                    "return_20d": ret,
                }
            except Exception as e:
                log.debug(f"  Momentum compute failed for {symbol}: {e}")
                continue

        return out
# --- Phase 4e backtest support ---

def backtest_mode(as_of_date: date, output_dir=None) -> int:
    """Run short_squeeze scanner as-of a historical date.

    Mostly clean: FINRA settlement files are date-keyed historical records,
    20-day momentum uses Alpaca cache (already date-aware).

    KNOWN LOOK-AHEAD: yfinance floatShares returns CURRENT float, not historical.
    For most names this is fine (float changes slowly via secondaries/buybacks),
    but stocks with major share-count events between as_of_date and today
    will have inflated/deflated short% calculations. Acceptable for v1.

    Output goes to <output_dir>/<as_of_date>/short_squeeze.csv.
    """
    from pathlib import Path

    output_dir = Path(output_dir) if output_dir else Path("backtest_output")
    scanner = ShortSqueezeScanner()

    try:
        result = scanner.run(as_of_date)
    except Exception as e:
        log.warning(f"short_squeeze backtest_mode failed for {as_of_date}: {e}")
        return 0

    if result.error or result.candidates.empty:
        return 0

    date_dir = output_dir / as_of_date.isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    out_path = date_dir / "short_squeeze.csv"
    result.candidates.to_csv(out_path, index=False)
    log.debug(f"  short_squeeze {as_of_date}: wrote {len(result.candidates)} candidates to {out_path}")

    return len(result.candidates)