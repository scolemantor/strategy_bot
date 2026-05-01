"""52-week breakout scanner.

Surfaces stocks that closed at a new 52-week high recently, on above-average
volume. Classic momentum signal: when a stock makes new highs everyone owns
it at a profit, there's no overhead supply, and breakouts can run.

Pipeline:
  1. Load the US equity universe from Alpaca
  2. Fetch ~260 days of daily bars per symbol (52 weeks + buffer)
  3. For each symbol, check: did the latest close exceed the trailing 252-day high?
  4. Compute: how high above prior high (breakout %), volume ratio (today vs avg)
  5. Rank by composite score: breakout strength * volume confirmation

Honest limits:
  - Many breakouts fail. False-positive rate is high.
  - This is idea generation, not signal generation.
  - Survivorship bias: we only see currently-listed names.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from src.config import load_credentials
from src.data import fetch_bars

from .base import Scanner, ScanResult, empty_result
from .universe import get_us_equity_universe

log = logging.getLogger(__name__)


class Breakout52wScanner(Scanner):
    name = "breakout_52w"
    description = "New 52-week highs on above-average volume"
    cadence = "daily"

    LOOKBACK_DAYS_FOR_HIGH = 252
    BREAKOUT_RECENCY_DAYS = 5
    MIN_VOLUME_RATIO = 1.5
    MIN_BREAKOUT_PCT = 0.005
    MIN_PRICE = 5.0
    MAX_UNIVERSE_SIZE = 5000
    BARS_FETCH_DAYS = 280

    def run(self, run_date: date) -> ScanResult:
        try:
            universe = get_us_equity_universe()
        except Exception as e:
            log.exception("Failed to load universe")
            return empty_result(self.name, run_date, error=f"universe: {e}")

        if len(universe) > self.MAX_UNIVERSE_SIZE:
            log.info(f"Universe size {len(universe)} > cap {self.MAX_UNIVERSE_SIZE}; truncating")
            universe = universe[:self.MAX_UNIVERSE_SIZE]

        log.info(f"Scanning {len(universe)} symbols for 52w breakouts")

        try:
            creds = load_credentials()
        except Exception as e:
            return empty_result(self.name, run_date, error=f"credentials: {e}")

        start = run_date - timedelta(days=self.BARS_FETCH_DAYS)
        end = run_date

        try:
            bars = fetch_bars(universe, start, end, creds, use_cache=True)
        except Exception as e:
            log.exception("Failed to fetch bars")
            return empty_result(self.name, run_date, error=f"bars fetch: {e}")

        log.info(f"Got bars for {len(bars)} symbols")

        rows = []
        for symbol, df in bars.items():
            try:
                analysis = self._analyze_symbol(symbol, df, run_date)
                if analysis is not None:
                    rows.append(analysis)
            except Exception as e:
                log.debug(f"Skipping {symbol}: {e}")

        log.info(f"Found {len(rows)} candidates")

        if not rows:
            return empty_result(self.name, run_date)

        df_out = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df_out,
            notes=[
                f"Universe size: {len(universe)}",
                f"Symbols with bars: {len(bars)}",
                f"Breakout recency: {self.BREAKOUT_RECENCY_DAYS} days",
                f"Min volume ratio: {self.MIN_VOLUME_RATIO}x",
                f"Min breakout: {self.MIN_BREAKOUT_PCT:.1%}",
                f"Min price: ${self.MIN_PRICE}",
            ],
        )

    def _analyze_symbol(
        self, symbol: str, df: pd.DataFrame, run_date: date,
    ) -> Optional[dict]:
        if df.empty or len(df) < self.LOOKBACK_DAYS_FOR_HIGH:
            return None

        df = df.sort_index()
        last_close = float(df["close"].iloc[-1])
        last_volume = float(df["volume"].iloc[-1])

        if last_close < self.MIN_PRICE:
            return None

        window = df.iloc[-(self.LOOKBACK_DAYS_FOR_HIGH + 1):-1]
        if window.empty:
            return None

        prior_high = float(window["high"].max())
        if prior_high <= 0:
            return None

        recent = df.iloc[-self.BREAKOUT_RECENCY_DAYS:]
        recent_high = float(recent["high"].max())
        if recent_high <= prior_high * (1 + self.MIN_BREAKOUT_PCT):
            return None

        vol_window = df["volume"].iloc[-61:-1]
        avg_volume = float(vol_window.mean()) if len(vol_window) > 0 else 0.0
        if avg_volume <= 0:
            return None
        volume_ratio = last_volume / avg_volume
        if volume_ratio < self.MIN_VOLUME_RATIO:
            return None

        breakout_pct = (recent_high - prior_high) / prior_high

        breakout_day = recent[recent["high"] > prior_high].index[0]
        breakout_day_str = (
            breakout_day.date().isoformat()
            if hasattr(breakout_day, "date") else str(breakout_day)[:10]
        )

        score = (breakout_pct * 100) + min(volume_ratio * 5, 25)

        return {
            "ticker": symbol,
            "last_close": round(last_close, 2),
            "prior_52w_high": round(prior_high, 2),
            "breakout_pct": round(breakout_pct * 100, 2),
            "volume_ratio": round(volume_ratio, 2),
            "breakout_day": breakout_day_str,
            "score": round(score, 2),
            "reason": (
                f"Closed ${last_close:.2f}, broke {prior_high:.2f} 52w high "
                f"({breakout_pct:.1%} above) on {volume_ratio:.1f}x avg volume"
            ),
        }