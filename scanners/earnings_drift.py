"""Earnings drift scanner.

Surfaces stocks that beat earnings expectations meaningfully (>10% surprise) AND
have continued to drift higher since the announcement. The 'post-earnings drift'
phenomenon is one of the most documented anomalies in academic finance — beat
stocks tend to outperform for 30-60 days after the announcement because analysts
revise estimates slowly and retail flows arrive late.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from .base import Scanner, ScanResult, empty_result
from .universe import get_us_equity_universe

log = logging.getLogger(__name__)


class EarningsDriftScanner(Scanner):
    name = "earnings_drift"
    description = "Post-earnings momentum: big beats with positive drift since announcement"
    cadence = "daily"

    LOOKBACK_DAYS = 60
    MIN_SURPRISE_PCT = 0.10
    MIN_POST_EARNINGS_GAIN = 0.0
    MIN_PRICE = 5.0
    MAX_UNIVERSE_SIZE = 1500
    REQUEST_DELAY_SEC = 0.3

    def run(self, run_date: date) -> ScanResult:
        try:
            import yfinance as yf
        except ImportError:
            return empty_result(
                self.name, run_date,
                error="yfinance not installed - run: python -m pip install yfinance",
            )

        try:
            universe = get_us_equity_universe()
        except Exception as e:
            log.exception("Failed to load universe")
            return empty_result(self.name, run_date, error=f"universe: {e}")

        if len(universe) > self.MAX_UNIVERSE_SIZE:
            log.info(
                f"Universe size {len(universe)} > cap {self.MAX_UNIVERSE_SIZE}; "
                f"sampling first {self.MAX_UNIVERSE_SIZE}"
            )
            universe = universe[:self.MAX_UNIVERSE_SIZE]

        log.info(f"Scanning {len(universe)} symbols for earnings drift candidates")
        log.info(f"Estimated time: {len(universe) * self.REQUEST_DELAY_SEC / 60:.1f} minutes")

        rows = []
        cutoff = pd.Timestamp(run_date - timedelta(days=self.LOOKBACK_DAYS))

        for i, symbol in enumerate(universe):
            try:
                analysis = self._analyze_symbol(yf, symbol, run_date, cutoff)
                if analysis is not None:
                    rows.append(analysis)
            except Exception as e:
                log.debug(f"Skipping {symbol}: {e}")

            time.sleep(self.REQUEST_DELAY_SEC)

            if (i + 1) % 100 == 0:
                log.info(f"Processed {i + 1}/{len(universe)} symbols, {len(rows)} candidates so far")

        log.info(f"Found {len(rows)} earnings-drift candidates")

        if not rows:
            return empty_result(self.name, run_date)

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Lookback: {self.LOOKBACK_DAYS} days",
                f"Min surprise: {self.MIN_SURPRISE_PCT:.0%}",
                f"Min post-earnings gain: {self.MIN_POST_EARNINGS_GAIN:.0%}",
                f"Universe scanned: {len(universe)}",
            ],
        )

    def _analyze_symbol(
        self, yf, symbol: str, run_date: date, cutoff: pd.Timestamp,
    ) -> Optional[dict]:
        try:
            ticker = yf.Ticker(symbol)
        except Exception as e:
            log.debug(f"yf.Ticker failed for {symbol}: {e}")
            return None

        try:
            earnings = ticker.get_earnings_dates(limit=8)
        except Exception as e:
            log.debug(f"get_earnings_dates failed for {symbol}: {e}")
            return None

        if earnings is None or earnings.empty:
            return None

        try:
            earnings.index = pd.to_datetime(earnings.index).tz_localize(None)
        except (TypeError, AttributeError):
            try:
                earnings.index = earnings.index.tz_convert("UTC").tz_localize(None)
            except Exception:
                pass

        recent = earnings[earnings.index >= cutoff].copy()
        if recent.empty:
            return None

        surprise_col = None
        for candidate in ["Surprise(%)", "Surprise (%)", "surprise"]:
            if candidate in recent.columns:
                surprise_col = candidate
                break
        if surprise_col is None:
            return None

        reported_col = None
        for candidate in ["Reported EPS", "EPS Actual", "reported"]:
            if candidate in recent.columns:
                reported_col = candidate
                break
        if reported_col is None:
            return None

        past = recent[recent[reported_col].notna()].copy()
        if past.empty:
            return None

        past = past.sort_index(ascending=False)
        latest = past.iloc[0]
        latest_date = past.index[0]

        surprise = float(latest[surprise_col]) if pd.notna(latest[surprise_col]) else None
        if surprise is None:
            return None
        if abs(surprise) > 1:
            surprise = surprise / 100.0

        if surprise < self.MIN_SURPRISE_PCT:
            return None

        try:
            hist = ticker.history(start=(latest_date - pd.Timedelta(days=2)).date(), period="3mo")
        except Exception as e:
            log.debug(f"history fetch failed for {symbol}: {e}")
            return None

        if hist is None or hist.empty:
            return None

        try:
            hist.index = pd.to_datetime(hist.index).tz_localize(None) if hasattr(hist.index, 'tz_localize') else pd.to_datetime(hist.index)
        except Exception:
            pass

        try:
            after_earnings = hist[hist.index >= latest_date]
            if after_earnings.empty:
                return None
            earnings_close = float(after_earnings["Close"].iloc[0])
            current_close = float(hist["Close"].iloc[-1])
        except Exception:
            return None

        if earnings_close <= 0 or current_close < self.MIN_PRICE:
            return None

        post_earnings_pct = (current_close - earnings_close) / earnings_close
        if post_earnings_pct < self.MIN_POST_EARNINGS_GAIN:
            return None

        days_since = (pd.Timestamp(run_date) - latest_date).days

        freshness = max(0, 1 - days_since / self.LOOKBACK_DAYS)
        score = (surprise * 100) + (post_earnings_pct * 50) + (freshness * 10)

        return {
            "ticker": symbol,
            "earnings_date": latest_date.date().isoformat(),
            "days_since_earnings": days_since,
            "surprise_pct": round(surprise * 100, 2),
            "earnings_close": round(earnings_close, 2),
            "current_close": round(current_close, 2),
            "post_earnings_pct": round(post_earnings_pct * 100, 2),
            "score": round(score, 2),
            "reason": (
                f"Beat by {surprise:.1%} on {latest_date.date()}, "
                f"up {post_earnings_pct:.1%} since ({days_since}d ago)"
            ),
        }