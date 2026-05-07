"""Earnings calendar scanner.

Forward-looking scanner — surfaces companies reporting earnings in the next
5 trading days, ranked by historical 3-day post-earnings move magnitude.

Use case: PRE-EARNINGS AWARENESS. This scanner does NOT recommend directional
bets. It surfaces names to watch during earnings week so positions don't get
blindsided by 15pct gaps. The directional play (post-earnings drift) is
scanner #3 earnings_drift, which fires AFTER results.

Source: yfinance Ticker.calendar (next earnings date) and Ticker.earnings_dates
(historical earnings dates) + Alpaca bars for actual price moves.

Universe: S&P 1500 (reuses get_sp1500_universe from scanner #8).

Filters:
  - Reporting in next 5 trading days (calendar lookup from yfinance)
  - Last 4 quarters of earnings exist (need history to compute average move)
  - Average absolute 3-day cumulative move > 5pct (historically explosive)

Honest limits:
  - yfinance calendar dates are sometimes wrong or stale (off by a day, missing entirely)
  - 4 quarters of history is small sample for average move calculation
  - Some recent IPOs won't have enough history and get filtered out
  - Doesn't predict direction — purely a magnitude/awareness signal
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from src.http_utils import with_deadline, yfinance_session

from .base import Scanner, ScanResult, empty_result
from .universe import get_sp1500_universe

log = logging.getLogger(__name__)

try:
    _YF_SESSION = yfinance_session(30)
except Exception:
    _YF_SESSION = None

CACHE_DIR = Path("data_cache")
EARNINGS_CACHE = CACHE_DIR / "yfinance_earnings"
EARNINGS_CACHE_TTL_HOURS = 24


def _earnings_cache_path(symbol: str) -> Path:
    return EARNINGS_CACHE / f"{symbol}.json"


def _load_cached_earnings(symbol: str) -> Optional[Dict]:
    p = _earnings_cache_path(symbol)
    if not p.exists():
        return None
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    if age_hours > EARNINGS_CACHE_TTL_HOURS:
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _save_cached_earnings(symbol: str, data: Dict) -> None:
    EARNINGS_CACHE.mkdir(parents=True, exist_ok=True)
    try:
        _earnings_cache_path(symbol).write_text(json.dumps(data, default=str))
    except Exception as e:
        log.debug(f"  Failed to cache earnings for {symbol}: {e}")


class EarningsCalendarScanner(Scanner):
    name = "earnings_calendar"
    description = "Companies reporting in next 5 trading days, ranked by historical 3-day post-earnings move"
    cadence = "daily"  # Earnings dates shift; want fresh checks daily

    LOOKAHEAD_TRADING_DAYS = 5
    MIN_QUARTERS_HISTORY = 3  # Need at least 3 prior earnings to compute meaningful average
    MIN_AVG_MOVE_PCT = 5.0  # Average absolute 3-day move must exceed this
    POST_EARNINGS_WINDOW_DAYS = 3  # Days after earnings for cumulative move
    REQUEST_DELAY_SEC = 0.2

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Lookahead: {self.LOOKAHEAD_TRADING_DAYS} trading days")
        log.info(f"Min historical quarters: {self.MIN_QUARTERS_HISTORY}")
        log.info(f"Min avg post-earnings move: {self.MIN_AVG_MOVE_PCT}%")
        log.info(f"Post-earnings window: {self.POST_EARNINGS_WINDOW_DAYS} trading days")

        try:
            import yfinance as yf
        except ImportError:
            return empty_result(self.name, run_date,
                error="yfinance not installed - run: python -m pip install yfinance")

        try:
            universe = get_sp1500_universe()
        except Exception as e:
            log.exception("Failed to load S&P 1500 universe")
            return empty_result(self.name, run_date, error=f"universe: {e}")

        log.info(f"Universe: {len(universe)} S&P 1500 tickers")

        # Window for "upcoming earnings"
        window_end = run_date + timedelta(days=self.LOOKAHEAD_TRADING_DAYS * 2)  # 2x for weekends
        log.info(f"Earnings window: {run_date} to {window_end}")

        cached_count = sum(1 for s in universe if _load_cached_earnings(s) is not None)
        log.info(f"  {cached_count} cached, {len(universe) - cached_count} need yfinance fetch")

        upcoming: List[Dict] = []
        for i, symbol in enumerate(universe):
            data = _load_cached_earnings(symbol)
            if data is None:
                data = self._fetch_earnings_data(yf, symbol)
                _save_cached_earnings(symbol, data)

            if data is None:
                continue

            next_earnings_str = data.get("next_earnings_date")
            historical = data.get("historical_dates", [])

            if not next_earnings_str:
                continue

            try:
                next_earnings = datetime.fromisoformat(next_earnings_str).date()
            except Exception:
                continue

            # Is it in our window?
            if next_earnings < run_date or next_earnings > window_end:
                continue

            if len(historical) < self.MIN_QUARTERS_HISTORY:
                continue

            upcoming.append({
                "symbol": symbol,
                "next_earnings_date": next_earnings,
                "historical_dates": historical,
                "earnings_time": data.get("earnings_time", ""),
            })

            if (i + 1) % 200 == 0:
                log.info(f"  Processed {i + 1}/{len(universe)}, {len(upcoming)} upcoming so far")

        log.info(f"Upcoming earnings (in window with sufficient history): {len(upcoming)}")

        if not upcoming:
            return empty_result(self.name, run_date)

        # Now compute historical average post-earnings move for each
        log.info(f"Computing historical post-earnings moves for {len(upcoming)} symbols...")
        try:
            from src.data import fetch_bars
            from src.config import load_credentials
            creds = load_credentials()
        except Exception as e:
            log.exception("Failed to load Alpaca credentials")
            return empty_result(self.name, run_date, error=f"creds: {e}")

        # Determine the date range we need to cover all historical earnings
        all_historical_dates = []
        for u in upcoming:
            for d_str in u["historical_dates"]:
                try:
                    d = datetime.fromisoformat(d_str).date() if isinstance(d_str, str) else d_str
                    all_historical_dates.append(d)
                except Exception:
                    continue

        if not all_historical_dates:
            return empty_result(self.name, run_date, error="no historical dates available")

        earliest = min(all_historical_dates) - timedelta(days=10)
        latest = run_date

        # Batch fetch bars for all upcoming symbols (already chunks at 100)
        symbols_to_fetch = [u["symbol"] for u in upcoming]
        try:
            bars_dict = fetch_bars(
                symbols=symbols_to_fetch,
                start=earliest,
                end=latest,
                creds=creds,
                use_cache=True,
            )
        except Exception as e:
            log.exception("Failed to fetch bars")
            return empty_result(self.name, run_date, error=f"bars: {e}")

        rows = []
        for u in upcoming:
            symbol = u["symbol"]
            bars = bars_dict.get(symbol)
            if bars is None or bars.empty:
                continue

            bars = bars.sort_index()
            moves = self._compute_post_earnings_moves(bars, u["historical_dates"])
            if len(moves) < self.MIN_QUARTERS_HISTORY:
                continue

            avg_abs_move = sum(abs(m) for m in moves) / len(moves)
            if avg_abs_move < self.MIN_AVG_MOVE_PCT:
                continue

            # Score: avg move + days-until-earnings (sooner = higher priority)
            days_until = (u["next_earnings_date"] - run_date).days
            urgency_bonus = max(0, 10 - days_until * 2)  # 10 for today, 0 for >5 days
            score = min(50, avg_abs_move) + urgency_bonus

            rows.append({
                "ticker": symbol,
                "earnings_date": u["next_earnings_date"].isoformat(),
                "earnings_time": u["earnings_time"],
                "days_until": days_until,
                "avg_abs_3d_move_pct": round(avg_abs_move, 2),
                "last_4q_moves_pct": ", ".join([f"{m:+.1f}%" for m in moves[:4]]),
                "n_quarters": len(moves),
                "score": round(score, 2),
                "reason": (
                    f"Reports {u['next_earnings_date']} ({u['earnings_time'] or 'time TBD'}), "
                    f"avg ±{avg_abs_move:.1f}% over last {len(moves)} quarters"
                ),
            })

        if not rows:
            log.info("No upcoming earnings met the historical-move threshold")
            return empty_result(self.name, run_date)

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Universe: S&P 1500",
                f"Lookahead: {self.LOOKAHEAD_TRADING_DAYS} trading days",
                f"Min avg post-earnings move: {self.MIN_AVG_MOVE_PCT}%",
                f"Window: {run_date} to {window_end}",
                f"Final candidates: {len(rows)}",
                "PRE-EARNINGS AWARENESS — not a directional signal. Use to manage existing positions.",
            ],
        )

    def _fetch_earnings_data(self, yf, symbol: str) -> Optional[Dict]:
        """Fetch next earnings date + last 4 quarters of historical earnings dates."""
        time.sleep(self.REQUEST_DELAY_SEC)
        try:
            ticker = yf.Ticker(symbol, session=_YF_SESSION)

            # Get historical earnings dates
            try:
                eh = with_deadline(lambda: ticker.earnings_dates, timeout=30, default=None)
                if eh is None or eh.empty:
                    historical = []
                else:
                    # Index is the earnings datetime; we want past dates only
                    now = pd.Timestamp.now(tz=eh.index.tz)
                    past = eh[eh.index < now]
                    historical = [d.date().isoformat() for d in past.index[:8]]  # last 8 just in case
            except Exception:
                historical = []

            # Get next earnings date from calendar
            next_earnings = None
            earnings_time = ""
            try:
                cal = with_deadline(lambda: ticker.calendar, timeout=30, default=None)
                if cal:
                    earnings_dates = cal.get("Earnings Date")
                    if earnings_dates:
                        # cal["Earnings Date"] is a list of dates (start, end of expected window)
                        if isinstance(earnings_dates, list) and earnings_dates:
                            next_earnings = earnings_dates[0]
                        else:
                            next_earnings = earnings_dates
                        if hasattr(next_earnings, "isoformat"):
                            next_earnings = next_earnings.isoformat()
                        else:
                            next_earnings = str(next_earnings)
            except Exception:
                pass

            return {
                "symbol": symbol,
                "next_earnings_date": next_earnings,
                "historical_dates": historical,
                "earnings_time": earnings_time,
                "fetched_at": datetime.now().isoformat(),
            }
        except Exception as e:
            log.debug(f"  yfinance earnings fetch failed for {symbol}: {e}")
            return None

    def _compute_post_earnings_moves(self, bars: pd.DataFrame, historical_dates: List[str]) -> List[float]:
        """For each historical earnings date, compute the cumulative pct move
        from close-before to close-N-days-after.

        Returns list of percent moves (signed).
        """
        if bars.empty:
            return []

        moves = []
        for d_str in historical_dates:
            try:
                d = datetime.fromisoformat(d_str).date() if isinstance(d_str, str) else d_str
            except Exception:
                continue

            d_ts = pd.Timestamp(d)

            # Find the closest trading day at or before earnings
            before_mask = bars.index <= d_ts
            if not before_mask.any():
                continue
            close_before = float(bars.loc[before_mask, "close"].iloc[-1])

            # Find the close N trading days after
            after_bars = bars[bars.index > d_ts]
            if len(after_bars) < self.POST_EARNINGS_WINDOW_DAYS:
                continue
            close_after = float(after_bars["close"].iloc[self.POST_EARNINGS_WINDOW_DAYS - 1])

            if close_before <= 0:
                continue

            move_pct = ((close_after - close_before) / close_before) * 100
            moves.append(move_pct)

        return moves