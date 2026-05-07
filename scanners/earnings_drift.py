"""Earnings drift scanner.

Surfaces S&P 500 stocks that beat earnings expectations meaningfully and have
continued to drift higher since the announcement. The post-earnings drift
phenomenon (PEAD) is one of the most documented anomalies in academic finance:
beat stocks tend to outperform for 30-60 days after the announcement because
analysts revise estimates slowly and retail flows arrive late.

Architecture notes:
- Universe = S&P 500 only (real operating companies that report quarterly).
  Earlier versions iterated 11k+ Alpaca tickers including ETFs and warrants —
  almost none have earnings data, so 99% of fetches were wasted.
- Per-symbol parquet cache for both earnings dates and price history. yfinance
  is slow and rate-limited; without caching, daily reruns would be punishing.
- Cache TTL = 6 hours. Earnings data changes after market close on report day,
  so 6h refresh catches new reports same-day without hammering yfinance.

Honest limits:
- yfinance data is unofficial; occasional missing or wrong values.
- Survivorship bias: current S&P 500 only includes still-listed names.
- "Beat" is vs sell-side estimates which are themselves noisy.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from src.http_utils import with_deadline, yfinance_session

from .base import Scanner, ScanResult, empty_result
from .universe import get_sp500_universe

log = logging.getLogger(__name__)

try:
    _YF_SESSION = yfinance_session(30)
except Exception:
    _YF_SESSION = None

EARNINGS_CACHE_DIR = Path("data_cache/yfinance_earnings")
EARNINGS_CACHE_TTL_HOURS = 6


def _earnings_cache_path(symbol: str) -> Path:
    return EARNINGS_CACHE_DIR / f"{symbol}.parquet"


def _load_cached_earnings(symbol: str) -> Optional[pd.DataFrame]:
    p = _earnings_cache_path(symbol)
    if not p.exists():
        return None
    age_hours = (time.time() - p.stat().st_mtime) / 3600
    if age_hours > EARNINGS_CACHE_TTL_HOURS:
        return None
    try:
        return pd.read_parquet(p)
    except Exception as e:
        log.debug(f"Failed to load earnings cache for {symbol}: {e}")
        return None


def _save_cached_earnings(symbol: str, df: pd.DataFrame) -> None:
    EARNINGS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        # Parquet doesn't like tz-aware datetimes in the index; flatten first
        df_to_save = df.copy()
        if hasattr(df_to_save.index, "tz") and df_to_save.index.tz is not None:
            df_to_save.index = df_to_save.index.tz_localize(None)
        df_to_save.to_parquet(_earnings_cache_path(symbol))
    except Exception as e:
        log.debug(f"Failed to save earnings cache for {symbol}: {e}")


class EarningsDriftScanner(Scanner):
    name = "earnings_drift"
    description = "Post-earnings momentum: big beats with positive drift since announcement"
    cadence = "daily"

    LOOKBACK_DAYS = 60
    MIN_SURPRISE_PCT = 0.05  # 5% beat — mega-caps rarely surprise more than this
    MAX_REASONABLE_SURPRISE_PCT = 0.50  # >50% almost always = near-zero estimate, garbage % math
    MIN_ABS_ESTIMATE = 0.10  # Skip filings with EPS estimate below $0.10 (% math unreliable)
    MIN_POST_EARNINGS_GAIN = 0.0  # any positive drift counts
    MIN_PRICE = 5.0
    REQUEST_DELAY_SEC = 0.2  # courtesy delay; only hits yfinance on cache misses

    def run(self, run_date: date) -> ScanResult:
        try:
            import yfinance as yf
        except ImportError:
            return empty_result(
                self.name, run_date,
                error="yfinance not installed - run: python -m pip install yfinance",
            )

        try:
            universe = get_sp500_universe()
        except Exception as e:
            log.exception("Failed to load S&P 500 universe")
            return empty_result(self.name, run_date, error=f"universe: {e}")

        log.info(f"Scanning {len(universe)} S&P 500 symbols for earnings drift")

        # Pre-flight cache check
        cached_count = sum(1 for s in universe if _load_cached_earnings(s) is not None)
        log.info(
            f"  {cached_count} cached, {len(universe) - cached_count} need yfinance fetch"
        )
        uncached = len(universe) - cached_count
        if uncached > 0:
            log.info(
                f"  Estimated time: ~{uncached * (self.REQUEST_DELAY_SEC + 0.7) / 60:.1f} min"
            )

        rows = []
        cutoff = pd.Timestamp(run_date - timedelta(days=self.LOOKBACK_DAYS))

        for i, symbol in enumerate(universe):
            try:
                analysis = self._analyze_symbol(yf, symbol, run_date, cutoff)
                if analysis is not None:
                    rows.append(analysis)
            except Exception as e:
                log.debug(f"Skipping {symbol}: {e}")

            if (i + 1) % 50 == 0:
                log.info(f"  Processed {i + 1}/{len(universe)}, {len(rows)} candidates so far")

        log.info(f"Found {len(rows)} earnings-drift candidates")

        if not rows:
            return empty_result(self.name, run_date)

        df = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df,
            notes=[
                f"Universe: S&P 500 ({len(universe)} symbols)",
                f"Lookback: {self.LOOKBACK_DAYS} days",
                f"Min surprise: {self.MIN_SURPRISE_PCT:.0%}",
                f"Min post-earnings gain: {self.MIN_POST_EARNINGS_GAIN:.0%}",
            ],
        )

    def _analyze_symbol(
        self, yf, symbol: str, run_date: date, cutoff: pd.Timestamp,
    ) -> Optional[dict]:
        # Try cached earnings first
        earnings = _load_cached_earnings(symbol)

        if earnings is None:
            # Cache miss — fetch from yfinance with courtesy delay
            time.sleep(self.REQUEST_DELAY_SEC)
            try:
                ticker = yf.Ticker(symbol, session=_YF_SESSION)
                earnings = with_deadline(lambda: ticker.get_earnings_dates(limit=8), timeout=30, default=None)
            except Exception as e:
                log.debug(f"yfinance earnings fetch failed for {symbol}: {e}")
                return None

            if earnings is None or earnings.empty:
                # Cache an empty marker so we don't re-fetch this symbol for 6h
                _save_cached_earnings(symbol, pd.DataFrame())
                return None

            # Strip tz info from index for parquet compatibility
            try:
                earnings.index = pd.to_datetime(earnings.index).tz_convert("UTC").tz_localize(None)
            except (TypeError, AttributeError):
                try:
                    earnings.index = pd.to_datetime(earnings.index).tz_localize(None)
                except Exception:
                    earnings.index = pd.to_datetime(earnings.index)

            _save_cached_earnings(symbol, earnings)

        if earnings.empty:
            return None

        # Find the right column (yfinance schema is fairly stable but defensive doesn't hurt)
        surprise_col = next(
            (c for c in ["Surprise(%)", "Surprise (%)", "surprise"] if c in earnings.columns),
            None,
        )
        reported_col = next(
            (c for c in ["Reported EPS", "EPS Actual", "reported"] if c in earnings.columns),
            None,
        )
        if surprise_col is None or reported_col is None:
            return None

        # Recent reports only, with actual reported numbers (skip future-dated estimates)
        recent = earnings[earnings.index >= cutoff].copy()
        past = recent[recent[reported_col].notna()].copy()
        if past.empty:
            return None

        past = past.sort_index(ascending=False)
        latest = past.iloc[0]
        latest_date = past.index[0]

        if pd.isna(latest[surprise_col]):
            return None

        surprise = float(latest[surprise_col])
        # yfinance returns surprise as a percentage (e.g. 5.32 means 5.32%)
        # Normalize: anything > 1 is in percent units, divide to get fraction
        if abs(surprise) > 1:
            surprise = surprise / 100.0

        if surprise < self.MIN_SURPRISE_PCT:
            return None

        # Filter implausible surprises (>50%): almost always near-zero EPS estimates
        # producing garbage percentage math. Real PEAD surprises are 5-30%.
        if surprise > self.MAX_REASONABLE_SURPRISE_PCT:
            log.debug(f"Skipping {symbol}: surprise {surprise:.1%} > sanity cap")
            return None

        # Also filter when the EPS estimate itself is near zero — even if the surprise %
        # is technically computed correctly, the signal is meaningless when estimate is tiny.
        estimate_col = next(
            (c for c in ["EPS Estimate", "Estimate", "estimate"] if c in earnings.columns),
            None,
        )
        if estimate_col is not None and not pd.isna(latest[estimate_col]):
            estimate = float(latest[estimate_col])
            if abs(estimate) < self.MIN_ABS_ESTIMATE:
                log.debug(f"Skipping {symbol}: estimate ${estimate:.3f} too small for reliable % math")
                return None

        # Now fetch price history to measure post-earnings drift
        try:
            ticker = yf.Ticker(symbol, session=_YF_SESSION)
            hist = with_deadline(
                lambda: ticker.history(
                    start=(latest_date - pd.Timedelta(days=2)).date(),
                    end=(pd.Timestamp(run_date) + pd.Timedelta(days=1)).date(),
                ),
                timeout=30,
                default=None,
            )
        except Exception as e:
            log.debug(f"history fetch failed for {symbol}: {e}")
            return None

        if hist is None or hist.empty:
            return None

        # Strip tz for clean comparison
        try:
            if hasattr(hist.index, "tz") and hist.index.tz is not None:
                hist.index = hist.index.tz_localize(None)
        except Exception:
            pass

        # Find first close on or after the earnings announcement
        latest_date_naive = pd.Timestamp(latest_date).tz_localize(None) if pd.Timestamp(latest_date).tz is not None else pd.Timestamp(latest_date)
        after = hist[hist.index >= latest_date_naive]
        if after.empty:
            return None

        earnings_close = float(after["Close"].iloc[0])
        current_close = float(hist["Close"].iloc[-1])

        if earnings_close <= 0 or current_close < self.MIN_PRICE:
            return None

        post_earnings_pct = (current_close - earnings_close) / earnings_close
        if post_earnings_pct < self.MIN_POST_EARNINGS_GAIN:
            return None

        days_since = (pd.Timestamp(run_date) - latest_date_naive).days

        # Score: surprise size + drift magnitude + recency bonus
        freshness = max(0, 1 - days_since / self.LOOKBACK_DAYS)
        score = (surprise * 100) + (post_earnings_pct * 50) + (freshness * 10)

        return {
            "ticker": symbol,
            "earnings_date": latest_date_naive.date().isoformat(),
            "days_since_earnings": days_since,
            "surprise_pct": round(surprise * 100, 2),
            "earnings_close": round(earnings_close, 2),
            "current_close": round(current_close, 2),
            "post_earnings_pct": round(post_earnings_pct * 100, 2),
            "score": round(score, 2),
            "reason": (
                f"Beat by {surprise:.1%} on {latest_date_naive.date()}, "
                f"up {post_earnings_pct:.1%} since ({days_since}d ago)"
            ),
        }
# --- Phase 4e backtest support ---

def backtest_mode(as_of_date: date, output_dir=None) -> int:
    """Run earnings_drift scanner as-of a historical date.

    Look-ahead protection: yfinance's get_earnings_dates returns the 8 most
    recent earnings as-of TODAY, which would include reports that occurred
    AFTER as_of_date. We add an explicit filter so only earnings reports
    with date < as_of_date are considered.

    The price history fetch is already date-bounded (end=as_of_date+1) in the
    parent _analyze_symbol logic, so no fix needed there.

    Output goes to <output_dir>/<as_of_date>/earnings_drift.csv.
    """
    from pathlib import Path
    import time

    output_dir = Path(output_dir) if output_dir else Path("backtest_output")

    try:
        import yfinance as yf
    except ImportError:
        log.warning("earnings_drift backtest_mode: yfinance not installed")
        return 0

    scanner = EarningsDriftScanner()

    try:
        universe = get_sp500_universe()
    except Exception as e:
        log.warning(f"earnings_drift backtest_mode: universe load failed: {e}")
        return 0

    log.info(f"earnings_drift backtest as-of {as_of_date}: scanning {len(universe)} S&P 500 symbols")

    rows = []
    cutoff = pd.Timestamp(as_of_date - timedelta(days=scanner.LOOKBACK_DAYS))
    as_of_ts = pd.Timestamp(as_of_date)

    for i, symbol in enumerate(universe):
        try:
            # Fetch earnings WITHOUT cache (cache stores today's view)
            time.sleep(scanner.REQUEST_DELAY_SEC)
            try:
                ticker = yf.Ticker(symbol, session=_YF_SESSION)
                earnings = with_deadline(lambda: ticker.get_earnings_dates(limit=16), timeout=30, default=None)
            except Exception:
                continue

            if earnings is None or earnings.empty:
                continue

            try:
                earnings.index = pd.to_datetime(earnings.index).tz_convert("UTC").tz_localize(None)
            except (TypeError, AttributeError):
                try:
                    earnings.index = pd.to_datetime(earnings.index).tz_localize(None)
                except Exception:
                    earnings.index = pd.to_datetime(earnings.index)

            # CRITICAL: filter out earnings that haven't happened yet as-of the historical date
            earnings = earnings[earnings.index < as_of_ts]
            if earnings.empty:
                continue

            # Run the same analysis logic but pass our filtered earnings
            analysis = _analyze_symbol_for_backtest(yf, symbol, as_of_date, cutoff, earnings, scanner)
            if analysis is not None:
                rows.append(analysis)
        except Exception as e:
            log.debug(f"  Skipping {symbol}: {e}")

        if (i + 1) % 50 == 0:
            log.info(f"  Processed {i + 1}/{len(universe)}, {len(rows)} candidates so far")

    if not rows:
        log.info(f"  No candidates for {as_of_date}")
        return 0

    df_out = pd.DataFrame(rows).sort_values("score", ascending=False).reset_index(drop=True)

    date_dir = output_dir / as_of_date.isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    out_path = date_dir / "earnings_drift.csv"
    df_out.to_csv(out_path, index=False)
    log.debug(f"  earnings_drift {as_of_date}: wrote {len(df_out)} candidates to {out_path}")

    return len(df_out)


def _analyze_symbol_for_backtest(yf, symbol, as_of_date, cutoff, earnings, scanner):
    """Same logic as EarningsDriftScanner._analyze_symbol but takes pre-filtered
    earnings DataFrame and uses as_of_date as the run_date."""
    surprise_col = next(
        (c for c in ["Surprise(%)", "Surprise (%)", "surprise"] if c in earnings.columns),
        None,
    )
    reported_col = next(
        (c for c in ["Reported EPS", "EPS Actual", "reported"] if c in earnings.columns),
        None,
    )
    if surprise_col is None or reported_col is None:
        return None

    recent = earnings[earnings.index >= cutoff].copy()
    past = recent[recent[reported_col].notna()].copy()
    if past.empty:
        return None

    past = past.sort_index(ascending=False)
    latest = past.iloc[0]
    latest_date = past.index[0]

    if pd.isna(latest[surprise_col]):
        return None

    surprise = float(latest[surprise_col])
    if abs(surprise) > 1:
        surprise = surprise / 100.0

    if surprise < scanner.MIN_SURPRISE_PCT:
        return None
    if surprise > scanner.MAX_REASONABLE_SURPRISE_PCT:
        return None

    estimate_col = next(
        (c for c in ["EPS Estimate", "Estimate", "estimate"] if c in earnings.columns),
        None,
    )
    if estimate_col is not None and not pd.isna(latest[estimate_col]):
        estimate = float(latest[estimate_col])
        if abs(estimate) < scanner.MIN_ABS_ESTIMATE:
            return None

    try:
        ticker = yf.Ticker(symbol, session=_YF_SESSION)
        hist = with_deadline(
            lambda: ticker.history(
                start=(latest_date - pd.Timedelta(days=2)).date(),
                end=(pd.Timestamp(as_of_date) + pd.Timedelta(days=1)).date(),
            ),
            timeout=30,
            default=None,
        )
    except Exception:
        return None

    if hist is None or hist.empty:
        return None

    try:
        if hasattr(hist.index, "tz") and hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
    except Exception:
        pass

    latest_date_naive = pd.Timestamp(latest_date).tz_localize(None) if pd.Timestamp(latest_date).tz is not None else pd.Timestamp(latest_date)
    after = hist[hist.index >= latest_date_naive]
    if after.empty:
        return None

    earnings_close = float(after["Close"].iloc[0])
    current_close = float(hist["Close"].iloc[-1])

    if earnings_close <= 0 or current_close < scanner.MIN_PRICE:
        return None

    post_earnings_pct = (current_close - earnings_close) / earnings_close
    if post_earnings_pct < scanner.MIN_POST_EARNINGS_GAIN:
        return None

    days_since = (pd.Timestamp(as_of_date) - latest_date_naive).days

    freshness = max(0, 1 - days_since / scanner.LOOKBACK_DAYS)
    score = (surprise * 100) + (post_earnings_pct * 50) + (freshness * 10)

    return {
        "ticker": symbol,
        "earnings_date": latest_date_naive.date().isoformat(),
        "days_since_earnings": days_since,
        "surprise_pct": round(surprise * 100, 2),
        "earnings_close": round(earnings_close, 2),
        "current_close": round(current_close, 2),
        "post_earnings_pct": round(post_earnings_pct * 100, 2),
        "score": round(score, 2),
        "reason": (
            f"Beat by {surprise:.1%} on {latest_date_naive.date()}, "
            f"up {post_earnings_pct:.1%} since ({days_since}d ago)"
        ),
    }