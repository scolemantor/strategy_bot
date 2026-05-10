"""Technical overlay scanner — Phase 8a backend.

Scans EVERY ticker on the watchlist (loaded fresh each run from
config/watchlist.yaml — newly-added tickers picked up within 15 min) and
computes a comprehensive technical setup-quality score plus per-ticker
metric breakdown.

Architecture note (Phase 8a refactor 2026-05-10): the original commit
(61b3506) read top-10 from master_ranked.csv. The reworked architecture
makes WATCHLIST the work surface — technical analysis runs on tickers
the user has expressed conviction about, not arbitrary scanner output.
master_ranked is a discovery feed surfaced separately at /signals.

Two intended schedules (configured separately in cron_schedule.yaml):
  - */15 9-16 * * 1-5: every 15 min during market hours, weekdays
  - Plus on-demand: `python scan.py run technical_overlay`

Outputs:
  scan_output/<run_date>/technical_overlay.csv
      flat per-ticker summary, one row per watchlist ticker, sortable
      by setup_score in dashboard views
  data_cache/technical/<TICKER>.json
      full metric breakdown per ticker (trend / momentum / volume /
      volatility / key_levels). Overwritten each run; the dashboard's
      GET /api/technical/{ticker} endpoint reads this directly.

Setup quality score (0-100):
  Weighted aggregate across five dimensions:
    Trend (35 pts):    above MAs, MA alignment, golden/death cross
    Momentum (25 pts): RSI zone, MACD direction + recent crosses
    Volume (20 pts):   vol ratio vs 20-day avg, OBV trend, up/down vol
    Setup (20 pts):    Bollinger position, distance to 52w high
    Penalty:           extreme RSI, below 200-day MA, falling volume

Indicator math: pandas-ta. Six indicators in use: SMA, RSI, MACD, ATR,
Bollinger Bands, OBV.

Phase 8 scope: backend scanner only. Frontend Watchlist page redesign
is Phase 8c. Excluded from scan_all (DISABLED_IN_SCAN_ALL) — runs as a
standalone scheduled job because its cadence is intraday, not daily.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# pandas-ta-classic — actively-maintained fork of the abandoned pandas-ta.
# Drop-in replacement under a different package name (`pandas_ta_classic`
# vs `pandas_ta`). Wrap import so a hypothetical install failure doesn't
# crash the scanner package on import — scanner degrades to a no-op +
# clear error message if TA_AVAILABLE is False at runtime.
try:
    import pandas_ta_classic as ta
    TA_AVAILABLE = True
    TA_IMPORT_ERROR = None
except Exception as _ta_err:  # ImportError or any compat issue
    ta = None
    TA_AVAILABLE = False
    TA_IMPORT_ERROR = str(_ta_err)

from src.config import load_credentials
from src.data import fetch_bars

from .base import Scanner, ScanResult, empty_result
from .watchlist import read_all_entries

log = logging.getLogger(__name__)

TECHNICAL_DETAIL_DIR = Path("data_cache/technical")
DEFAULT_BARS_FETCH_DAYS = 400  # need 252+ for 200-day MA + buffer
MIN_BARS_FOR_ANALYSIS = 200    # require at least 200 daily bars per ticker


# --- Helpers (indicator-tier classifications) ---

def _slope_class(series: pd.Series, lookback: int, threshold_pct: float = 0.5) -> str:
    """Compare last value to value `lookback` rows back. Returns 'rising',
    'falling', or 'flat' based on `threshold_pct` (default 0.5%)."""
    if series is None or len(series) < lookback + 1:
        return "unknown"
    last = series.iloc[-1]
    prior = series.iloc[-(lookback + 1)]
    if pd.isna(last) or pd.isna(prior) or prior == 0:
        return "unknown"
    pct = (last - prior) / abs(prior) * 100
    if pct > threshold_pct:
        return "rising"
    if pct < -threshold_pct:
        return "falling"
    return "flat"


def _detect_cross(
    series_a: pd.Series, series_b: pd.Series, window: int, direction: str,
) -> bool:
    """Did series_a cross series_b in the last `window` rows in `direction`?
    direction: 'above' = a went from below to above b; 'below' = inverse."""
    if series_a is None or series_b is None:
        return False
    a = series_a.tail(window + 1)
    b = series_b.tail(window + 1)
    if len(a) < 2 or len(b) < 2:
        return False
    diff = a - b
    if diff.isna().all():
        return False
    sign = np.sign(diff.dropna())
    if len(sign) < 2:
        return False
    if direction == "above":
        return bool(((sign.shift(1) < 0) & (sign > 0)).any())
    if direction == "below":
        return bool(((sign.shift(1) > 0) & (sign < 0)).any())
    return False


def _detect_macd_cross(macd: pd.Series, signal: pd.Series, window: int = 5) -> str:
    """Returns 'bullish', 'bearish', or 'none' based on MACD vs signal cross
    in last `window` bars."""
    if _detect_cross(macd, signal, window=window, direction="above"):
        return "bullish"
    if _detect_cross(macd, signal, window=window, direction="below"):
        return "bearish"
    return "none"


def _classify_rsi(rsi: Optional[float]) -> str:
    if rsi is None or pd.isna(rsi):
        return "unknown"
    if rsi >= 80:
        return "extreme_overbought"
    if rsi >= 70:
        return "overbought"
    if rsi >= 60:
        return "strong"
    if rsi >= 40:
        return "neutral"
    if rsi >= 30:
        return "weak"
    if rsi >= 20:
        return "oversold"
    return "extreme_oversold"


def _classify_obv_trend(obv: pd.Series) -> str:
    """30-day OBV trend: rising / falling / flat."""
    return _slope_class(obv, lookback=min(30, len(obv) - 1), threshold_pct=2.0)


def _safe_float(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        f = float(v)
        return f if not pd.isna(f) else None
    except (TypeError, ValueError):
        return None


# --- Indicator computation ---

def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Augment OHLCV DataFrame with technical indicator columns. Mutates a
    copy; original `df` is unchanged. Requires columns: open, high, low,
    close, volume."""
    df = df.sort_index().copy()

    # Trend — simple moving averages
    df["ma_20"] = ta.sma(df["close"], length=20)
    df["ma_50"] = ta.sma(df["close"], length=50)
    df["ma_200"] = ta.sma(df["close"], length=200)

    # Momentum — RSI + MACD
    df["rsi_14"] = ta.rsi(df["close"], length=14)
    macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
    if macd is not None and not macd.empty:
        df["macd"] = macd.iloc[:, 0]
        df["macd_hist"] = macd.iloc[:, 1]
        df["macd_signal"] = macd.iloc[:, 2]
    else:
        df["macd"] = df["macd_hist"] = df["macd_signal"] = np.nan

    # Volume
    df["vol_ma_20"] = ta.sma(df["volume"], length=20)
    df["obv"] = ta.obv(df["close"], df["volume"])

    # Volatility — ATR + Bollinger Bands
    df["atr_14"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    bb = ta.bbands(df["close"], length=20, std=2)
    if bb is not None and not bb.empty:
        df["bb_lower"] = bb.iloc[:, 0]
        df["bb_middle"] = bb.iloc[:, 1]
        df["bb_upper"] = bb.iloc[:, 2]
    else:
        df["bb_lower"] = df["bb_middle"] = df["bb_upper"] = np.nan

    return df


def _extract_metrics(df: pd.DataFrame, ticker: str) -> Optional[dict]:
    """Pull the latest-row metric snapshot from an indicator-augmented df.
    Returns None if df has fewer than MIN_BARS_FOR_ANALYSIS rows."""
    if df.empty or len(df) < MIN_BARS_FOR_ANALYSIS:
        return None

    latest = df.iloc[-1]
    last_close = _safe_float(latest["close"])
    if last_close is None:
        return None

    ma_20 = _safe_float(latest.get("ma_20"))
    ma_50 = _safe_float(latest.get("ma_50"))
    ma_200 = _safe_float(latest.get("ma_200"))

    above_ma_20 = ma_20 is not None and last_close > ma_20
    above_ma_50 = ma_50 is not None and last_close > ma_50
    above_ma_200 = ma_200 is not None and last_close > ma_200

    ma_20_slope = _slope_class(df["ma_20"], lookback=5)
    ma_50_slope = _slope_class(df["ma_50"], lookback=10)
    ma_200_slope = _slope_class(df["ma_200"], lookback=20)

    golden_cross = _detect_cross(df["ma_50"], df["ma_200"], window=30, direction="above")
    death_cross = _detect_cross(df["ma_50"], df["ma_200"], window=30, direction="below")

    rsi_14 = _safe_float(latest.get("rsi_14"))
    rsi_class = _classify_rsi(rsi_14)

    macd_hist = _safe_float(latest.get("macd_hist"))
    macd_above_signal = macd_hist is not None and macd_hist > 0
    macd_recent_cross = _detect_macd_cross(
        df["macd"], df["macd_signal"], window=5,
    )

    def _roc(n: int) -> Optional[float]:
        if len(df) <= n:
            return None
        prior = _safe_float(df["close"].iloc[-(n + 1)])
        if prior is None or prior == 0:
            return None
        return (last_close / prior - 1) * 100

    roc_5 = _roc(5)
    roc_10 = _roc(10)
    roc_20 = _roc(20)

    last_volume = _safe_float(latest["volume"])
    vol_ma_20 = _safe_float(latest.get("vol_ma_20"))
    vol_ratio_20 = (
        last_volume / vol_ma_20
        if last_volume is not None and vol_ma_20 is not None and vol_ma_20 > 0
        else None
    )

    obv_trend_30 = _classify_obv_trend(df["obv"])

    last_30 = df.tail(30)
    closes_shifted = last_30["close"].shift(1)
    up_mask = last_30["close"] > closes_shifted
    down_mask = last_30["close"] < closes_shifted
    up_vol = float(last_30.loc[up_mask, "volume"].sum())
    down_vol = float(last_30.loc[down_mask, "volume"].sum())
    up_down_vol_ratio = up_vol / down_vol if down_vol > 0 else None

    atr_14 = _safe_float(latest.get("atr_14"))
    bb_lower = _safe_float(latest.get("bb_lower"))
    bb_upper = _safe_float(latest.get("bb_upper"))
    if (
        bb_upper is not None and bb_lower is not None and bb_upper > bb_lower
    ):
        bb_position = (last_close - bb_lower) / (bb_upper - bb_lower)
    else:
        bb_position = None

    returns_30 = df["close"].pct_change().tail(30)
    valid_returns = returns_30.dropna()
    hv_30 = (
        float(valid_returns.std() * np.sqrt(252))
        if len(valid_returns) > 5
        else None
    )

    recent_30 = df.tail(30)
    high_30 = _safe_float(recent_30["high"].max())
    low_30 = _safe_float(recent_30["low"].min())

    last_252 = df.tail(252)
    high_52w = _safe_float(last_252["high"].max())
    low_52w = _safe_float(last_252["low"].min())
    pct_from_52w_high = (
        (last_close - high_52w) / high_52w * 100
        if high_52w is not None and high_52w > 0
        else None
    )
    pct_from_52w_low = (
        (last_close - low_52w) / low_52w * 100
        if low_52w is not None and low_52w > 0
        else None
    )

    return {
        "ticker": ticker,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "last_close": last_close,
        "trend": {
            "ma_20": ma_20,
            "ma_50": ma_50,
            "ma_200": ma_200,
            "above_ma_20": above_ma_20,
            "above_ma_50": above_ma_50,
            "above_ma_200": above_ma_200,
            "ma_20_slope": ma_20_slope,
            "ma_50_slope": ma_50_slope,
            "ma_200_slope": ma_200_slope,
            "golden_cross_recent": golden_cross,
            "death_cross_recent": death_cross,
        },
        "momentum": {
            "rsi_14": rsi_14,
            "rsi_class": rsi_class,
            "macd_hist": macd_hist,
            "macd_above_signal": macd_above_signal,
            "macd_recent_cross": macd_recent_cross,
            "roc_5d": roc_5,
            "roc_10d": roc_10,
            "roc_20d": roc_20,
        },
        "volume": {
            "last_volume": last_volume,
            "vol_ma_20": vol_ma_20,
            "vol_ratio_20d": vol_ratio_20,
            "obv_trend_30d": obv_trend_30,
            "up_down_vol_ratio_30d": up_down_vol_ratio,
        },
        "volatility": {
            "atr_14": atr_14,
            "bb_lower": bb_lower,
            "bb_upper": bb_upper,
            "bb_position": bb_position,
            "hv_30d_annualized": hv_30,
        },
        "key_levels": {
            "high_30d": high_30,
            "low_30d": low_30,
            "high_52w": high_52w,
            "low_52w": low_52w,
            "pct_from_52w_high": pct_from_52w_high,
            "pct_from_52w_low": pct_from_52w_low,
        },
    }


def _compute_setup_score(metrics: dict) -> Tuple[float, str]:
    """Compute 0-100 setup quality score. Returns (score, summary_reason)."""
    trend = metrics["trend"]
    momentum = metrics["momentum"]
    volume = metrics["volume"]
    volatility = metrics["volatility"]
    key_levels = metrics["key_levels"]

    score = 50.0  # neutral baseline; positives push up, negatives down
    reason_parts: List[str] = []

    # TREND (35 pts)
    if trend["above_ma_20"]:
        score += 4
    if trend["above_ma_50"]:
        score += 6
    if trend["above_ma_200"]:
        score += 8
        reason_parts.append("above 200d")
    else:
        score -= 12
        reason_parts.append("BELOW 200d")

    if trend["ma_20_slope"] == "rising":
        score += 3
    elif trend["ma_20_slope"] == "falling":
        score -= 3
    if trend["ma_50_slope"] == "rising":
        score += 4
    elif trend["ma_50_slope"] == "falling":
        score -= 4

    if trend["golden_cross_recent"]:
        score += 5
        reason_parts.append("golden cross")
    if trend["death_cross_recent"]:
        score -= 8
        reason_parts.append("DEATH cross")

    # MOMENTUM (25 pts)
    rsi = momentum["rsi_14"]
    if rsi is not None:
        if 40 <= rsi <= 70:
            score += 8
            reason_parts.append(f"RSI {rsi:.0f} healthy")
        elif 30 <= rsi < 40:
            score += 4
            reason_parts.append(f"RSI {rsi:.0f} weak")
        elif 70 < rsi <= 80:
            score += 2
            reason_parts.append(f"RSI {rsi:.0f} overbought")
        elif rsi > 80:
            score -= 8
            reason_parts.append(f"RSI {rsi:.0f} EXTREME")
        elif rsi < 30:
            score -= 4
            reason_parts.append(f"RSI {rsi:.0f} oversold")

    if momentum["macd_above_signal"]:
        score += 5
    if momentum["macd_recent_cross"] == "bullish":
        score += 6
        reason_parts.append("MACD bull cross")
    elif momentum["macd_recent_cross"] == "bearish":
        score -= 6
        reason_parts.append("MACD bear cross")

    # VOLUME (20 pts)
    vr = volume["vol_ratio_20d"]
    if vr is not None:
        if vr >= 1.5:
            score += 8
            reason_parts.append(f"vol {vr:.1f}x")
        elif vr >= 1.0:
            score += 4
        elif vr < 0.5:
            score -= 4

    if volume["obv_trend_30d"] == "rising":
        score += 6
        reason_parts.append("OBV rising")
    elif volume["obv_trend_30d"] == "falling":
        score -= 6

    udvr = volume["up_down_vol_ratio_30d"]
    if udvr is not None and udvr > 1.2:
        score += 4

    # SETUP / VOLATILITY (20 pts)
    bb_pos = volatility["bb_position"]
    if bb_pos is not None:
        if 0.3 <= bb_pos <= 0.7:
            score += 5
            reason_parts.append("BB consol")
        elif bb_pos > 0.95:
            score += 3
            reason_parts.append("BB upper edge")

    pct_high = key_levels["pct_from_52w_high"]
    if pct_high is not None and pct_high >= -5:
        score += 5
        reason_parts.append(f"{abs(pct_high):.1f}% from 52w high")

    score = max(0.0, min(100.0, score))
    summary = ", ".join(reason_parts[:6]) or "no notable signals"
    return round(score, 1), summary


# --- Scanner class ---

class TechnicalOverlayScanner(Scanner):
    name = "technical_overlay"
    description = (
        "Technical setup quality (trend / momentum / volume / volatility) "
        "for every ticker on the watchlist"
    )
    cadence = "intraday"  # intraday refreshes every 15 min, not the daily pipeline

    BARS_FETCH_DAYS = DEFAULT_BARS_FETCH_DAYS

    def run(self, run_date: date) -> ScanResult:
        # Graceful degradation if pandas-ta failed to install. Returns
        # an empty (non-error) result with a clear note — scan pipeline
        # keeps working, dashboard sees zero technicals instead of a
        # blowup.
        if not TA_AVAILABLE:
            log.warning(
                f"pandas-ta-classic not available ({TA_IMPORT_ERROR}); "
                "technical_overlay scanner disabled. Reinstall via "
                "`pip install pandas-ta-classic>=0.4.47`"
            )
            return ScanResult(
                scanner_name=self.name,
                run_date=run_date,
                candidates=pd.DataFrame(columns=["ticker", "score", "reason"]),
                notes=[
                    f"pandas-ta unavailable: {TA_IMPORT_ERROR}",
                    "Scanner disabled (no error). See requirements.txt.",
                ],
            )

        # Phase 8a: read watchlist tickers (loaded fresh each run, so a
        # ticker added via dashboard at minute T appears in the next */15
        # cron fire). Skips conflict / top-N filtering — every entry on
        # the watchlist gets a technical breakdown.
        try:
            entries = read_all_entries()
        except Exception as e:
            log.exception("Failed to read watchlist entries")
            return empty_result(self.name, run_date, error=f"watchlist read: {e}")

        tickers = [e["ticker"] for e in entries if e.get("ticker")]
        log.info(
            f"Watchlist has {len(tickers)} ticker(s): "
            f"{', '.join(tickers) or '(empty)'}"
        )
        if not tickers:
            log.info("Empty watchlist — nothing to analyze")
            return empty_result(self.name, run_date)

        try:
            creds = load_credentials()
        except Exception as e:
            return empty_result(self.name, run_date, error=f"credentials: {e}")

        end = run_date
        start = end - timedelta(days=self.BARS_FETCH_DAYS)

        try:
            # batch_size large enough that the entire watchlist fits in
            # one batch. Watchlists are typically 5-15 tickers per spec.
            bars = fetch_bars(
                tickers, start, end, creds, use_cache=True,
                batch_size=max(50, len(tickers)),
            )
        except Exception as e:
            log.exception("Failed to fetch bars")
            return empty_result(self.name, run_date, error=f"bars fetch: {e}")

        TECHNICAL_DETAIL_DIR.mkdir(parents=True, exist_ok=True)

        rows: List[dict] = []
        for ticker in tickers:
            df = bars.get(ticker)
            if df is None or df.empty:
                log.warning(f"  {ticker}: no bars; skipping")
                continue
            try:
                augmented = _compute_indicators(df)
                metrics = _extract_metrics(augmented, ticker)
            except Exception as e:
                log.warning(f"  {ticker}: indicator compute failed: {e}; skipping")
                continue
            if metrics is None:
                log.warning(
                    f"  {ticker}: insufficient bar history "
                    f"(<{MIN_BARS_FOR_ANALYSIS} rows); skipping"
                )
                continue

            setup_score, reason = _compute_setup_score(metrics)
            metrics["setup_score"] = setup_score
            metrics["reason"] = reason

            # Per-ticker JSON detail file
            detail_path = TECHNICAL_DETAIL_DIR / f"{ticker}.json"
            try:
                detail_path.write_text(
                    json.dumps(metrics, indent=None, default=str),
                    encoding="utf-8",
                )
            except Exception as e:
                log.warning(f"  {ticker}: failed to write detail JSON: {e}")

            # Flat row for the CSV
            rows.append({
                "ticker": ticker,
                "score": setup_score,
                "setup_score": setup_score,
                "last_close": metrics["last_close"],
                "ma_20": metrics["trend"]["ma_20"],
                "ma_50": metrics["trend"]["ma_50"],
                "ma_200": metrics["trend"]["ma_200"],
                "above_ma_20": metrics["trend"]["above_ma_20"],
                "above_ma_50": metrics["trend"]["above_ma_50"],
                "above_ma_200": metrics["trend"]["above_ma_200"],
                "ma_20_slope": metrics["trend"]["ma_20_slope"],
                "ma_50_slope": metrics["trend"]["ma_50_slope"],
                "rsi_14": metrics["momentum"]["rsi_14"],
                "rsi_class": metrics["momentum"]["rsi_class"],
                "macd_above_signal": metrics["momentum"]["macd_above_signal"],
                "macd_recent_cross": metrics["momentum"]["macd_recent_cross"],
                "roc_5d": metrics["momentum"]["roc_5d"],
                "roc_20d": metrics["momentum"]["roc_20d"],
                "vol_ratio_20d": metrics["volume"]["vol_ratio_20d"],
                "obv_trend_30d": metrics["volume"]["obv_trend_30d"],
                "atr_14": metrics["volatility"]["atr_14"],
                "bb_position": metrics["volatility"]["bb_position"],
                "hv_30d_annualized": metrics["volatility"]["hv_30d_annualized"],
                "high_52w": metrics["key_levels"]["high_52w"],
                "low_52w": metrics["key_levels"]["low_52w"],
                "pct_from_52w_high": metrics["key_levels"]["pct_from_52w_high"],
                "pct_from_52w_low": metrics["key_levels"]["pct_from_52w_low"],
                "reason": reason,
            })

        if not rows:
            return empty_result(self.name, run_date)

        df_out = (
            pd.DataFrame(rows)
            .sort_values("setup_score", ascending=False)
            .reset_index(drop=True)
        )
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=df_out,
            notes=[
                "Source: config/watchlist.yaml (all entries)",
                f"Analyzed: {len(rows)} of {len(tickers)} tickers",
                f"Per-ticker detail JSON: {TECHNICAL_DETAIL_DIR}/<TICKER>.json",
            ],
        )


# --- CLI compatibility ---

def backtest_mode(as_of_date: date, output_dir=None) -> int:
    """Phase 4e backtest entry point. Falls through to live run() against
    current watchlist (no historical watchlist snapshot). Outputs to
    <output_dir>/<as_of_date>/technical_overlay.csv (defaults to
    backtest_output/)."""
    output_dir = Path(output_dir) if output_dir else Path("backtest_output")
    scanner = TechnicalOverlayScanner()
    try:
        result = scanner.run(as_of_date)
    except Exception as e:
        log.warning(f"technical_overlay backtest_mode failed for {as_of_date}: {e}")
        return 0
    if result.error or result.candidates.empty:
        return 0

    date_dir = output_dir / as_of_date.isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    out_path = date_dir / "technical_overlay.csv"
    result.candidates.to_csv(out_path, index=False)
    return len(result.candidates)
