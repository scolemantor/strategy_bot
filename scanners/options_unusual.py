"""Unusual options flow scanner #14 (Phase 4g.1).

First paid-data scanner. Reads Unusual Whales global flow-alerts feed,
filters/groups by ticker, scores on premium + alert count + directional
skew + strike proximity + volume/OI ratio, then emits the bullish
(call-skewed) tickers as candidates and the rest to a rejected CSV for
transparency.

Direction handling: this scanner is registered in scanner_weights.yaml
as direction=bullish, mirroring the insider_buying / insider_selling_clusters
split. Put-skewed flow gets its own scanner in Phase 4g.1c
(options_unusual_puts) — same client, same scoring, inverted filter.

Score components (0-200 scale, typical 30-130 range):
  Premium       80 max  log10 of total_premium per ticker
  Alert count   40 max  log2 of alert count
  Direction     30 max  |call_pct - 0.5| * 60
  Strike prox   30 max  weighted avg distance from spot (ATM = full)
  Vol/OI        20 max  weighted avg vol/OI ratio (>1 = new positions)

Failure handling: missing token -> empty result with explanatory note,
NEVER raises. UW 401/5xx -> empty result + error logged. Other scanners
in scan_all chain unaffected.
"""
from __future__ import annotations

import logging
import math
import os
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.unusual_whales_client import (
    API_TOKEN_ENV,
    get_flow_alerts,
)

from .base import Scanner, ScanResult, empty_result

log = logging.getLogger(__name__)

DEFAULT_MIN_PREMIUM = 100_000
DEFAULT_LOOKBACK_HOURS = 24
# Call-skew threshold for the main CSV. Below this, the row goes to
# the rejected CSV (mixed or put-skewed). Sean's spec: 0.60 captures
# meaningful directional plays without requiring near-pure one-sided
# flow (which is rare in practice).
BULLISH_CALL_SKEW_THRESHOLD = 0.60


class OptionsUnusualScanner(Scanner):
    name = "options_unusual"
    description = "Unusual options flow above premium threshold (UW data)"
    cadence = "daily"
    requires_paid_data = True

    def __init__(
        self,
        min_premium: Optional[int] = None,
        lookback_hours: Optional[int] = None,
    ):
        super().__init__()
        self.min_premium = min_premium if min_premium is not None else int(
            os.environ.get("UW_MIN_PREMIUM", DEFAULT_MIN_PREMIUM)
        )
        self.lookback_hours = lookback_hours if lookback_hours is not None else int(
            os.environ.get("UW_LOOKBACK_HOURS", DEFAULT_LOOKBACK_HOURS)
        )

    def run(self, run_date: date) -> ScanResult:
        log.info(
            f"options_unusual: min_premium=${self.min_premium:,} "
            f"lookback={self.lookback_hours}h"
        )

        # Token presence check before any HTTP. Mirrors congressional_trades
        # pattern — return empty cleanly so scan_all chain isn't broken
        # by a missing-credential setup error.
        if not os.environ.get(API_TOKEN_ENV):
            log.warning(
                f"{API_TOKEN_ENV} env var not set; options_unusual scanner "
                f"is disabled. Sign up at https://unusualwhales.com/api "
                f"and add the key to .env."
            )
            return ScanResult(
                scanner_name=self.name,
                run_date=run_date,
                candidates=pd.DataFrame(columns=["ticker", "score", "reason"]),
                notes=[f"{API_TOKEN_ENV} not configured; scanner disabled (no error)."],
            )

        try:
            raw_alerts = get_flow_alerts(
                limit=1000,  # large limit; client/server filter min_premium
                min_premium=self.min_premium,
                lookback_hours=self.lookback_hours,
                run_date=run_date,
            )
        except Exception as e:
            log.exception("Failed to fetch UW flow-alerts")
            return empty_result(self.name, run_date, error=f"UW flow-alerts: {e}")

        log.info(f"UW returned {len(raw_alerts)} raw alerts")

        # Parse + dedupe + drop expired + drop sub-threshold alerts
        parsed: List[dict] = []
        seen_ids: set = set()
        dropped_expired = 0
        dropped_sub_premium = 0
        dropped_no_ticker = 0
        for raw in raw_alerts:
            a = _parse_alert(raw)
            if a is None:
                dropped_no_ticker += 1
                continue
            tid = a.get("trade_id")
            if tid and tid in seen_ids:
                continue
            if tid:
                seen_ids.add(tid)
            if a["premium"] < self.min_premium:
                dropped_sub_premium += 1
                continue
            if a["expiry"] is not None and a["expiry"] < run_date:
                dropped_expired += 1
                continue
            parsed.append(a)
        log.info(
            f"Parsed: {len(parsed)} alerts "
            f"(dropped {dropped_no_ticker} no-ticker, "
            f"{dropped_sub_premium} sub-premium, "
            f"{dropped_expired} expired)"
        )

        if not parsed:
            return empty_result(self.name, run_date)

        # Group by ticker
        by_ticker: Dict[str, List[dict]] = defaultdict(list)
        for a in parsed:
            by_ticker[a["ticker"].upper()].append(a)
        log.info(f"Grouped into {len(by_ticker)} unique tickers")

        # Score each ticker; partition into bullish (main) vs everything else (rejected)
        bullish_rows: List[dict] = []
        rejected_rows: List[dict] = []
        for ticker, alerts in by_ticker.items():
            row = _score_ticker(ticker, alerts)
            if row["call_premium_pct"] >= BULLISH_CALL_SKEW_THRESHOLD:
                bullish_rows.append(row)
            else:
                row["rejection_reason"] = (
                    f"call_premium_pct={row['call_premium_pct']:.2f} below "
                    f"bullish threshold {BULLISH_CALL_SKEW_THRESHOLD}"
                )
                rejected_rows.append(row)

        log.info(
            f"Scoring distribution: {len(bullish_rows)} bullish "
            f"(call_skew >= {BULLISH_CALL_SKEW_THRESHOLD:.0%}), "
            f"{len(rejected_rows)} rejected (mixed/bearish)"
        )

        if bullish_rows:
            top = sorted(bullish_rows, key=lambda r: r["score"], reverse=True)[:5]
            log.info("  Top 5 bullish: " + ", ".join(
                f"{r['ticker']}:{r['score']:.0f}" for r in top
            ))

        if not bullish_rows and not rejected_rows:
            return empty_result(self.name, run_date)

        candidates_df = (
            pd.DataFrame(bullish_rows).sort_values("score", ascending=False).reset_index(drop=True)
            if bullish_rows
            else pd.DataFrame(columns=_OUTPUT_COLUMNS)
        )
        rejected_df = (
            pd.DataFrame(rejected_rows).sort_values("score", ascending=False).reset_index(drop=True)
            if rejected_rows
            else None
        )

        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=candidates_df,
            rejected_candidates=rejected_df,
            notes=[
                f"Min premium: ${self.min_premium:,}",
                f"Lookback: {self.lookback_hours}h",
                f"Raw alerts from UW: {len(raw_alerts)}",
                f"Parsed alerts: {len(parsed)} "
                f"(dropped {dropped_expired} expired, "
                f"{dropped_sub_premium} sub-premium, "
                f"{dropped_no_ticker} no-ticker)",
                f"Tickers: {len(by_ticker)} "
                f"({len(bullish_rows)} bullish in main, "
                f"{len(rejected_rows)} mixed/bearish in rejected)",
                f"Bullish call-skew threshold: {BULLISH_CALL_SKEW_THRESHOLD:.0%}",
            ],
        )


_OUTPUT_COLUMNS = [
    "ticker", "total_premium", "call_premium", "put_premium",
    "alert_count", "put_call_ratio", "directional_bias", "avg_dte",
    "score", "reason",
]


def _parse_alert(raw: dict) -> Optional[dict]:
    """Normalize one UW flow-alert dict into the scanner's internal shape.
    Returns None if the alert lacks the bare-minimum fields (ticker +
    premium + option_type)."""
    ticker = raw.get("ticker") or raw.get("underlying") or raw.get("symbol")
    if not ticker:
        return None

    # Option type — UW uses "call"/"put" but sometimes capitalized
    raw_type = (raw.get("option_type") or raw.get("type") or "").lower()
    if raw_type not in ("call", "put"):
        # Try parsing OCC option_symbol if direct field absent
        sym = raw.get("option_symbol") or raw.get("option_chain") or ""
        if isinstance(sym, str) and len(sym) >= 15:
            # OCC: <ticker><yymmdd><C|P><strike>
            for c in sym:
                if c in ("C", "P"):
                    raw_type = "call" if c == "C" else "put"
                    break
        if raw_type not in ("call", "put"):
            return None

    premium = _to_float(raw.get("premium")) or _to_float(raw.get("total_premium"))
    if premium is None:
        return None

    strike = _to_float(raw.get("strike"))
    spot = (
        _to_float(raw.get("underlying_price"))
        or _to_float(raw.get("spot"))
        or _to_float(raw.get("underlying_spot"))
    )
    volume = _to_float(raw.get("volume"))
    oi = _to_float(raw.get("open_interest")) or _to_float(raw.get("oi"))

    expiry = _to_date(raw.get("expiry") or raw.get("expires") or raw.get("expiration"))

    trade_id = raw.get("trade_id") or raw.get("id") or raw.get("alert_id")

    return {
        "ticker": str(ticker).upper(),
        "type": raw_type,
        "premium": premium,
        "strike": strike,
        "spot": spot,
        "volume": volume,
        "open_interest": oi,
        "expiry": expiry,
        "trade_id": str(trade_id) if trade_id else None,
    }


def _to_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _to_date(v) -> Optional[date]:
    if v is None or v == "":
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.strptime(v[:10], fmt).date()
            except ValueError:
                continue
        # Try ISO with time component
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).date()
        except (ValueError, AttributeError):
            return None
    return None


def _score_ticker(ticker: str, alerts: List[dict]) -> dict:
    """Compute the per-ticker score + descriptive fields. 0-200 scale.

    Components:
      Premium    80 max  log10(total / 10_000) * 20, capped at 80
      Alerts     40 max  log2(count) * 10, capped at 40
      Direction  30 max  |call_pct - 0.5| * 60
      Strike     30 max  weighted-avg ATM proximity (full at-the-money)
      Vol/OI     20 max  weighted-avg vol/OI ratio
    """
    call_premium = sum(a["premium"] for a in alerts if a["type"] == "call")
    put_premium = sum(a["premium"] for a in alerts if a["type"] == "put")
    total_premium = call_premium + put_premium
    alert_count = len(alerts)
    call_pct = (call_premium / total_premium) if total_premium > 0 else 0.5

    # Premium component (80 max)
    # $10k -> 0, $100k -> 20, $1M -> 40, $10M -> 60, $100M -> 80
    if total_premium > 10_000:
        premium_pts = min(80.0, math.log10(total_premium / 10_000) * 20)
    else:
        premium_pts = 0.0

    # Alert count component (40 max)
    # 1 -> 0, 2 -> 10, 4 -> 20, 8 -> 30, 16+ -> 40
    if alert_count >= 1:
        alert_pts = min(40.0, math.log2(alert_count) * 10)
    else:
        alert_pts = 0.0

    # Direction component (30 max) — pure one-sided gets full points
    direction_pts = abs(call_pct - 0.5) * 60

    # Strike proximity component (30 max) — premium-weighted distance from spot
    prox_num = 0.0
    prox_den = 0.0
    for a in alerts:
        if a["spot"] is None or a["strike"] is None or a["spot"] <= 0:
            continue
        dist_pct = abs(a["strike"] - a["spot"]) / a["spot"]
        # Bucketed to keep scoring stable for far-OTM lottery tickets
        if dist_pct <= 0.025:
            pt = 30.0
        elif dist_pct <= 0.05:
            pt = 24.0
        elif dist_pct <= 0.10:
            pt = 18.0
        elif dist_pct <= 0.20:
            pt = 9.0
        else:
            pt = 0.0
        prox_num += pt * a["premium"]
        prox_den += a["premium"]
    strike_pts = (prox_num / prox_den) if prox_den > 0 else 0.0

    # Vol/OI component (20 max) — premium-weighted avg ratio
    voi_num = 0.0
    voi_den = 0.0
    for a in alerts:
        if a["volume"] is None or a["open_interest"] is None or a["open_interest"] <= 0:
            continue
        ratio = a["volume"] / a["open_interest"]
        if ratio >= 1.0:
            pt = 20.0
        elif ratio >= 0.5:
            pt = 14.0
        elif ratio >= 0.2:
            pt = 8.0
        else:
            pt = 0.0
        voi_num += pt * a["premium"]
        voi_den += a["premium"]
    voi_pts = (voi_num / voi_den) if voi_den > 0 else 0.0

    score = premium_pts + alert_pts + direction_pts + strike_pts + voi_pts

    # Avg DTE (premium-weighted) for downstream readers
    dte_num = 0.0
    dte_den = 0.0
    today = date.today()
    for a in alerts:
        if a["expiry"] is None:
            continue
        days = (a["expiry"] - today).days
        if days < 0:
            continue
        dte_num += days * a["premium"]
        dte_den += a["premium"]
    avg_dte = round(dte_num / dte_den) if dte_den > 0 else None

    if call_pct >= BULLISH_CALL_SKEW_THRESHOLD:
        directional_bias = "bullish"
    elif (1 - call_pct) >= BULLISH_CALL_SKEW_THRESHOLD:
        directional_bias = "bearish"
    else:
        directional_bias = "mixed"

    pcr = (put_premium / call_premium) if call_premium > 0 else None

    reason = (
        f"{alert_count} alert{'s' if alert_count != 1 else ''} "
        f"${total_premium / 1e6:.1f}M total, "
        f"{call_pct * 100:.0f}% calls"
    )
    if avg_dte is not None:
        reason += f", avg DTE {avg_dte}"

    return {
        "ticker": ticker,
        "total_premium": round(total_premium, 2),
        "call_premium": round(call_premium, 2),
        "put_premium": round(put_premium, 2),
        "call_premium_pct": round(call_pct, 4),
        "alert_count": alert_count,
        "put_call_ratio": round(pcr, 3) if pcr is not None else None,
        "directional_bias": directional_bias,
        "avg_dte": avg_dte,
        "score": round(score, 2),
        "reason": reason,
    }
