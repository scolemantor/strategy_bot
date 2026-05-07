"""Sector rotation scanner.

Surfaces sector ETFs showing positive relative strength vs SPY across
multiple time horizons, with momentum accelerating (1-month pace exceeding
the 3-month average pace).

Sector momentum is one of the more academically durable factor strategies
(Jegadeesh-Titman 1993), but it's also crowded. Late-cycle markets often
see sector rotation tells before the broader index responds.

Source: Alpaca daily bars for 11 SPDR sector ETFs + SPY benchmark.

Output is sector signals, not individual ticker picks. Used to inform
which branches to overweight (Phase 9 enhancement) — for now, surfaces
to the user as research/awareness.

Honest limits:
  - 11-name universe means small sample; relative strength is noisy at this scale
  - Look-back periods are arbitrary calendar days, not perfect lookback windows
  - Doesn't account for factor crowding — when "everyone" notices the rotation,
    the move is often nearly over
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, List, Optional

import pandas as pd

from .base import Scanner, ScanResult, empty_result

log = logging.getLogger(__name__)

# 11 SPDR sector ETFs + benchmark
SECTOR_ETFS = {
    "XLF":  "Financials",
    "XLK":  "Technology",
    "XLE":  "Energy",
    "XLV":  "Healthcare",
    "XLI":  "Industrials",
    "XLP":  "Consumer Staples",
    "XLY":  "Consumer Discretionary",
    "XLU":  "Utilities",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLC":  "Communications",
}
BENCHMARK = "SPY"


class SectorRotationScanner(Scanner):
    name = "sector_rotation"
    description = "Sector ETF relative strength vs SPY at 1m/3m/6m horizons"
    cadence = "weekly"  # Sector signals don't require daily granularity

    # Lookback windows in calendar days (approximate trading-day equivalents)
    LOOKBACK_1M_DAYS = 30
    LOOKBACK_3M_DAYS = 90
    LOOKBACK_6M_DAYS = 180

    # Min trading days required for each lookback (sanity check)
    MIN_BARS_1M = 18
    MIN_BARS_3M = 55
    MIN_BARS_6M = 110

    # Filter thresholds
    MIN_RS_1M = 0.0  # Sector must be outperforming SPY over last month
    MIN_RS_3M = 0.0  # AND over last quarter

    def run(self, run_date: date) -> ScanResult:
        log.info(f"Universe: {len(SECTOR_ETFS)} sector ETFs vs {BENCHMARK}")
        log.info(f"Lookbacks: 1m, 3m, 6m")

        # Fetch bars for all 12 symbols (11 sectors + benchmark)
        symbols = list(SECTOR_ETFS.keys()) + [BENCHMARK]

        try:
            from src.data import fetch_bars
            from src.config import load_credentials

            creds = load_credentials()
            # Fetch 200 calendar days to ensure we have enough trading days for 6-month lookback
            start = run_date - timedelta(days=200)
            end = run_date
            bars_dict = fetch_bars(
                symbols=symbols,
                start=start,
                end=end,
                creds=creds,
                use_cache=True,
            )
        except Exception as e:
            log.exception("Failed to fetch sector bars")
            return empty_result(self.name, run_date, error=f"bars: {e}")

        # Validate we have benchmark
        spy_bars = bars_dict.get(BENCHMARK)
        if spy_bars is None or spy_bars.empty:
            log.error(f"No bars for benchmark {BENCHMARK}")
            return empty_result(self.name, run_date, error=f"no bars for {BENCHMARK}")

        spy_bars = spy_bars.sort_index()
        spy_returns = self._compute_returns(spy_bars)
        if spy_returns is None:
            log.error(f"Could not compute returns for {BENCHMARK}")
            return empty_result(self.name, run_date, error=f"benchmark returns failed")

        log.info(
            f"Benchmark {BENCHMARK} returns: "
            f"1m={spy_returns['1m']*100:+.2f}%, "
            f"3m={spy_returns['3m']*100:+.2f}%, "
            f"6m={spy_returns['6m']*100:+.2f}%"
        )

        # Compute relative strength for each sector
        rows: List[Dict] = []
        for etf, sector_name in SECTOR_ETFS.items():
            sector_bars = bars_dict.get(etf)
            if sector_bars is None or sector_bars.empty:
                log.warning(f"  No bars for {etf} ({sector_name}); skipping")
                continue

            sector_bars = sector_bars.sort_index()
            sector_returns = self._compute_returns(sector_bars)
            if sector_returns is None:
                log.warning(f"  Could not compute returns for {etf}; skipping")
                continue

            rs_1m = sector_returns["1m"] - spy_returns["1m"]
            rs_3m = sector_returns["3m"] - spy_returns["3m"]
            rs_6m = sector_returns["6m"] - spy_returns["6m"]

            # Acceleration: monthly pace > quarterly average pace
            # If 1m RS / 1 > 3m RS / 3, sector is accelerating relative to SPY
            monthly_pace_1m = rs_1m
            monthly_pace_3m = rs_3m / 3 if rs_3m else 0
            accelerating = monthly_pace_1m > monthly_pace_3m

            log.info(
                f"  {etf} ({sector_name[:20]:<20}): "
                f"ret 1m={sector_returns['1m']*100:+.2f}% 3m={sector_returns['3m']*100:+.2f}% "
                f"| RS 1m={rs_1m*100:+.2f}% 3m={rs_3m*100:+.2f}% 6m={rs_6m*100:+.2f}% "
                f"| accel={accelerating}"
            )

            rows.append({
                "ticker": etf,
                "sector_name": sector_name,
                "ret_1m": sector_returns["1m"] * 100,
                "ret_3m": sector_returns["3m"] * 100,
                "ret_6m": sector_returns["6m"] * 100,
                "rs_1m": rs_1m * 100,
                "rs_3m": rs_3m * 100,
                "rs_6m": rs_6m * 100,
                "accelerating": accelerating,
                "rs_1m_raw": rs_1m,  # for filter logic
                "rs_3m_raw": rs_3m,
            })

        if not rows:
            return empty_result(self.name, run_date, error="No sector returns computed")

        df = pd.DataFrame(rows)

        # Filter: positive 1m AND 3m relative strength (rotating INTO this sector)
        before = len(df)
        df = df[(df["rs_1m_raw"] > self.MIN_RS_1M) & (df["rs_3m_raw"] > self.MIN_RS_3M)].copy()
        log.info(f"RS filter (1m and 3m both positive): {before} -> {len(df)}")

        if df.empty:
            return empty_result(self.name, run_date)

        # Score: composite of 1m + 3m RS + accelerating bonus
        # NOTE: 6m intentionally dropped from scoring — Alpaca returns
        # unadjusted prices for some sector ETFs, producing nonsensical
        # 6-month returns when distributions/splits occurred in the window.
        # 6m values are still in the output for diagnostic visibility but
        # should not be acted on without cross-checking.
        df["score"] = (
            df["rs_1m"].clip(0, 30) * 1.5 +   # 1m weighted highest (most recent)
            df["rs_3m"].clip(0, 30) * 1.0 +
            df["accelerating"].astype(int) * 10  # bonus for accelerating
        )

        # Build clean output
        out_rows = []
        for _, r in df.sort_values("score", ascending=False).iterrows():
            out_rows.append({
                "ticker": r["ticker"],
                "sector_name": r["sector_name"],
                "ret_1m_pct": round(float(r["ret_1m"]), 2),
                "ret_3m_pct": round(float(r["ret_3m"]), 2),
                "ret_6m_pct": round(float(r["ret_6m"]), 2),
                "rs_1m_pct": round(float(r["rs_1m"]), 2),
                "rs_3m_pct": round(float(r["rs_3m"]), 2),
                "rs_6m_pct": round(float(r["rs_6m"]), 2),
                "accelerating": bool(r["accelerating"]),
                "score": round(float(r["score"]), 2),
                "reason": (
                    f"{r['sector_name']}: RS 1m {r['rs_1m']:+.1f}%, 3m {r['rs_3m']:+.1f}%"
                    + (", accelerating" if r["accelerating"] else "")
                    + f" (sector +{r['ret_1m']:.1f}%/1m vs SPY +{spy_returns['1m']*100:.1f}%/1m)"
                ),
            })

        out_df = pd.DataFrame(out_rows)
        return ScanResult(
            scanner_name=self.name,
            run_date=run_date,
            candidates=out_df,
            notes=[
                f"Universe: 11 SPDR sector ETFs vs {BENCHMARK}",
                f"Lookbacks: 1m / 3m / 6m",
                f"Filter: positive RS at both 1m AND 3m vs SPY",
                f"Benchmark SPY: 1m {spy_returns['1m']*100:+.2f}%, 3m {spy_returns['3m']*100:+.2f}%, 6m {spy_returns['6m']*100:+.2f}%",
                f"Sectors passing filter: {len(out_df)}/{len(SECTOR_ETFS)}",
                "Use case: informs which branches to overweight (Phase 9 overlay)",
            ],
        )

    def _compute_returns(self, bars: pd.DataFrame) -> Optional[Dict[str, float]]:
        """Compute 1m, 3m, 6m returns from a DataFrame of bars.

        Uses approximate trading-day counts: ~21 days/month.
        Returns None if insufficient data.
        """
        if bars is None or bars.empty:
            return None

        closes = bars["close"]
        if len(closes) < self.MIN_BARS_1M:
            return None

        last = float(closes.iloc[-1])

        # Approximate trading-day lookback indices
        idx_1m = -22 if len(closes) >= 22 else 0
        idx_3m = -64 if len(closes) >= 64 else 0
        idx_6m = -127 if len(closes) >= 127 else 0

        ref_1m = float(closes.iloc[idx_1m])
        ref_3m = float(closes.iloc[idx_3m])
        ref_6m = float(closes.iloc[idx_6m])

        if ref_1m <= 0 or ref_3m <= 0 or ref_6m <= 0:
            return None

        return {
            "1m": (last - ref_1m) / ref_1m,
            "3m": (last - ref_3m) / ref_3m,
            "6m": (last - ref_6m) / ref_6m,
        }
# --- Phase 4e backtest support ---

def backtest_mode(as_of_date: date, output_dir=None) -> int:
    """Run sector_rotation scanner as-of a historical date.

    Pure price/return data on 12 ETFs (11 sectors + SPY). The live scanner is
    already date-aware (start=run_date-200d, end=run_date). Bar cache handles
    historical data. No look-ahead concerns.

    Output goes to <output_dir>/<as_of_date>/sector_rotation.csv.
    """
    from pathlib import Path

    output_dir = Path(output_dir) if output_dir else Path("backtest_output")
    scanner = SectorRotationScanner()

    try:
        result = scanner.run(as_of_date)
    except Exception as e:
        log.warning(f"sector_rotation backtest_mode failed for {as_of_date}: {e}")
        return 0

    if result.error or result.candidates.empty:
        return 0

    date_dir = output_dir / as_of_date.isoformat()
    date_dir.mkdir(parents=True, exist_ok=True)
    out_path = date_dir / "sector_rotation.csv"
    result.candidates.to_csv(out_path, index=False)
    log.debug(f"  sector_rotation {as_of_date}: wrote {len(result.candidates)} candidates to {out_path}")

    return len(result.candidates)