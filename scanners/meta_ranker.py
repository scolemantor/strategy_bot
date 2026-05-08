"""Cross-scanner meta-ranker (Phase 4c).

Aggregates all 13 scanner outputs from a single date into a master ranked
DataFrame keyed on ticker. Surfaces multi-scanner conviction (a ticker hit
by 3 scanners is stronger than 1) with bonus scoring for cross-validation
and category diversity.

Detects conflicts when a ticker shows up in both bullish AND bearish
scanners (e.g. insider_buying [#1] AND insider_selling_clusters [#13]).
Conflicts get flagged separately — they're not "stronger signals", they're
"interesting situations needing human review."

Three categories of output:
  1. master_ranked.csv      — top tickers ranked by composite score
  2. conflicts.csv          — tickers with bullish + bearish signals
  3. category_summary.csv   — per-scanner breakdown showing how each scanner
                              contributed to the master output

Composite score formula:
  normalized_score = scanner_raw_score / scanner_p90_score
  weighted_score   = normalized_score * scanner_weight
  raw              = sum(weighted_score for each scanner that hit)
                     [conflict tickers: bullish_sum - bearish_sum + neutral_sum]
  multi_bonus      = lookup(num_scanners) from config (1.0 / 1.5 / 2.0 / 2.5 / 3.0)
  diversity_bonus  = lookup(num_categories) from config (1.0 / 1.2 / 1.4 / 1.5)
  composite_score  = raw * multi_bonus * diversity_bonus

Three Phase-4c-v2 design choices:
  Fix 1: Normalize per-scanner scores against their 90th percentile so
         multi-scanner cross-validation outweighs raw absolute magnitude
         from any single scanner.
  Fix 2: Same-scanner duplicate hits (e.g. ticker appears twice in
         thirteen_f_changes from two different funds) are aggregated
         into a single hit with summed weighted score, but counted as
         1 scanner for the multi-scanner bonus.
  Fix 3: Earnings_calendar contribution requires another scanner to
         also hit the ticker — by itself it's just calendar noise that
         would flood the universe with hundreds of mostly-uninteresting names.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

log = logging.getLogger(__name__)

CONFIG_DIR = Path("config")
SCANNER_WEIGHTS_PATH = CONFIG_DIR / "scanner_weights.yaml"

EARNINGS_CALENDAR_NAME = "earnings_calendar"

# Sentinels emitted by scanners when a CIK->ticker lookup fails (most often
# spinoff_tracker, when SEC's mapping doesn't yet include the spun-off
# entity). These rows are real filings but unactionable — filtered out of
# master_ranked, while the underlying scanner CSV still contains them.
INVALID_TICKERS = frozenset({"?", "", "NAN", "NONE", "<NA>"})


@dataclass
class ScannerHit:
    """A single scanner's contribution to a ticker's composite score."""
    scanner_name: str
    direction: str  # bullish / bearish / neutral
    category: str   # conviction / technical / event / value
    weight: float
    raw_score: float
    weighted_score: float
    reason: str


@dataclass
class TickerAggregate:
    """All scanner hits for a single ticker, plus computed composite score."""
    ticker: str
    hits: List[ScannerHit] = field(default_factory=list)
    composite_score: float = 0.0
    is_conflict: bool = False

    def scanner_names(self) -> List[str]:
        return sorted([h.scanner_name for h in self.hits])

    def categories(self) -> List[str]:
        return sorted(set(h.category for h in self.hits))

    def directions(self) -> List[str]:
        return sorted(set(h.direction for h in self.hits))

    def has_bullish(self) -> bool:
        return any(h.direction == "bullish" for h in self.hits)

    def has_bearish(self) -> bool:
        return any(h.direction == "bearish" for h in self.hits)


def _load_config() -> Dict:
    """Load scanner weights config. Raises if missing."""
    if not SCANNER_WEIGHTS_PATH.exists():
        raise FileNotFoundError(
            f"Scanner weights config not found at {SCANNER_WEIGHTS_PATH}. "
            f"Run 'notepad config/scanner_weights.yaml' to create it."
        )
    try:
        import yaml
        return yaml.safe_load(SCANNER_WEIGHTS_PATH.read_text())
    except ImportError:
        raise ImportError("PyYAML required. Run: pip install pyyaml")


def _load_scanner_csv(date_dir: Path, scanner_name: str) -> Optional[pd.DataFrame]:
    """Load a scanner's CSV output for a specific date. None if missing/empty."""
    p = date_dir / f"{scanner_name}.csv"
    if not p.exists():
        log.debug(f"  No output file for {scanner_name} at {p}")
        return None
    try:
        df = pd.read_csv(p)
        if df.empty or "ticker" not in df.columns:
            return None
        return df
    except Exception as e:
        log.warning(f"  Failed to read {p}: {e}")
        return None


def aggregate(
    run_date: date, output_dir: Path = Path("scan_output")
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run the meta-ranker for a given date.

    Returns (master_ranked_df, conflicts_df, category_summary_df).
    Also writes them to disk in <output_dir>/<run_date>/.
    """
    log.info(f"Meta-ranker for {run_date}")
    config = _load_config()

    multi_bonus_table = config.get("multi_scanner_bonus", {})
    diversity_bonus_table = config.get("category_diversity_bonus", {})
    scanner_configs = config.get("scanners", {})

    date_dir = output_dir / run_date.isoformat()
    if not date_dir.exists():
        log.warning(f"  No output directory for {run_date} at {date_dir}")
        empty = pd.DataFrame()
        return empty, empty, empty

    # Step 1: Load each scanner's output.
    # raw_hits[ticker][scanner_name] = (sc_cfg, raw_score_sum, reasons, n_intra_hits)
    raw_hits: Dict[str, Dict[str, Tuple[Dict, float, str, int]]] = defaultdict(dict)
    scanner_summary: List[Dict] = []
    scanner_norm_factor: Dict[str, float] = {}

    for scanner_name, sc_cfg in scanner_configs.items():
        weight = float(sc_cfg.get("weight", 1.0))
        if weight == 0.0:
            log.info(f"  {scanner_name}: weight=0, excluded")
            continue

        df = _load_scanner_csv(date_dir, scanner_name)
        if df is None:
            scanner_summary.append({
                "scanner": scanner_name,
                "direction": sc_cfg.get("direction", "?"),
                "category": sc_cfg.get("category", "?"),
                "weight": weight,
                "candidates": 0,
                "contributed_to_master": 0,
            })
            continue

        if "score" not in df.columns:
            log.warning(f"  {scanner_name} has no 'score' column; skipping")
            continue

        # Drop rows with unmapped/empty tickers before they pollute the
        # aggregate. spinoff_tracker can emit ticker="?" when SEC's
        # CIK->ticker map lags the filing. fillna("") routes NaN/None
        # through the same isin() check (NaN survives astype(str) in
        # recent pandas).
        mask_invalid = (
            df["ticker"].fillna("").astype(str).str.strip().str.upper().isin(INVALID_TICKERS)
        )
        n_dropped_unmapped = int(mask_invalid.sum())
        if n_dropped_unmapped > 0:
            df = df[~mask_invalid].copy()
        if df.empty:
            log.info(f"  {scanner_name}: 0 valid tickers after unmapped filter; skipping")
            continue

        # Fix 1: compute 90th-percentile score for normalization
        scores = pd.to_numeric(df["score"], errors="coerce").dropna()
        if scores.empty:
            log.warning(f"  {scanner_name} has no valid scores; skipping")
            continue
        norm_factor = float(scores.quantile(0.9)) if len(scores) >= 5 else float(scores.max())
        if norm_factor <= 0:
            norm_factor = 1.0
        scanner_norm_factor[scanner_name] = norm_factor

        log.info(
            f"  {scanner_name}: {len(df)} candidates "
            f"(weight {weight}, {sc_cfg.get('direction')}/{sc_cfg.get('category')}, "
            f"p90 norm={norm_factor:.1f}, unmapped_dropped={n_dropped_unmapped})"
        )

        # Fix 2: same-scanner duplicates collapse via groupby
        for ticker_raw, group in df.groupby("ticker"):
            ticker = str(ticker_raw).strip().upper()
            if not ticker:
                continue

            try:
                raw_score_sum = float(pd.to_numeric(group["score"], errors="coerce").sum())
            except Exception:
                continue

            if "reason" in group.columns:
                reasons = " ; ".join(str(r)[:60] for r in group["reason"].dropna().head(3))
            else:
                reasons = ""
            reasons = reasons[:180]

            n_intra = len(group)
            raw_hits[ticker][scanner_name] = (sc_cfg, raw_score_sum, reasons, n_intra)

        scanner_summary.append({
            "scanner": scanner_name,
            "direction": sc_cfg.get("direction", "?"),
            "category": sc_cfg.get("category", "?"),
            "weight": weight,
            "candidates": len(df),
            "contributed_to_master": 0,  # filled later
        })

    if not raw_hits:
        log.info("  No tickers aggregated; meta-ranker output empty")
        empty = pd.DataFrame()
        return empty, empty, empty

    # Fix 3: drop earnings_calendar-only tickers (not multi-scanner)
    drops = [t for t, sc in raw_hits.items() if list(sc.keys()) == [EARNINGS_CALENDAR_NAME]]
    for t in drops:
        del raw_hits[t]
    log.info(f"  Filtered {len(drops)} earnings_calendar-only tickers (FIX 3)")
    log.info(f"  Aggregated {len(raw_hits)} unique tickers across all scanners (post-filter)")

    # Step 2: build TickerAggregate objects with normalized scores (Fix 1)
    aggregates: Dict[str, TickerAggregate] = {}
    for ticker, scanners_dict in raw_hits.items():
        agg = TickerAggregate(ticker=ticker)
        for scanner_name, (sc_cfg, raw_score_sum, reasons, n_intra) in scanners_dict.items():
            weight = float(sc_cfg.get("weight", 1.0))
            norm_factor = scanner_norm_factor.get(scanner_name, 1.0)

            # Fix 1: normalize against scanner's own p90 score
            normalized_score = raw_score_sum / norm_factor
            weighted_score = normalized_score * weight

            note_suffix = f" ({n_intra} hits)" if n_intra > 1 else ""
            hit = ScannerHit(
                scanner_name=scanner_name,
                direction=sc_cfg.get("direction", "neutral"),
                category=sc_cfg.get("category", "other"),
                weight=weight,
                raw_score=raw_score_sum,
                weighted_score=weighted_score,
                reason=(reasons + note_suffix)[:180],
            )
            agg.hits.append(hit)
        aggregates[ticker] = agg

    # Step 3: composite scores and conflict detection
    for agg in aggregates.values():
        agg.is_conflict = agg.has_bullish() and agg.has_bearish()

        if agg.is_conflict:
            bullish_sum = sum(h.weighted_score for h in agg.hits if h.direction == "bullish")
            bearish_sum = sum(h.weighted_score for h in agg.hits if h.direction == "bearish")
            neutral_sum = sum(h.weighted_score for h in agg.hits if h.direction == "neutral")
            raw = bullish_sum - bearish_sum + neutral_sum
        else:
            raw = sum(h.weighted_score for h in agg.hits)

        n_scanners = len(agg.hits)
        n_categories = len(agg.categories())

        multi_mult = float(multi_bonus_table.get(min(n_scanners, 5), 1.0))
        div_mult = float(diversity_bonus_table.get(min(n_categories, 4), 1.0))

        agg.composite_score = raw * multi_mult * div_mult

    # Step 4: build output DataFrames
    master_rows: List[Dict] = []
    conflict_rows: List[Dict] = []

    for agg in aggregates.values():
        scanner_list = ", ".join(agg.scanner_names())
        category_list = ", ".join(agg.categories())
        direction_list = ", ".join(agg.directions())

        reasons_compact = " | ".join(
            f"[{h.scanner_name}] {h.reason[:60]}"
            for h in sorted(agg.hits, key=lambda x: abs(x.weighted_score), reverse=True)[:4]
        )

        row = {
            "ticker": agg.ticker,
            "composite_score": round(agg.composite_score, 3),
            "n_scanners": len(agg.hits),
            "n_categories": len(agg.categories()),
            "directions": direction_list,
            "scanners_hit": scanner_list,
            "categories_hit": category_list,
            "is_conflict": agg.is_conflict,
            "reasons": reasons_compact,
        }
        if agg.is_conflict:
            conflict_rows.append(row)
        master_rows.append(row)

    master_df = (
        pd.DataFrame(master_rows)
        .sort_values("composite_score", ascending=False)
        .reset_index(drop=True)
    )
    conflicts_df = (
        pd.DataFrame(conflict_rows)
        .sort_values("composite_score", ascending=False)
        .reset_index(drop=True)
        if conflict_rows
        else pd.DataFrame()
    )

    # Update contributed_to_master counts now that filters applied
    for s in scanner_summary:
        scanner_name = s["scanner"]
        s["contributed_to_master"] = sum(
            1 for agg in aggregates.values()
            if any(h.scanner_name == scanner_name for h in agg.hits)
        )
    summary_df = pd.DataFrame(scanner_summary)

    # Step 5: write to disk
    date_dir.mkdir(parents=True, exist_ok=True)

    master_path = date_dir / "master_ranked.csv"
    master_df.to_csv(master_path, index=False)
    log.info(f"  Wrote {master_path} ({len(master_df)} tickers)")

    if not conflicts_df.empty:
        conflicts_path = date_dir / "conflicts.csv"
        conflicts_df.to_csv(conflicts_path, index=False)
        log.info(f"  Wrote {conflicts_path} ({len(conflicts_df)} conflicts)")

    summary_path = date_dir / "category_summary.csv"
    summary_df.to_csv(summary_path, index=False)
    log.info(f"  Wrote {summary_path}")

    _emit_alerts(run_date, scanner_summary, master_df, conflicts_df)

    # Phase 7.5 commit 3: per-ticker reverse index for dashboard ticker-detail
    # page. One JSON file per ticker under data_cache/ticker_index/, holding
    # the last 30 days of master_ranked appearances. Avoids the 30-days *
    # 14-scanners file-read cost on every page load.
    try:
        _write_ticker_reverse_index(run_date, master_df)
    except Exception as e:
        log.warning(f"  ticker_index write failed (non-fatal): {e}")

    return master_df, conflicts_df, summary_df


def _write_ticker_reverse_index(
    run_date: date, master_df: pd.DataFrame, retention_days: int = 30,
) -> None:
    """For each ticker in master_df, upsert today's entry into
    data_cache/ticker_index/<TICKER>.json and prune entries older than
    retention_days. Single read/write per affected ticker.

    File schema:
      {"ticker": "AAPL",
       "history": [
         {"date": "2026-05-08", "scanners": [...], "composite_score": 2.4,
          "n_categories": 2, "directions": "bullish", "is_conflict": false}
       ]}
    """
    import json
    from datetime import timedelta as _td

    if master_df is None or master_df.empty:
        return

    index_dir = Path("data_cache/ticker_index")
    index_dir.mkdir(parents=True, exist_ok=True)
    cutoff = run_date - _td(days=retention_days)
    today_iso = run_date.isoformat()

    for _, row in master_df.iterrows():
        ticker = str(row.get("ticker", "")).strip().upper()
        if not ticker or ticker == "?":
            continue

        path = index_dir / f"{ticker}.json"
        existing = {"ticker": ticker, "history": []}
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                existing = {"ticker": ticker, "history": []}

        history = [h for h in existing.get("history", []) if h.get("date") != today_iso]

        # Prune older than cutoff
        def _keep(h: dict) -> bool:
            try:
                return date.fromisoformat(h.get("date", "")) >= cutoff
            except Exception:
                return False
        history = [h for h in history if _keep(h)]

        scanners = [s.strip() for s in str(row.get("scanners_hit", "")).split(",") if s.strip()]
        history.append({
            "date": today_iso,
            "scanners": scanners,
            "composite_score": float(row.get("composite_score", 0.0) or 0.0),
            "n_categories": int(row.get("n_categories", 0) or 0),
            "directions": str(row.get("directions", "")),
            "is_conflict": bool(row.get("is_conflict", False)),
        })
        history.sort(key=lambda h: h.get("date", ""), reverse=True)

        path.write_text(
            json.dumps({"ticker": ticker, "history": history}, indent=None),
            encoding="utf-8",
        )


def _emit_alerts(run_date, scanner_summary, master_df, conflicts_df) -> None:
    """Fire daily_summary alert + log meta_ranker_complete. Wrapped in try/except
    so any alerting failure never blocks aggregate() from returning."""
    try:
        from src.alerting.setup import init_default_bridge
        from src.alerting import bridge, events
        init_default_bridge()

        scan_count = sum(1 for s in scanner_summary if s.get("candidates", 0) > 0)
        candidates_count = len(master_df) if master_df is not None else 0
        conflicts_count = (
            len(conflicts_df) if conflicts_df is not None and not conflicts_df.empty else 0
        )

        bridge.alert(events.daily_summary(
            scan_count=scan_count,
            candidates_count=candidates_count,
            conflicts_count=conflicts_count,
            watchlist_signals_count=0,         # populated by watchlist.py separately
            account_value=0.0,                 # placeholder - Phase 7 wires broker
            daily_pnl=0.0,                     # placeholder
        ))

        if bridge.is_initialized():
            from src.alerting.bridge import _bridge
            if _bridge is not None and _bridge._logger is not None:
                _bridge._logger.log(
                    "meta_ranker_complete",
                    f"meta-ranker aggregated {candidates_count} tickers ({conflicts_count} conflicts)",
                    level="INFO",
                    payload={
                        "run_date": run_date.isoformat(),
                        "scan_count": scan_count,
                        "candidates_count": candidates_count,
                        "conflicts_count": conflicts_count,
                    },
                )
    except Exception as e:
        log.warning(f"meta_ranker alerting hook failed: {e}")


def cli():
    """CLI entry point. Run via: python -m scanners.meta_ranker [--date YYYY-MM-DD]"""
    import argparse
    parser = argparse.ArgumentParser(description="Cross-scanner meta-ranker")
    parser.add_argument(
        "--date",
        type=lambda s: date.fromisoformat(s),
        default=date.today(),
        help="Date to aggregate (YYYY-MM-DD). Default: today.",
    )
    parser.add_argument(
        "--output-dir",
        default="scan_output",
        help="Scanner output directory. Default: scan_output/",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    master_df, conflicts_df, summary_df = aggregate(args.date, Path(args.output_dir))

    if master_df.empty:
        print("No data to display")
        return

    print(f"\n=== Top 30 by composite score ({args.date}) ===")
    display_cols = [
        "ticker", "composite_score", "n_scanners", "n_categories",
        "scanners_hit", "is_conflict",
    ]
    print(master_df.head(30)[display_cols].to_string(index=False))
    print()

    if not conflicts_df.empty:
        print(f"\n=== Conflicts (bullish + bearish on same ticker) ===")
        cf_cols = ["ticker", "composite_score", "n_scanners", "directions", "scanners_hit"]
        print(conflicts_df[cf_cols].to_string(index=False))
        print()

    print(f"\n=== Per-scanner contribution ===")
    print(summary_df.to_string(index=False))
    print()


if __name__ == "__main__":
    cli()