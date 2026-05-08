"""Read scan_output/<date>/*.csv files and shape them for API responses.

Single read path — production scans write these CSVs nightly via cron;
the dashboard never writes here. Errors fall through to empty responses
so a partial scan output (e.g. one CSV missing) doesn't 500 the page.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd

log = logging.getLogger(__name__)

SCAN_OUTPUT_DIR = Path("scan_output")
TICKER_INDEX_DIR = Path("data_cache/ticker_index")

# Status priority for watchlist aggregation (higher = wins when a ticker has
# multiple rows with conflicting delta_flag values across scanners).
STATUS_PRIORITY = {
    "EXITED": 5,
    "NEW": 4,
    "STRONGER": 3,
    "WEAKER": 2,
    "SAME": 1,
    "STABLE": 1,
    "": 0,
}


def _safe_read_csv(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception as e:
        log.warning(f"data_loader: failed to read {path}: {e}")
        return None


def latest_scan_date(base: Path = SCAN_OUTPUT_DIR) -> Optional[date]:
    """Most recent date subdir under scan_output/ that contains master_ranked.csv."""
    if not base.exists():
        return None
    candidates: List[date] = []
    for child in base.iterdir():
        if not child.is_dir():
            continue
        try:
            d = date.fromisoformat(child.name)
        except ValueError:
            continue
        if (child / "master_ranked.csv").exists():
            candidates.append(d)
    if not candidates:
        return None
    return max(candidates)


def list_recent_dates(limit: int = 30, base: Path = SCAN_OUTPUT_DIR) -> List[date]:
    """Return up to `limit` most recent dates that have master_ranked.csv."""
    if not base.exists():
        return []
    out: List[date] = []
    for child in base.iterdir():
        if not child.is_dir():
            continue
        try:
            d = date.fromisoformat(child.name)
        except ValueError:
            continue
        if (child / "master_ranked.csv").exists():
            out.append(d)
    out.sort(reverse=True)
    return out[:limit]


def load_master_ranked(target_date: date) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Returns (master, conflicts, category_summary). Any may be None if missing."""
    day_dir = SCAN_OUTPUT_DIR / target_date.isoformat()
    return (
        _safe_read_csv(day_dir / "master_ranked.csv"),
        _safe_read_csv(day_dir / "conflicts.csv"),
        _safe_read_csv(day_dir / "category_summary.csv"),
    )


def master_ranked_to_response(target_date: date) -> dict:
    master, conflicts, summary = load_master_ranked(target_date)
    if master is None:
        return {
            "date": target_date.isoformat(),
            "total_count": 0,
            "conflicts_count": 0,
            "candidates": [],
            "scanner_breakdown": [],
        }

    candidates = []
    for _, row in master.iterrows():
        candidates.append({
            "ticker": str(row.get("ticker", "")),
            "composite_score": float(row.get("composite_score", 0.0) or 0.0),
            "n_scanners": int(row.get("n_scanners", 0) or 0),
            "n_categories": int(row.get("n_categories", 0) or 0),
            "directions": str(row.get("directions", "")),
            "scanners_hit": str(row.get("scanners_hit", "")),
            "categories_hit": str(row.get("categories_hit", "")),
            "is_conflict": bool(row.get("is_conflict", False)),
            "reasons": str(row.get("reasons", "")),
        })

    breakdown: List[dict] = []
    if summary is not None:
        for _, row in summary.iterrows():
            breakdown.append({
                "scanner": str(row.get("scanner", "")),
                "direction": str(row.get("direction", "")),
                "category": str(row.get("category", "")),
                "weight": float(row.get("weight", 0.0) or 0.0),
                "candidates": int(row.get("candidates", 0) or 0),
                "contributed_to_master": int(row.get("contributed_to_master", 0) or 0),
            })

    return {
        "date": target_date.isoformat(),
        "total_count": len(candidates),
        "conflicts_count": int(len(conflicts)) if conflicts is not None else 0,
        "candidates": candidates,
        "scanner_breakdown": breakdown,
    }


def conflicts_for_date(target_date: date) -> List[dict]:
    df = _safe_read_csv(SCAN_OUTPUT_DIR / target_date.isoformat() / "conflicts.csv")
    if df is None:
        return []
    return df.fillna("").to_dict(orient="records")


def category_summary_for_date(target_date: date) -> List[dict]:
    df = _safe_read_csv(SCAN_OUTPUT_DIR / target_date.isoformat() / "category_summary.csv")
    if df is None:
        return []
    return df.fillna("").to_dict(orient="records")


def watchlist_for_date(target_date: date) -> dict:
    """Read watchlist_digest.csv and aggregate per-ticker rows into members."""
    df = _safe_read_csv(SCAN_OUTPUT_DIR / target_date.isoformat() / "watchlist_digest.csv")
    if df is None or df.empty:
        return {"date": target_date.isoformat(), "members": []}

    # Group by ticker; pick highest-priority delta_flag, sum scores, list scanners.
    members: List[dict] = []
    for ticker_raw, group in df.groupby("ticker"):
        ticker = str(ticker_raw).strip().upper()
        if not ticker:
            continue
        statuses = [str(s).strip().upper() for s in group.get("delta_flag", [])]
        # Pick highest-priority status (EXITED > NEW > STRONGER > WEAKER > SAME)
        status = max(statuses, key=lambda s: STATUS_PRIORITY.get(s, 0)) if statuses else ""
        if status == "SAME":
            status = "STABLE"
        scanners = sorted(set(str(s) for s in group.get("scanner", []) if str(s).strip()))
        scores = pd.to_numeric(group.get("score"), errors="coerce").dropna()
        composite = float(scores.sum()) if not scores.empty else None
        stale_flags = [s for s in group.get("stale_flag", []) if isinstance(s, str) and s.strip()]
        reasons = [str(r) for r in group.get("scanner_reason", []) if isinstance(r, str) and r.strip()]
        members.append({
            "ticker": ticker,
            "status": status or "STABLE",
            "composite_score": composite,
            "scanners_hit": ", ".join(scanners),
            "delta_flag": status,
            "stale_flag": stale_flags[0] if stale_flags else None,
            "scanner_reason": reasons[0] if reasons else None,
        })

    members.sort(
        key=lambda m: (
            -STATUS_PRIORITY.get(m["status"], 0),
            -(m["composite_score"] or 0.0),
        ),
    )
    return {"date": target_date.isoformat(), "members": members}


def history_summary(limit: int = 30) -> List[dict]:
    """For ScanHistory page — list dates with summary stats."""
    out = []
    for d in list_recent_dates(limit=limit):
        master, _, summary = load_master_ranked(d)
        if master is None:
            continue
        top_5 = master.head(5)["ticker"].astype(str).tolist() if "ticker" in master.columns else []
        scanner_count = 0
        if summary is not None and "candidates" in summary.columns:
            scanner_count = int((summary["candidates"] > 0).sum())
        out.append({
            "date": d.isoformat(),
            "candidate_count": len(master),
            "scanner_count": scanner_count,
            "top_5": top_5,
        })
    return out


# --- Ticker reverse index (Phase 7.5 commit 3, per Q7) ---

def load_ticker_index(symbol: str) -> Optional[dict]:
    """Read data_cache/ticker_index/{SYMBOL}.json. None if missing."""
    path = TICKER_INDEX_DIR / f"{symbol.upper()}.json"
    if not path.exists():
        return None
    try:
        import json
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"data_loader: failed to read ticker index {path}: {e}")
        return None


def scanner_csvs_with_ticker(symbol: str, lookback_days: int = 7) -> Iterable[Tuple[date, str, dict]]:
    """Yield (date, scanner_name, row_dict) tuples for any row with ticker=symbol
    across recent scanner CSVs. Used by ticker detail's recent_signals when
    the reverse index is missing or incomplete."""
    symbol_upper = symbol.upper()
    today = latest_scan_date() or date.today()
    for delta in range(lookback_days):
        d = today - timedelta(days=delta)
        day_dir = SCAN_OUTPUT_DIR / d.isoformat()
        if not day_dir.exists():
            continue
        for csv_path in day_dir.glob("*.csv"):
            scanner_name = csv_path.stem
            if scanner_name in {"master_ranked", "conflicts", "category_summary", "watchlist_digest"}:
                continue
            if scanner_name.endswith("_rejected"):
                continue
            df = _safe_read_csv(csv_path)
            if df is None or "ticker" not in df.columns:
                continue
            for _, row in df[df["ticker"].astype(str).str.upper() == symbol_upper].iterrows():
                yield (d, scanner_name, row.to_dict())
