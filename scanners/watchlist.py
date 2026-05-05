"""Watchlist tracker (Phase 4d).

Maintains a curated list of tickers you actively want to follow. Provides:
  - add/remove/list operations on the watchlist YAML
  - daily digest filtering scanner output to watchlist names only
  - delta detection: NEW / DROPPED / STRONGER / WEAKER vs prior day
  - STALE flag for tickers with no scanner appearances in N days

Output: scan_output/<date>/watchlist_digest.csv

Watchlist storage: config/watchlist.yaml. Settings (stale_days,
delta_threshold_pct) and ticker entries (added_date, reason, category)
all live in that file.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

log = logging.getLogger(__name__)

CONFIG_DIR = Path("config")
WATCHLIST_PATH = CONFIG_DIR / "watchlist.yaml"

# Scanners we filter against. macro_calendar excluded (events not tickers).
SCANNERS_TO_CHECK = [
    "insider_buying",
    "breakout_52w",
    "earnings_drift",
    "spinoff_tracker",
    "fda_calendar",
    "thirteen_f_changes",
    "short_squeeze",
    "small_cap_value",
    "sector_rotation",
    "earnings_calendar",
    "ipo_lockup",
    "insider_selling_clusters",
]


def _load_watchlist() -> Dict:
    """Load watchlist YAML. Returns dict with 'settings' and 'tickers' keys."""
    if not WATCHLIST_PATH.exists():
        return {"settings": {"stale_days": 14, "delta_threshold_pct": 0.10}, "tickers": {}}
    try:
        import yaml
        data = yaml.safe_load(WATCHLIST_PATH.read_text()) or {}
        # Defensive: ensure both keys exist
        if "settings" not in data:
            data["settings"] = {"stale_days": 14, "delta_threshold_pct": 0.10}
        if "tickers" not in data or data["tickers"] is None:
            data["tickers"] = {}
        return data
    except ImportError:
        raise ImportError("PyYAML required. Run: pip install pyyaml")
    except Exception as e:
        log.warning(f"Failed to load watchlist: {e}")
        return {"settings": {"stale_days": 14, "delta_threshold_pct": 0.10}, "tickers": {}}


def _save_watchlist(data: Dict) -> None:
    """Write watchlist YAML. Preserves comments by writing in a structured way."""
    import yaml
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    # We do a simple dump — comments aren't preserved across writes, but the
    # structure is. Users editing manually can keep their comments; CLI writes
    # rewrite cleanly.
    settings = data.get("settings", {})
    tickers = data.get("tickers", {})

    out_lines = [
        "# Watchlist tracker (Phase 4d).",
        "# Edit via CLI: python scan.py watch add/remove/list TICKER",
        "",
        "settings:",
        f"  stale_days: {settings.get('stale_days', 14)}",
        f"  delta_threshold_pct: {settings.get('delta_threshold_pct', 0.10)}",
        "",
        "tickers:",
    ]
    if not tickers:
        out_lines[-1] = "tickers: {}"
    else:
        for ticker, meta in sorted(tickers.items()):
            out_lines.append(f"  {ticker}:")
            for key in ("added_date", "reason", "category"):
                if key in meta and meta[key] is not None:
                    val = str(meta[key]).replace('"', '\\"')
                    out_lines.append(f'    {key}: "{val}"')
    out_lines.append("")  # trailing newline

    WATCHLIST_PATH.write_text("\n".join(out_lines))


# ---------- CLI operations ----------

def add_ticker(ticker: str, reason: str = "", category: str = "") -> bool:
    """Add a ticker to the watchlist. Returns True if added, False if already present."""
    ticker = ticker.upper().strip()
    if not ticker:
        return False

    data = _load_watchlist()
    if ticker in data["tickers"]:
        log.info(f"  {ticker} already on watchlist")
        return False

    data["tickers"][ticker] = {
        "added_date": date.today().isoformat(),
        "reason": reason or "(no reason given)",
        "category": category or "general",
    }
    _save_watchlist(data)
    log.info(f"  Added {ticker} to watchlist (reason: {reason or 'none'})")
    return True


def remove_ticker(ticker: str) -> bool:
    """Remove a ticker from the watchlist. Returns True if removed, False if not found."""
    ticker = ticker.upper().strip()
    data = _load_watchlist()
    if ticker not in data["tickers"]:
        log.info(f"  {ticker} not on watchlist")
        return False
    del data["tickers"][ticker]
    _save_watchlist(data)
    log.info(f"  Removed {ticker} from watchlist")
    return True


def list_tickers() -> List[Dict]:
    """Return current watchlist as a list of {ticker, added_date, reason, category} dicts."""
    data = _load_watchlist()
    out = []
    for ticker, meta in sorted(data["tickers"].items()):
        out.append({
            "ticker": ticker,
            "added_date": meta.get("added_date", ""),
            "reason": meta.get("reason", ""),
            "category": meta.get("category", ""),
        })
    return out


# ---------- Daily digest ----------

def _load_scanner_csv_for_watchlist(date_dir: Path, scanner_name: str, tickers: List[str]) -> pd.DataFrame:
    """Load a scanner's CSV filtered to watchlist tickers. Returns empty DataFrame if no hits."""
    p = date_dir / f"{scanner_name}.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(p)
        if df.empty or "ticker" not in df.columns:
            return pd.DataFrame()
        df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
        filtered = df[df["ticker"].isin(tickers)].copy()
        if filtered.empty:
            return pd.DataFrame()
        filtered["scanner"] = scanner_name
        return filtered
    except Exception as e:
        log.debug(f"  Failed to load {p}: {e}")
        return pd.DataFrame()


def _compute_deltas(today_df: pd.DataFrame, yesterday_df: pd.DataFrame, threshold_pct: float) -> Dict[Tuple[str, str], str]:
    """Compute per-(ticker, scanner) delta flag vs prior day.

    Returns dict keyed on (ticker, scanner_name) -> flag string.
    Flags: NEW, DROPPED, STRONGER, WEAKER, SAME.
    """
    out: Dict[Tuple[str, str], str] = {}

    # Build lookup of (ticker, scanner) -> score for both days
    today_scores = {}
    if not today_df.empty:
        for _, row in today_df.iterrows():
            try:
                today_scores[(row["ticker"], row["scanner"])] = float(row.get("score", 0))
            except (ValueError, TypeError):
                pass

    yesterday_scores = {}
    if not yesterday_df.empty:
        for _, row in yesterday_df.iterrows():
            try:
                yesterday_scores[(row["ticker"], row["scanner"])] = float(row.get("score", 0))
            except (ValueError, TypeError):
                pass

    all_keys = set(today_scores) | set(yesterday_scores)

    for key in all_keys:
        in_today = key in today_scores
        in_yesterday = key in yesterday_scores

        if in_today and not in_yesterday:
            out[key] = "NEW"
        elif in_yesterday and not in_today:
            out[key] = "DROPPED"
        elif in_today and in_yesterday:
            today_score = today_scores[key]
            yesterday_score = yesterday_scores[key]
            if yesterday_score == 0:
                out[key] = "SAME"
            else:
                pct_change = (today_score - yesterday_score) / abs(yesterday_score)
                if pct_change > threshold_pct:
                    out[key] = "STRONGER"
                elif pct_change < -threshold_pct:
                    out[key] = "WEAKER"
                else:
                    out[key] = "SAME"

    return out


def _last_seen_per_ticker(output_dir: Path, tickers: List[str], lookback_days: int = 30) -> Dict[str, Optional[date]]:
    """For each ticker, find the most recent date it appeared in any scanner CSV.
    Returns dict of {ticker: date or None}."""
    out: Dict[str, Optional[date]] = {t: None for t in tickers}

    for days_back in range(0, lookback_days + 1):
        check_date = date.today() - timedelta(days=days_back)
        date_dir = output_dir / check_date.isoformat()
        if not date_dir.exists():
            continue

        for scanner in SCANNERS_TO_CHECK:
            p = date_dir / f"{scanner}.csv"
            if not p.exists():
                continue
            try:
                df = pd.read_csv(p)
                if df.empty or "ticker" not in df.columns:
                    continue
                df_tickers = df["ticker"].astype(str).str.upper().str.strip().unique()
                for t in tickers:
                    if t in df_tickers and out[t] is None:
                        out[t] = check_date
            except Exception:
                continue

        # Stop early if all tickers have been found
        if all(v is not None for v in out.values()):
            break

    return out


def run_digest(run_date: date, output_dir: Path = Path("scan_output")) -> pd.DataFrame:
    """Generate watchlist digest for the given date.

    Compares to yesterday's scanner output for delta detection, and looks back
    further to compute STALE flags (no scanner hit in N days).

    Returns the digest DataFrame and writes it to:
      scan_output/<run_date>/watchlist_digest.csv
    """
    log.info(f"Watchlist digest for {run_date}")
    data = _load_watchlist()
    settings = data["settings"]
    watchlist_tickers = list(data["tickers"].keys())

    if not watchlist_tickers:
        log.info("  Watchlist is empty; nothing to digest")
        return pd.DataFrame()

    log.info(f"  Tracking {len(watchlist_tickers)} ticker(s): {', '.join(sorted(watchlist_tickers))}")

    stale_days = int(settings.get("stale_days", 14))
    delta_threshold = float(settings.get("delta_threshold_pct", 0.10))

    # Load today's scanner hits
    date_dir = output_dir / run_date.isoformat()
    today_hits_dfs = []
    for scanner in SCANNERS_TO_CHECK:
        df = _load_scanner_csv_for_watchlist(date_dir, scanner, watchlist_tickers)
        if not df.empty:
            today_hits_dfs.append(df)
    today_hits_df = pd.concat(today_hits_dfs, ignore_index=True) if today_hits_dfs else pd.DataFrame()

    # Load yesterday's hits for delta comparison
    yesterday = run_date - timedelta(days=1)
    yesterday_dir = output_dir / yesterday.isoformat()
    yesterday_hits_dfs = []
    for scanner in SCANNERS_TO_CHECK:
        df = _load_scanner_csv_for_watchlist(yesterday_dir, scanner, watchlist_tickers)
        if not df.empty:
            yesterday_hits_dfs.append(df)
    yesterday_hits_df = pd.concat(yesterday_hits_dfs, ignore_index=True) if yesterday_hits_dfs else pd.DataFrame()

    # Compute deltas
    deltas = _compute_deltas(today_hits_df, yesterday_hits_df, delta_threshold)

    # Compute last-seen for each watchlist ticker (for STALE detection)
    last_seen = _last_seen_per_ticker(output_dir, watchlist_tickers, lookback_days=stale_days + 5)

    # Build digest rows
    digest_rows = []

    for ticker in sorted(watchlist_tickers):
        meta = data["tickers"].get(ticker, {})

        # Today's scanner hits for this ticker
        if not today_hits_df.empty:
            ticker_today = today_hits_df[today_hits_df["ticker"] == ticker]
        else:
            ticker_today = pd.DataFrame()

        # Compute STALE flag
        seen_date = last_seen.get(ticker)
        if seen_date is None:
            stale_flag = f"STALE (no hits in {stale_days}+ days)"
        else:
            days_since_seen = (run_date - seen_date).days
            if days_since_seen > stale_days:
                stale_flag = f"STALE ({days_since_seen} days since last hit)"
            else:
                stale_flag = ""

        if ticker_today.empty:
            # No hits today — record placeholder row
            digest_rows.append({
                "ticker": ticker,
                "scanner": "",
                "score": 0,
                "delta_flag": "",
                "stale_flag": stale_flag,
                "last_seen": seen_date.isoformat() if seen_date else "",
                "added_date": meta.get("added_date", ""),
                "reason": meta.get("reason", ""),
                "category": meta.get("category", ""),
                "scanner_reason": "",
            })
        else:
            # One row per (ticker, scanner) hit today
            for _, row in ticker_today.iterrows():
                scanner_name = row["scanner"]
                delta_flag = deltas.get((ticker, scanner_name), "SAME")
                digest_rows.append({
                    "ticker": ticker,
                    "scanner": scanner_name,
                    "score": round(float(row.get("score", 0)), 2),
                    "delta_flag": delta_flag,
                    "stale_flag": "",  # has a hit today, not stale
                    "last_seen": run_date.isoformat(),
                    "added_date": meta.get("added_date", ""),
                    "reason": meta.get("reason", ""),
                    "category": meta.get("category", ""),
                    "scanner_reason": str(row.get("reason", ""))[:80],
                })

    # Add DROPPED entries (in yesterday but not today, on watchlist)
    if not yesterday_hits_df.empty:
        for _, row in yesterday_hits_df.iterrows():
            ticker = row["ticker"]
            scanner_name = row["scanner"]
            key = (ticker, scanner_name)
            if deltas.get(key) == "DROPPED":
                meta = data["tickers"].get(ticker, {})
                digest_rows.append({
                    "ticker": ticker,
                    "scanner": scanner_name,
                    "score": 0,
                    "delta_flag": "DROPPED",
                    "stale_flag": "",
                    "last_seen": (yesterday).isoformat(),
                    "added_date": meta.get("added_date", ""),
                    "reason": meta.get("reason", ""),
                    "category": meta.get("category", ""),
                    "scanner_reason": "(no longer surfacing)",
                })

    digest_df = pd.DataFrame(digest_rows)
    if not digest_df.empty:
        # Sort: tickers with deltas first, then by ticker
        delta_priority = {"NEW": 0, "STRONGER": 1, "DROPPED": 2, "WEAKER": 3, "SAME": 4, "": 5}
        digest_df["_priority"] = digest_df["delta_flag"].map(delta_priority).fillna(5)
        digest_df = digest_df.sort_values(["_priority", "ticker"]).drop(columns=["_priority"]).reset_index(drop=True)

    # Write to disk
    date_dir.mkdir(parents=True, exist_ok=True)
    digest_path = date_dir / "watchlist_digest.csv"
    digest_df.to_csv(digest_path, index=False)
    log.info(f"  Wrote {digest_path} ({len(digest_df)} rows)")

    return digest_df