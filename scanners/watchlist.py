"""Watchlist tracker (Phase 4d, extended in Phase 8a).

Maintains a curated list of tickers you actively want to follow. Provides:
  - add/remove/list operations on the watchlist YAML (legacy 3-field)
  - Phase 8a: rich CRUD with tier / position_size / entry_price / stop_loss /
    target_price / notes / auto_added / added_at / last_modified fields
  - File locking via scanners.watchlist_lock
  - JSONL audit log via scanners.watchlist_audit
  - daily digest filtering scanner output to watchlist names only
  - delta detection: NEW / DROPPED / STRONGER / WEAKER vs prior day
  - STALE flag for tickers with no scanner appearances in N days

Output: scan_output/<date>/watchlist_digest.csv

Watchlist storage: config/watchlist.yaml. Each ticker entry can carry
either the legacy 3-field schema (added_date / reason / category) or
the extended Phase 8a schema (adds: tier, position_size, entry_price,
stop_loss, target_price, notes, auto_added, added_at, last_modified).
The save function writes whatever fields are present — no migration
required; entries upgrade lazily as they're touched.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .watchlist_audit import append_audit_log
from .watchlist_lock import watchlist_lock

log = logging.getLogger(__name__)

CONFIG_DIR = Path("config")
WATCHLIST_PATH = CONFIG_DIR / "watchlist.yaml"

# Defaults applied when read_all_entries() encounters a legacy entry
# missing the Phase 8a fields. Existing legacy fields (added_date /
# reason / category) are preserved.
PHASE_8A_DEFAULTS: Dict = {
    "tier": 2,
    "position_size": None,
    "entry_price": None,
    "stop_loss": None,
    "target_price": None,
    "notes": "",
    "auto_added": False,
}

# Updatable via PUT /api/watchlist/entries/{ticker} — anything else in
# a request body is silently ignored (defends against the API client
# clobbering audit fields like added_at).
UPDATABLE_FIELDS = frozenset({
    "tier", "position_size", "entry_price", "stop_loss", "target_price",
    "notes", "reason", "category",
})

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
    """Write watchlist YAML. Generic key-value writer — preserves whatever
    fields are present per ticker (legacy 3-field schema OR Phase 8a
    extended schema OR any mix). Comments not preserved across writes."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    settings = data.get("settings", {})
    tickers = data.get("tickers", {})

    out_lines = [
        "# Watchlist tracker (Phase 4d, extended Phase 8a).",
        "# Edit via CLI: python scan.py watch add/remove/list TICKER",
        "# Or via dashboard API: POST /api/watchlist/entries",
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
            # Stable field ordering: legacy fields first (so file diffs
            # against old version are minimal), then Phase 8a fields.
            field_order = (
                "added_date", "reason", "category",
                "tier", "position_size", "entry_price", "stop_loss",
                "target_price", "notes", "auto_added", "added_at",
                "last_modified",
            )
            seen_keys = set()
            for key in field_order:
                if key in meta and meta[key] is not None and meta[key] != "":
                    out_lines.append(_yaml_kv(key, meta[key]))
                    seen_keys.add(key)
            # Catch-all for any other keys we don't know about
            for key, val in meta.items():
                if key in seen_keys:
                    continue
                if val is None or val == "":
                    continue
                out_lines.append(_yaml_kv(key, val))
    out_lines.append("")

    WATCHLIST_PATH.write_text("\n".join(out_lines))


def _yaml_kv(key: str, val) -> str:
    """Render one `    key: value` line for a ticker entry. Quotes
    strings, leaves numbers/bools as native YAML."""
    if isinstance(val, bool):
        return f"    {key}: {str(val).lower()}"
    if isinstance(val, (int, float)):
        return f"    {key}: {val}"
    s = str(val).replace('"', '\\"')
    return f'    {key}: "{s}"'


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
    """Return current watchlist as a list of {ticker, added_date, reason, category} dicts.
    Legacy CLI shape — for full Phase 8a entry data, use read_all_entries()."""
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


# ---------- Phase 8a extended CRUD ----------

def read_all_entries() -> List[Dict]:
    """Return all watchlist entries with Phase 8a fields default-populated
    for legacy entries. The file itself is NOT mutated — defaults are
    applied on read; first subsequent update_entry() persists the full
    schema for that one ticker."""
    data = _load_watchlist()
    out: List[Dict] = []
    for ticker, meta in sorted(data["tickers"].items()):
        entry: Dict = {"ticker": ticker}
        # Phase 8a fields with defaults
        for field, default in PHASE_8A_DEFAULTS.items():
            entry[field] = meta.get(field, default)
        # Date fields: prefer Phase 8a (added_at) but fall back to legacy
        # (added_date string in YYYY-MM-DD form)
        added_at = meta.get("added_at") or meta.get("added_date") or ""
        last_modified = meta.get("last_modified") or added_at
        entry["added_at"] = added_at
        entry["last_modified"] = last_modified
        # Preserve legacy fields for callers that still expect them
        entry["added_date"] = meta.get("added_date", "")
        entry["reason"] = meta.get("reason", "")
        entry["category"] = meta.get("category", "general")
        out.append(entry)
    return out


def read_entry(ticker: str) -> Optional[Dict]:
    """Return one entry with defaults applied, or None if not present."""
    ticker = ticker.upper().strip()
    for entry in read_all_entries():
        if entry["ticker"] == ticker:
            return entry
    return None


def add_entry(
    ticker: str,
    source: str,
    user_agent: Optional[str] = None,
    **fields,
) -> Tuple[bool, Optional[Dict], Optional[Dict]]:
    """Add a new ticker with Phase 8a schema. Returns
    (success, before_state, after_state). If ticker already exists,
    returns (False, existing, existing) without mutation."""
    ticker = ticker.upper().strip()
    if not ticker:
        return False, None, None

    with watchlist_lock():
        data = _load_watchlist()
        if ticker in data["tickers"]:
            existing = dict(data["tickers"][ticker])
            return False, existing, existing

        now = datetime.now(timezone.utc).isoformat()
        entry: Dict = {
            "added_date": date.today().isoformat(),  # legacy compat
            "reason": fields.get("reason", ""),
            "category": fields.get("category", "general"),
            "tier": int(fields.get("tier", PHASE_8A_DEFAULTS["tier"])),
            "notes": fields.get("notes", ""),
            "auto_added": (source == "auto"),
            "added_at": now,
            "last_modified": now,
        }
        # Optional position fields — only persisted if non-null
        for f in ("position_size", "entry_price", "stop_loss", "target_price"):
            if fields.get(f) is not None:
                entry[f] = fields[f]

        data["tickers"][ticker] = entry
        _save_watchlist(data)

    log.info(f"  Added {ticker} to watchlist (source={source}, tier={entry['tier']})")
    append_audit_log("add", ticker, source, None, entry, user_agent)
    return True, None, entry


def remove_entry(
    ticker: str,
    source: str,
    user_agent: Optional[str] = None,
) -> Tuple[bool, Optional[Dict]]:
    """Remove a ticker. Returns (success, before_state)."""
    ticker = ticker.upper().strip()
    with watchlist_lock():
        data = _load_watchlist()
        if ticker not in data["tickers"]:
            return False, None
        before = dict(data["tickers"][ticker])
        del data["tickers"][ticker]
        _save_watchlist(data)

    log.info(f"  Removed {ticker} from watchlist (source={source})")
    append_audit_log("remove", ticker, source, before, None, user_agent)
    return True, before


def update_entry(
    ticker: str,
    fields: Dict,
    source: str,
    user_agent: Optional[str] = None,
) -> Tuple[bool, Optional[Dict], Optional[Dict]]:
    """Update fields on an existing ticker. Only UPDATABLE_FIELDS are
    honored — other keys in `fields` are silently dropped (defends
    against client clobbering audit fields like added_at). Returns
    (success, before_state, after_state)."""
    ticker = ticker.upper().strip()
    with watchlist_lock():
        data = _load_watchlist()
        if ticker not in data["tickers"]:
            return False, None, None
        before = dict(data["tickers"][ticker])
        for key, val in fields.items():
            if key not in UPDATABLE_FIELDS:
                continue
            data["tickers"][ticker][key] = val
        data["tickers"][ticker]["last_modified"] = datetime.now(timezone.utc).isoformat()
        after = dict(data["tickers"][ticker])
        _save_watchlist(data)

    log.info(f"  Updated {ticker} (source={source}, fields={list(fields.keys())})")
    append_audit_log("update", ticker, source, before, after, user_agent)
    return True, before, after


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

    _emit_watchlist_alerts(digest_df)
    _emit_daily_summary_email(run_date, output_dir, digest_df)

    return digest_df


def _emit_watchlist_alerts(digest_df: pd.DataFrame) -> None:
    """Fire one watchlist_signal alert per NEW or STRONGER delta. Each alert's
    dedup_key embeds (ticker, signal_type, date) so re-runs don't spam.
    Wrapped in try/except so any alerting failure never blocks digest."""
    if digest_df is None or digest_df.empty or "delta_flag" not in digest_df.columns:
        return
    try:
        from src.alerting.setup import init_default_bridge
        from src.alerting import bridge, events
        init_default_bridge()

        signals = digest_df[digest_df["delta_flag"].isin(["NEW", "STRONGER"])]
        for _, row in signals.iterrows():
            bridge.alert(events.watchlist_signal(
                ticker=str(row.get("ticker", "")),
                signal_type=str(row.get("delta_flag", "")),
                scanner=str(row.get("scanner", "")),
                change_description=str(row.get("scanner_reason", ""))[:200],
            ))
    except Exception as e:
        log.warning(f"watchlist alerting hook failed: {e}")


def _emit_daily_summary_email(
    run_date: date,
    output_dir: Path,
    digest_df: pd.DataFrame,
) -> None:
    """Fire the rich daily_summary_email at the end of the morning sequence.

    Reads master_ranked.csv + conflicts.csv from today's scan_output/<date>/
    so the email body has the full picture from all three morning jobs.
    Pushover suppresses this event_type via config; only the email channel
    dispatches it.

    Wrapped in try/except so any failure here never blocks digest_df from
    returning to the caller.
    """
    try:
        from src.alerting.setup import init_default_bridge
        from src.alerting import bridge, events
        init_default_bridge()

        date_dir = Path(output_dir) / run_date.isoformat()
        master_path = date_dir / "master_ranked.csv"
        conflicts_path = date_dir / "conflicts.csv"

        top_picks = []
        candidates_count = 0
        if master_path.exists():
            mdf = pd.read_csv(master_path)
            candidates_count = len(mdf)
            for _, r in mdf.head(10).iterrows():
                top_picks.append({
                    "ticker": str(r.get("ticker", "")),
                    "composite_score": float(r.get("composite_score", 0.0)),
                    "scanners_hit": str(r.get("scanners_hit", "")),
                })

        conflicts_list = []
        conflicts_count = 0
        if conflicts_path.exists():
            cdf = pd.read_csv(conflicts_path)
            conflicts_count = len(cdf)
            for _, r in cdf.iterrows():
                conflicts_list.append({
                    "ticker": str(r.get("ticker", "")),
                    "directions": str(r.get("directions", "")),
                    "scanners_hit": str(r.get("scanners_hit", "")),
                })

        watchlist_deltas = []
        if not digest_df.empty and "delta_flag" in digest_df.columns:
            wl_signals = digest_df[
                digest_df["delta_flag"].isin(["NEW", "STRONGER", "WEAKER", "DROPPED"])
            ]
            for _, r in wl_signals.iterrows():
                watchlist_deltas.append({
                    "ticker": str(r.get("ticker", "")),
                    "signal_type": str(r.get("delta_flag", "")),
                    "scanner": str(r.get("scanner", "")),
                    "change": str(r.get("scanner_reason", ""))[:200],
                })

        attachments = []
        if master_path.exists():
            attachments.append(str(master_path))

        # scan_count = how many scanner CSVs landed today (excluding aggregates)
        scan_count = 0
        if date_dir.exists():
            for p in date_dir.glob("*.csv"):
                if p.name in (
                    "master_ranked.csv", "conflicts.csv",
                    "category_summary.csv", "watchlist_digest.csv",
                ):
                    continue
                if p.name.endswith("_rejected.csv"):
                    continue
                scan_count += 1

        bridge.alert(events.daily_summary_email(
            scan_count=scan_count,
            candidates_count=candidates_count,
            conflicts_count=conflicts_count,
            watchlist_signals_count=len(watchlist_deltas),
            top_picks=top_picks,
            conflicts=conflicts_list,
            watchlist_deltas=watchlist_deltas,
            attachments=attachments,
        ))
    except Exception as e:
        log.warning(f"daily_summary_email hook failed: {e}")