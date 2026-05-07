"""Render daily summary content for email body.

HTML uses inline CSS only (email clients strip <style>). Plain-text version
mirrors structure for clients that prefer text. Both gracefully render
empty sections — when a list is empty, the section renders a "(none today)"
placeholder rather than an empty table.

Account state section is currently always "(broker integration pending --
Phase 7a)" until broker access is wired.
"""
from __future__ import annotations

from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from . import Alert

ACCOUNT_PLACEHOLDER = "(broker integration pending -- Phase 7a)"
EMPTY_SECTION_PLACEHOLDER = "(none today)"
WATCHLIST_EMPTY_PLACEHOLDER = "(no signals at email render time)"


# === HTML helpers ===

def _score_color(score: float) -> str:
    if score >= 50:
        return "#0a7a2f"     # green
    if score >= 0:
        return "#b87a00"     # orange
    return "#a60e0e"         # red


def _delta_badge_color(delta: str) -> str:
    return {
        "NEW":      "#0a7a2f",
        "STRONGER": "#0066cc",
        "WEAKER":   "#b87a00",
        "STALE":    "#666666",
        "DROPPED":  "#666666",
    }.get(delta, "#333333")


def _load_top_picks_from_csv(path: Path, limit: int = 10) -> List[Dict[str, Any]]:
    df = pd.read_csv(path)
    out = []
    for _, r in df.head(limit).iterrows():
        out.append({
            "ticker": str(r.get("ticker", "")),
            "composite_score": float(r.get("composite_score", 0.0)),
            "scanners_hit": str(r.get("scanners_hit", "")),
        })
    return out


# === HTML render ===

def render_daily_summary_html(
    alert: Alert,
    master_ranked_path: Optional[Path] = None,
) -> str:
    """Returns HTML email body. Sections rendered in order:
       1. Header (date + 'strategy_bot daily summary')
       2. Top picks  (master_ranked_path if exists, else payload['top_picks'])
       3. Conflicts  (payload['conflicts'])
       4. Watchlist deltas  (payload['watchlist_deltas'])
       5. Account state placeholder
       6. Footer
    """
    payload = alert.payload or {}
    date_str = alert.timestamp.strftime("%Y-%m-%d")

    if master_ranked_path is not None and master_ranked_path.exists():
        try:
            top_picks = _load_top_picks_from_csv(master_ranked_path)
        except Exception:
            top_picks = payload.get("top_picks") or []
    else:
        top_picks = payload.get("top_picks") or []

    conflicts = payload.get("conflicts") or []
    watchlist_deltas = payload.get("watchlist_deltas") or []

    parts: List[str] = []
    parts.append(_html_header(date_str))
    parts.append(_html_summary_row(payload))
    parts.append(_html_top_picks(top_picks))
    parts.append(_html_conflicts(conflicts))
    parts.append(_html_watchlist(watchlist_deltas))
    parts.append(_html_account_state())
    parts.append(_html_footer(alert))

    body = "\n".join(parts)
    return (
        '<!DOCTYPE html><html><body '
        'style="font-family: -apple-system, BlinkMacSystemFont, sans-serif; '
        'max-width: 720px; margin: 0 auto; padding: 16px; color: #222;">'
        f"{body}</body></html>"
    )


def _html_header(date_str: str) -> str:
    return (
        f'<h1 style="margin: 0 0 8px 0; font-size: 22px;">strategy_bot daily summary</h1>'
        f'<div style="color: #666; margin-bottom: 24px;">{date_str}</div>'
    )


def _html_summary_row(payload: Dict[str, Any]) -> str:
    fields = [
        ("Scans run", payload.get("scan_count", 0)),
        ("Candidates", payload.get("candidates_count", 0)),
        ("Conflicts", payload.get("conflicts_count", 0)),
        ("Watchlist signals", payload.get("watchlist_signals_count", 0)),
    ]
    cells = []
    for label, val in fields:
        cells.append(
            '<td style="padding: 8px 16px 8px 0;">'
            f'<div style="font-size: 12px; color: #888;">{escape(label)}</div>'
            f'<div style="font-size: 18px; font-weight: 600;">{val}</div>'
            "</td>"
        )
    return (
        '<table style="border-collapse: collapse; margin-bottom: 24px;"><tr>'
        + "".join(cells)
        + "</tr></table>"
    )


def _html_top_picks(picks: List[Dict[str, Any]]) -> str:
    header = '<h2 style="font-size: 16px; margin: 16px 0 8px 0;">Top picks</h2>'
    if not picks:
        return header + f'<p style="color: #888;">{EMPTY_SECTION_PLACEHOLDER}</p>'
    rows = []
    for p in picks:
        ticker = escape(str(p.get("ticker", "")))
        score = float(p.get("composite_score", 0.0))
        scanners = escape(str(p.get("scanners_hit", "")))
        color = _score_color(score)
        rows.append(
            "<tr>"
            f'<td style="padding: 4px 12px; font-weight: 600;">{ticker}</td>'
            f'<td style="padding: 4px 12px; color: {color}; font-weight: 600;">{score:.2f}</td>'
            f'<td style="padding: 4px 12px; color: #555;">{scanners}</td>'
            "</tr>"
        )
    return (
        header
        + '<table style="border-collapse: collapse; width: 100%;">'
        + '<tr style="border-bottom: 1px solid #ddd;">'
        + '<th style="text-align: left; padding: 4px 12px;">Ticker</th>'
        + '<th style="text-align: left; padding: 4px 12px;">Score</th>'
        + '<th style="text-align: left; padding: 4px 12px;">Scanners</th>'
        + "</tr>"
        + "".join(rows)
        + "</table>"
    )


def _html_conflicts(conflicts: List[Dict[str, Any]]) -> str:
    header = '<h2 style="font-size: 16px; margin: 24px 0 8px 0;">Conflicts</h2>'
    if not conflicts:
        return header + f'<p style="color: #888;">{EMPTY_SECTION_PLACEHOLDER}</p>'
    rows = []
    for c in conflicts:
        ticker = escape(str(c.get("ticker", "")))
        directions = escape(str(c.get("directions", "")))
        scanners = escape(str(c.get("scanners_hit", "")))
        rows.append(
            '<tr style="background: #fbebeb;">'
            f'<td style="padding: 4px 12px; font-weight: 600; color: #a60e0e;">{ticker}</td>'
            f'<td style="padding: 4px 12px;">{directions}</td>'
            f'<td style="padding: 4px 12px; color: #555;">{scanners}</td>'
            "</tr>"
        )
    return (
        header
        + '<table style="border-collapse: collapse; width: 100%;">'
        + '<tr style="border-bottom: 1px solid #ddd;">'
        + '<th style="text-align: left; padding: 4px 12px;">Ticker</th>'
        + '<th style="text-align: left; padding: 4px 12px;">Directions</th>'
        + '<th style="text-align: left; padding: 4px 12px;">Scanners</th>'
        + "</tr>"
        + "".join(rows)
        + "</table>"
    )


def _html_watchlist(deltas: List[Dict[str, Any]]) -> str:
    header = '<h2 style="font-size: 16px; margin: 24px 0 8px 0;">Watchlist deltas</h2>'
    if not deltas:
        return header + f'<p style="color: #888;">{WATCHLIST_EMPTY_PLACEHOLDER}</p>'
    rows = []
    for d in deltas:
        ticker = escape(str(d.get("ticker", "")))
        signal = escape(str(d.get("signal_type", "")))
        scanner = escape(str(d.get("scanner", "")))
        change = escape(str(d.get("change", "")))
        badge_color = _delta_badge_color(str(d.get("signal_type", "")))
        rows.append(
            "<tr>"
            f'<td style="padding: 4px 12px; font-weight: 600;">{ticker}</td>'
            f'<td style="padding: 4px 12px;">'
            f'<span style="display: inline-block; padding: 2px 8px; '
            f'background: {badge_color}; color: white; border-radius: 3px; '
            f'font-size: 11px; font-weight: 600;">{signal}</span></td>'
            f'<td style="padding: 4px 12px; color: #555;">{scanner}</td>'
            f'<td style="padding: 4px 12px; color: #555; font-size: 13px;">{change}</td>'
            "</tr>"
        )
    return (
        header
        + '<table style="border-collapse: collapse; width: 100%;">'
        + '<tr style="border-bottom: 1px solid #ddd;">'
        + '<th style="text-align: left; padding: 4px 12px;">Ticker</th>'
        + '<th style="text-align: left; padding: 4px 12px;">Signal</th>'
        + '<th style="text-align: left; padding: 4px 12px;">Scanner</th>'
        + '<th style="text-align: left; padding: 4px 12px;">Change</th>'
        + "</tr>"
        + "".join(rows)
        + "</table>"
    )


def _html_account_state() -> str:
    return (
        '<h2 style="font-size: 16px; margin: 24px 0 8px 0;">Account state</h2>'
        f'<p style="color: #888;">{ACCOUNT_PLACEHOLDER}</p>'
    )


def _html_footer(alert: Alert) -> str:
    return (
        '<hr style="margin: 32px 0 8px 0; border: none; border-top: 1px solid #eee;">'
        '<p style="color: #888; font-size: 11px;">'
        f'Sent {alert.timestamp.isoformat()} -- '
        f'automated message from strategy_bot ({escape(alert.source)})'
        '</p>'
    )


# === Plain-text render ===

def render_daily_summary_text(alert: Alert) -> str:
    payload = alert.payload or {}
    date_str = alert.timestamp.strftime("%Y-%m-%d")
    lines: List[str] = []
    lines.append(f"strategy_bot daily summary -- {date_str}")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"Scans run:           {payload.get('scan_count', 0)}")
    lines.append(f"Candidates:          {payload.get('candidates_count', 0)}")
    lines.append(f"Conflicts:           {payload.get('conflicts_count', 0)}")
    lines.append(f"Watchlist signals:   {payload.get('watchlist_signals_count', 0)}")
    lines.append("")

    lines.append("Top picks")
    lines.append("-" * 60)
    picks = payload.get("top_picks") or []
    if not picks:
        lines.append(f"  {EMPTY_SECTION_PLACEHOLDER}")
    else:
        for p in picks:
            ticker = str(p.get("ticker", ""))
            score = float(p.get("composite_score", 0.0))
            scanners = str(p.get("scanners_hit", ""))
            lines.append(f"  {ticker:<8} {score:>7.2f}  {scanners}")
    lines.append("")

    lines.append("Conflicts")
    lines.append("-" * 60)
    conflicts = payload.get("conflicts") or []
    if not conflicts:
        lines.append(f"  {EMPTY_SECTION_PLACEHOLDER}")
    else:
        for c in conflicts:
            ticker = str(c.get("ticker", ""))
            directions = str(c.get("directions", ""))
            scanners = str(c.get("scanners_hit", ""))
            lines.append(f"  {ticker:<8} {directions:<25} {scanners}")
    lines.append("")

    lines.append("Watchlist deltas")
    lines.append("-" * 60)
    deltas = payload.get("watchlist_deltas") or []
    if not deltas:
        lines.append(f"  {WATCHLIST_EMPTY_PLACEHOLDER}")
    else:
        for d in deltas:
            ticker = str(d.get("ticker", ""))
            signal = str(d.get("signal_type", ""))
            scanner = str(d.get("scanner", ""))
            change = str(d.get("change", ""))[:60]
            lines.append(f"  {ticker:<8} {signal:<10} {scanner:<22} {change}")
    lines.append("")

    lines.append("Account state")
    lines.append("-" * 60)
    lines.append(f"  {ACCOUNT_PLACEHOLDER}")
    lines.append("")

    lines.append("-" * 60)
    lines.append(f"Sent {alert.timestamp.isoformat()}")
    lines.append(f"Automated message from strategy_bot ({alert.source})")
    return "\n".join(lines)
