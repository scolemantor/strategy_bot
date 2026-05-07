"""Pre-defined alert constructors.

Every alert raised by strategy_bot goes through one of these so titles,
dedup_keys, and payload schemas stay consistent across the codebase.
Downstream consumers (Pushover dispatcher, JsonLinesLogger, future iOS
app, future Phase 7.5 API) read these payload shapes directly — keep
them stable.

Design contract per constructor:
  - severity, source, title, body, dedup_key are policy-set here
  - payload is a flat dict of the input args (raw values, not formatted)
  - clock parameter (optional) overrides datetime.now(UTC) for tests
  - title is hard-truncated to 250 chars (Pushover limit)
  - body is hard-truncated to 1024 chars (Pushover limit)

Callers: `from src.alerting.events import kill_switch_triggered`.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Optional

from . import Alert

DefaultClock = Callable[[], datetime]

TITLE_MAX = 250
BODY_MAX = 1024


def _now(clock: Optional[DefaultClock]) -> datetime:
    return clock() if clock is not None else datetime.now(timezone.utc)


def _date_iso(ts: datetime) -> str:
    return ts.date().isoformat()


def _truncate(s: str, max_len: int) -> str:
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _build_alert(
    *,
    severity: str,
    title: str,
    body: str,
    ts: datetime,
    source_func_name: str,
    payload: Dict[str, Any],
    dedup_key: Optional[str],
) -> Alert:
    return Alert(
        severity=severity,
        title=_truncate(title, TITLE_MAX),
        body=_truncate(body, BODY_MAX),
        timestamp=ts,
        source=f"src.alerting.events.{source_func_name}",
        payload=payload,
        dedup_key=dedup_key,
    )


# === CRITICAL =============================================================

def kill_switch_triggered(
    reason: str,
    current_drawdown: float,
    hwm_drawdown: float,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="CRITICAL",
        title=f"KILL SWITCH: {reason}",
        body=(
            f"Kill switch triggered.\n"
            f"Reason: {reason}\n"
            f"Current drawdown: {current_drawdown:.2%}\n"
            f"HWM drawdown:     {hwm_drawdown:.2%}\n"
            f"Time:             {ts.isoformat()}"
        ),
        ts=ts,
        source_func_name="kill_switch_triggered",
        payload={
            "reason": reason,
            "current_drawdown": current_drawdown,
            "hwm_drawdown": hwm_drawdown,
        },
        dedup_key=f"kill_switch:{reason}",
    )


def drawdown_breach(
    current_drawdown: float,
    threshold: float,
    account_value: float,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="CRITICAL",
        title=f"Drawdown breach: {current_drawdown:.1%} (threshold {threshold:.1%})",
        body=(
            f"Drawdown threshold breached.\n"
            f"Current drawdown: {current_drawdown:.2%}\n"
            f"Threshold:        {threshold:.2%}\n"
            f"Account value:    ${account_value:,.2f}\n"
            f"Time:             {ts.isoformat()}"
        ),
        ts=ts,
        source_func_name="drawdown_breach",
        payload={
            "current_drawdown": current_drawdown,
            "threshold": threshold,
            "account_value": account_value,
        },
        dedup_key=f"drawdown:{_date_iso(ts)}",
    )


def order_failure(
    symbol: str,
    side: str,
    qty: float,
    error_message: str,
    alpaca_order_id: Optional[str] = None,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    body_lines = [
        "Order failed.",
        f"Symbol:  {symbol}",
        f"Side:    {side}",
        f"Qty:     {qty}",
        f"Error:   {error_message}",
    ]
    if alpaca_order_id:
        body_lines.append(f"Alpaca order ID: {alpaca_order_id}")
    body_lines.append(f"Time:    {ts.isoformat()}")
    return _build_alert(
        severity="CRITICAL",
        title=f"Order failed: {side} {qty} {symbol}",
        body="\n".join(body_lines),
        ts=ts,
        source_func_name="order_failure",
        payload={
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "error_message": error_message,
            "alpaca_order_id": alpaca_order_id,
        },
        dedup_key=f"order_fail:{symbol}:{side}:{qty}",
    )


def auth_failure(
    service: str,
    error_message: str,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="CRITICAL",
        title=f"Auth failure: {service}",
        body=(
            f"Authentication failed for {service}.\n"
            f"Error: {error_message}\n"
            f"Time:  {ts.isoformat()}"
        ),
        ts=ts,
        source_func_name="auth_failure",
        payload={"service": service, "error_message": error_message},
        dedup_key=f"auth_fail:{service}",
    )


def scanner_exception(
    scanner_name: str,
    exception_class: str,
    exception_message: str,
    traceback: str,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    tb_first = "\n".join(traceback.splitlines()[:3]) if traceback else ""
    return _build_alert(
        severity="CRITICAL",
        title=f"Scanner exception: {scanner_name} ({exception_class})",
        body=(
            f"Scanner exception in {scanner_name}.\n"
            f"Exception: {exception_class}: {exception_message}\n"
            f"Traceback (first 3 lines):\n{tb_first}\n"
            f"Time: {ts.isoformat()}"
        ),
        ts=ts,
        source_func_name="scanner_exception",
        payload={
            "scanner_name": scanner_name,
            "exception_class": exception_class,
            "exception_message": exception_message,
            "traceback": traceback,
        },
        dedup_key=f"scanner_exc:{scanner_name}:{exception_class}",
    )


def regime_flip(
    from_regime: str,
    to_regime: str,
    spy_price: float,
    spy_200dma: float,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="CRITICAL",
        title=f"Regime flip: {from_regime} -> {to_regime}",
        body=(
            f"Regime detector flipped.\n"
            f"From:        {from_regime}\n"
            f"To:          {to_regime}\n"
            f"SPY price:   ${spy_price:.2f}\n"
            f"SPY 200dma:  ${spy_200dma:.2f}\n"
            f"Time:        {ts.isoformat()}"
        ),
        ts=ts,
        source_func_name="regime_flip",
        payload={
            "from_regime": from_regime,
            "to_regime": to_regime,
            "spy_price": spy_price,
            "spy_200dma": spy_200dma,
        },
        dedup_key=f"regime:{from_regime}:{to_regime}:{_date_iso(ts)}",
    )


# === OPERATIONAL ==========================================================

def daily_summary(
    scan_count: int,
    candidates_count: int,
    conflicts_count: int,
    watchlist_signals_count: int,
    account_value: float,
    daily_pnl: float,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="OPERATIONAL",
        title=f"Daily summary {_date_iso(ts)}",
        body=(
            f"Scans run:           {scan_count}\n"
            f"Candidates:          {candidates_count}\n"
            f"Conflicts:           {conflicts_count}\n"
            f"Watchlist signals:   {watchlist_signals_count}\n"
            f"Account value:       ${account_value:,.2f}\n"
            f"Daily P&L:           ${daily_pnl:+,.2f}"
        ),
        ts=ts,
        source_func_name="daily_summary",
        payload={
            "scan_count": scan_count,
            "candidates_count": candidates_count,
            "conflicts_count": conflicts_count,
            "watchlist_signals_count": watchlist_signals_count,
            "account_value": account_value,
            "daily_pnl": daily_pnl,
        },
        dedup_key=f"daily_summary:{_date_iso(ts)}",
    )


def rebalance_executed(
    orders_placed: int,
    total_value: float,
    drift_before: float,
    drift_after: float,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="OPERATIONAL",
        title=f"Rebalance executed: {orders_placed} orders",
        body=(
            f"Rebalance complete.\n"
            f"Orders placed:  {orders_placed}\n"
            f"Total value:    ${total_value:,.2f}\n"
            f"Drift before:   {drift_before:.2%}\n"
            f"Drift after:    {drift_after:.2%}"
        ),
        ts=ts,
        source_func_name="rebalance_executed",
        payload={
            "orders_placed": orders_placed,
            "total_value": total_value,
            "drift_before": drift_before,
            "drift_after": drift_after,
        },
        dedup_key=f"rebalance:{_date_iso(ts)}",
    )


def scanner_complete(
    scanner_name: str,
    candidates_count: int,
    runtime_seconds: float,
    errors_count: int,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="OPERATIONAL",
        title=f"Scanner done: {scanner_name} ({candidates_count} candidates)",
        body=(
            f"Scanner: {scanner_name}\n"
            f"Candidates: {candidates_count}\n"
            f"Runtime: {runtime_seconds:.1f}s\n"
            f"Errors: {errors_count}"
        ),
        ts=ts,
        source_func_name="scanner_complete",
        payload={
            "scanner_name": scanner_name,
            "candidates_count": candidates_count,
            "runtime_seconds": runtime_seconds,
            "errors_count": errors_count,
        },
        dedup_key=f"scanner_done:{scanner_name}:{_date_iso(ts)}",
    )


def watchlist_signal(
    ticker: str,
    signal_type: str,
    scanner: str,
    change_description: str,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="OPERATIONAL",
        title=f"Watchlist: {ticker} {signal_type}",
        body=(
            f"Watchlist signal.\n"
            f"Ticker:  {ticker}\n"
            f"Signal:  {signal_type}\n"
            f"Scanner: {scanner}\n"
            f"Change:  {change_description}"
        ),
        ts=ts,
        source_func_name="watchlist_signal",
        payload={
            "ticker": ticker,
            "signal_type": signal_type,
            "scanner": scanner,
            "change_description": change_description,
        },
        dedup_key=f"watchlist:{ticker}:{signal_type}:{_date_iso(ts)}",
    )


def backtest_complete(
    start_date: date,
    end_date: date,
    final_metrics_summary: Dict[str, Any],
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    metrics_block = "\n".join(
        f"  {k}: {v}" for k, v in (final_metrics_summary or {}).items()
    )
    return _build_alert(
        severity="OPERATIONAL",
        title=f"Backtest complete: {start_date} to {end_date}",
        body=(
            f"Backtest finished.\n"
            f"Start: {start_date}\n"
            f"End:   {end_date}\n"
            f"Metrics:\n{metrics_block}"
        ),
        ts=ts,
        source_func_name="backtest_complete",
        payload={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "final_metrics_summary": final_metrics_summary,
        },
        dedup_key=f"backtest_done:{end_date.isoformat()}",
    )


# === INFO =================================================================

def system_startup(
    version: str,
    hostname: str,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="INFO",
        title=f"strategy_bot startup ({hostname})",
        body=(
            f"strategy_bot starting.\n"
            f"Version:  {version}\n"
            f"Hostname: {hostname}\n"
            f"Time:     {ts.isoformat()}"
        ),
        ts=ts,
        source_func_name="system_startup",
        payload={"version": version, "hostname": hostname},
        dedup_key=None,
    )


def scan_started(
    scanner_count: int,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="INFO",
        title=f"Scan started: {scanner_count} scanners",
        body=(
            f"Beginning scan run with {scanner_count} scanners.\n"
            f"Time: {ts.isoformat()}"
        ),
        ts=ts,
        source_func_name="scan_started",
        payload={"scanner_count": scanner_count},
        dedup_key=None,
    )


def new_candidate(
    ticker: str,
    scanner: str,
    score: float,
    reason: str,
    *,
    clock: Optional[DefaultClock] = None,
) -> Alert:
    ts = _now(clock)
    return _build_alert(
        severity="INFO",
        title=f"New candidate: {ticker} ({scanner})",
        body=(
            f"Ticker:  {ticker}\n"
            f"Scanner: {scanner}\n"
            f"Score:   {score:.2f}\n"
            f"Reason:  {reason}"
        ),
        ts=ts,
        source_func_name="new_candidate",
        payload={
            "ticker": ticker,
            "scanner": scanner,
            "score": score,
            "reason": reason,
        },
        dedup_key=None,
    )
