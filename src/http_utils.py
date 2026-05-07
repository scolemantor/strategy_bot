"""HTTP timeout utilities.

Wraps requests.Session instances so calls that omit a `timeout` kwarg
get a sensible default. Used to defang the alpaca-py SDK and yfinance,
neither of which sets a timeout on its internal session — a stalled
TCP connection would otherwise hang a scanner forever.

Also provides `with_deadline`: a per-call wall-clock guard. Socket
timeouts only bound a single HTTP request, but yfinance and alpaca-py
both make multi-request high-level calls (yfinance .info hits multiple
quoteSummary modules; alpaca-py retries on 429). A hung high-level call
can therefore exceed the socket timeout many times over.
"""
from __future__ import annotations

import threading
from typing import Callable, TypeVar

import requests

T = TypeVar("T")


def apply_default_timeout(session: requests.Session, timeout: float) -> requests.Session:
    """Inject a default timeout into any session.request call missing one.

    Mutates and returns the session so callers can chain.
    """
    original = session.request

    def request_with_timeout(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return original(method, url, **kwargs)

    session.request = request_with_timeout  # type: ignore[method-assign]
    return session


def yfinance_session(timeout: float = 30) -> requests.Session:
    """Build a requests.Session suitable for `yf.Ticker(symbol, session=...)`."""
    return apply_default_timeout(requests.Session(), timeout)


def with_deadline(fn: Callable[[], T], timeout: float, default: T = None) -> T:
    """Run fn() with a wall-clock deadline.

    Returns `default` if fn does not complete within `timeout` seconds; the
    worker thread is abandoned and will be cleaned up when the python process
    exits. Acceptable for daily scans where a hung yfinance/alpaca call would
    otherwise stall the whole loop.

    Re-raises any exception fn() raises, so existing try/except blocks at
    call sites still work — only the timeout case is new.
    """
    result: list = [default]
    exc: list = [None]

    def runner():
        try:
            result[0] = fn()
        except BaseException as e:
            exc[0] = e

    t = threading.Thread(target=runner, daemon=True, name="io-deadline")
    t.start()
    t.join(timeout)
    if t.is_alive():
        return default
    if exc[0] is not None:
        raise exc[0]
    return result[0]
