"""HTTP timeout utilities.

Wraps requests.Session instances so calls that omit a `timeout` kwarg
get a sensible default. Used to defang the alpaca-py SDK and yfinance,
neither of which sets a timeout on its internal session — a stalled
TCP connection would otherwise hang a scanner forever.
"""
from __future__ import annotations

import requests


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
