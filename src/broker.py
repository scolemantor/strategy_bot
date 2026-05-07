"""Thin wrapper around alpaca-py for the trading bot.

Exposes only the operations the bot needs and converts SDK types into
plain dataclasses so the rest of the code never imports from alpaca.*.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

from .config import BrokerCredentials
from .data import BATCH_DELAY_SEC
from .http_utils import apply_default_timeout, with_deadline

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Position:
    symbol: str
    qty: float
    market_value: float
    avg_entry_price: float


@dataclass(frozen=True)
class Account:
    cash: float
    portfolio_value: float
    buying_power: float
    daytrade_count: int


@dataclass(frozen=True)
class OrderResult:
    symbol: str
    side: str
    qty: float
    status: str
    order_id: str
    submitted_at: Optional[str] = None
    error: Optional[str] = None


class AlpacaBroker:
    """All broker interactions go through this class."""

    def __init__(self, creds: BrokerCredentials):
        self._trading = TradingClient(
            api_key=creds.api_key,
            secret_key=creds.secret_key,
            paper=creds.paper,
        )
        apply_default_timeout(self._trading._session, 60)
        self._data = StockHistoricalDataClient(
            api_key=creds.api_key,
            secret_key=creds.secret_key,
        )
        apply_default_timeout(self._data._session, 60)
        self.paper = creds.paper

    def get_account(self) -> Account:
        a = self._trading.get_account()
        return Account(
            cash=float(a.cash),
            portfolio_value=float(a.portfolio_value),
            buying_power=float(a.buying_power),
            daytrade_count=int(a.daytrade_count),
        )

    def get_positions(self) -> Dict[str, Position]:
        result: Dict[str, Position] = {}
        for p in self._trading.get_all_positions():
            result[p.symbol] = Position(
                symbol=p.symbol,
                qty=float(p.qty),
                market_value=float(p.market_value),
                avg_entry_price=float(p.avg_entry_price),
            )
        return result

    def get_quote(self, symbol: str) -> float:
        """Return midpoint of bid/ask for a single symbol, or 0 if unavailable."""
        try:
            req = StockLatestQuoteRequest(symbol_or_symbols=[symbol])
            resp = with_deadline(
                lambda: self._data.get_stock_latest_quote(req), timeout=30, default=None,
            )
            if resp is None:
                log.warning(f"get_quote deadline for {symbol}")
                return 0.0
            quote = resp[symbol]
            bid = float(quote.bid_price) if quote.bid_price else 0.0
            ask = float(quote.ask_price) if quote.ask_price else 0.0
            if bid > 0 and ask > 0:
                return (bid + ask) / 2
            return ask if ask > 0 else bid
        except Exception as e:
            log.warning(f"Failed to get quote for {symbol}: {e}")
            return 0.0

    def get_quotes(self, symbols: List[str]) -> Dict[str, float]:
        """Return midpoint quotes for a batch of symbols."""
        if not symbols:
            return {}
        try:
            req = StockLatestQuoteRequest(symbol_or_symbols=symbols)
            resp = with_deadline(
                lambda: self._data.get_stock_latest_quote(req), timeout=30, default=None,
            )
            if resp is None:
                raise TimeoutError("batch quote deadline")
            quotes: Dict[str, float] = {}
            for sym in symbols:
                if sym not in resp:
                    quotes[sym] = 0.0
                    continue
                q = resp[sym]
                bid = float(q.bid_price) if q.bid_price else 0.0
                ask = float(q.ask_price) if q.ask_price else 0.0
                if bid > 0 and ask > 0:
                    quotes[sym] = (bid + ask) / 2
                else:
                    quotes[sym] = ask if ask > 0 else bid
            return quotes
        except Exception as e:
            log.warning(f"Batch quote fetch failed, falling back to per-symbol: {e}")
            fallback: Dict[str, float] = {}
            for i, s in enumerate(symbols):
                if i > 0:
                    time.sleep(BATCH_DELAY_SEC)
                fallback[s] = self.get_quote(s)
            return fallback

    def is_market_open(self) -> bool:
        try:
            return bool(self._trading.get_clock().is_open)
        except Exception as e:
            log.warning(f"Failed to check market clock: {e}")
            return False

    def place_market_order(self, symbol: str, qty: float, side: str) -> OrderResult:
        """Submit a fractional market order. Returns OrderResult, never raises."""
        try:
            order_side = OrderSide.BUY if side.lower() == "buy" else OrderSide.SELL
            req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=order_side,
                time_in_force=TimeInForce.DAY,
            )
            order = self._trading.submit_order(req)
            return OrderResult(
                symbol=symbol,
                side=side,
                qty=qty,
                status=str(order.status),
                order_id=str(order.id),
                submitted_at=str(order.submitted_at) if order.submitted_at else None,
            )
        except Exception as e:
            return OrderResult(
                symbol=symbol,
                side=side,
                qty=qty,
                status="error",
                order_id="",
                error=str(e),
            )
