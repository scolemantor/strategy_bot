"""End-to-end tests for the executor + ledger integration.

These exercise the wiring between order execution and the lot ledger:
  - Dry runs do not touch the ledger.
  - Buy fills create new lots at order.est_price.
  - Sell fills consume lots via tax-aware selection (HIFO + LT preference +
    losses first).
  - Broker errors do not touch the ledger.
  - Ledger failures after broker submission flag the order with
    _LEDGER_FAILED status but don't stop the batch.

A MockBroker stands in for AlpacaBroker. The real LotLedger is used (backed
by a temp SQLite file) because there's nothing meaningful to gain from
mocking that side.

Run with: pytest tests/test_executor_ledger.py -v
"""
from __future__ import annotations

from datetime import date
from typing import Optional

import pytest

from src.broker import OrderResult, Position
from src.executor import execute_orders
from src.lot_ledger import LotLedger
from src.lot_migration import seed_from_broker, reconcile_with_broker
from src.strategy import RebalanceOrder


# --- Mocks -----------------------------------------------------------------


class MockBroker:
    """In-memory fake of AlpacaBroker for testing executor logic.

    Records every order placement. Returns a successful OrderResult by default;
    set `next_error` to make the next call return an error result instead.
    """

    paper = True

    def __init__(self):
        self.orders_placed: list[tuple[str, float, str]] = []
        self.next_error: Optional[str] = None

    def place_market_order(self, symbol: str, qty: float, side: str) -> OrderResult:
        self.orders_placed.append((symbol, qty, side))
        if self.next_error is not None:
            err = self.next_error
            self.next_error = None
            return OrderResult(
                symbol=symbol, side=side, qty=qty,
                status="error", order_id="", error=err,
            )
        return OrderResult(
            symbol=symbol, side=side, qty=qty,
            status="accepted",
            order_id=f"mock-{len(self.orders_placed)}",
        )


def make_order(
    symbol: str,
    side: str,
    qty: float,
    price: float,
    *,
    target_value: float = 0.0,
    current_value: float = 0.0,
) -> RebalanceOrder:
    """Construct a RebalanceOrder for tests."""
    delta = qty * price * (1 if side == "buy" else -1)
    return RebalanceOrder(
        symbol=symbol,
        side=side,
        target_value=target_value,
        current_value=current_value,
        delta_value=delta,
        estimated_qty=qty,
        est_price=price,
    )


@pytest.fixture
def ledger(tmp_path):
    return LotLedger(tmp_path / "exec_ledger.sqlite")


# --- Dry-run safety --------------------------------------------------------


class TestDryRun:
    def test_dry_run_does_not_touch_ledger(self, ledger):
        broker = MockBroker()
        order = make_order("VTI", "buy", 5.0, 200.0)
        results = execute_orders([order], broker, dry_run=True, ledger=ledger)

        assert results[0].status == "dry_run"
        assert ledger.get_total_qty("VTI") == 0
        assert broker.orders_placed == []

    def test_dry_run_does_not_consume_existing_lots(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 200.0)
        broker = MockBroker()
        order = make_order("VTI", "sell", 5.0, 250.0)
        execute_orders([order], broker, dry_run=True, ledger=ledger)

        assert ledger.get_total_qty("VTI") == 10  # untouched


# --- Buy flow --------------------------------------------------------------


class TestBuyFlow:
    def test_buy_creates_lot(self, ledger):
        broker = MockBroker()
        order = make_order("VTI", "buy", 5.0, 200.0)
        execute_orders(
            [order], broker, dry_run=False,
            ledger=ledger, today=date(2026, 5, 1),
        )

        lots = ledger.get_open_lots("VTI")
        assert len(lots) == 1
        assert lots[0].original_qty == 5.0
        assert lots[0].cost_basis_per_share == 200.0
        assert lots[0].purchase_date == date(2026, 5, 1)
        assert lots[0].is_synthetic is False

    def test_multiple_buys_create_multiple_lots(self, ledger):
        broker = MockBroker()
        orders = [
            make_order("VTI", "buy", 3.0, 200.0),
            make_order("VTI", "buy", 2.0, 220.0),
        ]
        execute_orders(
            orders, broker, dry_run=False,
            ledger=ledger, today=date(2026, 5, 1),
        )

        lots = ledger.get_open_lots("VTI")
        assert len(lots) == 2
        assert ledger.get_total_qty("VTI") == 5.0

    def test_buy_with_zero_price_fails_ledger_update(self, ledger):
        """est_price=0 is invalid; should mark order as ledger-failed but not crash."""
        broker = MockBroker()
        order = make_order("VTI", "buy", 5.0, 0.0)  # bad price
        results = execute_orders(
            [order], broker, dry_run=False,
            ledger=ledger, today=date(2026, 5, 1),
        )

        assert "_LEDGER_FAILED" in results[0].status
        assert ledger.get_total_qty("VTI") == 0
        # Broker call still happened
        assert len(broker.orders_placed) == 1


# --- Sell flow with tax-aware selection -----------------------------------


class TestSellFlow:
    def test_sell_consumes_lots(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 200.0)
        broker = MockBroker()
        order = make_order("VTI", "sell", 4.0, 250.0)
        execute_orders(
            [order], broker, dry_run=False,
            ledger=ledger, today=date(2025, 6, 1),
        )

        assert ledger.get_total_qty("VTI") == 6.0

    def test_sell_picks_loss_lot_first(self, ledger):
        """Two LT lots, one gain and one loss at current price → loss sold first."""
        gain_lot = ledger.insert_lot("VTI", 5, date(2024, 1, 1), 200.0)  # gain at 250
        loss_lot = ledger.insert_lot("VTI", 5, date(2024, 1, 1), 300.0)  # loss at 250
        broker = MockBroker()
        order = make_order("VTI", "sell", 3.0, 250.0)
        execute_orders(
            [order], broker, dry_run=False,
            ledger=ledger, today=date(2025, 6, 1),
        )

        # All 3 should come from the loss lot (priority 2: LT loss before LT gain)
        gain = next(l for l in ledger.get_open_lots("VTI") if l.lot_id == gain_lot)
        loss = next(l for l in ledger.get_open_lots("VTI") if l.lot_id == loss_lot)
        assert gain.remaining_qty == 5  # untouched
        assert loss.remaining_qty == 2  # 5 - 3

    def test_sell_uses_hifo_within_gains(self, ledger):
        """All LT gains, HIFO ordering — highest basis first."""
        low_basis = ledger.insert_lot("VTI", 5, date(2024, 1, 1), 100.0)
        high_basis = ledger.insert_lot("VTI", 5, date(2024, 1, 1), 240.0)
        broker = MockBroker()
        order = make_order("VTI", "sell", 3.0, 250.0)
        execute_orders(
            [order], broker, dry_run=False,
            ledger=ledger, today=date(2025, 6, 1),
        )

        # HIFO: high_basis lot (240) hit first
        low = next(l for l in ledger.get_open_lots("VTI") if l.lot_id == low_basis)
        high = next(l for l in ledger.get_open_lots("VTI") if l.lot_id == high_basis)
        assert low.remaining_qty == 5  # untouched
        assert high.remaining_qty == 2

    def test_sell_spans_multiple_lots(self, ledger):
        ledger.insert_lot("VTI", 3, date(2024, 1, 1), 300.0)  # LT loss
        ledger.insert_lot("VTI", 5, date(2024, 1, 1), 280.0)  # LT loss (smaller)
        broker = MockBroker()
        order = make_order("VTI", "sell", 6.0, 250.0)
        execute_orders(
            [order], broker, dry_run=False,
            ledger=ledger, today=date(2025, 6, 1),
        )

        # 3 from biggest loss, then 3 from second loss
        assert ledger.get_total_qty("VTI") == 2.0


# --- Broker error handling -------------------------------------------------


class TestBrokerErrors:
    def test_broker_error_does_not_touch_ledger(self, ledger):
        broker = MockBroker()
        broker.next_error = "rate limit exceeded"
        order = make_order("VTI", "buy", 5.0, 200.0)
        results = execute_orders(
            [order], broker, dry_run=False,
            ledger=ledger, today=date(2026, 5, 1),
        )

        assert results[0].status == "error"
        assert results[0].error == "rate limit exceeded"
        assert ledger.get_total_qty("VTI") == 0  # ledger untouched

    def test_broker_error_on_one_does_not_stop_batch(self, ledger):
        broker = MockBroker()
        broker.next_error = "fail this one"  # only fails first
        orders = [
            make_order("VTI", "buy", 5.0, 200.0),  # fails
            make_order("BND", "buy", 10.0, 75.0),  # succeeds
        ]
        results = execute_orders(
            orders, broker, dry_run=False,
            ledger=ledger, today=date(2026, 5, 1),
        )

        assert results[0].status == "error"
        assert results[1].status == "accepted"
        assert ledger.get_total_qty("VTI") == 0
        assert ledger.get_total_qty("BND") == 10.0  # second buy recorded


# --- Ledger-disabled path (backward compat) --------------------------------


class TestLedgerDisabled:
    def test_no_ledger_works_normally(self):
        """When ledger=None, executor behaves like pre-Phase-3."""
        broker = MockBroker()
        order = make_order("VTI", "buy", 5.0, 200.0)
        results = execute_orders([order], broker, dry_run=False, ledger=None)

        assert results[0].status == "accepted"
        assert len(broker.orders_placed) == 1


# --- Full end-to-end scenario ----------------------------------------------


class TestEndToEnd:
    def test_seed_then_buy_then_sell_then_reconcile(self, ledger):
        """Walk through a realistic lifecycle: empty ledger → seed → buy → sell."""
        # 1. Initial broker state: holds 10 VTI from before bot started
        initial_positions = {
            "VTI": Position(symbol="VTI", qty=10, market_value=2000, avg_entry_price=200.0),
        }
        seed_from_broker(ledger, initial_positions, date(2025, 1, 1))
        assert ledger.get_total_qty("VTI") == 10

        # 2. Bot runs, decides to buy 5 more VTI at 220
        broker = MockBroker()
        buy = make_order("VTI", "buy", 5.0, 220.0)
        execute_orders(
            [buy], broker, dry_run=False,
            ledger=ledger, today=date(2025, 6, 1),
        )

        # 3. Reconcile against updated broker (now 15 shares)
        positions_after_buy = {
            "VTI": Position(symbol="VTI", qty=15, market_value=3450, avg_entry_price=213.33),
        }
        recon = reconcile_with_broker(ledger, positions_after_buy)
        assert recon.is_clean

        # 4. Bot sells 8 VTI at 230
        # Lots: synthetic at 200 (Jan 1, ST as of Aug 1 — only 7 mo)
        #       real at 220 (Jun 1, ST as of Aug 1 — 2 mo)
        # At 230: synthetic = ST gain $30/sh, real = ST gain $10/sh
        # ST gains, HIFO → real lot (220) first
        sell = make_order("VTI", "sell", 8.0, 230.0)
        execute_orders(
            [sell], broker, dry_run=False,
            ledger=ledger, today=date(2025, 8, 1),
        )

        # Real lot (5 shares) fully consumed; 3 more from synthetic
        # Synthetic remaining: 10 - 3 = 7
        assert ledger.get_total_qty("VTI") == 7

        open_lots = ledger.get_open_lots("VTI")
        assert len(open_lots) == 1
        assert open_lots[0].cost_basis_per_share == 200.0
        assert open_lots[0].remaining_qty == 7

        # 5. Final reconcile against final broker state (7 shares left)
        positions_final = {
            "VTI": Position(symbol="VTI", qty=7, market_value=1610, avg_entry_price=200.0),
        }
        assert reconcile_with_broker(ledger, positions_final).is_clean
