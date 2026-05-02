"""Tests for LotLedger.

Run with: pytest tests/test_lot_ledger.py -v
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from src.lot_ledger import Consumption, Lot, LotLedger


@pytest.fixture
def ledger(tmp_path):
    """Fresh LotLedger backed by a temp SQLite file."""
    db = tmp_path / "test_ledger.sqlite"
    return LotLedger(db)


# --- Schema initialization -------------------------------------------------


class TestSchemaInitialization:
    def test_creates_db_file(self, tmp_path):
        db = tmp_path / "ledger.sqlite"
        LotLedger(db)
        assert db.exists()

    def test_idempotent_initialization(self, tmp_path):
        """Reopening a ledger on existing DB doesn't break or wipe data."""
        db = tmp_path / "ledger.sqlite"
        l1 = LotLedger(db)
        l1.insert_lot("VTI", 10, date(2024, 1, 1), 245.0)
        l2 = LotLedger(db)  # Should not error or wipe
        assert l2.get_total_qty("VTI") == 10

    def test_creates_parent_directory(self, tmp_path):
        db = tmp_path / "deep" / "nested" / "ledger.sqlite"
        LotLedger(db)
        assert db.parent.exists()
        assert db.exists()


# --- insert_lot ------------------------------------------------------------


class TestInsertLot:
    def test_basic_insertion(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        assert lot_id > 0

    def test_returns_unique_ids(self, ledger):
        id1 = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        id2 = ledger.insert_lot("VTI", 5, date(2024, 2, 15), 250.00)
        assert id1 != id2

    def test_zero_qty_rejected(self, ledger):
        with pytest.raises(ValueError, match="must be positive"):
            ledger.insert_lot("VTI", 0, date(2024, 1, 15), 245.30)

    def test_negative_qty_rejected(self, ledger):
        with pytest.raises(ValueError, match="must be positive"):
            ledger.insert_lot("VTI", -1, date(2024, 1, 15), 245.30)

    def test_negative_cost_basis_rejected(self, ledger):
        with pytest.raises(ValueError, match="non-negative"):
            ledger.insert_lot("VTI", 10, date(2024, 1, 15), -10.0)

    def test_zero_cost_basis_allowed(self, ledger):
        """Zero cost basis is valid (e.g., gifted shares)."""
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 0.0)
        assert lot_id > 0

    def test_synthetic_flag_persisted(self, ledger):
        ledger.insert_lot(
            "VTI", 10, date(2024, 1, 15), 245.30, is_synthetic=True
        )
        lots = ledger.get_open_lots("VTI")
        assert len(lots) == 1
        assert lots[0].is_synthetic is True

    def test_real_flag_default(self, ledger):
        """is_synthetic defaults to False (real fill, not migration-seeded)."""
        ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        lots = ledger.get_open_lots("VTI")
        assert lots[0].is_synthetic is False

    def test_notes_persisted(self, ledger):
        ledger.insert_lot(
            "VTI", 10, date(2024, 1, 15), 245.30, notes="initial seed"
        )
        lots = ledger.get_open_lots("VTI")
        assert lots[0].notes == "initial seed"

    def test_fractional_qty_supported(self, ledger):
        """Alpaca supports fractional shares."""
        ledger.insert_lot("VTI", 0.123, date(2024, 1, 15), 245.30)
        assert ledger.get_total_qty("VTI") == pytest.approx(0.123)


# --- get_open_lots ---------------------------------------------------------


class TestGetOpenLots:
    def test_returns_empty_for_unknown_symbol(self, ledger):
        assert ledger.get_open_lots("UNKNOWN") == []

    def test_single_lot(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        lots = ledger.get_open_lots("VTI")
        assert len(lots) == 1
        assert lots[0].symbol == "VTI"
        assert lots[0].original_qty == 10
        assert lots[0].remaining_qty == 10
        assert lots[0].purchase_date == date(2024, 1, 15)
        assert lots[0].cost_basis_per_share == 245.30

    def test_multiple_lots_same_symbol(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.insert_lot("VTI", 5, date(2024, 2, 15), 250.00)
        ledger.insert_lot("VTI", 3, date(2024, 3, 15), 260.00)
        lots = ledger.get_open_lots("VTI")
        assert len(lots) == 3

    def test_filters_other_symbols(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.insert_lot("SPY", 5, date(2024, 1, 15), 500.00)
        lots = ledger.get_open_lots("VTI")
        assert len(lots) == 1
        assert lots[0].symbol == "VTI"

    def test_excludes_fully_consumed(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(lot_id, 10, date(2024, 6, 15), 270.00)
        assert ledger.get_open_lots("VTI") == []

    def test_includes_partially_consumed(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(lot_id, 4, date(2024, 6, 15), 270.00)
        lots = ledger.get_open_lots("VTI")
        assert len(lots) == 1
        assert lots[0].remaining_qty == 6
        assert lots[0].original_qty == 10  # original is preserved

    def test_ordered_by_purchase_date_then_lot_id(self, ledger):
        # Insert out of date order
        l_late = ledger.insert_lot("VTI", 1, date(2024, 3, 1), 260.00)
        l_early = ledger.insert_lot("VTI", 1, date(2024, 1, 1), 240.00)
        l_mid = ledger.insert_lot("VTI", 1, date(2024, 2, 1), 250.00)
        lots = ledger.get_open_lots("VTI")
        assert [l.lot_id for l in lots] == [l_early, l_mid, l_late]


# --- consume_lot -----------------------------------------------------------


class TestConsumeLot:
    def test_basic_consumption(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        cid = ledger.consume_lot(lot_id, 4, date(2024, 6, 15), 270.00)
        assert cid > 0
        assert ledger.get_open_lots("VTI")[0].remaining_qty == 6

    def test_multiple_consumptions(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(lot_id, 3, date(2024, 6, 1), 270.00)
        ledger.consume_lot(lot_id, 4, date(2024, 7, 1), 280.00)
        assert ledger.get_open_lots("VTI")[0].remaining_qty == 3

    def test_full_consumption_removes_from_open(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(lot_id, 10, date(2024, 6, 15), 270.00)
        assert ledger.get_open_lots("VTI") == []

    def test_full_consumption_via_multiple_steps(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(lot_id, 5, date(2024, 6, 1), 270.00)
        ledger.consume_lot(lot_id, 5, date(2024, 7, 1), 280.00)
        assert ledger.get_open_lots("VTI") == []
        assert ledger.get_total_qty("VTI") == 0

    def test_over_consumption_rejected(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        with pytest.raises(ValueError, match="only 10"):
            ledger.consume_lot(lot_id, 11, date(2024, 6, 15), 270.00)

    def test_over_consumption_after_partial(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(lot_id, 4, date(2024, 6, 15), 270.00)
        with pytest.raises(ValueError, match="only 6"):
            ledger.consume_lot(lot_id, 7, date(2024, 7, 15), 275.00)

    def test_nonexistent_lot_rejected(self, ledger):
        with pytest.raises(ValueError, match="not found"):
            ledger.consume_lot(99999, 1, date(2024, 6, 15), 270.00)

    def test_zero_qty_rejected(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        with pytest.raises(ValueError, match="must be positive"):
            ledger.consume_lot(lot_id, 0, date(2024, 6, 15), 270.00)

    def test_negative_qty_rejected(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        with pytest.raises(ValueError, match="must be positive"):
            ledger.consume_lot(lot_id, -1, date(2024, 6, 15), 270.00)

    def test_negative_sale_price_rejected(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        with pytest.raises(ValueError, match="non-negative"):
            ledger.consume_lot(lot_id, 1, date(2024, 6, 15), -1.0)

    def test_zero_sale_price_allowed(self, ledger):
        """Allow zero (worthless asset, charitable transfer, etc.)."""
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        cid = ledger.consume_lot(lot_id, 1, date(2024, 6, 15), 0.0)
        assert cid > 0

    def test_floating_point_tolerance_at_boundary(self, ledger):
        """Consuming exactly the qty shouldn't fail due to fp accumulation."""
        # 0.1 + 0.2 = 0.30000000000000004 in IEEE 754
        lot_id = ledger.insert_lot("VTI", 0.1 + 0.2, date(2024, 1, 15), 245.30)
        # Consuming "exactly" 0.3 should work even though source qty is slightly bigger
        ledger.consume_lot(lot_id, 0.3, date(2024, 6, 15), 270.00)
        # Remaining should be ~0 (well within EPS)
        lots = ledger.get_open_lots("VTI")
        assert lots == []


# --- get_total_qty ---------------------------------------------------------


class TestGetTotalQty:
    def test_zero_for_unknown(self, ledger):
        assert ledger.get_total_qty("UNKNOWN") == 0

    def test_single_lot(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        assert ledger.get_total_qty("VTI") == 10

    def test_multiple_lots_summed(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.insert_lot("VTI", 5, date(2024, 2, 15), 250.00)
        ledger.insert_lot("VTI", 3, date(2024, 3, 15), 260.00)
        assert ledger.get_total_qty("VTI") == 18

    def test_excludes_consumed_qty(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(lot_id, 4, date(2024, 6, 15), 270.00)
        assert ledger.get_total_qty("VTI") == 6

    def test_excludes_other_symbols(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.insert_lot("SPY", 5, date(2024, 1, 15), 500.00)
        assert ledger.get_total_qty("VTI") == 10
        assert ledger.get_total_qty("SPY") == 5


# --- get_all_lots ----------------------------------------------------------


class TestGetAllLots:
    def test_includes_consumed(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(lot_id, 10, date(2024, 6, 15), 270.00)
        all_lots = ledger.get_all_lots("VTI")
        assert len(all_lots) == 1
        assert all_lots[0].remaining_qty == 0
        assert all_lots[0].original_qty == 10

    def test_open_and_consumed_both_present(self, ledger):
        l1 = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.insert_lot("VTI", 5, date(2024, 2, 15), 250.00)
        ledger.consume_lot(l1, 10, date(2024, 6, 15), 270.00)
        all_lots = ledger.get_all_lots("VTI")
        open_lots = ledger.get_open_lots("VTI")
        assert len(all_lots) == 2
        assert len(open_lots) == 1


# --- Lot dataclass methods -------------------------------------------------


class TestLotMethods:
    def _make_lot(self, **kwargs):
        defaults = dict(
            lot_id=1,
            symbol="VTI",
            original_qty=10,
            remaining_qty=10,
            purchase_date=date(2024, 1, 1),
            cost_basis_per_share=245.30,
            is_synthetic=False,
        )
        defaults.update(kwargs)
        return Lot(**defaults)

    def test_long_term_under_one_year_is_false(self):
        lot = self._make_lot(purchase_date=date(2024, 1, 1))
        assert lot.is_long_term_at(date(2024, 12, 31)) is False

    def test_long_term_exactly_365_days_is_false(self):
        """Per IRS rule, must be MORE than one year (held >365 days)."""
        lot = self._make_lot(purchase_date=date(2024, 1, 1))
        # 2024-01-01 + 365 days = 2024-12-31
        assert lot.is_long_term_at(date(2024, 12, 31)) is False

    def test_long_term_one_year_one_day_is_true(self):
        """One year and one day — the canonical long-term boundary."""
        lot = self._make_lot(purchase_date=date(2024, 1, 1))
        assert lot.is_long_term_at(date(2025, 1, 2)) is True

    def test_cost_basis_remaining(self):
        lot = self._make_lot(remaining_qty=6, cost_basis_per_share=245.30)
        assert lot.cost_basis_remaining() == pytest.approx(245.30 * 6)

    def test_unrealized_pnl_gain(self):
        lot = self._make_lot(remaining_qty=10, cost_basis_per_share=200.0)
        assert lot.unrealized_pnl_at(220.0) == pytest.approx(200.0)

    def test_unrealized_pnl_loss(self):
        lot = self._make_lot(remaining_qty=10, cost_basis_per_share=200.0)
        assert lot.unrealized_pnl_at(180.0) == pytest.approx(-200.0)


# --- Audit trail (consumptions) --------------------------------------------


class TestConsumptionAuditTrail:
    def test_consumptions_persist(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(lot_id, 4, date(2024, 6, 1), 270.00)
        ledger.consume_lot(lot_id, 3, date(2024, 7, 1), 280.00)
        consumptions = ledger.get_consumptions(lot_id)
        assert len(consumptions) == 2
        assert {c.qty for c in consumptions} == {4.0, 3.0}

    def test_consumptions_ordered_by_sale_date(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        # Insert out of order
        ledger.consume_lot(lot_id, 3, date(2024, 7, 1), 280.00)
        ledger.consume_lot(lot_id, 4, date(2024, 6, 1), 270.00)
        consumptions = ledger.get_consumptions(lot_id)
        assert consumptions[0].sale_date == date(2024, 6, 1)
        assert consumptions[1].sale_date == date(2024, 7, 1)

    def test_consumption_fields_preserved(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.consume_lot(
            lot_id, 4, date(2024, 6, 1), 270.00, notes="rebalance"
        )
        consumptions = ledger.get_consumptions(lot_id)
        assert consumptions[0].lot_id == lot_id
        assert consumptions[0].qty == 4
        assert consumptions[0].sale_price_per_share == 270.00
        assert consumptions[0].notes == "rebalance"


# --- all_symbols / all_open_symbols ----------------------------------------


class TestAllSymbols:
    def test_distinct_symbols_sorted(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.insert_lot("VTI", 5, date(2024, 2, 15), 250.00)
        ledger.insert_lot("SPY", 3, date(2024, 3, 15), 500.00)
        ledger.insert_lot("BND", 20, date(2024, 4, 15), 75.00)
        assert ledger.all_symbols() == ["BND", "SPY", "VTI"]

    def test_empty(self, ledger):
        assert ledger.all_symbols() == []

    def test_all_open_excludes_fully_consumed_symbols(self, ledger):
        l1 = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        ledger.insert_lot("SPY", 5, date(2024, 1, 15), 500.00)
        ledger.consume_lot(l1, 10, date(2024, 6, 15), 270.00)
        assert ledger.all_symbols() == ["SPY", "VTI"]
        assert ledger.all_open_symbols() == ["SPY"]


# --- Persistence -----------------------------------------------------------


class TestPersistenceAcrossInstances:
    def test_data_survives_reopen(self, tmp_path):
        db = tmp_path / "ledger.sqlite"
        l1 = LotLedger(db)
        lot_id = l1.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        l1.consume_lot(lot_id, 4, date(2024, 6, 15), 270.00)

        l2 = LotLedger(db)
        lots = l2.get_open_lots("VTI")
        assert len(lots) == 1
        assert lots[0].remaining_qty == 6

        consumptions = l2.get_consumptions(lot_id)
        assert len(consumptions) == 1
        assert consumptions[0].qty == 4


# --- Transaction safety ----------------------------------------------------


class TestTransactionRollback:
    def test_failed_consume_does_not_leave_partial_state(self, ledger):
        """If consume_lot validates and rejects, no consumption row is written."""
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 15), 245.30)
        with pytest.raises(ValueError):
            ledger.consume_lot(lot_id, 999, date(2024, 6, 15), 270.00)
        # Lot should still be untouched
        assert ledger.get_total_qty("VTI") == 10
        assert ledger.get_consumptions(lot_id) == []
