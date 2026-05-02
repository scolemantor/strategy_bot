"""Tests for lot_migration: seed_from_broker and reconcile_with_broker.

Run with: pytest tests/test_lot_migration.py -v
"""
from __future__ import annotations

from datetime import date

import pytest

from src.broker import Position
from src.lot_ledger import LotLedger
from src.lot_migration import (
    ReconciliationMismatch,
    ReconciliationReport,
    SeedResult,
    reconcile_with_broker,
    seed_from_broker,
)


@pytest.fixture
def ledger(tmp_path):
    return LotLedger(tmp_path / "test_ledger.sqlite")


def make_position(symbol: str, qty: float, avg_entry_price: float) -> Position:
    return Position(
        symbol=symbol,
        qty=qty,
        market_value=qty * avg_entry_price,
        avg_entry_price=avg_entry_price,
    )


SEED_DATE = date(2026, 5, 1)


# --- seed_from_broker ------------------------------------------------------


class TestSeedFromBroker:
    def test_empty_positions_returns_empty_result(self, ledger):
        result = seed_from_broker(ledger, {}, SEED_DATE)
        assert result.seeded_symbols == []
        assert result.skipped_symbols == []
        assert result.created_lot_ids == []

    def test_single_position_seeded(self, ledger):
        positions = {"VTI": make_position("VTI", 10, 245.30)}
        result = seed_from_broker(ledger, positions, SEED_DATE)
        assert result.seeded_symbols == ["VTI"]
        assert len(result.created_lot_ids) == 1

        lots = ledger.get_open_lots("VTI")
        assert len(lots) == 1
        assert lots[0].symbol == "VTI"
        assert lots[0].original_qty == 10
        assert lots[0].cost_basis_per_share == 245.30
        assert lots[0].purchase_date == SEED_DATE
        assert lots[0].is_synthetic is True

    def test_multiple_positions_seeded(self, ledger):
        positions = {
            "VTI": make_position("VTI", 10, 245.30),
            "BND": make_position("BND", 20, 75.00),
            "GLD": make_position("GLD", 5, 200.00),
        }
        result = seed_from_broker(ledger, positions, SEED_DATE)
        assert sorted(result.seeded_symbols) == ["BND", "GLD", "VTI"]
        assert ledger.get_total_qty("VTI") == 10
        assert ledger.get_total_qty("BND") == 20
        assert ledger.get_total_qty("GLD") == 5

    def test_idempotent_default(self, ledger):
        """Re-seeding with only_missing=True (default) skips existing symbols."""
        positions = {"VTI": make_position("VTI", 10, 245.30)}
        seed_from_broker(ledger, positions, SEED_DATE)
        # Run again with same positions
        result = seed_from_broker(ledger, positions, SEED_DATE)
        assert result.seeded_symbols == []
        assert result.skipped_symbols == ["VTI"]
        # Still only one lot
        assert len(ledger.get_open_lots("VTI")) == 1

    def test_re_seeds_when_only_missing_false(self, ledger):
        """only_missing=False creates a new synthetic lot even if symbol exists."""
        positions = {"VTI": make_position("VTI", 10, 245.30)}
        seed_from_broker(ledger, positions, SEED_DATE)
        # Force re-seed
        result = seed_from_broker(ledger, positions, SEED_DATE, only_missing=False)
        assert result.seeded_symbols == ["VTI"]
        # Two lots now
        assert len(ledger.get_open_lots("VTI")) == 2

    def test_zero_qty_position_skipped(self, ledger):
        positions = {
            "VTI": make_position("VTI", 10, 245.30),
            "BND": make_position("BND", 0, 75.00),
        }
        result = seed_from_broker(ledger, positions, SEED_DATE)
        assert result.seeded_symbols == ["VTI"]
        assert result.skipped_symbols == ["BND"]
        assert ledger.get_total_qty("BND") == 0

    def test_skips_partially_consumed_existing_lot(self, ledger):
        """If symbol has any lots (even fully consumed), default seed skips."""
        # Manually insert a lot, fully consume it
        lot_id = ledger.insert_lot("VTI", 5, date(2024, 1, 1), 200.0)
        ledger.consume_lot(lot_id, 5, date(2025, 1, 1), 250.0)
        # Broker reports a position (different qty, suggesting later buys)
        positions = {"VTI": make_position("VTI", 10, 245.30)}
        result = seed_from_broker(ledger, positions, SEED_DATE)
        assert result.seeded_symbols == []
        assert result.skipped_symbols == ["VTI"]

    def test_synthetic_lot_has_notes(self, ledger):
        positions = {"VTI": make_position("VTI", 10, 245.30)}
        seed_from_broker(ledger, positions, SEED_DATE)
        lots = ledger.get_open_lots("VTI")
        assert lots[0].notes == "seeded from broker on 2026-05-01"

    def test_fractional_qty_seeded(self, ledger):
        positions = {"VTI": make_position("VTI", 0.123, 245.30)}
        result = seed_from_broker(ledger, positions, SEED_DATE)
        assert result.seeded_symbols == ["VTI"]
        assert ledger.get_total_qty("VTI") == pytest.approx(0.123)


# --- reconcile_with_broker -------------------------------------------------


class TestReconcileClean:
    def test_empty_ledger_empty_broker(self, ledger):
        report = reconcile_with_broker(ledger, {})
        assert report.is_clean is True
        assert report.matched_symbols == []
        assert report.mismatches == []

    def test_matched_single_symbol(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 245.30)
        positions = {"VTI": make_position("VTI", 10, 245.30)}
        report = reconcile_with_broker(ledger, positions)
        assert report.is_clean is True
        assert report.matched_symbols == ["VTI"]

    def test_matched_multiple_symbols(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 245.30)
        ledger.insert_lot("BND", 20, date(2024, 1, 1), 75.00)
        positions = {
            "VTI": make_position("VTI", 10, 245.30),
            "BND": make_position("BND", 20, 75.00),
        }
        report = reconcile_with_broker(ledger, positions)
        assert report.is_clean is True
        assert sorted(report.matched_symbols) == ["BND", "VTI"]

    def test_matched_after_partial_consumption(self, ledger):
        lot_id = ledger.insert_lot("VTI", 10, date(2024, 1, 1), 245.30)
        ledger.consume_lot(lot_id, 4, date(2025, 1, 1), 270.00)
        positions = {"VTI": make_position("VTI", 6, 245.30)}
        report = reconcile_with_broker(ledger, positions)
        assert report.is_clean is True

    def test_within_tolerance(self, ledger):
        """Sub-EPS dust difference is treated as match."""
        ledger.insert_lot("VTI", 10.0, date(2024, 1, 1), 245.30)
        positions = {"VTI": make_position("VTI", 10.0 + 1e-10, 245.30)}
        report = reconcile_with_broker(ledger, positions)
        assert report.is_clean is True


class TestReconcileMismatch:
    def test_broker_more_than_ledger(self, ledger):
        """Manual buy outside the bot — broker has more shares than ledger."""
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 245.30)
        positions = {"VTI": make_position("VTI", 15, 245.30)}
        report = reconcile_with_broker(ledger, positions)
        assert report.is_clean is False
        assert len(report.mismatches) == 1
        m = report.mismatches[0]
        assert m.symbol == "VTI"
        assert m.ledger_qty == 10
        assert m.broker_qty == 15
        assert m.delta == 5

    def test_ledger_more_than_broker(self, ledger):
        """Manual sell outside the bot — ledger has more than broker reports."""
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 245.30)
        positions = {"VTI": make_position("VTI", 7, 245.30)}
        report = reconcile_with_broker(ledger, positions)
        assert report.is_clean is False
        m = report.mismatches[0]
        assert m.delta == -3

    def test_symbol_in_ledger_not_broker(self, ledger):
        """Ledger has open lots for symbol broker doesn't hold."""
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 245.30)
        report = reconcile_with_broker(ledger, {})
        assert report.is_clean is False
        m = report.mismatches[0]
        assert m.symbol == "VTI"
        assert m.ledger_qty == 10
        assert m.broker_qty == 0

    def test_symbol_in_broker_not_ledger(self, ledger):
        """Broker holds a symbol the ledger doesn't know about."""
        positions = {"VTI": make_position("VTI", 10, 245.30)}
        report = reconcile_with_broker(ledger, positions)
        assert report.is_clean is False
        m = report.mismatches[0]
        assert m.ledger_qty == 0
        assert m.broker_qty == 10

    def test_multiple_mismatches(self, ledger):
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 245.30)
        ledger.insert_lot("BND", 20, date(2024, 1, 1), 75.00)
        positions = {
            "VTI": make_position("VTI", 12, 245.30),  # broker has more
            "BND": make_position("BND", 18, 75.00),   # broker has less
        }
        report = reconcile_with_broker(ledger, positions)
        assert len(report.mismatches) == 2
        by_sym = {m.symbol: m for m in report.mismatches}
        assert by_sym["VTI"].delta == 2
        assert by_sym["BND"].delta == -2

    def test_partial_match_partial_mismatch(self, ledger):
        """Some symbols match, others mismatch — both reported."""
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 245.30)
        ledger.insert_lot("BND", 20, date(2024, 1, 1), 75.00)
        positions = {
            "VTI": make_position("VTI", 10, 245.30),  # match
            "BND": make_position("BND", 25, 75.00),   # broker has more
        }
        report = reconcile_with_broker(ledger, positions)
        assert "VTI" in report.matched_symbols
        assert len(report.mismatches) == 1
        assert report.mismatches[0].symbol == "BND"

    def test_zero_qty_position_treated_as_no_position(self, ledger):
        """A broker position with qty=0 is not 'holding' the symbol."""
        ledger.insert_lot("VTI", 10, date(2024, 1, 1), 245.30)
        positions = {"VTI": make_position("VTI", 0, 245.30)}
        report = reconcile_with_broker(ledger, positions)
        # Ledger has 10 qty, broker effectively has 0 → mismatch
        assert report.is_clean is False
        assert report.mismatches[0].broker_qty == 0


# --- Report methods --------------------------------------------------------


class TestReconciliationReport:
    def test_is_clean_true_when_no_mismatches(self):
        r = ReconciliationReport(matched_symbols=["VTI"], mismatches=[])
        assert r.is_clean is True

    def test_is_clean_false_with_mismatches(self):
        r = ReconciliationReport(
            matched_symbols=[],
            mismatches=[ReconciliationMismatch("VTI", 10, 15)],
        )
        assert r.is_clean is False

    def test_summary_clean(self):
        r = ReconciliationReport(matched_symbols=["VTI", "BND"], mismatches=[])
        assert "clean" in r.summary().lower()
        assert "2" in r.summary()

    def test_summary_with_mismatches(self):
        r = ReconciliationReport(
            matched_symbols=[],
            mismatches=[
                ReconciliationMismatch("VTI", 10, 15),
                ReconciliationMismatch("BND", 20, 18),
            ],
        )
        s = r.summary()
        assert "2 mismatch" in s
        assert "VTI" in s
        assert "BND" in s


# --- End-to-end migration scenario -----------------------------------------


class TestEndToEndMigration:
    def test_seed_then_reconcile_clean(self, ledger):
        """Standard flow: seed, then reconcile, expect clean report."""
        positions = {
            "VTI": make_position("VTI", 10, 245.30),
            "BND": make_position("BND", 20, 75.00),
            "GLD": make_position("GLD", 5, 200.00),
        }
        seed_from_broker(ledger, positions, SEED_DATE)
        report = reconcile_with_broker(ledger, positions)
        assert report.is_clean is True

    def test_seed_then_drift_then_reconcile_mismatch(self, ledger):
        """Seed, simulate drift (manual broker change), reconcile flags it."""
        positions = {"VTI": make_position("VTI", 10, 245.30)}
        seed_from_broker(ledger, positions, SEED_DATE)
        # Simulate user buying 5 more shares manually outside the bot
        positions_after_drift = {"VTI": make_position("VTI", 15, 245.30)}
        report = reconcile_with_broker(ledger, positions_after_drift)
        assert report.is_clean is False
        assert report.mismatches[0].delta == 5
