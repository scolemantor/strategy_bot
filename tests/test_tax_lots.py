"""Tests for tax_lots: lot selection algorithm and realized P/L estimation.

Run with: pytest tests/test_tax_lots.py -v
"""
from __future__ import annotations

from datetime import date

import pytest

from src.lot_ledger import Lot
from src.tax_lots import (
    InsufficientLotsError,
    LotSelection,
    estimate_realized_pnl,
    select_lots_to_sell,
    split_realized_pnl,
)


# --- helpers ---------------------------------------------------------------


def make_lot(
    lot_id: int,
    qty: float,
    purchase_date: date,
    cost_basis: float,
    *,
    symbol: str = "VTI",
    is_synthetic: bool = False,
) -> Lot:
    """Construct an open Lot with remaining_qty == qty (no consumptions yet)."""
    return Lot(
        lot_id=lot_id,
        symbol=symbol,
        original_qty=qty,
        remaining_qty=qty,
        purchase_date=purchase_date,
        cost_basis_per_share=cost_basis,
        is_synthetic=is_synthetic,
    )


# Reference dates: sale_date is fixed; "long-term" means purchase >365 days ago.
SALE_DATE = date(2025, 6, 1)
LT_PURCHASE = date(2024, 1, 1)   # ~17 months ago: long-term
ST_PURCHASE = date(2025, 3, 1)   # ~3 months ago: short-term


# --- Trivial cases ---------------------------------------------------------


class TestTrivialCases:
    def test_zero_qty_returns_empty(self):
        lot = make_lot(1, 10, ST_PURCHASE, 200.0)
        assert select_lots_to_sell("VTI", 0, [lot], SALE_DATE, 250.0) == []

    def test_negative_qty_returns_empty(self):
        lot = make_lot(1, 10, ST_PURCHASE, 200.0)
        assert select_lots_to_sell("VTI", -5, [lot], SALE_DATE, 250.0) == []

    def test_empty_lots_with_positive_qty_raises(self):
        with pytest.raises(InsufficientLotsError) as exc:
            select_lots_to_sell("VTI", 5, [], SALE_DATE, 250.0)
        assert exc.value.symbol == "VTI"
        assert exc.value.requested == 5
        assert exc.value.available == 0

    def test_qty_exceeds_available_raises(self):
        lot = make_lot(1, 5, ST_PURCHASE, 200.0)
        with pytest.raises(InsufficientLotsError) as exc:
            select_lots_to_sell("VTI", 10, [lot], SALE_DATE, 250.0)
        assert exc.value.requested == 10
        assert exc.value.available == 5


# --- Single-lot scenarios --------------------------------------------------


class TestSingleLot:
    def test_single_lot_partial_sell(self):
        lot = make_lot(1, 10, ST_PURCHASE, 200.0)
        sel = select_lots_to_sell("VTI", 4, [lot], SALE_DATE, 250.0)
        assert sel == [LotSelection(lot_id=1, qty=4)]

    def test_single_lot_exact_sell(self):
        lot = make_lot(1, 10, ST_PURCHASE, 200.0)
        sel = select_lots_to_sell("VTI", 10, [lot], SALE_DATE, 250.0)
        assert sel == [LotSelection(lot_id=1, qty=10)]


# --- Bucket ordering: losses before gains ----------------------------------


class TestLossesBeforeGains:
    def test_st_loss_before_lt_gain(self):
        """ST loss should be selected first, even when LT gain is available."""
        lt_gain = make_lot(1, 10, LT_PURCHASE, 100.0)   # current 250 > basis 100 → gain
        st_loss = make_lot(2, 10, ST_PURCHASE, 300.0)   # current 250 < basis 300 → loss
        sel = select_lots_to_sell("VTI", 5, [lt_gain, st_loss], SALE_DATE, 250.0)
        assert sel[0].lot_id == 2  # ST loss

    def test_lt_loss_before_lt_gain(self):
        lt_gain = make_lot(1, 10, LT_PURCHASE, 100.0)
        lt_loss = make_lot(2, 10, LT_PURCHASE, 300.0)
        sel = select_lots_to_sell("VTI", 5, [lt_gain, lt_loss], SALE_DATE, 250.0)
        assert sel[0].lot_id == 2

    def test_lt_loss_before_st_gain(self):
        st_gain = make_lot(1, 10, ST_PURCHASE, 100.0)
        lt_loss = make_lot(2, 10, LT_PURCHASE, 300.0)
        sel = select_lots_to_sell("VTI", 5, [st_gain, lt_loss], SALE_DATE, 250.0)
        assert sel[0].lot_id == 2  # loss wins


# --- Bucket ordering: ST losses before LT losses ---------------------------


class TestSTLossBeforeLTLoss:
    def test_st_loss_picked_before_lt_loss(self):
        """ST losses preferred over LT losses (higher tax benefit)."""
        lt_loss = make_lot(1, 10, LT_PURCHASE, 300.0)
        st_loss = make_lot(2, 10, ST_PURCHASE, 300.0)
        sel = select_lots_to_sell("VTI", 5, [lt_loss, st_loss], SALE_DATE, 250.0)
        assert sel[0].lot_id == 2  # ST loss

    def test_st_loss_picked_even_when_smaller_than_lt_loss(self):
        """ST loss wins on bucket priority even with smaller per-share loss."""
        lt_loss_big = make_lot(1, 10, LT_PURCHASE, 500.0)  # $250 loss/share
        st_loss_small = make_lot(2, 10, ST_PURCHASE, 260.0)  # $10 loss/share
        sel = select_lots_to_sell("VTI", 5, [lt_loss_big, st_loss_small], SALE_DATE, 250.0)
        assert sel[0].lot_id == 2  # ST loss bucket beats LT loss bucket


# --- Bucket ordering: LT gains before ST gains -----------------------------


class TestLTGainBeforeSTGain:
    def test_lt_gain_picked_before_st_gain(self):
        """LT gains preferred over ST gains (preferential tax rate)."""
        lt_gain = make_lot(1, 10, LT_PURCHASE, 200.0)
        st_gain = make_lot(2, 10, ST_PURCHASE, 200.0)
        sel = select_lots_to_sell("VTI", 5, [lt_gain, st_gain], SALE_DATE, 250.0)
        assert sel[0].lot_id == 1  # LT gain

    def test_lt_gain_picked_even_at_higher_basis(self):
        """LT gain wins on bucket priority even when ST gain has higher basis."""
        lt_gain_low_basis = make_lot(1, 10, LT_PURCHASE, 100.0)
        st_gain_high_basis = make_lot(2, 10, ST_PURCHASE, 240.0)
        sel = select_lots_to_sell(
            "VTI", 5, [lt_gain_low_basis, st_gain_high_basis], SALE_DATE, 250.0
        )
        assert sel[0].lot_id == 1  # LT gain bucket


# --- Within-bucket ordering ------------------------------------------------


class TestWithinLossBucket:
    def test_largest_loss_first_in_st_loss_bucket(self):
        """Within ST losses, largest loss per share first."""
        small_loss = make_lot(1, 10, ST_PURCHASE, 260.0)   # $10 loss/share
        big_loss = make_lot(2, 10, ST_PURCHASE, 400.0)     # $150 loss/share
        med_loss = make_lot(3, 10, ST_PURCHASE, 300.0)     # $50 loss/share
        sel = select_lots_to_sell(
            "VTI", 30, [small_loss, big_loss, med_loss], SALE_DATE, 250.0
        )
        assert [s.lot_id for s in sel] == [2, 3, 1]

    def test_largest_loss_first_in_lt_loss_bucket(self):
        small_loss = make_lot(1, 10, LT_PURCHASE, 260.0)
        big_loss = make_lot(2, 10, LT_PURCHASE, 400.0)
        sel = select_lots_to_sell("VTI", 20, [small_loss, big_loss], SALE_DATE, 250.0)
        assert [s.lot_id for s in sel] == [2, 1]


class TestWithinGainBucket:
    def test_hifo_within_lt_gains(self):
        """Among LT gains, HIFO — highest cost basis first."""
        low_basis = make_lot(1, 10, LT_PURCHASE, 100.0)    # $150 gain/share
        high_basis = make_lot(2, 10, LT_PURCHASE, 240.0)   # $10 gain/share
        med_basis = make_lot(3, 10, LT_PURCHASE, 200.0)    # $50 gain/share
        sel = select_lots_to_sell(
            "VTI", 30, [low_basis, high_basis, med_basis], SALE_DATE, 250.0
        )
        # HIFO = highest basis first = smallest realized gain
        assert [s.lot_id for s in sel] == [2, 3, 1]

    def test_hifo_within_st_gains(self):
        low_basis = make_lot(1, 10, ST_PURCHASE, 100.0)
        high_basis = make_lot(2, 10, ST_PURCHASE, 240.0)
        sel = select_lots_to_sell("VTI", 20, [low_basis, high_basis], SALE_DATE, 250.0)
        assert [s.lot_id for s in sel] == [2, 1]


# --- Multi-bucket end-to-end ordering --------------------------------------


class TestFullOrdering:
    def test_all_four_buckets_correct_priority(self):
        """All four bucket types present — verify exact order."""
        lots = [
            make_lot(1, 10, LT_PURCHASE, 100.0),    # LT gain ($150/share)
            make_lot(2, 10, ST_PURCHASE, 100.0),    # ST gain ($150/share)
            make_lot(3, 10, LT_PURCHASE, 400.0),    # LT loss ($150/share)
            make_lot(4, 10, ST_PURCHASE, 400.0),    # ST loss ($150/share)
        ]
        sel = select_lots_to_sell("VTI", 40, lots, SALE_DATE, 250.0)
        # Expected order: ST loss → LT loss → LT gain → ST gain
        assert [s.lot_id for s in sel] == [4, 3, 1, 2]


# --- Spanning lots ---------------------------------------------------------


class TestSpanning:
    def test_spans_two_lots(self):
        lot1 = make_lot(1, 10, ST_PURCHASE, 300.0)  # ST loss
        lot2 = make_lot(2, 10, LT_PURCHASE, 300.0)  # LT loss
        sel = select_lots_to_sell("VTI", 15, [lot1, lot2], SALE_DATE, 250.0)
        assert sel == [
            LotSelection(lot_id=1, qty=10),
            LotSelection(lot_id=2, qty=5),
        ]

    def test_spans_across_bucket_boundary(self):
        st_loss = make_lot(1, 5, ST_PURCHASE, 300.0)
        lt_gain = make_lot(2, 10, LT_PURCHASE, 100.0)
        # Need 8 — first 5 from ST loss, then 3 from LT gain
        sel = select_lots_to_sell("VTI", 8, [st_loss, lt_gain], SALE_DATE, 250.0)
        assert sel == [
            LotSelection(lot_id=1, qty=5),
            LotSelection(lot_id=2, qty=3),
        ]


# --- Edge cases ------------------------------------------------------------


class TestEdgeCases:
    def test_break_even_lot_classified_as_gain(self):
        """current_price == cost_basis: not a loss, falls into gain bucket."""
        breakeven = make_lot(1, 10, LT_PURCHASE, 250.0)
        loss = make_lot(2, 10, LT_PURCHASE, 300.0)
        sel = select_lots_to_sell("VTI", 5, [breakeven, loss], SALE_DATE, 250.0)
        assert sel[0].lot_id == 2  # loss preferred over break-even

    def test_zero_remaining_qty_lots_filtered_out(self):
        """Lots with remaining_qty == 0 are ignored."""
        consumed = Lot(
            lot_id=1, symbol="VTI",
            original_qty=10, remaining_qty=0,
            purchase_date=ST_PURCHASE,
            cost_basis_per_share=200.0,
            is_synthetic=False,
        )
        active = make_lot(2, 10, ST_PURCHASE, 200.0)
        sel = select_lots_to_sell("VTI", 5, [consumed, active], SALE_DATE, 250.0)
        assert sel == [LotSelection(lot_id=2, qty=5)]

    def test_lt_st_boundary_at_366_days(self):
        """Lot purchased 366 days ago is long-term; 365 days is short-term."""
        sale = date(2025, 6, 1)
        lt_lot = make_lot(1, 10, date(2024, 5, 31), 200.0)  # 366 days = LT
        st_lot = make_lot(2, 10, date(2024, 6, 1), 200.0)   # 365 days = ST
        # Both are gains (current 250 > basis 200). LT preferred.
        sel = select_lots_to_sell("VTI", 5, [st_lot, lt_lot], sale, 250.0)
        assert sel[0].lot_id == 1  # LT lot

    def test_partial_consumption_uses_remaining_only(self):
        """Already-partially-consumed lot uses remaining_qty, not original_qty."""
        partial = Lot(
            lot_id=1, symbol="VTI",
            original_qty=10, remaining_qty=3,
            purchase_date=ST_PURCHASE,
            cost_basis_per_share=200.0,
            is_synthetic=False,
        )
        full = make_lot(2, 10, ST_PURCHASE, 200.0)
        # Both gains (current 250 > basis 200), HIFO ties → order by lot_id
        sel = select_lots_to_sell("VTI", 5, [partial, full], SALE_DATE, 250.0)
        # Should take 3 from partial, then 2 from full
        total = sum(s.qty for s in sel)
        assert total == pytest.approx(5)


# --- Property: total qty matches request -----------------------------------


class TestTotalQtyInvariant:
    def test_selection_total_equals_request_when_filled(self):
        lots = [
            make_lot(1, 10, LT_PURCHASE, 100.0),
            make_lot(2, 7, ST_PURCHASE, 200.0),
            make_lot(3, 5, LT_PURCHASE, 400.0),
        ]
        for qty in [1, 5, 10, 15, 22]:
            sel = select_lots_to_sell("VTI", qty, lots, SALE_DATE, 250.0)
            assert sum(s.qty for s in sel) == pytest.approx(qty)

    def test_no_lot_selected_more_than_remaining(self):
        lots = [
            make_lot(1, 5, ST_PURCHASE, 300.0),
            make_lot(2, 7, LT_PURCHASE, 300.0),
        ]
        sel = select_lots_to_sell("VTI", 10, lots, SALE_DATE, 250.0)
        # Verify no selection exceeds the lot's remaining_qty
        by_id = {l.lot_id: l for l in lots}
        for s in sel:
            assert s.qty <= by_id[s.lot_id].remaining_qty + 1e-9


# --- estimate_realized_pnl -------------------------------------------------


class TestEstimateRealizedPnL:
    def test_simple_gain(self):
        lot = make_lot(1, 10, LT_PURCHASE, 200.0)
        sel = [LotSelection(lot_id=1, qty=5)]
        # Selling 5 shares at 250 with basis 200 = $50/share gain × 5 = $250
        assert estimate_realized_pnl(sel, [lot], 250.0) == pytest.approx(250.0)

    def test_simple_loss(self):
        lot = make_lot(1, 10, LT_PURCHASE, 300.0)
        sel = [LotSelection(lot_id=1, qty=5)]
        # Selling 5 at 250 with basis 300 = -$50/share × 5 = -$250
        assert estimate_realized_pnl(sel, [lot], 250.0) == pytest.approx(-250.0)

    def test_multiple_lots(self):
        lot1 = make_lot(1, 10, LT_PURCHASE, 200.0)  # gain at 250
        lot2 = make_lot(2, 10, ST_PURCHASE, 300.0)  # loss at 250
        sel = [
            LotSelection(lot_id=1, qty=5),  # +$250
            LotSelection(lot_id=2, qty=5),  # -$250
        ]
        assert estimate_realized_pnl(sel, [lot1, lot2], 250.0) == pytest.approx(0.0)


# --- split_realized_pnl ----------------------------------------------------


class TestSplitRealizedPnL:
    def test_breakdown_by_term_and_sign(self):
        lots = [
            make_lot(1, 10, LT_PURCHASE, 200.0),  # LT gain at 250: +$50/share
            make_lot(2, 10, LT_PURCHASE, 300.0),  # LT loss at 250: -$50/share
            make_lot(3, 10, ST_PURCHASE, 200.0),  # ST gain at 250: +$50/share
            make_lot(4, 10, ST_PURCHASE, 300.0),  # ST loss at 250: -$50/share
        ]
        sel = [LotSelection(l.lot_id, 5) for l in lots]
        out = split_realized_pnl(sel, lots, SALE_DATE, 250.0)
        assert out["lt_gain"] == pytest.approx(250.0)
        assert out["lt_loss"] == pytest.approx(250.0)
        assert out["st_gain"] == pytest.approx(250.0)
        assert out["st_loss"] == pytest.approx(250.0)

    def test_empty_selections_returns_zeros(self):
        out = split_realized_pnl([], [], SALE_DATE, 250.0)
        assert out == {"st_gain": 0.0, "st_loss": 0.0, "lt_gain": 0.0, "lt_loss": 0.0}

    def test_loss_values_are_positive_magnitudes(self):
        """Losses are reported as positive magnitudes, not negative numbers."""
        lot = make_lot(1, 10, LT_PURCHASE, 300.0)
        sel = [LotSelection(lot_id=1, qty=10)]  # $50/share loss × 10 = $500 loss
        out = split_realized_pnl(sel, [lot], SALE_DATE, 250.0)
        assert out["lt_loss"] == pytest.approx(500.0)
        assert out["lt_gain"] == 0.0
