"""Tax-aware lot selection for sell decisions.

Pure decision logic. Given a request to sell N shares of a symbol and a list
of open lots, return an ordered list of (lot_id, qty) instructions that
minimize the tax burden of the sale.

This module reads `Lot` objects from `lot_ledger` but does NOT write back
to the ledger. The caller commits the selection by calling
`ledger.consume_lot(...)` for each `LotSelection` returned. This separation
lets the rebalancer preview a selection before executing, supports dry runs,
and keeps the algorithm pure and testable in isolation from SQLite.

Selection priority (most-preferred-to-sell first):
  1. Short-term losses, biggest loss per share first.
     ST losses offset ST gains at ordinary income rates (highest tax benefit
     per dollar of loss). Harvesting biggest losses first maximizes deduction.
  2. Long-term losses, biggest loss per share first.
     LT losses get preferential-rate deduction. Still always preferable to
     realizing any gain.
  3. Long-term gains, HIFO (highest cost basis per share first).
     Among gains, prefer LT (preferential rate). HIFO minimizes the size
     of the realized gain.
  4. Short-term gains, HIFO.
     Last resort. Ordinary income rate.

Edge cases:
  - qty_to_sell == 0: returns empty list.
  - qty_to_sell > total available: raises InsufficientLotsError.
  - Empty lots list with positive qty: raises InsufficientLotsError.
  - Lot at exact break-even (current_price == cost_basis): classified as
    gain (no loss to harvest, included in gain HIFO ordering).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import List

from .lot_ledger import Lot, _QTY_EPS


@dataclass(frozen=True)
class LotSelection:
    """One sell instruction: take this much from this lot."""
    lot_id: int
    qty: float


class InsufficientLotsError(Exception):
    """Raised when requested sell qty exceeds total available across open lots."""

    def __init__(self, symbol: str, requested: float, available: float):
        self.symbol = symbol
        self.requested = requested
        self.available = available
        super().__init__(
            f"Cannot sell {requested} shares of {symbol}: "
            f"only {available} available across all open lots"
        )


def select_lots_to_sell(
    symbol: str,
    qty_to_sell: float,
    lots: List[Lot],
    sale_date: date,
    current_price: float,
) -> List[LotSelection]:
    """Pick which lots to sell to fill `qty_to_sell` shares of `symbol`.

    Returns an ordered list of LotSelection instructions. The caller commits
    the sale by calling ledger.consume_lot for each selection in order.

    Args:
        symbol: The symbol being sold (used in error messages).
        qty_to_sell: Total shares to sell. If <= 0, returns empty list.
        lots: All open lots for the symbol. Lots with remaining_qty <= 0
            are filtered out.
        sale_date: Date of the planned sale. Used to classify each lot as
            long-term or short-term based on its purchase_date.
        current_price: Estimated sale price per share. Used to classify
            each lot as gain (current >= cost_basis) or loss (current <
            cost_basis).

    Raises:
        InsufficientLotsError: If qty_to_sell exceeds total open qty.
    """
    if qty_to_sell <= 0:
        return []

    open_lots = [l for l in lots if l.remaining_qty > _QTY_EPS]
    total_open = sum(l.remaining_qty for l in open_lots)
    if qty_to_sell > total_open + _QTY_EPS:
        raise InsufficientLotsError(symbol, qty_to_sell, total_open)

    # Bucket lots into the four categories: {ST,LT} x {loss,gain}.
    # Break-even (current_price == cost_basis) is treated as gain — no loss
    # to harvest, falls into HIFO ordering.
    st_losses: List[Lot] = []
    lt_losses: List[Lot] = []
    lt_gains: List[Lot] = []
    st_gains: List[Lot] = []
    for lot in open_lots:
        is_long_term = lot.is_long_term_at(sale_date)
        is_loss = current_price < lot.cost_basis_per_share
        if is_loss and is_long_term:
            lt_losses.append(lot)
        elif is_loss:
            st_losses.append(lot)
        elif is_long_term:
            lt_gains.append(lot)
        else:
            st_gains.append(lot)

    # Sort each bucket.
    # Losses: largest loss per share first (cost_basis - current_price, descending).
    # Gains: HIFO (cost_basis_per_share descending — selling highest basis
    #        produces smallest realized gain).
    loss_size = lambda l: l.cost_basis_per_share - current_price
    st_losses.sort(key=loss_size, reverse=True)
    lt_losses.sort(key=loss_size, reverse=True)
    lt_gains.sort(key=lambda l: l.cost_basis_per_share, reverse=True)
    st_gains.sort(key=lambda l: l.cost_basis_per_share, reverse=True)

    ordered = st_losses + lt_losses + lt_gains + st_gains

    # Walk the ordered candidates, taking from each until the request is filled.
    selections: List[LotSelection] = []
    remaining = qty_to_sell
    for lot in ordered:
        if remaining <= _QTY_EPS:
            break
        take = min(lot.remaining_qty, remaining)
        selections.append(LotSelection(lot_id=lot.lot_id, qty=take))
        remaining -= take

    return selections


def estimate_realized_pnl(
    selections: List[LotSelection],
    lots: List[Lot],
    current_price: float,
) -> float:
    """Compute total realized P/L if `selections` were executed at `current_price`.

    Useful for previewing the tax impact of a planned sale. Positive = gain,
    negative = loss.

    Lots not present in `lots` (by lot_id) are silently skipped. Caller is
    responsible for passing the same lots used to generate the selections.
    """
    by_id = {l.lot_id: l for l in lots}
    pnl = 0.0
    for sel in selections:
        lot = by_id.get(sel.lot_id)
        if lot is None:
            continue
        pnl += sel.qty * (current_price - lot.cost_basis_per_share)
    return pnl


def split_realized_pnl(
    selections: List[LotSelection],
    lots: List[Lot],
    sale_date: date,
    current_price: float,
) -> dict:
    """Break realized P/L from `selections` into ST gain, ST loss, LT gain, LT loss.

    Returns a dict with keys: 'st_gain', 'st_loss', 'lt_gain', 'lt_loss'.
    All values are non-negative magnitudes (loss values are reported as
    positive numbers, e.g. lt_loss=500 means $500 of long-term losses
    realized).
    """
    by_id = {l.lot_id: l for l in lots}
    out = {"st_gain": 0.0, "st_loss": 0.0, "lt_gain": 0.0, "lt_loss": 0.0}
    for sel in selections:
        lot = by_id.get(sel.lot_id)
        if lot is None:
            continue
        pnl = sel.qty * (current_price - lot.cost_basis_per_share)
        is_lt = lot.is_long_term_at(sale_date)
        if pnl >= 0:
            out["lt_gain" if is_lt else "st_gain"] += pnl
        else:
            out["lt_loss" if is_lt else "st_loss"] += -pnl
    return out
