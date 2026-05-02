"""Migration and reconciliation between LotLedger and broker state.

Bridges the local lot ledger with whatever the broker reports as actual
held positions. Two operations:

  1. seed_from_broker: Initial migration. For each symbol the broker holds
     but the ledger has no lots for, create one synthetic lot at the
     broker's reported avg_entry_price, dated `seed_date`. Idempotent — safe
     to call on a partially seeded ledger.

  2. reconcile_with_broker: Sanity check. Compare ledger-total qty to
     broker-reported qty for every symbol either side knows about.
     Returns a structured ReconciliationReport; the caller decides how to
     react. The recommended default is to halt the rebalancer on any
     mismatch.

Architecture:
  - Functions accept `positions: Dict[str, Position]` rather than a broker
    object. This decouples the migration code from the Alpaca SDK, keeps
    tests simple, and lets the caller fetch positions however they like.
  - All synthetic lots are tagged is_synthetic=True so future analysis can
    distinguish migration seeds from real fills.
  - seed_from_broker never updates an existing lot. By default
    (only_missing=True), it only inserts for symbols with no existing lots
    of any kind in the ledger.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from typing import Dict, List

from .broker import Position
from .lot_ledger import LotLedger, _QTY_EPS

log = logging.getLogger(__name__)


# --- Migration -------------------------------------------------------------


@dataclass(frozen=True)
class SeedResult:
    """Outcome of a seed_from_broker call."""
    seeded_symbols: List[str]
    skipped_symbols: List[str]  # already had lots, or zero-qty positions
    created_lot_ids: List[int]


def seed_from_broker(
    ledger: LotLedger,
    positions: Dict[str, Position],
    seed_date: date,
    only_missing: bool = True,
) -> SeedResult:
    """Seed synthetic lots in the ledger from current broker positions.

    For each symbol in `positions`:
      - If position qty <= 0, skip (broker reports no holding).
      - If `only_missing=True` (default) and the ledger already has any lots
        for that symbol (open or closed), skip. This makes the operation
        idempotent.
      - Otherwise create a single synthetic lot:
          qty = position.qty
          cost_basis_per_share = position.avg_entry_price
          purchase_date = seed_date
          is_synthetic = True
          notes = "seeded from broker on YYYY-MM-DD"

    For paper accounts, seed_date is typically date.today() since the dates
    are not real. For live accounts, seed_date should reflect the actual
    earliest purchase date (or the trade history should be replayed via
    individual insert_lot calls instead of using this helper).

    Returns SeedResult describing what happened.
    """
    seeded: List[str] = []
    skipped: List[str] = []
    created: List[int] = []

    for symbol in sorted(positions.keys()):
        pos = positions[symbol]
        if pos.qty <= _QTY_EPS:
            log.warning(f"Skipping {symbol}: broker reports zero qty")
            skipped.append(symbol)
            continue

        if only_missing:
            existing = ledger.get_all_lots(symbol)
            if existing:
                log.info(
                    f"Skipping {symbol}: ledger already has {len(existing)} lot(s)"
                )
                skipped.append(symbol)
                continue

        lot_id = ledger.insert_lot(
            symbol=symbol,
            qty=pos.qty,
            purchase_date=seed_date,
            cost_basis_per_share=pos.avg_entry_price,
            is_synthetic=True,
            notes=f"seeded from broker on {seed_date.isoformat()}",
        )
        seeded.append(symbol)
        created.append(lot_id)
        log.info(
            f"Seeded {symbol}: {pos.qty} shares @ "
            f"${pos.avg_entry_price:.4f}/share (lot_id={lot_id})"
        )

    return SeedResult(
        seeded_symbols=seeded,
        skipped_symbols=skipped,
        created_lot_ids=created,
    )


# --- Reconciliation --------------------------------------------------------


@dataclass(frozen=True)
class ReconciliationMismatch:
    """Records a single symbol where ledger qty differs from broker qty."""
    symbol: str
    ledger_qty: float
    broker_qty: float

    @property
    def delta(self) -> float:
        """broker_qty - ledger_qty.
        Positive = broker has more (manual buy, dividend reinvestment, split).
        Negative = ledger has more (manual sell, transfer out)."""
        return self.broker_qty - self.ledger_qty


@dataclass(frozen=True)
class ReconciliationReport:
    matched_symbols: List[str]
    mismatches: List[ReconciliationMismatch]

    @property
    def is_clean(self) -> bool:
        return len(self.mismatches) == 0

    def summary(self) -> str:
        """Human-readable summary suitable for logging or stderr."""
        if self.is_clean:
            return (
                f"Reconciliation clean across {len(self.matched_symbols)} symbol(s)"
            )
        lines = [f"Reconciliation has {len(self.mismatches)} mismatch(es):"]
        for m in self.mismatches:
            lines.append(
                f"  {m.symbol}: ledger={m.ledger_qty}, broker={m.broker_qty}, "
                f"delta={m.delta:+.6f}"
            )
        return "\n".join(lines)


def reconcile_with_broker(
    ledger: LotLedger,
    positions: Dict[str, Position],
    tolerance: float = _QTY_EPS,
) -> ReconciliationReport:
    """Compare ledger qty to broker qty for every symbol either side has.

    A "symbol" is checked when the broker has a non-zero position OR the
    ledger has any open lots. If either side reports the symbol but the
    other does not, that's a mismatch (broker_qty=0 or ledger_qty=0 on the
    side that doesn't have it).

    Returns a ReconciliationReport. Caller decides how to handle:
      - is_clean is True → continue safely.
      - is_clean is False → halt and ask user to investigate (recommended
        default per project convention).

    The tolerance defaults to _QTY_EPS to absorb floating-point noise from
    fractional share math. Increase it if you want to tolerate dust drift.
    """
    matched: List[str] = []
    mismatches: List[ReconciliationMismatch] = []

    # Set of symbols to check: union of broker-held and ledger-open.
    broker_symbols = {s for s, p in positions.items() if p.qty > _QTY_EPS}
    ledger_symbols = set(ledger.all_open_symbols())
    all_symbols = sorted(broker_symbols | ledger_symbols)

    for symbol in all_symbols:
        ledger_qty = ledger.get_total_qty(symbol)
        broker_qty = positions[symbol].qty if symbol in positions else 0.0

        if abs(ledger_qty - broker_qty) <= tolerance:
            matched.append(symbol)
        else:
            mismatches.append(
                ReconciliationMismatch(
                    symbol=symbol,
                    ledger_qty=ledger_qty,
                    broker_qty=broker_qty,
                )
            )

    return ReconciliationReport(
        matched_symbols=matched,
        mismatches=mismatches,
    )
