"""Lot ledger for tax-aware rebalancing.

Tracks per-purchase lot data (qty, date, cost basis) so the rebalancer can
make tax-efficient sell decisions: HIFO within long-term holdings, prefer
losses before gains, avoid forced short-term sells when possible.

Architecture:
  - Two tables and one view in SQLite.
  - `lots` is append-only and conceptually immutable: each row is one
    purchase. Original qty never changes.
  - `lot_consumptions` is append-only: each row records selling some qty
    from a specific lot.
  - `lot_remaining` is a view that computes the open qty per lot as
    original_qty - sum(consumptions). This gives a full audit trail —
    no row is ever updated or deleted, so the database serves as both
    operational state and tax history.

Concurrency:
  - SQLite serializes single-writer access natively. The bot is a single
    process so concurrent writes are not expected.
  - Each method opens its own short-lived connection. If we ever need
    cross-method transactions, refactor to a context-managed connection.

Migration:
  - Schema is created on first construction and is idempotent.
  - When seeding from existing Alpaca positions, use is_synthetic=True
    to mark synthetic lots so reconciliation can distinguish them.
"""
from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterator, List, Optional

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Lot:
    """A tax lot — the record of one purchase of a symbol.

    `remaining_qty` reflects how much of the original purchase is still held
    (i.e., not yet consumed by a sale). When `remaining_qty == 0`, the lot
    is fully consumed and exists only as audit history.
    """
    lot_id: int
    symbol: str
    original_qty: float
    remaining_qty: float
    purchase_date: date
    cost_basis_per_share: float
    is_synthetic: bool
    notes: Optional[str] = None

    def is_long_term_at(self, sale_date: date) -> bool:
        """True if the holding period at sale_date qualifies for long-term
        capital gains treatment.

        IRS rule: long-term applies when the holding period is MORE than one
        year. The standard interpretation is that the asset must be held for
        at least one year and one day. We implement this as `(sale - purchase).days > 365`,
        which is the safer side of the boundary.
        """
        return (sale_date - self.purchase_date).days > 365

    def cost_basis_remaining(self) -> float:
        """Total cost basis for the un-sold portion of this lot."""
        return self.remaining_qty * self.cost_basis_per_share

    def unrealized_pnl_at(self, current_price: float) -> float:
        """Unrealized P/L on the un-sold portion of this lot at current_price."""
        return self.remaining_qty * (current_price - self.cost_basis_per_share)


@dataclass(frozen=True)
class Consumption:
    """A record of selling some qty from a lot."""
    consumption_id: int
    lot_id: int
    qty: float
    sale_date: date
    sale_price_per_share: float
    notes: Optional[str] = None


# Tolerance for float equality when checking "consume exactly N from a lot
# with N remaining" — guards against fp accumulation in the qty math.
_QTY_EPS = 1e-9


class LotLedger:
    """SQLite-backed tax lot ledger.

    Standard usage:
        ledger = LotLedger("/path/to/lot_ledger.sqlite")
        lot_id = ledger.insert_lot("VTI", 10, date.today(), 245.30)
        ledger.consume_lot(lot_id, 4, date.today(), 270.00)

    The constructor creates the schema if needed; reopening an existing
    database is safe.
    """

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS lots (
            lot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            original_qty REAL NOT NULL CHECK (original_qty > 0),
            purchase_date TEXT NOT NULL,
            cost_basis_per_share REAL NOT NULL CHECK (cost_basis_per_share >= 0),
            is_synthetic INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS lot_consumptions (
            consumption_id INTEGER PRIMARY KEY AUTOINCREMENT,
            lot_id INTEGER NOT NULL REFERENCES lots(lot_id),
            qty REAL NOT NULL CHECK (qty > 0),
            sale_date TEXT NOT NULL,
            sale_price_per_share REAL NOT NULL CHECK (sale_price_per_share >= 0),
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            notes TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_lots_symbol ON lots(symbol);
        CREATE INDEX IF NOT EXISTS idx_consumptions_lot_id ON lot_consumptions(lot_id);

        CREATE VIEW IF NOT EXISTS lot_remaining AS
        SELECT
            l.lot_id,
            l.symbol,
            l.original_qty,
            l.original_qty - COALESCE((
                SELECT SUM(c.qty) FROM lot_consumptions c WHERE c.lot_id = l.lot_id
            ), 0) AS remaining_qty,
            l.purchase_date,
            l.cost_basis_per_share,
            l.is_synthetic,
            l.notes,
            l.created_at
        FROM lots l;
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(self.SCHEMA)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        """Open a connection, commit on success, rollback on failure, close always."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def insert_lot(
        self,
        symbol: str,
        qty: float,
        purchase_date: date,
        cost_basis_per_share: float,
        is_synthetic: bool = False,
        notes: Optional[str] = None,
    ) -> int:
        """Insert a new lot. Returns the assigned lot_id.

        Raises ValueError on invalid inputs (qty <= 0 or cost basis < 0).
        """
        if qty <= 0:
            raise ValueError(f"Lot qty must be positive, got {qty}")
        if cost_basis_per_share < 0:
            raise ValueError(
                f"Cost basis per share must be non-negative, got {cost_basis_per_share}"
            )
        with self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO lots
                   (symbol, original_qty, purchase_date, cost_basis_per_share, is_synthetic, notes)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    symbol,
                    qty,
                    purchase_date.isoformat(),
                    cost_basis_per_share,
                    1 if is_synthetic else 0,
                    notes,
                ),
            )
            return int(cur.lastrowid)

    def consume_lot(
        self,
        lot_id: int,
        qty: float,
        sale_date: date,
        sale_price_per_share: float,
        notes: Optional[str] = None,
    ) -> int:
        """Record selling `qty` shares from lot `lot_id`.

        Validates that the consumption doesn't exceed remaining qty (with a
        small tolerance for float precision). Returns consumption_id.
        Raises ValueError on validation failure.
        """
        if qty <= 0:
            raise ValueError(f"Consumption qty must be positive, got {qty}")
        if sale_price_per_share < 0:
            raise ValueError(
                f"Sale price must be non-negative, got {sale_price_per_share}"
            )

        with self._conn() as conn:
            row = conn.execute(
                "SELECT remaining_qty FROM lot_remaining WHERE lot_id = ?",
                (lot_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Lot {lot_id} not found")
            remaining = float(row["remaining_qty"])
            if qty > remaining + _QTY_EPS:
                raise ValueError(
                    f"Cannot consume {qty} from lot {lot_id}: only {remaining} remaining"
                )
            cur = conn.execute(
                """INSERT INTO lot_consumptions
                   (lot_id, qty, sale_date, sale_price_per_share, notes)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    lot_id,
                    qty,
                    sale_date.isoformat(),
                    sale_price_per_share,
                    notes,
                ),
            )
            return int(cur.lastrowid)

    def get_open_lots(self, symbol: str) -> List[Lot]:
        """Return all lots with remaining_qty > 0 for a symbol, ordered by
        purchase_date ascending then lot_id ascending."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM lot_remaining
                   WHERE symbol = ? AND remaining_qty > ?
                   ORDER BY purchase_date ASC, lot_id ASC""",
                (symbol, _QTY_EPS),
            ).fetchall()
            return [self._row_to_lot(r) for r in rows]

    def get_all_lots(self, symbol: str) -> List[Lot]:
        """Return ALL lots for a symbol, including fully consumed ones (audit)."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM lot_remaining
                   WHERE symbol = ?
                   ORDER BY purchase_date ASC, lot_id ASC""",
                (symbol,),
            ).fetchall()
            return [self._row_to_lot(r) for r in rows]

    def get_total_qty(self, symbol: str) -> float:
        """Return total open qty across all lots for a symbol."""
        with self._conn() as conn:
            row = conn.execute(
                """SELECT COALESCE(SUM(remaining_qty), 0) AS total
                   FROM lot_remaining
                   WHERE symbol = ? AND remaining_qty > ?""",
                (symbol, _QTY_EPS),
            ).fetchone()
            return float(row["total"]) if row else 0.0

    def get_consumptions(self, lot_id: int) -> List[Consumption]:
        """Return all consumptions against a specific lot, ordered by sale_date."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM lot_consumptions
                   WHERE lot_id = ?
                   ORDER BY sale_date ASC, consumption_id ASC""",
                (lot_id,),
            ).fetchall()
            return [
                Consumption(
                    consumption_id=int(r["consumption_id"]),
                    lot_id=int(r["lot_id"]),
                    qty=float(r["qty"]),
                    sale_date=date.fromisoformat(r["sale_date"]),
                    sale_price_per_share=float(r["sale_price_per_share"]),
                    notes=r["notes"],
                )
                for r in rows
            ]

    def all_symbols(self) -> List[str]:
        """Return distinct symbols present in the ledger (any lot, open or closed)."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT symbol FROM lots ORDER BY symbol"
            ).fetchall()
            return [r["symbol"] for r in rows]

    def all_open_symbols(self) -> List[str]:
        """Return distinct symbols with at least one open lot."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT DISTINCT symbol FROM lot_remaining
                   WHERE remaining_qty > ?
                   ORDER BY symbol""",
                (_QTY_EPS,),
            ).fetchall()
            return [r["symbol"] for r in rows]

    @staticmethod
    def _row_to_lot(row: sqlite3.Row) -> Lot:
        return Lot(
            lot_id=int(row["lot_id"]),
            symbol=str(row["symbol"]),
            original_qty=float(row["original_qty"]),
            remaining_qty=float(row["remaining_qty"]),
            purchase_date=date.fromisoformat(row["purchase_date"]),
            cost_basis_per_share=float(row["cost_basis_per_share"]),
            is_synthetic=bool(row["is_synthetic"]),
            notes=row["notes"],
        )
