"""Order execution: route approved orders to the broker, optionally update lot ledger.

Defaults to dry_run=True so callers can never accidentally place orders just by
calling execute_orders(). The CLI explicitly opts in via --execute.

Ledger integration:
  - When `ledger` is provided AND dry_run=False, the ledger is updated after
    each successful broker submission:
      * Buy: insert a new lot at order.est_price, dated `today`.
      * Sell: select lots via select_lots_to_sell (HIFO + LT preference +
        loss-first), then call consume_lot for each selection.
  - Dry runs do NOT touch the ledger — preview only.
  - If the broker call fails, the ledger is not touched for that order.
  - If the ledger update fails AFTER a successful broker submission, the
    order's status gets a "_LEDGER_FAILED" suffix, an error is logged, and
    the batch continues. The next reconciliation run will catch the drift.

Limitation (paper-grade):
  - Ledger updates use order.estimated_qty and order.est_price (the latest
    quote at the time orders were computed), not the actual fill qty/price.
    For paper trading this is acceptable — fills are immediate and close
    to quote. For live money, this should be replaced with fill-status
    polling so the ledger reflects actual fills, not requested fills. The
    reconciliation step will detect persistent drift between the two.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import List, Optional

from .broker import AlpacaBroker, OrderResult
from .lot_ledger import LotLedger
from .strategy import RebalanceOrder
from .tax_lots import InsufficientLotsError, select_lots_to_sell

log = logging.getLogger(__name__)


def execute_orders(
    orders: List[RebalanceOrder],
    broker: AlpacaBroker,
    dry_run: bool = True,
    ledger: Optional[LotLedger] = None,
    today: Optional[date] = None,
) -> List[OrderResult]:
    """Submit each order. Errors do not stop the batch — they are reported per-order.

    Args:
        orders: Approved orders to execute.
        broker: Broker for order submission. Anything with a
            `place_market_order(symbol, qty, side) -> OrderResult` method works
            (the type hint is AlpacaBroker but duck typing is supported for tests).
        dry_run: If True, log what would happen and return synthetic dry_run
            OrderResults. The ledger is not touched.
        ledger: If provided AND dry_run=False, update ledger after each
            successful broker submission. If None, ledger updates are skipped
            (pre-Phase-3 behavior).
        today: Date assigned to ledger writes. Defaults to date.today().
            Override for testing.

    Returns:
        OrderResult per input order, in input order. Ledger failures appear
        as status with "_LEDGER_FAILED" suffix and a populated `error` field.
    """
    if today is None:
        today = date.today()

    results: List[OrderResult] = []
    for order in orders:
        if dry_run:
            log.info(
                f"DRY RUN: would {order.side} {order.estimated_qty} {order.symbol} "
                f"(target ${order.target_value:,.0f}, current ${order.current_value:,.0f}, "
                f"delta ${order.delta_value:+,.0f})"
            )
            results.append(OrderResult(
                symbol=order.symbol,
                side=order.side,
                qty=order.estimated_qty,
                status="dry_run",
                order_id="",
            ))
            continue

        log.info(f"Submitting {order.side} {order.estimated_qty} {order.symbol}")
        result = broker.place_market_order(
            symbol=order.symbol,
            qty=order.estimated_qty,
            side=order.side,
        )
        if result.error:
            log.error(f"Order failed for {order.symbol}: {result.error}")
            results.append(result)
            continue

        log.info(f"Order accepted: id={result.order_id} status={result.status}")

        # Broker submission succeeded. If ledger is configured, update it.
        if ledger is not None:
            try:
                if order.side == "buy":
                    _record_buy_in_ledger(ledger, order, today)
                else:
                    _record_sell_in_ledger(ledger, order, today)
            except Exception as e:
                log.error(
                    f"LEDGER UPDATE FAILED for {order.symbol} after successful "
                    f"broker submission (order_id={result.order_id}): {e}. "
                    f"MANUAL RECONCILIATION REQUIRED before next run."
                )
                # Replace the result with one flagging the ledger failure
                result = OrderResult(
                    symbol=result.symbol,
                    side=result.side,
                    qty=result.qty,
                    status=f"{result.status}_LEDGER_FAILED",
                    order_id=result.order_id,
                    submitted_at=result.submitted_at,
                    error=f"Ledger update failed: {e}",
                )

        results.append(result)
    return results


def _record_buy_in_ledger(
    ledger: LotLedger,
    order: RebalanceOrder,
    today: date,
) -> None:
    """Insert a new lot for a buy fill.

    Uses order.est_price as cost basis (latest quote at compute time, not
    actual fill price). is_synthetic=False because this is a real fill.
    """
    if order.est_price <= 0:
        raise ValueError(
            f"Cannot record buy for {order.symbol}: est_price={order.est_price}"
        )
    ledger.insert_lot(
        symbol=order.symbol,
        qty=order.estimated_qty,
        purchase_date=today,
        cost_basis_per_share=order.est_price,
        is_synthetic=False,
        notes=f"buy fill on {today.isoformat()}",
    )


def _record_sell_in_ledger(
    ledger: LotLedger,
    order: RebalanceOrder,
    today: date,
) -> None:
    """Consume lots for a sell fill, picking which lots via tax-aware selection.

    Uses order.est_price as the sale price. The selection algorithm is
    HIFO + LT preference + opportunistic loss harvesting (see tax_lots.py).
    """
    if order.est_price <= 0:
        raise ValueError(
            f"Cannot record sell for {order.symbol}: est_price={order.est_price}"
        )
    open_lots = ledger.get_open_lots(order.symbol)
    selections = select_lots_to_sell(
        symbol=order.symbol,
        qty_to_sell=order.estimated_qty,
        lots=open_lots,
        sale_date=today,
        current_price=order.est_price,
    )
    for sel in selections:
        ledger.consume_lot(
            lot_id=sel.lot_id,
            qty=sel.qty,
            sale_date=today,
            sale_price_per_share=order.est_price,
            notes=f"sell on {today.isoformat()}",
        )
