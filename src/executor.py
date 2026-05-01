"""Order execution: route approved orders to the broker.

Defaults to dry_run=True so you can never accidentally place orders just by
calling execute_orders(). The CLI explicitly opts in via --execute.
"""
from __future__ import annotations

import logging
from typing import List

from .broker import AlpacaBroker, OrderResult
from .strategy import RebalanceOrder

log = logging.getLogger(__name__)


def execute_orders(
    orders: List[RebalanceOrder],
    broker: AlpacaBroker,
    dry_run: bool = True,
) -> List[OrderResult]:
    """Submit each order. Errors do not stop the batch - they are reported per-order."""
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
        else:
            log.info(f"Order accepted: id={result.order_id} status={result.status}")
        results.append(result)
    return results
