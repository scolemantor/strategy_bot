"""Risk manager: validates orders before they reach the broker.

Has authority to:
  - reject individual orders that violate per-order limits
  - halt the entire batch (kill switch) for system-wide conditions

Returning halt=True must stop execution. There is no override path here
on purpose - if the bot wants to override a halt, the human does it by
adjusting config and rerunning.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .broker import Account
from .config import StrategyConfig
from .strategy import RebalanceOrder


@dataclass(frozen=True)
class RiskCheckResult:
    approved: List[RebalanceOrder]
    rejected: List[Tuple[RebalanceOrder, str]] = field(default_factory=list)
    halt: bool = False
    halt_reason: Optional[str] = None


def check_orders(
    orders: List[RebalanceOrder],
    account: Account,
    cfg: StrategyConfig,
    market_open: bool,
    portfolio_high_water_mark: Optional[float] = None,
    seeding_mode: bool = False,
) -> RiskCheckResult:
    """Run all pre-execution risk checks.

    Halts apply to the whole batch. Per-order rejections only filter that order.

    seeding_mode=True bypasses the per-order size limit only. All kill switches
    (market hours, drawdown, max daily orders) still apply. Use only for
    initial portfolio seeding, never for ongoing operation.
    """

    if cfg.risk.require_market_hours and not market_open:
        return RiskCheckResult(
            approved=[],
            rejected=[(o, "market closed") for o in orders],
            halt=True,
            halt_reason="market_closed",
        )

    if portfolio_high_water_mark is not None and portfolio_high_water_mark > 0:
        drawdown = (portfolio_high_water_mark - account.portfolio_value) / portfolio_high_water_mark
        if drawdown > cfg.risk.drawdown_kill_switch_pct:
            return RiskCheckResult(
                approved=[],
                rejected=[(o, f"drawdown {drawdown:.1%} exceeds limit {cfg.risk.drawdown_kill_switch_pct:.1%}") for o in orders],
                halt=True,
                halt_reason=f"drawdown_{drawdown:.2%}",
            )

    if len(orders) > cfg.risk.max_daily_orders:
        return RiskCheckResult(
            approved=[],
            rejected=[(o, f"batch size {len(orders)} exceeds max_daily_orders {cfg.risk.max_daily_orders}") for o in orders],
            halt=True,
            halt_reason="too_many_orders",
        )

    approved: List[RebalanceOrder] = []
    rejected: List[Tuple[RebalanceOrder, str]] = []
    max_order_value = account.portfolio_value * cfg.risk.max_order_pct_of_portfolio

    for order in orders:
        if not seeding_mode and abs(order.delta_value) > max_order_value:
            rejected.append((
                order,
                f"order ${abs(order.delta_value):,.0f} exceeds max ${max_order_value:,.0f} "
                f"({cfg.risk.max_order_pct_of_portfolio:.0%} of portfolio)",
            ))
            continue
        if order.side == "sell" and order.current_value <= 0:
            rejected.append((order, "cannot sell - no position held"))
            continue
        approved.append(order)

    return RiskCheckResult(approved=approved, rejected=rejected, halt=False)