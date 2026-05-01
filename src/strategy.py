"""Oak rebalancer: target weights and rebalance order generation.

Pure functions, no I/O. All inputs flow in via arguments, all outputs flow out
via return values. This makes the strategy trivially testable and replaceable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from .broker import Position
from .config import StrategyConfig


@dataclass(frozen=True)
class TargetHolding:
    """Snapshot of one holding's current vs. target state."""
    symbol: str
    sleeve: str
    target_value: float
    current_value: float
    drift_pct: float  # (current - target) / target; positive means overweight


@dataclass(frozen=True)
class RebalanceOrder:
    """An order proposed by the strategy. Sizing in dollars; quantity is estimated."""
    symbol: str
    side: str             # "buy" or "sell"
    target_value: float
    current_value: float
    delta_value: float    # target - current; positive means buy
    estimated_qty: float


def _sleeve_for_symbol(symbol: str, cfg: StrategyConfig) -> str:
    if symbol in cfg.allocation.trunk.holdings:
        return "trunk"
    if symbol in cfg.allocation.branches.holdings:
        return "branches"
    return "untracked"


def compute_target_values(
    portfolio_value: float,
    cfg: StrategyConfig,
) -> Dict[str, float]:
    """Map each tracked symbol to its target dollar value.

    Acorns sleeve is intentionally excluded - it is held as cash in Phase 1
    and managed manually.
    """
    targets: Dict[str, float] = {}
    trunk_value = portfolio_value * cfg.allocation.trunk.weight
    for symbol, weight in cfg.allocation.trunk.holdings.items():
        targets[symbol] = trunk_value * weight
    branches_value = portfolio_value * cfg.allocation.branches.weight
    for symbol, weight in cfg.allocation.branches.holdings.items():
        targets[symbol] = branches_value * weight
    return targets


def compute_holding_status(
    positions: Dict[str, Position],
    targets: Dict[str, float],
    cfg: StrategyConfig,
) -> List[TargetHolding]:
    """For each tracked symbol, return its current vs target state."""
    statuses: List[TargetHolding] = []
    for symbol, target in targets.items():
        current = positions[symbol].market_value if symbol in positions else 0.0
        drift = (current - target) / target if target > 0 else 0.0
        statuses.append(TargetHolding(
            symbol=symbol,
            sleeve=_sleeve_for_symbol(symbol, cfg),
            target_value=target,
            current_value=current,
            drift_pct=drift,
        ))
    return statuses


def compute_rebalance_orders(
    positions: Dict[str, Position],
    portfolio_value: float,
    quotes: Dict[str, float],
    cfg: StrategyConfig,
) -> List[RebalanceOrder]:
    """Generate rebalance orders for holdings outside the drift threshold.

    Skips orders below the minimum size to avoid trade-cost drag and
    rounding noise.
    """
    targets = compute_target_values(portfolio_value, cfg)
    statuses = compute_holding_status(positions, targets, cfg)

    orders: List[RebalanceOrder] = []
    for status in statuses:
        # If we have nothing AND target is positive, treat as a 100% drift (need to buy in)
        if status.current_value == 0 and status.target_value > 0:
            drift_significant = True
        else:
            drift_significant = abs(status.drift_pct) >= cfg.rebalance.drift_threshold

        if not drift_significant:
            continue

        delta = status.target_value - status.current_value
        if abs(delta) < cfg.rebalance.min_order_size_usd:
            continue

        price = quotes.get(status.symbol, 0)
        if price <= 0:
            continue  # can't size without a price

        side = "buy" if delta > 0 else "sell"
        qty = round(abs(delta) / price, 4)
        if qty <= 0:
            continue

        orders.append(RebalanceOrder(
            symbol=status.symbol,
            side=side,
            target_value=status.target_value,
            current_value=status.current_value,
            delta_value=delta,
            estimated_qty=qty,
        ))
    return orders
