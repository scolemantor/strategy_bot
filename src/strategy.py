"""Oak rebalancer with volatility-weighted sizing and regime detection.

Pure functions, no I/O. Inputs flow in via arguments, outputs via return values.

The new behaviors:
  - Per-sleeve weighting_method: 'equal' uses config weights as-is; 'inverse_volatility'
    sizes each holding inversely to its trailing 90-day volatility, optionally biased
    by the value in the config holdings dict.
  - Regime overlay: if regime.enabled and the benchmark is below its moving average,
    scales total equity exposure down by offsignal_cash_pct.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .broker import Position
from .config import BranchesConfig, StrategyConfig, TrunkConfig

log = logging.getLogger(__name__)

VOL_WINDOW_DAYS = 90
MIN_WEIGHT_WITHIN_SLEEVE = 0.05
MAX_WEIGHT_WITHIN_SLEEVE = 0.40


@dataclass(frozen=True)
class TargetHolding:
    symbol: str
    sleeve: str
    target_value: float
    current_value: float
    drift_pct: float


@dataclass(frozen=True)
class RebalanceOrder:
    symbol: str
    side: str
    target_value: float
    current_value: float
    delta_value: float
    estimated_qty: float


@dataclass(frozen=True)
class RegimeStatus:
    enabled: bool
    benchmark: str
    benchmark_price: float
    moving_average: float
    is_offsignal: bool
    risk_multiplier: float


def _sleeve_for_symbol(symbol: str, cfg: StrategyConfig) -> str:
    if symbol in cfg.allocation.trunk.holdings:
        return "trunk"
    if symbol in cfg.allocation.branches.holdings:
        return "branches"
    return "untracked"


def compute_sleeve_weights(
    sleeve_cfg: TrunkConfig | BranchesConfig,
    historical_prices: Optional[pd.DataFrame],
) -> Dict[str, float]:
    """Return symbol -> weight within the sleeve. Sums to 1.0.

    For 'equal' weighting, returns config holdings dict directly.
    For 'inverse_volatility', computes weights from trailing vol with config
    holdings values as bias multipliers (use 1.0 for pure inverse-vol).
    Falls back to equal weighting if insufficient history.
    """
    symbols = list(sleeve_cfg.holdings.keys())

    if sleeve_cfg.weighting_method == "equal":
        return dict(sleeve_cfg.holdings)

    if historical_prices is None or historical_prices.empty:
        log.warning("No historical prices supplied for vol weighting; using equal weights")
        return {s: 1 / len(symbols) for s in symbols}

    available_cols = [s for s in symbols if s in historical_prices.columns]
    if len(available_cols) < len(symbols):
        missing = set(symbols) - set(available_cols)
        log.warning(f"Missing history for {missing}; using equal weights")
        return {s: 1 / len(symbols) for s in symbols}

    prices = historical_prices[available_cols].dropna()
    if len(prices) < VOL_WINDOW_DAYS:
        log.warning(
            f"Need {VOL_WINDOW_DAYS} days of history for vol weighting, have {len(prices)}; "
            "using equal weights"
        )
        return {s: 1 / len(symbols) for s in symbols}

    returns = prices.tail(VOL_WINDOW_DAYS).pct_change().dropna()
    vols = returns.std() * np.sqrt(252)

    bias = {s: float(sleeve_cfg.holdings[s]) for s in symbols}
    raw = {s: bias[s] / float(vols[s]) for s in symbols if vols[s] > 0}
    total = sum(raw.values())
    if total <= 0:
        return {s: 1 / len(symbols) for s in symbols}
    normalized = {s: w / total for s, w in raw.items()}

    bounded = {
        s: max(MIN_WEIGHT_WITHIN_SLEEVE, min(MAX_WEIGHT_WITHIN_SLEEVE, w))
        for s, w in normalized.items()
    }
    total_bounded = sum(bounded.values())
    return {s: w / total_bounded for s, w in bounded.items()}


def evaluate_regime(
    cfg: StrategyConfig,
    historical_prices: Optional[pd.DataFrame],
) -> RegimeStatus:
    """Decide whether we are 'risk on' (full equity exposure) or 'risk off' (scaled down).

    Returns a status with risk_multiplier in [0, 1] applied to all equity targets.
    1.0 = full risk on, lower values = risk off.
    """
    if not cfg.regime.enabled:
        return RegimeStatus(
            enabled=False, benchmark=cfg.regime.benchmark,
            benchmark_price=0.0, moving_average=0.0,
            is_offsignal=False, risk_multiplier=1.0,
        )

    bench = cfg.regime.benchmark
    if historical_prices is None or historical_prices.empty or bench not in historical_prices.columns:
        log.warning(f"Cannot evaluate regime: {bench} history unavailable; defaulting to risk-on")
        return RegimeStatus(
            enabled=True, benchmark=bench,
            benchmark_price=0.0, moving_average=0.0,
            is_offsignal=False, risk_multiplier=1.0,
        )

    series = historical_prices[bench].dropna()
    if len(series) < cfg.regime.ma_window:
        log.warning(
            f"Need {cfg.regime.ma_window} days of {bench} history, have {len(series)}; "
            "defaulting to risk-on"
        )
        return RegimeStatus(
            enabled=True, benchmark=bench,
            benchmark_price=float(series.iloc[-1]) if len(series) else 0.0,
            moving_average=0.0, is_offsignal=False, risk_multiplier=1.0,
        )

    ma = float(series.tail(cfg.regime.ma_window).mean())
    last_price = float(series.iloc[-1])
    is_offsignal = last_price < ma
    multiplier = (1 - cfg.regime.offsignal_cash_pct) if is_offsignal else 1.0

    return RegimeStatus(
        enabled=True, benchmark=bench,
        benchmark_price=last_price, moving_average=ma,
        is_offsignal=is_offsignal, risk_multiplier=multiplier,
    )


def compute_target_values(
    portfolio_value: float,
    cfg: StrategyConfig,
    historical_prices: Optional[pd.DataFrame] = None,
) -> Dict[str, float]:
    """Map each tracked symbol to its target dollar value.

    Applies sleeve weighting (equal or inverse-vol) and the regime risk multiplier.
    The acorns sleeve is held as cash and not tracked here.
    """
    regime = evaluate_regime(cfg, historical_prices)
    risk_mult = regime.risk_multiplier

    targets: Dict[str, float] = {}

    trunk_value = portfolio_value * cfg.allocation.trunk.weight * risk_mult
    trunk_weights = compute_sleeve_weights(cfg.allocation.trunk, historical_prices)
    for symbol, w in trunk_weights.items():
        targets[symbol] = trunk_value * w

    branches_value = portfolio_value * cfg.allocation.branches.weight * risk_mult
    branches_weights = compute_sleeve_weights(cfg.allocation.branches, historical_prices)
    for symbol, w in branches_weights.items():
        targets[symbol] = branches_value * w

    return targets


def compute_holding_status(
    positions: Dict[str, Position],
    targets: Dict[str, float],
    cfg: StrategyConfig,
) -> List[TargetHolding]:
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
    historical_prices: Optional[pd.DataFrame] = None,
) -> List[RebalanceOrder]:
    """Generate rebalance orders for holdings outside the drift threshold."""
    targets = compute_target_values(portfolio_value, cfg, historical_prices)
    statuses = compute_holding_status(positions, targets, cfg)

    orders: List[RebalanceOrder] = []
    for status in statuses:
        if status.current_value == 0 and status.target_value > 0:
            drift_significant = True
        elif status.target_value == 0 and status.current_value > 0:
            # Position should be closed entirely (regime offsignal can do this)
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
            continue

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
