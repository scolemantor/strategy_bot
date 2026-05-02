"""Oak rebalancer with volatility-weighted sizing and regime detection.

Pure functions, no I/O. Inputs flow in via arguments, outputs via return values.

Behaviors:
  - Per-sleeve weighting_method: 'equal' uses config weights as-is;
    'inverse_volatility' sizes each holding inversely to its trailing vol,
    optionally biased by the value in the config holdings dict.
  - Vol-weight clipping uses iterative water-filling so the configured
    min/max weights actually hold (single-pass cap-then-renormalize is broken).
  - Symbols with insufficient history or zero/missing vol fall back the entire
    sleeve to equal weight rather than silently dropping the symbol from the
    target allocation (safer than ghosting a holding).
  - Regime overlay: if regime.enabled and the benchmark is below its moving
    average, scales total equity exposure down by offsignal_cash_pct.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .broker import Position
from .config import BranchesConfig, StrategyConfig, TrunkConfig, WeightingConfig

log = logging.getLogger(__name__)


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


def _waterfill_clip(
    weights: Dict[str, float],
    min_weight: float,
    max_weight: float,
) -> Dict[str, float]:
    """Clip weights to [min_weight, max_weight] preserving total = 1.0.

    Iterative water-filling: pin out-of-bound weights to their caps, redistribute
    spillover pro-rata to remaining (free) weights, repeat until all free weights
    are in bounds.

    Why not single-pass cap-then-renormalize? Renormalizing after clipping pushes
    capped weights right back over the cap. Worked example: weights = [0.85, 0.05,
    0.05, 0.05] and max=0.40. Cap → [0.40, 0.05, 0.05, 0.05] (sum 0.55), divide by
    0.55 → [0.727, 0.091, 0.091, 0.091]. First weight is now 0.727, well above 0.40.

    Raises ValueError if bounds are infeasible (min*n > 1 or max*n < 1).
    """
    n = len(weights)
    if n == 0:
        return {}

    if min_weight * n > 1.0 + 1e-9 or max_weight * n < 1.0 - 1e-9:
        raise ValueError(
            f"Infeasible bounds for {n} symbols: need min*n <= 1 <= max*n, "
            f"got min*n = {min_weight*n:.4f}, max*n = {max_weight*n:.4f}"
        )

    w = dict(weights)
    pinned: set[str] = set()

    # Pin the single most extreme violator per iteration, then redistribute.
    # Pinning ALL violators at once over-constrains (e.g. weights [0.90, 0.04,
    # 0.04, 0.02] with min=0.05, max=0.40 would pin A to max AND B/C/D to min,
    # leaving no free weights to absorb the missing mass). At most 2n+1
    # iterations: each iteration pins exactly one weight.
    for _ in range(2 * n + 1):
        worst: tuple[str, str, float] | None = None  # (symbol, kind, magnitude)
        for s, v in w.items():
            if s in pinned:
                continue
            if v > max_weight + 1e-12:
                violation = v - max_weight
                if worst is None or violation > worst[2]:
                    worst = (s, "high", violation)
            elif v < min_weight - 1e-12:
                violation = min_weight - v
                if worst is None or violation > worst[2]:
                    worst = (s, "low", violation)

        if worst is None:
            break  # All free weights in bounds

        s, kind, _ = worst
        w[s] = max_weight if kind == "high" else min_weight
        pinned.add(s)

        free = [k for k in w if k not in pinned]
        if not free:
            break

        pinned_mass = sum(w[k] for k in pinned)
        free_target = 1.0 - pinned_mass
        free_current = sum(w[k] for k in free)

        if free_current <= 0:
            share = free_target / len(free)
            for k in free:
                w[k] = share
        else:
            scale = free_target / free_current
            for k in free:
                w[k] = w[k] * scale

    return w


def compute_sleeve_weights(
    sleeve_cfg: TrunkConfig | BranchesConfig,
    historical_prices: Optional[pd.DataFrame],
    weighting_cfg: WeightingConfig,
) -> Dict[str, float]:
    """Return symbol -> weight within the sleeve. Sums to 1.0.

    For 'equal' weighting, returns config holdings dict directly.

    For 'inverse_volatility', sizes each holding inversely to its trailing vol
    (window from weighting_cfg.vol_window_days) with config holdings values used
    as bias multipliers (use 1.0 for pure inverse-vol). Result is clipped to
    [min, max] from weighting_cfg via iterative water-filling.

    Falls back the entire sleeve to equal weighting if any symbol is missing
    history, has insufficient data, or has zero/non-finite vol. Never silently
    drops a symbol — the rebalancer treating a position as untracked is more
    dangerous than a temporary fallback to equal weight.
    """
    symbols = list(sleeve_cfg.holdings.keys())
    n = len(symbols)
    equal = {s: 1 / n for s in symbols}

    if sleeve_cfg.weighting_method == "equal":
        return dict(sleeve_cfg.holdings)

    if historical_prices is None or historical_prices.empty:
        log.warning("No historical prices supplied for vol weighting; using equal weights")
        return equal

    missing = set(symbols) - set(historical_prices.columns)
    if missing:
        log.warning(
            f"Missing history for {sorted(missing)}; falling back to equal weights for sleeve"
        )
        return equal

    # Per-column vol calc — don't cross-drop rows just because one column has
    # NaNs. Each symbol's vol is computed from its own non-null tail.
    vol_window = weighting_cfg.vol_window_days
    vols: Dict[str, float] = {}
    for s in symbols:
        col = historical_prices[s].dropna()
        if len(col) < vol_window:
            log.warning(
                f"{s}: need {vol_window} days of history for vol weighting, "
                f"have {len(col)}; falling back to equal weights for sleeve"
            )
            return equal
        returns = col.tail(vol_window).pct_change().dropna()
        v = float(returns.std())
        if not np.isfinite(v) or v <= 0:
            log.warning(
                f"{s}: zero or non-finite vol ({v}); falling back to equal weights for sleeve"
            )
            return equal
        vols[s] = v

    bias = {s: float(sleeve_cfg.holdings[s]) for s in symbols}
    raw = {s: bias[s] / vols[s] for s in symbols}
    total = sum(raw.values())
    if total <= 0:
        log.warning("Vol-weighted total is zero; falling back to equal weights")
        return equal
    normalized = {s: w / total for s, w in raw.items()}

    return _waterfill_clip(
        normalized,
        min_weight=weighting_cfg.min_weight_within_sleeve,
        max_weight=weighting_cfg.max_weight_within_sleeve,
    )


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
    trunk_weights = compute_sleeve_weights(cfg.allocation.trunk, historical_prices, cfg.weighting)
    for symbol, w in trunk_weights.items():
        targets[symbol] = trunk_value * w

    branches_value = portfolio_value * cfg.allocation.branches.weight * risk_mult
    branches_weights = compute_sleeve_weights(cfg.allocation.branches, historical_prices, cfg.weighting)
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
