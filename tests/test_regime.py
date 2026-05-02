"""Tests for regime detection (defensive tagging + whipsaw protection) and
the YAML backward-compat schema for HoldingConfig.

Run with: pytest tests/test_regime.py -v
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.broker import Position
from src.config import (
    AcornsConfig, AllocationConfig, BranchesConfig, HoldingConfig,
    PortfolioConfig, RebalanceConfig, RegimeConfig, RiskConfig,
    StrategyConfig, TrunkConfig, WeightingConfig, _normalize_holdings,
)
from src.strategy import compute_target_values, evaluate_regime


# --- helpers ---------------------------------------------------------------


def build_strategy(
    *,
    trunk_holdings: dict,
    branches_holdings: dict,
    regime_enabled: bool = True,
    buffer_pct: float = 0.02,
    min_consecutive_days: int = 3,
    ma_window: int = 200,
    offsignal_cash_pct: float = 0.40,
    trunk_method: str = "equal",
    branches_method: str = "equal",
) -> StrategyConfig:
    return StrategyConfig(
        portfolio=PortfolioConfig(total_target_value_usd=200_000),
        allocation=AllocationConfig(
            trunk=TrunkConfig(weight=0.50, holdings=trunk_holdings, weighting_method=trunk_method),
            branches=BranchesConfig(weight=0.35, holdings=branches_holdings, weighting_method=branches_method),
            acorns=AcornsConfig(weight=0.15),
        ),
        weighting=WeightingConfig(),
        rebalance=RebalanceConfig(drift_threshold=0.05, min_order_size_usd=100),
        risk=RiskConfig(
            max_order_pct_of_portfolio=0.05,
            max_daily_orders=20,
            drawdown_kill_switch_pct=0.05,
            require_market_hours=True,
        ),
        regime=RegimeConfig(
            enabled=regime_enabled,
            benchmark="SPY",
            ma_window=ma_window,
            offsignal_cash_pct=offsignal_cash_pct,
            buffer_pct=buffer_pct,
            min_consecutive_days=min_consecutive_days,
        ),
    )


def make_spy_history(*segments: tuple[int, float], start_price: float = 400.0) -> pd.DataFrame:
    """Construct a SPY price series from segments. Each segment is a
    (n_days, pct_offset_from_start_price) tuple.

    Example: make_spy_history((250, 0.0), (30, -0.05)) builds 250 days at
    $400 followed by 30 days at $380. Useful for controlling exactly what the
    MA + threshold computation will see at the end of the series.
    """
    series: list[float] = []
    for n_days, offset_pct in segments:
        series.extend([start_price * (1 + offset_pct)] * n_days)
    return pd.DataFrame({"SPY": series})


# --- HoldingConfig backward compatibility ----------------------------------


class TestNormalizeHoldings:
    def test_flat_format_normalizes_to_equity(self):
        result = _normalize_holdings({"VTI": 0.70, "BND": 0.30})
        assert result == {
            "VTI": {"weight": 0.70, "risk_class": "equity"},
            "BND": {"weight": 0.30, "risk_class": "equity"},
        }

    def test_structured_format_passes_through(self):
        result = _normalize_holdings({
            "VTI": {"weight": 0.70, "risk_class": "equity"},
            "BND": {"weight": 0.30, "risk_class": "defensive"},
        })
        assert result["BND"]["risk_class"] == "defensive"

    def test_mixed_format_works(self):
        # Some holdings flat (legacy), some structured (new tagging)
        result = _normalize_holdings({
            "VTI": 0.70,
            "BND": {"weight": 0.30, "risk_class": "defensive"},
        })
        assert result["VTI"]["risk_class"] == "equity"
        assert result["BND"]["risk_class"] == "defensive"


class TestTrunkConfigBackwardCompat:
    def test_loads_legacy_flat_yaml(self):
        # Existing YAMLs use flat format with bare numerics
        cfg = TrunkConfig(
            weight=0.50,
            holdings={"VTI": 0.70, "VXUS": 0.10, "BND": 0.10, "GLD": 0.10},
        )
        assert isinstance(cfg.holdings["VTI"], HoldingConfig)
        assert cfg.holdings["VTI"].weight == 0.70
        # Untagged → default equity (preserves prior behavior)
        assert cfg.holdings["VTI"].risk_class == "equity"
        assert cfg.holdings["BND"].risk_class == "equity"

    def test_loads_new_structured_yaml(self):
        cfg = TrunkConfig(
            weight=0.50,
            holdings={
                "VTI": {"weight": 0.70, "risk_class": "equity"},
                "VXUS": {"weight": 0.10, "risk_class": "equity"},
                "BND": {"weight": 0.10, "risk_class": "defensive"},
                "GLD": {"weight": 0.10, "risk_class": "defensive"},
            },
        )
        assert cfg.holdings["BND"].risk_class == "defensive"
        assert cfg.holdings["GLD"].risk_class == "defensive"
        assert cfg.holdings["VTI"].risk_class == "equity"

    def test_equal_weights_still_validated(self):
        # Equal-weight holdings must still sum to 1.0
        with pytest.raises(ValueError, match="must be 1.0"):
            TrunkConfig(weight=0.50, holdings={"A": 0.30, "B": 0.30})

    def test_inverse_vol_bias_must_be_positive(self):
        with pytest.raises(ValueError, match="must be positive"):
            TrunkConfig(
                weight=0.50,
                holdings={"A": 1.0, "B": 0.0},
                weighting_method="inverse_volatility",
            )


# --- evaluate_regime: whipsaw protection -----------------------------------


class TestRegimeOnSignal:
    def test_clear_uptrend_is_on(self):
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
        )
        # 250 flat warmup, then 50-day +5% tail. MA at end ≈ $405, price = $420.
        # Distance ≈ +3.7%, outside buffer → ON window fires.
        prices = make_spy_history((250, 0.0), (50, 0.05))
        status = evaluate_regime(cfg, prices)
        assert status.is_offsignal is False
        assert status.risk_multiplier == 1.0

    def test_clear_downtrend_is_off(self):
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
        )
        # 250 flat warmup, then 30-day -5% tail. MA ≈ $397, price = $380.
        # Distance ≈ -4.3%, well outside the 2% buffer → OFF.
        prices = make_spy_history((250, 0.0), (30, -0.05))
        status = evaluate_regime(cfg, prices)
        assert status.is_offsignal is True
        assert status.risk_multiplier == pytest.approx(0.60)


class TestRegimeBufferZone:
    def test_inside_buffer_does_not_flip_off(self):
        """SPY -1% from MA with buffer=2% should NOT trigger off."""
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
            buffer_pct=0.02,
            min_consecutive_days=3,
        )
        # 30-day -1% tail. MA ≈ $399.40, price = $396.
        # Distance ≈ -0.85%, inside the 2% buffer → no OFF event fires.
        prices = make_spy_history((250, 0.0), (30, -0.01))
        status = evaluate_regime(cfg, prices)
        assert status.is_offsignal is False

    def test_outside_buffer_does_flip_off(self):
        """SPY -5% from MA with buffer=2% SHOULD trigger off after N days."""
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
            buffer_pct=0.02,
            min_consecutive_days=3,
        )
        prices = make_spy_history((250, 0.0), (30, -0.05))
        status = evaluate_regime(cfg, prices)
        assert status.is_offsignal is True


class TestRegimeConsecutiveDays:
    def test_one_day_below_does_not_flip_with_n3(self):
        """Single day below threshold with N=3 should NOT flip off."""
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
            buffer_pct=0.02,
            min_consecutive_days=3,
        )
        # 30-day +5% tail (lock in ON), then 1 day at -5%. Single OFF day.
        # ON window most recent → state = ON.
        prices = make_spy_history((250, 0.0), (30, 0.05), (1, -0.05))
        status = evaluate_regime(cfg, prices)
        assert status.is_offsignal is False

    def test_n_consecutive_days_below_flips_off(self):
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
            buffer_pct=0.02,
            min_consecutive_days=3,
        )
        # 30 days +5% then 3 days -5%. Last 3 days fire OFF window.
        prices = make_spy_history((250, 0.0), (30, 0.05), (3, -0.05))
        status = evaluate_regime(cfg, prices)
        assert status.is_offsignal is True

    def test_alternating_does_not_flip_with_n3(self):
        """Alternating above/below days with N=3 should never lock in OFF."""
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
            buffer_pct=0.02,
            min_consecutive_days=3,
        )
        warmup = [400.0] * 250
        # 60 days alternating ±5% — neither N=3 ON nor OFF window triggers
        alternating = [420.0 if i % 2 == 0 else 380.0 for i in range(60)]
        prices = pd.DataFrame({"SPY": warmup + alternating})
        status = evaluate_regime(cfg, prices)
        # No N=3 window of clear above-or-below ever fires → default ON
        assert status.is_offsignal is False


class TestRegimeStateInheritance:
    def test_inherits_off_state_from_recent_history(self):
        """If most recent unambiguous N-day window was OFF and we're now in
        the buffer zone, state remains OFF."""
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
            buffer_pct=0.02,
            min_consecutive_days=3,
        )
        # Clear OFF then 5 ambiguous days inside buffer (~-0.5% from start)
        prices = make_spy_history((250, 0.0), (30, -0.05), (5, -0.005))
        status = evaluate_regime(cfg, prices)
        assert status.is_offsignal is True

    def test_inherits_on_state_from_recent_history(self):
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
            buffer_pct=0.02,
            min_consecutive_days=3,
        )
        prices = make_spy_history((250, 0.0), (30, 0.05), (5, 0.005))
        status = evaluate_regime(cfg, prices)
        assert status.is_offsignal is False


class TestRegimeBackwardCompat:
    def test_legacy_settings_reproduce_old_behavior(self):
        """buffer=0, N=1 should match the old daily-flip behavior."""
        cfg = build_strategy(
            trunk_holdings={"VTI": 1.0},
            branches_holdings={"SMH": 1.0},
            buffer_pct=0.0,
            min_consecutive_days=1,
        )
        # Single day below MA flips off in legacy mode
        prices = make_spy_history((250, 0.0), (10, 0.025), (1, -0.0125))
        status = evaluate_regime(cfg, prices)
        assert status.is_offsignal is True


# --- compute_target_values: defensive tagging ------------------------------


class TestDefensiveTagging:
    def test_off_signal_does_not_scale_defensive(self):
        """The headline behavior change: BND and GLD targets should NOT shrink
        when SPY drops below 200dma. Previously the regime risk_multiplier
        scaled all of trunk including BND/GLD, which is the opposite of what
        defensive holdings are for."""
        cfg = build_strategy(
            trunk_holdings={
                "VTI": {"weight": 0.70, "risk_class": "equity"},
                "VXUS": {"weight": 0.10, "risk_class": "equity"},
                "BND": {"weight": 0.10, "risk_class": "defensive"},
                "GLD": {"weight": 0.10, "risk_class": "defensive"},
            },
            branches_holdings={"SMH": 1.0},
        )
        prices = make_spy_history((250, 0.0), (30, -0.05))
        targets = compute_target_values(200_000, cfg, prices)

        # Regime is OFF: equity scales by 0.6, defensive holds static
        # Trunk is 50% of $200k = $100k
        # VTI target = $100k × 0.70 × 0.60 = $42,000
        # VXUS target = $100k × 0.10 × 0.60 = $6,000
        # BND target = $100k × 0.10 = $10,000  ← unchanged from ON state
        # GLD target = $100k × 0.10 = $10,000  ← unchanged from ON state
        assert targets["VTI"] == pytest.approx(42_000)
        assert targets["VXUS"] == pytest.approx(6_000)
        assert targets["BND"] == pytest.approx(10_000)
        assert targets["GLD"] == pytest.approx(10_000)

    def test_on_signal_full_targets_for_all(self):
        cfg = build_strategy(
            trunk_holdings={
                "VTI": {"weight": 0.70, "risk_class": "equity"},
                "BND": {"weight": 0.30, "risk_class": "defensive"},
            },
            branches_holdings={"SMH": 1.0},
        )
        prices = make_spy_history((250, 0.0), (50, 0.05))
        targets = compute_target_values(200_000, cfg, prices)
        # ON: nothing scaled, both at full sleeve weight
        assert targets["VTI"] == pytest.approx(70_000)  # 100k × 0.70
        assert targets["BND"] == pytest.approx(30_000)  # 100k × 0.30

    def test_legacy_yaml_treats_all_as_equity(self):
        """Legacy YAML (untagged holdings) defaults all to equity — preserves
        the prior behavior of scaling everything in regime-off."""
        cfg = build_strategy(
            trunk_holdings={"VTI": 0.70, "BND": 0.30},  # flat format, no risk_class
            branches_holdings={"SMH": 1.0},
        )
        prices = make_spy_history((250, 0.0), (30, -0.05))
        targets = compute_target_values(200_000, cfg, prices)
        # Both treated as equity → both scale by 0.60 (legacy behavior)
        assert targets["VTI"] == pytest.approx(70_000 * 0.60)
        assert targets["BND"] == pytest.approx(30_000 * 0.60)

    def test_freed_equity_capital_becomes_cash_not_redistributed(self):
        """Total deployed should drop in OFF state — the freed capital is NOT
        redistributed to defensive holdings, it just becomes cash."""
        cfg = build_strategy(
            trunk_holdings={
                "VTI": {"weight": 0.70, "risk_class": "equity"},
                "BND": {"weight": 0.30, "risk_class": "defensive"},
            },
            branches_holdings={"SMH": {"weight": 1.0, "risk_class": "equity"}},
        )
        # Get ON state targets
        on_prices = make_spy_history((250, 0.0), (50, 0.05))
        on_targets = compute_target_values(200_000, cfg, on_prices)
        on_total = sum(on_targets.values())

        # Get OFF state targets
        off_prices = make_spy_history((250, 0.0), (30, -0.05))
        off_targets = compute_target_values(200_000, cfg, off_prices)
        off_total = sum(off_targets.values())

        # Total deployed is lower in OFF state
        assert off_total < on_total
        # BND target is the same (defensive)
        assert off_targets["BND"] == pytest.approx(on_targets["BND"])
        # The difference equals 40% of equity targets in ON state
        equity_in_on = on_targets["VTI"] + on_targets["SMH"]
        expected_freed = equity_in_on * 0.40  # offsignal_cash_pct
        assert (on_total - off_total) == pytest.approx(expected_freed)


# --- RegimeConfig validation -----------------------------------------------


class TestRegimeConfigValidation:
    def test_defaults(self):
        rc = RegimeConfig()
        assert rc.buffer_pct == 0.02
        assert rc.min_consecutive_days == 3
        assert rc.ma_window == 200
        assert rc.offsignal_cash_pct == 0.40

    def test_buffer_pct_in_range(self):
        with pytest.raises(ValueError):
            RegimeConfig(buffer_pct=-0.01)
        with pytest.raises(ValueError):
            RegimeConfig(buffer_pct=0.6)

    def test_min_consecutive_days_min_one(self):
        with pytest.raises(ValueError):
            RegimeConfig(min_consecutive_days=0)
