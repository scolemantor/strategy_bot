"""Tests for auto-liquidation of held-but-untracked positions.

When the YAML allocation changes (e.g. VXUS removed, BND→BIL swap),
held positions outside the new tracked set should be auto-sold to zero
on the next rebalance. The YAML is the source of truth.
"""
from __future__ import annotations

import pytest

from src.broker import Position
from src.config import (
    AcornsConfig,
    AllocationConfig,
    BranchesConfig,
    HoldingConfig,
    PortfolioConfig,
    RebalanceConfig,
    RegimeConfig,
    RiskConfig,
    StrategyConfig,
    TrunkConfig,
    WeightingConfig,
)
from src.strategy import compute_rebalance_orders


def _make_cfg() -> StrategyConfig:
    """Minimal V3-shaped config with VTI and BIL only — no VXUS, no BND."""
    return StrategyConfig(
        portfolio=PortfolioConfig(total_target_value_usd=100_000),
        allocation=AllocationConfig(
            trunk=TrunkConfig(
                weight=0.7,
                weighting_method="equal",
                holdings={
                    "VTI": HoldingConfig(weight=0.8, risk_class="equity"),
                    "BIL": HoldingConfig(weight=0.1, risk_class="defensive"),
                    "GLD": HoldingConfig(weight=0.1, risk_class="defensive"),
                },
            ),
            branches=BranchesConfig(
                weight=0.2,
                weighting_method="equal",
                holdings={
                    "SMH": HoldingConfig(weight=1.0, risk_class="equity"),
                },
            ),
            acorns=AcornsConfig(weight=0.1),
        ),
        weighting=WeightingConfig(),
        rebalance=RebalanceConfig(drift_threshold=0.05, min_order_size_usd=100),
        risk=RiskConfig(),
        regime=RegimeConfig(enabled=False),
    )


def test_held_untracked_position_generates_sell_order():
    """A position not in the YAML should be auto-liquidated."""
    cfg = _make_cfg()
    # VXUS held but not in YAML
    positions = {
        "VXUS": Position(symbol="VXUS", qty=50, market_value=5000, avg_entry_price=100),
        "VTI": Position(symbol="VTI", qty=200, market_value=56000, avg_entry_price=280),
    }
    quotes = {"VTI": 280.0, "BIL": 91.0, "GLD": 425.0, "SMH": 510.0, "VXUS": 100.0}

    orders = compute_rebalance_orders(positions, 100_000, quotes, cfg)

    vxus_orders = [o for o in orders if o.symbol == "VXUS"]
    assert len(vxus_orders) == 1
    assert vxus_orders[0].side == "sell"
    assert vxus_orders[0].target_value == 0
    assert vxus_orders[0].estimated_qty == pytest.approx(50, rel=0.01)


def test_multiple_untracked_positions_all_liquidated():
    cfg = _make_cfg()
    positions = {
        "VXUS": Position(symbol="VXUS", qty=50, market_value=5000, avg_entry_price=100),
        "BND": Position(symbol="BND", qty=68, market_value=5015, avg_entry_price=73.75),
        "VTI": Position(symbol="VTI", qty=200, market_value=56000, avg_entry_price=280),
    }
    quotes = {
        "VTI": 280.0, "BIL": 91.0, "GLD": 425.0, "SMH": 510.0,
        "VXUS": 100.0, "BND": 73.75,
    }

    orders = compute_rebalance_orders(positions, 100_000, quotes, cfg)
    sells = {o.symbol for o in orders if o.side == "sell"}

    assert "VXUS" in sells
    assert "BND" in sells


def test_zero_qty_untracked_position_ignored():
    """A position with qty=0 isn't really held — skip it."""
    cfg = _make_cfg()
    positions = {
        "VXUS": Position(symbol="VXUS", qty=0, market_value=0, avg_entry_price=100),
    }
    quotes = {"VTI": 280.0, "BIL": 91.0, "GLD": 425.0, "SMH": 510.0, "VXUS": 100.0}

    orders = compute_rebalance_orders(positions, 100_000, quotes, cfg)
    vxus_orders = [o for o in orders if o.symbol == "VXUS"]
    assert vxus_orders == []


def test_untracked_with_no_quote_skipped_gracefully():
    """If we can't price the untracked position, no order — but no crash."""
    cfg = _make_cfg()
    positions = {
        "WEIRDSYM": Position(symbol="WEIRDSYM", qty=10, market_value=500, avg_entry_price=50),
    }
    quotes = {"VTI": 280.0, "BIL": 91.0, "GLD": 425.0, "SMH": 510.0}
    # No quote for WEIRDSYM

    orders = compute_rebalance_orders(positions, 100_000, quotes, cfg)
    weird_orders = [o for o in orders if o.symbol == "WEIRDSYM"]
    assert weird_orders == []  # no order generated, no crash


def test_tracked_position_unaffected():
    """The new logic shouldn't change orders for tracked positions."""
    cfg = _make_cfg()
    # VTI is correctly sized at exactly target — no sell expected
    # Target: 100k * 0.7 (trunk) * 0.8 (VTI weight) = 56k
    positions = {
        "VTI": Position(symbol="VTI", qty=200, market_value=56000, avg_entry_price=280),
    }
    quotes = {"VTI": 280.0, "BIL": 91.0, "GLD": 425.0, "SMH": 510.0}

    orders = compute_rebalance_orders(positions, 100_000, quotes, cfg)
    vti_orders = [o for o in orders if o.symbol == "VTI"]
    # VTI is at target, shouldn't generate an order
    assert vti_orders == []


def test_untracked_below_min_order_size_filtered():
    """Tiny dust positions below min_order_size_usd are filtered."""
    cfg = _make_cfg()
    # Tiny VXUS holding worth less than $100 (min_order_size_usd)
    positions = {
        "VXUS": Position(symbol="VXUS", qty=0.5, market_value=50, avg_entry_price=100),
    }
    quotes = {"VTI": 280.0, "BIL": 91.0, "GLD": 425.0, "SMH": 510.0, "VXUS": 100.0}

    orders = compute_rebalance_orders(positions, 100_000, quotes, cfg)
    vxus_orders = [o for o in orders if o.symbol == "VXUS"]
    # $50 < $100 min_order_size_usd → no order
    assert vxus_orders == []
