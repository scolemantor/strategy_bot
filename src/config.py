"""Configuration loading and validation."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Literal, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator


WeightingMethod = Literal["equal", "inverse_volatility"]


class TrunkConfig(BaseModel):
    weight: float = Field(..., ge=0, le=1)
    holdings: Dict[str, float]
    weighting_method: WeightingMethod = "equal"

    @model_validator(mode="after")
    def weights_valid(self):
        if self.weighting_method == "equal":
            total = sum(self.holdings.values())
            if abs(total - 1.0) > 1e-3:
                raise ValueError(f"Trunk equal-weight holdings sum to {total:.4f}, must be 1.0")
        else:
            if any(v <= 0 for v in self.holdings.values()):
                raise ValueError("Inverse-vol bias multipliers must be positive")
        return self


class BranchesConfig(BaseModel):
    weight: float = Field(..., ge=0, le=1)
    holdings: Dict[str, float]
    weighting_method: WeightingMethod = "equal"

    @model_validator(mode="after")
    def weights_valid(self):
        if self.weighting_method == "equal":
            total = sum(self.holdings.values())
            if abs(total - 1.0) > 1e-3:
                raise ValueError(f"Branches equal-weight holdings sum to {total:.4f}, must be 1.0")
        else:
            if any(v <= 0 for v in self.holdings.values()):
                raise ValueError("Inverse-vol bias multipliers must be positive")
        return self


class AcornsConfig(BaseModel):
    weight: float = Field(..., ge=0, le=1)


class AllocationConfig(BaseModel):
    trunk: TrunkConfig
    branches: BranchesConfig
    acorns: AcornsConfig

    @model_validator(mode="after")
    def sleeves_sum_to_one(self):
        total = self.trunk.weight + self.branches.weight + self.acorns.weight
        if abs(total - 1.0) > 1e-3:
            raise ValueError(f"Sleeve weights sum to {total:.4f}, must be 1.0")
        return self

    @model_validator(mode="after")
    def no_symbol_overlap(self):
        trunk_syms = set(self.trunk.holdings)
        branches_syms = set(self.branches.holdings)
        overlap = trunk_syms & branches_syms
        if overlap:
            raise ValueError(f"Symbols cannot be in both trunk and branches: {overlap}")
        return self


class WeightingConfig(BaseModel):
    """Parameters for inverse-volatility sleeve weighting.

    Defaults match the prior hardcoded constants in strategy.py. Add a
    `weighting:` block to strategy.yaml to override; otherwise defaults apply.
    """
    vol_window_days: int = Field(90, ge=10, le=500)
    min_weight_within_sleeve: float = Field(0.05, ge=0, le=1)
    max_weight_within_sleeve: float = Field(0.40, ge=0, le=1)

    @model_validator(mode="after")
    def bounds_consistent(self):
        if self.min_weight_within_sleeve >= self.max_weight_within_sleeve:
            raise ValueError(
                f"min_weight_within_sleeve ({self.min_weight_within_sleeve}) must be < "
                f"max_weight_within_sleeve ({self.max_weight_within_sleeve})"
            )
        return self


class RegimeConfig(BaseModel):
    enabled: bool = False
    benchmark: str = "SPY"
    ma_window: int = Field(200, ge=10)
    offsignal_cash_pct: float = Field(0.40, ge=0, le=1)


class RebalanceConfig(BaseModel):
    drift_threshold: float = Field(0.05, ge=0, le=1)
    min_order_size_usd: float = Field(100, ge=0)


class RiskConfig(BaseModel):
    max_order_pct_of_portfolio: float = Field(0.05, ge=0, le=1)
    max_daily_orders: int = Field(20, ge=0)
    drawdown_kill_switch_pct: float = Field(0.05, ge=0, le=1)
    require_market_hours: bool = True


class PortfolioConfig(BaseModel):
    total_target_value_usd: float = Field(..., gt=0)


class StrategyConfig(BaseModel):
    portfolio: PortfolioConfig
    allocation: AllocationConfig
    weighting: WeightingConfig = Field(default_factory=WeightingConfig)
    rebalance: RebalanceConfig
    risk: RiskConfig
    regime: RegimeConfig = Field(default_factory=RegimeConfig)

    def all_tracked_symbols(self) -> list[str]:
        return list(self.allocation.trunk.holdings) + list(self.allocation.branches.holdings)

    def needs_history(self) -> bool:
        return (
            self.allocation.trunk.weighting_method == "inverse_volatility"
            or self.allocation.branches.weighting_method == "inverse_volatility"
            or self.regime.enabled
        )


class BrokerCredentials(BaseModel):
    api_key: str
    secret_key: str
    paper: bool = True


def load_credentials() -> BrokerCredentials:
    load_dotenv()
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    paper = os.getenv("ALPACA_PAPER", "true").lower() == "true"
    if not api_key or not secret_key:
        raise RuntimeError(
            "ALPACA_API_KEY and ALPACA_SECRET_KEY must be set in your environment "
            "or in a .env file. See .env.example for the template."
        )
    return BrokerCredentials(api_key=api_key, secret_key=secret_key, paper=paper)


def load_strategy(path: str | Path = "config/strategy.yaml") -> StrategyConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Strategy config not found at {path}")
    with open(path) as f:
        data = yaml.safe_load(f)
    return StrategyConfig.model_validate(data)
