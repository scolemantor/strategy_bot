"""Configuration loading and validation."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator


class TrunkConfig(BaseModel):
    weight: float = Field(..., ge=0, le=1)
    holdings: Dict[str, float]

    @model_validator(mode="after")
    def weights_sum_to_one(self):
        total = sum(self.holdings.values())
        if abs(total - 1.0) > 1e-3:
            raise ValueError(f"Trunk holdings sum to {total:.4f}, must be 1.0")
        return self


class BranchesConfig(BaseModel):
    weight: float = Field(..., ge=0, le=1)
    holdings: Dict[str, float]

    @model_validator(mode="after")
    def weights_sum_to_one(self):
        total = sum(self.holdings.values())
        if abs(total - 1.0) > 1e-3:
            raise ValueError(f"Branches holdings sum to {total:.4f}, must be 1.0")
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
    rebalance: RebalanceConfig
    risk: RiskConfig

    def all_tracked_symbols(self) -> list[str]:
        return list(self.allocation.trunk.holdings) + list(self.allocation.branches.holdings)


class BrokerCredentials(BaseModel):
    api_key: str
    secret_key: str
    paper: bool = True


def load_credentials() -> BrokerCredentials:
    """Load Alpaca credentials from environment or .env."""
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
    """Load and validate the strategy YAML."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Strategy config not found at {path}")
    with open(path) as f:
        data = yaml.safe_load(f)
    return StrategyConfig.model_validate(data)
