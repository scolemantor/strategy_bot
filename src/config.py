"""Configuration loading and validation."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Literal, Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator


WeightingMethod = Literal["equal", "inverse_volatility"]
RiskClass = Literal["equity", "defensive"]


class HoldingConfig(BaseModel):
    """Per-holding config: weight (or bias multiplier) plus risk classification.

    - weight: for equal-weighted sleeves, the within-sleeve weight (the values
      across a sleeve must sum to 1.0). For inverse-volatility sleeves, a bias
      multiplier (use 1.0 for pure inverse-vol).
    - risk_class: 'equity' holdings get scaled down by the regime risk
      multiplier when SPY drops below its 200dma. 'defensive' holdings (BND,
      GLD, etc.) hold their static target through regime changes — they're
      the assets you want to keep when equities sell off.
    """
    weight: float = Field(..., ge=0)
    risk_class: RiskClass = "equity"


def _normalize_holdings(value: Any) -> Any:
    """Convert legacy flat-dict format ({'VTI': 0.70}) to structured format
    ({'VTI': {weight: 0.70, risk_class: 'equity'}}).

    Accepts either format and returns the structured form. Untagged holdings
    default to risk_class='equity' for backward compatibility.
    """
    if not isinstance(value, dict):
        return value  # let Pydantic raise the right error

    normalized: Dict[str, Any] = {}
    for symbol, entry in value.items():
        if isinstance(entry, (int, float)):
            normalized[symbol] = {"weight": float(entry), "risk_class": "equity"}
        elif isinstance(entry, dict):
            normalized[symbol] = entry
        elif isinstance(entry, HoldingConfig):
            normalized[symbol] = entry.model_dump()
        else:
            normalized[symbol] = entry  # let Pydantic raise on unknown type
    return normalized


class TrunkConfig(BaseModel):
    weight: float = Field(..., ge=0, le=1)
    holdings: Dict[str, HoldingConfig]
    weighting_method: WeightingMethod = "equal"

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if isinstance(data, dict) and "holdings" in data:
            data = {**data, "holdings": _normalize_holdings(data["holdings"])}
        return data

    @model_validator(mode="after")
    def weights_valid(self):
        if self.weighting_method == "equal":
            total = sum(h.weight for h in self.holdings.values())
            if abs(total - 1.0) > 1e-3:
                raise ValueError(f"Trunk equal-weight holdings sum to {total:.4f}, must be 1.0")
        else:
            if any(h.weight <= 0 for h in self.holdings.values()):
                raise ValueError("Inverse-vol bias multipliers must be positive")
        return self


class BranchesConfig(BaseModel):
    weight: float = Field(..., ge=0, le=1)
    holdings: Dict[str, HoldingConfig]
    weighting_method: WeightingMethod = "equal"

    @model_validator(mode="before")
    @classmethod
    def _normalize(cls, data: Any) -> Any:
        if isinstance(data, dict) and "holdings" in data:
            data = {**data, "holdings": _normalize_holdings(data["holdings"])}
        return data

    @model_validator(mode="after")
    def weights_valid(self):
        if self.weighting_method == "equal":
            total = sum(h.weight for h in self.holdings.values())
            if abs(total - 1.0) > 1e-3:
                raise ValueError(f"Branches equal-weight holdings sum to {total:.4f}, must be 1.0")
        else:
            if any(h.weight <= 0 for h in self.holdings.values()):
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
    """200dma trend-following regime overlay with whipsaw protection.

    Flips OFF when the benchmark closes below its `ma_window`-day moving
    average by more than `buffer_pct` for `min_consecutive_days` consecutive
    days. Flips ON when the benchmark closes above MA × (1 + buffer_pct) for
    min_consecutive_days. Otherwise maintains prior state, which is determined
    by scanning history backwards for the most recent unambiguous N-day window.

    Stateless: same price history always produces the same regime.

    When OFF, holdings tagged risk_class='equity' scale by (1 - offsignal_cash_pct).
    Holdings tagged 'defensive' (BND, GLD) hold static targets — they're the
    assets meant to anchor the portfolio during equity drawdowns.

    Set buffer_pct=0 and min_consecutive_days=1 to revert to the original
    daily-flip behavior (not recommended — generates whipsaws in choppy
    markets and creates unnecessary tax events in taxable accounts).
    """
    enabled: bool = False
    benchmark: str = "SPY"
    ma_window: int = Field(200, ge=10)
    offsignal_cash_pct: float = Field(0.40, ge=0, le=1)
    buffer_pct: float = Field(0.02, ge=0, le=0.5)
    min_consecutive_days: int = Field(3, ge=1, le=30)


class RebalanceConfig(BaseModel):
    drift_threshold: float = Field(0.05, ge=0, le=1)
    min_order_size_usd: float = Field(100, ge=0)


class RiskConfig(BaseModel):
    max_order_pct_of_portfolio: float = Field(0.05, ge=0, le=1)
    max_daily_orders: int = Field(20, ge=0)
    drawdown_kill_switch_pct: float = Field(0.05, ge=0, le=1)
    require_market_hours: bool = True


class LedgerConfig(BaseModel):
    """Tax lot ledger configuration.

    When enabled, the bot maintains a SQLite database of every buy/sell to
    track per-lot cost basis for tax-aware sell decisions (HIFO with
    long-term preference and opportunistic loss harvesting).

    The database is persistent and survives across runs. Default path is
    outside the repo so it survives clean clones — override for tests or
    multi-account setups. The path is expanded with ~ for home directory.

    When disabled (default), the bot runs exactly as it did pre-Phase 3:
    no ledger reads, no ledger writes, no reconciliation. Existing YAMLs
    without a ledger: block continue to work unchanged.
    """
    enabled: bool = False
    db_path: str = "~/strategy_bot_data/lot_ledger.sqlite"


class PortfolioConfig(BaseModel):
    total_target_value_usd: float = Field(..., gt=0)


class StrategyConfig(BaseModel):
    portfolio: PortfolioConfig
    allocation: AllocationConfig
    weighting: WeightingConfig = Field(default_factory=WeightingConfig)
    rebalance: RebalanceConfig
    risk: RiskConfig
    regime: RegimeConfig = Field(default_factory=RegimeConfig)
    ledger: LedgerConfig = Field(default_factory=LedgerConfig)

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
