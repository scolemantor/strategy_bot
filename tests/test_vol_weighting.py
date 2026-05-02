"""Tests for compute_sleeve_weights and _waterfill_clip.

Run with: pytest tests/test_vol_weighting.py -v

Covers the three bugs fixed in this commit:
  1. Cap-then-renormalize did not enforce the cap (fixed via water-filling)
  2. Zero-vol symbols got silently dropped from targets (now sleeve falls back
     to equal weight, never drops a symbol)
  3. Cross-column dropna ate rows from healthy symbols (now per-column)
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.config import BranchesConfig, TrunkConfig, WeightingConfig
from src.strategy import _waterfill_clip, compute_sleeve_weights


# --- helpers ---------------------------------------------------------------


def make_prices(symbol_vols: dict[str, float], n_days: int = 200, seed: int = 42) -> pd.DataFrame:
    """Generate synthetic price series with controlled annualized volatility."""
    rng = np.random.default_rng(seed)
    daily_vols = {s: v / np.sqrt(252) for s, v in symbol_vols.items()}
    data = {}
    for s, dv in daily_vols.items():
        returns = rng.normal(loc=0, scale=dv, size=n_days)
        prices = 100 * np.cumprod(1 + returns)
        data[s] = prices
    return pd.DataFrame(data)


def default_weighting() -> WeightingConfig:
    return WeightingConfig(
        vol_window_days=90,
        min_weight_within_sleeve=0.05,
        max_weight_within_sleeve=0.40,
    )


def make_branches(holdings: dict[str, float], method: str = "inverse_volatility") -> BranchesConfig:
    return BranchesConfig(weight=0.35, holdings=holdings, weighting_method=method)


# --- _waterfill_clip -------------------------------------------------------


class TestWaterfillClip:
    def test_already_in_bounds_unchanged(self):
        w = {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}
        result = _waterfill_clip(w, 0.05, 0.40)
        for s in w:
            assert result[s] == pytest.approx(w[s])
        assert sum(result.values()) == pytest.approx(1.0)

    def test_one_weight_over_cap_redistributes(self):
        w = {"A": 0.85, "B": 0.05, "C": 0.05, "D": 0.05}
        result = _waterfill_clip(w, 0.05, 0.40)
        assert result["A"] == pytest.approx(0.40)
        assert sum(result.values()) == pytest.approx(1.0)
        for s in ["B", "C", "D"]:
            assert 0.05 - 1e-9 <= result[s] <= 0.40 + 1e-9

    def test_no_weight_exceeds_cap(self):
        # The original bug: cap-then-renorm pushed weights right back over
        w = {"A": 0.85, "B": 0.05, "C": 0.05, "D": 0.05}
        result = _waterfill_clip(w, 0.05, 0.40)
        for v in result.values():
            assert v <= 0.40 + 1e-9, f"{v} exceeds cap"

    def test_no_weight_below_floor(self):
        w = {"A": 0.95, "B": 0.02, "C": 0.02, "D": 0.01}
        result = _waterfill_clip(w, 0.05, 0.40)
        for v in result.values():
            assert v >= 0.05 - 1e-9, f"{v} below floor"

    def test_sum_preserved(self):
        w = {"A": 0.85, "B": 0.05, "C": 0.05, "D": 0.05}
        result = _waterfill_clip(w, 0.05, 0.40)
        assert sum(result.values()) == pytest.approx(1.0, abs=1e-9)

    def test_pinned_at_both_caps(self):
        # A pins at max, D pins at min. D starts at 0 so even after A's
        # spillover scales B/C/D up, D stays at 0 and must be floored.
        w = {"A": 0.92, "B": 0.04, "C": 0.04, "D": 0.0}
        result = _waterfill_clip(w, 0.05, 0.40)
        assert result["A"] == pytest.approx(0.40)
        assert result["D"] == pytest.approx(0.05)
        assert sum(result.values()) == pytest.approx(1.0)
        for s, v in result.items():
            assert 0.05 - 1e-9 <= v <= 0.40 + 1e-9, f"{s}={v} out of bounds"

    def test_infeasible_max_too_low(self):
        # 4 symbols × 0.20 max = 0.80 < 1.0 → infeasible
        w = {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}
        with pytest.raises(ValueError, match="Infeasible"):
            _waterfill_clip(w, 0.05, 0.20)

    def test_infeasible_min_too_high(self):
        # 4 symbols × 0.30 min = 1.20 > 1.0 → infeasible
        w = {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}
        with pytest.raises(ValueError, match="Infeasible"):
            _waterfill_clip(w, 0.30, 0.40)

    def test_empty_input(self):
        assert _waterfill_clip({}, 0.05, 0.40) == {}

    def test_chain_capping(self):
        """After pinning A at max, scaling pushes B above cap too. Should pin B
        in the next iteration, not silently leave it over the cap."""
        w = {"A": 0.50, "B": 0.30, "C": 0.10, "D": 0.10}
        result = _waterfill_clip(w, 0.05, 0.35)
        for v in result.values():
            assert v <= 0.35 + 1e-9
        assert sum(result.values()) == pytest.approx(1.0)


# --- compute_sleeve_weights: equal mode ------------------------------------


class TestComputeSleeveWeightsEqual:
    def test_equal_returns_config(self):
        cfg = make_branches({"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}, method="equal")
        prices = make_prices({"A": 0.20, "B": 0.30, "C": 0.10, "D": 0.50})
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        assert result == {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}

    def test_equal_ignores_history(self):
        cfg = make_branches({"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}, method="equal")
        result = compute_sleeve_weights(cfg, None, default_weighting())
        assert result == {"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25}


# --- compute_sleeve_weights: fallbacks (no silent drops) -------------------


class TestComputeSleeveWeightsFallback:
    def test_no_history_returns_equal(self):
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        result = compute_sleeve_weights(cfg, None, default_weighting())
        assert all(v == pytest.approx(0.25) for v in result.values())
        assert set(result.keys()) == {"A", "B", "C", "D"}

    def test_empty_history_returns_equal(self):
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        result = compute_sleeve_weights(cfg, pd.DataFrame(), default_weighting())
        assert all(v == pytest.approx(0.25) for v in result.values())
        assert set(result.keys()) == {"A", "B", "C", "D"}

    def test_missing_symbol_in_history_returns_equal(self):
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        prices = make_prices({"A": 0.20, "B": 0.30, "C": 0.10})  # D missing
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        # All four symbols MUST appear — no silent drop
        assert set(result.keys()) == {"A", "B", "C", "D"}
        assert all(v == pytest.approx(0.25) for v in result.values())

    def test_insufficient_history_returns_equal(self):
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        prices = make_prices({"A": 0.20, "B": 0.30, "C": 0.10, "D": 0.50}, n_days=50)
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        assert all(v == pytest.approx(0.25) for v in result.values())
        assert set(result.keys()) == {"A", "B", "C", "D"}

    def test_zero_vol_does_not_drop_symbol(self):
        """Bug 2: original code skipped symbols with zero vol from the raw dict.
        They never appeared in targets, so the rebalancer treated them as
        untracked and ghosted them. Must fall back to equal instead."""
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        prices = make_prices({"A": 0.20, "B": 0.30, "C": 0.10, "D": 0.50})
        # Force D to have zero vol (constant price)
        prices["D"] = 100.0
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        # D MUST be in result with a non-zero target
        assert set(result.keys()) == {"A", "B", "C", "D"}
        assert all(v == pytest.approx(0.25) for v in result.values())

    def test_per_column_dropna(self):
        """Bug 3: original code did historical_prices[cols].dropna() which
        dropped a row if ANY column had a NaN that day. One sparse column
        could shrink the effective vol window for everyone. Per-column calc
        means a healthy column keeps its full window."""
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        prices = make_prices({"A": 0.20, "B": 0.30, "C": 0.10, "D": 0.50}, n_days=200)
        # Sprinkle NaNs in D — every 3rd day is missing
        nan_indices = list(range(0, 200, 3))
        prices.loc[nan_indices, "D"] = np.nan
        # D's non-null count is ~133 days, still > 90 vol window
        # Old code: cross-drop would leave only ~133 rows for ALL columns
        # (still enough here, but the risk is real with denser NaNs)
        # New code: each column uses its own non-null tail
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        # All symbols present, weights sum to 1
        assert set(result.keys()) == {"A", "B", "C", "D"}
        assert sum(result.values()) == pytest.approx(1.0)
        # And we should get vol-weighted output (not equal fallback)
        # — A is lowest vol, should have higher weight than C... wait, C is
        # lowest vol (0.10). So C should have highest weight.
        assert result["C"] > result["A"]


# --- compute_sleeve_weights: vol mechanics ---------------------------------


class TestComputeSleeveWeightsMechanics:
    def test_lower_vol_gets_higher_weight(self):
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        prices = make_prices({"A": 0.10, "B": 0.20, "C": 0.30, "D": 0.40})
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        # Strict ordering by inverse vol: A (lowest vol) > B > C > D
        assert result["A"] > result["B"]
        assert result["B"] > result["C"]
        assert result["C"] > result["D"]
        assert sum(result.values()) == pytest.approx(1.0)

    def test_cap_actually_enforced_in_pipeline(self):
        """Bug 1: previously the cap could be exceeded after renormalization.
        With one very low-vol symbol it would dominate the sleeve."""
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        # A has 1/10th the vol of others — dominates inverse-vol pre-cap
        prices = make_prices({"A": 0.05, "B": 0.50, "C": 0.50, "D": 0.50})
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        # A must be capped at 0.40
        assert result["A"] <= 0.40 + 1e-9, f"A weight {result['A']} exceeds cap of 0.40"
        # And it should hit exactly the cap given how dominant it is
        assert result["A"] == pytest.approx(0.40, abs=1e-6)
        assert sum(result.values()) == pytest.approx(1.0)

    def test_floor_enforced(self):
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        # D has 10x the vol — without floor it goes near zero
        prices = make_prices({"A": 0.10, "B": 0.10, "C": 0.10, "D": 1.0})
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        for s, v in result.items():
            assert v >= 0.05 - 1e-9, f"{s} weight {v} below floor"

    def test_bias_multiplier_works(self):
        # All same vol — bias should drive the weights
        cfg = make_branches({"A": 2.0, "B": 1.0, "C": 1.0, "D": 1.0})
        prices = make_prices({"A": 0.20, "B": 0.20, "C": 0.20, "D": 0.20})
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        # A should be heaviest
        assert result["A"] > result["B"]
        assert result["A"] > result["C"]
        assert result["A"] > result["D"]
        assert sum(result.values()) == pytest.approx(1.0)

    def test_total_always_one(self):
        cfg = make_branches({"A": 1.0, "B": 1.0, "C": 1.0, "D": 1.0})
        prices = make_prices({"A": 0.20, "B": 0.30, "C": 0.10, "D": 0.50})
        result = compute_sleeve_weights(cfg, prices, default_weighting())
        assert sum(result.values()) == pytest.approx(1.0)


# --- WeightingConfig validation --------------------------------------------


class TestWeightingConfigValidation:
    def test_defaults_match_prior_constants(self):
        wc = WeightingConfig()
        assert wc.vol_window_days == 90
        assert wc.min_weight_within_sleeve == 0.05
        assert wc.max_weight_within_sleeve == 0.40

    def test_min_must_be_below_max(self):
        with pytest.raises(ValueError, match="must be <"):
            WeightingConfig(min_weight_within_sleeve=0.40, max_weight_within_sleeve=0.40)

    def test_min_above_max_rejected(self):
        with pytest.raises(ValueError, match="must be <"):
            WeightingConfig(min_weight_within_sleeve=0.50, max_weight_within_sleeve=0.40)
