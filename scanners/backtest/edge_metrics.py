"""Edge metrics for scanner backtest evaluation.

Given a forward-returns DataFrame (output from forward_returns.compute_returns_for_candidates),
compute per-scanner edge at each horizon using multiple win/loss definitions.

Win definitions:
  - hit_rate_any_beat       : beat SPY by any amount (excess > 0)
  - hit_rate_material_beat  : beat SPY by 2%+ (excess > +0.02)
  - hit_rate_strong_beat    : beat SPY by 5%+ (excess > +0.05)
  - hit_rate_absolute_pos   : stock went up at all (forward_return > 0)
  - hit_rate_after_costs    : beat SPY by enough to cover round-trip costs (excess > +0.005)

Magnitude metrics:
  - mean_excess_return      : mean excess vs SPY across all candidates
  - median_excess_return    : median excess (less outlier-sensitive)
  - mean_winner_size        : average size of "wins" (excess > 0)
  - mean_loser_size         : average size of "losses" (excess < 0)
  - win_loss_ratio          : mean_winner_size / |mean_loser_size|

Risk-adjusted:
  - sharpe_excess           : annualized Sharpe of excess returns

Sample size:
  - n_candidates            : total candidate-rows at this horizon
  - n_with_data             : candidates with valid forward returns

Statistical rigor: this is sloppy v1. We do not compute confidence intervals,
t-stats, or regime decomposition. That's Phase 4e v2.

Annualization for Sharpe: we treat the scanner edge as a strategy that holds
a basket of all surfaced candidates for N trading days. Annualization factor
is sqrt(252 / N).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class HorizonMetrics:
    horizon_days: int
    n_candidates: int
    n_with_data: int
    # Hit rates under different win definitions
    hit_rate_any_beat: float
    hit_rate_after_costs: float
    hit_rate_material_beat: float
    hit_rate_strong_beat: float
    hit_rate_absolute_pos: float
    # Magnitude metrics
    mean_excess_return: float
    median_excess_return: float
    std_excess_return: float
    mean_winner_size: float
    mean_loser_size: float
    win_loss_ratio: float
    # Risk-adjusted
    sharpe_excess: float


# Thresholds for win definitions (decimal, not percent)
THRESH_AFTER_COSTS = 0.005       # +0.5% beat covers round-trip costs
THRESH_MATERIAL_BEAT = 0.02      # +2% beat is meaningfully above noise
THRESH_STRONG_BEAT = 0.05        # +5% beat is a real edge


def compute_metrics_for_horizon(returns_df: pd.DataFrame, horizon_days: int) -> Optional[HorizonMetrics]:
    """Compute edge metrics for a single horizon.

    returns_df expected columns: ticker, surface_date, horizon_days,
    forward_return, excess_return.
    Filters to rows matching horizon_days, drops nulls, computes metrics.
    """
    h_df = returns_df[returns_df["horizon_days"] == horizon_days].copy()
    n_total = len(h_df)
    h_df = h_df.dropna(subset=["excess_return"])
    n_with_data = len(h_df)

    if n_with_data == 0:
        return HorizonMetrics(
            horizon_days=horizon_days,
            n_candidates=n_total,
            n_with_data=0,
            hit_rate_any_beat=float("nan"),
            hit_rate_after_costs=float("nan"),
            hit_rate_material_beat=float("nan"),
            hit_rate_strong_beat=float("nan"),
            hit_rate_absolute_pos=float("nan"),
            mean_excess_return=float("nan"),
            median_excess_return=float("nan"),
            std_excess_return=float("nan"),
            mean_winner_size=float("nan"),
            mean_loser_size=float("nan"),
            win_loss_ratio=float("nan"),
            sharpe_excess=float("nan"),
        )

    excess = h_df["excess_return"].astype(float)
    forward = h_df["forward_return"].astype(float) if "forward_return" in h_df.columns else None

    # Hit rates under various definitions
    hit_any = float((excess > 0).mean())
    hit_costs = float((excess > THRESH_AFTER_COSTS).mean())
    hit_material = float((excess > THRESH_MATERIAL_BEAT).mean())
    hit_strong = float((excess > THRESH_STRONG_BEAT).mean())

    if forward is not None:
        forward_clean = forward.dropna()
        hit_abs_pos = float((forward_clean > 0).mean()) if len(forward_clean) > 0 else float("nan")
    else:
        hit_abs_pos = float("nan")

    # Magnitudes
    mean_ex = float(excess.mean())
    median_ex = float(excess.median())
    std_ex = float(excess.std()) if n_with_data > 1 else 0.0

    winners = excess[excess > 0]
    losers = excess[excess < 0]
    mean_winner = float(winners.mean()) if len(winners) > 0 else 0.0
    mean_loser = float(losers.mean()) if len(losers) > 0 else 0.0
    if mean_loser != 0:
        win_loss_ratio = float(mean_winner / abs(mean_loser))
    else:
        win_loss_ratio = float("nan")

    # Annualized Sharpe
    if std_ex > 0 and horizon_days > 0:
        ann_factor = math.sqrt(252.0 / horizon_days)
        sharpe = (mean_ex / std_ex) * ann_factor
    else:
        sharpe = float("nan")

    return HorizonMetrics(
        horizon_days=horizon_days,
        n_candidates=n_total,
        n_with_data=n_with_data,
        hit_rate_any_beat=hit_any,
        hit_rate_after_costs=hit_costs,
        hit_rate_material_beat=hit_material,
        hit_rate_strong_beat=hit_strong,
        hit_rate_absolute_pos=hit_abs_pos,
        mean_excess_return=mean_ex,
        median_excess_return=median_ex,
        std_excess_return=std_ex,
        mean_winner_size=mean_winner,
        mean_loser_size=mean_loser,
        win_loss_ratio=win_loss_ratio,
        sharpe_excess=sharpe,
    )


def compute_edge_report(
    returns_df: pd.DataFrame,
    scanner_name: str,
    horizons: List[int],
) -> pd.DataFrame:
    """Build a per-scanner, per-horizon edge report DataFrame.

    Returns one row per horizon with all metrics. Suitable for concatenation
    across multiple scanners into a master edge_report.csv.
    """
    rows = []
    for h in horizons:
        m = compute_metrics_for_horizon(returns_df, h)
        if m is None:
            continue
        rows.append({
            "scanner": scanner_name,
            "horizon_days": m.horizon_days,
            "n_candidates": m.n_candidates,
            "n_with_data": m.n_with_data,
            # Hit rates under multiple definitions
            "hit_any_beat": round(m.hit_rate_any_beat, 4),
            "hit_after_costs": round(m.hit_rate_after_costs, 4),
            "hit_material_2pct": round(m.hit_rate_material_beat, 4),
            "hit_strong_5pct": round(m.hit_rate_strong_beat, 4),
            "hit_absolute_pos": round(m.hit_rate_absolute_pos, 4) if not math.isnan(m.hit_rate_absolute_pos) else float("nan"),
            # Magnitudes (in percent)
            "mean_excess_pct": round(m.mean_excess_return * 100, 3),
            "median_excess_pct": round(m.median_excess_return * 100, 3),
            "std_excess_pct": round(m.std_excess_return * 100, 3),
            "mean_winner_pct": round(m.mean_winner_size * 100, 3),
            "mean_loser_pct": round(m.mean_loser_size * 100, 3),
            "win_loss_ratio": round(m.win_loss_ratio, 3) if not math.isnan(m.win_loss_ratio) else float("nan"),
            # Risk-adjusted
            "sharpe_annualized": round(m.sharpe_excess, 3) if not math.isnan(m.sharpe_excess) else float("nan"),
        })
    return pd.DataFrame(rows)