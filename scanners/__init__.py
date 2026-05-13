"""Registry of all scanners. Add new ones here as they're built.

Currently 16 scanners registered, 15 active in scan_all:
  - #14 options_unusual added Phase 4g.1 (2026-05-12) — first paid-data
    scanner; requires UNUSUAL_WHALES_API_TOKEN env var. Returns empty
    cleanly (not an error) when token is missing, so other scanners in
    scan_all are unaffected.
  - #18 congressional_trades enabled in scan_all 2026-05-09 after
    standalone validation. Pending Phase 4g.1b UW-backed replacement;
    current Quiver impl remains active until that ships.
  - technical_overlay (Phase 8a) excluded from scan_all — runs intraday
    on its own cron, not as part of the daily pipeline.
  - #15-17 (FMP fundamentals, Polygon, Benzinga) deferred per Phase
    4g.2/4g.3/4g.4 roadmap.
"""
from __future__ import annotations

from typing import Dict, Type

from .base import Scanner
from .breakout_52w import Breakout52wScanner
from .congressional_trades import CongressionalTradesScanner
from .earnings_calendar import EarningsCalendarScanner
from .earnings_drift import EarningsDriftScanner
from .fda_calendar import FdaCalendarScanner
from .insider_buying import InsiderBuyingScanner
from .insider_selling_clusters import InsiderSellingClustersScanner
from .ipo_lockup import IpoLockupScanner
from .macro_calendar import MacroCalendarScanner
from .options_unusual import OptionsUnusualScanner
from .sector_rotation import SectorRotationScanner
from .short_squeeze import ShortSqueezeScanner
from .small_cap_value import SmallCapValueScanner
from .spinoff_tracker import SpinoffTrackerScanner
from .technical_overlay import TechnicalOverlayScanner
from .thirteen_f_changes import ThirteenFChangesScanner

SCANNERS: Dict[str, Type[Scanner]] = {
    "insider_buying": InsiderBuyingScanner,
    "breakout_52w": Breakout52wScanner,
    "earnings_drift": EarningsDriftScanner,
    "spinoff_tracker": SpinoffTrackerScanner,
    "fda_calendar": FdaCalendarScanner,
    "thirteen_f_changes": ThirteenFChangesScanner,
    "short_squeeze": ShortSqueezeScanner,
    "small_cap_value": SmallCapValueScanner,
    "sector_rotation": SectorRotationScanner,
    "earnings_calendar": EarningsCalendarScanner,
    "macro_calendar": MacroCalendarScanner,
    "ipo_lockup": IpoLockupScanner,
    "insider_selling_clusters": InsiderSellingClustersScanner,
    "options_unusual": OptionsUnusualScanner,  # #14 — Phase 4g.1
    "congressional_trades": CongressionalTradesScanner,
    "technical_overlay": TechnicalOverlayScanner,
}

# Scanners temporarily skipped during `scan.py all` but still available
# via `scan.py run NAME`. Add a comment with reason + date when disabling.
DISABLED_IN_SCAN_ALL: set[str] = {
    # Phase 8a: technical_overlay runs as a standalone scheduled job
    # (every 15 min during market hours) — its cadence is intraday, not
    # daily, so it shouldn't fire as part of scan_all.
    "technical_overlay",
}


def get_scanner(name: str) -> Scanner:
    if name not in SCANNERS:
        raise KeyError(f"Unknown scanner: {name}. Available: {list(SCANNERS.keys())}")
    return SCANNERS[name]()


def list_scanners() -> Dict[str, Scanner]:
    return {name: cls() for name, cls in SCANNERS.items()}