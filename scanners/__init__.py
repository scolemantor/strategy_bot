"""Registry of all scanners. Add new ones here as they're built.

Currently 14 scanners registered, 14 active in scan_all (#18
congressional_trades enabled in scan_all 2026-05-09 after standalone
validation; #14-17 paid-data scanners still deferred per Phase 4g).
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
from .sector_rotation import SectorRotationScanner
from .short_squeeze import ShortSqueezeScanner
from .small_cap_value import SmallCapValueScanner
from .spinoff_tracker import SpinoffTrackerScanner
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
    "congressional_trades": CongressionalTradesScanner,
}

# Scanners temporarily skipped during `scan.py all` but still available
# via `scan.py run NAME`. Add a comment with reason + date when disabling.
DISABLED_IN_SCAN_ALL: set[str] = set()


def get_scanner(name: str) -> Scanner:
    if name not in SCANNERS:
        raise KeyError(f"Unknown scanner: {name}. Available: {list(SCANNERS.keys())}")
    return SCANNERS[name]()


def list_scanners() -> Dict[str, Scanner]:
    return {name: cls() for name, cls in SCANNERS.items()}