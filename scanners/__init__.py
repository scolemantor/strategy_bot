"""Registry of all scanners. Add new ones here as they're built."""
from __future__ import annotations

from typing import Dict, Type

from .base import Scanner
from .breakout_52w import Breakout52wScanner
from .earnings_drift import EarningsDriftScanner
from .fda_calendar import FdaCalendarScanner
from .insider_buying import InsiderBuyingScanner
from .spinoff_tracker import SpinoffTrackerScanner

SCANNERS: Dict[str, Type[Scanner]] = {
    "insider_buying": InsiderBuyingScanner,
    "breakout_52w": Breakout52wScanner,
    "earnings_drift": EarningsDriftScanner,
    "spinoff_tracker": SpinoffTrackerScanner,
    "fda_calendar": FdaCalendarScanner,
}


def get_scanner(name: str) -> Scanner:
    if name not in SCANNERS:
        raise KeyError(f"Unknown scanner: {name}. Available: {list(SCANNERS.keys())}")
    return SCANNERS[name]()


def list_scanners() -> Dict[str, Scanner]:
    return {name: cls() for name, cls in SCANNERS.items()}