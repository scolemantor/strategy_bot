"""Registry of all scanners. Add new ones here as they're built."""
from __future__ import annotations

from typing import Dict, Type

from .base import Scanner
from .insider_buying import InsiderBuyingScanner

SCANNERS: Dict[str, Type[Scanner]] = {
    "insider_buying": InsiderBuyingScanner,
}


def get_scanner(name: str) -> Scanner:
    if name not in SCANNERS:
        raise KeyError(f"Unknown scanner: {name}. Available: {list(SCANNERS.keys())}")
    return SCANNERS[name]()


def list_scanners() -> Dict[str, Scanner]:
    return {name: cls() for name, cls in SCANNERS.items()}