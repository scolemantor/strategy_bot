"""Base scanner class. All 17 scanners inherit from this.

The contract is intentionally tiny:
  - Each scanner has a name, description, and run() method
  - run() returns a ScanResult containing a DataFrame of candidates
  - Required columns: ticker, score, reason
  - Anything else is scanner-specific data added as columns

Scanners are independent. They never share state. They never call each other.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

log = logging.getLogger(__name__)


@dataclass
class ScanResult:
    scanner_name: str
    run_date: date
    candidates: pd.DataFrame
    error: Optional[str] = None
    notes: List[str] = field(default_factory=list)
    rejected_candidates: Optional[pd.DataFrame] = None

    @property
    def count(self) -> int:
        return 0 if self.candidates is None else len(self.candidates)

    def is_success(self) -> bool:
        return self.error is None


class Scanner(ABC):
    """Abstract base for all scanners."""

    name: str = "unnamed"
    description: str = ""
    cadence: str = "daily"
    requires_paid_data: bool = False

    @abstractmethod
    def run(self, run_date: date) -> ScanResult:
        """Execute the scan and return ranked candidates."""
        ...

    def __str__(self) -> str:
        paid = " [PAID DATA]" if self.requires_paid_data else ""
        return f"{self.name}: {self.description}{paid}"


def empty_result(scanner_name: str, run_date: date, error: Optional[str] = None) -> ScanResult:
    """Helper for scanners that completed but found nothing, or errored out."""
    return ScanResult(
        scanner_name=scanner_name,
        run_date=run_date,
        candidates=pd.DataFrame(columns=["ticker", "score", "reason"]),
        error=error,
    )


def save_result(result: ScanResult, output_dir: Path) -> Optional[Path]:
    """Write a scan result to CSV. Returns the path or None if nothing to save."""
    if result.candidates is None or result.candidates.empty:
        return None
    day_dir = output_dir / result.run_date.isoformat()
    day_dir.mkdir(parents=True, exist_ok=True)
    out_path = day_dir / f"{result.scanner_name}.csv"
    try:
        result.candidates.to_csv(out_path, index=False)
        return out_path
    except Exception as e:
        log.error(f"Failed to save {result.scanner_name}: {e}")
        return None