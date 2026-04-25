"""Shared domain models used across services and presentation layers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class DailyExtremes:
    """Daily temperature extremes for a single date."""

    date: date
    temp_max_c: float
    temp_min_c: Optional[float] = None
