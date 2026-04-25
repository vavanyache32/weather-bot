"""Shared domain models used across services and presentation layers."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


@dataclass
class DailyExtremes:
    """Daily temperature extremes for a single date."""

    date: date
    temp_max_c: float
    temp_min_c: Optional[float] = None


@dataclass
class Observation:
    """A single observation from any source.

    * ``observed_at`` — UTC, timezone-aware.
    * ``max_temperature_c`` — explicit daily maximum reported by the source
      (e.g. SYNOP section 333 or IEM ``max_dayairtemp``).  ``None`` when the
      source does not provide an explicit max.
    * ``raw`` — original message / line for auditability.
    """

    source: str
    station: str
    observed_at: datetime
    air_temperature_c: Optional[float] = None
    max_temperature_c: Optional[float] = None
    raw: str = ""

    def dedup_key(self) -> tuple[str, str, str]:
        return (self.source, self.station, self.observed_at.isoformat())


@dataclass
class DailyMaxAggregate:
    """Result of aggregating observations for a single local day."""

    value_c: Optional[int] = None
    source: Optional[str] = None
    confirmed_by: list[str] = field(default_factory=list)


@dataclass
class ForecastPoint:
    """A single forecast point from any predictive source.

    * ``issued_at`` — when the forecast was released (UTC).
    * ``valid_at`` — the time the forecast is valid for (UTC).
    * ``lead_time_h`` — difference valid_at − issued_at in hours.
    * ``daily_tmax_c`` — explicit daily maximum if the source provides it.
    * ``air_temperature_c`` — point/hourly temperature if the source provides it.
    * ``quantile`` — for ensemble summaries (0.1, 0.5, 0.9, …).
    """

    source: str
    station: str
    issued_at: datetime
    valid_at: datetime
    lead_time_h: int
    lat: float = 55.5914
    lon: float = 37.2615
    model: Optional[str] = None
    member: Optional[int] = None
    air_temperature_c: Optional[float] = None
    daily_tmax_c: Optional[float] = None
    daily_tmin_c: Optional[float] = None
    quantile: Optional[float] = None
    raw: str = ""


@dataclass
class DailyForecastAggregate:
    """Aggregated forecast summary for a single day."""

    station: str
    valid_date: date
    consensus_c: Optional[float] = None
    band_low_c: Optional[float] = None
    band_high_c: Optional[float] = None
    model_spread_c: Optional[float] = None
    mixed_forecast_c: Optional[float] = None
    models: list[tuple[str, Optional[str], float]] = field(default_factory=list)
    sources_used: list[str] = field(default_factory=list)
