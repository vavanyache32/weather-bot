"""Aggregate raw forecast rows into daily summary statistics.

Consensus, prediction band, model spread, mixed forecast.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from statistics import median, quantiles
from typing import Optional

import aiosqlite

from ..models import DailyForecastAggregate, ForecastPoint

logger = logging.getLogger(__name__)


class ForecastAggregator:
    """Read ``forecasts`` table and compute daily aggregates."""

    def __init__(self, db: aiosqlite.Connection) -> None:
        self._db = db

    async def compute_daily_aggregates(
        self,
        station: str,
        valid_date: date,
    ) -> DailyForecastAggregate:
        """Return aggregated forecast summary for ``valid_date``."""
        start = datetime(valid_date.year, valid_date.month, valid_date.day, tzinfo=timezone.utc)
        end = start + timedelta(days=1)

        rows = await self._db.execute_fetchall(
            """
            SELECT source, model, daily_tmax_c, quantile
            FROM forecasts
            WHERE station = ? AND valid_at >= ? AND valid_at < ?
            """,
            (station, start.isoformat(), end.isoformat()),
        )

        # Separate deterministic vs quantile rows
        deterministic: list[tuple[str, Optional[str], float]] = []
        quantile_rows: list[tuple[float, float]] = []  # (quantile, value)

        for row in rows:
            source, model, tmax, q = row
            if tmax is None:
                continue
            if q is not None:
                quantile_rows.append((q, float(tmax)))
            else:
                deterministic.append((source, model, float(tmax)))

        if not deterministic and not quantile_rows:
            return DailyForecastAggregate(station=station, valid_date=valid_date)

        # Consensus = median of deterministic daily_tmax
        det_values = [v for _, _, v in deterministic]
        consensus = median(det_values) if det_values else None

        # Band from quantiles (prefer 0.1 / 0.9 from ensemble)
        band_low: Optional[float] = None
        band_high: Optional[float] = None
        q_map = {q: v for q, v in quantile_rows}
        if 0.1 in q_map and 0.9 in q_map:
            band_low, band_high = q_map[0.1], q_map[0.9]
        elif det_values:
            try:
                q10, _, q90 = quantiles(det_values, n=10, method="inclusive")
                band_low, band_high = q10, q90
            except Exception:
                band_low, band_high = min(det_values), max(det_values)

        # Spread = max - min across deterministic models
        spread = (max(det_values) - min(det_values)) if det_values else None

        return DailyForecastAggregate(
            station=station,
            valid_date=valid_date,
            consensus_c=consensus,
            band_low_c=band_low,
            band_high_c=band_high,
            model_spread_c=spread,
            models=deterministic,
            sources_used=list(dict.fromkeys(s for s, _, _ in deterministic)),
        )

    async def compute_range(
        self,
        station: str,
        start_date: date,
        end_date: date,
    ) -> list[DailyForecastAggregate]:
        """Compute aggregates for every day in the inclusive range."""
        out: list[DailyForecastAggregate] = []
        current = start_date
        while current <= end_date:
            agg = await self.compute_daily_aggregates(station, current)
            out.append(agg)
            current += timedelta(days=1)
        return out
