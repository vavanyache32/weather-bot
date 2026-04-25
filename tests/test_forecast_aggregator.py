"""Tests for ForecastAggregator."""
from __future__ import annotations

from datetime import date, datetime, timezone

import aiosqlite
import pytest

from bot.db import _SCHEMA
from bot.services.forecast_aggregator import ForecastAggregator


async def _memory_db() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(":memory:")
    conn.row_factory = aiosqlite.Row
    await conn.executescript(_SCHEMA)
    await conn.commit()
    return conn


async def _insert_sample(conn: aiosqlite.Connection) -> None:
    """Insert deterministic + quantile rows for 2026-04-25."""
    rows = [
        # Open-Meteo models
        ("open_meteo", "ecmwf_ifs025", None, "UUWW", 55.5914, 37.2615,
         "2026-04-25T10:00:00+00:00", "2026-04-25T12:00:00+00:00", 2,
         None, 15.0, None, None, ""),
        ("open_meteo", "gfs_seamless", None, "UUWW", 55.5914, 37.2615,
         "2026-04-25T10:00:00+00:00", "2026-04-25T12:00:00+00:00", 2,
         None, 16.0, None, None, ""),
        ("open_meteo", "icon_seamless", None, "UUWW", 55.5914, 37.2615,
         "2026-04-25T10:00:00+00:00", "2026-04-25T12:00:00+00:00", 2,
         None, 14.0, None, None, ""),
        # Ensemble quantiles
        ("open_meteo", "icon_seamless_quantile", None, "UUWW", 55.5914, 37.2615,
         "2026-04-25T10:00:00+00:00", "2026-04-25T12:00:00+00:00", 2,
         None, 13.0, None, 0.1, ""),
        ("open_meteo", "icon_seamless_quantile", None, "UUWW", 55.5914, 37.2615,
         "2026-04-25T10:00:00+00:00", "2026-04-25T12:00:00+00:00", 2,
         None, 15.0, None, 0.5, ""),
        ("open_meteo", "icon_seamless_quantile", None, "UUWW", 55.5914, 37.2615,
         "2026-04-25T10:00:00+00:00", "2026-04-25T12:00:00+00:00", 2,
         None, 17.0, None, 0.9, ""),
    ]
    await conn.executemany(
        """
        INSERT OR IGNORE INTO forecasts
        (source, model, member, station, lat, lon, issued_at, valid_at,
         lead_time_h, air_temperature_c, daily_tmax_c, daily_tmin_c,
         quantile, raw)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    await conn.commit()


@pytest.mark.asyncio
async def test_compute_daily_aggregates() -> None:
    """Aggregator returns consensus, band, and spread for a day."""
    conn = await _memory_db()
    await _insert_sample(conn)
    agg = ForecastAggregator(conn)
    result = await agg.compute_daily_aggregates("UUWW", date(2026, 4, 25))
    await conn.close()

    assert result.station == "UUWW"
    assert result.valid_date == date(2026, 4, 25)
    # Consensus = median of [14, 15, 16] = 15
    assert result.consensus_c == 15.0
    # Band from quantiles: 13..17
    assert result.band_low_c == 13.0
    assert result.band_high_c == 17.0
    # Spread = max - min = 16 - 14 = 2
    assert result.model_spread_c == 2.0
    assert len(result.models) == 3


@pytest.mark.asyncio
async def test_compute_range() -> None:
    """Aggregator returns a list for a date range."""
    conn = await _memory_db()
    await _insert_sample(conn)
    agg = ForecastAggregator(conn)
    results = await agg.compute_range("UUWW", date(2026, 4, 25), date(2026, 4, 26))
    await conn.close()

    assert len(results) == 2
    assert results[0].valid_date == date(2026, 4, 25)
    assert results[0].consensus_c == 15.0
    # Second day has no data
    assert results[1].consensus_c is None
