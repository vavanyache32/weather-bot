"""Tests for forecast pipeline in WeatherScheduler."""
from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import pytest

from bot.db import _SCHEMA
from bot.models import ForecastPoint
from bot.scheduler import WeatherScheduler
from bot.services.forecast_aggregator import ForecastAggregator
from bot.storage import StateStore


async def _memory_db() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(":memory:")
    conn.row_factory = aiosqlite.Row
    await conn.executescript(_SCHEMA)
    await conn.commit()
    return conn


class MockProvider:
    """A fake forecast provider that returns canned points."""

    def __init__(self, points: list[ForecastPoint]) -> None:
        self._points = points

    async def fetch(self) -> list[ForecastPoint]:
        return self._points


def _make_scheduler(tmp_path, db: aiosqlite.Connection, providers: list) -> WeatherScheduler:
    config = MagicMock()
    config.timezone = "Europe/Moscow"
    config.noaa_station = "UUWW"
    config.forecast_days = 3
    config.forecast_detailed_refresh_seconds = 0  # always refresh

    aggregator = ForecastAggregator(db)

    sched = WeatherScheduler(
        config=config,
        bot=MagicMock(),
        store=StateStore(tmp_path / "state.json"),
        noaa=MagicMock(),
        yandex=MagicMock(),
        forecast=MagicMock(),
        llm=MagicMock(),
        db=db,
        forecast_providers=providers,
        aggregator=aggregator,
    )
    return sched


@pytest.mark.asyncio
async def test_refresh_detailed_inserts_and_aggregates(tmp_path) -> None:
    """Detailed refresh inserts points into SQLite and writes aggregates to state."""
    db = await _memory_db()
    points = [
        ForecastPoint(
            source="open_meteo",
            model="ecmwf_ifs025",
            station="UUWW",
            issued_at=datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc),
            valid_at=datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
            lead_time_h=2,
            daily_tmax_c=15.0,
        ),
        ForecastPoint(
            source="open_meteo",
            model="gfs_seamless",
            station="UUWW",
            issued_at=datetime(2026, 4, 25, 10, 0, tzinfo=timezone.utc),
            valid_at=datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
            lead_time_h=2,
            daily_tmax_c=17.0,
        ),
    ]
    sched = _make_scheduler(tmp_path, db, [MockProvider(points)])

    result = await sched.refresh_forecast_detailed(force=True)
    assert result is True

    state = await sched._store.get()
    assert len(state.forecast_aggregates) > 0
    agg = state.forecast_aggregates[0]
    assert agg["station"] == "UUWW"
    assert agg["valid_date"] == "2026-04-25"
    # Consensus = median of [15, 17] = 16
    assert agg["consensus_c"] == 16.0
    assert agg["model_spread_c"] == 2.0

    await db.close()
