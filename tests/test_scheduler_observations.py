"""Tests for observation aggregation and daily-max logic in WeatherScheduler."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from bot.models import Observation
from bot.scheduler import WeatherScheduler


@pytest.fixture
def scheduler():
    """Return a WeatherScheduler with mocked dependencies."""
    config = MagicMock()
    config.timezone = "Europe/Moscow"
    config.noaa_station = "UUWW"
    return WeatherScheduler(
        config=config,
        bot=MagicMock(),
        store=MagicMock(),
        noaa=MagicMock(),
        yandex=MagicMock(),
        forecast=MagicMock(),
        llm=MagicMock(),
        db=MagicMock(),
    )


class TestDailyMaxAggregation:
    def test_explicit_max_priority(self, scheduler):
        """When explicit max exists, it wins over point observations."""
        scheduler._observations = [
            Observation(
                source="noaa_metar",
                station="UUWW",
                observed_at=datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
                air_temperature_c=12.0,
            ),
            Observation(
                source="ogimet_synop",
                station="27611",
                observed_at=datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
                air_temperature_c=14.0,
                max_temperature_c=15.0,
            ),
            Observation(
                source="yandex",
                station="UUWW",
                observed_at=datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
                air_temperature_c=13.0,
            ),
        ]
        result = scheduler._compute_daily_max("2026-04-25")
        assert result.value_c == 15
        assert "explicit" in (result.source or "")
        assert "ogimet_synop" in (result.source or "")

    def test_point_fallback_when_no_explicit(self, scheduler):
        """Without explicit max, highest point observation is chosen."""
        scheduler._observations = [
            Observation(
                source="noaa_metar",
                station="UUWW",
                observed_at=datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
                air_temperature_c=12.0,
            ),
            Observation(
                source="yandex",
                station="UUWW",
                observed_at=datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
                air_temperature_c=13.5,
            ),
        ]
        result = scheduler._compute_daily_max("2026-04-25")
        assert result.value_c == 14  # 13.5 rounded half-up -> 14
        assert result.source == "point_max"
        assert "yandex" in result.confirmed_by

    def test_empty_observations(self, scheduler):
        result = scheduler._compute_daily_max("2026-04-25")
        assert result.value_c is None
        assert result.source is None

    def test_dedup(self, scheduler):
        obs = Observation(
            source="ogimet_synop",
            station="27611",
            observed_at=datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
            air_temperature_c=10.0,
        )
        scheduler._observations = []
        scheduler._dedup_and_merge([obs, obs, obs])
        assert len(scheduler._observations) == 1

    def test_trim_observations(self, scheduler):
        old = Observation(
            source="ogimet_synop",
            station="27611",
            observed_at=datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc),
            air_temperature_c=10.0,
        )
        recent = Observation(
            source="ogimet_synop",
            station="27611",
            observed_at=datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc),
            air_temperature_c=10.0,
        )
        scheduler._observations = [old, recent]
        scheduler._trim_observations()
        assert len(scheduler._observations) == 1
        assert scheduler._observations[0] is recent
