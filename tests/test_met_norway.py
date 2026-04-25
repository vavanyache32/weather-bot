"""Tests for MetNorwayService."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from bot.services.met_norway import MetNorwayService


FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def met_norway_response() -> dict:
    return json.loads((FIXTURE_DIR / "met_norway.json").read_text())


class TestMetNorwayParsing:
    def test_parse_returns_points(self, met_norway_response: dict) -> None:
        """Parsing a valid payload yields hourly + daily points."""
        svc = MetNorwayService(
            session=MagicMock(), lat=55.5914, lon=37.2615, user_agent="test-bot"
        )
        points = svc._parse(met_norway_response)

        # Hourly points: 2 in fixture
        hourly = [p for p in points if p.air_temperature_c is not None]
        assert len(hourly) == 2
        assert hourly[0].air_temperature_c == 14.5

        # Daily points: 1 day in fixture (2026-04-25)
        daily = [p for p in points if p.daily_tmax_c is not None]
        assert len(daily) == 1
        assert daily[0].daily_tmax_c == 14.5
        assert daily[0].daily_tmin_c == 12.0


class TestMetNorwayFetchMock:
    @pytest.mark.asyncio
    async def test_fetch_with_mock(self, met_norway_response: dict) -> None:
        """Fetching via mocked HTTP returns parsed points."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=met_norway_response)
        mock_resp.headers = {"Expires": "Sat, 25 Apr 2026 14:00:00 GMT"}
        session = MagicMock()
        session.get = MagicMock(
            return_value=MagicMock(
                __aenter__=AsyncMock(return_value=mock_resp),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        svc = MetNorwayService(
            session=session, lat=55.5914, lon=37.2615, user_agent="test-bot"
        )
        points = await svc.fetch()

        assert len(points) > 0
        assert any(p.source == "met_norway" for p in points)
