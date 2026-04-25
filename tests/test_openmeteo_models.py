"""Tests for OpenMeteoModelsService."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from bot.services.openmeteo_models import MODELS, OpenMeteoModelsService


FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def models_response() -> dict:
    return json.loads((FIXTURE_DIR / "openmeteo_models.json").read_text())


class TestOpenMeteoModelsParsing:
    def test_parse_returns_points(self, models_response: dict) -> None:
        """Parsing a valid payload yields the expected ForecastPoints."""
        svc = OpenMeteoModelsService(session=MagicMock(), lat=55.5914, lon=37.2615)
        points = svc._parse(models_response)

        # Only 2 models present in fixture, 2 days each
        assert len(points) == 4
        # All points should have source="open_meteo"
        assert all(p.source == "open_meteo" for p in points)
        # Models should alternate
        models_in_points = [p.model for p in points]
        assert "ecmwf_ifs025" in models_in_points
        assert "gfs_seamless" in models_in_points
        # Daily values present
        assert all(p.daily_tmax_c is not None for p in points)
        assert points[0].valid_at.year == 2026


class TestOpenMeteoModelsFetchMock:
    @pytest.mark.asyncio
    async def test_fetch_with_mock(self, models_response: dict) -> None:
        """Fetching via mocked HTTP returns parsed points."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=models_response)
        session = MagicMock()
        session.get = MagicMock(
            return_value=MagicMock(
                __aenter__=AsyncMock(return_value=mock_resp),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        svc = OpenMeteoModelsService(session=session, lat=55.5914, lon=37.2615)
        points = await svc.fetch()

        assert len(points) == 4
        assert all(p.source == "open_meteo" for p in points)
