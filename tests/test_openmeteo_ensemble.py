"""Tests for OpenMeteoEnsembleService."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from bot.services.openmeteo_ensemble import ENSEMBLE_MODELS, OpenMeteoEnsembleService


FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def ensemble_response() -> dict:
    return json.loads((FIXTURE_DIR / "openmeteo_ensemble.json").read_text())


class TestOpenMeteoEnsembleParsing:
    def test_parse_returns_members_and_quantiles(self, ensemble_response: dict) -> None:
        """Parsing a valid payload yields member points + quantile points."""
        svc = OpenMeteoEnsembleService(session=MagicMock(), lat=55.5914, lon=37.2615)
        points = svc._parse(ensemble_response)

        # Members: 2 members x 8 hours = 16 points
        member_points = [p for p in points if p.member is not None]
        assert len(member_points) == 16

        # Quantiles: 3 quantiles per model per day
        # The fixture has data for 2 days (25th and 26th) for icon_seamless only
        # because the keys are icon_seamless_eps. Wait, let me check.
        # Actually the fixture only provides icon_seamless members, so only 1 model.
        # 2 days x 3 quantiles x 1 model = 6 quantile points.
        quantile_points = [p for p in points if p.quantile is not None]
        assert len(quantile_points) == 6

        # All member points should have air_temperature_c
        assert all(p.air_temperature_c is not None for p in member_points)
        # Quantile points should have daily_tmax_c
        assert all(p.daily_tmax_c is not None for p in quantile_points)


class TestOpenMeteoEnsembleFetchMock:
    @pytest.mark.asyncio
    async def test_fetch_with_mock(self, ensemble_response: dict) -> None:
        """Fetching via mocked HTTP returns parsed points."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=ensemble_response)
        session = MagicMock()
        session.get = MagicMock(
            return_value=MagicMock(
                __aenter__=AsyncMock(return_value=mock_resp),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        svc = OpenMeteoEnsembleService(session=session, lat=55.5914, lon=37.2615)
        points = await svc.fetch()

        assert len(points) > 0
        assert any(p.member is not None for p in points)
        assert any(p.quantile is not None for p in points)
