"""Tests for TAFService."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from bot.services.taf import TAFService


FIXTURE_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def taf_response() -> dict:
    return json.loads((FIXTURE_DIR / "taf.json").read_text())


class TestTAFParsing:
    def test_parse_returns_points(self, taf_response: dict) -> None:
        """Parsing a valid TAF JSON yields ForecastPoints for TX/TN."""
        svc = TAFService(session=MagicMock(), station="UUWW")
        points = svc._parse(taf_response)

        # Fixture has TX17 and TN08
        assert len(points) == 2
        tmax_point = next(p for p in points if p.daily_tmax_c is not None)
        tmin_point = next(p for p in points if p.daily_tmin_c is not None)
        assert tmax_point.daily_tmax_c == 17.0
        assert tmin_point.daily_tmin_c == 8.0
        assert tmax_point.source == "taf"


class TestTAFFetchMock:
    @pytest.mark.asyncio
    async def test_fetch_with_mock(self, taf_response: dict) -> None:
        """Fetching via mocked HTTP returns parsed points."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=taf_response)
        session = MagicMock()
        session.get = MagicMock(
            return_value=MagicMock(
                __aenter__=AsyncMock(return_value=mock_resp),
                __aexit__=AsyncMock(return_value=False),
            )
        )

        svc = TAFService(session=session, station="UUWW")
        points = await svc.fetch()

        assert len(points) == 2
