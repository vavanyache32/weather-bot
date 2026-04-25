"""Unit tests for OGIMET SYNOP parser and service."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from bot.services.ogimet import OgimetService


class TestOgimetParsing:
    """Tests that exercise synop2bufr via OgimetService._parse_line."""

    def test_fixture_1_with_explicit_max(self):
        svc = OgimetService(session=None)  # type: ignore[arg-type]
        line = (
            "27611,2025,04,24,18,00,"
            "AAXX 24181 27611 12560 63401 10130 20051 39875 40103 52001 60002 82501 333 10194=="
        )
        obs = svc._parse_line(line)
        assert obs is not None
        assert obs.source == "ogimet_synop"
        assert obs.station == "27611"
        assert obs.observed_at == datetime(2025, 4, 24, 18, 0, tzinfo=timezone.utc)
        assert obs.air_temperature_c == pytest.approx(13.0, abs=0.01)
        assert obs.max_temperature_c == pytest.approx(19.4, abs=0.01)
        assert obs.raw == (
            "AAXX 24181 27611 12560 63401 10130 20051 39875 40103 52001 60002 82501 333 10194=="
        )

    def test_fixture_2_with_min_only(self):
        svc = OgimetService(session=None)  # type: ignore[arg-type]
        line = (
            "27611,2025,04,24,06,00,"
            "AAXX 24061 27611 12560 70403 10119 20082 39905 40134 57001 69902 87500 333 20087 31///=="
        )
        obs = svc._parse_line(line)
        assert obs is not None
        assert obs.air_temperature_c == pytest.approx(11.9, abs=0.01)
        assert obs.max_temperature_c is None

    def test_parse_line_empty_returns_none(self):
        svc = OgimetService(session=None)  # type: ignore[arg-type]
        assert svc._parse_line("") is None
        assert svc._parse_line("27611,2025,04,24,18,00,") is None


class TestOgimetFetchMock:
    """Tests with mocked aiohttp session."""

    @pytest.mark.asyncio
    async def test_fetch_observations_mock(self):
        text = (
            "27611,2025,04,24,18,00,AAXX 24181 27611 12560 63401 10130 20051 39875 40103 52001 60002 82501 333 10194==\n"
            "27611,2025,04,24,06,00,AAXX 24061 27611 12560 70403 10119 20082 39905 40134 57001 69902 87500 333 20087 31///==\n"
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = AsyncMock(return_value=text)
        session = MagicMock()
        session.get = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_resp), __aexit__=AsyncMock(return_value=False)))

        svc = OgimetService(session=session, station_id="27611", rate_limit_seconds=0)
        obs_list = await svc.fetch_observations()

        assert len(obs_list) == 2
        assert obs_list[0].max_temperature_c == pytest.approx(19.4, abs=0.01)
        assert obs_list[1].air_temperature_c == pytest.approx(11.9, abs=0.01)

    @pytest.mark.asyncio
    async def test_fetch_observations_dedup(self):
        text = (
            "27611,2025,04,24,18,00,AAXX 24181 27611 12560 63401 10130 20051 39875 40103 52001 60002 82501 333 10194==\n"
            "27611,2025,04,24,18,00,AAXX 24181 27611 12560 63401 10130 20051 39875 40103 52001 60002 82501 333 10194==\n"
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = AsyncMock(return_value=text)
        session = MagicMock()
        session.get = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_resp), __aexit__=AsyncMock(return_value=False)))

        svc = OgimetService(session=session, station_id="27611", rate_limit_seconds=0)
        obs_list = await svc.fetch_observations()

        assert len(obs_list) == 2  # ogimet itself does not dedup
