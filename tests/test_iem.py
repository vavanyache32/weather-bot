"""Unit tests for IEM ASOS client."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from bot.services.iem import IEMASOSService


class TestIEMCurrentMock:
    @pytest.mark.asyncio
    async def test_fetch_current_explicit_max(self):
        payload = {
            "generated_at": "2026-04-25T14:08:41Z",
            "last_ob": {
                "utc_valid": "2026-04-25T14:00:00Z",
                "airtemp[F]": 50.0,
                "max_dayairtemp[F]": 59.0,
                "min_dayairtemp[F]": 32.0,
                "raw": "UUWW 251400Z 25003MPS CAVOK 10/M05 Q1001 R24/000062 NOSIG",
            },
        }

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=payload)
        session = MagicMock()
        session.get = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_resp), __aexit__=AsyncMock(return_value=False)))

        svc = IEMASOSService(session=session, station="UUWW", network="RU__ASOS")
        obs = await svc.fetch_current()

        assert obs is not None
        assert obs.source == "iem_asos"
        assert obs.station == "UUWW"
        assert obs.observed_at == datetime(2026, 4, 25, 14, 0, tzinfo=timezone.utc)
        # 50 F -> 10 C
        assert obs.air_temperature_c == pytest.approx(10.0, abs=0.01)
        # 59 F -> 15 C
        assert obs.max_temperature_c == pytest.approx(15.0, abs=0.01)
        assert obs.raw == "UUWW 251400Z 25003MPS CAVOK 10/M05 Q1001 R24/000062 NOSIG"

    @pytest.mark.asyncio
    async def test_fetch_current_no_max(self):
        payload = {
            "last_ob": {
                "utc_valid": "2026-04-25T14:00:00Z",
                "airtemp[F]": 32.0,
                "max_dayairtemp[F]": None,
                "raw": "RAW",
            },
        }

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=payload)
        session = MagicMock()
        session.get = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=mock_resp), __aexit__=AsyncMock(return_value=False)))

        svc = IEMASOSService(session=session)
        obs = await svc.fetch_current()

        assert obs is not None
        assert obs.air_temperature_c == pytest.approx(0.0, abs=0.01)
        assert obs.max_temperature_c is None
