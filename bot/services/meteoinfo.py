"""Гидрометцентр России (meteoinfo.ru) 5-day forecast client.

Endpoint (unofficial, unstable):
    https://meteoinfo.ru/hmc-output/forecast5d/27611.json

Feature-flagged by default (meteoinfo_enabled=false).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from ..models import ForecastPoint

logger = logging.getLogger(__name__)

METEOINFO_URL = "https://meteoinfo.ru/hmc-output/forecast5d/27611.json"


class MeteoInfoService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        station_id: str = "27611",
        timeout_seconds: int = 15,
        retries: int = 3,
    ) -> None:
        self._session = session
        self._station_id = station_id
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._retries = max(1, retries)

    async def fetch(self) -> list[ForecastPoint]:
        last_err: Exception | None = None
        for attempt in range(1, self._retries + 1):
            try:
                async with self._session.get(
                    METEOINFO_URL,
                    timeout=self._timeout,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "MeteoInfo fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue
            return self._parse(data)

        logger.error("MeteoInfo fetch failed after %d attempts: %s", self._retries, last_err)
        return []

    def _parse(self, data: dict) -> list[ForecastPoint]:
        """Parse meteoinfo JSON. Structure is guessed from docs; adapt as needed."""
        out: list[ForecastPoint] = []
        issued_at = datetime.now(timezone.utc)
        # TODO: implement real parsing once the endpoint shape is known.
        # Expected: list of days with tmax, tmin, date.
        logger.warning("MeteoInfo parser is a TODO — endpoint shape unknown or unstable")
        return out
