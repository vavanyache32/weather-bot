"""NOAA METAR client for station UUWW (Moscow / Vnukovo).

``api.weather.gov/stations/UUWW`` does **not** cover international
stations (NWS station catalogue is US-centric). The correct NOAA/NWS
endpoint for METAR-based temperatures at UUWW is the Aviation Weather
Center JSON API:

    https://aviationweather.gov/api/data/metar?ids=UUWW&format=json&taf=false&hours=1

Each entry contains ``temp`` — the decoded METAR temperature in whole
degrees Celsius (i.e. already rounded, matching Polymarket resolution
semantics) and ``obsTime`` as a Unix timestamp.  We pick the entry with
the latest ``obsTime`` to defend against occasional out-of-order
responses.

The service class keeps the name ``NOAAService`` because the data is
still an official NOAA/NWS product; only the endpoint changed.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

AWC_METAR_URL = "https://aviationweather.gov/api/data/metar"


class NOAAService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        station: str,
        user_agent: str,
        timeout_seconds: int,
        retries: int,
    ) -> None:
        self._session = session
        self._station = station
        self._headers = {
            "User-Agent": user_agent,
            "Accept": "application/json",
        }
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._retries = max(1, retries)

    async def get_temperature_c(self) -> Optional[float]:
        """Return the latest METAR temperature in Celsius, or None on failure."""
        params = {
            "ids": self._station,
            "format": "json",
            "taf": "false",
            "hours": "1",
        }
        last_err: Exception | None = None

        for attempt in range(1, self._retries + 1):
            try:
                async with self._session.get(
                    AWC_METAR_URL,
                    params=params,
                    headers=self._headers,
                    timeout=self._timeout,
                ) as resp:
                    resp.raise_for_status()
                    # AWC sometimes serves application/json, sometimes text/json.
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "NOAA METAR fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue

            if not isinstance(data, list) or not data:
                logger.warning(
                    "NOAA METAR returned no observations for %s", self._station
                )
                return None

            latest = max(
                (r for r in data if isinstance(r, dict)),
                key=lambda r: r.get("obsTime") or 0,
                default=None,
            )
            if latest is None:
                logger.warning("NOAA METAR returned unexpected shape: %r", data)
                return None

            temp = latest.get("temp")
            if temp is None:
                logger.warning(
                    "NOAA METAR has no temp field for %s (raw=%s)",
                    self._station,
                    latest.get("rawOb"),
                )
                return None

            try:
                celsius = float(temp)
            except (TypeError, ValueError):
                logger.warning("NOAA METAR non-numeric temp %r", temp)
                return None

            logger.info(
                "NOAA METAR: station=%s temp=%s°C obsTime=%s raw=%s",
                self._station,
                temp,
                latest.get("reportTime") or latest.get("obsTime"),
                latest.get("rawOb"),
            )
            return celsius

        logger.error(
            "NOAA METAR fetch failed after %d attempts: %s",
            self._retries,
            last_err,
        )
        return None
