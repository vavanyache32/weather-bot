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
MESONET_URL = "https://mesonet.agron.iastate.edu/json/current.py"


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

    async def _fetch_mesonet_full(self) -> Optional[tuple[float, str]]:
        """Fast path: Iowa State Mesonet serves UUWW updates quicker than AWC.

        Returns (temp_c, utc_valid_iso) or None on failure.
        """
        params = {"station": self._station, "network": "RU__ASOS"}
        try:
            async with self._session.get(
                MESONET_URL,
                params=params,
                headers=self._headers,
                timeout=self._timeout,
            ) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except Exception as exc:
            logger.debug("Mesonet fetch failed: %s", exc)
            return None

        if not isinstance(data, dict):
            return None
        last_ob = data.get("last_ob")
        if not isinstance(last_ob, dict):
            return None

        temp_f = last_ob.get("airtemp[F]")
        if temp_f is None:
            return None
        try:
            temp_f = float(temp_f)
        except (TypeError, ValueError):
            return None

        temp_c = (temp_f - 32) * 5 / 9
        utc_valid = last_ob.get("utc_valid")
        logger.info(
            "Mesonet: station=%s temp=%.1f°F -> %.2f°C utc_valid=%s raw=%s",
            self._station,
            temp_f,
            temp_c,
            utc_valid,
            last_ob.get("raw", "n/a"),
        )
        return temp_c, utc_valid

    async def _fetch_mesonet(self) -> Optional[float]:
        """Convenience wrapper that returns only the temperature."""
        result = await self._fetch_mesonet_full()
        return result[0] if result is not None else None

    async def _fetch_awc_full(self) -> Optional[tuple[float, str]]:
        """Fetch from Aviation Weather Center JSON API.

        Returns (temp_c, obs_time_iso) or None on failure.
        obs_time_iso is normalised to ISO-8601 string.
        """
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
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "AWC fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue

            if not isinstance(data, list) or not data:
                logger.warning("AWC returned no observations for %s", self._station)
                return None

            latest = max(
                (r for r in data if isinstance(r, dict)),
                key=lambda r: r.get("obsTime") or 0,
                default=None,
            )
            if latest is None:
                logger.warning("AWC returned unexpected shape: %r", data)
                return None

            temp = latest.get("temp")
            if temp is None:
                logger.warning(
                    "AWC has no temp field for %s (raw=%s)",
                    self._station,
                    latest.get("rawOb"),
                )
                return None

            try:
                celsius = float(temp)
            except (TypeError, ValueError):
                logger.warning("AWC non-numeric temp %r", temp)
                return None

            raw_obs = latest.get("reportTime") or latest.get("obsTime")
            # Normalise Unix timestamp to ISO string if needed.
            if isinstance(raw_obs, int):
                from datetime import datetime, timezone
                obs_time = datetime.fromtimestamp(raw_obs, tz=timezone.utc).isoformat()
            else:
                obs_time = str(raw_obs) if raw_obs else ""

            logger.info(
                "AWC: station=%s temp=%s°C obsTime=%s raw=%s",
                self._station,
                temp,
                obs_time,
                latest.get("rawOb"),
            )
            return celsius, obs_time

        logger.error("AWC fetch failed after %d attempts: %s", self._retries, last_err)
        return None

    async def get_latest(self) -> tuple[Optional[float], Optional[str]]:
        """Return (temperature_c, obs_time_iso) from the latest METAR.

        Polls Mesonet and AWC in parallel and picks the result with the
        later observation time.  This usually shaves a few minutes off the
        latency because the two services update on slightly different cadences.
        """
        import asyncio

        mesonet_task = asyncio.create_task(self._fetch_mesonet_full())
        awc_task = asyncio.create_task(self._fetch_awc_full())
        results = await asyncio.gather(mesonet_task, awc_task, return_exceptions=True)

        candidates: list[tuple[float, str]] = []
        for idx, res in enumerate(results):
            if isinstance(res, Exception):
                logger.warning("Source %d failed: %s", idx, res)
                continue
            if res is not None:
                candidates.append(res)

        if not candidates:
            return None, None

        # Pick the one with the later obs_time string (ISO-8601 sorts correctly).
        best = max(candidates, key=lambda x: x[1])
        return best

    async def get_temperature_c(self) -> Optional[float]:
        """Return the latest METAR temperature in Celsius, or None on failure."""
        temp, _ = await self.get_latest()
        return temp
