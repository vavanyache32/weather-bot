"""Open-Meteo client (free, no API key).

Docs: https://open-meteo.com/en/docs

    GET https://api.open-meteo.com/v1/forecast
        ?latitude=<lat>&longitude=<lon>
        &daily=temperature_2m_max,temperature_2m_min
        &timezone=<IANA tz>
        &forecast_days=<N>

We read ``daily.time[i]`` + ``daily.temperature_2m_max[i]`` pairs.  An
optional ``models`` query parameter can pin a specific NWP model
(``ecmwf_ifs04``, ``gfs_seamless``, …); left empty we get Open-Meteo's
"seamless" mix which is typically the best single number you can ask
for without wiring up raw ECMWF.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

import aiohttp

from ..models import DailyExtremes

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


class OpenMeteoService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        lat: float,
        lon: float,
        timezone: str,
        timeout_seconds: int,
        retries: int,
        forecast_days: int = 5,
        model: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        self._session = session
        self._lat = lat
        self._lon = lon
        self._tz = timezone
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._retries = max(1, retries)
        self._forecast_days = max(1, min(forecast_days, 16))
        self._model = model
        self._headers: dict[str, str] = {}
        if user_agent:
            self._headers["User-Agent"] = user_agent

    async def fetch_daily(self) -> Optional[list[DailyExtremes]]:
        params = {
            "latitude": self._lat,
            "longitude": self._lon,
            "daily": "temperature_2m_max,temperature_2m_min",
            "timezone": self._tz,
            "forecast_days": self._forecast_days,
        }
        if self._model:
            params["models"] = self._model

        last_err: Exception | None = None
        for attempt in range(1, self._retries + 1):
            try:
                async with self._session.get(
                    OPEN_METEO_URL,
                    params=params,
                    headers=self._headers,
                    timeout=self._timeout,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "Open-Meteo fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue

            daily = data.get("daily") or {}
            times = daily.get("time") or []
            maxes = daily.get("temperature_2m_max") or []
            mins = daily.get("temperature_2m_min") or []
            if not times or not maxes:
                logger.warning("Open-Meteo returned empty daily payload: %s", data)
                return []

            out: list[DailyExtremes] = []
            for i, t in enumerate(times):
                try:
                    d = date.fromisoformat(t)
                except (TypeError, ValueError):
                    continue
                try:
                    tmax = float(maxes[i])
                except (IndexError, TypeError, ValueError):
                    continue
                try:
                    tmin = float(mins[i]) if i < len(mins) else None
                except (TypeError, ValueError):
                    tmin = None
                out.append(DailyExtremes(date=d, temp_max_c=tmax, temp_min_c=tmin))

            logger.info(
                "Open-Meteo daily forecast: %d days, today_max=%s",
                len(out),
                out[0].temp_max_c if out else None,
            )
            return out

        logger.error(
            "Open-Meteo fetch failed after %d attempts: %s",
            self._retries,
            last_err,
        )
        return None
