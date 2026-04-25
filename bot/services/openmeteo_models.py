"""Open-Meteo multi-model forecast client.

Fetches daily Tmax/Tmin from individual NWP models:
  ecmwf_ifs025, gfs_seamless, icon_seamless, gem_seamless,
  meteofrance_seamless, jma_seamless, ukmo_seamless

Endpoint: https://api.open-meteo.com/v1/forecast
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from ..models import ForecastPoint

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
MODELS = [
    "ecmwf_ifs025",
    "gfs_seamless",
    "icon_seamless",
    "gem_seamless",
    "meteofrance_seamless",
    "jma_seamless",
    "ukmo_seamless",
]


class OpenMeteoModelsService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        lat: float = 55.5914,
        lon: float = 37.2615,
        forecast_days: int = 14,
        timeout_seconds: int = 15,
        retries: int = 3,
    ) -> None:
        self._session = session
        self._lat = lat
        self._lon = lon
        self._forecast_days = max(1, min(forecast_days, 16))
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._retries = max(1, retries)

    async def fetch(self) -> list[ForecastPoint]:
        """Fetch daily forecasts from all models and return as ForecastPoints."""
        params = {
            "latitude": self._lat,
            "longitude": self._lon,
            "daily": "temperature_2m_max,temperature_2m_min",
            "models": ",".join(MODELS),
            "timezone": "UTC",
            "forecast_days": self._forecast_days,
        }
        last_err: Exception | None = None
        for attempt in range(1, self._retries + 1):
            try:
                async with self._session.get(
                    OPEN_METEO_URL,
                    params=params,
                    timeout=self._timeout,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "Open-Meteo models fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue
            return self._parse(data)

        logger.error(
            "Open-Meteo models fetch failed after %d attempts: %s",
            self._retries,
            last_err,
        )
        return []

    def _parse(self, data: dict) -> list[ForecastPoint]:
        out: list[ForecastPoint] = []
        daily = data.get("daily") or {}
        times = daily.get("time") or []
        if not times:
            logger.warning("Open-Meteo models returned empty daily payload")
            return out

        issued_at = datetime.now(timezone.utc)
        for model in MODELS:
            max_key = f"temperature_2m_max_{model}"
            min_key = f"temperature_2m_min_{model}"
            maxes = daily.get(max_key) or []
            mins = daily.get(min_key) or []
            if not maxes:
                continue
            for i, date_s in enumerate(times):
                try:
                    valid_at = datetime.strptime(date_s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except (ValueError, TypeError):
                    continue
                try:
                    tmax = float(maxes[i]) if i < len(maxes) else None
                except (TypeError, ValueError):
                    tmax = None
                try:
                    tmin = float(mins[i]) if i < len(mins) else None
                except (TypeError, ValueError):
                    tmin = None
                if tmax is None and tmin is None:
                    continue
                lead = int((valid_at - issued_at).total_seconds() // 3600)
                out.append(
                    ForecastPoint(
                        source="open_meteo",
                        model=model,
                        station="UUWW",
                        lat=self._lat,
                        lon=self._lon,
                        issued_at=issued_at,
                        valid_at=valid_at,
                        lead_time_h=lead,
                        daily_tmax_c=tmax,
                        daily_tmin_c=tmin,
                    )
                )
        logger.info("Open-Meteo models: %d forecast points", len(out))
        return out
