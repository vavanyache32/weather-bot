"""Yandex.Weather API client (secondary source + hourly forecast).

Docs: https://yandex.ru/dev/weather/doc/dg/concepts/about.html

    GET https://api.weather.yandex.ru/v2/forecast?lat=<lat>&lon=<lon>
    Header: X-Yandex-API-Key: <key>

We read two things from a single response:

* ``fact.temp``               — current temperature in °C
* ``forecasts[].hours[].temp`` — hourly forecast in local time (°C)

The hourly forecast is used to estimate the temperature ~30 minutes from
now via linear interpolation between the two bracketing hour points.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional
from zoneinfo import ZoneInfo

import aiohttp

# Forward-declared for typing without circular imports.
from .openmeteo import DailyExtremes  # noqa: E402  (re-used as a simple "day + max" record)

logger = logging.getLogger(__name__)

YANDEX_URL = "https://api.weather.yandex.ru/v2/forecast"


@dataclass
class YandexReading:
    """One successful fetch from Yandex.

    * ``hourly``   — sorted list of (aware datetime, temp °C) for the next
      ~24h. Used for the +30min interpolation.
    * ``daily``    — list of ``DailyExtremes`` (date, max, min) parsed from
      ``forecasts[].parts.day``. Used as the Yandex arm of the ensemble
      daily-max forecast.
    """

    current_c: Optional[float] = None
    hourly: list[tuple[datetime, float]] = field(default_factory=list)
    daily: list[DailyExtremes] = field(default_factory=list)


class YandexService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: Optional[str],
        lat: float,
        lon: float,
        timeout_seconds: int,
        retries: int,
        timezone: str = "Europe/Moscow",
    ) -> None:
        self._session = session
        self._api_key = api_key
        self._lat = lat
        self._lon = lon
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._retries = max(1, retries)
        self._tz = ZoneInfo(timezone)

    # ---------- public ----------

    async def fetch(self) -> Optional[YandexReading]:
        """Single HTTP call → current temp + hourly forecast."""
        if not self._api_key:
            logger.warning("YANDEX_API_KEY is not configured; skipping Yandex fetch")
            return None

        params = {"lat": self._lat, "lon": self._lon}
        headers = {"X-Yandex-API-Key": self._api_key}
        last_err: Exception | None = None

        for attempt in range(1, self._retries + 1):
            try:
                async with self._session.get(
                    YANDEX_URL,
                    params=params,
                    headers=headers,
                    timeout=self._timeout,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "Yandex fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue

            return self._parse(data)

        logger.error(
            "Yandex fetch failed after %d attempts: %s", self._retries, last_err
        )
        return None

    async def get_temperature_c(self) -> Optional[float]:
        """Thin wrapper kept for call sites that only want current temp."""
        reading = await self.fetch()
        return reading.current_c if reading else None

    # ---------- parsing ----------

    def _parse(self, data: dict) -> YandexReading:
        reading = YandexReading()

        fact = data.get("fact") or {}
        temp = fact.get("temp")
        if temp is not None:
            try:
                reading.current_c = float(temp)
            except (TypeError, ValueError):
                logger.warning("Yandex returned non-numeric fact.temp %r", temp)

        reading.hourly = self._parse_hourly(data)
        reading.daily = self._parse_daily(data)

        logger.info(
            "Yandex observation: %s°C; hourly points: %d; daily points: %d",
            reading.current_c,
            len(reading.hourly),
            len(reading.daily),
        )
        return reading

    def _parse_hourly(self, data: dict) -> list[tuple[datetime, float]]:
        out: list[tuple[datetime, float]] = []
        for day in data.get("forecasts") or []:
            date_s = day.get("date")
            if not date_s:
                continue
            try:
                d = date.fromisoformat(date_s)
            except ValueError:
                continue
            for h in day.get("hours") or []:
                hour_s = h.get("hour")
                temp = h.get("temp")
                if hour_s is None or temp is None:
                    continue
                try:
                    hour_i = int(hour_s)
                    temp_f = float(temp)
                except (TypeError, ValueError):
                    continue
                dt = datetime(d.year, d.month, d.day, hour_i, 0, tzinfo=self._tz)
                out.append((dt, temp_f))
        out.sort(key=lambda x: x[0])
        return out

    def _parse_daily(self, data: dict) -> list[DailyExtremes]:
        """Extract daily max/min from ``forecasts[].parts.day``.

        Yandex structures the day as ``parts.day`` (14:00–00:00 Moscow) with
        ``temp_max`` / ``temp_min``; when missing, ``parts.day_short.temp``
        is sometimes the only available signal.  We prefer ``day.temp_max``,
        fall back to ``day_short.temp`` only as a point estimate.
        """
        out: list[DailyExtremes] = []
        for day in data.get("forecasts") or []:
            date_s = day.get("date")
            if not date_s:
                continue
            try:
                d = date.fromisoformat(date_s)
            except ValueError:
                continue
            parts = day.get("parts") or {}
            day_part = parts.get("day") or {}
            short_part = parts.get("day_short") or {}

            tmax = day_part.get("temp_max")
            tmin = day_part.get("temp_min")
            if tmax is None:
                tmax = short_part.get("temp")
            if tmax is None:
                continue
            try:
                tmax_f = float(tmax)
                tmin_f = float(tmin) if tmin is not None else None
            except (TypeError, ValueError):
                continue
            out.append(DailyExtremes(date=d, temp_max_c=tmax_f, temp_min_c=tmin_f))
        return out


def predict_temperature_c(
    hourly: list[tuple[datetime, float]], target: datetime
) -> Optional[float]:
    """Linearly interpolate an hourly forecast at ``target``.

    * Empty input → ``None``.
    * Target before/after the available range → the nearest endpoint
      (clamping), rather than guessing.
    """
    if not hourly:
        return None
    if target <= hourly[0][0]:
        return hourly[0][1]
    if target >= hourly[-1][0]:
        return hourly[-1][1]
    for (t1, v1), (t2, v2) in zip(hourly, hourly[1:]):
        if t1 <= target <= t2:
            span = (t2 - t1).total_seconds()
            if span <= 0:
                return v1
            frac = (target - t1).total_seconds() / span
            return v1 + (v2 - v1) * frac
    return None
