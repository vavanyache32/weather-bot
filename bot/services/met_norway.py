"""MET Norway Locationforecast client.

Endpoint: https://api.met.no/weatherapi/locationforecast/2.0/complete
Requires descriptive User-Agent header.
Respects Expires / If-Modified-Since caching semantics.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from ..models import ForecastPoint

logger = logging.getLogger(__name__)

MET_NORWAY_URL = "https://api.met.no/weatherapi/locationforecast/2.0/complete"


class MetNorwayService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        lat: float = 55.5914,
        lon: float = 37.2615,
        user_agent: str = "weatherapp-telegram-bot",
        timeout_seconds: int = 15,
        retries: int = 3,
    ) -> None:
        self._session = session
        self._lat = lat
        self._lon = lon
        self._headers = {
            "User-Agent": user_agent,
            "Accept": "application/json",
        }
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._retries = max(1, retries)
        self._expires_at: Optional[datetime] = None

    async def fetch(self) -> list[ForecastPoint]:
        """Fetch forecast, respecting Expires header."""
        if self._expires_at and datetime.now(timezone.utc) < self._expires_at:
            logger.debug("MET Norway cache valid until %s; skipping fetch", self._expires_at)
            return []

        params = {"lat": self._lat, "lon": self._lon}
        last_err: Exception | None = None
        for attempt in range(1, self._retries + 1):
            try:
                async with self._session.get(
                    MET_NORWAY_URL,
                    params=params,
                    headers=self._headers,
                    timeout=self._timeout,
                ) as resp:
                    resp.raise_for_status()
                    # Respect Expires
                    expires = resp.headers.get("Expires")
                    if expires:
                        try:
                            self._expires_at = datetime.strptime(
                                expires, "%a, %d %b %Y %H:%M:%S %Z"
                            ).replace(tzinfo=timezone.utc)
                        except ValueError:
                            pass
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "MET Norway fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue
            return self._parse(data)

        logger.error(
            "MET Norway fetch failed after %d attempts: %s",
            self._retries,
            last_err,
        )
        return []

    def _parse(self, data: dict) -> list[ForecastPoint]:
        out: list[ForecastPoint] = []
        properties = data.get("properties") or {}
        timeseries = properties.get("timeseries") or []
        if not timeseries:
            logger.warning("MET Norway returned empty timeseries")
            return out

        issued_at = datetime.now(timezone.utc)
        for entry in timeseries:
            time_str = entry.get("time")
            details = (entry.get("data") or {}).get("instant", {}).get("details", {})
            temp = details.get("air_temperature")
            if time_str is None or temp is None:
                continue
            try:
                valid_at = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            lead = int((valid_at - issued_at).total_seconds() // 3600)
            out.append(
                ForecastPoint(
                    source="met_norway",
                    model="met_norway_mix",
                    station="UUWW",
                    lat=self._lat,
                    lon=self._lon,
                    issued_at=issued_at,
                    valid_at=valid_at,
                    lead_time_h=lead,
                    air_temperature_c=float(temp),
                )
            )

        # Aggregate daily max from hourly points
        daily_max = self._aggregate_daily(out, issued_at)
        out.extend(daily_max)

        logger.info("MET Norway: %d hourly + %d daily points", len(timeseries), len(daily_max))
        return out

    def _aggregate_daily(
        self, points: list[ForecastPoint], issued_at: datetime
    ) -> list[ForecastPoint]:
        from collections import defaultdict
        from zoneinfo import ZoneInfo

        daily: dict[str, list[float]] = defaultdict(list)
        tz = ZoneInfo("Europe/Moscow")
        for p in points:
            local = p.valid_at.astimezone(tz)
            date_str = local.date().isoformat()
            daily[date_str].append(p.air_temperature_c)

        out: list[ForecastPoint] = []
        for date_str, temps in sorted(daily.items()):
            if not temps:
                continue
            valid_at = datetime.strptime(date_str, "%Y-%m-%d").replace(
                hour=12, tzinfo=timezone.utc
            )
            lead = int((valid_at - issued_at).total_seconds() // 3600)
            out.append(
                ForecastPoint(
                    source="met_norway",
                    model="met_norway_mix",
                    station="UUWW",
                    lat=self._lat,
                    lon=self._lon,
                    issued_at=issued_at,
                    valid_at=valid_at,
                    lead_time_h=lead,
                    daily_tmax_c=max(temps),
                    daily_tmin_c=min(temps),
                )
            )
        return out
