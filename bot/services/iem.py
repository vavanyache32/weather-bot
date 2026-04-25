"""Iowa Environmental Mesonet (IEM) ASOS client for UUWW.

Endpoints::

    Current (JSON):
        https://mesonet.agron.iastate.edu/json/current.py?station=UUWW&network=RU__ASOS

    Historical (CSV):
        https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py

IEM aggregates METAR + SPECI and exposes ``last_ob.max_dayairtemp[F]``,
which is an explicit daily maximum (not a point-in-time observation).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from ..models import Observation

logger = logging.getLogger(__name__)

IEM_CURRENT_URL = "https://mesonet.agron.iastate.edu/json/current.py"


def _fahrenheit_to_celsius(f: float) -> float:
    return (f - 32) * 5 / 9


class IEMASOSService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        station: str = "UUWW",
        network: str = "RU__ASOS",
        timeout_seconds: int = 15,
        retries: int = 3,
    ) -> None:
        self._session = session
        self._station = station
        self._network = network
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._retries = max(1, retries)

    async def fetch_current(self) -> Optional[Observation]:
        """Return the latest observation from IEM current.py, or None."""
        params = {"station": self._station, "network": self._network}
        last_err: Exception | None = None

        for attempt in range(1, self._retries + 1):
            try:
                async with self._session.get(
                    IEM_CURRENT_URL,
                    params=params,
                    timeout=self._timeout,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "IEM fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue

            return self._parse(data)

        logger.error(
            "IEM fetch failed after %d attempts: %s",
            self._retries,
            last_err,
        )
        return None

    def _parse(self, data: dict) -> Optional[Observation]:
        if not isinstance(data, dict):
            logger.warning("IEM returned non-dict: %r", data)
            return None

        last_ob = data.get("last_ob")
        if not isinstance(last_ob, dict):
            logger.debug("IEM response has no last_ob")
            return None

        # Temperatures
        air_f = last_ob.get("airtemp[F]")
        max_day_f = last_ob.get("max_dayairtemp[F]")
        min_day_f = last_ob.get("min_dayairtemp[F]")

        def f_to_c(val) -> Optional[float]:
            if val is None:
                return None
            try:
                return _fahrenheit_to_celsius(float(val))
            except (TypeError, ValueError):
                return None

        air_c = f_to_c(air_f)
        max_c = f_to_c(max_day_f)
        min_c = f_to_c(min_day_f)

        # Timestamp
        utc_valid = last_ob.get("utc_valid")
        observed_at: Optional[datetime] = None
        if utc_valid:
            try:
                # IEM sends ISO-like strings ending with Z, e.g. 2026-04-25T14:00:00Z
                observed_at = datetime.fromisoformat(utc_valid.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                pass
        if observed_at is None:
            observed_at = datetime.now(timezone.utc)

        raw = last_ob.get("raw", "")
        if isinstance(raw, str):
            raw = raw.strip()

        return Observation(
            source="iem_asos",
            station=self._station,
            observed_at=observed_at,
            air_temperature_c=air_c,
            max_temperature_c=max_c,
            raw=raw,
        )
