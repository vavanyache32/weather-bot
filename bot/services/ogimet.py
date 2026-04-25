"""OGIMET SYNOP client for station 27611 (Vnukovo / UUWW).

Endpoint::

    GET https://www.ogimet.com/cgi-bin/getsynop
        ?block=27611&begin=YYYYMMDDHHmm&end=YYYYMMDDHHmm

Rate-limit: **≤ 1 request per 10 s** on this endpoint.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp

from ..models import Observation

logger = logging.getLogger(__name__)

OGIMET_URL = "https://www.ogimet.com/cgi-bin/getsynop"

# Respect OGIMET rate-limit globally within the process.
_OGIMET_LOCK = asyncio.Lock()


def _fahrenheit_to_celsius(f: float) -> float:
    return (f - 32) * 5 / 9


class OgimetService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        station_id: str = "27611",
        timeout_seconds: int = 15,
        retries: int = 3,
        interval_seconds: int = 600,
        rate_limit_seconds: float = 10.0,
    ) -> None:
        self._session = session
        self._station_id = station_id
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._retries = max(1, retries)
        self._interval = interval_seconds
        self._rate_limit = rate_limit_seconds

    async def fetch_observations(self) -> list[Observation]:
        """Fetch SYNOP messages for the last 12 h and parse into Observations."""
        now_utc = datetime.now(timezone.utc)
        begin = now_utc - timedelta(hours=12)
        params = {
            "block": self._station_id,
            "begin": begin.strftime("%Y%m%d%H%M"),
            "end": now_utc.strftime("%Y%m%d%H%M"),
        }

        last_err: Exception | None = None
        for attempt in range(1, self._retries + 1):
            try:
                # Rate-limit: hard floor of 10 s between OGIMET requests
                async with _OGIMET_LOCK:
                    await asyncio.sleep(self._rate_limit)
                    async with self._session.get(
                        OGIMET_URL,
                        params=params,
                        timeout=self._timeout,
                    ) as resp:
                        resp.raise_for_status()
                        text = await resp.text()
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "OGIMET fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue

            return self._parse(text)

        logger.error(
            "OGIMET fetch failed after %d attempts: %s",
            self._retries,
            last_err,
        )
        return []

    def _parse(self, text: str) -> list[Observation]:
        out: list[Observation] = []
        for line in text.strip().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            obs = self._parse_line(line)
            if obs is not None:
                out.append(obs)
        logger.info("OGIMET parsed %d observations", len(out))
        return out

    def _parse_line(self, line: str) -> Optional[Observation]:
        # Format: 27611,2025,04,24,18,00,AAXX ... ==
        parts = line.split(",")
        if len(parts) < 7:
            logger.debug("OGIMET line too short: %r", line)
            return None

        try:
            year = int(parts[1])
            month = int(parts[2])
            day = int(parts[3])
            hour = int(parts[4])
            minute = int(parts[5])
            synop_raw = ",".join(parts[6:])  # SYNOP may contain commas? unlikely
        except (ValueError, IndexError) as exc:
            logger.warning("OGIMET line format error: %s — %r", exc, line)
            return None

        # Clean trailing == and whitespace
        synop_clean = synop_raw.replace("==", "").strip()
        if not synop_clean:
            return None

        try:
            import synop2bufr

            parsed = synop2bufr.parse_synop(synop_clean, year, month)
            data = parsed[0] if isinstance(parsed, tuple) else parsed
        except Exception as exc:
            logger.warning(
                "SYNOP parse failed for %s: %s (raw=%r)",
                self._station_id,
                exc,
                synop_clean,
            )
            return None

        if not isinstance(data, dict):
            return None

        air_k = data.get("air_temperature")
        max_k = data.get("maximum_temperature")
        min_k = data.get("minimum_temperature")

        def k_to_c(val) -> Optional[float]:
            if val is None:
                return None
            try:
                return float(val) - 273.15
            except (TypeError, ValueError):
                return None

        air_c = k_to_c(air_k)
        max_c = k_to_c(max_k)
        min_c = k_to_c(min_k)

        if air_c is None and max_c is None:
            return None

        observed_at = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)

        return Observation(
            source="ogimet_synop",
            station=self._station_id,
            observed_at=observed_at,
            air_temperature_c=air_c,
            max_temperature_c=max_c,
            raw=synop_raw,
        )
