"""TAF (Terminal Aerodrome Forecast) client for UUWW.

Endpoint: https://aviationweather.gov/api/data/taf?ids=UUWW&format=json&hours=30
Parsing via metar-taf-parser-mivek.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from ..models import ForecastPoint

logger = logging.getLogger(__name__)

TAF_URL = "https://aviationweather.gov/api/data/taf"


class TAFService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        station: str = "UUWW",
        timeout_seconds: int = 15,
        retries: int = 3,
    ) -> None:
        self._session = session
        self._station = station
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._retries = max(1, retries)

    async def fetch(self) -> list[ForecastPoint]:
        params = {"ids": self._station, "format": "json", "hours": "30"}
        last_err: Exception | None = None
        for attempt in range(1, self._retries + 1):
            try:
                async with self._session.get(
                    TAF_URL,
                    params=params,
                    timeout=self._timeout,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "TAF fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue
            return self._parse(data)

        logger.error("TAF fetch failed after %d attempts: %s", self._retries, last_err)
        return []

    def _parse(self, data: list | dict) -> list[ForecastPoint]:
        out: list[ForecastPoint] = []
        entries: list[dict] = []
        if isinstance(data, dict):
            if "features" in data:
                entries = data["features"]
            else:
                entries = [data]
        elif isinstance(data, list):
            entries = data
        else:
            logger.warning("TAF returned unexpected shape: %r", type(data))
            return out

        issued_at = datetime.now(timezone.utc)
        from metar_taf_parser.parser.parser import TAFParser

        for entry in entries:
            props = entry.get("properties") or entry
            raw = (
                props.get("rawTAF")
                or props.get("rawTaf")
                or props.get("rawOb")
                or props.get("raw")
                or ""
            )
            if not raw:
                continue
            try:
                taf = TAFParser().parse(raw)
            except Exception as exc:
                logger.warning("TAF parse failed: %s (raw=%r)", exc, raw)
                continue

            # TX/TN explicit daily max/min
            if taf.max_temperature:
                tx = taf.max_temperature
                try:
                    valid_at = datetime(
                        datetime.now(timezone.utc).year,
                        datetime.now(timezone.utc).month,
                        tx.day,
                        tx.hour,
                        tzinfo=timezone.utc,
                    )
                    out.append(
                        ForecastPoint(
                            source="taf",
                            model="TAF",
                            station=self._station,
                            issued_at=issued_at,
                            valid_at=valid_at,
                            lead_time_h=int((valid_at - issued_at).total_seconds() // 3600),
                            daily_tmax_c=float(tx.temperature),
                            raw=raw,
                        )
                    )
                except Exception as exc:
                    logger.warning("TAF TX extraction failed: %s", exc)

            if taf.min_temperature:
                tn = taf.min_temperature
                try:
                    valid_at = datetime(
                        datetime.now(timezone.utc).year,
                        datetime.now(timezone.utc).month,
                        tn.day,
                        tn.hour,
                        tzinfo=timezone.utc,
                    )
                    out.append(
                        ForecastPoint(
                            source="taf",
                            model="TAF",
                            station=self._station,
                            issued_at=issued_at,
                            valid_at=valid_at,
                            lead_time_h=int((valid_at - issued_at).total_seconds() // 3600),
                            daily_tmin_c=float(tn.temperature),
                            raw=raw,
                        )
                    )
                except Exception as exc:
                    logger.warning("TAF TN extraction failed: %s", exc)

        logger.info("TAF: %d forecast points from %d entries", len(out), len(data))
        return out
