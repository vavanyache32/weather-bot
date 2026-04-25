"""Open-Meteo Ensemble forecast client.

Endpoint: https://ensemble-api.open-meteo.com/v1/ensemble
Fetches hourly temperature from ECMWF, GFS and ICON ensemble members.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timezone
from typing import Optional

import aiohttp

from ..models import ForecastPoint

logger = logging.getLogger(__name__)

ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
ENSEMBLE_MODELS = ["icon_seamless", "gfs_seamless", "ecmwf_ifs025"]


class OpenMeteoEnsembleService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        lat: float = 55.5914,
        lon: float = 37.2615,
        forecast_days: int = 10,
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
        """Fetch ensemble members and return as ForecastPoints.

        Also computes daily quantiles (0.1, 0.5, 0.9) per model and appends
        them as separate ForecastPoint rows with quantile set.
        """
        params = {
            "latitude": self._lat,
            "longitude": self._lon,
            "hourly": "temperature_2m",
            "models": ",".join(ENSEMBLE_MODELS),
            "timezone": "UTC",
            "forecast_days": self._forecast_days,
        }
        last_err: Exception | None = None
        for attempt in range(1, self._retries + 1):
            try:
                async with self._session.get(
                    ENSEMBLE_URL,
                    params=params,
                    timeout=self._timeout,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
            except Exception as exc:
                last_err = exc
                logger.warning(
                    "Open-Meteo ensemble fetch attempt %d/%d failed: %s",
                    attempt,
                    self._retries,
                    exc,
                )
                if attempt < self._retries:
                    await asyncio.sleep(min(2 ** attempt, 10))
                continue
            return self._parse(data)

        logger.error(
            "Open-Meteo ensemble fetch failed after %d attempts: %s",
            self._retries,
            last_err,
        )
        return []

    def _parse(self, data: dict) -> list[ForecastPoint]:
        out: list[ForecastPoint] = []
        hourly = data.get("hourly") or {}
        times = hourly.get("time") or []
        if not times:
            logger.warning("Open-Meteo ensemble returned empty hourly payload")
            return out

        issued_at = datetime.now(timezone.utc)

        # Parse raw member points
        for model in ENSEMBLE_MODELS:
            member_idx = 1
            while True:
                key = f"temperature_2m_member{member_idx:02d}_{model}_eps"
                temps = hourly.get(key)
                if temps is None:
                    break
                for i, ts in enumerate(times):
                    if i >= len(temps):
                        break
                    try:
                        temp = float(temps[i])
                    except (TypeError, ValueError):
                        continue
                    try:
                        valid_at = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                        if valid_at.tzinfo is None:
                            valid_at = valid_at.replace(tzinfo=timezone.utc)
                    except (ValueError, TypeError):
                        continue
                    lead = int((valid_at - issued_at).total_seconds() // 3600)
                    out.append(
                        ForecastPoint(
                            source="open_meteo",
                            model=model,
                            member=member_idx,
                            station="UUWW",
                            lat=self._lat,
                            lon=self._lon,
                            issued_at=issued_at,
                            valid_at=valid_at,
                            lead_time_h=lead,
                            air_temperature_c=temp,
                        )
                    )
                member_idx += 1

        # Compute daily quantiles per model
        for model in ENSEMBLE_MODELS:
            quantile_points = self._compute_quantiles(out, model, times, issued_at)
            out.extend(quantile_points)

        logger.info("Open-Meteo ensemble: %d forecast points", len(out))
        return out

    def _compute_quantiles(
        self,
        points: list[ForecastPoint],
        model: str,
        times: list,
        issued_at: datetime,
    ) -> list[ForecastPoint]:
        """Aggregate hourly members into daily Tmax quantiles (0.1, 0.5, 0.9)."""
        from collections import defaultdict
        from statistics import quantiles

        # Group by local Moscow date
        daily_temps: dict[str, list[float]] = defaultdict(list)
        for p in points:
            if p.model != model or p.member is None:
                continue
            local_dt = p.valid_at.astimezone(
                __import__("zoneinfo").ZoneInfo("Europe/Moscow")
            )
            date_str = local_dt.date().isoformat()
            daily_temps[date_str].append(p.air_temperature_c)

        out: list[ForecastPoint] = []
        for date_str, temps in sorted(daily_temps.items()):
            if len(temps) < 3:
                continue
            try:
                qs = quantiles(temps, n=10, method="inclusive")
                q10, q50, q90 = qs[0], qs[4], qs[8]
            except Exception:
                continue
            valid_at = datetime.strptime(date_str, "%Y-%m-%d").replace(
                hour=12, tzinfo=timezone.utc
            )
            lead = int((valid_at - issued_at).total_seconds() // 3600)
            for quantile, val in [(0.1, q10), (0.5, q50), (0.9, q90)]:
                out.append(
                    ForecastPoint(
                        source="open_meteo",
                        model=f"{model}_quantile",
                        station="UUWW",
                        lat=self._lat,
                        lon=self._lon,
                        issued_at=issued_at,
                        valid_at=valid_at,
                        lead_time_h=lead,
                        daily_tmax_c=val,
                        quantile=quantile,
                    )
                )
        return out
