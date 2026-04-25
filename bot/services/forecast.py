"""Ensemble daily-max forecast from Open-Meteo + Yandex.

Philosophy:

* The **numbers** come from real NWP / vendor forecasts (never from an
  LLM). Open-Meteo is the primary since it exposes the "seamless"
  multi-model mix for free; Yandex is a second independent signal.
* We aggregate by date and return, per day, per-source maxima and a
  simple ensemble summary (mean, min, max, spread). The spread is what
  downstream code (and the optional LLM commentary) uses to communicate
  confidence.
* Anything an LLM ever says about the forecast is strictly an
  annotation of these numbers — it never overrides them.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from .openmeteo import DailyExtremes, OpenMeteoService
from .yandex import YandexService

logger = logging.getLogger(__name__)


@dataclass
class DailyMaxForecast:
    date: date
    open_meteo_c: Optional[int] = None
    yandex_c: Optional[int] = None

    @property
    def values(self) -> list[int]:
        return [v for v in (self.open_meteo_c, self.yandex_c) if v is not None]

    @property
    def mean_c(self) -> Optional[float]:
        vs = self.values
        return sum(vs) / len(vs) if vs else None

    @property
    def min_c(self) -> Optional[int]:
        vs = self.values
        return min(vs) if vs else None

    @property
    def max_c(self) -> Optional[int]:
        vs = self.values
        return max(vs) if vs else None

    @property
    def spread_c(self) -> Optional[int]:
        vs = self.values
        return (max(vs) - min(vs)) if len(vs) >= 2 else None


@dataclass
class ForecastBundle:
    """What we show to the user / feed to the LLM for analysis."""

    days: list[DailyMaxForecast] = field(default_factory=list)

    def today(self) -> Optional[DailyMaxForecast]:
        return self.days[0] if self.days else None


class ForecastService:
    def __init__(
        self, open_meteo: OpenMeteoService, yandex: YandexService
    ) -> None:
        self._open_meteo = open_meteo
        self._yandex = yandex

    async def fetch(self, days: int = 5) -> ForecastBundle:
        """Pull both sources in parallel and fold into an ensemble bundle.

        Never raises — missing sources just show up as ``None`` slots.
        """
        open_meteo_task = asyncio.create_task(self._open_meteo.fetch_daily())
        yandex_task = asyncio.create_task(self._yandex.fetch())

        results = await asyncio.gather(
            open_meteo_task, yandex_task, return_exceptions=True
        )
        om_list = None if isinstance(results[0], Exception) else results[0]
        yx_reading = None if isinstance(results[1], Exception) else results[1]
        for idx, res in enumerate(results):
            if isinstance(res, Exception):
                logger.warning("Forecast source %d failed: %s", idx, res)

        om_by_date: dict[date, DailyExtremes] = {}
        if om_list:
            for item in om_list:
                om_by_date[item.date] = item

        yx_by_date: dict[date, DailyExtremes] = {}
        if yx_reading and yx_reading.daily:
            for item in yx_reading.daily:
                yx_by_date[item.date] = item

        all_dates = sorted(set(om_by_date) | set(yx_by_date))[:days]
        result: list[DailyMaxForecast] = []
        for d in all_dates:
            om = om_by_date.get(d)
            yx = yx_by_date.get(d)
            result.append(
                DailyMaxForecast(
                    date=d,
                    open_meteo_c=round(om.temp_max_c) if om else None,
                    yandex_c=round(yx.temp_max_c) if yx else None,
                )
            )
        logger.info(
            "Forecast ensemble: %d days (om=%d, yx=%d)",
            len(result),
            len(om_by_date),
            len(yx_by_date),
        )
        return ForecastBundle(days=result)
