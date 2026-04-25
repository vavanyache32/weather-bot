"""Background scheduler that polls weather APIs and pushes Telegram updates.

Notification rules (matches the product spec):

* Poll NOAA every ``poll_interval_seconds`` (default 1 min); Yandex is
  throttled to once per 10 min so we don't burn API quota.
* Track the **max** NOAA temperature for the current day (Moscow local
  time), resetting at local midnight.  This matches Polymarket's
  resolution semantics: daily max, not average / not latest.
* Send a Telegram message when NOAA or Yandex temperature changes,
  or when a new daily maximum is reached.
* If NOAA is unavailable but Yandex is, still notify so the user is not
  left in the dark.
* If both sources return ``None``, skip silently and log.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from aiogram import Bot


def _round_half_up(value: float) -> int:
    """Round using "round half up" (not Python's banker's rounding)."""
    from decimal import ROUND_HALF_UP, Decimal

    return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
from aiogram.exceptions import TelegramAPIError

from .config import Config
from .models import (
    DailyForecastAggregate,
    DailyMaxAggregate,
    ForecastPoint,
    Observation,
)
from .services.forecast import DailyMaxForecast, ForecastBundle, ForecastService
from .services.forecast_aggregator import ForecastAggregator
from .services.iem import IEMASOSService
from .services.llm import LLMService
from .services.met_norway import MetNorwayService
from .services.noaa import NOAAService
from .services.ogimet import OgimetService
from .services.openmeteo_ensemble import OpenMeteoEnsembleService
from .services.openmeteo_models import OpenMeteoModelsService
from .services.taf import TAFService
from .services.wis2 import WIS2Service
from .services.yandex import YandexReading, YandexService, predict_temperature_c
from .storage import StateStore, WeatherState

# Horizon of the short-term forecast line we attach to every message.
FORECAST_HORIZON = timedelta(minutes=30)

# Cap verified_forecasts to the last N days so state.json stays bounded.
MAX_VERIFIED_DAYS = 30
# Drop forecast_history entries for dates more than N days in the past
# (they couldn't be finalised — probably the bot was off the relevant day).
FORECAST_HISTORY_PRUNE_DAYS = 14

logger = logging.getLogger(__name__)

# Human-readable labels for forecast models shown in Telegram.
_MODEL_LABELS: dict[str, str] = {
    "ecmwf_ifs025": "ECMWF",
    "gfs_seamless": "GFS",
    "icon_seamless": "ICON",
    "gem_seamless": "GEM",
    "meteofrance_seamless": "Météo-France",
    "jma_seamless": "JMA",
    "ukmo_seamless": "UKMO",
    "met_norway_mix": "MET Norway",
    "TAF": "TAF",
    "open_meteo": "Open-Meteo",
    "yandex": "Yandex",
}


def _model_label(model: Optional[str]) -> str:
    if not model:
        return "?"
    return _MODEL_LABELS.get(model, model)


@dataclass
class TickResult:
    noaa_temp_c: Optional[int]
    yandex_temp_c: Optional[int]
    daily_max_c: Optional[int]
    predicted_30min_c: Optional[int]
    new_max: bool
    notified: bool


class WeatherScheduler:
    def __init__(
        self,
        config: Config,
        bot: Bot,
        store: StateStore,
        noaa: NOAAService,
        yandex: YandexService,
        forecast: ForecastService,
        llm: LLMService,
        db: "aiosqlite.Connection",
        ogimet: Optional[OgimetService] = None,
        iem: Optional[IEMASOSService] = None,
        wis2: Optional[WIS2Service] = None,
        forecast_providers: Optional[list] = None,
        aggregator: Optional[ForecastAggregator] = None,
    ) -> None:
        self._config = config
        self._bot = bot
        self._store = store
        self._noaa = noaa
        self._yandex = yandex
        self._forecast = forecast
        self._llm = llm
        self._db = db
        self._ogimet = ogimet
        self._iem = iem
        self._wis2 = wis2
        self._forecast_providers = forecast_providers or []
        self._aggregator = aggregator
        self._tz = ZoneInfo(config.timezone)
        self._last_forecast_refresh: Optional[datetime] = None
        self._last_yandex_reading: Optional[YandexReading] = None
        self._last_yandex_fetch: Optional[datetime] = None
        self._last_ogimet_fetch: Optional[datetime] = None
        self._last_iem_fetch: Optional[datetime] = None
        self._last_detailed_refresh: Optional[datetime] = None
        self._observations: list[Observation] = []

    def _load_observations_from_state(self, state: WeatherState) -> None:
        """Hydrate in-memory observation window from persisted state."""
        raw_list = state.observations or []
        loaded: list[Observation] = []
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            try:
                observed_at = datetime.fromisoformat(item["observed_at"])
                loaded.append(
                    Observation(
                        source=item.get("source", ""),
                        station=item.get("station", ""),
                        observed_at=observed_at,
                        air_temperature_c=item.get("air_temperature_c"),
                        max_temperature_c=item.get("max_temperature_c"),
                        raw=item.get("raw", ""),
                    )
                )
            except (KeyError, ValueError, TypeError):
                continue
        self._observations = loaded

    def today_iso(self) -> str:
        return datetime.now(self._tz).date().isoformat()

    def _dedup_and_merge(self, incoming: list[Observation]) -> None:
        """Add new observations, dropping exact duplicates by key."""
        existing = {o.dedup_key() for o in self._observations}
        for obs in incoming:
            if obs.dedup_key() not in existing:
                self._observations.append(obs)
                existing.add(obs.dedup_key())

    def _trim_observations(self, max_age: timedelta = timedelta(hours=24), max_count: int = 200) -> None:
        """Keep observations bounded by age and count."""
        cutoff = datetime.now(timezone.utc) - max_age
        self._observations = [o for o in self._observations if o.observed_at >= cutoff]
        if len(self._observations) > max_count:
            self._observations = self._observations[-max_count:]

    def _compute_daily_max(self, today_iso: str) -> DailyMaxAggregate:
        """Aggregate today's observations.

        Priority:
        1. Explicit max_temperature_c from any source (SYNOP, IEM).
        2. If none available, max of air_temperature_c across all sources.
        """
        today_date = datetime.strptime(today_iso, "%Y-%m-%d").date()
        day_start = datetime(today_date.year, today_date.month, today_date.day, tzinfo=self._tz)
        day_end = day_start + timedelta(days=1)

        today_obs = [
            o for o in self._observations
            if day_start <= o.observed_at.astimezone(self._tz) < day_end
        ]

        if not today_obs:
            return DailyMaxAggregate()

        # Explicit max track
        explicit_obs = [o for o in today_obs if o.max_temperature_c is not None]
        if explicit_obs:
            best = max(explicit_obs, key=lambda o: _round_half_up(o.max_temperature_c))  # type: ignore[arg-type]
            value = _round_half_up(best.max_temperature_c)  # type: ignore[arg-type]
            confirmed = [
                o.source for o in explicit_obs
                if _round_half_up(o.max_temperature_c) == value  # type: ignore[arg-type]
            ]
            return DailyMaxAggregate(
                value_c=value,
                source=f"{best.source} (explicit)",
                confirmed_by=list(dict.fromkeys(confirmed)),
            )

        # Point max track
        point_obs = [o for o in today_obs if o.air_temperature_c is not None]
        if point_obs:
            best = max(point_obs, key=lambda o: _round_half_up(o.air_temperature_c))  # type: ignore[arg-type]
            value = _round_half_up(best.air_temperature_c)  # type: ignore[arg-type]
            confirmed = [
                o.source for o in point_obs
                if _round_half_up(o.air_temperature_c) == value  # type: ignore[arg-type]
            ]
            return DailyMaxAggregate(
                value_c=value,
                source="point_max",
                confirmed_by=list(dict.fromkeys(confirmed)),
            )

        return DailyMaxAggregate()

    async def run(self) -> None:
        """Main loop. Catches per-tick errors so one bad poll can't kill the bot."""
        logger.info(
            "Scheduler starting: station=%s interval=%ds tz=%s",
            self._config.noaa_station,
            self._config.poll_interval_seconds,
            self._config.timezone,
        )
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                logger.info("Scheduler cancelled; exiting loop")
                raise
            except Exception:
                logger.exception("Unhandled error in scheduler tick")
            await asyncio.sleep(self._config.poll_interval_seconds)

    async def tick(self) -> TickResult:
        """Run one poll + maybe-notify cycle. Returns a summary for tests."""
        noaa_raw = await self._noaa.get_temperature_c()

        # Throttle Yandex so we don't burn API quota when polling NOAA
        # every minute. 10 min is more than enough for current temp + hourly.
        now_local = datetime.now(self._tz)

        if (
            self._last_yandex_fetch is None
            or (now_local - self._last_yandex_fetch).total_seconds() >= 600
        ):
            yandex_reading = await self._yandex.fetch()
            if yandex_reading is not None:
                self._last_yandex_reading = yandex_reading
                self._last_yandex_fetch = now_local
            else:
                # Keep previous reading on transient failure; retry on next tick.
                yandex_reading = self._last_yandex_reading
        else:
            yandex_reading = self._last_yandex_reading

        yandex_raw = yandex_reading.current_c if yandex_reading else None

        # Per Polymarket rules: round to whole degrees Celsius.
        noaa_temp = _round_half_up(noaa_raw) if noaa_raw is not None else None
        yandex_temp = _round_half_up(yandex_raw) if yandex_raw is not None else None

        # Short-term forecast (Yandex hourly, linearly interpolated to +30m).
        target = now_local + FORECAST_HORIZON
        predicted_raw = (
            predict_temperature_c(yandex_reading.hourly, target)
            if yandex_reading
            else None
        )
        predicted_temp = _round_half_up(predicted_raw) if predicted_raw is not None else None

        today_iso = now_local.date().isoformat()
        state = await self._store.get()

        # Midnight reset (Moscow local time).
        if state.daily_max_date != today_iso:
            logger.info(
                "New day (%s); resetting daily max (was %s on %s)",
                today_iso,
                state.daily_max_c,
                state.daily_max_date,
            )
            # Finalise yesterday BEFORE the reset: match its closing
            # max against whatever forecast we had recorded for it in
            # advance. Only runs if both values are available.
            await self._finalize_day(
                state.daily_max_date, state.daily_max_c
            )
            state.daily_max_c = None
            state.daily_max_date = today_iso

        # --- Multi-source observation aggregation ---
        if not self._observations and state.observations:
            self._load_observations_from_state(state)

        new_obs: list[Observation] = []

        # OGIMET (throttled)
        if self._ogimet and self._config.ogimet_enabled:
            if (
                self._last_ogimet_fetch is None
                or (now_local - self._last_ogimet_fetch).total_seconds()
                >= self._config.ogimet_interval_seconds
            ):
                try:
                    ogimet_obs = await self._ogimet.fetch_observations()
                    new_obs.extend(ogimet_obs)
                    self._last_ogimet_fetch = now_local
                except Exception:
                    logger.exception("OGIMET fetch failed")

        # IEM (throttled)
        if self._iem and self._config.iem_enabled:
            if (
                self._last_iem_fetch is None
                or (now_local - self._last_iem_fetch).total_seconds()
                >= self._config.iem_interval_seconds
            ):
                try:
                    iem_obs = await self._iem.fetch_current()
                    if iem_obs is not None:
                        new_obs.append(iem_obs)
                    self._last_iem_fetch = now_local
                except Exception:
                    logger.exception("IEM fetch failed")

        # WIS2 (push)
        if self._wis2 and self._wis2.enabled:
            try:
                wis2_obs = self._wis2.get_recent_observations()
                if wis2_obs:
                    new_obs.extend(wis2_obs)
            except Exception:
                logger.exception("WIS2 observation retrieval failed")

        # Synthetic observations for legacy sources so they participate in aggregation
        if noaa_raw is not None:
            new_obs.append(
                Observation(
                    source="noaa_metar",
                    station=self._config.noaa_station,
                    observed_at=datetime.now(timezone.utc),
                    air_temperature_c=float(noaa_raw),
                    raw="",
                )
            )
        if yandex_raw is not None:
            new_obs.append(
                Observation(
                    source="yandex",
                    station="UUWW",
                    observed_at=datetime.now(timezone.utc),
                    air_temperature_c=float(yandex_raw),
                    raw="",
                )
            )

        self._dedup_and_merge(new_obs)
        self._trim_observations()

        # Compute daily max with explicit-max priority
        daily_max_agg = self._compute_daily_max(today_iso)
        new_daily_max = daily_max_agg.value_c

        new_max = False
        if new_daily_max is not None:
            if state.daily_max_c is None or new_daily_max > state.daily_max_c:
                state.daily_max_c = new_daily_max
                new_max = True

        # Notify rules:
        #   * new daily max        → always
        #   * NOAA temp changed    → only if enough time passed since last notify
        #   * Yandex alone         → never (too noisy)
        #   * NOAA down, Yandex up → once per outage
        should_notify = False
        notified_noaa_down = state.notified_noaa_down

        # Throttle: min seconds between ordinary temperature-change messages.
        last_notify_iso = state.last_notification_at_iso
        last_notify = datetime.fromisoformat(last_notify_iso) if last_notify_iso else None
        notify_cooldown = self._config.notify_min_interval_seconds
        cooldown_ok = (
            last_notify is None
            or (now_local - last_notify).total_seconds() >= notify_cooldown
        )

        if new_max:
            should_notify = True
        elif noaa_temp is not None and noaa_temp != state.last_noaa_temp_c:
            if cooldown_ok:
                should_notify = True
        elif noaa_temp is None and yandex_temp is not None:
            # NOAA is down but Yandex is up — notify once.
            if not state.notified_noaa_down:
                should_notify = True
                notified_noaa_down = True

        # Persist: only overwrite last_*_temp when we actually got a reading,
        # so comparisons stay stable across transient failures.
        await self._store.update(
            last_noaa_temp_c=(
                noaa_temp if noaa_temp is not None else state.last_noaa_temp_c
            ),
            last_yandex_temp_c=(
                yandex_temp if yandex_temp is not None else state.last_yandex_temp_c
            ),
            daily_max_c=state.daily_max_c,
            daily_max_date=state.daily_max_date,
            predicted_30min_c=(
                predicted_temp
                if predicted_temp is not None
                else state.predicted_30min_c
            ),
            predicted_30min_target_iso=(
                target.isoformat(timespec="minutes")
                if predicted_temp is not None
                else state.predicted_30min_target_iso
            ),
            notified_noaa_down=(
                False if noaa_temp is not None else notified_noaa_down
            ),
            observations=[
                {
                    "source": o.source,
                    "station": o.station,
                    "observed_at": o.observed_at.isoformat(),
                    "air_temperature_c": o.air_temperature_c,
                    "max_temperature_c": o.max_temperature_c,
                    "raw": o.raw,
                }
                for o in self._observations
            ],
            daily_max_source=daily_max_agg.source,
            daily_max_confirmed_by=daily_max_agg.confirmed_by,
        )

        # Refresh the daily-max ensemble forecast + optional LLM analysis
        # on a slower cadence (NWP models only update hourly anyway).
        await self.refresh_forecast(
            noaa_temp=noaa_temp,
            daily_max_so_far=state.daily_max_c,
            predicted_30min=predicted_temp,
            force=False,
        )
        try:
            await self.refresh_forecast_detailed(force=False)
        except Exception:
            logger.exception("Detailed forecast refresh failed")

        notified = False
        if noaa_temp is None and yandex_temp is None:
            logger.warning(
                "Both NOAA and Yandex unavailable or null; skipping notification"
            )
        elif should_notify:
            message = format_update_message(
                noaa_temp=noaa_temp,
                daily_max_c=state.daily_max_c,
                yandex_temp=yandex_temp,
                predicted_30min_c=predicted_temp,
                new_max=new_max,
                now_local=now_local,
                daily_max_source=daily_max_agg.source,
                daily_max_confirmed_by=daily_max_agg.confirmed_by,
            )
            try:
                await self._bot.send_message(
                    self._config.telegram_chat_id, message
                )
                notified = True
                await self._store.update(
                    last_notification_at_iso=now_local.isoformat(timespec="minutes")
                )

                logger.info(
                    "Sent update: %s", message.replace("\n", " | ")
                )
            except TelegramAPIError:
                logger.exception("Telegram send failed")
        else:
            logger.info(
                "No change: noaa=%s max=%s yandex=%s",
                noaa_temp,
                state.daily_max_c,
                yandex_temp,
            )

        return TickResult(
            noaa_temp_c=noaa_temp,
            yandex_temp_c=yandex_temp,
            daily_max_c=state.daily_max_c,
            predicted_30min_c=predicted_temp,
            new_max=new_max,
            notified=notified,
        )

    async def refresh_forecast(
        self,
        noaa_temp: Optional[int] = None,
        daily_max_so_far: Optional[int] = None,
        predicted_30min: Optional[int] = None,
        force: bool = False,
    ) -> bool:
        """Fetch ensemble forecast (+ optional LLM analysis) and persist it.

        When invoked from the scheduler this is called each tick with
        ``force=False`` so the cadence gate (``forecast_refresh_seconds``)
        applies.  Handlers call with ``force=True`` to refresh on demand
        (user tapped "🔄 Обновить").

        Inputs that are ``None`` are pulled from current state so the LLM
        prompt still has the latest observations.

        Returns True if a refresh actually happened, False if skipped.
        """
        now_local = datetime.now(self._tz)
        interval = self._config.forecast_refresh_seconds
        if (
            not force
            and self._last_forecast_refresh is not None
            and (now_local - self._last_forecast_refresh).total_seconds() < interval
        ):
            return False

        # Fill missing observation inputs from state.
        if noaa_temp is None or daily_max_so_far is None or predicted_30min is None:
            state = await self._store.get()
            if noaa_temp is None:
                noaa_temp = state.last_noaa_temp_c
            if daily_max_so_far is None:
                daily_max_so_far = state.daily_max_c
            if predicted_30min is None:
                predicted_30min = state.predicted_30min_c

        try:
            bundle = await self._forecast.fetch(days=self._config.forecast_days)
        except Exception:
            logger.exception("Forecast ensemble fetch failed")
            return False

        analysis_text: Optional[str] = None
        if self._llm.enabled and bundle.days:
            try:
                analysis_text = await self._llm.analyze(
                    bundle=bundle,
                    noaa_temp_c=noaa_temp,
                    daily_max_so_far_c=daily_max_so_far,
                    predicted_30min_c=predicted_30min,
                )
            except Exception:
                logger.exception("LLM analysis failed")

        # Record first-sighting of each FUTURE date in forecast_history.
        # Today's forecast is intentionally NOT stored — any number observed
        # after the day has started is partially contaminated with in-day
        # observations and would make the backtest dishonest.
        today_iso = now_local.date().isoformat()
        state_now = await self._store.get()
        history = dict(state_now.forecast_history or {})
        stamp = now_local.isoformat(timespec="minutes")
        recorded = 0
        for d in bundle.days:
            date_iso = d.date.isoformat()
            if date_iso <= today_iso:
                continue  # only store strictly-future dates
            if date_iso in history:
                continue  # first-seen wins
            history[date_iso] = {
                "open_meteo_c": d.open_meteo_c,
                "yandex_c": d.yandex_c,
                "first_seen_iso": stamp,
            }
            recorded += 1
        # Prune very old unfinalised entries so state.json doesn't grow forever.
        cutoff = (now_local.date() - timedelta(days=FORECAST_HISTORY_PRUNE_DAYS)).isoformat()
        history = {k: v for k, v in history.items() if k >= cutoff}

        await self._store.update(
            forecast_days=[_serialize_day(d) for d in bundle.days],
            forecast_fetched_at_iso=now_local.isoformat(timespec="minutes"),
            analysis_text=analysis_text,
            analysis_generated_at_iso=(
                now_local.isoformat(timespec="minutes")
                if analysis_text
                else None
            ),
            forecast_history=history,
        )
        self._last_forecast_refresh = now_local
        logger.info(
            "Forecast refreshed (force=%s): %d days, %d new future-day snapshots, analysis=%s",
            force,
            len(bundle.days),
            recorded,
            "yes" if analysis_text else "no",
        )
        return True

    async def refresh_forecast_detailed(self, force: bool = False) -> bool:
        """Fetch from all forecast providers, store in SQLite, recompute aggregates.

        Cadence is controlled by ``forecast_detailed_refresh_seconds``.
        """
        now_local = datetime.now(self._tz)
        interval = self._config.forecast_detailed_refresh_seconds
        if (
            not force
            and self._last_detailed_refresh is not None
            and (now_local - self._last_detailed_refresh).total_seconds() < interval
        ):
            return False

        if not self._forecast_providers:
            return False

        # Fetch from all providers in parallel
        tasks = [asyncio.create_task(p.fetch()) for p in self._forecast_providers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Also fetch legacy bundle (Open-Meteo + Yandex) and inject as synthetic points
        try:
            bundle = await self._forecast.fetch(days=self._config.forecast_days)
        except Exception:
            logger.exception("Legacy forecast fetch failed")
            bundle = None

        total_inserted = 0
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Forecast provider failed: %s", result)
                continue
            points: list[ForecastPoint] = result
            for pt in points:
                try:
                    await self._db.execute(
                        """
                        INSERT OR IGNORE INTO forecasts
                        (source, model, member, station, lat, lon, issued_at, valid_at,
                         lead_time_h, air_temperature_c, daily_tmax_c, daily_tmin_c,
                         quantile, raw)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            pt.source,
                            pt.model,
                            pt.member,
                            pt.station,
                            pt.lat,
                            pt.lon,
                            pt.issued_at.isoformat(),
                            pt.valid_at.isoformat(),
                            pt.lead_time_h,
                            pt.air_temperature_c,
                            pt.daily_tmax_c,
                            pt.daily_tmin_c,
                            pt.quantile,
                            pt.raw,
                        ),
                    )
                    total_inserted += 1
                except Exception as exc:
                    logger.warning("Failed to insert forecast point: %s", exc)

        # Inject legacy Yandex / old Open-Meteo as synthetic points so they
        # participate in consensus / spread calculations.
        if bundle:
            issued_at = datetime.now(timezone.utc)
            for day in bundle.days:
                base_valid = datetime(
                    day.date.year, day.date.month, day.date.day, 12, tzinfo=timezone.utc
                )
                if day.open_meteo_c is not None:
                    try:
                        await self._db.execute(
                            """
                            INSERT OR IGNORE INTO forecasts
                            (source, model, station, lat, lon, issued_at, valid_at,
                             lead_time_h, daily_tmax_c)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                "open_meteo_legacy",
                                "Open-Meteo",
                                "UUWW",
                                55.5914,
                                37.2615,
                                issued_at.isoformat(),
                                base_valid.isoformat(),
                                0,
                                float(day.open_meteo_c),
                            ),
                        )
                        total_inserted += 1
                    except Exception as exc:
                        logger.warning("Legacy OM insert failed: %s", exc)
                if day.yandex_c is not None:
                    try:
                        await self._db.execute(
                            """
                            INSERT OR IGNORE INTO forecasts
                            (source, model, station, lat, lon, issued_at, valid_at,
                             lead_time_h, daily_tmax_c)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                "yandex",
                                "Yandex",
                                "UUWW",
                                55.5914,
                                37.2615,
                                issued_at.isoformat(),
                                base_valid.isoformat(),
                                0,
                                float(day.yandex_c),
                            ),
                        )
                        total_inserted += 1
                    except Exception as exc:
                        logger.warning("Legacy Yandex insert failed: %s", exc)

        await self._db.commit()

        # Recompute aggregates for today + next days
        aggregate_dicts: list[dict] = []
        if self._aggregator:
            from datetime import date as _date
            today = now_local.date()
            aggregates = await self._aggregator.compute_range(
                "UUWW",
                today,
                today + timedelta(days=self._config.forecast_days),
            )
            logger.info(
                "Forecast aggregates recomputed: %d days", len(aggregates)
            )
            for agg in aggregates:
                aggregate_dicts.append(
                    {
                        "station": agg.station,
                        "valid_date": agg.valid_date.isoformat(),
                        "consensus_c": agg.consensus_c,
                        "band_low_c": agg.band_low_c,
                        "band_high_c": agg.band_high_c,
                        "model_spread_c": agg.model_spread_c,
                        "mixed_forecast_c": agg.mixed_forecast_c,
                        "models": list(agg.models),
                        "sources_used": list(agg.sources_used),
                    }
                )
            await self._store.update(forecast_aggregates=aggregate_dicts)

        self._last_detailed_refresh = now_local
        logger.info(
            "Detailed forecast refresh: %d points inserted", total_inserted
        )
        return True

    async def analyze_day(self, date_iso: str) -> Optional[str]:
        """Run LLM analysis for a single forecast day. Returns text or None."""
        logger.info("Analyzing day %s (llm enabled=%s)", date_iso, self._llm.enabled)
        state = await self._store.get()
        day_raw = next(
            (d for d in state.forecast_days if d.get("date") == date_iso), None
        )
        if day_raw is None:
            logger.warning("Day %s not found in forecast_days", date_iso)
            return None
        day = _deserialize_day(day_raw)
        if day is None:
            logger.warning("Day %s deserialization failed", date_iso)
            return None
        result = await self._llm.analyze_day(
            day=day,
            noaa_temp_c=state.last_noaa_temp_c,
            daily_max_so_far_c=state.daily_max_c,
            predicted_30min_c=state.predicted_30min_c,
        )
        if result is None:
            logger.warning("LLM returned None for day %s", date_iso)
        else:
            logger.info("LLM analysis for day %s received (%d chars)", date_iso, len(result))
        return result

    async def _finalize_day(
        self, date_iso: Optional[str], actual_max_c: Optional[int]
    ) -> None:
        """Record {forecast, actual} for a closed day into verified_forecasts.

        Only runs if:
          * the day has an actual max (bot observed something), AND
          * we recorded a forecast for this date before it started (lives in
            forecast_history).
        Otherwise the day is silently skipped — a backtest entry with missing
        halves would be worse than no entry.
        """
        if not date_iso or actual_max_c is None:
            return
        state = await self._store.get()
        history = dict(state.forecast_history or {})
        snapshot = history.pop(date_iso, None)
        if snapshot is None:
            logger.info(
                "Day %s finalised without a prior forecast — skipping backtest entry",
                date_iso,
            )
            return

        entry = {
            "date": date_iso,
            "open_meteo_c": snapshot.get("open_meteo_c"),
            "yandex_c": snapshot.get("yandex_c"),
            "actual_max_c": actual_max_c,
            "first_seen_iso": snapshot.get("first_seen_iso"),
        }
        verified = [e for e in (state.verified_forecasts or []) if e.get("date") != date_iso]
        verified.insert(0, entry)
        verified = verified[:MAX_VERIFIED_DAYS]

        await self._store.update(
            verified_forecasts=verified,
            forecast_history=history,
        )
        logger.info(
            "Finalised %s: actual=%s°C, OM=%s°C, Yx=%s°C",
            date_iso,
            actual_max_c,
            snapshot.get("open_meteo_c"),
            snapshot.get("yandex_c"),
        )


def format_update_message(
    noaa_temp: Optional[int],
    daily_max_c: Optional[int],
    yandex_temp: Optional[int],
    predicted_30min_c: Optional[int],
    new_max: bool,
    now_local: Optional[datetime] = None,
    daily_max_source: Optional[str] = None,
    daily_max_confirmed_by: Optional[list[str]] = None,
) -> str:
    """Build the live temperature update pushed to Telegram."""
    lines: list[str] = []
    if now_local is not None:
        time_str = now_local.strftime("%H:%M")
        lines.append(f"⏰ {time_str}")
    lines.append(
        f"🌡 Внуково (METAR): {noaa_temp}°C"
        if noaa_temp is not None
        else "🌡 Внуково (METAR): н/д"
    )
    if daily_max_c is not None:
        max_line = f"📈 Макс сегодня: {daily_max_c}°C"
        # Minimal human-readable hint, nothing technical.
        if daily_max_source and "explicit" in daily_max_source:
            max_line += " (по официальным данным)"
        elif daily_max_source == "point_max":
            max_line += " (по текущим замерам)"
        lines.append(max_line)
    else:
        lines.append("📈 Макс сегодня: н/д")
    lines.append(
        f"🟡 Яндекс: {yandex_temp}°C"
        if yandex_temp is not None
        else "🟡 Яндекс: н/д"
    )
    if predicted_30min_c is not None:
        lines.append(f"🔮 Через ~30 мин: {predicted_30min_c}°C")
    if new_max:
        lines.append("🔥 Новый дневной максимум!")
    return "\n".join(lines)


def _serialize_day(d: DailyMaxForecast) -> dict:
    return {
        "date": d.date.isoformat(),
        "open_meteo_c": d.open_meteo_c,
        "yandex_c": d.yandex_c,
    }


def _deserialize_day(raw: dict) -> Optional[DailyMaxForecast]:
    from datetime import date as _date  # local import to avoid top-level churn

    d = raw.get("date")
    if not d:
        return None
    try:
        day = _date.fromisoformat(d)
    except (TypeError, ValueError):
        return None
    return DailyMaxForecast(
        date=day,
        open_meteo_c=raw.get("open_meteo_c"),
        yandex_c=raw.get("yandex_c"),
    )


def format_forecast_message(state: WeatherState) -> str:
    """Render the stored ensemble forecast + analysis for /forecast."""
    if state.forecast_aggregates:
        return _format_detailed_forecast(state)

    # Legacy fallback
    if not state.forecast_days:
        return (
            "Прогноз ещё не собран — подождите одного тика "
            "(около минуты после запуска) и нажмите «Обновить»."
        )

    days: list[DailyMaxForecast] = []
    for raw in state.forecast_days:
        d = _deserialize_day(raw)
        if d is not None:
            days.append(d)

    lines = ["📅 <b>Прогноз максимума по дням</b>"]
    today = days[0] if days else None
    if today:
        vals = today.values
        if len(vals) >= 2:
            lines.append(
                f"🌤 Сегодня ({today.date.isoformat()}): "
                f"<b>{today.min_c}–{today.max_c}°C</b> "
                f"(Open-Meteo {today.open_meteo_c}°C, Yandex {today.yandex_c}°C)"
            )
        elif vals:
            only = "Open-Meteo" if today.open_meteo_c is not None else "Yandex"
            lines.append(
                f"🌤 Сегодня ({today.date.isoformat()}): "
                f"<b>{vals[0]}°C</b> ({only}, один источник)"
            )
    if state.daily_max_c is not None:
        lines.append(f"📈 Уже сейчас макс по METAR: {state.daily_max_c}°C")
    if today and today.max_c is not None and state.daily_max_c is not None:
        delta = today.max_c - state.daily_max_c
        if delta > 0:
            lines.append(f"   до прогнозного максимума ещё ~{delta}°C")

    spread = today.spread_c if today else None
    if spread is not None:
        if spread == 0:
            lines.append("🟢 Модели согласны")
        elif spread <= 2:
            lines.append(f"🟢 Уверенность высокая (спред {spread}°C)")
        elif spread <= 4:
            lines.append(f"🟡 Уверенность средняя (спред {spread}°C)")
        else:
            lines.append(f"🔴 Уверенность низкая (спред {spread}°C)")

    # Next few days
    if len(days) > 1:
        lines.append("")
        lines.append("<b>Ближайшие дни:</b>")
        for d in days[1:]:
            parts = []
            if d.open_meteo_c is not None:
                parts.append(f"OM {d.open_meteo_c}°C")
            if d.yandex_c is not None:
                parts.append(f"Yx {d.yandex_c}°C")
            lines.append(f"  {d.date.isoformat()}: {', '.join(parts) or 'нет данных'}")

    if state.analysis_text:
        lines.append("")
        lines.append("🧠 <b>Анализ:</b>")
        lines.append(state.analysis_text)

    if state.forecast_fetched_at_iso:
        lines.append("")
        lines.append(f"<i>Обновлено: {state.forecast_fetched_at_iso}</i>")
    return "\n".join(lines)


def _format_detailed_forecast(state: WeatherState) -> str:
    """Render forecast using the new aggregate structure."""
    lines: list[str] = []
    for agg_raw in state.forecast_aggregates or []:
        date_str = agg_raw.get("valid_date", "?")
        consensus = agg_raw.get("consensus_c")
        band_low = agg_raw.get("band_low_c")
        band_high = agg_raw.get("band_high_c")
        spread = agg_raw.get("model_spread_c")
        models = agg_raw.get("models", [])

        if consensus is None:
            continue

        line = f"📅 <b>{date_str}</b>  consensus <b>{consensus:.1f}°C</b>"
        if band_low is not None and band_high is not None:
            line += f"  (P10..P90 {band_low:.1f}..{band_high:.1f}°C)"
        if spread is not None:
            line += f"  spread {spread:.1f}°C"
        lines.append(line)

        if models:
            seen: set[str] = set()
            model_parts: list[str] = []
            for source, model, val in models:
                label = _model_label(model) if model else _model_label(source)
                if label in seen:
                    continue
                seen.add(label)
                model_parts.append(f"{label} {val:.1f}°C")
                if len(model_parts) >= 10:
                    break
            if model_parts:
                lines.append("   " + " | ".join(model_parts))

    if state.daily_max_c is not None:
        lines.append(f"📈 Фактический макс сегодня: {state.daily_max_c}°C")

    if state.forecast_fetched_at_iso:
        lines.append(f"<i>Обновлено: {state.forecast_fetched_at_iso}</i>")
    return "\n".join(lines) if lines else "Прогноз ещё не собран."


def format_day_analysis_message(
    day: DailyMaxForecast, analysis_text: Optional[str]
) -> str:
    """Render a single-day forecast + LLM analysis for Telegram.

    Legacy fallback for old forecast_days structure.
    """
    lines: list[str] = []
    lines.append(f"📅 <b>Прогноз на {day.date.isoformat()}</b>")

    parts = []
    if day.open_meteo_c is not None:
        parts.append(f"Open-Meteo {day.open_meteo_c}°C")
    if day.yandex_c is not None:
        parts.append(f"Yandex {day.yandex_c}°C")
    if parts:
        lines.append(f"🌤 Модели: {', '.join(parts)}")
    else:
        lines.append("🌤 Модели: нет данных")

    if day.spread_c is not None:
        if day.spread_c == 0:
            lines.append("🟢 Модели полностью согласны")
        elif day.spread_c <= 2:
            lines.append(f"🟢 Уверенность высокая (спред {day.spread_c}°C)")
        elif day.spread_c <= 4:
            lines.append(f"🟡 Уверенность средняя (спред {day.spread_c}°C)")
        else:
            lines.append(f"🔴 Уверенность низкая (спред {day.spread_c}°C)")

    if analysis_text:
        lines.append("")
        lines.append("🧠 <b>Анализ ИИ:</b>")
        lines.append(analysis_text)
    else:
        lines.append("")
        lines.append("🧠 <b>Анализ ИИ:</b> недоступен (LLM не настроен или ошибка)")

    return "\n".join(lines)


def format_day_aggregate_message(
    agg: dict, analysis_text: Optional[str]
) -> str:
    """Render a single-day forecast from the new aggregate structure."""
    date_str = agg.get("valid_date", "?")
    consensus = agg.get("consensus_c")
    band_low = agg.get("band_low_c")
    band_high = agg.get("band_high_c")
    spread = agg.get("model_spread_c")
    models = agg.get("models", [])

    lines: list[str] = []
    lines.append(f"📅 <b>Прогноз на {date_str}</b>")

    if consensus is not None:
        lines.append(f"🎯 Consensus: <b>{consensus:.1f}°C</b>")
    if band_low is not None and band_high is not None:
        lines.append(f"📊 Диапазон P10..P90: <b>{band_low:.1f}..{band_high:.1f}°C</b>")

    if models:
        seen: set[str] = set()
        parts: list[str] = []
        for source, model, val in models:
            label = _model_label(model) if model else _model_label(source)
            if label in seen:
                continue
            seen.add(label)
            parts.append(f"{label} {val:.1f}°C")
            if len(parts) >= 12:
                break
        if parts:
            lines.append(f"🧩 Модели: {', '.join(parts)}")
    else:
        lines.append("🧩 Модели: нет данных")

    if spread is not None:
        if spread == 0:
            lines.append("🟢 Модели полностью согласны")
        elif spread <= 2:
            lines.append(f"🟢 Уверенность высокая (спред {spread:.1f}°C)")
        elif spread <= 4:
            lines.append(f"🟡 Уверенность средняя (спред {spread:.1f}°C)")
        else:
            lines.append(f"🔴 Уверенность низкая (спред {spread:.1f}°C)")

    if analysis_text:
        lines.append("")
        lines.append("🧠 <b>Анализ ИИ:</b>")
        lines.append(analysis_text)
    else:
        lines.append("")
        lines.append("🧠 <b>Анализ ИИ:</b> недоступен (LLM не настроен или ошибка)")

    return "\n".join(lines)


def format_verified_message(state: WeatherState, limit: int = 10) -> str:
    """Render verified_forecasts: 'forecast made in advance' vs actual."""
    entries = list(state.verified_forecasts or [])
    if not entries:
        return (
            "✅ <b>Сбывшиеся прогнозы</b>\n\n"
            "Пока пусто. Я сохраняю прогноз только для <b>будущих</b> дней "
            "(сделанный заранее, без подглядывания в утренние наблюдения). "
            "Первые результаты появятся завтра после полуночи.\n\n"
            "Это честный бэктест: сравниваю, что модели обещали, "
            "с тем, что реально намерил METAR Внуково."
        )

    shown = entries[:limit]
    lines = ["✅ <b>Сбывшиеся прогнозы</b>", ""]
    om_errors: list[int] = []
    yx_errors: list[int] = []
    ens_errors: list[int] = []
    for e in shown:
        lines.append(_format_verified_row(e))
        actual = e.get("actual_max_c")
        if actual is None:
            continue
        om = e.get("open_meteo_c")
        yx = e.get("yandex_c")
        if om is not None:
            om_errors.append(abs(om - actual))
        if yx is not None:
            yx_errors.append(abs(yx - actual))
        values = [v for v in (om, yx) if v is not None]
        if values:
            ens = sum(values) / len(values)
            ens_errors.append(abs(ens - actual))

    def mae(xs):
        return (sum(xs) / len(xs)) if xs else None

    lines.append("")
    lines.append(f"<i>Всего записей: {len(entries)} (показано {len(shown)})</i>")
    agg_parts = []
    om_mae = mae(om_errors)
    yx_mae = mae(yx_errors)
    ens_mae = mae(ens_errors)
    if om_mae is not None:
        agg_parts.append(f"Open-Meteo {om_mae:.1f}°C")
    if yx_mae is not None:
        agg_parts.append(f"Yandex {yx_mae:.1f}°C")
    if ens_mae is not None:
        agg_parts.append(f"ensemble {ens_mae:.1f}°C")
    if agg_parts:
        lines.append("📐 <b>Средняя ошибка (MAE):</b> " + " | ".join(agg_parts))

    return "\n".join(lines)


def _format_verified_row(e: dict) -> str:
    date = e.get("date", "?")
    actual = e.get("actual_max_c")
    om = e.get("open_meteo_c")
    yx = e.get("yandex_c")

    def fmt_delta(forecast):
        if forecast is None or actual is None:
            return "n/a"
        d = forecast - actual
        sign = "+" if d > 0 else ""  # negative sign from minus itself
        return f"{forecast}°C ({sign}{d})"

    errs = [
        abs(v - actual)
        for v in (om, yx)
        if v is not None and actual is not None
    ]
    worst = max(errs) if errs else None
    if worst is None:
        badge = "⚪"
    elif worst <= 2:
        badge = "🟢"
    elif worst <= 4:
        badge = "🟡"
    else:
        badge = "🔴"

    actual_str = f"{actual}°C" if actual is not None else "n/a"
    return (
        f"{badge} <b>{date}</b>  факт {actual_str}  |  "
        f"OM {fmt_delta(om)}  |  Yx {fmt_delta(yx)}"
    )


def format_status_message(state: WeatherState) -> str:
    lines = [
        f"🌡 Внуково (METAR): {state.last_noaa_temp_c}°C"
        if state.last_noaa_temp_c is not None
        else "🌡 Внуково (METAR): н/д",
    ]
    if state.daily_max_c is not None:
        max_line = f"📈 Макс сегодня: {state.daily_max_c}°C"
        if state.daily_max_source and "explicit" in state.daily_max_source:
            max_line += " (по официальным данным)"
        elif state.daily_max_source == "point_max":
            max_line += " (по текущим замерам)"
        lines.append(max_line)
    else:
        lines.append("📈 Макс сегодня: н/д")
    lines.append(
        f"🟡 Яндекс: {state.last_yandex_temp_c}°C"
        if state.last_yandex_temp_c is not None
        else "🟡 Яндекс: н/д",
    )
    if state.predicted_30min_c is not None:
        lines.append(
            f"🔮 Через ~30 мин: {state.predicted_30min_c}°C"
        )
    return "\n".join(lines)
