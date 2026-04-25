"""Persistent state for the weather bot.

Stores the last observed NOAA/Yandex temperatures and the running daily
maximum (per Polymarket resolution semantics).  Persisted to a small JSON
file so the bot survives restarts without spamming duplicate messages or
losing the current day's maximum.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class WeatherState:
    last_noaa_temp_c: Optional[int] = None
    last_yandex_temp_c: Optional[int] = None
    daily_max_c: Optional[int] = None
    daily_max_date: Optional[str] = None  # ISO date string (YYYY-MM-DD), Moscow TZ
    # Short-term forecast derived from Yandex hourly forecast at the last tick.
    predicted_30min_c: Optional[int] = None
    predicted_30min_target_iso: Optional[str] = None
    # --- daily-max ensemble forecast (Open-Meteo + Yandex) ---
    # Serialised list of {date, open_meteo_c, yandex_c} dicts, ascending by date.
    forecast_days: list = None  # type: ignore[assignment]
    forecast_fetched_at_iso: Optional[str] = None
    # Optional LLM analysis text (annotates the forecast; never a prediction).
    analysis_text: Optional[str] = None
    analysis_generated_at_iso: Optional[str] = None
    # Flag to deduplicate "NOAA is down, falling back to Yandex" notifications.
    notified_noaa_down: bool = False

    # --- Forecast-vs-actual backtest data ---
    # Dict keyed by ISO date → first-seen forecast for that FUTURE date:
    #   {"2026-04-24": {"open_meteo_c": 6, "yandex_c": 6, "first_seen_iso": "2026-04-23T08:05"}}
    # We only record dates strictly after the current local day at capture time,
    # so verification stays honest (prediction was made before the day started).
    forecast_history: dict = None  # type: ignore[assignment]
    # Finalised days: {date, open_meteo_c, yandex_c, actual_max_c, first_seen_iso}
    # Newest first, bounded to ~30 days.
    verified_forecasts: list = None  # type: ignore[assignment]

    # --- Multi-source observations (sliding window) ---
    # List of Observation dicts.  Kept bounded by the scheduler.
    observations: list = None  # type: ignore[assignment]
    # Metadata about how the current daily_max_c was derived.
    daily_max_source: Optional[str] = None
    daily_max_confirmed_by: list = None  # type: ignore[assignment]
    # --- Detailed forecast aggregates (consensus, band, spread) ---
    forecast_aggregates: list = None  # type: ignore[assignment]
    # When the last Telegram notification was sent (ISO, local tz).
    last_notification_at_iso: Optional[str] = None

    def __post_init__(self) -> None:
        if self.forecast_days is None:
            self.forecast_days = []
        if self.forecast_history is None:
            self.forecast_history = {}
        if self.verified_forecasts is None:
            self.verified_forecasts = []
        if self.observations is None:
            self.observations = []
        if self.daily_max_confirmed_by is None:
            self.daily_max_confirmed_by = []
        if self.forecast_aggregates is None:
            self.forecast_aggregates = []


class StateStore:
    """Thread-safe (within a single event loop) JSON-backed state store.

    Writes are debounced so that rapid successive updates (e.g. tick + forecast
    refresh) coalesce into a single disk flush.
    """

    def __init__(
        self,
        path: Path,
        flush_interval: float = 4.0,
    ) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._state = self._load()
        self._flush_interval = flush_interval
        self._dirty = False
        self._flush_task: Optional[asyncio.Task] = None

    # ---------- internal ----------

    def _load(self) -> WeatherState:
        if not self._path.exists():
            return WeatherState()
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
            # Only accept known fields; ignore extras for forward compat.
            known = {f for f in WeatherState.__dataclass_fields__}
            return WeatherState(**{k: v for k, v in raw.items() if k in known})
        except Exception:
            logger.exception("Failed to load state from %s; starting fresh", self._path)
            return WeatherState()

    def _persist_locked(self) -> None:
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(asdict(self._state), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self._path)
        self._dirty = False
        logger.debug("State persisted to %s", self._path)

    async def _debounced_flush(self) -> None:
        await asyncio.sleep(self._flush_interval)
        async with self._lock:
            if self._dirty:
                self._persist_locked()

    def _trigger_flush(self) -> None:
        self._dirty = True
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._debounced_flush())

    @staticmethod
    def _snapshot(state: WeatherState) -> WeatherState:
        return WeatherState(**asdict(state))

    # ---------- public API ----------

    async def get(self) -> WeatherState:
        async with self._lock:
            return self._snapshot(self._state)

    async def update(self, **fields) -> WeatherState:
        async with self._lock:
            for key, value in fields.items():
                if not hasattr(self._state, key):
                    raise AttributeError(f"Unknown state field: {key}")
                setattr(self._state, key, value)
            self._trigger_flush()
            return self._snapshot(self._state)

    async def update_atomic(
        self, fn: Callable[[WeatherState], None]
    ) -> WeatherState:
        """Apply a synchronous mutator function under the store lock.

        Guarantees read-modify-write atomicity and triggers a debounced flush.
        """
        async with self._lock:
            fn(self._state)
            self._trigger_flush()
            return self._snapshot(self._state)

    async def flush(self) -> None:
        """Force an immediate disk write. Call on graceful shutdown."""
        async with self._lock:
            if self._dirty:
                self._persist_locked()

    async def close(self) -> None:
        """Cancel pending flush and force write."""
        if self._flush_task is not None and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self.flush()
