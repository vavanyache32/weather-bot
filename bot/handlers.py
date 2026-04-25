"""Telegram command and callback handlers."""
from __future__ import annotations

import logging
from datetime import date as _date

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from .keyboards import (
    CB_ABOUT,
    CB_FORECAST,
    CB_FORECAST_OVERVIEW,
    CB_FORECAST_REFRESH,
    CB_MAIN,
    CB_REFRESH,
    CB_STATUS,
    CB_VERIFIED,
    CB_VERIFIED_REFRESH,
    back_kb,
    forecast_days_kb,
    forecast_day_kb,
    forecast_kb,
    main_menu_kb,
    status_kb,
    verified_kb,
)
from .scheduler import (
    WeatherScheduler,
    format_day_aggregate_message,
    format_day_analysis_message,
    format_forecast_message,
    format_status_message,
    format_verified_message,
)
from .services.forecast import DailyMaxForecast
from .storage import StateStore

logger = logging.getLogger(__name__)

WELCOME_TEXT = (
    "👋 Привет!\n\n"
    "Я слежу за температурой в Москве (Внуково, UUWW) по данным NOAA METAR "
    "и сравниваю с Yandex.Weather. Присылаю обновление, "
    "когда температура изменилась или обновился дневной максимум.\n\n"
    "Дополнительно строю ансамблевый прогноз максимума на ближайшие дни "
    "(Open-Meteo + Yandex) и, если включён LLM, прикладываю короткий "
    "комментарий к числам.\n\n"
    "Выберите действие:"
)

ABOUT_TEXT = (
    "ℹ️ <b>О боте</b>\n\n"
    "• Наблюдения: NOAA / NWS Aviation Weather Center (METAR UUWW), OGIMET SYNOP, IEM ASOS.\n"
    "• Прогноз-ансамбль: Open-Meteo (7 NWP models + ensemble), MET Norway, TAF.\n"
    "• Логика наблюдений соответствует Polymarket: максимум за локальные "
    "сутки (Москва) в целых °C.\n"
    "• LLM (если настроен) только комментирует реальные числа — сам "
    "прогноз не выдумывает.\n\n"
    "Команды:\n"
    "  /start — главное меню\n"
    "  /status — текущее состояние\n"
    "  /forecast — прогноз максимума + анализ"
)


def build_router(store: StateStore, scheduler: WeatherScheduler) -> Router:
    router = Router(name="weather-commands")

    # -------- slash commands --------

    @router.message(Command("start"))
    async def on_start(message: Message) -> None:
        await message.answer(WELCOME_TEXT, reply_markup=main_menu_kb())

    @router.message(Command("status"))
    async def on_status(message: Message) -> None:
        state = await store.get()
        await message.answer(
            format_status_message(state), reply_markup=status_kb()
        )

    @router.message(Command("forecast"))
    async def on_forecast(message: Message) -> None:
        state = await store.get()
        if not state.forecast_days:
            await message.answer("Секунду, собираю прогноз...")
            await scheduler.refresh_forecast(force=True)
            state = await store.get()
        today_iso = scheduler.today_iso()
        await message.answer(
            "📅 <b>Выбери дату для прогноза:</b>",
            reply_markup=forecast_days_kb(state.forecast_days, today_iso),
            parse_mode="HTML",
        )

    @router.message(Command("verified"))
    async def on_verified(message: Message) -> None:
        state = await store.get()
        await message.answer(
            format_verified_message(state),
            reply_markup=verified_kb(),
            parse_mode="HTML",
        )

    # -------- inline-button callbacks --------

    @router.callback_query(F.data == CB_MAIN)
    async def cb_main(callback: CallbackQuery) -> None:
        await _safe_edit(callback, WELCOME_TEXT, main_menu_kb())
        await callback.answer()

    @router.callback_query(F.data == CB_STATUS)
    async def cb_status(callback: CallbackQuery) -> None:
        state = await store.get()
        await _safe_edit(callback, format_status_message(state), status_kb())
        await callback.answer()

    @router.callback_query(F.data == CB_REFRESH)
    async def cb_refresh(callback: CallbackQuery) -> None:
        state = await store.get()
        await _safe_edit(callback, format_status_message(state), status_kb())
        await callback.answer("Обновлено")

    @router.callback_query(F.data == CB_FORECAST)
    async def cb_forecast(callback: CallbackQuery) -> None:
        state = await store.get()
        if not state.forecast_days:
            await callback.answer("Собираю прогноз...")
            await scheduler.refresh_forecast(force=True)
            state = await store.get()
        today_iso = scheduler.today_iso()
        await _safe_edit(
            callback,
            "📅 <b>Выбери дату для прогноза:</b>",
            forecast_days_kb(state.forecast_days, today_iso),
            parse_mode="HTML",
        )
        await callback.answer()

    @router.callback_query(F.data == CB_FORECAST_OVERVIEW)
    async def cb_forecast_overview(callback: CallbackQuery) -> None:
        state = await store.get()
        if not state.forecast_days:
            await callback.answer("Собираю прогноз...")
            await scheduler.refresh_forecast(force=True)
            state = await store.get()
        else:
            await callback.answer()
        await _safe_edit(
            callback, format_forecast_message(state), forecast_kb(), parse_mode="HTML"
        )

    @router.callback_query(F.data == CB_FORECAST_REFRESH)
    async def cb_forecast_refresh(callback: CallbackQuery) -> None:
        await callback.answer("Обновляю...")
        await scheduler.refresh_forecast(force=True)
        try:
            await scheduler.refresh_forecast_detailed(force=True)
        except Exception:
            logger.exception("Detailed forecast refresh failed")
        state = await store.get()
        await _safe_edit(
            callback, format_forecast_message(state), forecast_kb(), parse_mode="HTML"
        )

    @router.callback_query(F.data.startswith("forecast:day:"))
    async def cb_forecast_day(callback: CallbackQuery) -> None:
        date_iso = callback.data.split(":", 2)[2]
        await callback.answer("Анализирую...")
        state = await store.get()

        # Prefer new aggregate data if available
        agg_raw = next(
            (a for a in state.forecast_aggregates or [] if a.get("valid_date") == date_iso),
            None,
        )
        if agg_raw is not None:
            text = format_day_aggregate_message(agg_raw, state.analysis_text)
            await _safe_edit(callback, text, forecast_day_kb(), parse_mode="HTML")
            return

        # Legacy fallback
        analysis = await scheduler.analyze_day(date_iso)
        state = await store.get()
        day_raw = next(
            (d for d in state.forecast_days if d.get("date") == date_iso), None
        )
        if day_raw is None:
            await _safe_edit(
                callback,
                "Данные устарели. Возвращаюсь к выбору даты.",
                forecast_days_kb(state.forecast_days, scheduler.today_iso()),
                parse_mode="HTML",
            )
            return
        day = DailyMaxForecast(
            date=_date.fromisoformat(day_raw["date"]),
            open_meteo_c=day_raw.get("open_meteo_c"),
            yandex_c=day_raw.get("yandex_c"),
        )
        text = format_day_analysis_message(day, analysis)
        await _safe_edit(callback, text, forecast_day_kb(), parse_mode="HTML")

    @router.callback_query(F.data == CB_VERIFIED)
    async def cb_verified(callback: CallbackQuery) -> None:
        state = await store.get()
        await _safe_edit(
            callback, format_verified_message(state), verified_kb(), parse_mode="HTML"
        )
        await callback.answer()

    @router.callback_query(F.data == CB_VERIFIED_REFRESH)
    async def cb_verified_refresh(callback: CallbackQuery) -> None:
        state = await store.get()
        await _safe_edit(
            callback, format_verified_message(state), verified_kb(), parse_mode="HTML"
        )
        await callback.answer("Обновлено")

    @router.callback_query(F.data == CB_ABOUT)
    async def cb_about(callback: CallbackQuery) -> None:
        await _safe_edit(callback, ABOUT_TEXT, back_kb(), parse_mode="HTML")
        await callback.answer()

    return router


async def _safe_edit(callback: CallbackQuery, text: str, markup, parse_mode=None) -> None:
    """Edit the message the callback was attached to; ignore no-op edits.

    Telegram returns "message is not modified" if the user taps the same
    button twice — that's harmless and should not bubble up as an error.
    """
    if callback.message is None:
        return
    try:
        await callback.message.edit_text(
            text, reply_markup=markup, parse_mode=parse_mode
        )
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            return
        logger.warning("edit_text failed: %s", exc)
