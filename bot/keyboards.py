"""Inline keyboards for the bot menu."""
from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

# Callback data identifiers. Kept short (Telegram limits to 64 bytes) and
# centralised so handlers and keyboards cannot drift apart.
CB_MAIN = "menu:main"
CB_STATUS = "menu:status"
CB_ABOUT = "menu:about"
CB_REFRESH = "menu:status:refresh"
CB_FORECAST = "menu:forecast"
CB_FORECAST_REFRESH = "menu:forecast:refresh"
CB_FORECAST_OVERVIEW = "menu:forecast:overview"
CB_VERIFIED = "menu:verified"
CB_VERIFIED_REFRESH = "menu:verified:refresh"


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статус", callback_data=CB_STATUS)],
            [InlineKeyboardButton(text="📅 Прогноз", callback_data=CB_FORECAST)],
            [InlineKeyboardButton(text="✅ Сбывшиеся", callback_data=CB_VERIFIED)],
            [InlineKeyboardButton(text="ℹ️ О боте", callback_data=CB_ABOUT)],
        ]
    )


def status_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=CB_REFRESH)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_MAIN)],
        ]
    )


def forecast_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=CB_FORECAST_REFRESH)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_MAIN)],
        ]
    )


def forecast_days_kb(forecast_days: list[dict], today_iso: str) -> InlineKeyboardMarkup:
    """Inline keyboard with one button per forecast day."""
    from datetime import date as _date, timedelta

    today = _date.fromisoformat(today_iso)
    tomorrow = today + timedelta(days=1)

    day_buttons: list[InlineKeyboardButton] = []
    for raw in forecast_days:
        date_iso = raw.get("date")
        if not date_iso:
            continue
        d = _date.fromisoformat(date_iso)
        if d == today:
            label = f"Сегодня ({d.day:02d}.{d.month:02d})"
        elif d == tomorrow:
            label = f"Завтра ({d.day:02d}.{d.month:02d})"
        else:
            label = f"{d.day:02d}.{d.month:02d}"
        day_buttons.append(
            InlineKeyboardButton(text=label, callback_data=f"forecast:day:{date_iso}")
        )

    # Arrange days two per row
    rows = [day_buttons[i : i + 2] for i in range(0, len(day_buttons), 2)]
    rows.append(
        [InlineKeyboardButton(text="📊 Общий прогноз", callback_data=CB_FORECAST_OVERVIEW)]
    )
    rows.append(
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_MAIN)]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def forecast_day_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К выбору даты", callback_data=CB_FORECAST)],
            [InlineKeyboardButton(text="⬅️ В меню", callback_data=CB_MAIN)],
        ]
    )


def verified_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=CB_VERIFIED_REFRESH)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_MAIN)],
        ]
    )


def back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_MAIN)],
        ]
    )
