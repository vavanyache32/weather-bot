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
