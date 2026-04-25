"""Entry point: ``python -m bot``.

Wires together:
  * config (env) → HTTP session → NOAA/Yandex/Open-Meteo/LLM services → state
  * aiogram Dispatcher for command handlers (/start, /status, /forecast)
  * a background scheduler task for periodic polling + notifications
    and hourly forecast refresh (+ optional LLM analysis)
"""
from __future__ import annotations

import asyncio
import logging
import sys

import aiohttp
from aiogram import Bot, Dispatcher

from .config import ConfigError, load_config
from .handlers import build_router
from .proxy_session import ProxyAiohttpSession
from .logging_config import setup_logging
from .scheduler import WeatherScheduler
from .services.forecast import ForecastService
from .services.llm import LLMService
from .services.noaa import NOAAService
from .services.openmeteo import OpenMeteoService
from .services.yandex import YandexService
from .storage import StateStore

logger = logging.getLogger("bot")


async def _amain() -> None:
    config = load_config()
    setup_logging(config.log_level)

    if config.telegram_proxy:
        bot = Bot(
            token=config.telegram_bot_token,
            session=ProxyAiohttpSession(proxy=config.telegram_proxy),
        )
    else:
        bot = Bot(token=config.telegram_bot_token)
    dp = Dispatcher()

    store = StateStore(config.state_file)

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        noaa = NOAAService(
            session=session,
            station=config.noaa_station,
            user_agent=config.http_user_agent,
            timeout_seconds=config.http_timeout_seconds,
            retries=config.http_retries,
        )
        yandex = YandexService(
            session=session,
            api_key=config.yandex_api_key,
            lat=config.moscow_lat,
            lon=config.moscow_lon,
            timeout_seconds=config.http_timeout_seconds,
            retries=config.http_retries,
            timezone=config.timezone,
        )
        open_meteo = OpenMeteoService(
            session=session,
            lat=config.moscow_lat,
            lon=config.moscow_lon,
            timezone=config.timezone,
            timeout_seconds=config.http_timeout_seconds,
            retries=config.http_retries,
            forecast_days=config.forecast_days,
            user_agent=config.http_user_agent,
        )
        forecast = ForecastService(open_meteo=open_meteo, yandex=yandex)
        llm = LLMService(
            api_key=config.llm_api_key,
            base_url=config.llm_base_url,
            model=config.llm_model,
            timeout_seconds=config.llm_timeout_seconds,
        )
        scheduler = WeatherScheduler(
            config=config,
            bot=bot,
            store=store,
            noaa=noaa,
            yandex=yandex,
            forecast=forecast,
            llm=llm,
        )

        dp.include_router(build_router(store, scheduler))

        logger.info(
            "Starting weather bot: station=%s chat_id=%s llm=%s",
            config.noaa_station,
            config.telegram_chat_id,
            f"{config.llm_model} via {config.llm_base_url}" if llm.enabled else "disabled",
        )

        scheduler_task = asyncio.create_task(scheduler.run(), name="scheduler")
        try:
            # handle_signals=True breaks pythonw.exe on Windows; on Linux we
            # need it so aiogram catches SIGTERM from systemd gracefully.
            await dp.start_polling(bot, handle_signals=(sys.platform != "win32"))
        finally:
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                pass
            await store.close()
            await bot.session.close()


def main() -> None:
    try:
        asyncio.run(_amain())
    except ConfigError as exc:
        logging.basicConfig(level=logging.ERROR)
        logging.getLogger("bot").error("Configuration error: %s", exc)
        raise SystemExit(2) from exc
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
