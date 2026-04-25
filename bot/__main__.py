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
from .db import init_db
from .handlers import build_router
from .proxy_session import ProxyAiohttpSession
from .logging_config import setup_logging
from .scheduler import WeatherScheduler
from .services.forecast import ForecastService
from .services.forecast_aggregator import ForecastAggregator
from .services.iem import IEMASOSService
from .services.llm import LLMService
from .services.met_norway import MetNorwayService
from .services.noaa import NOAAService
from .services.ogimet import OgimetService
from .services.openmeteo import OpenMeteoService
from .services.openmeteo_ensemble import OpenMeteoEnsembleService
from .services.openmeteo_models import OpenMeteoModelsService
from .services.taf import TAFService
from .services.wis2 import WIS2Service
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
    db = await init_db(config.db_path)

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
        ogimet = (
            OgimetService(
                session=session,
                station_id=config.ogimet_station_id,
                timeout_seconds=config.ogimet_timeout_seconds,
                retries=config.http_retries,
                interval_seconds=config.ogimet_interval_seconds,
            )
            if config.ogimet_enabled
            else None
        )
        iem = (
            IEMASOSService(
                session=session,
                station=config.iem_station,
                network=config.iem_network,
                timeout_seconds=config.iem_timeout_seconds,
                retries=config.http_retries,
            )
            if config.iem_enabled
            else None
        )
        wis2 = WIS2Service(
            broker=config.wis2_broker,
            topic=config.wis2_topic,
            username=config.wis2_username,
            password=config.wis2_password,
            wigos_id=config.wis2_wigos_id,
            enabled=config.wis2_enabled,
        )
        # Detailed forecast providers
        forecast_providers = []
        if config.forecast_models_enabled:
            forecast_providers.append(
                OpenMeteoModelsService(
                    session=session,
                    lat=config.moscow_lat,
                    lon=config.moscow_lon,
                    forecast_days=config.forecast_days,
                    timeout_seconds=config.http_timeout_seconds,
                    retries=config.http_retries,
                )
            )
        if config.forecast_ensemble_enabled:
            forecast_providers.append(
                OpenMeteoEnsembleService(
                    session=session,
                    lat=config.moscow_lat,
                    lon=config.moscow_lon,
                    forecast_days=config.forecast_days,
                    timeout_seconds=config.http_timeout_seconds,
                    retries=config.http_retries,
                )
            )
        if config.met_norway_enabled:
            forecast_providers.append(
                MetNorwayService(
                    session=session,
                    lat=config.moscow_lat,
                    lon=config.moscow_lon,
                    user_agent=config.http_user_agent,
                    timeout_seconds=config.http_timeout_seconds,
                    retries=config.http_retries,
                )
            )
        if config.taf_enabled:
            forecast_providers.append(
                TAFService(
                    session=session,
                    station=config.noaa_station,
                    timeout_seconds=config.http_timeout_seconds,
                    retries=config.http_retries,
                )
            )

        aggregator = ForecastAggregator(db)

        scheduler = WeatherScheduler(
            config=config,
            bot=bot,
            store=store,
            noaa=noaa,
            yandex=yandex,
            forecast=forecast,
            llm=llm,
            db=db,
            ogimet=ogimet,
            iem=iem,
            wis2=wis2,
            forecast_providers=forecast_providers,
            aggregator=aggregator,
        )

        dp.include_router(build_router(store, scheduler))

        logger.info(
            "Starting weather bot: station=%s chat_id=%s llm=%s",
            config.noaa_station,
            config.telegram_chat_id,
            f"{config.llm_model} via {config.llm_base_url}" if llm.enabled else "disabled",
        )

        scheduler_task = asyncio.create_task(scheduler.run(), name="scheduler")
        wis2_task = None
        if wis2.enabled:
            wis2_task = asyncio.create_task(wis2.start(), name="wis2")
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
            if wis2_task is not None:
                wis2_task.cancel()
                try:
                    await wis2_task
                except asyncio.CancelledError:
                    pass
                await wis2.stop()
            await db.close()
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
