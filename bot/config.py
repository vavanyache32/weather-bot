"""Runtime configuration loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


@dataclass(frozen=True)
class Config:
    # Secrets
    telegram_bot_token: str
    telegram_chat_id: int
    telegram_proxy: str | None
    yandex_api_key: str | None

    # Data source settings (Polymarket uses Vnukovo / UUWW)
    noaa_station: str = "UUWW"
    # Vnukovo airport coordinates (close to the station used by NOAA/METAR).
    moscow_lat: float = 55.5914
    moscow_lon: float = 37.2615

    # Scheduler
    poll_interval_seconds: int = 60  # 1 minute
    timezone: str = "Europe/Moscow"

    # HTTP
    http_timeout_seconds: int = 15
    http_retries: int = 3
    # NOAA requires an identifying User-Agent with contact info.
    http_user_agent: str = field(
        default="weatherapp-telegram-bot (https://example.com; contact@example.com)"
    )

    # Persistence
    state_file: Path = Path("state.json")

    # Logging
    log_level: str = "INFO"

    # Forecast horizon in days for /forecast command
    forecast_days: int = 5
    # How often (seconds) to refresh the forecast bundle in the scheduler.
    # NWP models do not refresh faster than hourly; polling more often is
    # wasteful. Default 1h.
    forecast_refresh_seconds: int = 3600

    # --- LLM (optional analysis) ---
    llm_api_key: str | None = None
    # Moonshot/Kimi defaults; OpenAI-compatible endpoints work unchanged.
    llm_base_url: str = "https://api.moonshot.ai/v1"
    llm_model: str = "kimi-latest"
    # Thinking models (kimi-k2.6, o1-style) can take 30–90s end-to-end on
    # non-trivial prompts. 120s gives headroom; auto-retries are disabled
    # in LLMService so one failure is bounded.
    llm_timeout_seconds: int = 120

    # --- OGIMET SYNOP (station 27611 = Vnukovo) ---
    ogimet_enabled: bool = True
    ogimet_station_id: str = "27611"
    ogimet_interval_seconds: int = 600  # 10 min
    ogimet_timeout_seconds: int = 15

    # --- IEM ASOS (METAR + SPECI aggregator) ---
    iem_enabled: bool = True
    iem_station: str = "UUWW"
    iem_network: str = "RU__ASOS"
    iem_interval_seconds: int = 300  # 5 min
    iem_timeout_seconds: int = 15

    # --- WIS 2.0 MQTT (optional, disabled by default) ---
    wis2_enabled: bool = False
    wis2_broker: str = "mqtts://globalbroker.meteo.fr:8883"
    wis2_topic: str = "origin/a/wis2/ru-roshydromet/data/core/weather/surface-based-observations/synop"
    wis2_username: str = "everyone"
    wis2_password: str = "everyone"
    wis2_wigos_id: str = "0-20000-0-27611"

    # --- SQLite DB ---
    db_path: Path = Path("weather.db")

    # --- Detailed forecast pipeline (new sources) ---
    forecast_detailed_refresh_seconds: int = 3600  # 1 hour
    forecast_models_enabled: bool = True
    forecast_ensemble_enabled: bool = True
    met_norway_enabled: bool = True
    met_norway_interval_seconds: int = 1800  # 30 min
    taf_enabled: bool = True
    taf_interval_seconds: int = 1800  # 30 min
    meteoinfo_enabled: bool = False
    meteoinfo_interval_seconds: int = 10800  # 3 hours

    # --- Notifications ---
    notify_min_interval_seconds: int = 1800  # min 30 min between temp-change msgs

    # --- Alerts ---
    alerts_enabled: bool = True
    alerts_config_path: Path | None = Path("alerts.yaml")
    webhook_url: str | None = None
    email_enabled: bool = False


# Strings that appear in .env.example and are obvious "you forgot to edit"
# values. We bail out early with a human-readable message instead of making
# the user read a 50-line aiogram/aiohttp traceback.
_TOKEN_PLACEHOLDERS = {
    "",
    "your-token-here",
    "123456789:aa...your-token-here",
}
_CHAT_ID_PLACEHOLDERS = {0, 123456789}
_YANDEX_PLACEHOLDERS = {"", "your-yandex-api-key"}


def _get_required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"Required environment variable {name!r} is not set")
    return value


def load_config() -> Config:
    """Load config from the process environment (and .env file)."""
    load_dotenv()
    token = _get_required("TELEGRAM_BOT_TOKEN").strip()
    chat_id_raw = _get_required("TELEGRAM_CHAT_ID").strip()

    if token.lower() in _TOKEN_PLACEHOLDERS or "your-token-here" in token.lower():
        raise ConfigError(
            "TELEGRAM_BOT_TOKEN is still the placeholder from .env.example. "
            "Put your real token from @BotFather into .env."
        )
    # A real Telegram bot token looks like "123456789:AA<35 chars>".
    if ":" not in token or len(token) < 20:
        raise ConfigError(
            "TELEGRAM_BOT_TOKEN does not look like a Telegram bot token "
            "(expected format '<digits>:<chars>'). Check your .env."
        )

    try:
        chat_id = int(chat_id_raw)
    except ValueError as exc:
        raise ConfigError("TELEGRAM_CHAT_ID must be an integer") from exc
    if chat_id in _CHAT_ID_PLACEHOLDERS:
        raise ConfigError(
            "TELEGRAM_CHAT_ID is still the placeholder from .env.example. "
            "Put your real chat id (e.g. from @userinfobot) into .env."
        )

    yandex_key_raw = (os.environ.get("YANDEX_API_KEY") or "").strip()
    if yandex_key_raw.lower() in _YANDEX_PLACEHOLDERS:
        yandex_key = None
    else:
        yandex_key = yandex_key_raw

    poll_seconds = int(os.environ.get("POLL_INTERVAL_SECONDS", 60))
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    state_file = Path(os.environ.get("STATE_FILE", "state.json"))
    user_agent = os.environ.get(
        "HTTP_USER_AGENT",
        "weatherapp-telegram-bot (https://example.com; contact@example.com)",
    )

    forecast_days = int(os.environ.get("FORECAST_DAYS", 5))
    forecast_refresh = int(os.environ.get("FORECAST_REFRESH_SECONDS", 3600))

    llm_api_key = (os.environ.get("LLM_API_KEY") or "").strip() or None
    llm_base_url = (
        os.environ.get("LLM_BASE_URL") or "https://api.moonshot.ai/v1"
    ).strip()
    llm_model = (os.environ.get("LLM_MODEL") or "kimi-latest").strip()
    llm_timeout = int(os.environ.get("LLM_TIMEOUT", 120))

    telegram_proxy = (os.environ.get("TELEGRAM_PROXY") or "").strip() or None

    # OGIMET
    ogimet_enabled = os.environ.get("OGIMET_ENABLED", "true").lower() in ("1", "true", "yes")
    ogimet_station_id = os.environ.get("OGIMET_STATION_ID", "27611").strip()
    ogimet_interval = int(os.environ.get("OGIMET_INTERVAL_SECONDS", 600))
    ogimet_timeout = int(os.environ.get("OGIMET_TIMEOUT_SECONDS", 15))

    # IEM
    iem_enabled = os.environ.get("IEM_ENABLED", "true").lower() in ("1", "true", "yes")
    iem_station = os.environ.get("IEM_STATION", "UUWW").strip()
    iem_network = os.environ.get("IEM_NETWORK", "RU__ASOS").strip()
    iem_interval = int(os.environ.get("IEM_INTERVAL_SECONDS", 300))
    iem_timeout = int(os.environ.get("IEM_TIMEOUT_SECONDS", 15))

    # WIS 2.0
    wis2_enabled = os.environ.get("WIS2_ENABLED", "false").lower() in ("1", "true", "yes")
    wis2_broker = os.environ.get("WIS2_BROKER", "mqtts://globalbroker.meteo.fr:8883").strip()
    wis2_topic = os.environ.get(
        "WIS2_TOPIC",
        "origin/a/wis2/ru-roshydromet/data/core/weather/surface-based-observations/synop",
    ).strip()
    wis2_username = os.environ.get("WIS2_USERNAME", "everyone").strip()
    wis2_password = os.environ.get("WIS2_PASSWORD", "everyone").strip()
    wis2_wigos_id = os.environ.get("WIS2_WIGOS_ID", "0-20000-0-27611").strip()

    # DB & forecast pipeline
    db_path = Path(os.environ.get("DB_PATH", "weather.db"))
    forecast_detailed_refresh = int(os.environ.get("FORECAST_DETAILED_REFRESH_SECONDS", 3600))
    forecast_models_enabled = os.environ.get("FORECAST_MODELS_ENABLED", "true").lower() in ("1", "true", "yes")
    forecast_ensemble_enabled = os.environ.get("FORECAST_ENSEMBLE_ENABLED", "true").lower() in ("1", "true", "yes")
    met_norway_enabled = os.environ.get("MET_NORWAY_ENABLED", "true").lower() in ("1", "true", "yes")
    met_norway_interval = int(os.environ.get("MET_NORWAY_INTERVAL_SECONDS", 1800))
    taf_enabled = os.environ.get("TAF_ENABLED", "true").lower() in ("1", "true", "yes")
    taf_interval = int(os.environ.get("TAF_INTERVAL_SECONDS", 1800))
    meteoinfo_enabled = os.environ.get("METEOINFO_ENABLED", "false").lower() in ("1", "true", "yes")
    meteoinfo_interval = int(os.environ.get("METEOINFO_INTERVAL_SECONDS", 10800))

    # Notifications
    notify_min_interval = int(os.environ.get("NOTIFY_MIN_INTERVAL_SECONDS", 1800))

    # Alerts
    alerts_enabled = os.environ.get("ALERTS_ENABLED", "true").lower() in ("1", "true", "yes")
    alerts_config_path_raw = (os.environ.get("ALERTS_CONFIG_PATH") or "").strip()
    alerts_config_path = Path(alerts_config_path_raw) if alerts_config_path_raw else None
    webhook_url = (os.environ.get("WEBHOOK_URL") or "").strip() or None
    email_enabled = os.environ.get("EMAIL_ENABLED", "false").lower() in ("1", "true", "yes")

    return Config(
        telegram_bot_token=token,
        telegram_chat_id=chat_id,
        telegram_proxy=telegram_proxy,
        yandex_api_key=yandex_key,
        poll_interval_seconds=poll_seconds,
        log_level=log_level,
        state_file=state_file,
        http_user_agent=user_agent,
        forecast_days=forecast_days,
        forecast_refresh_seconds=forecast_refresh,
        llm_api_key=llm_api_key,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_timeout_seconds=llm_timeout,
        ogimet_enabled=ogimet_enabled,
        ogimet_station_id=ogimet_station_id,
        ogimet_interval_seconds=ogimet_interval,
        ogimet_timeout_seconds=ogimet_timeout,
        iem_enabled=iem_enabled,
        iem_station=iem_station,
        iem_network=iem_network,
        iem_interval_seconds=iem_interval,
        iem_timeout_seconds=iem_timeout,
        wis2_enabled=wis2_enabled,
        wis2_broker=wis2_broker,
        wis2_topic=wis2_topic,
        wis2_username=wis2_username,
        wis2_password=wis2_password,
        wis2_wigos_id=wis2_wigos_id,
        db_path=db_path,
        forecast_detailed_refresh_seconds=forecast_detailed_refresh,
        forecast_models_enabled=forecast_models_enabled,
        forecast_ensemble_enabled=forecast_ensemble_enabled,
        met_norway_enabled=met_norway_enabled,
        met_norway_interval_seconds=met_norway_interval,
        taf_enabled=taf_enabled,
        taf_interval_seconds=taf_interval,
        meteoinfo_enabled=meteoinfo_enabled,
        meteoinfo_interval_seconds=meteoinfo_interval,
        notify_min_interval_seconds=notify_min_interval,
        alerts_enabled=alerts_enabled,
        alerts_config_path=alerts_config_path,
        webhook_url=webhook_url,
        email_enabled=email_enabled,
    )
