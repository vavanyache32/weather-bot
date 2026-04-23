"""Runtime configuration loaded from environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


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
    poll_interval_seconds: int = 30 * 60  # 30 minutes
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
    llm_api_key: Optional[str] = None
    # Moonshot/Kimi defaults; OpenAI-compatible endpoints work unchanged.
    llm_base_url: str = "https://api.moonshot.ai/v1"
    llm_model: str = "kimi-latest"
    # Thinking models (kimi-k2.6, o1-style) can take 30–90s end-to-end on
    # non-trivial prompts. 120s gives headroom; auto-retries are disabled
    # in LLMService so one failure is bounded.
    llm_timeout_seconds: int = 120


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

    poll_seconds = int(os.environ.get("POLL_INTERVAL_SECONDS", 30 * 60))
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
    )
