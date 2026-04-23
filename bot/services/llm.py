"""Optional LLM-powered analysis of the forecast bundle.

The LLM only **annotates** numbers we already have — it never invents a
forecast. Designed around an OpenAI-compatible Chat Completions API so
that Moonshot (Kimi), OpenAI, DeepSeek, Together, local llama.cpp
servers, etc. are all drop-in with just an env var swap.

Required env:
  LLM_API_KEY     — API key for the provider
Optional env (sensible defaults for Moonshot/Kimi):
  LLM_BASE_URL    — default https://api.moonshot.ai/v1
  LLM_MODEL       — default kimi-latest
  LLM_TIMEOUT     — default 30 (seconds)

If LLM_API_KEY is missing, the service is silently disabled and
``analyze(...)`` returns ``None`` — the rest of the bot keeps working.
"""
from __future__ import annotations

import logging
from typing import Optional

from .forecast import DailyMaxForecast, ForecastBundle

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "Ты — ассистент погодного бота по Москве (Внуково, UUWW). "
    "Тебе дают РЕАЛЬНЫЕ числа из численных моделей (Open-Meteo) и вендорского "
    "прогноза (Yandex), плюс текущие наблюдения METAR. "
    "Твоя задача — написать 2–4 коротких предложения по-русски, "
    "комментируя эти числа: тренд, расхождение моделей, уровень уверенности, "
    "контекст для рынка Polymarket (максимум за сутки по METAR в целых °C). "
    "Строгие правила: "
    "1) НЕ ВЫДУМЫВАЙ числа — используй только те, что переданы ниже. "
    "2) Если модели расходятся на ≥3°C — прямо скажи, что уверенность низкая. "
    "3) Не используй markdown и эмодзи. Просто связный текст."
)


class LLMService:
    def __init__(
        self,
        api_key: Optional[str],
        base_url: str,
        model: str,
        timeout_seconds: int = 30,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url
        self._model = model
        self._timeout = timeout_seconds
        self._client = None  # lazy init so import stays cheap if disabled

    @property
    def enabled(self) -> bool:
        return bool(self._api_key)

    def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            from openai import AsyncOpenAI  # imported lazily
        except ImportError as exc:
            logger.error(
                "LLM is configured but the 'openai' package is not installed: %s",
                exc,
            )
            return None
        # max_retries=0: thinking models (kimi-k2.6, o1, etc.) have high,
        # variable inference latency. If one call times out, retrying
        # doesn't make the server faster — it just triples the wait.
        # Fail fast, try again next refresh cycle.
        self._client = AsyncOpenAI(
            api_key=self._api_key,
            base_url=self._base_url,
            timeout=self._timeout,
            max_retries=0,
        )
        return self._client

    async def _execute_prompt(self, prompt: str) -> Optional[str]:
        """Send prompt to LLM and return text, or None on failure."""
        if not self.enabled:
            logger.warning("LLM disabled (no API key)")
            return None
        client = self._get_client()
        if client is None:
            logger.warning("LLM client init failed (openai package missing?)")
            return None

        logger.info("LLM prompt length=%d, model=%s", len(prompt), self._model)
        try:
            resp = await client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                # kimi-k2.* is a thinking model that only accepts temperature=1
                # (rejects anything else with 400). 1.0 is also OpenAI's default,
                # so it's safe for OpenAI/DeepSeek/other providers.
                temperature=1,
                # Thinking models burn tokens on reasoning_content before
                # emitting message.content. On our forecast prompt kimi-k2.6
                # can spend 2000+ tokens just reasoning; if the cap hits
                # during reasoning, the final answer comes back empty.
                # 5000 gives plenty of headroom for reasoning + 2–4 sentences.
                max_tokens=5000,
            )
        except Exception:
            logger.exception("LLM analysis call failed")
            return None

        try:
            text = (resp.choices[0].message.content or "").strip()
        except (AttributeError, IndexError):
            logger.warning("LLM response had unexpected shape: %r", resp)
            return None

        if not text:
            # Thinking models sometimes emit only reasoning_content and an empty
            # final content (usually when max_tokens is still too tight). Log
            # clearly rather than silently showing nothing in /forecast.
            logger.warning(
                "LLM returned empty content (usage=%s). "
                "Raise max_tokens or check the model is not stuck in reasoning.",
                getattr(resp, "usage", None),
            )
            return None

        logger.info("LLM response length=%d", len(text))
        return text

    async def analyze(
        self,
        bundle: ForecastBundle,
        noaa_temp_c: Optional[int],
        daily_max_so_far_c: Optional[int],
        predicted_30min_c: Optional[int],
    ) -> Optional[str]:
        """Return a short RU narrative for the whole bundle, or None."""
        prompt = self._build_prompt(
            bundle=bundle,
            noaa_temp_c=noaa_temp_c,
            daily_max_so_far_c=daily_max_so_far_c,
            predicted_30min_c=predicted_30min_c,
        )
        return await self._execute_prompt(prompt)

    async def analyze_day(
        self,
        day: DailyMaxForecast,
        noaa_temp_c: Optional[int],
        daily_max_so_far_c: Optional[int],
        predicted_30min_c: Optional[int],
    ) -> Optional[str]:
        """Return a short RU narrative for a single day, or None."""
        prompt = self._build_day_prompt(
            day=day,
            noaa_temp_c=noaa_temp_c,
            daily_max_so_far_c=daily_max_so_far_c,
            predicted_30min_c=predicted_30min_c,
        )
        return await self._execute_prompt(prompt)

    @staticmethod
    def _build_prompt(
        bundle: ForecastBundle,
        noaa_temp_c: Optional[int],
        daily_max_so_far_c: Optional[int],
        predicted_30min_c: Optional[int],
    ) -> str:
        lines: list[str] = []
        lines.append("Текущие наблюдения (NOAA METAR, UUWW):")
        lines.append(
            f"  сейчас: {noaa_temp_c}°C"
            if noaa_temp_c is not None
            else "  сейчас: нет данных"
        )
        lines.append(
            f"  макс за сегодня по METAR: {daily_max_so_far_c}°C"
            if daily_max_so_far_c is not None
            else "  макс за сегодня: пока нет данных"
        )
        if predicted_30min_c is not None:
            lines.append(f"  интерполированный прогноз +30 мин: {predicted_30min_c}°C")

        lines.append("")
        lines.append("Прогноз максимума по дням (целые °C):")
        if bundle.days:
            for d in bundle.days:
                lines.append(_format_day_for_prompt(d))
        else:
            lines.append("  нет данных")

        lines.append("")
        lines.append(
            "Сделай короткий вывод: ожидаемый максимум сегодня, "
            "насколько согласны модели, и что это означает для Polymarket-рынка "
            "'Highest temperature today'."
        )
        return "\n".join(lines)

    @staticmethod
    def _build_day_prompt(
        day: DailyMaxForecast,
        noaa_temp_c: Optional[int],
        daily_max_so_far_c: Optional[int],
        predicted_30min_c: Optional[int],
    ) -> str:
        lines: list[str] = []
        lines.append("Текущие наблюдения (NOAA METAR, UUWW):")
        lines.append(
            f"  сейчас: {noaa_temp_c}°C"
            if noaa_temp_c is not None
            else "  сейчас: нет данных"
        )
        lines.append(
            f"  макс за сегодня по METAR: {daily_max_so_far_c}°C"
            if daily_max_so_far_c is not None
            else "  макс за сегодня: пока нет данных"
        )
        if predicted_30min_c is not None:
            lines.append(f"  интерполированный прогноз +30 мин: {predicted_30min_c}°C")

        lines.append("")
        lines.append(f"Прогноз максимума на {day.date.isoformat()}:")
        parts = []
        if day.open_meteo_c is not None:
            parts.append(f"Open-Meteo {day.open_meteo_c}°C")
        if day.yandex_c is not None:
            parts.append(f"Yandex {day.yandex_c}°C")
        if day.spread_c is not None:
            parts.append(f"(разброс моделей {day.spread_c}°C)")
        lines.append("  " + " ".join(parts) if parts else "  нет данных")

        lines.append("")
        lines.append(
            f"Сделай короткий вывод по прогнозу на {day.date.isoformat()}: "
            "ожидаемый максимум, насколько согласны модели, уровень уверенности."
        )
        return "\n".join(lines)


def _format_day_for_prompt(d: DailyMaxForecast) -> str:
    parts = [f"  {d.date.isoformat()}:"]
    if d.open_meteo_c is not None:
        parts.append(f"Open-Meteo {d.open_meteo_c}°C")
    if d.yandex_c is not None:
        parts.append(f"Yandex {d.yandex_c}°C")
    if d.spread_c is not None:
        parts.append(f"(spread {d.spread_c}°C)")
    return " ".join(parts)
