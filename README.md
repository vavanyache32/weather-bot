# Moscow (UUWW) Temperature Telegram Bot

Production-ready async Telegram bot that tracks temperature at **Moscow /
Vnukovo airport (ICAO: UUWW)** and pushes updates every 30 minutes.

* **Primary source** — NOAA / NWS **Aviation Weather Center** METAR JSON
  feed for station `UUWW`:
  `https://aviationweather.gov/api/data/metar?ids=UUWW&format=json`.
  The `api.weather.gov/stations/UUWW` endpoint that is sometimes cited
  does not exist — NWS's station catalogue is US-only. AWC METAR is the
  same upstream data (`09/M03` → 9 °C) in whole degrees, and matches
  [Polymarket][pm] resolution rules (daily **max**, not average, not
  latest).
* **Secondary source** — Yandex.Weather `api.weather.yandex.ru/v2/forecast`
  for Moscow coordinates, shown alongside NOAA for comparison.
* **New observation sources** (added to close coverage gaps and obtain
  explicit daily Tmax from meteorological reports):
  * **OGIMET SYNOP** — station `27611` (Vnukovo). Fetched every 10 min.
    Provides explicit daily max/min via section 333 of SYNOP bulletins.
  * **IEM ASOS** — `mesonet.agron.iastate.edu` aggregator for `UUWW`.
    Fetched every 5 min. Provides `max_dayairtemp` (explicit daily max)
    alongside current METAR temperature.
  * **WIS 2.0 MQTT** (optional, disabled by default) — skeleton prepared
    for future push ingestion of BUFR data from the WMO Global Broker.
    Requires `paho-mqtt` + `eccodes` to complete.

[pm]: https://polymarket.com/

Notifications are sent **only** when:

* the rounded NOAA temperature changed since the previous poll, **or**
* a new daily maximum was reached.

If NOAA is unavailable, the bot still notifies on Yandex changes so the
user isn't left in the dark.

---

## Project layout

```
weatherapp/
├── bot/
│   ├── __init__.py
│   ├── __main__.py           # entry point: python -m bot
│   ├── config.py             # .env → Config dataclass
│   ├── handlers.py           # /start, /status
│   ├── logging_config.py
│   ├── scheduler.py          # polling loop + notification logic
│   ├── storage.py            # JSON-backed state store
│   └── services/
│       ├── __init__.py
│       ├── noaa.py           # api.weather.gov client
│       └── yandex.py         # api.weather.yandex.ru client
├── .env.example
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Clone / copy the project

```bash
cd weatherapp
```

### 2. Create a virtual environment and install deps

Linux / macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows (PowerShell):

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Configure environment

Copy the template and fill it in:

```bash
cp .env.example .env
```

Required:

| Variable              | Description                                        |
|-----------------------|----------------------------------------------------|
| `TELEGRAM_BOT_TOKEN`  | Token from [@BotFather](https://t.me/BotFather).   |
| `TELEGRAM_CHAT_ID`    | Target chat ID for scheduled updates.              |
| `YANDEX_API_KEY`      | Yandex.Weather API key (optional but recommended). |

Finding your chat ID: DM the bot, then visit
`https://api.telegram.org/bot<TOKEN>/getUpdates`, or use
[@userinfobot](https://t.me/userinfobot).

### 4. Run

```bash
python -m bot
```

You should see logs like:

```
2026-04-22 21:24:31 [INFO] bot: Starting weather bot: station=UUWW chat_id=...
2026-04-22 21:24:31 [INFO] bot.scheduler: Scheduler starting: station=UUWW interval=1800s tz=Europe/Moscow
2026-04-22 21:24:32 [INFO] bot.services.noaa: NOAA METAR: station=UUWW temp=9°C obsTime=2026-04-22T22:00:00.000Z raw=METAR UUWW 222200Z 26005MPS 9999 -RA OVC048 09/M03 Q0992 ...
2026-04-22 21:24:32 [INFO] bot.services.yandex: Yandex observation: 8.0°C ...
2026-04-22 21:24:32 [INFO] bot.scheduler: Sent update: 🌡 NOAA (Vnukovo): 8°C | 📈 Max today: 8°C | 🟡 Yandex: 8°C
```

---

## Commands

| Command     | Description                                                       |
|-------------|-------------------------------------------------------------------|
| `/start`    | Greeting + inline menu (Status / Forecast / About).               |
| `/status`   | Shows last NOAA temp, today's max, Yandex temp, +30min prediction.|
| `/forecast` | Ensemble daily-max forecast + optional LLM analysis.              |

## Ensemble forecast (`/forecast`)

* **Open-Meteo** (free, no key) — seamless NWP mix (ECMWF/GFS/ICON) via
  `api.open-meteo.com/v1/forecast`.
* **Yandex** — `forecasts[].parts.day.temp_max` from the same
  `/v2/forecast` response we already fetch.

Per day we show each source's rounded max plus a spread indicator:

* spread 0–2 °C → 🟢 high confidence
* spread 3–4 °C → 🟡 medium confidence
* spread ≥ 5 °C → 🔴 low confidence

The ensemble is refreshed once per hour by default
(`FORECAST_REFRESH_SECONDS`) because NWP models themselves update hourly.

## Optional LLM commentary

If `LLM_API_KEY` is set, every forecast refresh also runs a short LLM
prompt that adds a 2–4 sentence Russian narrative ("модели расходятся…",
"для рынка Polymarket X °C вероятность…"). The LLM **never** predicts
numbers — it only annotates the ensemble values; the prompt explicitly
forbids it from inventing anything.

Defaults target **Moonshot / Kimi** (OpenAI-compatible):

```env
LLM_BASE_URL=https://api.moonshot.ai/v1
LLM_MODEL=kimi-latest
LLM_API_KEY=<from moonshot dashboard>
```

Drop-in alternatives — just change the three env vars:

| Provider | `LLM_BASE_URL` | `LLM_MODEL` |
|----------|----------------|-------------|
| OpenAI | `https://api.openai.com/v1` | `gpt-4o-mini` |
| DeepSeek | `https://api.deepseek.com/v1` | `deepseek-chat` |
| Local llama.cpp / Ollama (OpenAI-compat) | `http://localhost:11434/v1` | `llama3.1` |
| Moonshot (China zone) | `https://api.moonshot.cn/v1` | `moonshot-v1-8k` |

If the key is missing or empty, the bot runs without the 🧠 Анализ block;
everything else works.


---

## Behavior details

### Polymarket resolution semantics

* NOAA temperature is read from the `temp` field of the latest AWC METAR
  JSON entry (i.e. the decoded temperature group of the raw METAR such
  as `09/M03`). It is already published in whole degrees Celsius.
* We still round defensively to the nearest whole °C before storing /
  comparing, so the pipeline is robust if upstream ever switches to
  decimals.
* `daily_max_c` is the **maximum** rounded NOAA METAR reading observed
  since local midnight in `Europe/Moscow`, and is reset on date
  rollover — matching Polymarket's "highest temperature today" markets.

### Resilience

* HTTP calls retry up to 3× with exponential backoff.
* Per-tick exceptions are caught and logged; one bad poll does not stop
  the loop.
* State is persisted to `state.json` atomically (write to `.tmp` +
  `rename`) so restarts keep today's max and avoid duplicate messages.
* If a source returns `null`, it is skipped (no spam, no crash).

### New observation sources

| Source | Cadence | Explicit Tmax | Notes |
|---|---|---|---|
| NOAA METAR (AWC) | every 1 min | no | Point-in-time METAR temp; bot tracks running max. |
| Yandex.Weather | every 10 min | no | Current temp + hourly forecast. |
| **OGIMET SYNOP** | every 10 min | **yes** | Station `27611`; parsed via `synop2bufr`. |
| **IEM ASOS** | every 5 min | **yes** | Aggregates METAR+SPECI; exposes `max_dayairtemp`. |
| WIS 2.0 MQTT | push (real-time) | **yes** | Skeleton only — requires `paho-mqtt` + `eccodes`. |

The scheduler deduplicates by `(source, station, observed_at)` and computes
the daily maximum with the following priority:

1. **Explicit max** (`max_temperature_c`) from OGIMET or IEM — authoritative.
2. If no explicit max is available, the highest point observation
   (`air_temperature_c`) across all sources is used.

### Why this tracks *max*, not latest

Polymarket "Highest temperature in Moscow today" markets resolve on the
single highest rounded °C observed at UUWW during the local day, per the
latest NOAA observation feed. The bot matches that exact rule, so its
`Max today` line can be used as an early signal for those markets.

---

## Deployment tips

* Run under a process supervisor (`systemd`, `supervisord`, Docker
  restart policy, etc.) — the bot self-recovers from transient errors
  but not from process death.
* Keep `state.json` on a persistent volume.
* NOAA requires a descriptive `User-Agent` with contact info; override
  the default via `HTTP_USER_AGENT` for production.
* The Yandex.Weather public API has limited availability for new
  customers; if your key is invalid, the bot will log a warning and
  continue to run on NOAA only.

---

## Development

Smoke test that everything imports and the scheduler message formatter
works without touching the network:

```bash
python -c "from bot.scheduler import format_update_message; from datetime import datetime; from zoneinfo import ZoneInfo; dt = datetime(2026,4,23,19,0,tzinfo=ZoneInfo('Europe/Moscow')); print(format_update_message(9, 10, 8, 7, True, now_local=dt))"
```

Expected:

```
⏰ 19:00 (МСК)
🌡 NOAA (Vnukovo): 9°C
📈 Max today: 10°C
🟡 Yandex: 8°C
🔮 Через 30 мин: 7°C (Yandex)
🔥 NEW DAILY MAX!
```
