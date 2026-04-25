"""Async SQLite persistence layer for forecasts, alerts and skill scores.

Uses aiosqlite for an async API over a local SQLite file.
Tables:
  * forecasts       — individual ForecastPoint rows
  * alert_log       — sent notifications with cooldown tracking
  * forecast_skill  — MAE/RMSE/bias per (source, model, lead_day)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("weather.db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS forecasts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    model TEXT,
    member INTEGER,
    station TEXT NOT NULL,
    lat REAL,
    lon REAL,
    issued_at TEXT NOT NULL,
    valid_at TEXT NOT NULL,
    lead_time_h INTEGER NOT NULL,
    air_temperature_c REAL,
    daily_tmax_c REAL,
    daily_tmin_c REAL,
    quantile REAL,
    raw TEXT,
    UNIQUE(source, model, member, valid_at, issued_at)
);
CREATE INDEX IF NOT EXISTS idx_forecasts_lookup
    ON forecasts(station, valid_at, source, model);
CREATE INDEX IF NOT EXISTS idx_forecasts_issued
    ON forecasts(issued_at);

CREATE TABLE IF NOT EXISTS alert_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id TEXT NOT NULL,
    station TEXT NOT NULL,
    valid_date TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    message TEXT,
    is_recovery BOOLEAN DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_alerts_lookup
    ON alert_log(alert_id, station, valid_date, sent_at);

CREATE TABLE IF NOT EXISTS forecast_skill (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    model TEXT,
    lead_day INTEGER NOT NULL,
    metric TEXT NOT NULL,
    window TEXT,
    value REAL NOT NULL,
    n INTEGER,
    computed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_skill_lookup
    ON forecast_skill(source, model, lead_day, metric, window);
"""


async def init_db(db_path: Optional[Path] = None) -> aiosqlite.Connection:
    """Open (or create) the SQLite DB, run schema if needed, return connection."""
    path = db_path or DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(path)
    conn.row_factory = aiosqlite.Row
    await conn.executescript(_SCHEMA)
    await conn.commit()
    # Enable WAL mode for better concurrent read/write performance.
    await conn.execute("PRAGMA journal_mode=WAL")
    logger.info("SQLite DB ready at %s", path)
    return conn
