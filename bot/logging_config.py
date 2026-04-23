"""Centralised logging configuration."""
from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path


_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Log file lives next to the project root (two levels up from this file:
# bot/logging_config.py -> bot/ -> <project root>).
_LOG_FILE = Path(__file__).resolve().parent.parent / "bot.log"


def _stdout_is_usable() -> bool:
    """Return True if sys.stdout is a real, writable stream.

    Under ``pythonw.exe`` (no console) sys.stdout is None, and writing to it
    would crash the logger. We also guard against the MSYS/Git-Bash case where
    stdout's file descriptor is invalid for Windows flush calls.
    """
    stream = sys.stdout
    if stream is None:
        return False
    try:
        fd = stream.fileno()
    except (OSError, ValueError, AttributeError):
        return False
    return fd >= 0


def setup_logging(level: str = "INFO") -> None:
    """Configure root logging once for the whole process.

    Always attaches a RotatingFileHandler writing to ``<project>/bot.log`` so
    logs survive regardless of how the process is launched (Task Scheduler with
    pythonw.exe, detached from any console, etc.). A StreamHandler is added
    only when stdout is actually usable.
    """
    root = logging.getLogger()
    if root.handlers:
        # Already configured (e.g. reimport); just update level.
        root.setLevel(getattr(logging, level.upper(), logging.INFO))
        return

    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # File handler: 5 MB x 3 backups, appended, UTF-8.
    try:
        _LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            _LOG_FILE,
            maxBytes=5 * 1024 * 1024,
            backupCount=3,
            encoding="utf-8",
            delay=True,
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    except OSError:
        # If we can't open the log file for any reason, keep going silently -
        # stdout handler (if usable) will still produce visible output.
        pass

    if _stdout_is_usable():
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        root.addHandler(stream_handler)

    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Tame overly chatty third-party loggers.
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
