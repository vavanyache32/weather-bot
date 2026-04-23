@echo off
REM ===========================================================
REM  Weatherapp Telegram bot launcher (Windows)
REM  - creates .venv on first run
REM  - installs / updates requirements.txt
REM  - checks that .env exists
REM  - runs: python -m bot
REM ===========================================================

setlocal EnableExtensions
cd /d "%~dp0"

REM --- pick a Python launcher: prefer 'py', fall back to 'python'
set "PYLAUNCH=py -3"
where py >nul 2>nul
if errorlevel 1 (
    set "PYLAUNCH=python"
    where python >nul 2>nul
    if errorlevel 1 (
        echo [ERROR] Python is not installed or not on PATH.
        echo         Install Python 3.10+ from https://www.python.org/downloads/
        goto :fail
    )
)

REM --- create venv if missing
if not exist ".venv\Scripts\python.exe" (
    echo [setup] Creating virtual environment in .venv ...
    %PYLAUNCH% -m venv .venv
    if errorlevel 1 goto :fail
)

set "VENV_PY=.venv\Scripts\python.exe"

REM --- install / update deps (quiet; skip if already satisfied)
echo [setup] Installing requirements ...
"%VENV_PY%" -m pip install --disable-pip-version-check -q -r requirements.txt
if errorlevel 1 goto :fail

REM --- require .env
if not exist ".env" (
    echo [ERROR] .env not found.
    echo         Copy .env.example to .env and fill in:
    echo           TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, YANDEX_API_KEY
    goto :fail
)

REM --- make sure emojis print fine in the console
set "PYTHONIOENCODING=utf-8"
set "PYTHONUTF8=1"
chcp 65001 >nul 2>nul

echo [run] Starting bot (Ctrl+C to stop) ...
"%VENV_PY%" -m bot
set "EXITCODE=%ERRORLEVEL%"

echo.
echo [done] Bot exited with code %EXITCODE%
pause
endlocal & exit /b %EXITCODE%

:fail
echo.
pause
endlocal & exit /b 1
