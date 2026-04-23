@echo off
REM ============================================================
REM  run_bot.bat - launcher used by Task Scheduler "WeatherBot"
REM ------------------------------------------------------------
REM  * Uses pythonw.exe so no console window is attached.
REM    This avoids spurious STATUS_CONTROL_C_EXIT (0xC000013A)
REM    when Task Scheduler runs the task hidden.
REM  * Waits for network connectivity before the first launch.
REM  * Infinite retry loop with exponential-ish backoff - if
REM    Python exits for any reason (crash, network blip, OS
REM    signal), we restart it. The only stop condition is
REM    rc==2 (ConfigError), which requires human intervention.
REM  * All application logging is handled by the bot itself via
REM    bot/logging_config.py (RotatingFileHandler -> bot.log).
REM    We also append stdout/stderr to bot.log as a safety net
REM    for crashes that happen before the logger is set up.
REM ============================================================

setlocal EnableExtensions EnableDelayedExpansion
cd /d "%~dp0"

set "PYTHONIOENCODING=utf-8"
set "PYTHONUTF8=1"

if not exist ".venv\Scripts\pythonw.exe" (
    >>bot.log echo [%date% %time%] ERROR: .venv missing, run start.bat once to bootstrap
    exit /b 1
)

REM ---- Wait for network (up to ~2 minutes) ------------------
set /a NETTRIES=0
:WAITNET
ping -n 1 -w 1000 api.telegram.org >nul 2>&1
if not errorlevel 1 goto NETOK
set /a NETTRIES+=1
if %NETTRIES% GEQ 24 (
    >>bot.log echo [%date% %time%] network still unreachable after ~2min, launching anyway
    goto NETOK
)
REM 5-second wait between pings
ping -n 6 127.0.0.1 >nul
goto WAITNET
:NETOK

REM ---- Infinite restart loop --------------------------------
set /a FAILS=0
:RUNLOOP
>>bot.log echo [%date% %time%] --- bot starting (attempt=%FAILS%) ---
REM pythonw.exe is a Windows-subsystem binary; from a bat/cmd console
REM it does NOT block by default. "start /B /WAIT" makes cmd wait for
REM the process to exit and captures its ERRORLEVEL.
start "" /B /WAIT ".venv\Scripts\pythonw.exe" -m bot
set "RC=%ERRORLEVEL%"
>>bot.log echo [%date% %time%] --- bot exited rc=%RC% ---

if "%RC%"=="2" (
    >>bot.log echo [%date% %time%] config error rc=2, stopping retry loop.
    exit /b 2
)

set /a FAILS+=1
set "WAIT=5"
if %FAILS% GEQ 2 set "WAIT=10"
if %FAILS% GEQ 3 set "WAIT=20"
if %FAILS% GEQ 4 set "WAIT=40"
if %FAILS% GEQ 5 set "WAIT=60"
>>bot.log echo [%date% %time%] sleeping %WAIT%s before restart
ping -n %WAIT% 127.0.0.1 >nul
goto RUNLOOP
