@echo off
REM -*- coding: utf-8 -*-
REM realtime_pipeline/manage_processes.bat
REM
REM Script quan ly processes tren Windows
REM Su dung: manage_processes.bat [start|stop|status]

setlocal enabledelayedexpansion

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
set "LOGS_DIR=%ROOT_DIR%\logs"

REM Colors (Windows 10+)
set "GREEN=[92m"
set "RED=[91m"
set "YELLOW=[93m"
set "BLUE=[94m"
set "NC=[0m"

REM Default settings
set "DEFAULT_TICKER=SHB"
set "DEFAULT_INTERVAL=1800"

REM Tao logs directory
if not exist "%LOGS_DIR%" mkdir "%LOGS_DIR%"

if "%1"=="" goto status
if /i "%1"=="start" goto start
if /i "%1"=="stop" goto stop
if /i "%1"=="restart" goto restart
if /i "%1"=="status" goto status
goto usage

:start
echo.
echo %BLUE%======================================================%NC%
echo %BLUE%       Starting FinSent-Agent Realtime System       %NC%
echo %BLUE%======================================================%NC%
echo.

echo %BLUE%[1/4]%NC% Starting Scheduler...
cd /d "%ROOT_DIR%"
start /B "FinSent-Scheduler" python realtime_pipeline\scheduler.py --ticker %DEFAULT_TICKER% --interval %DEFAULT_INTERVAL% > "%LOGS_DIR%\scheduler.log" 2>&1
timeout /t 2 /nobreak >nul
echo   %GREEN%Started%NC% - PID saved, logs: logs\scheduler.log

echo.
echo %BLUE%[2/4]%NC% Starting Vector Worker...
start /B "FinSent-VectorWorker" python realtime_pipeline\run_vector_worker.py > "%LOGS_DIR%\vector_worker.log" 2>&1
timeout /t 2 /nobreak >nul
echo   %GREEN%Started%NC% - logs: logs\vector_worker.log

echo.
echo %BLUE%[3/4]%NC% Starting Dashboard Demo (port 8501)...
start /B "FinSent-DashDemo" streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0 --server.headless true > "%LOGS_DIR%\dashboard_demo.log" 2>&1
timeout /t 3 /nobreak >nul
echo   %GREEN%Started%NC% - http://localhost:8501

echo.
echo %BLUE%[4/4]%NC% Starting Dashboard Realtime (port 8502)...
start /B "FinSent-DashRT" streamlit run dashboard_realtime.py --server.port 8502 --server.address 0.0.0.0 --server.headless true > "%LOGS_DIR%\dashboard_rt.log" 2>&1
timeout /t 3 /nobreak >nul
echo   %GREEN%Started%NC% - http://localhost:8502

echo.
echo %GREEN%======================================================%NC%
echo %GREEN%All processes started!%NC%
echo %GREEN%======================================================%NC%
echo.
echo Access dashboards:
echo   Demo:     http://localhost:8501
echo   Realtime: http://localhost:8502
echo.
echo View logs:
echo   manage_processes.bat status
echo.
goto end

:stop
echo.
echo %RED%======================================================%NC%
echo %RED%       Stopping FinSent-Agent Processes             %NC%
echo %RED%======================================================%NC%
echo.

REM Stop by window title
taskkill /FI "WINDOWTITLE eq FinSent-Scheduler*" /F >nul 2>&1
echo %BLUE%[1/4]%NC% Scheduler stopped

taskkill /FI "WINDOWTITLE eq FinSent-VectorWorker*" /F >nul 2>&1
echo %BLUE%[2/4]%NC% Vector Worker stopped

taskkill /FI "WINDOWTITLE eq FinSent-DashDemo*" /F >nul 2>&1
echo %BLUE%[3/4]%NC% Dashboard Demo stopped

taskkill /FI "WINDOWTITLE eq FinSent-DashRT*" /F >nul 2>&1
echo %BLUE%[4/4]%NC% Dashboard Realtime stopped

REM Also kill by process name (fallback)
taskkill /IM python.exe /F /FI "WINDOWTITLE eq FinSent*" >nul 2>&1
taskkill /IM streamlit.exe /F /FI "WINDOWTITLE eq FinSent*" >nul 2>&1

echo.
echo %RED%======================================================%NC%
echo %RED%All processes stopped!%NC%
echo %RED%======================================================%NC%
echo.
goto end

:status
echo.
echo %BLUE%======================================================%NC%
echo %BLUE%       FinSent-Agent Process Status                 %NC%
echo %BLUE%======================================================%NC%
echo.

REM Check processes
tasklist /FI "WINDOWTITLE eq FinSent-Scheduler*" 2>nul | find /I "python.exe" >nul
if %ERRORLEVEL%==0 (
    echo %BLUE%[1/4]%NC% Scheduler:       %GREEN%Running%NC%
) else (
    echo %BLUE%[1/4]%NC% Scheduler:       %YELLOW%Not running%NC%
)

tasklist /FI "WINDOWTITLE eq FinSent-VectorWorker*" 2>nul | find /I "python.exe" >nul
if %ERRORLEVEL%==0 (
    echo %BLUE%[2/4]%NC% Vector Worker:   %GREEN%Running%NC%
) else (
    echo %BLUE%[2/4]%NC% Vector Worker:   %YELLOW%Not running%NC%
)

tasklist /FI "WINDOWTITLE eq FinSent-DashDemo*" 2>nul | find /I "python.exe" >nul
if %ERRORLEVEL%==0 (
    echo %BLUE%[3/4]%NC% Dashboard Demo:  %GREEN%Running%NC% - http://localhost:8501
) else (
    echo %BLUE%[3/4]%NC% Dashboard Demo:  %YELLOW%Not running%NC%
)

tasklist /FI "WINDOWTITLE eq FinSent-DashRT*" 2>nul | find /I "python.exe" >nul
if %ERRORLEVEL%==0 (
    echo %BLUE%[4/4]%NC% Dashboard RT:    %GREEN%Running%NC% - http://localhost:8502
) else (
    echo %BLUE%[4/4]%NC% Dashboard RT:    %YELLOW%Not running%NC%
)

echo.
echo %BLUE%Log files:%NC%
echo   Scheduler:      %LOGS_DIR%\scheduler.log
echo   Vector Worker:  %LOGS_DIR%\vector_worker.log
echo   Dashboard Demo: %LOGS_DIR%\dashboard_demo.log
echo   Dashboard RT:   %LOGS_DIR%\dashboard_rt.log
echo.
goto end

:restart
call :stop
timeout /t 2 /nobreak >nul
call :start
goto end

:usage
echo.
echo Usage: %~nx0 [start^|stop^|restart^|status]
echo.
echo Commands:
echo   start    - Start all processes
echo   stop     - Stop all processes
echo   restart  - Restart all processes
echo   status   - Show process status
echo.
goto end

:end
endlocal
