@echo off
setlocal EnableExtensions
title Beelzebub - Production Paper Ledger Launcher

rem ============================================================
rem Beelzebub / BeezConsole production-ledger launcher
rem
rem Put this .bat file in the Trader project root, beside:
rem   beez_console.py
rem   main.py
rem   .venv312\
rem
rem This sets the ledger path only for Beelzebub and its children.
rem It does NOT permanently modify your Windows environment.
rem ============================================================

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "LEDGER=N:\Beelzebub\runtime\hot\lane_iii_paper.sqlite3"
set "PYTHON=%ROOT%\.venv312\Scripts\python.exe"
set "LAUNCHER=%ROOT%\beez_console.py"

echo.
echo ============================================================
echo   BEELZEBUB - PRODUCTION PAPER LEDGER
echo ============================================================
echo.
echo Project:
echo   %ROOT%
echo.
echo Ledger:
echo   %LEDGER%
echo.

rem --- Fail closed if required files are missing ----------------

if not exist "%LEDGER%" (
    echo [ERROR] Production paper ledger was not found.
    echo.
    echo Expected:
    echo   %LEDGER%
    echo.
    echo Beelzebub was NOT started.
    pause
    exit /b 10
)

if not exist "%PYTHON%" (
    echo [ERROR] Beelzebub Python 3.12 environment was not found.
    echo.
    echo Expected:
    echo   %PYTHON%
    echo.
    echo Beelzebub was NOT started.
    pause
    exit /b 11
)

if not exist "%LAUNCHER%" (
    echo [ERROR] BeezConsole launcher was not found.
    echo.
    echo Expected:
    echo   %LAUNCHER%
    echo.
    echo Put this batch file in the Trader project root.
    echo Beelzebub was NOT started.
    pause
    exit /b 12
)

rem --- A running backend cannot inherit this new environment ----
rem Refuse rather than silently attach to a possibly misconfigured
rem process already listening on BeezConsole port 8090.

powershell.exe -NoLogo -NoProfile -NonInteractive -Command ^
  "$c = Get-NetTCPConnection -LocalAddress 127.0.0.1 -LocalPort 8090 -State Listen -ErrorAction SilentlyContinue; if ($c) { exit 1 } else { exit 0 }"

if errorlevel 1 (
    echo [ERROR] Something is already listening on 127.0.0.1:8090.
    echo.
    echo If that is an existing BeezConsole backend, it may have been
    echo started WITHOUT the production ledger environment variable.
    echo.
    echo Close/stop the existing Beelzebub backend, then run this file
    echo again. This launcher refuses to guess.
    echo.
    echo Beelzebub was NOT started by this launcher.
    pause
    exit /b 13
)

rem --- Point this Beelzebub process tree at the production ledger

set "BEELZEBUB_L3G_PAPER_LEDGER=%LEDGER%"

echo [OK] BEELZEBUB_L3G_PAPER_LEDGER is set.
echo.

rem Show ledger size for an operator sanity check.
powershell.exe -NoLogo -NoProfile -NonInteractive -Command ^
  "$f = Get-Item -LiteralPath $env:BEELZEBUB_L3G_PAPER_LEDGER; Write-Host ('Ledger size: {0:N2} GiB  ({1:N0} bytes)' -f ($f.Length / 1GB), $f.Length); Write-Host ('Last write : {0}' -f $f.LastWriteTime)"

echo.
echo Starting BeezConsole...
echo.

pushd "%ROOT%"
"%PYTHON%" "%LAUNCHER%"
set "RC=%ERRORLEVEL%"
popd

if not "%RC%"=="0" (
    echo.
    echo [ERROR] BeezConsole launcher returned exit code %RC%.
    echo Check:
    echo   %ROOT%\logs\beez-console-server.log
    echo.
    pause
    exit /b %RC%
)

echo.
echo [OK] BeezConsole launch request completed.
echo     Backend children inherited:
echo       BEELZEBUB_L3G_PAPER_LEDGER=%LEDGER%
echo.
echo You may close this window.
timeout /t 3 /nobreak >nul
exit /b 0
