@echo off
setlocal EnableExtensions EnableDelayedExpansion
title Beelzebub - Clear BeezConsole Port 8090

rem ============================================================
rem Beelzebub / BeezConsole stale-backend cleanup
rem
rem PURPOSE:
rem   Clear 127.0.0.1:8090 before launching a newly changed
rem   BeezConsole backend.
rem
rem SAFETY:
rem   - Only targets the PID currently LISTENING on port 8090.
rem   - Inspects the Windows process command line first.
rem   - Refuses to kill an unrelated process.
rem   - Uses taskkill /T to remove the stale backend process tree.
rem
rem DO NOT use this during normal trading/commissioning operations.
rem ============================================================

echo.
echo ============================================================
echo   BEELZEBUB - CLEAR STALE BEEZCONSOLE BACKEND
echo ============================================================
echo.
echo This utility is intended for maintenance / code-change restarts.
echo It will inspect the process LISTENING on 127.0.0.1:8090.
echo.

set "PSFILE=%TEMP%\beelzebub_clear_8090_%RANDOM%_%RANDOM%.ps1"

> "%PSFILE%" echo $ErrorActionPreference = 'Stop'
>>"%PSFILE%" echo $conn = Get-NetTCPConnection -LocalPort 8090 -State Listen -ErrorAction SilentlyContinue ^| Where-Object { $_.LocalAddress -in @('127.0.0.1','0.0.0.0','::1','::') } ^| Select-Object -First 1
>>"%PSFILE%" echo if (-not $conn) {
>>"%PSFILE%" echo   Write-Host '[OK] Nothing is listening on port 8090.'
>>"%PSFILE%" echo   exit 0
>>"%PSFILE%" echo }
>>"%PSFILE%" echo $pidValue = [int]$conn.OwningProcess
>>"%PSFILE%" echo $proc = Get-CimInstance Win32_Process -Filter ("ProcessId={0}" -f $pidValue)
>>"%PSFILE%" echo if (-not $proc) {
>>"%PSFILE%" echo   Write-Host ('[ERROR] Port 8090 is held by PID {0}, but process metadata could not be read.' -f $pidValue)
>>"%PSFILE%" echo   exit 20
>>"%PSFILE%" echo }
>>"%PSFILE%" echo $cmd = [string]$proc.CommandLine
>>"%PSFILE%" echo $name = [string]$proc.Name
>>"%PSFILE%" echo Write-Host ('Listener PID : {0}' -f $pidValue)
>>"%PSFILE%" echo Write-Host ('Process      : {0}' -f $name)
>>"%PSFILE%" echo Write-Host ('Command line : {0}' -f $cmd)
>>"%PSFILE%" echo Write-Host ''
>>"%PSFILE%" echo $normalized = $cmd.ToLowerInvariant()
>>"%PSFILE%" echo $looksLikeBeez = (
>>"%PSFILE%" echo   $normalized.Contains('copy-control-center') -or
>>"%PSFILE%" echo   $normalized.Contains('beez_console.py') -or
>>"%PSFILE%" echo   ($normalized.Contains('\trader\') -and $normalized.Contains('main.py'))
>>"%PSFILE%" echo )
>>"%PSFILE%" echo if (-not $looksLikeBeez) {
>>"%PSFILE%" echo   Write-Host '[REFUSED] Port 8090 is owned by a process that does not look like BeezConsole/Beelzebub.'
>>"%PSFILE%" echo   Write-Host 'No process was killed.'
>>"%PSFILE%" echo   exit 21
>>"%PSFILE%" echo }
>>"%PSFILE%" echo Write-Host '[MATCH] BeezConsole/Beelzebub listener identified.'
>>"%PSFILE%" echo Write-Host ('Stopping PID {0} and its child process tree...' -f $pidValue)
>>"%PSFILE%" echo ^& taskkill.exe /PID $pidValue /T /F
>>"%PSFILE%" echo Start-Sleep -Milliseconds 800
>>"%PSFILE%" echo $remaining = Get-NetTCPConnection -LocalPort 8090 -State Listen -ErrorAction SilentlyContinue
>>"%PSFILE%" echo if ($remaining) {
>>"%PSFILE%" echo   Write-Host '[ERROR] Port 8090 is still in LISTEN state.'
>>"%PSFILE%" echo   $remaining ^| Format-Table LocalAddress,LocalPort,OwningProcess -AutoSize
>>"%PSFILE%" echo   exit 22
>>"%PSFILE%" echo }
>>"%PSFILE%" echo Write-Host '[OK] Port 8090 is clear.'
>>"%PSFILE%" echo exit 0

powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%PSFILE%"
set "RC=%ERRORLEVEL%"

del /q "%PSFILE%" >nul 2>&1

echo.
if "%RC%"=="0" (
    echo Maintenance cleanup completed successfully.
) else (
    echo Cleanup stopped with exit code %RC%.
    echo Review the messages above before doing anything else.
)

echo.
pause
exit /b %RC%
