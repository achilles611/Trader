[CmdletBinding()]
param(
    [string]$Config = "config/copytrade.yaml"
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$hotRoot = Join-Path $root "runtime\hot"
$lockPath = Join-Path $hotRoot "beelzebub-d7-processes.json"
New-Item -ItemType Directory -Force -Path $hotRoot | Out-Null

if (Test-Path -LiteralPath $lockPath) {
    $prior = Get-Content -LiteralPath $lockPath -Raw | ConvertFrom-Json
    $running = @()
    foreach ($candidatePid in @($prior.worker_pid, $prior.observer_pid)) {
        if ($candidatePid -and (Get-Process -Id $candidatePid -ErrorAction SilentlyContinue)) {
            $running += $candidatePid
        }
    }
    if ($running.Count -gt 0) {
        throw "Beelzebub D.7 worker/observer already holds $lockPath (PIDs: $($running -join ', ')). Use scripts/stop_beelzebub.ps1 first."
    }
    Remove-Item -LiteralPath $lockPath -Force
}

$python = Join-Path $root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python)) { $python = "python" }
$coldLogs = "D:\BeelzebubData\logs"
try { New-Item -ItemType Directory -Force -Path $coldLogs | Out-Null } catch { $coldLogs = Join-Path $hotRoot "logs"; New-Item -ItemType Directory -Force -Path $coldLogs | Out-Null }
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$workerLog = Join-Path $coldLogs "scientific-worker-$stamp.log"
$observerLog = Join-Path $coldLogs "public-observer-$stamp.log"
$workerErrorLog = "$workerLog.err"
$observerErrorLog = "$observerLog.err"

$stream = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::Write, [System.IO.FileShare]::None)
try {
    $worker = Start-Process -FilePath $python -ArgumentList @((Join-Path $root "main.py"), "science", "--config", (Join-Path $root $Config), "run") -WorkingDirectory $root -RedirectStandardOutput $workerLog -RedirectStandardError $workerErrorLog -PassThru -WindowStyle Hidden
    $observer = Start-Process -FilePath $python -ArgumentList @((Join-Path $root "main.py"), "science", "--config", (Join-Path $root $Config), "observe") -WorkingDirectory $root -RedirectStandardOutput $observerLog -RedirectStandardError $observerErrorLog -PassThru -WindowStyle Hidden
    $payload = [ordered]@{
        worker_pid = $worker.Id; observer_pid = $observer.Id; started_at = (Get-Date).ToUniversalTime().ToString("o")
        config = (Join-Path $root $Config); worker_log = $workerLog; observer_log = $observerLog
        execution_mode = "SIMULATION_SHADOW_ONLY"
    } | ConvertTo-Json -Compress
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($payload)
    $stream.Write($bytes, 0, $bytes.Length)
} catch {
    if ($worker) { Stop-Process -Id $worker.Id -Force -ErrorAction SilentlyContinue }
    if ($observer) { Stop-Process -Id $observer.Id -Force -ErrorAction SilentlyContinue }
    throw
} finally {
    $stream.Dispose()
}

Write-Output "Started paper-only D.7 public observer and scientific worker. Lock: $lockPath"
