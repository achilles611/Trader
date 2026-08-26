[CmdletBinding()]
param(
    [string]$LedgerPath = "N:\Beelzebub\runtime\hot\lane_iii_paper.sqlite3",
    [string]$AuditRoot = "N:\Beelzebub\runtime\audit",
    [string]$LedgerEpoch = "",
    [int]$Port = 8090
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

# Maintenance only. Do not start/restart this backend during active trading or
# commissioning. A pre-existing listener must be inspected and stopped with
# stop_beezconsole.ps1; this script never starts a second backend.
if (Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue) {
    throw "Port $Port is already listening. Run scripts\stop_beezconsole.ps1 only after confirming a maintenance window."
}

$env:BEELZEBUB_L3G_PAPER_LEDGER = $LedgerPath
$env:BEELZEBUB_LEDGER_AUDIT_ROOT = $AuditRoot
if ($LedgerEpoch) { $env:BEELZEBUB_L3G_PAPER_LEDGER_EPOCH = $LedgerEpoch }
$env:BEELZEBUB_GIT_SHA = (git -C $root rev-parse HEAD).Trim()
New-Item -ItemType Directory -Force -Path $AuditRoot | Out-Null

$launcher = Join-Path $root "BeezConsole.exe"
if (Test-Path -LiteralPath $launcher) {
    $process = Start-Process -FilePath $launcher -WorkingDirectory $root -PassThru -WindowStyle Hidden
} else {
    $python = Join-Path $root ".venv312\Scripts\python.exe"
    if (-not (Test-Path -LiteralPath $python)) { throw "Missing required project Python: $python" }
    $process = Start-Process -FilePath $python -ArgumentList @("beez_console.py") -WorkingDirectory $root -PassThru -WindowStyle Hidden
}
Write-Output "Started BeezConsole launcher PID $($process.Id). Waiting for HTTP health."

for ($attempt = 0; $attempt -lt 240; $attempt++) {
    try {
        $health = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/health" -TimeoutSec 2
        $binding = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/runtime-binding" -TimeoutSec 2
        $binding | ConvertTo-Json -Depth 4
        exit 0
    } catch { Start-Sleep -Milliseconds 500 }
}
throw "BeezConsole did not reach HTTP health. Inspect logs\beez-console-server.log."
