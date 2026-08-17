[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$lockPath = Join-Path $root "runtime\hot\beelzebub-d7-processes.json"
if (-not (Test-Path -LiteralPath $lockPath)) { Write-Output "No D.7 process lock exists."; exit 0 }
$record = Get-Content -LiteralPath $lockPath -Raw | ConvertFrom-Json
foreach ($processId in @($record.worker_pid, $record.observer_pid)) {
    if ($processId -and (Get-Process -Id $processId -ErrorAction SilentlyContinue)) {
        Stop-Process -Id $processId -ErrorAction Stop
    }
}
Remove-Item -LiteralPath $lockPath -Force
Write-Output "Stopped the recorded D.7 public observer and scientific worker."
