[CmdletBinding()]
param()

$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$lockPath = Join-Path $root "runtime\hot\beelzebub-d7-processes.json"
if (Test-Path -LiteralPath $lockPath) {
    $record = Get-Content -LiteralPath $lockPath -Raw | ConvertFrom-Json
    $worker = [bool](Get-Process -Id $record.worker_pid -ErrorAction SilentlyContinue)
    $observer = [bool](Get-Process -Id $record.observer_pid -ErrorAction SilentlyContinue)
    [ordered]@{ lock = $lockPath; worker_running = $worker; observer_running = $observer; worker_log = $record.worker_log; observer_log = $record.observer_log; execution_mode = $record.execution_mode } | ConvertTo-Json
} else {
    [ordered]@{ lock = $lockPath; worker_running = $false; observer_running = $false; execution_mode = "SIMULATION_SHADOW_ONLY" } | ConvertTo-Json
}
