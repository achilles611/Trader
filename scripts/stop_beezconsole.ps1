[CmdletBinding()]
param(
    [int]$Port = 8090
)

$ErrorActionPreference = "Stop"

# Maintenance only. Do not stop/restart BeezConsole during active trading or
# commissioning. This script refuses a listener whose command line does not
# identify the repository's known local control-center process.
$listeners = @(Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
    Where-Object { $_.LocalAddress -in @("127.0.0.1", "::1", "0.0.0.0", "::") })
if ($listeners.Count -eq 0) { Write-Output "Port $Port is already free."; exit 0 }
if ($listeners.Count -ne 1) { throw "Refusing to stop: port $Port has $($listeners.Count) listeners." }

$ownerProcessId = [int]$listeners[0].OwningProcess
$process = Get-CimInstance Win32_Process -Filter "ProcessId = $ownerProcessId" -ErrorAction Stop
$command = [string]$process.CommandLine
if ($command -notmatch '(?i)(beezconsole|beez_console|copy-control-center)') {
    throw "Refusing to kill unrelated port $Port owner. PID $ownerProcessId command: $command"
}

Write-Output "Stopping BeezConsole listener PID ${ownerProcessId}: $command"
& "$env:SystemRoot\System32\taskkill.exe" /PID $ownerProcessId /T /F | Out-Host
for ($attempt = 0; $attempt -lt 40; $attempt++) {
    if (-not (Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)) {
        Write-Output "Port $Port is free."
        exit 0
    }
    Start-Sleep -Milliseconds 250
}
throw "Port $Port is still listening after the BeezConsole process tree was stopped."
