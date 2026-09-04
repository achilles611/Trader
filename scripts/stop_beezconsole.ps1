[CmdletBinding()]
param(
    [int]$Port = 8090
)

$ErrorActionPreference = "Stop"

# Maintenance only. Do not stop/restart BeezConsole during active trading or
# commissioning. The backend verifies exact flat/disarmed broker truth, drains
# the ledger, closes its transports, and proves the final checkpoint.
$listeners = @(Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
    Where-Object { $_.LocalAddress -in @("127.0.0.1", "::1", "0.0.0.0", "::") })
if ($listeners.Count -eq 0) { Write-Output "Port $Port is already free."; exit 0 }
if ($listeners.Count -ne 1) { throw "Refusing to stop: port $Port has $($listeners.Count) listeners." }

$ownerProcessId = [int]$listeners[0].OwningProcess
$process = Get-CimInstance Win32_Process -Filter "ProcessId = $ownerProcessId" -ErrorAction Stop
$command = [string]$process.CommandLine
if ($command -notmatch '(?i)(beezconsole|beez_console|copy-control-center)') {
    throw "Refusing to stop unrelated port $Port owner. PID $ownerProcessId command: $command"
}

Write-Output "Stopping BeezConsole listener PID ${ownerProcessId}: $command"
$requestId = "shutdown-$([Guid]::NewGuid().ToString('N'))"
$headers = @{ "X-Beelzebub-Graceful-Shutdown-Action" = "beezconsole-graceful-shutdown-v1" }
$body = @{ request_id = $requestId } | ConvertTo-Json -Compress
try {
    $response = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/system/graceful-shutdown" `
        -Method Post -Headers $headers -ContentType "application/json" -Body $body -TimeoutSec 10
}
catch {
    throw "Controlled shutdown was refused. The process was left running: $($_.Exception.Message)"
}
if ($response.accepted -ne $true -or $response.request_id -ne $requestId) {
    throw "Controlled shutdown returned an invalid acknowledgement. The process was left running."
}
for ($attempt = 0; $attempt -lt 720; $attempt++) {
    if (-not (Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)) {
        $remaining = Get-Process -Id $ownerProcessId -ErrorAction SilentlyContinue
        if ($null -ne $remaining) {
            Start-Sleep -Milliseconds 250
            continue
        }
        Write-Output "Controlled shutdown complete. Port $Port is free and PID $ownerProcessId exited."
        exit 0
    }
    Start-Sleep -Milliseconds 250
}
throw "Controlled shutdown did not complete within 180 seconds. No force termination was attempted."
