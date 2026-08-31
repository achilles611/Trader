[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
try {
    $event = [Threading.EventWaitHandle]::OpenExisting("Global\BeelzebubL3HNativeKill")
    $event.Set(); $event.Dispose()
    [pscustomobject]@{ outcome = "KILL_SIGNAL_SENT"; live_armed = $false; note = "The native AddOn must independently verify flattening and remain disarmed." } | ConvertTo-Json
} catch {
    [pscustomobject]@{ outcome = "KILL_SIGNAL_UNAVAILABLE"; live_armed = $false; error = $_.Exception.Message } | ConvertTo-Json
    exit 1
}
