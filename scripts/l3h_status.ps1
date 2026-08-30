[CmdletBinding()]
param(
    [string]$AuthorityRoot = "N:\Beelzebub\authority\l3h"
)

$ErrorActionPreference = "Stop"
$ports = 8090, 48135, 48136, 48137
$listeners = Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
    Where-Object { $_.LocalPort -in $ports } |
    ForEach-Object {
        $process = Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue
        [pscustomobject]@{ port = $_.LocalPort; pid = $_.OwningProcess; process = if ($process) { $process.ProcessName } else { "<exited>" } }
    }
$capabilityRoot = Join-Path ([IO.Path]::GetFullPath($AuthorityRoot)) "capabilities"
[pscustomobject]@{
    terminal_status = "BLOCKED_CAPABILITY_MISSING"
    authority_root = [IO.Path]::GetFullPath($AuthorityRoot)
    local_capability_count = @(Get-ChildItem -LiteralPath $capabilityRoot -File -Filter '*.json' -ErrorAction SilentlyContinue).Count
    listeners = @($listeners)
    live_armed = $false
    next_action = "Review exact blocker; no scheduled or status action may arm L3H."
} | ConvertTo-Json -Depth 4
