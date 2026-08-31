[CmdletBinding()]
param(
    [string]$AuthorityRoot = (Join-Path $env:LOCALAPPDATA "Beelzebub\authority\l3h")
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
$eventRoot = Join-Path ([IO.Path]::GetFullPath($AuthorityRoot)) "events"
$gatewayStatusPath = Join-Path $eventRoot "l3h-gateway-status.json"
$commissioningResultsPath = Join-Path $eventRoot "l3h-sim101-mechanical-results.json"
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$verify = & (Join-Path $PSScriptRoot "l3h_verify_install.ps1") | ConvertFrom-Json
$disk = Get-PSDrive -Name ([IO.Path]::GetPathRoot($AuthorityRoot).TrimEnd(':','\')) -ErrorAction SilentlyContinue
$gateway = if (Test-Path -LiteralPath $gatewayStatusPath) { Get-Content -Raw -LiteralPath $gatewayStatusPath | ConvertFrom-Json } else { $null }
$results = if (Test-Path -LiteralPath $commissioningResultsPath) { Get-Content -Raw -LiteralPath $commissioningResultsPath | ConvertFrom-Json } else { $null }
$reconciliation = if ($gateway) { $gateway.reconciliation } else { $null }
$stages = if ($results) { $results.stages } else { $null }
$mechanicallyCommissioned = $gateway -and $stages -and
    $gateway.live_capital -eq "DENIED" -and $gateway.live_armed -eq $false -and
    $gateway.gateway.state -eq "AUTHENTICATED" -and $gateway.gateway.loopback_only -eq $true -and $gateway.gateway.port -eq 48137 -and
    $gateway.source_fingerprint -eq $verify.installed_addon_fingerprint -and
    $reconciliation.account -eq "Sim101" -and $reconciliation.contract -eq "MNQ SEP26" -and $reconciliation.position -eq "FLAT" -and
    $reconciliation.quantity -eq 0 -and $reconciliation.owned_working_orders -eq 0 -and $reconciliation.foreign_or_unknown_orders -eq 0 -and
    $stages.probe.runtime_hello -eq "PASS" -and $stages.'restart-proof'.restart -eq "PASS" -and
    $stages.negative.bad_signature.reason -eq "DENY_BAD_SIGNATURE" -and $stages.negative.replay.reason -eq "DENY_REPLAY" -and
    $stages.negative.duplicate.reason -eq "DUPLICATE_COMMAND_NOOP" -and $stages.negative.wrong_contract.reason -eq "DENY_WRONG_CONTRACT" -and
    $stages.negative.qty_2_reject.reason -eq "DENY_QTY" -and $stages.'long-kill-command'.long.protection -eq "PASS" -and
    $stages.'short-await-menu-kill'.native_menu_kill -eq "PASS" -and $stages.'long-await-script-kill'.script_kill -eq "PASS" -and
    $stages.'unknown-transport'.unknown_state -eq "PASS" -and $stages.reconnect.reconnect -eq "PASS" -and $stages.'foreign-await'.foreign_activity -eq "PASS"
[pscustomobject]@{
    terminal_status = if ($mechanicallyCommissioned) { "L3H_MECHANICALLY_COMMISSIONED" } else { "BLOCKED_SIM101_COMMISSIONING" }
    authority_root = [IO.Path]::GetFullPath($AuthorityRoot)
    local_capability_count = @(Get-ChildItem -LiteralPath $capabilityRoot -File -Filter 'l3h-cap-*.json' -ErrorAction SilentlyContinue | Where-Object { $_.Name -notlike '*.attestation.json' }).Count
    listeners = @($listeners)
    account_class = if ($gateway) { $gateway.account_class } else { "UNKNOWN" }
    contract = "MNQ SEP26"
    execution_gateway = if ($mechanicallyCommissioned) { "AUTHENTICATED_LOOPBACK" } elseif ($listeners.port -contains 48137) { "LISTENER_PRESENT_UNVERIFIED" } else { "DISCONNECTED" }
    addon_provenance = $verify.addon_provenance
    repository_addon_hash = $verify.repository_addon_hash
    installed_addon_hash = $verify.installed_addon_hash
    compiled_dll_hash = $verify.compiled_dll_hash
    disk_free_bytes = if ($disk) { $disk.Free } else { $null }
    reconciliation = if ($mechanicallyCommissioned) { "PASS" } else { "NOT_PROVEN" }
    protection = if ($mechanicallyCommissioned) { "PASS" } else { "NOT_PROVEN" }
    kill_paths = if ($mechanicallyCommissioned) { "PASS" } else { "NOT_PROVEN" }
    live_armed = $false
    next_action = if ($mechanicallyCommissioned) { "L3H.2 is mechanically commissioned. L3H.3 requires a separate capital-bearing authorization; live authority remains disarmed." } else { "Install and visibly compile the dedicated AddOn, then conduct only the Sim101 mechanical matrix; no status action may arm L3H." }
} | ConvertTo-Json -Depth 4
