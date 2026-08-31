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
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$verify = & (Join-Path $PSScriptRoot "l3h_verify_install.ps1") | ConvertFrom-Json
$disk = Get-PSDrive -Name ([IO.Path]::GetPathRoot($AuthorityRoot).TrimEnd(':','\')) -ErrorAction SilentlyContinue
[pscustomobject]@{
    terminal_status = "BLOCKED_CAPABILITY_MISSING"
    authority_root = [IO.Path]::GetFullPath($AuthorityRoot)
    local_capability_count = @(Get-ChildItem -LiteralPath $capabilityRoot -File -Filter '*.json' -ErrorAction SilentlyContinue).Count
    listeners = @($listeners)
    account_class = "UNKNOWN"
    contract = "MNQ SEP26"
    execution_gateway = if ($listeners.port -contains 48137) { "LISTENER_PRESENT_UNVERIFIED" } else { "DISCONNECTED" }
    addon_provenance = $verify.addon_provenance
    repository_addon_hash = $verify.repository_addon_hash
    installed_addon_hash = $verify.installed_addon_hash
    compiled_dll_hash = $verify.compiled_dll_hash
    disk_free_bytes = if ($disk) { $disk.Free } else { $null }
    reconciliation = "NOT_STARTED"
    protection = "NOT_PROVEN"
    kill_paths = "NOT_PROVEN"
    live_armed = $false
    next_action = "Install and visibly compile the dedicated AddOn, then conduct only the Sim101 mechanical matrix; no status action may arm L3H."
} | ConvertTo-Json -Depth 4
