[CmdletBinding()]
param(
    [string]$AuthorityRoot = (Join-Path $env:LOCALAPPDATA "Beelzebub\authority\l3h")
)

$ErrorActionPreference = "Stop"
$resolvedRoot = [IO.Path]::GetFullPath($AuthorityRoot)
$keyRoot = Join-Path $resolvedRoot "keys"
$capabilityRoot = Join-Path $resolvedRoot "capabilities"
$eventRoot = Join-Path $resolvedRoot "events"
New-Item -ItemType Directory -Force -Path $keyRoot, $capabilityRoot, $eventRoot | Out-Null

# The current Windows user and Administrators retain access; inheritance is
# removed so a local capability/key never becomes a repository artifact.
$acl = Get-Acl -LiteralPath $resolvedRoot
$currentIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$acl.SetAccessRuleProtection($true, $false)
$acl.SetAccessRule((New-Object Security.AccessControl.FileSystemAccessRule($currentIdentity, "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow")))
$acl.SetAccessRule((New-Object Security.AccessControl.FileSystemAccessRule("BUILTIN\Administrators", "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow")))
Set-Acl -LiteralPath $resolvedRoot -AclObject $acl

foreach ($keyName in "l3h.capability.hmac.key", "l3h.execution.local.key") {
    $keyPath = Join-Path $keyRoot $keyName
    if (-not (Test-Path -LiteralPath $keyPath)) {
        $bytes = New-Object byte[] 32
        [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
        [IO.File]::WriteAllBytes($keyPath, $bytes)
        [Array]::Clear($bytes, 0, $bytes.Length)
    }
}

$disk = Get-PSDrive -Name ([IO.Path]::GetPathRoot($resolvedRoot).TrimEnd(':','\')) -ErrorAction Stop
$freeHealthy = $disk.Free -gt 0
$repo = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$addonSource = Join-Path $repo "ninjatrader\NinjaScript\AddOns\BeelzebubLiveExecutionAddOn.cs"
$projectPython = Join-Path $repo ".venv312\Scripts\python.exe"

[pscustomobject]@{
    authority_root = $resolvedRoot
    capability_key_present = Test-Path -LiteralPath (Join-Path $keyRoot "l3h.capability.hmac.key")
    gateway_key_present = Test-Path -LiteralPath (Join-Path $keyRoot "l3h.execution.local.key")
    capability_root = $capabilityRoot
    event_root = $eventRoot
    free_bytes = $disk.Free
    disk_healthy = $freeHealthy
    addon_source_present = Test-Path -LiteralPath $addonSource
    python_312_present = Test-Path -LiteralPath $projectPython
    live_armed = $false
    next_action = "Review a local signed capability and native binding; this script creates no binding, opens no listener, and sends no order."
} | ConvertTo-Json -Depth 3
