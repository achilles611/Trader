[CmdletBinding()]
param(
    [string]$AuthorityRoot = "N:\Beelzebub\authority\l3h"
)

$ErrorActionPreference = "Stop"
$resolvedRoot = [IO.Path]::GetFullPath($AuthorityRoot)
$keyRoot = Join-Path $resolvedRoot "keys"
$capabilityRoot = Join-Path $resolvedRoot "capabilities"
New-Item -ItemType Directory -Force -Path $keyRoot, $capabilityRoot | Out-Null

# The current Windows user and Administrators retain access; inheritance is
# removed so a local capability/key never becomes a repository artifact.
$acl = Get-Acl -LiteralPath $resolvedRoot
$acl.SetAccessRuleProtection($true, $false)
$acl.SetAccessRule((New-Object Security.AccessControl.FileSystemAccessRule($env:USERNAME, "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow")))
$acl.SetAccessRule((New-Object Security.AccessControl.FileSystemAccessRule("BUILTIN\Administrators", "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow")))
Set-Acl -LiteralPath $resolvedRoot -AclObject $acl

$keyPath = Join-Path $keyRoot "l3h.capability.hmac.key"
if (-not (Test-Path -LiteralPath $keyPath)) {
    $bytes = New-Object byte[] 32
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    [IO.File]::WriteAllBytes($keyPath, $bytes)
    [Array]::Clear($bytes, 0, $bytes.Length)
}

[pscustomobject]@{
    authority_root = $resolvedRoot
    key_present = Test-Path -LiteralPath $keyPath
    capability_root = $capabilityRoot
    live_armed = $false
    next_action = "Create and independently review a local signed capability; this script does not create an account binding or send an order."
} | ConvertTo-Json -Depth 3
