[CmdletBinding()]
param(
    [string]$AuthorityRoot = (Join-Path $env:LOCALAPPDATA "Beelzebub\authority\l3h")
)

$ErrorActionPreference = "Stop"
& (Join-Path $PSScriptRoot "l3h_status.ps1") -AuthorityRoot $AuthorityRoot
Write-Output "L3H recovery is observational only: unresolved command evidence must be reconciled with NinjaTrader before a new capability can be considered."
