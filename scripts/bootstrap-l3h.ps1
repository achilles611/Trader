[CmdletBinding()]
param(
    [string]$AuthorityRoot = (Join-Path $env:LOCALAPPDATA "Beelzebub\authority\l3h")
)

$ErrorActionPreference = "Stop"
& (Join-Path $PSScriptRoot "l3h_bootstrap.ps1") -AuthorityRoot $AuthorityRoot
