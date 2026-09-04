[CmdletBinding()]
param(
    [string]$LedgerPath = "N:\Beelzebub\runtime\hot\lane_iii_paper.sqlite3",
    [string]$AuditRoot = "N:\Beelzebub\runtime\audit",
    [string]$LedgerEpoch = "",
    [ValidateSet("NY_HIGH_CONFLUENCE_COMMISSIONING_V1", "BEELZEBUB_SCALPER_V2", "BEELZEBUB_FIVE_MINUTE_BIAS_V1")]
    [string]$PaperProfile = "BEELZEBUB_SCALPER_V2"
)

$ErrorActionPreference = "Stop"
# Maintenance only. Do not use restart/kill tooling during active trading or
# commissioning. It safely clears only a verified BeezConsole owner, then
# launches exactly one backend with the authoritative bindings.
& (Join-Path $PSScriptRoot "stop_beezconsole.ps1")
& (Join-Path $PSScriptRoot "start_beezconsole.ps1") -LedgerPath $LedgerPath -AuditRoot $AuditRoot -LedgerEpoch $LedgerEpoch -PaperProfile $PaperProfile
