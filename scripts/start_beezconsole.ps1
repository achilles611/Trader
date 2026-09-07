[CmdletBinding()]
param(
    [string]$LedgerPath,
    [string]$AuditRoot,
    [string]$LedgerEpoch,
    [ValidateSet("NY_HIGH_CONFLUENCE_COMMISSIONING_V1", "BEELZEBUB_SCALPER_V2", "BEELZEBUB_FIVE_MINUTE_BIAS_V1", "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2")]
    [string]$PaperProfile,
    [ValidateRange(8090, 8090)]
    [int]$Port = 8090
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

$bindingParameterNames = @("LedgerPath", "AuditRoot", "LedgerEpoch", "PaperProfile")
$suppliedBindingParameters = @($bindingParameterNames | Where-Object { $PSBoundParameters.ContainsKey($_) })
if ($suppliedBindingParameters.Count -notin @(0, $bindingParameterNames.Count)) {
    throw "Explicit maintenance launch requires -LedgerPath, -AuditRoot, -LedgerEpoch, and -PaperProfile together."
}
$explicitMaintenance = $suppliedBindingParameters.Count -eq $bindingParameterNames.Count

# restart_beezconsole.ps1 historically forwards its four defaults even when the
# operator supplies no binding. Treat only that exact legacy empty-epoch tuple
# as an ordinary remembered launch. Any customized empty binding still fails.
$legacyRestartDefaults = (
    $explicitMaintenance -and
    [string]::Equals($LedgerPath, "N:\Beelzebub\runtime\hot\lane_iii_paper.sqlite3", [StringComparison]::OrdinalIgnoreCase) -and
    [string]::Equals($AuditRoot, "N:\Beelzebub\runtime\audit", [StringComparison]::OrdinalIgnoreCase) -and
    [string]::IsNullOrWhiteSpace($LedgerEpoch) -and
    $PaperProfile -eq "BEELZEBUB_SCALPER_V2"
)
if ($legacyRestartDefaults) { $explicitMaintenance = $false }

# With no binding arguments, the launcher ignores inherited paper environment
# variables and restores only the backend-proven established selection. The
# complete four-argument form remains the documented maintenance-only fallback.
$launcherArguments = @()
if ($explicitMaintenance) {
    if (@($LedgerPath, $AuditRoot, $LedgerEpoch, $PaperProfile) | Where-Object { [string]::IsNullOrWhiteSpace($_) }) {
        throw "Explicit maintenance launch binding values cannot be empty."
    }
    if (-not $LedgerEpoch.StartsWith("L3G-PAPER-EPOCH-", [StringComparison]::Ordinal)) {
        throw "Explicit maintenance ledger epoch must begin with L3G-PAPER-EPOCH-."
    }
    if (-not [IO.Path]::IsPathFullyQualified($LedgerPath) -or -not [IO.Path]::IsPathFullyQualified($AuditRoot)) {
        throw "Explicit maintenance ledger and audit paths must be absolute."
    }
    $LedgerPath = [IO.Path]::GetFullPath($LedgerPath)
    $AuditRoot = [IO.Path]::GetFullPath($AuditRoot)
    New-Item -ItemType Directory -Force -Path $AuditRoot | Out-Null
    $launcherArguments = @(
        "--maintenance-ledger-path=`"$LedgerPath`"",
        "--maintenance-audit-root=`"$AuditRoot`"",
        "--maintenance-ledger-epoch=`"$LedgerEpoch`"",
        "--maintenance-paper-profile=`"$PaperProfile`""
    )
}

$python = Join-Path $root ".venv312\Scripts\python.exe"
$launcherSource = Join-Path $root "beez_console.py"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { throw "Missing required project Python: $python" }
if (-not (Test-Path -LiteralPath $launcherSource -PathType Leaf)) { throw "Missing launcher source: $launcherSource" }
# The checked-out source is authoritative. A rebuilt BeezConsole.exe delegates
# here as well, so this script must never execute an older bundled strategy
# selector merely because an ignored executable happens to exist.
$pythonArguments = @("`"$launcherSource`"") + $launcherArguments
$process = Start-Process -FilePath $python -ArgumentList $pythonArguments `
    -WorkingDirectory $root -PassThru -WindowStyle Hidden
Write-Output "Started BeezConsole launcher PID $($process.Id). Waiting for exact runtime binding."
$process.WaitForExit()
if ($process.ExitCode -ne 0) {
    throw (
        "BeezConsole refused startup or runtime adoption. " +
        "Inspect its startup error and logs\beez-console-server.log."
    )
}

$binding = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/runtime-binding" -TimeoutSec 2
if ($explicitMaintenance) {
    $expectedLedger = [IO.Path]::GetFullPath($LedgerPath)
    $expectedAudit = [IO.Path]::GetFullPath($AuditRoot)
    $pathComparison = [StringComparison]::OrdinalIgnoreCase
    $bindingMismatch = (
        -not [string]::Equals(
            [IO.Path]::GetFullPath([string]$binding.ledger), $expectedLedger, $pathComparison
        ) -or
        -not [string]::Equals([IO.Path]::GetFullPath([string]$binding.audit), $expectedAudit, $pathComparison) -or
        [string]$binding.ledger_epoch -ne $LedgerEpoch -or
        [string]$binding.entry_profile_version -ne $PaperProfile -or
        [string]$binding.control_center -ne "127.0.0.1:$Port"
    )
    if ($bindingMismatch) {
        throw "BeezConsole runtime binding does not match the explicit maintenance launch request."
    }
}
$binding | ConvertTo-Json -Depth 4
