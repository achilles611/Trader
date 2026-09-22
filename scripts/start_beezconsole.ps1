[CmdletBinding()]
param(
    [string]$LedgerPath,
    [string]$AuditRoot,
    [string]$LedgerEpoch,
    [ValidateSet("NY_HIGH_CONFLUENCE_COMMISSIONING_V1", "BEELZEBUB_SCALPER_V2", "BEELZEBUB_FIVE_MINUTE_BIAS_V1", "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2", "BEELZEBUB_PONG_DISCERNMENT_V1")]
    [string]$PaperProfile,
    [ValidateRange(8090, 8090)]
    [int]$Port = 8090,
    [ValidatePattern('^[0-9a-fA-F]{40}$')]
    [string]$ExpectedGitSha,
    [ValidatePattern('^[0-9a-fA-F]{40}$')]
    [string]$ExpectedGitTree
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Get-CheckoutIdentity {
    param([Parameter(Mandatory)][string]$RepositoryRoot)

    $headOutput = @(& git -C $RepositoryRoot rev-parse --verify 'HEAD^{commit}' 2>$null)
    $headExitCode = $LASTEXITCODE
    $treeOutput = @(& git -C $RepositoryRoot rev-parse --verify 'HEAD^{tree}' 2>$null)
    $treeExitCode = $LASTEXITCODE
    $head = if ($headOutput.Count -eq 1) { [string]$headOutput[0] } else { '' }
    $tree = if ($treeOutput.Count -eq 1) { [string]$treeOutput[0] } else { '' }
    if (
        $headExitCode -ne 0 -or $treeExitCode -ne 0 -or
        $head -notmatch '^[0-9a-f]{40}$' -or $tree -notmatch '^[0-9a-f]{40}$'
    ) {
        throw 'BeezConsole could not resolve an exact checkout HEAD and tree identity.'
    }
    return [pscustomobject]@{ head = $head; tree = $tree }
}

function Assert-CheckoutIdentity {
    param(
        [Parameter(Mandatory)]$Actual,
        [Parameter(Mandatory)]$Expected,
        [Parameter(Mandatory)][string]$Phase
    )

    if (
        -not [string]::Equals([string]$Actual.head, [string]$Expected.head, [StringComparison]::Ordinal) -or
        -not [string]::Equals([string]$Actual.tree, [string]$Expected.tree, [StringComparison]::Ordinal)
    ) {
        throw (
            "BeezConsole checkout Git identity mismatch at $Phase. " +
            "Expected HEAD $($Expected.head) / tree $($Expected.tree); " +
            "observed HEAD $($Actual.head) / tree $($Actual.tree). " +
            'No automatic stop or restart was attempted.'
        )
    }
}

function Assert-RuntimeSourcesMatchHead {
    param([Parameter(Mandatory)][string]$RepositoryRoot)

    $status = @(
        & git -C $RepositoryRoot status --porcelain=v1 --untracked-files=all -- `
            beez_console.py main.py src 2>$null
    )
    if ($LASTEXITCODE -ne 0) {
        throw 'BeezConsole could not verify the runtime source worktree against HEAD.'
    }
    if ($status.Count -ne 0) {
        throw (
            'BeezConsole runtime sources differ from the frozen checkout ' +
            "($($status -join ', ')). Refusing to launch."
        )
    }
}

function Get-LaunchSourceIdentity {
    param([Parameter(Mandatory)][string]$RepositoryRoot)

    $launcherPath = Join-Path $RepositoryRoot 'beez_console.py'
    $entrypointPath = Join-Path $RepositoryRoot 'main.py'
    foreach ($path in @($launcherPath, $entrypointPath)) {
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "BeezConsole launch source is missing: $path"
        }
    }
    return [pscustomobject]@{
        beez_console_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $launcherPath).Hash.ToLowerInvariant()
        main_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $entrypointPath).Hash.ToLowerInvariant()
    }
}

function Assert-LaunchSourceIdentity {
    param(
        [Parameter(Mandatory)]$Actual,
        [Parameter(Mandatory)]$Expected,
        [Parameter(Mandatory)][string]$Phase
    )

    if (
        -not [string]::Equals(
            [string]$Actual.beez_console_sha256,
            [string]$Expected.beez_console_sha256,
            [StringComparison]::Ordinal
        ) -or
        -not [string]::Equals(
            [string]$Actual.main_sha256,
            [string]$Expected.main_sha256,
            [StringComparison]::Ordinal
        )
    ) {
        throw (
            "BeezConsole launch source changed at $Phase. " +
            "Expected beez_console.py $($Expected.beez_console_sha256) / main.py $($Expected.main_sha256); " +
            "observed beez_console.py $($Actual.beez_console_sha256) / main.py $($Actual.main_sha256). " +
            'No automatic stop or restart was attempted.'
        )
    }
}

$expectedIdentityParameters = @('ExpectedGitSha', 'ExpectedGitTree')
$suppliedExpectedIdentityParameters = @(
    $expectedIdentityParameters | Where-Object { $PSBoundParameters.ContainsKey($_) }
)
if ($suppliedExpectedIdentityParameters.Count -notin @(0, $expectedIdentityParameters.Count)) {
    throw 'Expected launch identity requires -ExpectedGitSha and -ExpectedGitTree together.'
}
$beforeLaunchIdentity = Get-CheckoutIdentity -RepositoryRoot $root
$frozenLaunchIdentity = $beforeLaunchIdentity
if ($suppliedExpectedIdentityParameters.Count -eq $expectedIdentityParameters.Count) {
    $declaredIdentity = [pscustomobject]@{
        head = $ExpectedGitSha.ToLowerInvariant()
        tree = $ExpectedGitTree.ToLowerInvariant()
    }
    Assert-CheckoutIdentity -Actual $beforeLaunchIdentity -Expected $declaredIdentity -Phase 'before launch'
    $frozenLaunchIdentity = $declaredIdentity
}
Assert-RuntimeSourcesMatchHead -RepositoryRoot $root
$frozenLaunchSourceIdentity = Get-LaunchSourceIdentity -RepositoryRoot $root

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

$afterLauncherIdentity = Get-CheckoutIdentity -RepositoryRoot $root
Assert-CheckoutIdentity -Actual $afterLauncherIdentity -Expected $frozenLaunchIdentity -Phase 'after launcher startup'
Assert-RuntimeSourcesMatchHead -RepositoryRoot $root
$afterLauncherSourceIdentity = Get-LaunchSourceIdentity -RepositoryRoot $root
Assert-LaunchSourceIdentity -Actual $afterLauncherSourceIdentity -Expected $frozenLaunchSourceIdentity -Phase 'after launcher startup'

$binding = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/runtime-binding" -TimeoutSec 2
$afterBindingIdentity = Get-CheckoutIdentity -RepositoryRoot $root
Assert-CheckoutIdentity -Actual $afterBindingIdentity -Expected $frozenLaunchIdentity -Phase 'after runtime-binding read'
Assert-RuntimeSourcesMatchHead -RepositoryRoot $root
$afterBindingSourceIdentity = Get-LaunchSourceIdentity -RepositoryRoot $root
Assert-LaunchSourceIdentity -Actual $afterBindingSourceIdentity -Expected $frozenLaunchSourceIdentity -Phase 'after runtime-binding read'
if (-not [string]::Equals([string]$binding.git_sha, [string]$frozenLaunchIdentity.head, [StringComparison]::Ordinal)) {
    throw (
        "BeezConsole runtime Git identity mismatch. " +
        "Expected $($frozenLaunchIdentity.head); observed $([string]$binding.git_sha). " +
        'No automatic stop or restart was attempted.'
    )
}
$binding | Add-Member -NotePropertyName launch_guard -NotePropertyValue ([ordered]@{
    git_sha = $frozenLaunchIdentity.head
    git_tree = $frozenLaunchIdentity.tree
    beez_console_sha256 = $frozenLaunchSourceIdentity.beez_console_sha256
    main_sha256 = $frozenLaunchSourceIdentity.main_sha256
}) -Force
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
