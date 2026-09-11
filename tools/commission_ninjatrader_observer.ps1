[CmdletBinding()]
param(
    [switch] $InstallSource,
    [int] $Port = 8090,
    [int] $CompileTimeoutSeconds = 60,
    [int] $CheckpointSeconds = 10,
    [string] $NinjaTraderDocumentsRoot = (Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'NinjaTrader 8')
)

$ErrorActionPreference = 'Stop'
if ($PSVersionTable.PSVersion.Major -lt 7) {
    throw 'PowerShell 7 or later is required so NinjaTrader bundled Roslyn dependencies load consistently. Run with pwsh.'
}
if ($CompileTimeoutSeconds -lt 10 -or $CompileTimeoutSeconds -gt 300) {
    throw 'CompileTimeoutSeconds must be between 10 and 300.'
}
if ($CheckpointSeconds -lt 5 -or $CheckpointSeconds -gt 60) {
    throw 'CheckpointSeconds must be between 5 and 60.'
}

$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$repositoryAddOn = Join-Path $repositoryRoot 'ninjatrader\NinjaScript\AddOns\BeelzebubReadOnlyAddOn.cs'
$repositoryIndicator = Join-Path $repositoryRoot 'ninjatrader\NinjaScript\Indicators\BeelzebubReadOnlyMarketObserver.cs'
$installedAddOn = Join-Path $NinjaTraderDocumentsRoot 'bin\Custom\AddOns\BeelzebubReadOnlyAddOn.cs'
$installedIndicator = Join-Path $NinjaTraderDocumentsRoot 'bin\Custom\Indicators\BeelzebubReadOnlyMarketObserver.cs'
$compiledAssembly = Join-Path $NinjaTraderDocumentsRoot 'bin\Custom\NinjaTrader.Custom.dll'
$validator = Join-Path $repositoryRoot 'tools\validate_ninjatrader_observer.ps1'
$pythonContract = Join-Path $repositoryRoot 'src\l3f_provider\ninjatrader_observation.py'

foreach ($required in @($repositoryAddOn, $repositoryIndicator, $compiledAssembly, $validator, $pythonContract)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "Required observer input is missing: $required"
    }
}

function Get-Sha256([string] $Path) {
    return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Get-CanonicalSource([string] $Path, [switch] $WithoutGeneratedRegion) {
    $text = [IO.File]::ReadAllText($Path).Replace("`r`n", "`n")
    if ($WithoutGeneratedRegion) {
        $marker = '#region NinjaScript generated code. Neither change nor remove.'
        $index = $text.IndexOf($marker, [StringComparison]::Ordinal)
        if ($index -ge 0) { $text = $text.Substring(0, $index) }
    }
    return $text.TrimEnd() + "`n"
}

function Get-ExpectedFingerprint {
    $addOnText = Get-CanonicalSource $repositoryAddOn
    $indicatorText = Get-CanonicalSource $repositoryIndicator
    $addOnText = [regex]::Replace(
        $addOnText,
        '(private const string ProducerSourceFingerprint = ")[0-9a-f]{64}(";)',
        '$1<SOURCE_FINGERPRINT>$2'
    )
    $material = "NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs`n$addOnText" +
        "NinjaScript/Indicators/BeelzebubReadOnlyMarketObserver.cs`n$indicatorText"
    $bytes = [Text.UTF8Encoding]::new($false).GetBytes($material)
    $digest = [Security.Cryptography.SHA256]::HashData($bytes)
    return ($digest | ForEach-Object { $_.ToString('x2') }) -join ''
}

function Test-CompiledIdentity([string] $ExpectedFingerprint) {
    $bytes = [IO.File]::ReadAllBytes($compiledAssembly)
    $unicode = [Text.Encoding]::Unicode.GetString($bytes)
    $utf8 = [Text.Encoding]::UTF8.GetString($bytes)
    return (
        $unicode.Contains($ExpectedFingerprint) -and
        $utf8.Contains('BeelzebubAutomaticMarketObserver') -and
        $utf8.Contains('ShiftKnownPositions') -and
        $utf8.Contains('DepthPrefix')
    )
}

function Install-ObserverSource {
    param([string] $Source, [string] $Target, [switch] $PreserveGeneratedRegion)
    $targetDirectory = Split-Path -Parent $Target
    [IO.Directory]::CreateDirectory($targetDirectory) | Out-Null
    $sourceText = Get-CanonicalSource $Source
    $generated = ''
    if ($PreserveGeneratedRegion -and (Test-Path -LiteralPath $Target)) {
        $current = [IO.File]::ReadAllText($Target).Replace("`r`n", "`n")
        $marker = '#region NinjaScript generated code. Neither change nor remove.'
        $index = $current.IndexOf($marker, [StringComparison]::Ordinal)
        if ($index -ge 0) { $generated = "`n" + $current.Substring($index).TrimStart() }
    }
    if (Test-Path -LiteralPath $Target) {
        $backup = "$Target.unattended-before-$([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')).bak"
        Copy-Item -LiteralPath $Target -Destination $backup -ErrorAction Stop
    }
    [IO.File]::WriteAllText($Target, $sourceText + $generated, [Text.UTF8Encoding]::new($false))
}

function Get-PaperStatus {
    return Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/lane-iii/paper" -TimeoutSec 45
}

function Get-TailCoverage([string] $JournalPath) {
    $counts = [ordered]@{ records = 0; quote = 0; trade = 0; depth = 0; usable_depth = 0; unavailable_depth = 0 }
    foreach ($line in @(Get-Content -LiteralPath $JournalPath -Tail 2000)) {
        if ([String]::IsNullOrWhiteSpace($line)) { continue }
        $row = $line | ConvertFrom-Json
        $observation = $row.observation
        $counts.records++
        if ($observation.observation_type -eq 'QUOTE') { $counts.quote++ }
        elseif ($observation.observation_type -eq 'TRADE') { $counts.trade++ }
        elseif ($observation.observation_type -eq 'DEPTH') {
            $counts.depth++
            if ($observation.payload.status -eq 'UNAVAILABLE') { $counts.unavailable_depth++ }
            elseif (@($observation.payload.bids).Count -gt 0 -and @($observation.payload.asks).Count -gt 0) {
                $counts.usable_depth++
            }
        }
    }
    return $counts
}

function Get-Checkpoint {
    $paper = Get-PaperStatus
    $collection = $paper.pong_collection
    if ($null -eq $collection -or $null -eq $collection.producer) {
        throw 'The independent Pong collector does not have an authenticated producer.'
    }
    $coverage = Get-TailCoverage $collection.durability.journal_path
    return [ordered]@{
        observed_at = [DateTime]::UtcNow.ToString('o')
        collector_id = $collection.collector.collector_id
        expected_source_fingerprint = $collection.collector.expected_source_fingerprint
        loaded_source_fingerprint = $collection.producer.source_fingerprint
        loaded_build_fingerprint = $collection.producer.build_fingerprint
        loaded_build_timestamp = $collection.producer.build_timestamp
        producer_session_id = $collection.producer.session_id
        signature_verified = $collection.producer.signature_verified
        transport = $collection.transport.state
        durable_records = $collection.durability.durable_records
        journal_bytes = $collection.durability.journal_bytes
        journal_tip_sha256 = $collection.durability.journal_tip_sha256
        last_fsync_at = $collection.durability.last_fsync_at
        queue_depth = $collection.durability.queue_depth
        rejected_records = $collection.durability.rejected_records
        duplicate_records = $collection.durability.duplicate_records
        queue_overflow_records = $collection.durability.queue_overflow_records
        tail = $coverage
        observer = [ordered]@{
            state = $paper.market_observer.observer_attachment.state
            instrument = $paper.market_observer.observer_attachment.instrument
            observer_attached = $paper.market_observer.observer_attachment.observer_attached
            subscription_mode = $paper.market_observer.observer_attachment.subscription_mode
            active = $paper.market_observer.market_observer_active
        }
        safety = [ordered]@{
            state = $paper.state
            account = $paper.paper_account
            account_class = $paper.account_class
            instrument = $paper.market_instrument
            maximum_quantity = $paper.maximum_quantity
            paper_execution = $paper.paper_execution
            session_armed_state = $paper.session_armed_state
            live_capital = $paper.live_capital
            current_position = $paper.current_position
            current_quantity = $paper.current_quantity
            working_owned_orders = $paper.working_owned_orders
            working_entry_orders = $paper.working_entry_orders
            reconciliation_current = $paper.reconciliation_current
            authority_lockout = $paper.risk_continuity.authority_lockout
        }
    }
}

$validationText = (& $validator | Out-String).Trim()
$validation = $validationText | ConvertFrom-Json
if ($validation.success -ne $true -or $validation.errors -ne 0) {
    throw "Observer compiler validation failed: $validationText"
}

$expectedFingerprint = Get-ExpectedFingerprint
$pythonText = [IO.File]::ReadAllText($pythonContract)
$pythonMatch = [regex]::Match($pythonText, 'EXPECTED_OBSERVER_SOURCE_FINGERPRINT = "([0-9a-f]{64})"')
$addOnMatch = [regex]::Match((Get-CanonicalSource $repositoryAddOn), 'ProducerSourceFingerprint = "([0-9a-f]{64})"')
if (-not $pythonMatch.Success -or -not $addOnMatch.Success -or
    $pythonMatch.Groups[1].Value -ne $expectedFingerprint -or
    $addOnMatch.Groups[1].Value -ne $expectedFingerprint) {
    throw 'Checked-in observer source fingerprint constants do not match the source material.'
}

$installedCurrent = (
    (Test-Path -LiteralPath $installedAddOn) -and
    (Test-Path -LiteralPath $installedIndicator) -and
    (Get-CanonicalSource $repositoryAddOn) -ceq (Get-CanonicalSource $installedAddOn) -and
    (Get-CanonicalSource $repositoryIndicator) -ceq (Get-CanonicalSource $installedIndicator -WithoutGeneratedRegion)
)
$assemblyBefore = Get-Item -LiteralPath $compiledAssembly
$assemblyHashBefore = Get-Sha256 $compiledAssembly
$compileStatus = 'SKIPPED_CURRENT'

if (-not $installedCurrent) {
    if (-not $InstallSource) {
        throw 'Installed observer source differs from the repository. Re-run with -InstallSource.'
    }
    Install-ObserverSource -Source $repositoryAddOn -Target $installedAddOn
    Install-ObserverSource -Source $repositoryIndicator -Target $installedIndicator -PreserveGeneratedRegion
    $compileStatus = 'WAITING_FOR_NINJATRADER_SOURCE_WATCHER'
}

if (-not (Test-CompiledIdentity $expectedFingerprint) -or -not $installedCurrent) {
    $deadline = [DateTime]::UtcNow.AddSeconds($CompileTimeoutSeconds)
    do {
        Start-Sleep -Milliseconds 500
        $assemblyNow = Get-Item -LiteralPath $compiledAssembly
        if ($assemblyNow.LastWriteTimeUtc -gt $assemblyBefore.LastWriteTimeUtc -and
            (Test-CompiledIdentity $expectedFingerprint)) {
            $compileStatus = 'NINJATRADER_SOURCE_WATCHER_REBUILT'
            break
        }
    } while ([DateTime]::UtcNow -lt $deadline)
}
if (-not (Test-CompiledIdentity $expectedFingerprint)) {
    throw 'The compiled NinjaTrader assembly did not load the expected observer identity before timeout.'
}

$first = Get-Checkpoint
Start-Sleep -Seconds $CheckpointSeconds
$second = Get-Checkpoint
$observerVerified = (
    $second.observer.state -eq 'NATIVE_ADDON_OBSERVER_ACTIVE' -and
    $second.observer.instrument -eq 'MNQ SEP26' -and
    $second.observer.observer_attached -eq $true -and
    $second.observer.subscription_mode -eq 'NATIVE_ADDON' -and
    $second.observer.active -eq $true
)
$safetyVerified = (
    $second.safety.state -eq 'READY_DISARMED' -and
    $second.safety.account -eq 'Sim101' -and
    $second.safety.account_class -eq 'LOCAL_SIMULATION' -and
    $second.safety.instrument -eq 'MNQ SEP26' -and
    $second.safety.maximum_quantity -eq 1 -and
    $second.safety.paper_execution -eq 'DISARMED' -and
    $second.safety.session_armed_state -eq 'DISARMED' -and
    $second.safety.live_capital -eq 'DENIED' -and
    $second.safety.current_position -eq 'FLAT' -and
    $second.safety.current_quantity -eq 0 -and
    $second.safety.working_owned_orders -eq 0 -and
    $second.safety.working_entry_orders -eq 0 -and
    $second.safety.reconciliation_current -eq $true
)
$verified = (
    $first.collector_id -eq $second.collector_id -and
    $first.expected_source_fingerprint -eq $expectedFingerprint -and
    $first.loaded_source_fingerprint -eq $expectedFingerprint -and
    $second.loaded_source_fingerprint -eq $expectedFingerprint -and
    $first.producer_session_id -eq $second.producer_session_id -and
    $first.signature_verified -eq $true -and $second.signature_verified -eq $true -and
    $first.transport -eq 'HEALTHY' -and $second.transport -eq 'HEALTHY' -and
    $second.durable_records -gt $first.durable_records -and
    $second.journal_tip_sha256 -ne $first.journal_tip_sha256 -and
    $second.last_fsync_at -gt $first.last_fsync_at -and
    $first.tail.usable_depth -gt 0 -and $second.tail.usable_depth -gt 0 -and
    $first.rejected_records -eq 0 -and $second.rejected_records -eq 0 -and
    $first.duplicate_records -eq 0 -and $second.duplicate_records -eq 0 -and
    $first.queue_overflow_records -eq 0 -and $second.queue_overflow_records -eq 0 -and
    $observerVerified -and $safetyVerified
)

$result = [ordered]@{
    schema = 'beelzebub-unattended-ninjatrader-observer-commission-v1'
    milestone = if ($verified) { 'UNATTENDED_BUILD_AND_FEED_VERIFIED' } else { 'UNATTENDED_BUILD_AND_FEED_NOT_VERIFIED' }
    verified = $verified
    validation = $validation
    deployment = [ordered]@{
        install_requested = [bool]$InstallSource
        installed_source_parity = $true
        compile_status = $compileStatus
        expected_source_fingerprint = $expectedFingerprint
        compiled_assembly = $compiledAssembly
        compiled_at = (Get-Item -LiteralPath $compiledAssembly).LastWriteTimeUtc.ToString('o')
        compiled_sha256_before = $assemblyHashBefore
        compiled_sha256 = Get-Sha256 $compiledAssembly
    }
    single_attached_producer = $observerVerified
    checkpoint_1 = $first
    checkpoint_2 = $second
    safety_verified_disarmed = $safetyVerified
}
$result | ConvertTo-Json -Depth 8
if (-not $verified) { exit 2 }
