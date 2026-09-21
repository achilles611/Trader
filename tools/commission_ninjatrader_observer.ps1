[CmdletBinding()]
param(
    [switch] $InstallSource,
    [int] $Port = 8090,
    [int] $CompileTimeoutSeconds = 60,
    [int] $CheckpointSeconds = 10,
    [switch] $RequireDistinctProducer,
    [string] $PreReloadReceiptPath,
    [string] $EvidencePath,
    [ValidateSet('MNQ SEP26', 'MNQ DEC26')]
    [string] $ExpectedInstrument = 'MNQ SEP26',
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
if ($RequireDistinctProducer -and [String]::IsNullOrWhiteSpace($PreReloadReceiptPath)) {
    throw 'RequireDistinctProducer requires -PreReloadReceiptPath.'
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

function Get-LoadedAssemblyMatches([string] $BuildFingerprint) {
    $temporaryRoot = Join-Path $NinjaTraderDocumentsRoot 'tmp'
    if (-not (Test-Path -LiteralPath $temporaryRoot -PathType Container)) { return @() }
    return @(
        Get-ChildItem -LiteralPath $temporaryRoot -File -Filter '*.dll' -ErrorAction Stop |
            ForEach-Object {
                $hash = Get-Sha256 $_.FullName
                if ($hash -eq $BuildFingerprint) {
                    [ordered]@{
                        path = $_.FullName
                        sha256 = $hash
                        bytes = $_.Length
                        last_write_utc = $_.LastWriteTimeUtc.ToString('o')
                    }
                }
            }
    )
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
    # The native compiler briefly holds an exclusive lock while replacing it.
    # Treat that as not-yet-verified within the existing bounded wait.
    try { $bytes = [IO.File]::ReadAllBytes($compiledAssembly) }
    catch [IO.IOException] { return $false }
    $unicode = [Text.Encoding]::Unicode.GetString($bytes)
    $utf8 = [Text.Encoding]::UTF8.GetString($bytes)
    return (
        $unicode.Contains($ExpectedFingerprint) -and
        $utf8.Contains('BeelzebubAutomaticMarketObserver') -and
        $utf8.Contains('ShiftKnownPositions') -and
        $utf8.Contains('DepthPrefix')
    )
}

function Get-ObserverInstallText {
    param([string] $SourceText, [string] $CurrentText = '', [switch] $PreserveGeneratedRegion)
    $marker = '#region NinjaScript generated code. Neither change nor remove.'
    $source = $SourceText.Replace("`r`n", "`n")
    if ($source.Contains($marker)) {
        throw 'Repository observer must not contain NinjaTrader-generated wrappers.'
    }
    $result = $source.TrimEnd() + "`n"
    if ($PreserveGeneratedRegion) {
        $current = $CurrentText.Replace("`r`n", "`n")
        $index = $current.IndexOf($marker, [StringComparison]::Ordinal)
        if ($index -ge 0) {
            $suffix = $current.Substring($index)
            $pattern = '(?ms)^#region NinjaScript generated code\. Neither change nor remove\.\n.*?^#endregion[ \t]*(?=\n|\z)'
            $blocks = [regex]::Matches($suffix, $pattern)
            if ($blocks.Count -eq 0 -or -not [string]::IsNullOrWhiteSpace([regex]::Replace($suffix, $pattern, ''))) {
                throw 'Malformed generated wrapper suffix; refusing to discard unknown source.'
            }
            $first = $blocks[0].Value.TrimEnd()
            foreach ($block in $blocks) {
                if ($block.Value.TrimEnd() -cne $first) {
                    throw 'Nonidentical generated wrappers; manual review required.'
                }
            }
            # Retain one exact wrapper, never the entire potentially duplicated suffix.
            $result += "`n" + $first + "`n"
        }
    }
    # Keep deployed files in the native Windows editor's line-ending format.
    return $result.Replace("`n", "`r`n")
}

function Install-ObserverSource {
    param([string] $Source, [string] $Target, [switch] $PreserveGeneratedRegion)
    $targetDirectory = Split-Path -Parent $Target
    [IO.Directory]::CreateDirectory($targetDirectory) | Out-Null
    $current = if (Test-Path -LiteralPath $Target) { [IO.File]::ReadAllText($Target) } else { '' }
    $text = Get-ObserverInstallText -SourceText ([IO.File]::ReadAllText($Source)) -CurrentText $current -PreserveGeneratedRegion:$PreserveGeneratedRegion
    if ($current -ceq $text) { return }
    if (Test-Path -LiteralPath $Target) {
        $backup = "$Target.unattended-before-$([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')).bak"
        Copy-Item -LiteralPath $Target -Destination $backup -ErrorAction Stop
    }
    [IO.File]::WriteAllText($Target, $text, [Text.UTF8Encoding]::new($false))
}

function Get-PaperStatus {
    return Invoke-RestMethod -Uri "http://127.0.0.1:$Port/api/lane-iii/paper" -TimeoutSec 45
}

function Get-ReceiverReadiness {
    $paper = Get-PaperStatus
    $listener = $paper.market_observer
    $collection = $paper.pong_collection
    return [ordered]@{
        observed_at = [DateTime]::UtcNow.ToString('o')
        listener_state = $listener.state
        listener_host = $listener.host
        listener_port = $listener.port
        listener_error = $listener.error
        collector_state = $collection.state
        collector_id = $collection.collector.collector_id
        collector_thread_alive = $collection.collector.thread_alive
        expected_source_fingerprint = $collection.collector.expected_source_fingerprint
        ready = (
            $listener.state -eq 'LISTENING' -and
            [int]$listener.port -eq 48135 -and
            $null -eq $listener.error -and
            $collection.state -eq 'RUNNING' -and
            $collection.collector.thread_alive -eq $true
        )
    }
}

function Get-TailCoverage([string] $JournalPath) {
    $counts = [ordered]@{
        records = 0; quote = 0; trade = 0; depth = 0
        usable_depth = 0; unavailable_depth = 0; partial_available_depth = 0
    }
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
            else {
                $bidCount = @($observation.payload.bids).Count
                $askCount = @($observation.payload.asks).Count
                if ($bidCount -eq 10 -and $askCount -eq 10) { $counts.usable_depth++ }
                else { $counts.partial_available_depth++ }
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
    $pong = $paper.pong_production
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
        producer_queue_overflow_records = $collection.durability.producer_queue_overflow_records
        producer_queue_overflow_baseline = $collection.durability.producer_queue_overflow_baseline
        producer_queue_overflow_delta = $collection.durability.producer_queue_overflow_delta
        producer_transport_failed_records = $collection.durability.producer_transport_failed_records
        producer_transport_failed_baseline = $collection.durability.producer_transport_failed_baseline
        producer_transport_failed_delta = $collection.durability.producer_transport_failed_delta
        source_generation = $pong.source_identity.source_generation
        stream_counts = $pong.stream_counts
        missing_events = $pong.missing_events
        out_of_order_events = $pong.out_of_order_events
        input_error = $pong.input_error
        host_observation_queue_overflow = $pong.observation_queue_overflow
        host_first_fault = $pong.persisted_host_telemetry.first_fault
        host_telemetry_write_error = $pong.persisted_host_telemetry.write_error
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

$preReload = $null
$preReloadReceiptSha256 = $null
$preReloadJournal = $null
$receiverReadiness = $null
if ($RequireDistinctProducer) {
    $resolvedPreReload = (Resolve-Path -LiteralPath $PreReloadReceiptPath).Path
    $preReloadReceiptSha256 = Get-Sha256 $resolvedPreReload
    $preReload = Get-Content -LiteralPath $resolvedPreReload -Raw | ConvertFrom-Json
    $resolvedPreReloadJournal = (Resolve-Path -LiteralPath $preReload.journal.path).Path
    $preReloadJournalItem = Get-Item -LiteralPath $resolvedPreReloadJournal
    $preReloadJournal = [ordered]@{
        path = $resolvedPreReloadJournal
        bytes = $preReloadJournalItem.Length
        sha256 = Get-Sha256 $resolvedPreReloadJournal
    }
    if (
        $preReload.authority -ne 'OBSERVE_ONLY' -or
        $preReload.commands -ne 0 -or
        $null -eq $preReload.producer_boundary.identity -or
        [String]::IsNullOrWhiteSpace([string]$preReload.producer_boundary.expected_source_fingerprint) -or
        $preReload.producer_boundary.identity.source_fingerprint -ne $preReload.producer_boundary.expected_source_fingerprint -or
        $preReload.producer_boundary.identity_changes.Count -ne 0 -or
        $preReload.producer_boundary.rejected_observations -ne 0 -or
        $preReloadJournal.bytes -ne $preReload.journal.bytes -or
        $preReloadJournal.sha256 -ne $preReload.journal.sha256
    ) {
        throw 'Pre-reload producer receipt is not one stable authenticated observe-only identity.'
    }
    $receiverReadiness = Get-ReceiverReadiness
    if ($receiverReadiness.ready -ne $true) {
        throw 'Listener and independent collector must be ready before the native reload boundary.'
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
if (
    $RequireDistinctProducer -and
    $preReload.producer_boundary.identity.source_fingerprint -eq $expectedFingerprint
) {
    throw 'Pre-reload receipt already carries the candidate source; a distinct replacement was not demonstrated.'
}

$installedGeneratedValid = $false
if (Test-Path -LiteralPath $installedIndicator) {
    $installedText = [IO.File]::ReadAllText($installedIndicator)
    $preparedText = Get-ObserverInstallText -SourceText ([IO.File]::ReadAllText($repositoryIndicator)) -CurrentText $installedText -PreserveGeneratedRegion
    $installedGeneratedValid = $installedText.Replace("`r`n", "`n").TrimEnd() -ceq $preparedText.Replace("`r`n", "`n").TrimEnd()
}
$installedCurrent = (
    (Test-Path -LiteralPath $installedAddOn) -and
    (Test-Path -LiteralPath $installedIndicator) -and
    (Get-CanonicalSource $repositoryAddOn) -ceq (Get-CanonicalSource $installedAddOn) -and
    (Get-CanonicalSource $repositoryIndicator) -ceq (Get-CanonicalSource $installedIndicator -WithoutGeneratedRegion) -and
    $installedGeneratedValid
)
$assemblyBefore = Get-Item -LiteralPath $compiledAssembly
$assemblyHashBefore = Get-Sha256 $compiledAssembly
$compileStatus = 'SKIPPED_CURRENT'

if (-not $installedCurrent) {
    if (-not $InstallSource) {
        throw 'Installed observer source differs from the repository. Re-run with -InstallSource.'
    }
    if (-not (Test-Path -LiteralPath $installedAddOn) -or
        (Get-CanonicalSource $repositoryAddOn) -cne (Get-CanonicalSource $installedAddOn)) {
        Install-ObserverSource -Source $repositoryAddOn -Target $installedAddOn
    }
    Install-ObserverSource -Source $repositoryIndicator -Target $installedIndicator -PreserveGeneratedRegion
    $compileStatus = 'WAITING_FOR_NINJATRADER_SOURCE_WATCHER'
    $installedText = [IO.File]::ReadAllText($installedIndicator)
    $preparedText = Get-ObserverInstallText -SourceText ([IO.File]::ReadAllText($repositoryIndicator)) -CurrentText $installedText -PreserveGeneratedRegion
    $installedGeneratedValid = $installedText.Replace("`r`n", "`n").TrimEnd() -ceq $preparedText.Replace("`r`n", "`n").TrimEnd()
    $installedCurrent = (
        (Get-CanonicalSource $repositoryAddOn) -ceq (Get-CanonicalSource $installedAddOn) -and
        (Get-CanonicalSource $repositoryIndicator) -ceq (Get-CanonicalSource $installedIndicator -WithoutGeneratedRegion) -and
        $installedGeneratedValid
    )
    if (-not $installedCurrent) {
        throw 'Installed observer source does not match the repository after the supported installation step.'
    }
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
$loadedAssemblyMatches = Get-LoadedAssemblyMatches $second.loaded_build_fingerprint
$streamContinuityVerified = (
    @($second.missing_events.PSObject.Properties.Value | Where-Object { [int64]$_ -ne 0 }).Count -eq 0 -and
    @($second.out_of_order_events.PSObject.Properties.Value | Where-Object { [int64]$_ -ne 0 }).Count -eq 0
)
$producerLossVerified = (
    $first.producer_queue_overflow_records -eq 0 -and
    $second.producer_queue_overflow_records -eq 0 -and
    $first.producer_queue_overflow_baseline -eq 0 -and
    $second.producer_queue_overflow_baseline -eq 0 -and
    $first.producer_queue_overflow_delta -eq 0 -and
    $second.producer_queue_overflow_delta -eq 0 -and
    $first.producer_transport_failed_records -eq 0 -and
    $second.producer_transport_failed_records -eq 0 -and
    $first.producer_transport_failed_baseline -eq 0 -and
    $second.producer_transport_failed_baseline -eq 0 -and
    $first.producer_transport_failed_delta -eq 0 -and
    $second.producer_transport_failed_delta -eq 0
)
$recoveryWindowStable = (
    $second.rejected_records -eq $first.rejected_records -and
    $second.duplicate_records -eq $first.duplicate_records -and
    $second.queue_overflow_records -eq $first.queue_overflow_records
)
$hostFaultFree = (
    $null -eq $first.input_error -and $null -eq $second.input_error -and
    $first.host_observation_queue_overflow -eq 0 -and
    $second.host_observation_queue_overflow -eq 0 -and
    $null -eq $first.host_first_fault -and $null -eq $second.host_first_fault -and
    $null -eq $first.host_telemetry_write_error -and
    $null -eq $second.host_telemetry_write_error
)
$distinctProducerVerified = (
    -not $RequireDistinctProducer -or (
        $preReload.producer_boundary.identity.session_id -ne $first.producer_session_id -and
        $preReload.producer_boundary.identity.build_fingerprint -ne $first.loaded_build_fingerprint -and
        $preReload.producer_boundary.identity.source_fingerprint -ne $first.loaded_source_fingerprint
    )
)
$observerVerified = (
    $second.observer.state -eq 'NATIVE_ADDON_OBSERVER_ACTIVE' -and
    $second.observer.instrument -eq $ExpectedInstrument -and
    $second.observer.observer_attached -eq $true -and
    $second.observer.subscription_mode -eq 'NATIVE_ADDON' -and
    $second.observer.active -eq $true
)
$safetyVerified = (
    $second.safety.state -eq 'READY_DISARMED' -and
    $second.safety.account -eq 'Sim101' -and
    $second.safety.account_class -eq 'LOCAL_SIMULATION' -and
    $second.safety.instrument -eq $ExpectedInstrument -and
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
    $first.loaded_build_fingerprint -eq $second.loaded_build_fingerprint -and
    $first.loaded_build_timestamp -eq $second.loaded_build_timestamp -and
    $loadedAssemblyMatches.Count -gt 0 -and
    $first.source_generation -gt 0 -and
    $first.source_generation -eq $second.source_generation -and
    $first.signature_verified -eq $true -and $second.signature_verified -eq $true -and
    $first.transport -eq 'HEALTHY' -and $second.transport -eq 'HEALTHY' -and
    $second.durable_records -gt $first.durable_records -and
    $second.journal_tip_sha256 -ne $first.journal_tip_sha256 -and
    $second.last_fsync_at -gt $first.last_fsync_at -and
    $first.tail.usable_depth -gt 0 -and $second.tail.usable_depth -gt 0 -and
    $first.tail.partial_available_depth -eq 0 -and
    $second.tail.partial_available_depth -eq 0 -and
    $producerLossVerified -and $recoveryWindowStable -and
    $streamContinuityVerified -and $hostFaultFree -and
    $distinctProducerVerified -and
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
        expected_instrument = $ExpectedInstrument
        receiver_ready_before_reload = $receiverReadiness
        pre_reload_receipt_path = if ($RequireDistinctProducer) { (Resolve-Path -LiteralPath $PreReloadReceiptPath).Path } else { $null }
        pre_reload_receipt_sha256 = $preReloadReceiptSha256
        pre_reload_journal = $preReloadJournal
        pre_reload_producer = if ($RequireDistinctProducer) { $preReload.producer_boundary } else { $null }
        compiled_assembly = $compiledAssembly
        compiled_at = (Get-Item -LiteralPath $compiledAssembly).LastWriteTimeUtc.ToString('o')
        compiled_sha256_before = $assemblyHashBefore
        compiled_sha256 = Get-Sha256 $compiledAssembly
        loaded_assembly_matches = $loadedAssemblyMatches
        loaded_build_differs_from_compiled_file = (
            $second.loaded_build_fingerprint -ne (Get-Sha256 $compiledAssembly)
        )
    }
    single_attached_producer = $observerVerified
    checkpoint_1 = $first
    checkpoint_2 = $second
    safety_verified_disarmed = $safetyVerified
    distinct_producer_verified = $distinctProducerVerified
    producer_loss_verified = $producerLossVerified
    recovery_window_stable = $recoveryWindowStable
    stream_continuity_verified = $streamContinuityVerified
    host_fault_free = $hostFaultFree
    recovery_window = [ordered]@{
        baseline = 'checkpoint_1'
        current = 'checkpoint_2'
        next_gate_baseline = 'checkpoint_2'
        rejected_records = [ordered]@{
            baseline = $first.rejected_records
            current = $second.rejected_records
            delta = ([int64]$second.rejected_records - [int64]$first.rejected_records)
        }
        duplicate_records = [ordered]@{
            baseline = $first.duplicate_records
            current = $second.duplicate_records
            delta = ([int64]$second.duplicate_records - [int64]$first.duplicate_records)
        }
        collector_queue_overflow_records = [ordered]@{
            baseline = $first.queue_overflow_records
            current = $second.queue_overflow_records
            delta = ([int64]$second.queue_overflow_records - [int64]$first.queue_overflow_records)
        }
        clean_corpus_boundary = 'AFTER_CHECKPOINT_1'
        gate_b_deltas_begin_at = 'CHECKPOINT_2'
    }
    old_epoch_tail_disposition = if ($RequireDistinctProducer) {
        'FAILED_UNKNOWN_PENDING_TAIL_NOT_DRAIN_PROOF'
    } else { $null }
}
$resultJson = $result | ConvertTo-Json -Depth 10
if (-not [String]::IsNullOrWhiteSpace($EvidencePath)) {
    $target = [IO.Path]::GetFullPath($EvidencePath)
    [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($target)) | Out-Null
    $temporary = "$target.tmp-$([Guid]::NewGuid().ToString('N'))"
    try {
        [IO.File]::WriteAllText($temporary, $resultJson + "`n", [Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $temporary -Destination $target -Force
    }
    finally {
        if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force }
    }
}
$resultJson
if (-not $verified) { exit 2 }
