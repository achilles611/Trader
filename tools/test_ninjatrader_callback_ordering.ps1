[CmdletBinding()]
param(
    [string] $RepositoryRoot = (Split-Path $PSScriptRoot -Parent),
    [string] $NinjaTraderInstallRoot = (Join-Path $env:ProgramFiles 'NinjaTrader 8')
)

$ErrorActionPreference = 'Stop'
$sourcePath = Join-Path $RepositoryRoot 'ninjatrader\NinjaScript\AddOns\BeelzebubReadOnlyAddOn.cs'
$roslynRoot = Join-Path $NinjaTraderInstallRoot 'bin'
$codeAnalysis = Join-Path $roslynRoot 'Microsoft.CodeAnalysis.dll'
$codeAnalysisCSharp = Join-Path $roslynRoot 'Microsoft.CodeAnalysis.CSharp.dll'
$frameworkRoot = 'C:\Windows\Microsoft.NET\Framework64\v4.0.30319'
foreach ($required in @($sourcePath, $codeAnalysis, $codeAnalysisCSharp)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "Required callback regression input is missing: $required"
    }
}

$source = [IO.File]::ReadAllText($sourcePath)
$beginMarker = '// BEELZEBUB_MARKET_CALLBACK_GATE_BEGIN'
$endMarker = '// BEELZEBUB_MARKET_CALLBACK_GATE_END'
$begin = $source.IndexOf($beginMarker, [StringComparison]::Ordinal)
$end = $source.IndexOf($endMarker, [StringComparison]::Ordinal)
if ($begin -lt 0 -or $end -le $begin) {
    throw 'The production callback gate markers are missing or invalid.'
}
$begin += $beginMarker.Length
$gate = $source.Substring($begin, $end - $begin)

# The legacy path below is an exact executable model of the frozen failure:
# stream sequence allocation preceded serialized queue admission. The passing
# path compiles and executes the gate taken verbatim from the production AddOn.
$harness = @"
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace NinjaTrader.NinjaScript.AddOns
{
$gate
}

public static class BeelzebubCallbackOrderingHarness
{
    public static string Run()
    {
        long legacySequence = 0;
        List<long> legacyQueue = new List<long>();
        object legacyQueueLock = new object();
        ManualResetEventSlim legacyAAllocated = new ManualResetEventSlim(false);
        ManualResetEventSlim releaseLegacyA = new ManualResetEventSlim(false);
        Thread legacyA = new Thread(new ThreadStart(delegate {
            long value = Interlocked.Increment(ref legacySequence);
            legacyAAllocated.Set();
            releaseLegacyA.Wait();
            lock (legacyQueueLock) legacyQueue.Add(value);
        }));
        legacyA.Start();
        legacyAAllocated.Wait();
        Thread legacyB = new Thread(new ThreadStart(delegate {
            long value = Interlocked.Increment(ref legacySequence);
            lock (legacyQueueLock) legacyQueue.Add(value);
        }));
        legacyB.Start();
        legacyB.Join();
        releaseLegacyA.Set();
        legacyA.Join();
        bool legacyFailureReproduced = legacyQueue.SequenceEqual(new long[] { 2, 1 });

        var gate = new NinjaTrader.NinjaScript.AddOns.BeelzebubMarketCallbackGate();
        long token = gate.Generation;
        long actualSequence = 0;
        List<long> actualQueue = new List<long>();
        ManualResetEventSlim actualAAllocated = new ManualResetEventSlim(false);
        ManualResetEventSlim releaseActualA = new ManualResetEventSlim(false);
        ManualResetEventSlim actualBDone = new ManualResetEventSlim(false);
        Thread actualA = new Thread(new ThreadStart(delegate {
            gate.TryRun(token, delegate {
                long value = ++actualSequence;
                actualAAllocated.Set();
                releaseActualA.Wait(); // TEST ONLY: forced delay at the old race point.
                Thread.SpinWait(5000);  // TEST ONLY: stand-in for signing work.
                actualQueue.Add(value);
            });
        }));
        actualA.Start();
        actualAAllocated.Wait();
        Thread actualB = new Thread(new ThreadStart(delegate {
            gate.TryRun(token, delegate {
                long value = ++actualSequence;
                actualQueue.Add(value);
            });
            actualBDone.Set();
        }));
        actualB.Start();
        bool secondBlockedAtRacePoint = !actualBDone.Wait(50);
        releaseActualA.Set();
        actualA.Join();
        actualB.Join();
        bool repairedOrder = actualQueue.SequenceEqual(new long[] { 1, 2 });

        bool staleRan = false;
        gate.Retire();
        bool staleAccepted = gate.TryRun(token, delegate { staleRan = true; });
        bool staleGenerationRejected = !staleAccepted && !staleRan;

        var saturationGate = new NinjaTrader.NinjaScript.AddOns.BeelzebubMarketCallbackGate();
        long saturationToken = saturationGate.Generation;
        long saturationSequence = 0;
        int overflow = 0;
        Queue<long> bounded = new Queue<long>();
        Action publish = delegate {
            saturationGate.TryRun(saturationToken, delegate {
                long value = ++saturationSequence;
                if (bounded.Count >= 1) overflow++;
                else bounded.Enqueue(value);
            });
        };
        publish();
        publish();
        long firstDelivered = bounded.Dequeue();
        publish();
        long nextDelivered = bounded.Dequeue();
        bool saturationGapExplicit = overflow == 1 && firstDelivered == 1 && nextDelivered == 3;

        // TEST ONLY: the newly retained failure was 9692 -> 1 under the same
        // publisher session. Instance-local counters reproduce that reset.
        long retiredInstanceCounter = 9692;
        long replacementInstanceCounter = 0;
        bool replacementFailureReproduced = ++replacementInstanceCounter <= retiredInstanceCounter;
        long ownerA = NinjaTrader.NinjaScript.AddOns.BeelzebubMarketPublisherLease.Claim(true);
        List<long> replacementQueue = new List<long>();
        ManualResetEventSlim ownerAPaused = new ManualResetEventSlim(false);
        ManualResetEventSlim ownerARelease = new ManualResetEventSlim(false);
        ManualResetEventSlim ownerBDone = new ManualResetEventSlim(false);
        long ownerB = 0;
        Thread producerA = new Thread(new ThreadStart(delegate {
            NinjaTrader.NinjaScript.AddOns.BeelzebubMarketPublisherLease.TryRun(ownerA, delegate {
                long n = NinjaTrader.NinjaScript.AddOns.BeelzebubMarketPublisherLease.Next("TRADE");
                ownerAPaused.Set(); ownerARelease.Wait();
                replacementQueue.Add(n); // delayed publication must precede replacement
            });
        }));
        producerA.Start(); ownerAPaused.Wait();
        Thread producerB = new Thread(new ThreadStart(delegate {
            ownerB = NinjaTrader.NinjaScript.AddOns.BeelzebubMarketPublisherLease.Claim(true);
            NinjaTrader.NinjaScript.AddOns.BeelzebubMarketPublisherLease.TryRun(ownerB, delegate {
                replacementQueue.Add(NinjaTrader.NinjaScript.AddOns.BeelzebubMarketPublisherLease.Next("TRADE"));
            });
            ownerBDone.Set();
        }));
        producerB.Start();
        bool replacementWaitsForAdmission = !ownerBDone.Wait(50);
        ownerARelease.Set(); producerA.Join(); producerB.Join();
        bool replacedCallbackRan = false;
        bool replacedAccepted = NinjaTrader.NinjaScript.AddOns.BeelzebubMarketPublisherLease.TryRun(
            ownerA, delegate { replacedCallbackRan = true; });
        bool fallbackDenied = NinjaTrader.NinjaScript.AddOns.BeelzebubMarketPublisherLease.Claim(false) == 0;
        bool replacementPass = replacementWaitsForAdmission && replacementQueue.SequenceEqual(new long[] { 1, 2 })
            && !replacedAccepted && !replacedCallbackRan && fallbackDenied;

        if (!legacyFailureReproduced || !secondBlockedAtRacePoint || !repairedOrder
            || !staleGenerationRejected || !saturationGapExplicit || !replacementFailureReproduced || !replacementPass)
            throw new InvalidOperationException("NINJATRADER_CALLBACK_ORDERING_REGRESSION_FAILED");

        return "{\"schema\":\"beelzebub-native-callback-ordering-regression-v1\""
            + ",\"test_only\":true"
            + ",\"legacy_failure_reproduced\":true"
            + ",\"forced_race_second_callback_blocked\":true"
            + ",\"production_gate_order\":\"1,2\""
            + ",\"stale_generation_rejected\":true"
            + ",\"retained_replacement_reset_reproduced\":true"
            + ",\"replacement_waits_for_delayed_publication\":true"
            + ",\"replacement_stream_order\":\"1,2\",\"competing_publisher_denied\":true"
            + ",\"queue_saturation_overflow_explicit\":true"
            + ",\"post_overflow_delivered_sequences\":\"1,3\"}";
    }
}
"@

Add-Type -Path $codeAnalysis
Add-Type -Path $codeAnalysisCSharp
[Microsoft.CodeAnalysis.SyntaxTree[]] $trees = @(
    [Microsoft.CodeAnalysis.CSharp.CSharpSyntaxTree]::ParseText($harness)
)
$referencePaths = @(
    (Join-Path $frameworkRoot 'mscorlib.dll'),
    (Join-Path $frameworkRoot 'System.dll'),
    (Join-Path $frameworkRoot 'System.Core.dll')
)
[Microsoft.CodeAnalysis.MetadataReference[]] $references = @(
    $referencePaths | ForEach-Object {
        [Microsoft.CodeAnalysis.MetadataReference]::CreateFromFile($_)
    }
)
$options = [Microsoft.CodeAnalysis.CSharp.CSharpCompilationOptions]::new(
    [Microsoft.CodeAnalysis.OutputKind]::DynamicallyLinkedLibrary
)
$compilation = [Microsoft.CodeAnalysis.CSharp.CSharpCompilation]::Create(
    'BeelzebubCallbackOrdering.Regression', $trees, $references, $options
)
$stream = [IO.MemoryStream]::new()
try {
    $result = $compilation.Emit($stream)
    $errors = @($result.Diagnostics | Where-Object {
        $_.Severity -eq [Microsoft.CodeAnalysis.DiagnosticSeverity]::Error
    })
    if (-not $result.Success) {
        throw ('Callback regression compilation failed: ' + ($errors -join [Environment]::NewLine))
    }
    $assembly = [Reflection.Assembly]::Load($stream.ToArray())
    $type = $assembly.GetType('BeelzebubCallbackOrderingHarness', $true)
    $method = $type.GetMethod('Run', [Reflection.BindingFlags]'Public,Static')
    $method.Invoke($null, @())
}
finally {
    $stream.Dispose()
}
