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
        throw "Required lifecycle regression input is missing: $required"
    }
}

$source = [IO.File]::ReadAllText($sourcePath).Replace("`r`n", "`n")
$classStart = $source.IndexOf('    public static class BeelzebubReadOnlyOutbound', [StringComparison]::Ordinal)
$classEnd = $source.LastIndexOf("`n    }`n}", [StringComparison]::Ordinal)
if ($classStart -lt 0 -or $classEnd -le $classStart) {
    throw 'The production outbound class boundary is missing or invalid.'
}
$outbound = $source.Substring($classStart, $classEnd + "`n    }".Length - $classStart)

# TEST ONLY: compile the production lifecycle code against a deterministic,
# in-memory transport. No socket is created and no NinjaTrader process, native
# feed, installation, ledger or runtime is touched. The teardown timeout is
# shortened only in this extracted test assembly so its timeout branch is
# exercised without waiting five seconds.
$outbound = $outbound.Replace('TcpClient', 'TestTcpClient')
$outbound = $outbound.Replace('NetworkStream', 'TestNetworkStream')
$outbound = $outbound.Replace(
    'private const int SenderTeardownTimeoutMilliseconds = 5000;',
    'private const int SenderTeardownTimeoutMilliseconds = 100;'
)

$harness = @"
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Web.Script.Serialization;

namespace NinjaTrader.Core
{
    public sealed class TestGeneralOptions
    {
        public TimeZoneInfo TimeZoneInfo { get { return TimeZoneInfo.Utc; } }
    }

    public static class Globals
    {
        public static readonly TestGeneralOptions GeneralOptions = new TestGeneralOptions();
    }
}

namespace NinjaTrader.NinjaScript.AddOns
{
    public static class BeelzebubMarketPublisherLease
    {
        public static long CurrentGeneration { get { return 1; } }
    }

    public static class BeelzebubProviderTimestamp
    {
        public static bool TryConvertUtc(
            DateTime value, TimeZoneInfo source, TimeZoneInfo destination,
            out DateTime utc, out string reason)
        {
            utc = value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime();
            reason = null;
            return true;
        }
    }

    public enum TestTransportMode
    {
        Success,
        BlockConnect,
        BlockWrite,
        IgnoreCloseWrite,
        FailWrite
    }

    public static class TestTransport
    {
        private static readonly object Sync = new object();
        private static readonly Queue<TestTransportMode> Modes = new Queue<TestTransportMode>();
        public static readonly ManualResetEventSlim ConnectEntered = new ManualResetEventSlim(false);
        public static readonly ManualResetEventSlim WriteEntered = new ManualResetEventSlim(false);
        public static readonly ManualResetEventSlim ReleaseWrite = new ManualResetEventSlim(false);
        public static readonly List<string> Writes = new List<string>();

        public static void Configure(params TestTransportMode[] modes)
        {
            lock (Sync)
            {
                Modes.Clear();
                foreach (TestTransportMode mode in modes) Modes.Enqueue(mode);
                Writes.Clear();
            }
            ConnectEntered.Reset();
            WriteEntered.Reset();
            ReleaseWrite.Reset();
        }

        public static TestTransportMode NextMode()
        {
            lock (Sync) return Modes.Count == 0 ? TestTransportMode.Success : Modes.Dequeue();
        }

        public static void Record(string value)
        {
            lock (Sync) Writes.Add(value);
        }

        public static string WrittenText()
        {
            lock (Sync) return String.Concat(Writes);
        }
    }

    public sealed class TestTcpClient
    {
        private readonly TestTransportMode mode = TestTransport.NextMode();
        private readonly ManualResetEventSlim closed = new ManualResetEventSlim(false);
        public int SendTimeout { get; set; }

        public void Connect(IPAddress address, int port)
        {
            TestTransport.ConnectEntered.Set();
            if (mode == TestTransportMode.BlockConnect)
            {
                closed.Wait();
                throw new IOException("test connect closed");
            }
            if (closed.IsSet) throw new ObjectDisposedException("TestTcpClient");
        }

        public TestNetworkStream GetStream()
        {
            return new TestNetworkStream(mode, closed);
        }

        public void Close()
        {
            if (mode != TestTransportMode.IgnoreCloseWrite)
                closed.Set();
        }
    }

    public sealed class TestNetworkStream
    {
        private readonly TestTransportMode mode;
        private readonly ManualResetEventSlim closed;

        public TestNetworkStream(TestTransportMode mode, ManualResetEventSlim closed)
        {
            this.mode = mode;
            this.closed = closed;
        }

        public void Write(byte[] bytes, int offset, int count)
        {
            TestTransport.WriteEntered.Set();
            if (mode == TestTransportMode.BlockWrite)
                closed.Wait();
            else if (mode == TestTransportMode.IgnoreCloseWrite)
                TestTransport.ReleaseWrite.Wait();
            if (mode == TestTransportMode.FailWrite)
                throw new IOException("test disconnect");
            if (closed.IsSet || mode == TestTransportMode.IgnoreCloseWrite)
                throw new IOException("test write closed");
            TestTransport.Record(Encoding.UTF8.GetString(bytes, offset, count));
        }
    }

$outbound
}

public static class BeelzebubOutboundLifecycleHarness
{
    private static readonly Type Outbound = typeof(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound);
    private static readonly MethodInfo Enqueue = Outbound.GetMethod("EnqueueLocked", BindingFlags.NonPublic | BindingFlags.Static);
    private static readonly object QueueLock = Outbound.GetField(
        "outboundFramesLock", BindingFlags.NonPublic | BindingFlags.Static).GetValue(null);

    private static bool Put(string frame)
    {
        lock (QueueLock)
            return (bool)Enqueue.Invoke(null, new object[] { frame });
    }

    private static long Counter(string name)
    {
        return (long)Outbound.GetField(name, BindingFlags.NonPublic | BindingFlags.Static).GetValue(null);
    }

    private static bool Flag(string name)
    {
        return (bool)Outbound.GetField(name, BindingFlags.NonPublic | BindingFlags.Static).GetValue(null);
    }

    private static void Require(bool value, string code)
    {
        if (!value) throw new InvalidOperationException(code);
    }

    private static void WaitUntil(Func<bool> condition, string code)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(2);
        while (!condition() && DateTime.UtcNow < deadline) Thread.Sleep(1);
        Require(condition(), code);
    }

    public static string Run()
    {
        // Stop during a blocked Connect closes the exact active owner, joins,
        // accounts the dequeued frame as failed, and refuses later enqueue.
        NinjaTrader.NinjaScript.AddOns.TestTransport.Configure(
            NinjaTrader.NinjaScript.AddOns.TestTransportMode.BlockConnect);
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Start(), "START_CONNECT");
        long connectFailures = Counter("transportFailedFrames");
        long connectDiscarded = Counter("senderShutdownDiscardedFrames");
        Require(Put("connect-blocked\n"), "ENQUEUE_CONNECT");
        Require(NinjaTrader.NinjaScript.AddOns.TestTransport.ConnectEntered.Wait(2000), "CONNECT_NOT_ENTERED");
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Shutdown(), "CONNECT_STOP_INCOMPLETE");
        Require(Counter("transportFailedFrames") == connectFailures + 1, "CONNECT_FAILURE_NOT_ACCOUNTED");
        Require(Counter("senderShutdownDiscardedFrames") == connectDiscarded + 1, "CONNECT_STOP_NOT_ACCOUNTED");
        long postStopFailures = Counter("transportFailedFrames");
        long postStopDiscarded = Counter("senderShutdownDiscardedFrames");
        Require(!Put("post-stop\n"), "POST_STOP_ENQUEUE_ACCEPTED");
        Require(Counter("transportFailedFrames") == postStopFailures + 1, "POST_STOP_FAILURE_NOT_ACCOUNTED");
        Require(Counter("senderShutdownDiscardedFrames") == postStopDiscarded + 1, "POST_STOP_DISCARD_NOT_ACCOUNTED");
        Require(!Flag("senderRunning"), "POST_CONNECT_SENDER_RUNNING");
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Shutdown(), "REPEATED_STOP_FAILED");

        // Stop during a blocked Write closes that exact client and releases the
        // writer without waiting for the socket timeout.
        NinjaTrader.NinjaScript.AddOns.TestTransport.Configure(
            NinjaTrader.NinjaScript.AddOns.TestTransportMode.BlockWrite);
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Start(), "START_WRITE");
        long writeFailures = Counter("transportFailedFrames");
        long writeDiscarded = Counter("senderShutdownDiscardedFrames");
        Require(Put("write-blocked\n"), "ENQUEUE_WRITE");
        Require(NinjaTrader.NinjaScript.AddOns.TestTransport.WriteEntered.Wait(2000), "BLOCKED_WRITE_NOT_ENTERED");
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Shutdown(), "WRITE_STOP_INCOMPLETE");
        Require(Counter("transportFailedFrames") == writeFailures + 1, "WRITE_FAILURE_NOT_ACCOUNTED");
        Require(Counter("senderShutdownDiscardedFrames") == writeDiscarded + 1, "WRITE_STOP_NOT_ACCOUNTED");
        Require(NinjaTrader.NinjaScript.AddOns.TestTransport.WrittenText().Length == 0, "BLOCKED_WRITE_RECORDED");

        // Ordered writes survive a clean replacement generation. The output is
        // exactly the input sequence even if the sender chooses batch boundaries.
        NinjaTrader.NinjaScript.AddOns.TestTransport.Configure(
            NinjaTrader.NinjaScript.AddOns.TestTransportMode.Success);
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Start(), "RESTART_ORDER");
        Require(Put("one\n") && Put("two\n") && Put("three\n"), "ORDER_ENQUEUE");
        WaitUntil(delegate { return NinjaTrader.NinjaScript.AddOns.TestTransport.WrittenText() == "one\ntwo\nthree\n"; }, "ORDER_MISMATCH");
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Shutdown(), "ORDER_STOP_INCOMPLETE");

        // Stop with one in-flight and two pending frames. The deliberately
        // uninterruptible test write forces the bounded timeout. Replacement
        // remains refused until the old generation actually exits.
        NinjaTrader.NinjaScript.AddOns.TestTransport.Configure(
            NinjaTrader.NinjaScript.AddOns.TestTransportMode.IgnoreCloseWrite);
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Start(), "START_TIMEOUT");
        long pendingFailures = Counter("transportFailedFrames");
        long pendingDiscarded = Counter("senderShutdownDiscardedFrames");
        Require(Put("inflight\n"), "ENQUEUE_INFLIGHT");
        Require(NinjaTrader.NinjaScript.AddOns.TestTransport.WriteEntered.Wait(2000), "WRITE_NOT_ENTERED");
        Require(Put("pending-a\n") && Put("pending-b\n"), "ENQUEUE_PENDING");
        Require(!NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Shutdown(), "TIMEOUT_NOT_REPORTED");
        Require(!NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Start(), "GENERATION_REPLACED_DURING_TIMEOUT");
        Require(Counter("transportFailedFrames") == pendingFailures + 2, "PENDING_FAILURES_NOT_ACCOUNTED");
        Require(Counter("senderShutdownDiscardedFrames") == pendingDiscarded + 2, "PENDING_STOP_NOT_ACCOUNTED");
        NinjaTrader.NinjaScript.AddOns.TestTransport.ReleaseWrite.Set();
        WaitUntil(delegate { return !Flag("senderRunning"); }, "OLD_GENERATION_DID_NOT_EXIT");
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Shutdown(), "POST_TIMEOUT_STOP_INCOMPLETE");
        Require(Counter("transportFailedFrames") == pendingFailures + 3, "INFLIGHT_FAILURE_NOT_ACCOUNTED");
        Require(Counter("senderShutdownDiscardedFrames") == pendingDiscarded + 3, "INFLIGHT_STOP_NOT_ACCOUNTED");
        Require(NinjaTrader.NinjaScript.AddOns.TestTransport.WrittenText().Length == 0, "UNSENT_FRAME_RECORDED_AS_WRITE");

        // A disconnect is a transport failure, not delivery, and a later stop
        // completes without reconnect resurrection.
        NinjaTrader.NinjaScript.AddOns.TestTransport.Configure(
            NinjaTrader.NinjaScript.AddOns.TestTransportMode.FailWrite);
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Start(), "START_DISCONNECT");
        long disconnectFailures = Counter("transportFailedFrames");
        Require(Put("disconnect\n"), "ENQUEUE_DISCONNECT");
        WaitUntil(delegate { return Counter("transportFailedFrames") == disconnectFailures + 1; }, "DISCONNECT_NOT_ACCOUNTED");
        Require(NinjaTrader.NinjaScript.AddOns.TestTransport.WrittenText().Length == 0, "FAILED_WRITE_RECORDED");
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Shutdown(), "DISCONNECT_STOP_INCOMPLETE");
        Require(NinjaTrader.NinjaScript.AddOns.BeelzebubReadOnlyOutbound.Shutdown(), "SECOND_REPEATED_STOP_FAILED");

        return "{\"schema\":\"beelzebub-native-outbound-lifecycle-regression-v1\""
            + ",\"test_only\":true,\"network_opened\":false"
            + ",\"blocked_connect_stop\":true,\"blocked_write_stop\":true,\"ordered_write\":true"
            + ",\"pending_and_inflight_accounted\":true,\"bounded_timeout_reported\":true"
            + ",\"replacement_refused_until_owner_exit\":true,\"disconnect_accounted\":true"
            + ",\"post_stop_enqueue_refused\":true,\"repeated_stop_idempotent\":true"
            + ",\"unsent_delivery_claimed\":false}";
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
    (Join-Path $frameworkRoot 'System.Core.dll'),
    (Join-Path $frameworkRoot 'System.Web.Extensions.dll')
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
    'BeelzebubOutboundLifecycle.Regression', $trees, $references, $options
)
$stream = [IO.MemoryStream]::new()
try {
    $result = $compilation.Emit($stream)
    $errors = @($result.Diagnostics | Where-Object {
        $_.Severity -eq [Microsoft.CodeAnalysis.DiagnosticSeverity]::Error
    })
    if (-not $result.Success) {
        throw ('Outbound lifecycle regression compilation failed: ' + ($errors -join [Environment]::NewLine))
    }
    $assembly = [Reflection.Assembly]::Load($stream.ToArray())
    $type = $assembly.GetType('BeelzebubOutboundLifecycleHarness', $true)
    $method = $type.GetMethod('Run', [Reflection.BindingFlags]'Public,Static')
    try { $method.Invoke($null, @()) }
    catch [Reflection.TargetInvocationException] {
        throw $_.Exception.InnerException.ToString()
    }
}
finally {
    $stream.Dispose()
}
