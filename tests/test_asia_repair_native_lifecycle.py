import re
import unittest
from pathlib import Path

from src.l3f_provider.ninjatrader_observation import (
    expected_observer_source_fingerprint,
)


ROOT = Path(__file__).parents[1]
ADDON_PATH = (
    ROOT
    / "ninjatrader"
    / "NinjaScript"
    / "AddOns"
    / "BeelzebubReadOnlyAddOn.cs"
)
TOOL_PATH = ROOT / "tools" / "test_ninjatrader_outbound_lifecycle.ps1"


class AsiaRepairNativeLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.source = ADDON_PATH.read_text(encoding="utf-8")
        cls.outbound = cls.source[
            cls.source.index("    public static class BeelzebubReadOnlyOutbound") :
        ]

    def test_embedded_source_fingerprint_uses_canonical_repository_algorithm(self) -> None:
        match = re.search(
            r'private const string ProducerSourceFingerprint = "([0-9a-f]{64})";',
            self.source,
        )
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), expected_observer_source_fingerprint())

    def test_addon_activation_and_termination_own_explicit_lifecycle(self) -> None:
        active = self.source.index("if (!BeelzebubReadOnlyOutbound.Start())")
        subscribe = self.source.index("Account.AccountStatusUpdate += OnAccountStatusUpdate;")
        self.assertLess(active, subscribe)
        detach = self.source.index("DetachKnownAccounts();")
        shutdown = self.source.index("if (!BeelzebubReadOnlyOutbound.Shutdown())")
        terminated = self.source.index('Diagnostic("ADDON_TERMINATED")')
        self.assertLess(detach, shutdown)
        self.assertLess(shutdown, terminated)
        self.assertIn('Diagnostic("ADDON_OUTBOUND_SHUTDOWN_INCOMPLETE")', self.source)

    def test_stop_closes_exact_client_outside_lock_then_boundedly_joins(self) -> None:
        shutdown = self.outbound[
            self.outbound.index("public static bool Shutdown()") :
        ]
        capture = shutdown.index("client = senderClient;")
        close = shutdown.index("CloseClient(client);")
        join = shutdown.index("thread.Join(SenderTeardownTimeoutMilliseconds)")
        final_check = shutdown.index(
            "bool complete = !senderRunning && senderThread == null && senderClient == null;"
        )
        self.assertLess(capture, close)
        self.assertLess(close, join)
        self.assertLess(join, final_check)
        self.assertIn('"BRIDGE_SENDER_TEARDOWN_TIMEOUT"', shutdown)
        self.assertIn("Monitor.PulseAll(outboundFramesLock);", shutdown)

    def test_stop_refuses_enqueue_and_generation_replacement(self) -> None:
        start = self.outbound[
            self.outbound.index("public static bool Start()") :
            self.outbound.index("private static bool EnqueueLocked")
        ]
        enqueue = self.outbound[
            self.outbound.index("private static bool EnqueueLocked") :
            self.outbound.index("private static void SenderLoop")
        ]
        self.assertIn(
            "if (senderRunning || senderThread != null || senderClient != null)",
            start,
        )
        self.assertIn("return senderAccepting && !senderStopping;", start)
        self.assertIn("if (!senderAccepting || senderStopping)", enqueue)
        self.assertIn('"BRIDGE_ENQUEUE_REFUSED_STOPPED_GENERATION"', enqueue)
        self.assertNotIn("senderStopping = false;", enqueue)
        self.assertIn("SenderLoop(generation)", enqueue)

    def test_sender_checks_generation_and_accounts_every_dequeued_failure(self) -> None:
        loop = self.outbound[
            self.outbound.index("private static void SenderLoop") :
            self.outbound.index("private static bool EnsureSigningKey")
        ]
        self.assertGreaterEqual(
            loop.count("senderStopping || generation != senderGeneration"), 4
        )
        self.assertIn("RecordBatchFailure(frames.Count, true);", loop)
        self.assertEqual(loop.count("RecordBatchFailure(frames.Count, stopped);"), 2)
        self.assertLess(loop.index("try\n                    {"), loop.index("Encoding.UTF8.GetBytes"))
        self.assertIn("if (Object.ReferenceEquals(senderClient, client))", loop)
        self.assertIn(
            "if (Object.ReferenceEquals(senderThread, Thread.CurrentThread))",
            loop,
        )

    def test_socket_write_is_not_mislabeled_as_receiver_delivery(self) -> None:
        self.assertNotIn("BRIDGE_PUBLISH_DELIVERED", self.outbound)
        self.assertIn(
            "BRIDGE_SOCKET_WRITE_COMPLETED_NO_RECEIVER_ACK",
            self.outbound,
        )
        self.assertIn(
            "A completed socket write is not a receiver ACK",
            self.outbound,
        )

    def test_standalone_regression_uses_in_memory_transport_only(self) -> None:
        tool = TOOL_PATH.read_text(encoding="utf-8")
        self.assertIn("network_opened\\\":false", tool)
        self.assertIn("$outbound = $outbound.Replace('TcpClient', 'TestTcpClient')", tool)
        self.assertIn("TestTransportMode.BlockConnect", tool)
        self.assertIn("TestTransportMode.IgnoreCloseWrite", tool)
        self.assertIn("GENERATION_REPLACED_DURING_TIMEOUT", tool)
        self.assertNotIn("TcpListener", tool)
        self.assertNotIn("Socket.Connect", tool)


if __name__ == "__main__":
    unittest.main()
