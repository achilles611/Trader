"""Receiver repair safety regressions. All operational identities below are TEST ONLY."""
from collections import deque
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import socket
from unittest.mock import Mock
from unittest.mock import patch

from src.l3g_paper.ledger import PaperLedger, deferred_capacity_allows_authority
from src.l3f_provider.ninjatrader_commission import NinjaTraderListenerWorker, NinjaTraderCommissioningHarness
from src.l3f_provider.ninjatrader_observation import LoopbackBridgeConfig
from src.l3g_paper.runtime import ObservationFanout
from tests.test_l3g_pong_inputs import KEY
from src.l3g_paper.pong import PongStateMachine
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from src.l3g_paper.pong_quantity import PongQuantityPolicy
from src.l3g_paper.pong_runtime import PongLoadedRuntimeIdentity, PongRuntimeBridge
from tests import test_l3g_pong_production as production_fixtures


class ReceiverRepairSafetyTests(unittest.TestCase):
    @staticmethod
    def _fanout(paper, record_failure=None):
        return ObservationFanout(
            shadow_observation=Mock(), shadow_transport=Mock(), shadow_rejection=Mock(), shadow_duplicate=Mock(),
            paper_observation=paper, paper_transport=Mock(), paper_rejection=Mock(), paper_duplicate=Mock(),
            record_failure=record_failure or Mock(),
        )

    def test_required_paper_failure_propagates_even_when_incident_writer_fails(self):
        failure = ValueError("TEST ONLY required sink failure")
        paper = Mock(side_effect=failure)
        recorder = Mock(side_effect=OSError("TEST ONLY evidence write failure"))
        fanout = self._fanout(paper, recorder)
        for method in (fanout.on_required_paper_observation, fanout.on_required_observation):
            with self.assertRaises(ValueError) as caught:
                method(object())
            self.assertIs(caught.exception, failure)
        self.assertEqual(paper.call_count, 2)
        self.assertEqual(recorder.call_count, 2)
        self.assertEqual(fanout._shadow_observation.call_count, 1)

    def test_required_paper_pause_refuses_without_buffering_or_later_delivery(self):
        paper = Mock()
        fanout = self._fanout(paper)
        fanout.begin_startup_paper_observation_pause()
        with self.assertRaisesRegex(RuntimeError, "PONG_REQUIRED_PAPER_SINK_PAUSED"):
            fanout.on_required_paper_observation(object())
        self.assertFalse(fanout._paused_paper_observations)
        self.assertEqual(fanout.end_startup_paper_observation_pause()["replayed_observations"], 0)
        paper.assert_not_called()

    def test_required_consumer_socket_failure_stops_receiver_and_disconnects(self):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        worker = NinjaTraderListenerWorker(
            LoopbackBridgeConfig(port=port), attestation_key=KEY, require_attestation=True,
        )
        paper = Mock(side_effect=ValueError("TEST ONLY required consumer failure"))
        fanout = self._fanout(paper)
        transport = Mock()
        worker.set_observation_sinks(
            on_observation=fanout.on_required_paper_observation,
            on_local_bridge_state=transport, on_rejection=Mock(), on_duplicate=Mock(),
            fail_on_observation_error=True,
        )
        try:
            self.assertEqual(worker.start().state, "LISTENING")
            wire = production_fixtures.PongProductionHostTests._qualifying_depth(1).signed_wire_bytes()
            with socket.create_connection(("127.0.0.1", port), timeout=2) as sender:
                sender.sendall(wire + b"\n")
                self.assertTrue(worker._finished_event.wait(2), "Required failure must terminate receiver")
            state = worker.status()
            self.assertEqual(state.state, "FAILED")
            self.assertIn("NINJATRADER_REQUIRED_OBSERVATION_SINK_FAILED", state.error)
            self.assertEqual(paper.call_count, 1)
            self.assertEqual(worker._harness.summary.downstream_callbacks, 0)
            self.assertEqual(worker._harness.summary.health["LOCAL_BRIDGE"], "DISCONNECTED")
            self.assertFalse(worker._thread.is_alive())
            with self.assertRaises(OSError):
                socket.create_connection(("127.0.0.1", port), timeout=0.2)
        finally:
            worker.stop()
        self.assertEqual(worker.status().state, "FAILED", "Stop cannot erase first consumer failure")

    def test_required_auth_rejection_records_once_then_fails_receiver_closed(self):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        rejection = Mock()
        worker = NinjaTraderListenerWorker(
            LoopbackBridgeConfig(port=port),
            attestation_key=KEY,
            require_attestation=True,
            expected_source_fingerprint="0" * 64,
        )
        worker.set_observation_sinks(
            on_observation=Mock(),
            on_local_bridge_state=Mock(),
            on_rejection=rejection,
            on_duplicate=Mock(),
            fail_on_observation_error=True,
        )
        try:
            self.assertEqual(worker.start().state, "LISTENING")
            wire = production_fixtures.PongProductionHostTests._qualifying_depth(1).signed_wire_bytes()
            with socket.create_connection(("127.0.0.1", port), timeout=2) as sender:
                sender.sendall((wire + b"\n") * 2)
                self.assertTrue(worker._finished_event.wait(2), "Required rejection must terminate receiver")
            state = worker.status()
            self.assertEqual(state.state, "FAILED")
            self.assertIn(
                "NINJATRADER_REQUIRED_OBSERVATION_REJECTED:AUTH_FAILED:ninjatrader_source_fingerprint",
                state.error,
            )
            self.assertEqual(rejection.call_count, 1)
            self.assertEqual(worker._harness.summary.rejected, 1)
            self.assertEqual(worker._harness.summary.health["LOCAL_BRIDGE"], "DISCONNECTED")
            self.assertFalse(worker._thread.is_alive())
            with self.assertRaises(OSError):
                socket.create_connection(("127.0.0.1", port), timeout=0.2)
        finally:
            worker.stop()
        self.assertEqual(worker.status().state, "FAILED", "Stop cannot erase first auth rejection")

    def test_optional_legacy_consumer_failure_remains_compatible(self):
        sink = Mock(side_effect=ValueError("TEST ONLY optional consumer"))
        harness = NinjaTraderCommissioningHarness(
            attestation_key=KEY, require_attestation=True, on_observation=sink,
        )
        with self.assertLogs("src.l3f_provider.ninjatrader_commission", level="ERROR"):
            harness.ingest((production_fixtures.PongProductionHostTests._qualifying_depth(1).signed_wire_bytes(),))
        self.assertEqual(sink.call_count, 1)
        self.assertEqual(harness.summary.accepted, 1)

    def test_flat_account_without_bridge_remains_protected(self):
        with TemporaryDirectory(prefix="receiver_repair-") as root:
            host = production_fixtures.PongProductionHostTests()._qualifying_host(root)
            host._runtime = production_fixtures.qualifying_runtime_snapshot()
            host._service_periodic_deadline(20_000_000)
            self.assertEqual(host._event_protection_reason_locked(), "RUNTIME_BRIDGE_UNAVAILABLE")
            self.assertEqual(host._periodic_deadline.metrics()["protection_mode_supersessions"], 1)
            self.assertEqual(host._periodic_evaluations, 0)
            host._maybe_create_bridge()
            self.assertIsNone(host._bridge, "Unqualified collection must not fabricate a bridge")

    def test_real_durable_flat_bridge_allows_periodic_and_reconciliation_loss_protects(self):
        with TemporaryDirectory(prefix="receiver_repair-") as root:
            host = production_fixtures.PongProductionHostTests()._qualifying_host(root)
            runtime = production_fixtures.qualifying_runtime_snapshot()
            host._runtime = runtime
            topology = DEC26_TOPOLOGY
            identity = PongLoadedRuntimeIdentity(
                execution_session_id=runtime["execution_session_id"],
                addon_protocol_version=runtime["addon_protocol_version"],
                addon_source_fingerprint=runtime["addon_source_fingerprint"],
                addon_build_fingerprint=runtime["addon_build_fingerprint"],
                verified_at=runtime["observed_at"], attestation_verified=True,
                instrument=topology.native_instrument, canonical_contract=topology.canonical_contract,
                topology_version=topology.version, topology_hash=topology.binding_hash,
            )
            policy = PongQuantityPolicy(
                instrument=topology.native_instrument, canonical_contract=topology.canonical_contract,
                reconciliation_source_fingerprint=identity.addon_source_fingerprint,
                reconciliation_build_fingerprint=identity.addon_build_fingerprint,
                topology_version=topology.version, topology_hash=topology.binding_hash,
            )
            bridge_path = Path(root) / "TEST-ONLY-bridge.json"
            host._bridge = PongRuntimeBridge.create(PongStateMachine(policy), bridge_path, runtime_identity=identity)
            self.assertTrue(bridge_path.is_file())
            for sequence in range(1, 6):
                host._consume_observation(production_fixtures.PongProductionHostTests._qualifying_depth(sequence))
            self.assertIsNone(host._event_protection_reason_locked())
            self.assertEqual(host._event_driven_evaluations, 0)
            host._service_periodic_deadline(20_000_000)
            self.assertEqual(host._periodic_evaluations, 1)
            self.assertEqual(host.status()["microstructure_pipeline"]["evaluation_count"], 1)
            host._runtime = {**runtime, "reconciliation_current": False}
            host._service_periodic_deadline(40_000_000)
            self.assertEqual(host._event_protection_reason_locked(), "RUNTIME_RECONCILIATION_UNRESOLVED")
            self.assertEqual(host._periodic_evaluations, 1)
            self.assertEqual(host._periodic_deadline.metrics()["protection_mode_supersessions"], 1)
            self.assertEqual(host._bridge.machine.actual_quantity, 0)
            self.assertFalse(host._pending_native)

    def test_capacity_uses_ledger_volume_not_process_temp_volume(self):
        with TemporaryDirectory(prefix="receiver_repair-") as root:
            ledger = PaperLedger(Path(root) / "authority.sqlite3")
            try:
                disk_result = type("Disk", (), {"free": 200 * 1024**3})()
                with patch("src.l3g_paper.ledger.shutil.disk_usage", return_value=disk_result) as disk:
                    self.assertEqual(ledger._current_database_free_bytes(), disk_result.free)
                    disk.assert_called_once_with(ledger.path.parent)
            finally:
                ledger.close()

    def test_database_runway_warning_denies_authority_without_reducing_reserves(self):
        with TemporaryDirectory(prefix="receiver_repair-") as root:
            ledger = PaperLedger(Path(root) / "authority.sqlite3")
            try:
                with ledger._deferred_condition:
                    ledger._database_growth_samples = deque()
                    with patch.object(ledger, "_current_database_size_bytes", return_value=1024**3), patch.object(ledger, "_current_database_free_bytes", return_value=8 * 1024**3):
                        snapshot = ledger._sample_database_capacity_locked(100, force=True)
                        capacity = ledger._capacity_snapshot_locked(100)
                    self.assertEqual(snapshot["database_effective_headroom_bytes"], 4 * 1024**3)
                    self.assertEqual(snapshot["database_warning_reasons"], ["AUTHORITY_LEDGER_DISK_FREE_WARNING"])
                    self.assertEqual(capacity["state"], "DEGRADED")
                    self.assertFalse(deferred_capacity_allows_authority(capacity))
                    with patch.object(ledger, "_current_database_size_bytes", return_value=1024**3), patch.object(ledger, "_current_database_free_bytes", return_value=4 * 1024**3):
                        exhausted = ledger._sample_database_capacity_locked(101, force=True)
                    self.assertEqual(exhausted["database_capacity_fault_reason"], "AUTHORITY_LEDGER_DISK_RUNWAY_EXHAUSTED")
                    with patch.object(ledger, "_current_database_size_bytes", return_value=1024**3), patch.object(ledger, "_current_database_free_bytes", return_value=200 * 1024**3):
                        restored_space = ledger._sample_database_capacity_locked(102, force=True)
                    self.assertTrue(restored_space["database_capacity_fault_latched"], "Free space alone cannot reset exhausted writer")
            finally:
                ledger.close()


if __name__ == "__main__":
    unittest.main()

