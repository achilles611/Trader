from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3g_paper.pong import PongStateMachine
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from src.l3g_paper.pong_periodic_telemetry import (
    PONG_PERIODIC_TELEMETRY_SCHEMA,
    aligned_periodic_window,
)
from src.l3g_paper.pong_quantity import PongQuantityPolicy
from src.l3g_paper.pong_runtime import PongLoadedRuntimeIdentity, PongRuntimeBridge
from src.l3g_paper.pong_scheduler import (
    PongPeriodicDeadline,
    PongSchedulingConfig,
    PongSchedulingMode,
)
from tests import test_l3g_pong_production as production_fixtures
from tests.test_l3g_pong_inputs import verified
from tools.asia_periodic_healthy_flat import run_offline_scenario


N_TEST_ROOT = Path("N:/Beelzebub/scratch/asia-repair-periodic-tests")


def _snapshot(*, at_ns: int, ordinary: int, depth: int, generation: int = 1):
    counters = {
        "depth_events": depth,
        "ordinary_evaluation_dispatches": ordinary,
        "ordinary_completed_full_analyses": ordinary,
        "protection_evaluation_dispatches": 0,
        "protection_completed_full_analyses": 0,
        "scheduled_opportunities": ordinary,
        "claimed_current_opportunities": ordinary,
        "dispatched_opportunities": ordinary,
        "completed_opportunities": ordinary,
        "applied_opportunities": ordinary,
        "late_opportunities": 0,
        "on_time_opportunities": ordinary,
        "obsolete_opportunities": 0,
        "protection_supersessions": 0,
        "intentional_no_work": 0,
        "shutdown_opportunities": 0,
    }
    return {
        "schema": PONG_PERIODIC_TELEMETRY_SCHEMA,
        "captured_monotonic_ns": at_ns,
        "identity": {
            "counter_epoch_id": "counter-epoch",
            "counter_reset_ordinal": 0,
            "counter_reset_reason": "HOST_CONSTRUCTION",
            "host_process_instance_id": "host-process",
            "experiment_id": "experiment",
            "loaded_artifact_hash": "a" * 64,
            "selected_profile": "profile",
            "qualifying_architecture": "architecture",
            "topology_hash": "b" * 64,
            "runtime_execution_session_id": "runtime-session",
            "runtime_addon_source_fingerprint": "e" * 64,
            "runtime_addon_build_fingerprint": "f" * 64,
            "source_session_id": "source-session",
            "source_fingerprint": "c" * 64,
            "source_build_fingerprint": "d" * 64,
            "source_generation": generation,
        },
        "counters": counters,
        "scheduler": {
            "deadline_reconciliation": True,
            "completion_reconciliation": True,
        },
        "context": {"host_observation_queue_depth": 0},
    }


class AsiaRepairPeriodicTelemetryTests(unittest.TestCase):
    def test_offline_tool_uses_real_bridge_virtual_time_and_no_dispatch(self):
        N_TEST_ROOT.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix="offline-tool-", dir=N_TEST_ROOT) as root:
            report = run_offline_scenario(Path(root))
        self.assertEqual(
            report["initial_protection_reason"], "RUNTIME_BRIDGE_UNAVAILABLE",
        )
        self.assertEqual(report["final_active_path"], "PERIODIC_FLAT")
        self.assertEqual(report["native_dispatches"], [])
        self.assertEqual(
            report["healthy_flat_window"]
            ["depth_events_per_ordinary_completed_full_analysis"],
            2.0,
        )
        self.assertEqual(
            report["protected_window"]["deltas"]
            ["protection_completed_full_analyses"],
            2,
        )
        self.assertEqual(report["paper_ledger"], "NOT_INSTANTIATED")

    def test_ratio_uses_aligned_depth_and_only_ordinary_completed_full_analysis(self):
        start = _snapshot(at_ns=1_000_000_000, ordinary=10, depth=100)
        end = _snapshot(at_ns=3_000_000_000, ordinary=12, depth=110)
        end["counters"]["protection_evaluation_dispatches"] = 9
        end["counters"]["protection_completed_full_analyses"] = 9

        window = aligned_periodic_window(start, end)

        self.assertTrue(window["eligible"])
        self.assertEqual(
            window["depth_events_per_ordinary_completed_full_analysis"], 5.0,
        )
        self.assertEqual(window["deltas"]["protection_evaluation_dispatches"], 9)
        self.assertEqual(
            window["rates"]["ordinary_completed_full_analyses_per_second"],
            1.0,
        )
        self.assertIsNone(window["threshold"])
        self.assertEqual(window["threshold_reason"], "POLICY_NOT_DEFINED")

    def test_zero_denominator_and_identity_change_are_explicitly_unavailable(self):
        start = _snapshot(at_ns=1, ordinary=4, depth=8)
        no_ordinary = _snapshot(at_ns=2, ordinary=4, depth=10)
        window = aligned_periodic_window(start, no_ordinary)
        self.assertTrue(window["eligible"])
        self.assertIsNone(
            window["depth_events_per_ordinary_completed_full_analysis"],
        )
        self.assertEqual(
            window["ratio_unavailable_reason"],
            "ORDINARY_COMPLETED_FULL_ANALYSES_ZERO",
        )

        changed = _snapshot(at_ns=2, ordinary=5, depth=10, generation=2)
        changed_window = aligned_periodic_window(start, changed)
        self.assertFalse(changed_window["eligible"])
        self.assertEqual(
            changed_window["eligibility_reason"],
            "PERIODIC_WINDOW_IDENTITY_CHANGED",
        )

    def test_scheduler_rejects_double_terminal_accounting(self):
        scheduler = PongPeriodicDeadline(
            PongSchedulingConfig(
                mode=PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
                interval_ms=20,
            ),
            clock_ns=lambda: 0,
        )
        due = scheduler.take_due(100_000_000)
        assert due is not None
        scheduler.dispatch(due)
        scheduler.complete(due, applied=True)
        with self.assertRaisesRegex(ValueError, "PONG_SCHEDULER_DUE_STATE_INVALID"):
            scheduler.complete(due, applied=True)
        metrics = scheduler.metrics()
        self.assertEqual(metrics["scheduled_opportunities"], 5)
        self.assertEqual(metrics["obsolete_opportunities"], 4)
        self.assertEqual(metrics["dispatched"], 1)
        self.assertEqual(metrics["completed"], 1)
        self.assertTrue(metrics["deadline_reconciliation"])

    def test_real_test_bridge_allows_full_periodic_then_reconciliation_loss_protects(self):
        N_TEST_ROOT.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix="healthy-flat-", dir=N_TEST_ROOT) as root:
            host = production_fixtures.PongProductionHostTests()._qualifying_host(root)
            runtime = production_fixtures.qualifying_runtime_snapshot()
            host._runtime = runtime
            host._collection = {
                "durability": {
                    "enqueued_records": 3,
                    "durable_records": 3,
                    "queue_depth": 0,
                },
            }
            host._service_periodic_deadline(20_000_000)
            self.assertEqual(
                host._event_protection_reason_locked(),
                "RUNTIME_BRIDGE_UNAVAILABLE",
            )

            topology = DEC26_TOPOLOGY
            identity = PongLoadedRuntimeIdentity(
                execution_session_id=runtime["execution_session_id"],
                addon_protocol_version=runtime["addon_protocol_version"],
                addon_source_fingerprint=runtime["addon_source_fingerprint"],
                addon_build_fingerprint=runtime["addon_build_fingerprint"],
                verified_at=runtime["observed_at"],
                attestation_verified=True,
                instrument=topology.native_instrument,
                canonical_contract=topology.canonical_contract,
                topology_version=topology.version,
                topology_hash=topology.binding_hash,
            )
            policy = PongQuantityPolicy(
                instrument=topology.native_instrument,
                canonical_contract=topology.canonical_contract,
                reconciliation_source_fingerprint=identity.addon_source_fingerprint,
                reconciliation_build_fingerprint=identity.addon_build_fingerprint,
                topology_version=topology.version,
                topology_hash=topology.binding_hash,
            )
            host._bridge = PongRuntimeBridge.create(
                PongStateMachine(policy),
                Path(root) / "bridges" / "TEST-ONLY.json",
                runtime_identity=identity,
            )

            now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            fixture = production_fixtures.PongProductionHostTests()
            for sequence, side in ((1, "Bid"), (2, "Ask")):
                template = fixture._qualifying_depth(
                    sequence,
                    side=side,
                    mutation_price=100 if side == "Bid" else 100.25,
                )
                host._consume_observation(verified(
                    "DEPTH", dict(template.payload),
                    local_sequence=sequence, observed=now,
                ))
            host._consume_observation(verified("TRADE", {
                "contract_id": topology.native_instrument,
                "price": 100.25,
                "size": 1,
                "aggressor_side": "BUY",
                "aggressor_source": "NATIVE_BID_ASK",
                "source_event_sequence": 1,
                "source_generation": 1,
                "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
            }, local_sequence=3, observed=now))
            start = host.status()["evaluation_schedule"]["periodic_ratio_telemetry"]

            for sequence, side in ((3, "Bid"), (4, "Ask")):
                template = fixture._qualifying_depth(
                    sequence,
                    side=side,
                    mutation_price=100 if side == "Bid" else 100.25,
                )
                host._consume_observation(verified(
                    "DEPTH", dict(template.payload),
                    local_sequence=sequence + 1, observed=now,
                ))
            host._service_periodic_deadline(40_000_000)
            end = host.status()["evaluation_schedule"]["periodic_ratio_telemetry"]
            window = aligned_periodic_window(start, end)
            self.assertTrue(window["eligible"])
            self.assertEqual(
                window["deltas"]["depth_events"], 2,
                (start, end, host.status().get("input_error")),
            )
            self.assertEqual(window["deltas"]["ordinary_completed_full_analyses"], 1)
            self.assertEqual(
                window["depth_events_per_ordinary_completed_full_analysis"], 2.0,
            )
            self.assertEqual(end["context"]["collector_durable_lag_records"], 0)

            host._runtime = {**runtime, "reconciliation_current": False}
            host._service_periodic_deadline(60_000_000)
            self.assertEqual(
                host._event_protection_reason_locked(),
                "RUNTIME_RECONCILIATION_UNRESOLVED",
            )
            host._runtime = runtime
            host._service_periodic_deadline(80_000_000)
            final = host.status()["evaluation_schedule"]
            self.assertEqual(final["active_path"], "PERIODIC_FLAT")
            self.assertEqual(final["ordinary_completed_full_analyses"], 2)
            self.assertEqual(
                final["deadline_metrics"]["protection_mode_supersessions"], 2,
            )


if __name__ == "__main__":
    unittest.main()
