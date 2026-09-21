from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import queue
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.l3f_provider.ninjatrader_observation import expected_observer_source_fingerprint
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION
from src.l3g_paper.fhb import FHBConfig
from src.l3g_paper.pong import PongState
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from src.l3g_paper.pong_microstructure import BookSide
from src.l3g_paper.pong_production import (
    PONG_HOST_TELEMETRY_SCHEMA,
    PONG_QUALIFYING_HOUR_SCHEMA,
    PongProductionHost,
)
from src.l3g_paper.pong_qualifying import (
    FHB_ARCHIVE_DISPOSITION,
    PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
)
from src.l3g_paper.pong_scheduler import PongSchedulingConfig, PongSchedulingMode
from src.lane_iii.contracts import canonical_hash
from tests.test_l3g_pong_inputs import verified, warm_record


def runtime_snapshot() -> dict[str, object]:
    return {
        "observed_at": "2026-09-11T18:30:00Z",
        "entry_profile_version": PONG_ENTRY_PROFILE_VERSION,
        "runtime_state": "READY_DISARMED",
        "runtime_active": True,
        "entry_fence_active": True,
        "evidence_sealed": False,
        "entries_paused": False,
        "position_quantity": 0,
        "working_owned_orders": 0,
        "working_entry_orders": 0,
        "unresolved_execution": False,
        "reconciliation_current": True,
        "protection_current": True,
        "safety_lockout": False,
        "lockout_reason": None,
        "account_name": "Sim101",
        "account_class": "LOCAL_SIMULATION",
        "instrument": "MNQ SEP26",
        "maximum_quantity": 1,
        "available_loss_allowance_dollars": "200",
        "live_authority": "DISARMED",
        "execution_session_id": "l3g-es-" + "1" * 32,
        "addon_protocol_version": "l3g-paper-addon-provenance-v1",
        "addon_source_fingerprint": "d" * 64,
        "addon_build_fingerprint": "b" * 64,
        "addon_provenance_valid": True,
    }


def qualifying_runtime_snapshot() -> dict[str, object]:
    return {**runtime_snapshot(), "instrument": DEC26_TOPOLOGY.native_instrument}


def pending_collection() -> dict[str, object]:
    return {
        "state": "COLLECTING",
        "producer": None,
        "durability": {"journal_path": "missing", "journal_tip_sha256": None},
        "required_interval": {
            "start_utc": "2026-09-11T19:00:00Z",
            "end_utc": "2026-09-11T20:00:00Z",
            "eligible": False,
            "blockers": ["HOURLY_INTERVAL_INCOMPLETE"],
        },
    }


class PongProductionHostTests(unittest.TestCase):
    def test_successful_admission_updates_high_water_before_interleaved_drain(
        self,
    ) -> None:
        with TemporaryDirectory(prefix="pong-host-admission-interleave-") as root:
            host = self._qualifying_host(root)
            inserted = threading.Event()
            drained = threading.Event()
            consumer_errors: list[str] = []
            original_put_nowait = host._observations.put_nowait

            def interleaved_put(item: object) -> None:
                original_put_nowait(item)
                inserted.set()
                self.assertTrue(drained.wait(1.0))

            def forced_consumer() -> None:
                if not inserted.wait(1.0):
                    consumer_errors.append("admission was not observed")
                    return
                try:
                    host._observations.get_nowait()
                except queue.Empty:
                    consumer_errors.append("admitted item was not drainable")
                    return
                drained.set()
                with host._admission_lock:
                    if host._observation_queue_occupancy != 1:
                        consumer_errors.append("occupancy was not published atomically")
                    host._observation_queue_occupancy -= 1
                host._observations.task_done()

            host._observations.put_nowait = interleaved_put
            consumer = threading.Thread(target=forced_consumer, daemon=True)
            consumer.start()
            host.on_observation(self._qualifying_depth(1))
            consumer.join(1.0)

            self.assertFalse(consumer.is_alive())
            self.assertEqual(consumer_errors, [])
            self.assertEqual(host._source_ingress_records, 1)
            self.assertEqual(host._observation_queue_occupancy, 0)
            self.assertEqual(host._observations.qsize(), 0)
            self.assertEqual(host._observation_queue_high_water, 1)

    def test_early_canonical_10x1_fault_persists_without_later_record(self) -> None:
        with TemporaryDirectory(prefix="pong-host-10x1-fault-") as root:
            host = self._qualifying_host(root)
            host._consume_observation(
                self._qualifying_depth(1, levels=10, ask_levels=1)
            )

            receipt = json.loads(
                (host.host_telemetry_root / "first-fault.json").read_text(
                    encoding="utf-8",
                )
            )
            self.assertEqual(
                receipt["fault"], "PONG_CANONICAL_DEPTH_10X10_REQUIRED",
            )
            self.assertEqual(receipt["receiver"]["last_stream_sequences"], {"DEPTH": 1})
            self.assertEqual(receipt["host_work"]["unprocessed_work"], 0)
            self.assertEqual(
                receipt["clock_domains"]["host_queue_age_and_checkpoint"],
                "PROCESS_MONOTONIC_SECONDS",
            )
            self.assertEqual(
                receipt["clock_domains"]["source_receipt_age"],
                "UTC_WALL_CLOCK_DURATION",
            )

    def test_final_latches_callback_queue_fault_without_later_record(self) -> None:
        with TemporaryDirectory(prefix="pong-host-final-fault-") as root:
            host = self._qualifying_host(root)
            host._observations = queue.Queue(maxsize=1)
            collector_refresh_lock_states: list[bool] = []

            def final_collector_status() -> dict[str, object]:
                collector_refresh_lock_states.append(host._lock._is_owned())
                value = pending_collection()
                value["durability"] = {
                    **value["durability"],
                    "written_records": 7,
                    "durable_records": 7,
                }
                return value

            host._collector_status = final_collector_status
            host.on_observation(self._qualifying_depth(1))
            host.on_observation(self._qualifying_depth(2))
            host.stop()

            fault = json.loads(
                (host.host_telemetry_root / "first-fault.json").read_text(
                    encoding="utf-8",
                )
            )
            final = json.loads(
                (host.host_telemetry_root / "final.json").read_text(
                    encoding="utf-8",
                )
            )
            self.assertEqual(
                fault["fault"], "PONG_PRODUCTION_OBSERVATION_QUEUE_OVERFLOW",
            )
            self.assertEqual(final["fault"], fault["fault"])
            self.assertEqual(
                final["snapshot"]["stage"],
                "POST_HOST_JOIN_PRE_COLLECTOR_STOP_REFRESH",
            )
            self.assertEqual(
                final["snapshot"]["collector_durability_scope"],
                "PRE_COLLECTOR_STOP_SNAPSHOT_NOT_FINAL_DURABILITY",
            )
            self.assertIsNotNone(final["snapshot"]["collector_snapshot_at_utc"])
            self.assertEqual(
                final["collector"]["durability"]["durable_records"], 7,
            )
            self.assertEqual(final["host_work"]["observation_queue_depth"], 1)
            self.assertEqual(final["host_work"]["observation_queue_high_water"], 1)
            self.assertEqual(collector_refresh_lock_states, [False])

    def test_failed_first_fault_write_retry_is_monotonic_rate_limited(self) -> None:
        with TemporaryDirectory(prefix="pong-host-fault-retry-") as root:
            host = self._qualifying_host(root)
            now = [10.0]
            host._monotonic_clock = lambda: now[0]
            with patch(
                "src.l3g_paper.pong_production._atomic_write",
                side_effect=OSError("disk unavailable"),
            ) as atomic_write:
                with host._lock:
                    host._persist_first_host_fault_locked("EARLY_FAULT")
                    for _ in range(100):
                        host._persist_first_host_fault_locked("LATER_FAULT")
                    self.assertEqual(atomic_write.call_count, 1)
                    now[0] += 4.999
                    host._persist_first_host_fault_locked("LATER_FAULT")
                    self.assertEqual(atomic_write.call_count, 1)
                    now[0] += 0.002
                    host._persist_first_host_fault_locked("LATER_FAULT")
                    self.assertEqual(atomic_write.call_count, 2)
            self.assertEqual(host._host_first_fault, "EARLY_FAULT")

    def test_checkpoint_latches_input_fault_without_later_record(self) -> None:
        with TemporaryDirectory(prefix="pong-host-checkpoint-fault-") as root:
            host = self._qualifying_host(root)
            with host._lock:
                host._input_error = "PONG_COMPUTE_QUEUE_OVERFLOW"
                host._persist_host_checkpoint_locked(10.0)
            receipt = json.loads(
                (host.host_telemetry_root / "first-fault.json").read_text(
                    encoding="utf-8",
                )
            )
            self.assertEqual(receipt["fault"], "PONG_COMPUTE_QUEUE_OVERFLOW")

    def test_queued_work_and_protection_elapsed_are_monotonic_and_bounded(self) -> None:
        with TemporaryDirectory(prefix="pong-host-queue-metrics-") as root:
            host = self._qualifying_host(root)
            now = [100.0]
            host._monotonic_clock = lambda: now[0]
            host.on_observation(self._qualifying_depth(1))
            now[0] = 101.0
            host.on_observation(self._qualifying_depth(2))
            now[0] = 105.0
            with host._lock:
                host._update_protection_accounting_locked()
                now[0] = 108.0
                host._bridge = SimpleNamespace(
                    machine=SimpleNamespace(
                        state=PongState.FLAT_WAIT_ALIGNMENT,
                        actual_quantity=0,
                        pending_action=None,
                    ),
                )
                host._runtime = qualifying_runtime_snapshot()
                host._update_protection_accounting_locked()
                now[0] = 113.0
                host._runtime["working_owned_orders"] = 1
                host._update_protection_accounting_locked()
                now[0] = 115.0
                self.assertTrue(host._persist_host_receipt_locked("checkpoint"))

            receipt = json.loads(
                (host.host_telemetry_root / "checkpoint.json").read_text(
                    encoding="utf-8",
                )
            )
            self.assertEqual(receipt["host_work"]["observation_queue_depth"], 2)
            self.assertEqual(receipt["host_work"]["observation_queue_high_water"], 2)
            self.assertEqual(
                receipt["host_work"]["oldest_pending_queue_age_ms"], 15000.0,
            )
            self.assertEqual(receipt["protection"]["eligible_elapsed_seconds"], 5.0)
            self.assertEqual(receipt["protection"]["protected_elapsed_seconds"], 5.0)
            self.assertEqual(receipt["protection"]["transition_count"], 2)
            self.assertEqual(
                receipt["protection"]["reason"], "RUNTIME_OWNED_ORDERS_PRESENT",
            )
            self.assertEqual(
                receipt["runtime_identity_and_authority"]["account_name"],
                "Sim101",
            )
            self.assertEqual(
                receipt["runtime_identity_and_authority"]["instrument"],
                DEC26_TOPOLOGY.native_instrument,
            )
            self.assertEqual(
                receipt["missing_metrics"]["host_rlock_wait"],
                "MISSING_NOT_INSTRUMENTED",
            )

    def test_atomic_host_receipts_survive_early_fault_and_status_unavailability(
        self,
    ) -> None:
        with TemporaryDirectory(prefix="pong-host-telemetry-") as root:
            host = self._qualifying_host(root)
            host.start()
            try:
                host._consume_observation(self._qualifying_depth(1))
                host._consume_observation(self._qualifying_depth(3))

                fault_path = host.host_telemetry_root / "first-fault.json"
                checkpoint_path = host.host_telemetry_root / "checkpoint.json"
                deadline = time.monotonic() + 2.0
                while (
                    (not fault_path.is_file() or not checkpoint_path.is_file())
                    and time.monotonic() < deadline
                ):
                    time.sleep(0.01)

                with (
                    patch.object(
                        host,
                        "status",
                        side_effect=ConnectionError("status endpoint unavailable"),
                    ),
                    patch.object(
                        host,
                        "_runtime_snapshot",
                        side_effect=TimeoutError("runtime status unavailable"),
                    ),
                    patch.object(
                        host,
                        "_collector_status",
                        side_effect=TimeoutError("collector status unavailable"),
                    ),
                ):
                    with self.assertRaisesRegex(
                        ConnectionError, "status endpoint unavailable",
                    ):
                        host.status()
                    with host._lock:
                        self.assertTrue(
                            host._persist_host_receipt_locked("checkpoint")
                        )
                    fault = json.loads(fault_path.read_text(encoding="utf-8"))
                    checkpoint = json.loads(
                        checkpoint_path.read_text(encoding="utf-8")
                    )

                self.assertEqual(fault["schema"], PONG_HOST_TELEMETRY_SCHEMA)
                self.assertEqual(fault["receipt_kind"], "first-fault")
                self.assertEqual(
                    fault["fault"], "PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP",
                )
                self.assertEqual(
                    fault["identity"]["loaded_artifact_hash"],
                    checkpoint["identity"]["loaded_artifact_hash"],
                )
                self.assertEqual(
                    fault["identity"]["host_process_instance_id"],
                    checkpoint["identity"]["host_process_instance_id"],
                )
                self.assertEqual(fault["identity"]["live_authority"], "DISARMED")
                self.assertFalse(fault["identity"]["activation_authorized"])
                self.assertEqual(
                    fault["producer_identity"]["session_id"],
                    "native-session",
                )
                self.assertEqual(
                    fault["producer_identity"]["build_fingerprint"], "b" * 64,
                )
                self.assertEqual(
                    fault["fhb"]["disposition"], FHB_ARCHIVE_DISPOSITION,
                )
                self.assertFalse(fault["fhb"]["instantiated"])
                self.assertFalse(
                    fault["clock_domains"]["cross_domain_subtraction_allowed"]
                )
                self.assertIn("unprocessed_work", fault["host_work"])
                self.assertEqual(
                    checkpoint["collector"]["durability"]["journal_path"],
                    "missing",
                )
                self.assertIn("last_stream_sequences", fault["receiver"])
                self.assertIn("metrics", fault["scheduler"])
                self.assertIn("reason", fault["protection"])
            finally:
                host.stop()

            for name in ("start", "first-fault", "checkpoint", "final"):
                receipt = json.loads(
                    (host.host_telemetry_root / f"{name}.json").read_text(
                        encoding="utf-8",
                    )
                )
                self.assertEqual(receipt["schema"], PONG_HOST_TELEMETRY_SCHEMA)
                self.assertEqual(receipt["receipt_kind"], name)

    def test_qualifying_host_import_does_not_import_or_hash_fhb(self) -> None:
        probe = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import sys; "
                    "import beez_console; "
                    "import src.copytrade.control_center; "
                    "from src.l3g_paper import pong_production as p; "
                    "assert 'src.l3g_paper.fhb' not in sys.modules; "
                    "assert 'src/l3g_paper/fhb.py' not in p._LOADED_SOURCE_HASHES"
                ),
            ],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(probe.returncode, 0, probe.stderr or probe.stdout)

    @staticmethod
    def _qualifying_depth(
        sequence: int,
        *,
        available: bool = True,
        mutation_price: object = 100,
        resolved_price: object | None = None,
        levels: int = 10,
        ask_levels: int | None = None,
        operation: str = "Update",
        side: str = "Bid",
        bid_start: float = 100,
        ask_start: float = 100.25,
        generation: int = 1,
        reset: bool = False,
    ):
        bids = [
            {"price": str(bid_start - index * 0.25), "size": str(index + 1)}
            for index in range(levels)
        ]
        asks = [
            {"price": str(ask_start + index * 0.25), "size": str(index + 1)}
            for index in range(levels if ask_levels is None else ask_levels)
        ]
        payload = {
            "contract_id": "MNQ DEC26",
            "side": side,
            "operation": operation,
            "mutation_price": mutation_price,
            "mutation_volume": 1,
            "is_reset": reset,
            "status": "AVAILABLE" if available else "UNAVAILABLE",
            "reason": None if available else "CROSSED_NATIVE_POSITIONAL_SNAPSHOT",
            "bids": bids,
            "asks": asks,
            "source_event_sequence": sequence,
            "source_generation": generation,
            "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
        }
        if resolved_price is not None:
            payload["mutation_resolved_price"] = resolved_price
        return verified("DEPTH", payload, local_sequence=sequence)

    def _qualifying_host(self, root: str) -> PongProductionHost:
        return PongProductionHost(
            Path(root),
            selected_profile=PONG_ENTRY_PROFILE_VERSION,
            expected_market_source_fingerprint=expected_observer_source_fingerprint(),
            runtime_snapshot=qualifying_runtime_snapshot,
            collector_status=pending_collection,
            controller_dispatch=lambda _kind, _reason: True,
            native_dispatch=lambda directive, _evidence: {
                "accepted": True, "command_id": None,
                "directive_id": directive.directive_id,
            },
            scheduling=PongSchedulingConfig(
                mode=PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
                interval_ms=20,
            ),
            qualifying_architecture=PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
            topology=DEC26_TOPOLOGY,
            clock_ns=lambda: 0,
        )

    def test_qualifying_runtime_has_no_fhb_gate_and_periodic_owns_analysis(self) -> None:
        with TemporaryDirectory(prefix="pong-no-fhb-qualifying-") as root:
            host = self._qualifying_host(root)
            machine = SimpleNamespace(
                state=PongState.FLAT_WAIT_ALIGNMENT,
                actual_quantity=0,
                pending_action=None,
            )
            host._bridge = SimpleNamespace(machine=machine, status=lambda: {})
            host._runtime = qualifying_runtime_snapshot()
            for sequence in range(1, 6):
                host._consume_observation(self._qualifying_depth(sequence))
            status = host.status()
            self.assertIsNone(host._fhb)
            self.assertEqual(
                status["qualifying_architecture"],
                PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
            )
            self.assertEqual(
                status["fhb_activation"]["disposition"],
                FHB_ARCHIVE_DISPOSITION,
            )
            self.assertEqual(
                status["fhb_activation"]["qualifying_runtime_state"],
                "DISABLED_NOT_REQUIRED",
            )
            self.assertFalse(status["fhb_activation"]["qualification_required"])
            self.assertFalse(status["fhb_activation"]["instantiated"])
            self.assertNotIn(
                "src/l3g_paper/fhb.py",
                status["module_import_source_sha256"],
            )
            self.assertEqual(status["canonical_depth_state"]["bid_levels"], 10)
            self.assertEqual(status["canonical_depth_state"]["ask_levels"], 10)
            self.assertEqual(status["microstructure_pipeline"]["evaluation_count"], 0)
            self.assertEqual(
                status["evaluation_schedule"]["event_driven_analytical_evaluations"],
                0,
            )
            host._service_periodic_deadline(20_000_000)
            status = host.status()
            self.assertEqual(
                status["evaluation_schedule"]["periodic_flat_analytical_evaluations"],
                1,
            )
            self.assertEqual(status["microstructure_pipeline"]["evaluation_count"], 1)

    def test_qualifying_depth_recovers_after_unavailable_and_resolves_zero_price(self) -> None:
        with TemporaryDirectory(prefix="pong-no-fhb-depth-") as root:
            host = self._qualifying_host(root)
            host._consume_observation(self._qualifying_depth(1))
            host._consume_observation(self._qualifying_depth(2, available=False))
            blocked = host.status()["canonical_depth_state"]
            self.assertFalse(blocked["ready"])
            self.assertIn("CROSSED_NATIVE_POSITIONAL_SNAPSHOT", blocked["reason"])
            self.assertIsNone(host.status()["input_error"])
            host._consume_observation(self._qualifying_depth(
                3, mutation_price=0, resolved_price=100,
            ))
            recovered = host.status()["canonical_depth_state"]
            self.assertTrue(recovered["ready"])
            self.assertEqual(recovered["bid_levels"], 10)
            self.assertEqual(recovered["ask_levels"], 10)
            self.assertEqual(recovered["last_source_sequence"], 3)
            self.assertEqual(host._depth[next(iter(host._depth))][-1].price, 100)

    def test_qualifying_depth_admits_removed_best_level_identity(self) -> None:
        with TemporaryDirectory(prefix="pong-no-fhb-remove-identity-") as root:
            host = self._qualifying_host(root)
            host._consume_observation(self._qualifying_depth(
                1,
                operation="Remove",
                mutation_price=0,
                resolved_price=100,
                bid_start=99.75,
            ))
            self.assertEqual(host._depth[BookSide.BID][-1].price, 100)
            host._consume_observation(self._qualifying_depth(
                2,
                operation="Remove",
                side="Ask",
                mutation_price=0,
                resolved_price=100.25,
                ask_start=100.50,
            ))
            self.assertEqual(host._depth[BookSide.ASK][-1].price, 100.25)

    def test_qualifying_depth_faults_remain_fail_closed(self) -> None:
        with TemporaryDirectory(prefix="pong-no-fhb-reset-") as root:
            reset = self._qualifying_host(root)
            reset._consume_observation(self._qualifying_depth(1))
            reset._consume_observation(self._qualifying_depth(2, reset=True))
            self.assertFalse(reset.status()["canonical_depth_state"]["ready"])
            reset._consume_observation(self._qualifying_depth(3))
            self.assertTrue(reset.status()["canonical_depth_state"]["ready"])

        with TemporaryDirectory(prefix="pong-no-fhb-gap-") as root:
            gap = self._qualifying_host(root)
            gap._consume_observation(self._qualifying_depth(1))
            gap._consume_observation(self._qualifying_depth(3))
            gap._consume_observation(self._qualifying_depth(4))
            self.assertEqual(
                gap.status()["input_error"],
                "PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP",
            )
            self.assertFalse(gap.status()["canonical_depth_state"]["ready"])

        with TemporaryDirectory(prefix="pong-no-fhb-generation-") as root:
            generation = self._qualifying_host(root)
            generation._consume_observation(self._qualifying_depth(1))
            generation._consume_observation(self._qualifying_depth(2, generation=2))
            generation._consume_observation(self._qualifying_depth(3, generation=2))
            self.assertEqual(
                generation.status()["input_error"],
                "PONG_NATIVE_SOURCE_GENERATION_CHANGED",
            )
            self.assertFalse(
                generation.status()["canonical_depth_state"]["ready"],
            )

    def test_qualifying_depth_requires_10x10_and_resolved_mutation_identity(self) -> None:
        with TemporaryDirectory(prefix="pong-no-fhb-depth-refusal-") as root:
            short = self._qualifying_host(root)
            short._consume_observation(self._qualifying_depth(1, levels=9))
            self.assertEqual(
                short.status()["input_error"],
                "PONG_CANONICAL_DEPTH_10X10_REQUIRED",
            )
            self.assertFalse(short.status()["canonical_depth_state"]["ready"])
        with TemporaryDirectory(prefix="pong-no-fhb-identity-refusal-") as root:
            unresolved = self._qualifying_host(root)
            unresolved._consume_observation(self._qualifying_depth(
                1, mutation_price=0,
            ))
            self.assertEqual(
                unresolved.status()["input_error"],
                "PONG_CANONICAL_DEPTH_MUTATION_IDENTITY_UNRESOLVED",
            )
            self.assertFalse(unresolved.status()["canonical_depth_state"]["ready"])

    def test_qualifying_architecture_refuses_archived_fhb(self) -> None:
        with TemporaryDirectory(prefix="pong-no-fhb-refusal-") as root:
            with self.assertRaisesRegex(
                ValueError, "FHB_ARCHIVED_NOT_PART_OF_QUALIFYING_BUILD",
            ):
                PongProductionHost(
                    Path(root),
                    selected_profile=PONG_ENTRY_PROFILE_VERSION,
                    expected_market_source_fingerprint=(
                        expected_observer_source_fingerprint()
                    ),
                    runtime_snapshot=runtime_snapshot,
                    collector_status=pending_collection,
                    controller_dispatch=lambda *_: True,
                    native_dispatch=lambda *_: {},
                    scheduling=PongSchedulingConfig(
                        mode=PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
                        interval_ms=20,
                    ),
                    qualifying_architecture=(
                        PONG_NO_FHB_QUALIFYING_ARCHITECTURE
                    ),
                    fhb_config=FHBConfig.periodic_protected(),
                    topology=DEC26_TOPOLOGY,
                )

    def test_qualifying_architecture_refuses_non_dec26_or_activation(self) -> None:
        from src.l3g_paper.pong_contract import SEP26_TOPOLOGY

        for topology, activation in (
            (SEP26_TOPOLOGY, False),
            (DEC26_TOPOLOGY, True),
        ):
            with self.subTest(topology=topology.version, activation=activation):
                with TemporaryDirectory(prefix="pong-qualifying-identity-") as root:
                    with self.assertRaisesRegex(
                        ValueError, "PONG_QUALIFYING_ARCHITECTURE_IDENTITY_INVALID",
                    ):
                        PongProductionHost(
                            Path(root),
                            selected_profile=PONG_ENTRY_PROFILE_VERSION,
                            expected_market_source_fingerprint=(
                                expected_observer_source_fingerprint()
                            ),
                            runtime_snapshot=runtime_snapshot,
                            collector_status=pending_collection,
                            controller_dispatch=lambda *_: True,
                            native_dispatch=lambda *_: {},
                            scheduling=PongSchedulingConfig(
                                mode=PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
                                interval_ms=20,
                            ),
                            qualifying_architecture=(
                                PONG_NO_FHB_QUALIFYING_ARCHITECTURE
                            ),
                            activation_authorized=activation,
                            topology=topology,
                        )

    def test_qualifying_runtime_refuses_legacy_certificate_identity(self) -> None:
        with TemporaryDirectory(prefix="pong-qualifying-certificate-") as root:
            state = Path(root) / DEC26_TOPOLOGY.state_namespace
            state.mkdir(parents=True)
            journal = state / "journal.jsonl"
            journal.write_text("preserved\n", encoding="utf-8")
            body = {
                "schema": PONG_QUALIFYING_HOUR_SCHEMA,
                "test_only": False,
                "eligible": True,
                "journal_path": str(journal),
            }
            body["certificate_hash"] = canonical_hash(body)
            (state / "qualified-hour.json").write_bytes(json.dumps(
                body, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            ).encode("utf-8"))
            host = self._qualifying_host(root)
            self.assertIsNone(host._certificate)
            self.assertTrue(
                str(host._certificate_error).startswith(
                    "PONG_QUALIFYING_HOUR_CERTIFICATE_INVALID",
                )
            )

    def test_periodic_mode_is_flat_only_and_protection_supersedes_deadline(self) -> None:
        with TemporaryDirectory(prefix="pong-production-") as root:
            host = PongProductionHost(
                Path(root),
                selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection,
                controller_dispatch=lambda _kind, _reason: True,
                native_dispatch=lambda directive, _evidence: {
                    "accepted": True, "command_id": None,
                    "directive_id": directive.directive_id,
                },
                scheduling=PongSchedulingConfig(
                    mode=PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
                    interval_ms=20,
                ),
                clock_ns=lambda: 0,
            )
            machine = SimpleNamespace(
                state=PongState.FLAT_WAIT_ALIGNMENT,
                actual_quantity=0,
                pending_action=None,
            )
            host._bridge = SimpleNamespace(machine=machine, status=lambda: {})
            host._runtime = runtime_snapshot()
            host._last_admitted_receipt_at = datetime.fromisoformat(
                "2026-09-18T00:00:00+00:00",
            )
            evaluations: list[str] = []
            host._evaluate_microstructure = lambda at: evaluations.append(at.isoformat())  # type: ignore[method-assign]

            host._service_periodic_deadline(20_000_000)
            self.assertEqual(len(evaluations), 1)
            self.assertEqual(host.status()["evaluation_schedule"]["active_path"], "PERIODIC_FLAT")

            machine.state = PongState.ENTRY_PENDING_LONG
            host._service_periodic_deadline(40_000_000)
            self.assertEqual(len(evaluations), 1)
            status = host.status()["evaluation_schedule"]
            self.assertEqual(status["active_path"], "EVENT_DRIVEN_PROTECTION")
            self.assertEqual(
                status["deadline_metrics"]["protection_mode_supersessions"], 1,
            )

            host._consume_observation(warm_record())
            host._consume_observation(verified("TRADE", {
                "contract_id": "MNQ SEP26", "price": 100, "size": 1,
                "aggressor_side": "BUY", "aggressor_source": "NATIVE_BID_ASK",
                "source_event_sequence": 100,
                "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
            }, local_sequence=2))
            self.assertEqual(len(evaluations), 2)
            self.assertEqual(
                host.status()["evaluation_schedule"]["event_driven_analytical_evaluations"],
                1,
            )

    def test_lifespan_host_owns_prepared_controller_without_entry_authority(self) -> None:
        with TemporaryDirectory(prefix="pong-production-") as root:
            host = PongProductionHost(
                Path(root),
                selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection,
                controller_dispatch=lambda _kind, _reason: True,
                native_dispatch=lambda directive, _evidence: {
                    "accepted": True, "command_id": None,
                    "directive_id": directive.directive_id,
                },
                activation_authorized=True,
            )
            host.start()
            deadline = time.monotonic() + 3
            while host.status()["controller"] is None and time.monotonic() < deadline:
                time.sleep(0.05)
            status = host.status()
            host.stop()
            self.assertEqual(status["registration"], "PRODUCTION_REGISTERED")
            self.assertEqual(status["controller"]["phase"], "PREPARED")
            self.assertFalse(status["entry_authority_ready"])
            self.assertIn("PONG_AUTHENTIC_HOUR_NOT_QUALIFIED", status["blocking_reasons"])
            self.assertEqual(status["maximum_quantity"], 1)
            self.assertEqual(status["live_authority"], "DISARMED")

    def test_native_window_preserves_stream_domains_and_exposes_real_gap(self) -> None:
        with TemporaryDirectory(prefix="pong-production-") as root:
            host = PongProductionHost(
                Path(root),
                selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection,
                controller_dispatch=lambda _kind, _reason: True,
                native_dispatch=lambda directive, _evidence: {
                    "accepted": True, "command_id": None,
                    "directive_id": directive.directive_id,
                },
            )
            host._consume_observation(warm_record())
            base = {
                "contract_id": "MNQ SEP26", "price": 100, "size": 1,
                "aggressor_side": "BUY", "aggressor_source": "NATIVE_BID_ASK",
                "source_event_sequence": 100,
                "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
            }
            host._consume_observation(verified("TRADE", base, local_sequence=2))
            quote = {
                "contract_id": "MNQ SEP26", "bid": 99.75, "ask": 100,
                "bid_size": 1, "ask_size": 1, "source_event_sequence": 500,
                "publication_policy": "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1",
            }
            host._consume_observation(verified("QUOTE", quote, local_sequence=3))
            host._consume_observation(verified(
                "TRADE", dict(base, source_event_sequence=102), local_sequence=4,
            ))
            host._consume_observation(verified(
                "TRADE", dict(base, source_event_sequence=103), local_sequence=5,
            ))
            status = host.status()
            self.assertEqual(status["stream_counts"]["TRADE"], 2)
            self.assertEqual(status["stream_counts"]["QUOTE"], 1)
            self.assertEqual(status["missing_events"]["TRADE"], 1)
            self.assertEqual(status["missing_events"]["QUOTE"], 0)
            self.assertEqual(status["input_error"], "PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP")

    def test_non_price_bearing_depth_shift_is_retained_without_latching_input_error(self) -> None:
        with TemporaryDirectory(prefix="pong-production-") as root:
            host = PongProductionHost(
                Path(root),
                selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection,
                controller_dispatch=lambda _kind, _reason: True,
                native_dispatch=lambda directive, _evidence: {
                    "accepted": True, "command_id": None,
                    "directive_id": directive.directive_id,
                },
            )
            host._consume_observation(warm_record())
            depth = {
                "contract_id": "MNQ SEP26",
                "side": "Ask",
                "mutation_price": 0,
                "mutation_volume": 0,
                "is_reset": False,
                "bids": [{"price": 99.75, "size": 1}],
                "asks": [{"price": 100, "size": 1}],
                "source_event_sequence": 700,
                "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
            }
            host._consume_observation(verified("DEPTH", depth, local_sequence=2))
            status = host.status()
            self.assertEqual(status["stream_counts"]["DEPTH"], 1)
            self.assertEqual(status["missing_events"]["DEPTH"], 0)
            self.assertIsNone(status["input_error"])


if __name__ == "__main__":
    unittest.main()
