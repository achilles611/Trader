from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
import threading
import time
import unittest

from src.l3f_provider.ninjatrader_commission import NinjaTraderListenerWorker
from src.l3f_provider.ninjatrader_observation import (
    LoopbackBridgeConfig,
    NinjaTraderObservation,
    NinjaTraderObservationError,
)
from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.runtime import LaneIIIPaperRuntime, ObservationFanout
from src.l3g_paper.verification import (
    LocalLedgerVerificationController,
    run_local_verification,
)


_OBSERVER_FRESHNESS_THRESHOLD_SECONDS = 15.0


def _ephemeral_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reservation:
        reservation.bind(("127.0.0.1", 0))
        return int(reservation.getsockname()[1])


def _wire_frame(kind: str, sequence: int, *, quote_id: str | None = None) -> bytes:
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    if kind == "QUOTE":
        payload: dict[str, object] = {
            "contract_id": "MNQ SEP26",
            "bid": "100.00",
            "ask": "100.25",
            "bid_size": 10,
            "ask_size": 12,
        }
    elif kind == "TRADE":
        payload = {
            "contract_id": "MNQ SEP26",
            "price": "100.25",
            "size": 2,
            "aggressor_side": "UNKNOWN",
            "aggressor_source": "BID_ASK_CLASSIFICATION",
            "bid_at_trade": "100.00",
            "ask_at_trade": "100.25",
            "derivation_quote_observation_id": quote_id,
        }
    elif kind == "DEPTH":
        payload = {
            "contract_id": "MNQ SEP26",
            "bids": [{"price": "99.75", "size": 10 + sequence % 3}],
            "asks": [{"price": "100.50", "size": 11}],
            "operation": "UPDATE",
            "side": "Bid",
            "mutation_price": "99.75",
            "mutation_volume": 10 + sequence % 3,
            "mutation_position": 0,
            "is_reset": False,
        }
    else:  # pragma: no cover - the bounded producer below owns the kinds
        raise ValueError(kind)
    return (
        json.dumps(
            {
                "schema": "lane-iii-phase-f2-ninjatrader-observation-v1",
                "observation_id": f"nt-load-{sequence}",
                "session_id": "nt-load-market-session",
                "observation_type": kind,
                "ninja_receipt_time": timestamp,
                "local_monotonic_sequence": sequence,
                "provider_timestamp": timestamp,
                "provider_sequence": None,
                "exchange_timestamp": None,
                "account": None,
                "payload": payload,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


class _ContinuousListenerLoad:
    """A bounded real socket/listener/fanout load against a temporary paper ledger."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.ledger = PaperLedger(root / "Epoch-002" / "paper.sqlite3")
        self.runtime = LaneIIIPaperRuntime(self.ledger)
        self.ledger.append(
            "SESSION_AUTHORITY",
            {"reason": "starvation load test verified anchor"},
            identity="l3g-starvation-load-anchor",
        )
        self.audit_root = root / "audit"
        self.full_report = run_local_verification(
            self.ledger.path, self.audit_root, requested_mode="full",
        )
        self.controller = LocalLedgerVerificationController(self.ledger.path, self.audit_root)

        self.shadow_sequences: list[int] = []
        self.rejections: list[NinjaTraderObservationError] = []
        self.duplicates = 0
        self.sink_failures: list[tuple[str, str, str]] = []
        self.sent_sequences: list[int] = []
        self.sent_kinds: list[str] = []
        self.sender_errors: list[BaseException] = []
        self.sender_connected = threading.Event()
        self.sender_stop = threading.Event()
        self.sender_thread: threading.Thread | None = None
        self.freshness_stop = threading.Event()
        self.freshness_thread: threading.Thread | None = None
        self.freshness_samples = 0
        self.stale_freshness_samples = 0
        self.max_level_one_age_seconds = 0.0
        self.max_level_one_stall_seconds = 0.0

        fanout = ObservationFanout(
            shadow_observation=self._shadow_observation,
            shadow_transport=lambda _: None,
            shadow_rejection=lambda _: None,
            shadow_duplicate=lambda: None,
            paper_observation=self.runtime.ingest,
            paper_transport=self.runtime.on_observation_transport_state,
            paper_rejection=self.runtime.on_observation_rejection,
            paper_duplicate=self.runtime.on_observation_duplicate,
            record_failure=lambda sink, event, error: self.sink_failures.append((sink, event, error)),
        )
        self.listener = NinjaTraderListenerWorker(
            LoopbackBridgeConfig(port=_ephemeral_port()),
        )
        self.listener.set_observation_sinks(
            on_observation=fanout.on_observation,
            on_local_bridge_state=fanout.on_transport_state,
            on_rejection=self._on_rejection,
            on_duplicate=self._on_duplicate,
        )

    def _shadow_observation(self, observation: NinjaTraderObservation) -> None:
        self.shadow_sequences.append(observation.local_monotonic_sequence)

    def _on_rejection(self, error: NinjaTraderObservationError) -> None:
        self.rejections.append(error)
        self.runtime.on_observation_rejection(error)

    def _on_duplicate(self) -> None:
        self.duplicates += 1
        self.runtime.on_observation_duplicate()

    def start(self) -> dict[str, object]:
        status = self.listener.start().as_dict()
        if status["state"] != "LISTENING":
            raise RuntimeError(f"listener failed to start: {status}")
        self.sender_thread = threading.Thread(
            target=self._send_continuously,
            name="L3GStarvationLoadSender",
            daemon=True,
        )
        self.sender_thread.start()
        if not self.sender_connected.wait(2.0):
            raise RuntimeError("load sender did not connect")
        self.freshness_thread = threading.Thread(
            target=self._monitor_level_one_freshness,
            name="L3GStarvationFreshnessMonitor",
            daemon=True,
        )
        self.freshness_thread.start()
        self.wait_for_accepted(12)
        return status

    def _monitor_level_one_freshness(self) -> None:
        last_timestamp: str | None = None
        last_advance = time.monotonic()
        while not self.freshness_stop.is_set():
            timestamp = self.listener.status().last_level_one_at
            now_monotonic = time.monotonic()
            if timestamp is not None:
                if timestamp != last_timestamp:
                    last_timestamp = timestamp
                    last_advance = now_monotonic
                try:
                    observed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    age = max(0.0, (datetime.now(timezone.utc) - observed).total_seconds())
                except ValueError:  # pragma: no cover - listener validation owns timestamp shape
                    age = _OBSERVER_FRESHNESS_THRESHOLD_SECONDS
                stall = now_monotonic - last_advance
                self.freshness_samples += 1
                self.max_level_one_age_seconds = max(self.max_level_one_age_seconds, age)
                self.max_level_one_stall_seconds = max(self.max_level_one_stall_seconds, stall)
                if age >= _OBSERVER_FRESHNESS_THRESHOLD_SECONDS:
                    self.stale_freshness_samples += 1
            time.sleep(0.005)

    def _send_continuously(self) -> None:
        deadline = time.monotonic() + 20.0
        sequence = 0
        try:
            with socket.create_connection(
                (self.listener.config.host, self.listener.config.port), timeout=2.0,
            ) as connection:
                connection.settimeout(5.0)
                self.sender_connected.set()
                while not self.sender_stop.is_set() and time.monotonic() < deadline:
                    sequence += 1
                    quote_id = f"nt-load-{sequence}"
                    for kind in ("QUOTE", "TRADE", "DEPTH"):
                        if kind != "QUOTE":
                            sequence += 1
                        connection.sendall(
                            _wire_frame(kind, sequence, quote_id=quote_id if kind == "TRADE" else None)
                        )
                        self.sent_sequences.append(sequence)
                        self.sent_kinds.append(kind)
                    time.sleep(0.004)
                if not self.sender_stop.is_set():
                    raise TimeoutError("bounded continuous sender reached its 20-second deadline")
        except BaseException as error:  # pragma: no cover - asserted by every test
            self.sender_errors.append(error)
            self.sender_connected.set()

    def wait_for_accepted(self, minimum: int, timeout: float = 5.0) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.listener.status().as_dict()
            if int(status["accepted_observations"]) >= minimum:
                return status
            if self.sender_errors:
                break
            time.sleep(0.005)
        raise AssertionError(
            f"listener did not accept {minimum} observations; "
            f"status={self.listener.status().as_dict()} sender_errors={self.sender_errors}"
        )

    def wait_for_progress(
        self, accepted: int, last_level_one_at: str | None, timeout: float = 5.0,
    ) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.listener.status().as_dict()
            if (
                int(status["accepted_observations"]) > accepted
                and status["last_level_one_at"] != last_level_one_at
            ):
                return status
            if self.sender_errors:
                break
            time.sleep(0.005)
        raise AssertionError(
            "listener level-one timestamp did not advance; "
            f"status={self.listener.status().as_dict()} sender_errors={self.sender_errors}"
        )

    def tail_preflight(self, _: str, __: object) -> dict[str, object]:
        return self.ledger.commissioning_tail_snapshot(
            int(self.full_report["verified_through_sequence"]),
            last_full_verified_sequence=int(self.full_report["last_full_verified_sequence"]),
        )

    def wait_for_verifier(
        self, verification_id: str, timeout: float = 15.0,
    ) -> dict[str, object]:
        deadline = time.monotonic() + timeout
        latest: dict[str, object] = {}
        while time.monotonic() < deadline:
            latest = self.controller.status()
            if (
                latest.get("verification_id") == verification_id
                and latest.get("status") != "IN_PROGRESS"
            ):
                return latest
            time.sleep(0.02)
        raise AssertionError(f"detached verifier did not finish: {latest}")

    def stop_sender_and_drain(self) -> dict[str, object]:
        self.sender_stop.set()
        if self.sender_thread is not None:
            self.sender_thread.join(6.0)
            if self.sender_thread.is_alive():
                raise AssertionError("continuous sender did not stop")
        expected = len(self.sent_sequences)
        status = self.wait_for_accepted(expected, timeout=8.0)
        self.ledger.flush_deferred()
        self.freshness_stop.set()
        if self.freshness_thread is not None:
            self.freshness_thread.join(2.0)
        return status

    def stored_observation_sequences(self) -> list[int]:
        with self.ledger._lock:  # test-only coherent read after the sender drains
            rows = self.ledger._connection.execute(
                "SELECT payload_json FROM lane_iii_paper_audit "
                "WHERE domain='OBSERVATION' ORDER BY ledger_sequence"
            ).fetchall()
        return [int(json.loads(str(row[0]))["payload"]["local_monotonic_sequence"]) for row in rows]

    def shutdown(self) -> None:
        self.sender_stop.set()
        self.freshness_stop.set()
        if self.sender_thread is not None:
            self.sender_thread.join(6.0)
        if self.freshness_thread is not None:
            self.freshness_thread.join(2.0)
        for child in tuple(self.controller._children.values()):
            if child.poll() is None:
                self.controller.cancel()
                try:
                    child.wait(timeout=5.0)
                except Exception:
                    child.terminate()
                    child.wait(timeout=5.0)
        self.listener.stop(timeout_seconds=5.0)
        self.ledger.close()


class LaneIIIStarvationHotfixLoadTests(unittest.TestCase):
    def assert_exact_load_integrity(
        self, rig: _ContinuousListenerLoad, listener_status: dict[str, object],
    ) -> None:
        self.assertEqual(listener_status["state"], "LISTENING")
        self.assertEqual(listener_status["market_observer_state"], "ACTIVE")
        self.assertEqual(listener_status["accepted_observations"], len(rig.sent_sequences))
        self.assertEqual(sum(listener_status["observation_types"].values()), len(rig.sent_sequences))
        self.assertEqual(listener_status["observation_types"].get("QUOTE"), rig.sent_kinds.count("QUOTE"))
        self.assertEqual(listener_status["observation_types"].get("TRADE"), rig.sent_kinds.count("TRADE"))
        self.assertEqual(listener_status["observation_types"].get("DEPTH"), rig.sent_kinds.count("DEPTH"))
        self.assertEqual(rig.rejections, [])
        self.assertEqual(rig.duplicates, 0)
        self.assertEqual(rig.sink_failures, [])
        self.assertEqual(rig.sender_errors, [])
        self.assertEqual(rig.shadow_sequences, rig.sent_sequences)
        self.assertEqual(rig.stored_observation_sequences(), rig.sent_sequences)
        self.assertEqual(rig.ledger.counts().get("OBSERVATION"), len(rig.sent_sequences))
        self.assertEqual(rig.runtime.policy.status()["last_local_sequence"], rig.sent_sequences[-1])
        self.assertEqual(rig.runtime.policy.status()["counters"]["local_sequence_gaps"], 0)
        self.assertEqual(rig.ledger.verify_chain(), (True, None))
        self.assertGreater(rig.freshness_samples, 0)
        self.assertEqual(rig.stale_freshness_samples, 0)
        self.assertLess(
            rig.max_level_one_age_seconds, _OBSERVER_FRESHNESS_THRESHOLD_SECONDS,
        )
        self.assertLess(
            rig.max_level_one_stall_seconds, _OBSERVER_FRESHNESS_THRESHOLD_SECONDS,
        )

    def test_repeated_rehearsal_keeps_real_listener_and_paper_ingest_advancing(self) -> None:
        with TemporaryDirectory() as folder:
            rig = _ContinuousListenerLoad(Path(folder))
            try:
                rig.start()
                before = rig.listener.status().as_dict()
                results = [rig.runtime.commissioning_rehearsal(rig.tail_preflight) for _ in range(20)]
                during = rig.wait_for_progress(
                    int(before["accepted_observations"]), str(before["last_level_one_at"]),
                )
                self.assertIsNotNone(rig.sender_thread)
                self.assertTrue(rig.sender_thread.is_alive())
                self.assertTrue(all(result["result"] == "BLOCKED" for result in results))
                self.assertTrue(all(result["commissioning_preflight_duration_seconds"] >= 0 for result in results))
                self.assertEqual(during["state"], "LISTENING")
                self.assertEqual(during["market_observer_state"], "ACTIVE")
                runtime_status = rig.runtime.status()
                self.assertEqual(runtime_status["entry_owner"], "NONE")
                self.assertFalse(runtime_status["commissioning_lifecycle"]["active"])
                self.assertEqual(runtime_status["live_capital"], "DENIED")
                ledger_health = rig.ledger.health_status()
                self.assertIsNotNone(ledger_health["last_deferred_barrier_token"])
                self.assertGreaterEqual(int(ledger_health["deferred_queue_high_water"]), 1)

                final_listener = rig.stop_sender_and_drain()
                self.assert_exact_load_integrity(rig, final_listener)
                self.assertGreater(
                    int(rig.ledger.health_status()["highest_sequence"]),
                    int(rig.full_report["verified_through_sequence"]),
                )
            finally:
                rig.shutdown()

    def test_detached_incremental_verifier_and_rehearsal_overlap_preserve_load(self) -> None:
        with TemporaryDirectory() as folder:
            rig = _ContinuousListenerLoad(Path(folder))
            try:
                rig.start()
                before = rig.listener.status().as_dict()
                preflight_reached = threading.Event()
                release_preflight = threading.Event()
                rehearsal_result: list[dict[str, object]] = []
                rehearsal_errors: list[BaseException] = []

                def overlapping_preflight(commissioning_id: str, snapshot: object) -> dict[str, object]:
                    result = rig.tail_preflight(commissioning_id, snapshot)
                    preflight_reached.set()
                    if not release_preflight.wait(5.0):
                        raise TimeoutError("overlap rehearsal was not released")
                    return result

                def rehearse() -> None:
                    try:
                        rehearsal_result.append(rig.runtime.commissioning_rehearsal(overlapping_preflight))
                    except BaseException as error:  # pragma: no cover - asserted below
                        rehearsal_errors.append(error)

                rehearsal_thread = threading.Thread(target=rehearse, name="L3GOverlapRehearsal")
                rehearsal_thread.start()
                self.assertTrue(preflight_reached.wait(5.0))
                progress_while_rehearsal_waited = rig.wait_for_progress(
                    int(before["accepted_observations"]), str(before["last_level_one_at"]),
                )

                started = rig.controller.start("auto")
                verification_id = str(started["verification_id"])
                self.assertEqual(started["requested_mode"], "auto")
                release_preflight.set()
                rehearsal_thread.join(5.0)
                self.assertFalse(rehearsal_thread.is_alive())
                self.assertEqual(rehearsal_errors, [])
                self.assertEqual(len(rehearsal_result), 1)
                self.assertEqual(rehearsal_result[0]["result"], "BLOCKED")

                repeated = [rig.runtime.commissioning_rehearsal(rig.tail_preflight) for _ in range(10)]
                report = rig.wait_for_verifier(verification_id)
                after_verifier = rig.wait_for_progress(
                    int(progress_while_rehearsal_waited["accepted_observations"]),
                    str(progress_while_rehearsal_waited["last_level_one_at"]),
                )
                self.assertTrue(all(result["result"] == "BLOCKED" for result in repeated))
                self.assertEqual(after_verifier["state"], "LISTENING")
                self.assertEqual(after_verifier["market_observer_state"], "ACTIVE")
                self.assertEqual(report["status"], "PASS", report)
                self.assertEqual(report["requested_mode"], "auto")
                self.assertEqual(report["verification_mode"], "incremental")
                self.assertTrue(report["chain_valid"])
                self.assertTrue(report["checkpoint_valid"])
                self.assertFalse(report["full_scan_required"])
                self.assertTrue(rig.controller.checkpoint_matches_report(report))

                final_listener = rig.stop_sender_and_drain()
                self.assert_exact_load_integrity(rig, final_listener)
                runtime_status = rig.runtime.status()
                self.assertEqual(runtime_status["entry_owner"], "NONE")
                self.assertFalse(runtime_status["commissioning_lifecycle"]["active"])
                self.assertEqual(runtime_status["live_capital"], "DENIED")
            finally:
                rig.shutdown()


if __name__ == "__main__":
    unittest.main()
