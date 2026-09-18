"""TEST ONLY authorization and dispatch-fence fixtures; no market history."""

from datetime import date
import hashlib
import json
import queue
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from src.l3g_paper.pong_authorization import PongLaunchAuthorization, _FIXED
from src.l3g_paper.pong_production import PongProductionHost
from tests.test_l3g_pong_controller import observation
from tests.test_l3g_pong_production import runtime_snapshot, pending_collection
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION
from src.l3f_provider.ninjatrader_observation import expected_observer_source_fingerprint
from tests.test_l3g_pong_inputs import verified


class PongAuthorizationTests(unittest.TestCase):
    def test_v2_binds_actual_acknowledgment_and_expiry_without_widening_scope(self):
        with TemporaryDirectory() as temp:
            source = Path(temp) / 'TEST-ONLY-source.txt'
            source.write_text('TEST ONLY explicit authorization', encoding='utf-8')
            value = dict(_FIXED, schema='pong-single-trial-authorization-v2',
                         activation_date_local='2026-09-14', ny_open_blackout=True,
                         source_path=str(source.resolve()), source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                         operator_acknowledged_at_utc='2026-09-14T16:00:00Z',
                         operator_acknowledged_at_denver='2026-09-14T10:00:00-06:00',
                         expires_at_utc='2026-09-15T06:00:00Z')
            path = Path(temp) / 'TEST-ONLY-authorization.json'
            path.write_text(json.dumps(value), encoding='utf-8')
            authorization = PongLaunchAuthorization.load(path)
            self.assertEqual(authorization.expires_at_utc, '2026-09-15T06:00:00Z')
            for changes in ({'expires_at_utc': '2026-09-15T07:00:00Z'},
                            {'operator_acknowledged_at_denver': '2026-09-14T09:00:00-06:00'},
                            {'operator_acknowledged_at_utc': '2026-09-15T06:00:00Z'}):
                path.write_text(json.dumps(dict(value, **changes)), encoding='utf-8')
                with self.assertRaisesRegex(ValueError, 'TIME_SCOPE_INVALID'):
                    PongLaunchAuthorization.load(path)

    def test_callback_queue_preserves_two_events_inside_one_second_without_timer_events(self):
        """TEST ONLY: pause worker A, enqueue B, then prove both causal evaluations."""
        with TemporaryDirectory() as temp:
            host = PongProductionHost(
                temp, selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot, collector_status=pending_collection,
                controller_dispatch=lambda *_: False, native_dispatch=lambda *_: {},
            )
            paused, release, done = threading.Event(), threading.Event(), threading.Event()
            evaluations = []
            def evaluate(at):
                evaluations.append((at.isoformat(), host._trades[-1].event.event_id))
                if len(evaluations) == 1:
                    paused.set()
                    if not release.wait(2):
                        raise AssertionError("TEST ONLY release missing")
                else:
                    done.set()
            payload = {"contract_id": "MNQ SEP26", "price": 100, "size": 1,
                       "aggressor_side": "BUY", "aggressor_source": "NATIVE_BID_ASK",
                       "source_event_sequence": 1,
                       "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1"}
            first = verified("TRADE", payload, local_sequence=1, observed="2026-09-11T18:30:00.001Z")
            second = verified("TRADE", dict(payload, source_event_sequence=2),
                              local_sequence=2, observed="2026-09-11T18:30:00.002Z")
            with patch.object(host, "_evaluate_microstructure", side_effect=evaluate):
                host.start()
                try:
                    host.on_observation(first)
                    self.assertTrue(paused.wait(2))
                    # This callback must return while the worker owns its state/I/O lock.
                    admitted = threading.Event()
                    def admit_second():
                        host.on_observation(second)
                        admitted.set()
                    sender = threading.Thread(target=admit_second)
                    sender.start()
                    self.assertTrue(admitted.wait(1))
                    release.set()
                    sender.join(1)
                    self.assertTrue(done.wait(2))
                    threading.Event().wait(0.6)
                    self.assertEqual([item[1] for item in evaluations],
                                     [first.observation_id, second.observation_id])
                finally:
                    release.set()
                    host.stop()

    def test_production_queue_overflow_is_explicit_and_denies_launch(self):
        with TemporaryDirectory() as temp:
            host = PongProductionHost(
                temp, selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot, collector_status=pending_collection,
                controller_dispatch=lambda *_: False, native_dispatch=lambda *_: {},
            )
            host._observations = queue.Queue(maxsize=1)
            record = verified("TRADE", {
                "contract_id": "MNQ SEP26", "price": 100, "size": 1,
                "aggressor_side": "BUY", "source_event_sequence": 1,
                "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
            }, local_sequence=1)
            host.on_observation(record)
            host.on_observation(record)
            status = host.status()
            self.assertEqual(status["observation_queue_overflow"], 1)
            self.assertEqual(status["input_error"], "PONG_PRODUCTION_OBSERVATION_QUEUE_OVERFLOW")
            self.assertFalse(status["operational_start_ready"])

    def test_missing_volume_history_and_expired_bias_remain_launch_blockers(self):
        with TemporaryDirectory() as temp:
            host = PongProductionHost(
                temp, selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot, collector_status=pending_collection,
                controller_dispatch=lambda *_: False, native_dispatch=lambda *_: {},
                activation_authorized=True,
            )
            host._certificate = {}
            host._daily_bias = SimpleNamespace(expires_at="2026-09-11T20:05:00Z")
            host._microstructure_blockers = ("VOLUME_REGIME_INSUFFICIENT_PRIOR_HISTORY",)
            with patch("src.l3g_paper.pong_production._now", return_value="2026-09-14T01:30:00Z"):
                status = host.authority_status()
                self.assertFalse(status["operational_start_ready"])
                self.assertIn("VOLUME_REGIME_INSUFFICIENT_PRIOR_HISTORY", status["blocking_reasons"])
                self.assertIn("PONG_QUALIFIED_HOUR_EXPIRED", status["blocking_reasons"])

    def test_completed_hour_handoff_waits_for_durability_after_collector_rollover(self):
        """TEST ONLY: assert the host handoff; never write a market certificate."""
        with TemporaryDirectory() as temp:
            host = PongProductionHost(
                temp, selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot, collector_status=pending_collection,
                controller_dispatch=lambda *_: False, native_dispatch=lambda *_: {},
            )
            hour = SimpleNamespace(start="2026-09-14T02:00:00Z", end="2026-09-14T03:00:00Z",
                                   available_at="2026-09-14T03:00:01Z", payload=lambda: {})
            host._hour_pair = (hour, hour)
            complete = {"eligible": True, "complete": True, "blockers": [],
                        "start_utc": hour.start, "end_utc": hour.end}
            host._collection = {
                "required_interval": {"eligible": False, "complete": False},
                "recent_completed_intervals": [complete],
                "durability": {"enqueued_records": 10, "written_records": 10,
                               "durable_records": 9, "queue_overflow_records": 0,
                               "rejected_records": 0, "duplicate_records": 0,
                               "last_fsync_at": hour.available_at},
            }
            bias = SimpleNamespace(configured=True, result_hash="a" * 64, payload=lambda: {})
            with patch.object(host, "_bias_from_inputs", return_value=bias), patch(
                "src.l3g_paper.pong_production._atomic_write"
            ) as persist:
                host._maybe_certify_hour()
                persist.assert_not_called()
                host._collection["durability"]["durable_records"] = 10
                host._maybe_certify_hour()
                persist.assert_called_once()
                self.assertEqual(persist.call_args.args[1]["required_interval"], complete)

    def test_scope_source_binding_and_stable_one_run_identity(self):
        with TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "TEST-ONLY-authority.md"
            source.write_text("TEST ONLY: one simulated fixture", encoding="utf-8")
            value = dict(_FIXED, activation_date_local="2026-09-13", ny_open_blackout=False,
                         source_path=str(source.resolve()),
                         source_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
            manifest = root / "authorization.json"
            manifest.write_text(json.dumps(value), encoding="utf-8")
            original = PongLaunchAuthorization.load(manifest)
            self.assertEqual(original, PongLaunchAuthorization.load(manifest))
            self.assertEqual(original.activation_date_local, date(2026, 9, 13))
            for key, wrong in (("maximum_quantity", 2), ("run_duration_seconds", 7200),
                               ("account", "Live"), ("maximum_runs", 2)):
                manifest.write_text(json.dumps(dict(value, **{key: wrong})), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "SCOPE_INVALID"):
                    PongLaunchAuthorization.load(manifest)
            manifest.write_text(json.dumps(value), encoding="utf-8")
            source.write_text("Changed authority", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "SOURCE_MISMATCH"):
                PongLaunchAuthorization.load(manifest)

    def test_production_authority_rechecks_durable_lease_deadline_and_date(self):
        with TemporaryDirectory() as temp:
            authorization = PongLaunchAuthorization(
                "pong-single-trial-test-only", date(2026, 9, 13), False, "a" * 64, "b" * 64,
            )
            host = PongProductionHost(
                temp, selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot, collector_status=pending_collection,
                controller_dispatch=lambda *_: False, native_dispatch=lambda *_: {},
                activation_authorized=True, launch_authorization=authorization,
            )
            with patch("src.l3g_paper.pong_production._now", return_value="2026-09-14T01:30:00Z"):
                host._open_controller()
                controller = host._controller
                self.assertIsNotNone(controller)
                self.assertIn("runs", controller.path.parts)
                self.assertFalse((Path(temp) / "controller-state.json").exists())
                controller.activate(observation("2026-09-14T01:30:00Z"))
                # Isolate the controller gate from the already-tested native gates.
                with patch.object(host, "_blockers_locked", return_value=[]):
                    self.assertTrue(host.authority_status()["entry_authority_ready"])
            with patch.object(host, "_blockers_locked", return_value=[]):
                for timestamp, reason in (
                    ("2026-09-14T01:30:06Z", "PONG_CONTROLLER_HEARTBEAT_EXPIRED"),
                    ("2026-09-14T02:30:00Z", "PONG_BOUNDED_RUN_DEADLINE_REACHED"),
                ):
                    with patch("src.l3g_paper.pong_production._now", return_value=timestamp):
                        status = host.authority_status()
                        self.assertFalse(status["entry_authority_ready"])
                        self.assertEqual(status["bounded_controller"], "BLOCKED")
                        self.assertIn(reason, status["blocking_reasons"])


if __name__ == "__main__":
    unittest.main()
