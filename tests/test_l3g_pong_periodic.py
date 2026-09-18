from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import itertools
import unittest
from unittest.mock import Mock, patch

from src.l3f_provider.ninjatrader_observation import expected_observer_source_fingerprint
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION
from src.l3g_paper.pong_periodic import OfflinePeriodicPong
from src.l3g_paper.pong_production import PongProductionHost
from tests.test_l3g_pong_inputs import verified


START = datetime(2026, 9, 11, 12, 30, tzinfo=timezone.utc)


def trade(sequence=1, *, milliseconds=0, generation=1):
    return verified("TRADE", {
        "contract_id": "MNQ SEP26", "price": 100, "size": 1,
        "aggressor_side": "BUY", "aggressor_source": "NATIVE_BID_ASK",
        "source_event_sequence": sequence, "source_generation": generation,
        "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
    }, local_sequence=sequence, observed=(START + timedelta(milliseconds=milliseconds)).isoformat())


def depth(sequence, volume, *, milliseconds=0):
    return verified("DEPTH", {
        "contract_id": "MNQ SEP26", "side": "Bid", "mutation_price": 99.75,
        "mutation_volume": volume, "is_reset": False,
        "bids": [{"price": 99.75, "size": volume}],
        "asks": [{"price": 100, "size": 1}],
        "source_event_sequence": sequence, "source_generation": 1,
        "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
    }, local_sequence=sequence, observed=(START + timedelta(milliseconds=milliseconds)).isoformat())


class PeriodicPongTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory(prefix="pong-periodic-test-")
        self.addCleanup(self.tmp.cleanup)
        self.host = PongProductionHost(
            Path(self.tmp.name), selected_profile=PONG_ENTRY_PROFILE_VERSION,
            expected_market_source_fingerprint=expected_observer_source_fingerprint(),
            runtime_snapshot=lambda: {}, collector_status=lambda: {},
            controller_dispatch=lambda *_: self.fail("control dispatch forbidden"),
            native_dispatch=lambda *_: self.fail("native dispatch forbidden"),
        )

    def test_default_delegates_frozen_event_behavior(self):
        adapter = OfflinePeriodicPong(self.host)
        original = self.host._consume_observation
        self.host._consume_observation = Mock(wraps=original)
        adapter.consume(trade())
        self.host._consume_observation.assert_called_once()
        self.assertEqual(self.host._last_microstructure_at, START)
        self.assertIsNone(adapter.tick(99_000_000, START))

    def test_between_tick_add_remove_and_small_changes_remain_in_view(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20)
        for sequence, volume in enumerate((5, 6, 7, 0), 1):
            adapter.consume(depth(sequence, volume, milliseconds=sequence))
        self.assertIsNone(self.host._last_microstructure_at)
        result = adapter.tick(20_000_000, START + timedelta(milliseconds=20))
        self.assertEqual([item.displayed_quantity for item in result.view.bids], [5, 6, 7, 0])
        self.assertEqual(result.view.watermark, (("DEPTH", 4),))
        with self.assertRaises(FrozenInstanceError):
            result.view.input_error = "mutated"
        adapter.consume(depth(5, 10, milliseconds=21))
        self.assertEqual(len(result.view.bids), 4)
        self.assertEqual(len(tuple(self.host._depth[next(iter(self.host._depth))])), 5)

    def test_absolute_deadlines_skip_obsolete_jobs_and_keep_phase(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20, phase_ns=10_000_000)
        self.assertIsNone(adapter.tick(9_000_000, START))
        first = adapter.tick(10_000_000, START)
        self.assertEqual(first.missed_deadlines, 0)
        late = adapter.tick(75_000_000, START + timedelta(milliseconds=65))
        self.assertEqual(late.missed_deadlines, 2)
        self.assertEqual(late.lateness_ns, 45_000_000)
        self.assertEqual(adapter.next_deadline_ns, 90_000_000)
        self.assertIsNone(adapter.tick(75_000_000, START + timedelta(milliseconds=65)))
        self.assertEqual(adapter.metrics()["evaluations"], 2)

    def test_quiet_ticks_recalculate_expiry_without_new_arrivals(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20)
        adapter.consume(trade())
        original = self.host._evaluate_microstructure
        self.host._evaluate_microstructure = Mock(wraps=original)
        adapter.tick(0, START)
        result = adapter.tick(6_000_000_000, START + timedelta(seconds=6))
        self.assertEqual(self.host._evaluate_microstructure.call_count, 2)
        self.assertEqual(result.view.trades[0].event.available_at, START.isoformat())
        self.assertEqual(result.blockers, ("PONG_MICROSTRUCTURE_NATIVE_WINDOW_INCOMPLETE",))
        self.assertEqual(adapter.metrics()["records_consumed"], 1)

    def test_blocked_frame_advances_existing_discernment_expiry_without_switch(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20)
        engine = Mock()
        engine.last_snapshot = object()
        self.host._discernment = engine
        result = adapter.tick(0, START)
        engine.on_expiry.assert_called_once_with("2026-09-11T12:30:00Z")
        self.assertTrue(result.expiry_maintenance)

    def test_persistent_service_overrun_is_failed_not_relaxed(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=1)
        with patch("src.l3g_paper.pong_periodic.time.perf_counter_ns",
                   side_effect=itertools.count(0, 1_000_000)):
            for index in range(3):
                adapter.tick(index * 1_000_000, START + timedelta(milliseconds=index))
        self.assertEqual(adapter.metrics()["service_overruns"], 3)
        self.assertTrue(adapter.metrics()["persistent_overrun_failed"])
        self.assertEqual(adapter.interval_ns, 1_000_000)

    def test_gap_epoch_and_future_timestamp_fail_closed(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20)
        adapter.consume(trade(1))
        adapter.consume(trade(3))
        result = adapter.tick(0, START)
        self.assertFalse(result.evaluated)
        self.assertEqual(result.blockers, ("PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP",))
        adapter.consume(trade(4, generation=2))
        result = adapter.tick(20_000_000, START)
        self.assertFalse(result.evaluated)
        self.assertEqual(result.view.epoch[1], 2)
        self.assertEqual(result.blockers, ("PONG_NATIVE_SOURCE_GENERATION_CHANGED",))

    def test_future_provider_timestamp_is_rejected_during_continuous_apply(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20)
        value = replace(trade(), provider_timestamp=(START + timedelta(seconds=1)).isoformat())
        adapter.consume(value)
        self.assertEqual(self.host._input_error, "PONG_NATIVE_PROVIDER_TIMESTAMP_AFTER_RECEIPT")
        self.assertFalse(adapter.tick(0, START).evaluated)

    def test_not_yet_available_input_cannot_enter_earlier_cutoff(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20)
        adapter.consume(trade(milliseconds=1))
        with self.assertRaisesRegex(ValueError, "INPUT_AFTER_CUTOFF"):
            adapter.tick(0, START)
        self.assertEqual(self.host._input_error, "PONG_PERIODIC_INPUT_AFTER_CUTOFF")

    def test_offline_boundary_checked_on_every_call(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20)
        self.host._bridge = object()
        with self.assertRaisesRegex(RuntimeError, "OFFLINE_DISARMED"):
            adapter.consume(trade())
        self.host._bridge = None
        self.host.activation_authorized = True
        with self.assertRaisesRegex(RuntimeError, "OFFLINE_DISARMED"):
            adapter.tick(0, START)

    def test_fault_and_detached_protection_fixture_do_not_wait_for_tick(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20)
        adapter.tick(0, START)
        state = {"protected_quantity": 0}
        adapter.service_control(state.update, protected_quantity=1)
        self.assertEqual(state["protected_quantity"], 1)
        adapter.latch_fault("DISCONNECT")
        self.assertEqual(self.host._microstructure_blockers, ("DISCONNECT",))
        self.assertIsNone(adapter.tick(1, START))
        self.assertFalse(adapter.tick(20_000_000, START).evaluated)
        self.assertIn("UNPROVEN", adapter.metrics()["protection_equivalence"])

    def test_reentrancy_and_clock_regression_are_rejected(self):
        adapter = OfflinePeriodicPong(self.host, interval_ms=20)
        def nested(_at):
            with self.assertRaisesRegex(RuntimeError, "REENTRANT"):
                adapter.tick(1, START)
        self.host._evaluate_microstructure = nested
        adapter.tick(0, START)
        with self.assertRaisesRegex(ValueError, "CLOCK_REGRESSED"):
            adapter.tick(-1, START)


class SyntheticProtectiveCadenceCounterexamples(unittest.TestCase):
    """Synthetic score timelines through the real detached state machines.

    These are architecture counterexamples, not reconstructed market signals,
    fills, profitability evidence, or market decision-fidelity measurements.
    """

    def frame(self, engine, score, at):
        from src.l3g_paper.discernment import DiscernmentEvidence, VolumeRegime
        from tests.test_l3g_pong import snapshot
        # Scores .31/.69 fit the production shared dependency bound using
        # required components only; optional research evidence is excluded.
        template = snapshot(0.31 if score < 0.5 else 0.69, minute=1, policy=engine.policy)
        components = tuple(item for item in template.component_effects if item.required)
        first_revision = engine.evidence_revision + 1
        values = tuple(DiscernmentEvidence(
            event_id=f"synthetic-cadence-{at.isoformat()}-{item.component_id}",
            component_id=item.component_id, component_version=item.component_version,
            dependency_group=item.dependency_group, log_odds_effect=item.log_odds_effect,
            required=item.required, source_event_at=at.isoformat(), received_at=at.isoformat(),
            available_at=at.isoformat(), valid_until=(at + timedelta(seconds=30)).isoformat(),
            evidence_revision=first_revision + index,
            continuity_domain="synthetic-protective-cadence-only", continuity_valid=True,
            source_ids=(f"synthetic-source-{at.isoformat()}-{item.component_id}",),
            source_hashes=("a" * 64,),
        ) for index, item in enumerate(components))
        return engine.on_evidence_frame(values, volume_regime=VolumeRegime.NORMAL)

    def held_long_pair(self):
        import copy
        from src.l3g_paper.discernment import DiscernmentDirection, DiscernmentEngine
        from src.l3g_paper.pong import PongDirection, PongStateMachine
        from tests.test_l3g_pong import PongStateMachineTests
        machine = PongStateMachine()
        engine = DiscernmentEngine(machine.discernment_policy)
        at = datetime(2026, 9, 11, 1, 1, 0, 1000, tzinfo=timezone.utc)
        seed_update = self.frame(engine, 0.30, at - timedelta(minutes=1))
        machine.observe_startup_seed(DiscernmentDirection.SHORT, seed_update.snapshot)
        entry_update = self.frame(engine, 0.70, at)
        self.assertIsNotNone(entry_update.switch)
        PongStateMachineTests()._enter(machine, entry_update.switch, PongDirection.LONG)
        self.assertEqual(machine.protected_quantity, 1)
        return at, engine, machine, copy.deepcopy(engine), copy.deepcopy(machine)

    def test_transient_opposite_signal_between_ticks_loses_protective_exit(self):
        import json
        from src.l3g_paper.daily_bias import DailyBiasDirection
        from src.l3g_paper.pong import PongActionKind, PongState
        from tests.test_l3g_pong import bias
        for interval_ms in (10, 20, 25):
            with self.subTest(interval_ms=interval_ms):
                at, event_engine, event_machine, periodic_engine, periodic_machine = self.held_long_pair()
                short_at = at + timedelta(milliseconds=interval_ms / 4)
                returned_at = at + timedelta(milliseconds=interval_ms / 2)
                tick_at = at + timedelta(milliseconds=interval_ms)
                short_update = self.frame(event_engine, 0.30, short_at)
                exit_update = event_machine.on_switch(short_update.switch, bias(DailyBiasDirection.LONG))
                self.assertEqual(event_machine.state, PongState.EXIT_PENDING)
                self.assertEqual(exit_update.actions[0].kind, PongActionKind.CANCEL_PROTECTIVE_STOP)
                returned = self.frame(event_engine, 0.70, returned_at)
                event_machine.on_switch(returned.switch, bias(DailyBiasDirection.LONG))
                periodic_update = self.frame(periodic_engine, 0.70, tick_at)
                self.assertIsNone(periodic_update.switch)
                self.assertEqual(periodic_machine.on_snapshot_update(periodic_update.snapshot).actions, ())
                self.assertEqual(periodic_machine.state, PongState.LONG)
                self.assertEqual(periodic_machine.protected_quantity, 1)
                self.assertEqual(event_machine.state, PongState.EXIT_PENDING)
                print(json.dumps(dict(schema="pong-periodic-synthetic-protection-v1",
                    fixture="TRANSIENT_OPPOSITE_SIGNAL", evidence_kind="SYNTHETIC_NOT_MARKET",
                    interval_ms=interval_ms, short_at=short_at.isoformat(),
                    returned_long_at=returned_at.isoformat(), tick_at=tick_at.isoformat(),
                    baseline_action=exit_update.actions[0].kind.value,
                    periodic_action=None, baseline_state=event_machine.state.value,
                    periodic_state=periodic_machine.state.value,
                    protective_outcome="CHANGED_EXIT_SIGNAL_MISSED")), flush=True)

    def test_sustained_opposite_signal_delays_exit_until_periodic_tick(self):
        import json
        from src.l3g_paper.daily_bias import DailyBiasDirection
        from src.l3g_paper.pong import PongActionKind, PongState
        from tests.test_l3g_pong import bias
        for interval_ms in (10, 20, 25):
            with self.subTest(interval_ms=interval_ms):
                at, event_engine, event_machine, periodic_engine, periodic_machine = self.held_long_pair()
                short_at = at + timedelta(milliseconds=interval_ms / 4)
                tick_at = at + timedelta(milliseconds=interval_ms)
                event_update = self.frame(event_engine, 0.30, short_at)
                event_result = event_machine.on_switch(event_update.switch, bias(DailyBiasDirection.LONG))
                self.assertEqual(periodic_machine.state, PongState.LONG)
                periodic_update = self.frame(periodic_engine, 0.30, tick_at)
                periodic_result = periodic_machine.on_switch(periodic_update.switch, bias(DailyBiasDirection.LONG))
                self.assertEqual(event_result.actions[0].kind, PongActionKind.CANCEL_PROTECTIVE_STOP)
                self.assertEqual(periodic_result.actions[0].kind, PongActionKind.CANCEL_PROTECTIVE_STOP)
                self.assertEqual(periodic_machine.state, PongState.EXIT_PENDING)
                delay = datetime.fromisoformat(periodic_update.switch.available_at.replace("Z", "+00:00")) - datetime.fromisoformat(event_update.switch.available_at.replace("Z", "+00:00"))
                self.assertEqual(delay, timedelta(milliseconds=interval_ms * .75))
                print(json.dumps(dict(schema="pong-periodic-synthetic-protection-v1",
                    fixture="SUSTAINED_OPPOSITE_SIGNAL", evidence_kind="SYNTHETIC_NOT_MARKET",
                    interval_ms=interval_ms, short_at=short_at.isoformat(), tick_at=tick_at.isoformat(),
                    baseline_action=event_result.actions[0].kind.value,
                    periodic_action=periodic_result.actions[0].kind.value,
                    baseline_state=event_machine.state.value,
                    periodic_state=periodic_machine.state.value,
                    exit_action_delay_ms=delay.total_seconds() * 1000,
                    protective_outcome="CHANGED_EXIT_SIGNAL_DELAYED")), flush=True)


if __name__ == "__main__":
    unittest.main()
