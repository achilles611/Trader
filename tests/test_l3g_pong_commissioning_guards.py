"""Offline denial tests: no native listener, secret, or account is accessed."""
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from src.l3g_paper.contracts import (
    PONG_PAPER_POLICY, PONG_PAPER_RISK_PROFILE, ExecutionAction,
    PaperRuntimeState, resolve_paper_profile,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.pong_quantity import PONG_QUANTITY_POLICY, PongReconciliationProof
from src.l3g_paper.discernment import DiscernmentEngine, VolumeRegime
from tests.test_l3g_discernment import evidence, policy


class PongCommissioningGuardTests(unittest.TestCase):
    def test_registration_never_grants_start_or_direct_entry(self):
        with TemporaryDirectory(prefix='pong-guard-') as root:
            ledger = PaperLedger(Path(root) / 'paper.sqlite3',
                                 policy=PONG_PAPER_POLICY, risk=PONG_PAPER_RISK_PROFILE)
            try:
                runtime = LaneIIIPaperRuntime(ledger)
                runtime._state = PaperRuntimeState.READY_DISARMED
                self.assertEqual(runtime.pong_controller_snapshot()["maximum_quantity"], 1)
                self.assertFalse(runtime.operational_paper_start('pong-test-start')['started'])
                self.assertFalse(runtime._arm_with_capacity_fence_locked()['armed'])
                self.assertFalse(runtime._arm_with_capacity_fence_locked(
                    target_state=PaperRuntimeState.PAPER_RUNNING, allow_starting=True)['armed'])
                # Even a direct call to the final entry boundary cannot use a
                # decision or reach risk/transport while integration is absent.
                self.assertFalse(runtime._request_entry_with_capacity_fence_locked(None))
                for action in (ExecutionAction.ENTER_LONG, ExecutionAction.ENTER_SHORT):
                    with self.subTest(action=action), self.assertRaisesRegex(RuntimeError, 'PONG_NATIVE_INTEGRATION'):
                        runtime._persist_and_send(SimpleNamespace(action=action), None)
                self.assertIsNone(runtime._operational_session)
                self.assertIsNone(runtime._bounded_trial)
                self.assertIsNone(runtime._last_command)
                self.assertEqual(runtime._state, PaperRuntimeState.READY_DISARMED)
            finally:
                ledger.close()

    def test_existing_profile_not_subject_to_pong_registration_guard(self):
        runtime = LaneIIIPaperRuntime.__new__(LaneIIIPaperRuntime)
        runtime.policy = SimpleNamespace(artifact=resolve_paper_profile('BEELZEBUB_SCALPER_V2').policy)
        runtime._pong_profile = False
        self.assertEqual(runtime._pong_activation_blockers(), ())

    def test_prepared_controller_can_start_runtime_but_cannot_admit_entry(self):
        runtime = LaneIIIPaperRuntime.__new__(LaneIIIPaperRuntime)
        runtime.policy = SimpleNamespace(artifact=PONG_PAPER_POLICY)
        runtime._pong_profile = True
        runtime._pong_entry_fence_active = True
        runtime._pong_integration_status = lambda: {
            'native_input_contract': 'READY',
            'native_runtime_bridge': 'READY',
            'bounded_controller': 'BLOCKED',
            'operational_start_ready': True,
            'entry_authority_ready': False,
            'blocking_reasons': ['PONG_RUNTIME_NOT_ACTIVE'],
        }
        self.assertEqual(runtime._pong_operational_start_blockers(), ())
        self.assertIn('PONG_BOUNDED_STOP_UNVERIFIED', runtime._pong_activation_blockers())
        self.assertIn('PONG_CONTROLLER_ENTRY_FENCE_ACTIVE', runtime._pong_activation_blockers())

    def test_pong_frozen_limits_cannot_be_widened(self):
        for changes in ({'maximum_entry_quantity': 2}, {'protective_stop_distance_points': 24}):
            with self.subTest(changes=changes), self.assertRaises((ValueError, TypeError)):
                replace(PONG_PAPER_RISK_PROFILE, **changes)
        with self.assertRaises(ValueError):
            replace(PONG_PAPER_POLICY, trial_required=False)
        self.assertEqual(PONG_PAPER_RISK_PROFILE.maximum_entry_quantity, 1)
        self.assertEqual(PONG_PAPER_POLICY.duration_seconds, 3600)

    def test_reconciliation_rebinding_does_not_relabel_an_old_build(self):
        custom = replace(PONG_QUANTITY_POLICY, reconciliation_source_fingerprint='b' * 64,
                         reconciliation_build_fingerprint='c' * 64)
        at = '2026-09-11T10:00:00Z'
        # Pure fixture only; this helper does not authenticate native messages.
        proof = PongReconciliationProof.create(source_hash='a' * 64, observed_at=at,
                  available_at=at, position_quantity=0, working_owned_orders=0, unresolved_execution=False)
        self.assertIsNotNone(proof.validation_reason(at=at, policy=custom))
        bound = PongReconciliationProof.create(source_hash='a' * 64, observed_at=at,
                  available_at=at, position_quantity=0, working_owned_orders=0, unresolved_execution=False,
                  source_fingerprint='b' * 64, build_fingerprint='c' * 64)
        self.assertIsNone(bound.validation_reason(at=at, policy=custom))
        self.assertNotEqual(custom.configuration_hash, PONG_QUANTITY_POLICY.configuration_hash)
        self.assertIsNotNone(replace(bound, source_hash='d' * 64).validation_reason(at=at, policy=custom))


class AtomicDiscernmentFrameTests(unittest.TestCase):
    @staticmethod
    def engine():
        return DiscernmentEngine(policy(required=(('flow', '1'), ('liquidity', '1')),
                         bounds=(('order-flow', 4.0), ('book', 4.0))))

    @staticmethod
    def frame(revision, first, second, at):
        return (evidence(revision, first, at=at),
                evidence(revision + 1, second, component='liquidity', group='book', at=at))

    def test_complete_frame_emits_one_mirrored_switch_and_duplicate_emits_none(self):
        engine = self.engine()
        seeded = engine.on_evidence_frame(self.frame(1, 2.0, 2.0, 1), volume_regime=VolumeRegime.NORMAL)
        self.assertTrue(seeded.snapshot.valid)
        self.assertIsNone(seeded.switch)
        frame = self.frame(3, -2.0, -2.0, 2)
        switched = engine.on_evidence_frame(frame, volume_regime=VolumeRegime.NORMAL)
        self.assertIsNotNone(switched.switch)
        self.assertEqual(switched.switch.sequence, 1)
        duplicate = engine.on_evidence_frame(frame, volume_regime=VolumeRegime.NORMAL)
        self.assertIsNone(duplicate.switch)
        restored = engine.on_evidence_frame(self.frame(5, 2.0, 2.0, 3), volume_regime=VolumeRegime.NORMAL)
        self.assertEqual(restored.switch.sequence, 2)

    def test_component_replacement_order_cannot_create_transient_switches(self):
        engine = self.engine()
        engine.on_evidence_frame(self.frame(1, -2.0, 4.0, 1), volume_regime=VolumeRegime.NORMAL)
        update = engine.on_evidence_frame(self.frame(3, 4.0, -2.0, 2), volume_regime=VolumeRegime.NORMAL)
        self.assertIsNone(update.switch)
        self.assertEqual(engine._switch_sequence, 0)

    def test_gap_and_partial_frames_cannot_switch(self):
        engine = self.engine()
        with self.assertRaises(ValueError):
            engine.on_evidence_frame((evidence(1, 3.0),), volume_regime=VolumeRegime.NORMAL)
        engine.on_evidence_frame(self.frame(1, 2.0, 2.0, 1), volume_regime=VolumeRegime.NORMAL)
        gap = engine.on_evidence_frame(self.frame(4, -2.0, -2.0, 2), volume_regime=VolumeRegime.NORMAL)
        self.assertFalse(gap.snapshot.valid)
        self.assertIsNone(gap.switch)


if __name__ == '__main__':
    unittest.main()
