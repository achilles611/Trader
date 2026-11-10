"""Focused checks for the offline-only P2/P5 experiment entry point."""
from copy import deepcopy
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts.lane_ii_continuous import long_research as research
from scripts.lane_ii_continuous.common import atomic_json, binding, read_json


def profile(identity, family, **knobs):
    return {'id': identity, 'horizon_seconds': 300, 'model': {
        'id': family, 'budget': 6., 'spread_max': .025, 'profit_target_fraction': .4,
        research.retained.FRICTION_CAP: .15, 'history_seconds': 10, 'agreement_seconds': 5,
        **knobs,
    }}


class ExperimentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.input_path, self.baseline_path, self.spec_path, self.plan_path = (
            self.root / name for name in ('INPUTS.json', 'BASELINE.json', 'WAVE.json', 'PLAN.json')
        )
        self.profiles = [
            profile('5m-P1', 'P1'),
            profile('5m-P2', 'P2', entry_elapsed_min=30, entry_elapsed_max=90),
            profile('5m-P3', 'P3'),
            profile('5m-P4', 'P4'),
            profile('5m-P5__confluence', 'P5', budget=10., profit_target_fraction=.8, high_confidence_budget=15.,
                    entry_remaining_min=15, entry_remaining_max=120, entry_elapsed_min=180),
            *[{'id': f'CONTROL-{i}', 'horizon_seconds': 60, 'model': {'id': 'C'}} for i in range(55)],
        ]
        atomic_json(self.input_path, {'profiles': self.profiles, 'sources': {}})
        ids = [row['id'] for row in self.profiles]
        count = len(ids)
        self.baseline = {'schema': 'OVERNIGHT_STUDY_REPLAY_RESULT_V1', 'inputs': binding(self.input_path),
                         'source_profile_count': count, 'source_profiles_with_financial_parity': count,
                         'source_profiles_with_recorded_decision_parity': count,
                         'parity': {identity: {'pass': True} for identity in ids},
                         'recorded_decision_parity': {identity: {'pass': True} for identity in ids},
                         'profiles': {identity: {'unresolved_positions': 0, 'entries': 1,
                                     'cash_exits': 1, 'settlements': 0, 'realized_pnl': 2.} for identity in ids}}
        atomic_json(self.baseline_path, self.baseline)
        self.experiment = {'schema': research.P2_P5_SCHEMA, 'id': 'test-wave', 'protocol': {},
                           'models': [
                               {'family': 'P2', 'parent_id': '5m-P2', 'arms': [
                                   {'id': 'seed', 'overrides': {'spread_max': .02}},
                               ]},
                               {'family': 'P5', 'parent_id': '5m-P5__confluence', 'arms': [
                                   {'id': 'seed', 'overrides': {'profit_target_fraction': 1.}},
                               ]},
                           ]}
        atomic_json(self.spec_path, self.experiment)

    def prepare(self):
        research.prepare(self.baseline_path, self.plan_path, experiment=self.spec_path)
        return read_json(self.plan_path)

    def test_prepare_binds_only_the_two_exact_parent_controls(self):
        before = deepcopy(self.profiles)
        plan = self.prepare()
        self.assertEqual(plan['scope'], research.P2_P5_SCOPE)
        self.assertEqual(plan['experiment'], binding(self.spec_path))
        self.assertEqual(plan['baseline'], binding(self.baseline_path))
        self.assertEqual([row['id'] for row in plan['selected']], ['5m-P2', '5m-P5__confluence'])
        self.assertEqual([row['id'] for row in plan['variants']], ['5m-P2__p2p5_seed', '5m-P5__confluence__p2p5_seed'])
        self.assertEqual(plan['variants'][1]['before'], {'profit_target_fraction': .8})
        self.assertEqual(plan['variants'][1]['after'], {'profit_target_fraction': 1.})
        self.assertEqual(self.profiles, before)
        self.assertNotIn('cohort_reference', plan)

    def test_legacy_prepare_preserves_five_families_and_fifty_variants(self):
        cohort, wallets = self.root / 'COHORT.json', self.root / 'WALLETS.json'
        atomic_json(cohort, {})
        atomic_json(wallets, {})
        research.prepare(self.baseline_path, self.plan_path, cohort, wallets)
        plan = read_json(self.plan_path)
        self.assertEqual(plan['scope'], 'RETAINED_RESEARCH_ONLY')
        self.assertEqual(len(plan['selected']), 5)
        self.assertEqual(len(plan['variants']), 50)
        self.assertEqual(plan['cohort_reference'], binding(cohort))
        self.assertEqual(plan['wallet_registry'], binding(wallets))
        self.assertNotIn('experiment', plan)

    def test_rejects_a_different_p5_variant_as_the_parent(self):
        self.experiment['models'][1]['parent_id'] = '5m-P5'
        with self.assertRaisesRegex(ValueError, 'FROZEN_P2_P5_PARENT_REQUIRED:P5'):
            research.experiment_scope(self.experiment, self.baseline, self.profiles)

    def test_rejects_invalid_noop_or_ignored_model_changes(self):
        cases = (
            (0, 'spread_max', .025, 'P2_P5_NOOP_OVERRIDE'),
            (0, 'budget', True, 'P2_P5_PARAMETER_BOUND'),
            (0, 'budget', float('nan'), 'P2_P5_PARAMETER_BOUND'),
            (0, 'budget', 26, 'P2_P5_PARAMETER_BOUND'),
            (0, 'agreement_seconds', 4.5, 'P2_P5_PARAMETER_BOUND'),
            (0, 'agreement_seconds', 11, 'P2_P5_INCONSISTENT_PARAMETERS'),
            (0, 'entry_elapsed_min', 91, 'P2_P5_INCONSISTENT_PARAMETERS'),
            (1, 'entry_elapsed_min', 140, 'UNSUPPORTED_P2_P5_OVERRIDE'),
            (1, 'high_confidence_budget', 8, 'P2_P5_INCONSISTENT_PARAMETERS'),
        )
        for model, field, value, reason in cases:
            with self.subTest(field=field, value=value):
                spec = deepcopy(self.experiment)
                spec['models'][model]['arms'][0]['overrides'] = {field: value}
                with self.assertRaisesRegex(ValueError, reason):
                    research.experiment_scope(spec, self.baseline, self.profiles)

    def test_rejects_duplicate_names_and_duplicate_effective_arms(self):
        for arm in ({'id': 'seed', 'overrides': {'spread_max': .015}},
                    {'id': 'other', 'overrides': {'spread_max': .02}}):
            with self.subTest(arm=arm):
                spec = deepcopy(self.experiment)
                spec['models'][0]['arms'].append(arm)
                with self.assertRaisesRegex(ValueError, 'DUPLICATE_P2_P5_ARM'):
                    research.experiment_scope(spec, self.baseline, self.profiles)

    def test_roster_capacity_includes_both_baseline_controls(self):
        for field in ('source_profile_count', 'source_profiles_with_financial_parity',
                      'source_profiles_with_recorded_decision_parity'):
            self.baseline[field] = 4
        self.experiment['models'][0]['arms'].append({'id': 'other', 'overrides': {'spread_max': .015}})
        with self.assertRaisesRegex(ValueError, 'RESEARCH_EXCEEDS_MEASURED_ROSTER'):
            research.experiment_scope(self.experiment, self.baseline, self.profiles[1:5])

    def test_changed_experiment_bytes_block_replay_before_opening_tape(self):
        self.prepare()
        with self.spec_path.open('ab') as stream:
            stream.write(b'\n')
        with patch.object(research.retained, 'sealed_input') as seal:
            with self.assertRaisesRegex(ValueError, 'QUALIFICATION_EVIDENCE_CHANGED'):
                research.run(self.plan_path, self.root / 'result', self.root / 'STATUS.json', 1)
        seal.assert_not_called()

    def test_changed_plan_arm_inventory_blocks_replay_before_opening_tape(self):
        plan = self.prepare()
        plan['variants'][1]['after']['profit_target_fraction'] = 1.1
        atomic_json(self.plan_path, plan)
        with patch.object(research.retained, 'sealed_input') as seal:
            with self.assertRaisesRegex(ValueError, 'RESEARCH_VARIANTS_CHANGED'):
                research.run(self.plan_path, self.root / 'result', self.root / 'STATUS.json', 1)
        seal.assert_not_called()

    def test_direct_run_uses_the_offline_boundary(self):
        paths = {'candidate_manifest': {'path': str(self.root / 'candidate' / 'SOURCE_MANIFEST.json')},
                 'receipt': {'path': str(self.root / 'study' / 'capture' / 'receipt.json')}}
        atomic_json(self.input_path, {'profiles': self.profiles, 'sources': paths})
        self.baseline['inputs'] = binding(self.input_path)
        atomic_json(self.baseline_path, self.baseline)
        self.prepare()

        def network_attempt(*args):
            sys.audit('socket.connect', None, None)

        with patch.object(research.retained, 'sealed_input', side_effect=network_attempt):
            with self.assertRaisesRegex(PermissionError, 'NETWORK_AND_DESCENDANTS_DENIED'):
                research.run(self.plan_path, self.root / 'result', self.root / 'STATUS.json', 1)


if __name__ == '__main__':
    unittest.main()
