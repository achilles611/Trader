"""Tests for fixed schedules, red-marker evidence, and longer phase gates."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.lane_ii_continuous.capture_checkpoint import CheckpointWriter
from scripts.lane_ii_continuous.common import atomic_json, binding, utc
from scripts.lane_ii_continuous.long_gate import assess_window, verified_hours
from scripts.lane_ii_continuous.long_qualification import LongRun, continuation_start, fixed_plan
from scripts.lane_ii_continuous.long_status import dashboard, due_slot, health
from tests.test_lane_ii_continuous_capture_checkpoint import Fixture, START


class FixedLongWindowTests(unittest.TestCase):
    def test_verified_crash_predecessor_reaches_owned_start_gate_with_same_cutoff(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = Fixture(root)
            try:
                CheckpointWriter(fixture.study, interval=900).boundary(now=START + 900)
                supervisor = LongRun.__new__(LongRun)
                supervisor.root = root
                supervisor.state_path = root / 'STATE.json'
                plan = {'phase': 'eight-hour', 'start_epoch': START, 'cutoff_epoch': START + 3600,
                        'cutoff_utc': utc(START + 3600)}
                plan_path = root / 'eight-hour' / 'PLAN.json'
                atomic_json(plan_path, plan)
                supervisor.data = {'phase': 'eight-hour', 'plans': {'eight-hour': binding(plan_path)},
                    'attempts': {'eight-hour': [{'path': str(fixture.run), 'segment_id': 'attempt-1',
                                                'predecessor': {'sha256': 'previous'}}]}}
                supervisor.config = {'paper_risk_manifest': fixture.anchor, 'campaign_id': 'fixture', 'limits': {},
                    'source_sha256': 'driver', 'interpreter': 'fixture-python', 'candidate': {'candidate_dir': str(root),
                    'source_manifest_sha256': fixture.study.manifest, 'startup_release': 'fixture\n'}}
                supervisor.clock, supervisor.samples = {'boot_id': 1}, []
                with patch('scripts.lane_ii_continuous.common.same_process', return_value=False):
                    predecessor = supervisor.restart_predecessor()
                self.assertEqual(supervisor.restart_predecessor(), predecessor)
                with (patch.object(supervisor, 'now', return_value=START + 905),
                      patch('scripts.lane_ii_continuous.long_qualification.admission', return_value={'admitted': True, 'reasons': []}),
                      patch('scripts.lane_ii_continuous.long_qualification.build_forward_command', return_value=['python']) as command,
                      patch('scripts.lane_ii_continuous.long_qualification.OwnedChild') as child):
                    supervisor.start_collector(predecessor, START + 1200)
                self.assertEqual(command.call_args.args[2:], (utc(START + 1200), plan['cutoff_utc']))
                environment = child.call_args.args[2]
                self.assertEqual(environment['LANE_II_PAPER_PREDECESSOR_RECEIPT_SHA256'], 'CRASH:' + predecessor['sha256'])
                self.assertEqual(environment['LANE_II_PAPER_RISK_MANIFEST_SHA256'], fixture.anchor['sha256'])
                self.assertEqual(environment['LANE_II_CAPTURE_CHECKPOINT_ORIGIN_EPOCH'], str(START))
            finally:
                fixture.close()

    def test_recovery_keeps_original_cutoff_and_declines_short_tail(self):
        plan = fixed_plan('eight-hour', 1_800_000_037, 180, {'sha256': 'previous'})
        self.assertEqual(plan['start_epoch'] % 900, 0)
        self.assertEqual(plan['cutoff_epoch'] - plan['start_epoch'], 28800)
        self.assertEqual(continuation_start(plan, plan['start_epoch'] - 875, 180), plan['start_epoch'])
        restart = continuation_start(plan, plan['start_epoch'] + 923, 180)
        self.assertGreaterEqual(restart, plan['start_epoch'] + 1103)
        self.assertEqual(restart % 300, 0)
        self.assertIsNone(continuation_start(plan, plan['cutoff_epoch'] - 301, 180))
        self.assertEqual(plan['cutoff_epoch'] - plan['start_epoch'], 28800)

    def test_offset_and_crash_marker_survive_refresh(self):
        plan = fixed_plan('eight-hour', 1_800_000_037, 180, {})
        slot = plan['start_epoch'] + 900
        self.assertEqual(due_slot(plan, slot + 2), slot)
        self.assertIsNone(due_slot(plan, slot + 59, 60))
        self.assertEqual(due_slot(plan, slot + 60, 60), slot)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crash = {'utc': utc(slot + 9), 'reason': 'COLLECTOR_EXIT', 'component': 'collector', 'count': 1}
            status = {'owner': {'pid': 5}, 'collector': {'owner': {'pid': 7}}, 'plan': plan,
                      'as_of_utc': utc(slot), 'state': 'COLLECTING', 'profiles': [], 'crashes': [], 'crash_count': 0}
            atomic_json(root / 'STATUS.json', status)
            with patch('scripts.lane_ii_continuous.long_status.same_process', return_value=True):
                self.assertEqual(health(root, 'host', epoch=slot + 5)['scheduled_epoch'], slot)
                self.assertEqual(health(root, 'codex', epoch=slot + 6)['scheduled_epoch'], slot)
            dashboard(root, epoch=slot + 60)
            atomic_json(root / 'STATUS.json', {**status, 'state': 'RECOVERING', 'crashes': [crash], 'crash_count': 1})
            value = dashboard(root, epoch=slot + 61, immediate=True)
            self.assertEqual(value['crashes'], [crash])
            self.assertEqual(value['history'][0]['epoch'], slot)
            self.assertEqual(value['plan']['cutoff_epoch'], plan['cutoff_epoch'])

    def test_missing_hour_or_actual_collection_interruption_blocks_promotion(self):
        plan = fixed_plan('eight-hour', 1_800_000_037, 180, {})
        start, cutoff = plan['start_epoch'], plan['cutoff_epoch']
        epochs = list(range(start + 3600, cutoff + 1, 3600))
        windows = list(range(start, cutoff, 300))
        terminal = {'state': 'COMPLETE', 'reason': 'DEADLINE', 'paper_start_utc': utc(start), 'cutoff_utc': utc(cutoff),
                    'actual_end_utc': utc(cutoff + .2), 'events_sha256': 'same', 'snapshot_publish_gaps': 0,
                    'real_orders': 0, 'model_api_calls': 0, 'authenticated_account_calls': 0}
        risk = {'flat': True}
        replay = {'source_events_sha256': 'same', 'risk_parity': {'flat': True, 'immutable_closure': risk},
                  'coverage': {'distinct_paper_scheduled_tick_seconds': 28800, 'expected_scheduled_tick_seconds': 28800,
                               'missing_scheduled_tick_seconds': 0, 'expected_5m_window_starts': windows,
                               'missing_data_treated_as_abstention': False, 'trade_quota_used': False,
                               'shared_fresh_eligible_window_starts': windows, 'per_profile': {'5m-P13': {}, '5m-P2': {}}}}
        with patch('scripts.lane_ii_continuous.long_gate.prospective_replay_pass', return_value=True):
            self.assertEqual(assess_window(plan, terminal, replay, risk, epochs, attempts=1, crashes=0)['status'], 'QUALIFIED')
            missing = assess_window(plan, terminal, replay, risk, epochs[:-1], attempts=1, crashes=0)
            self.assertIn('MISSING_HOURLY_INCREMENTAL_REPLAY_OR_ACCOUNTING', missing['holds'])
            restarted = assess_window(plan, terminal, replay, risk, epochs, attempts=2, crashes=1)
            self.assertIn('DISCONTINUOUS_ATTEMPT_HISTORY', restarted['holds'])

    def test_hour_must_bind_four_contiguous_accounting_receipts(self):
        plan = fixed_plan('eight-hour', START - 1, 0, {})
        start, hour_epoch = plan['start_epoch'], plan['start_epoch'] + 3600
        accounts = {'5m-P13': {'virtual_cash': 2998}, '5m-P2': {'virtual_cash': 3000}}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ranges, prior = [], None
            for index, epoch in enumerate(range(start + 900, hour_epoch + 1, 900), 1):
                path = root / 'ranges' / f'{index:06}.json'
                atomic_json(path, {'schema': 'LANE_II_INCREMENTAL_REPLAY_RANGE_V1', 'semantic_prefix_pass': True,
                    'accounting': accounts, 'coverage': {'planned_through_epoch': epoch}, 'previous_receipt_sha256': prior})
                ranges.append(binding(path))
                prior = ranges[-1]['sha256']
            path = root / 'hours' / f'{hour_epoch}.json'
            atomic_json(path, {'schema': 'LANE_II_HOURLY_REPLAY_HOUR_V1', 'epoch': hour_epoch, 'quarters': 4,
                'semantic_prefix_pass': True, 'accounts_nonadditive': True, 'accounting': accounts, 'range_receipts': ranges})
            replay = {'hourly_epochs': [hour_epoch], 'hourly_bindings': [{'epoch': hour_epoch, 'binding': binding(path)}]}
            self.assertEqual(verified_hours(plan, replay, root), [hour_epoch])
            atomic_json(ranges[-1]['path'], {'tampered': True})
            with self.assertRaisesRegex(ValueError, 'QUALIFICATION_EVIDENCE_CHANGED'):
                verified_hours(plan, replay, root)


if __name__ == '__main__':
    unittest.main()
