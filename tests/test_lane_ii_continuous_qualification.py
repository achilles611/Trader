from pathlib import Path
import sys
import tempfile
import unittest

from scripts.lane_ii_continuous.common import atomic_json, binding, read_json, utc
from scripts.lane_ii_continuous.prospective_supervisor import build_supervisor_result
from scripts.lane_ii_continuous.qualification import (
    POLICY, RELEASE, REVIEW, TESTS, identities, launch_reasons, phase_payload, register_short_release, write_phase_assessment,
)
from scripts.lane_ii_continuous.queue import Registry


class QualificationReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        manifest = self.root / 'risk.json'
        atomic_json(manifest, {'campaign_id': 'campaign-test'})
        self.config = {'root': str(self.root), 'paper_only': True, 'source_sha256': 'source',
                       'campaign_id': 'campaign-test', 'qualification_policy': POLICY,
                       'candidate': {'source_manifest_sha256': 'candidate'}, 'paper_risk_manifest': binding(manifest)}
        self.registry = Registry(self.root / 'queue.sqlite')
        specs = {
            'review': {'schema': REVIEW, 'identities': identities(self.config), 'decision': 'ACCEPT_SHORT_PAPER_ONLY', 'holds': []},
            'tests': {'schema': TESTS, 'identities': identities(self.config), 'exit_code': 0, 'tests_run': 100,
                      'prospective_capture_started': False},
            'historical_replay': {'assessment': {'all_requested_parity_passed': True, 'study_parity': 'PASS',
                'cohort_parity': 'PASS', 'replayed_profile_count': 60}},
        }
        evidence = {}
        for name, value in specs.items():
            path = self.root / (name + '.json')
            atomic_json(path, value)
            evidence[name] = binding(path)
        self.release = self.root / 'release.json'
        atomic_json(self.release, {'schema': RELEASE, 'identities': identities(self.config), 'evidence': evidence})

    def job(self, phase):
        return {'id': phase, 'experiment_id': 'campaign-test', 'segment_id': phase + '-001',
                'type': 'forward', 'payload': phase_payload(self.config, phase)}

    def test_preflight_admits_exact_short_and_does_not_claim_qualification(self):
        self.assertEqual(launch_reasons(self.config, self.registry, self.job('short')), ['BOUND_SHORT_RELEASE_REQUIRED'])
        gate = register_short_release(self.config, self.registry, self.release)
        self.assertFalse(gate['short_pipeline_qualified'])
        self.assertFalse(gate['profitability_qualified'])
        self.assertEqual(launch_reasons(self.config, self.registry, self.job('short')), [])
        self.assertIn('COMPLETE_SHORT_QUALIFICATION_REQUIRED', launch_reasons(self.config, self.registry, self.job('eight-hour')))
        self.assertIn('COMPLETE_EIGHT_HOUR_QUALIFICATION_REQUIRED', launch_reasons(self.config, self.registry, self.job('twenty-four-hour')))

    def test_tampered_review_or_modified_duration_cannot_launch(self):
        register_short_release(self.config, self.registry, self.release)
        wrong = self.job('short')
        wrong['payload']['seconds'] = 300
        self.assertEqual(launch_reasons(self.config, self.registry, wrong), ['UNFROZEN_PHASE_PAYLOAD'])
        atomic_json(self.root / 'review.json', {'decision': 'HOLD'})
        self.assertTrue(launch_reasons(self.config, self.registry, self.job('short'))[0].startswith('QUALIFICATION_BINDING:'))

    def test_predecessor_pass_is_necessary_and_hourly_gate_still_holds(self):
        register_short_release(self.config, self.registry, self.release)
        ref = binding(self.root / 'risk.json')
        write_phase_assessment(self.config, self.registry, 'short',
                               {'status': 'QUALIFIED', 'terminal_receipt': ref, 'risk_continuity': ref})
        job = self.job('eight-hour')
        job['payload'] = phase_payload(self.config, 'eight-hour', predecessor=ref)
        self.assertEqual(launch_reasons(self.config, self.registry, job), ['HOURLY_CAUSAL_REPLAY_NOT_QUALIFIED'])
        atomic_json(self.root / 'risk.json', {'changed': True})
        self.assertTrue(launch_reasons(self.config, self.registry, job)[0].startswith('QUALIFICATION_BINDING:'))


class PrewarmReplacementTests(unittest.TestCase):
    def test_replacement_rejects_an_elapsed_paper_start_or_account_change(self):
        from scripts.lane_ii_continuous.prewarm_recovery import _zero_paper
        start = 1700000000
        receipt = {'state': 'FAILED', 'reason': 'DETERMINISTIC_TRANSPORT_FAILURE', 'paper_start_utc': utc(start),
                   'actual_end_utc': utc(start - 1), 'cutoff_utc': utc(start + 900), 'ticks': 0, 'missed_ticks': 0,
                   'real_orders': 0, 'model_api_calls': 0, 'authenticated_account_calls': 0,
                   'profiles': [{'id': identity, 'virtual_initial_cash': 3000., 'virtual_cash': 3000., 'entries': 0,
                                 'cash_exits': 0, 'settlements': 0, 'unresolved_positions': 0, 'realized_pnl': 0.,
                                 'modeled_fees': 0., 'unresolved_basis': 0.} for identity in ('5m-P13', '5m-P2')]}
        _zero_paper(receipt)
        with self.assertRaisesRegex(ValueError, 'ONLY_ZERO_TICK_PREWARM'):
            _zero_paper({**receipt, 'actual_end_utc': utc(start)})
        with self.assertRaisesRegex(ValueError, 'ONLY_ZERO_TICK_PREWARM'):
            _zero_paper({**receipt, 'ticks': 1})
        receipt['profiles'][0]['virtual_cash'] -= .01
        with self.assertRaisesRegex(ValueError, 'PREWARM_ACCOUNTS_MUST_BE_UNCHANGED'):
            _zero_paper(receipt)


class ProspectiveSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.candidate = self.root / 'candidate'
        self.run = self.root / 'run'
        self.capture = self.run / 'capture'
        self.candidate.mkdir()
        self.capture.mkdir(parents=True)
        study = self.candidate / 'study.py'
        study.write_text("def cold_audit(root):\n    with (root / 'events.jsonl').open('rb') as stream:\n        n = sum(1 for row in stream)\n"
                         "    return {'status':'PASS_RETAINED_NEXT12_CAPTURE_INTEGRITY_ONLY','events_checked':n,'problems':[]}\n",
                         encoding='utf-8')
        self.manifest = self.candidate / 'SOURCE_MANIFEST.json'
        atomic_json(self.manifest, {'files': {'study.py': binding(study)['sha256']}})
        self.sha = binding(self.manifest)['sha256']
        start, cutoff = utc(1700000000), utc(1700000900)
        paths = {'receipt': self.capture / 'receipt.json', 'preregistration': self.capture / 'preregistration.json',
                 'collection_closed': self.capture / 'COLLECTION_CLOSED.json', 'source_manifest': self.manifest}
        atomic_json(paths['preregistration'], {'paper_start_utc': start, 'cutoff_utc': cutoff, 'source_manifest_sha256': self.sha})
        atomic_json(paths['receipt'], {'state': 'COMPLETE', 'reason': 'DEADLINE', 'paper_start_utc': start,
            'cutoff_utc': cutoff, 'pid': 25, 'events': 2, 'real_orders': 0, 'authenticated_account_calls': 0,
            'model_api_calls': 0, 'source_manifest_sha256': self.sha,
            'preregistration_sha256': binding(paths['preregistration'])['sha256']})
        atomic_json(paths['collection_closed'], {'schema': 'BTC_NEXT12_COLLECTION_CLOSED_V1',
            'source_manifest_sha256': self.sha, 'worker_pid': 25, 'fixed_cutoff_utc': cutoff,
            'state_at_collection_close': 'COMPLETE', 'collection_closed': True, 'network_closed': True,
            'producers_exited': True, 'snapshot_publisher_closed': True, 'finalization_only_no_more_http': True})
        owned = {'pid': 25, 'creation_epoch': 1699999800., 'boot_epoch': 1699900000.,
                 'executable_sha256': binding(sys.executable)['sha256'], 'source_manifest_sha256': self.sha}
        atomic_json(self.run / 'operator.json', {**owned, 'owned_job_before_startup_gate': True,
            'command': [sys.executable, '-I', '-S', '-B', str(study), 'worker', '--run-dir', str(self.capture),
                        '--paper-start-utc', start, '--cutoff-utc', cutoff, '--startup-gate']})
        atomic_json(self.run / 'exit.json', {**owned, 'exit_code': 0, 'forced_closure': False,
            'retained_handle_exit_observed': True, 'owned_job_closed': True, 'owned_children_at_parent_exit': []})
        (self.capture / 'events.jsonl').write_bytes(b'{}\n{}\n')
        self.audit = self.root / 'raw-audit.json'
        atomic_json(self.audit, {'schema': 'LANE_II_READONLY_CAPTURE_AUDIT_V1',
            'status': 'PASS_RETAINED_CAPTURE_INTEGRITY_ONLY', 'problem_counts': {}, 'events_checked': 2,
            'capture_dir': str(self.capture), 'source_dir': str(self.candidate),
            'input_bindings': [binding(path) for path in paths.values()]})
        self.result = self.root / 'PAPER_RUN_RESULT.json'

    def bridge(self, owner='ABSENT'):
        return build_supervisor_result(self.candidate, self.run, self.audit, self.result,
            expected_manifest_sha256=self.sha, process_probe=lambda operator: {'state': owner})

    def test_frozen_cold_audit_is_invoked_and_result_is_external(self):
        value = self.bridge()
        self.assertEqual(value['capture_audit']['events_checked'], 2)
        self.assertEqual(value['bindings']['worker_exit'], binding(self.run / 'exit.json'))
        self.assertFalse((self.run / 'PAPER_RUN_RESULT.json').exists())
        self.assertEqual(read_json(self.result), value)

    def test_pid_reuse_without_matching_creation_identity_refused(self):
        path = self.run / 'exit.json'
        changed = read_json(path)
        changed['creation_epoch'] += 1
        atomic_json(path, changed)
        with self.assertRaisesRegex(ValueError, 'GENUINE_OWNED_EXIT'):
            self.bridge(owner='PID_REUSED')
        self.assertFalse(self.result.exists())

    def test_no_bridge_if_actual_audit_disagrees_with_claimed_count(self):
        (self.capture / 'events.jsonl').write_bytes(b'{}\n')
        with self.assertRaisesRegex(ValueError, 'FROZEN_COLD_AUDIT_FAILED'):
            self.bridge()
        self.assertFalse(self.result.exists())


if __name__ == '__main__':
    unittest.main()
