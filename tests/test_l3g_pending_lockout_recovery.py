"""TEST ONLY: original incident identity and crash-safe evidence recovery."""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
import hashlib
import json

from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.pending_lockout import RECOVERY_SCHEMA, record_pending, resolve_pending
from src.l3g_paper.verification import run_local_verification


class PendingLockoutRecoveryTests(unittest.TestCase):
    def test_runtime_recovery_requires_safe_binding_and_retains_capacity_lock(self):
        from dataclasses import replace
        from types import SimpleNamespace
        from src.l3g_paper.runtime import PaperRuntimeState
        with TemporaryDirectory() as directory:
            ledger, marker, bundle = self.fixture(Path(directory))
            try:
                runtime = LaneIIIPaperRuntime(ledger)
                runtime._state = PaperRuntimeState.LOCKED_OUT  # TEST ONLY started-runtime fixture
                safe = replace(runtime._snapshot, position_snapshot_complete=True,
                               order_snapshot_complete=True, reconciliation_current=True)
                runtime._snapshot = safe
                runtime._transport = SimpleNamespace(status=lambda: SimpleNamespace(
                    authenticated_client=True, reconciled=True, addon_provenance_valid=True))
                before = runtime.risk_continuity_snapshot()
                proof = self.proof(ledger)
                with patch.object(runtime, 'operational_paper_readiness', return_value={'ledger': proof}):
                    for changes in ({'working_owned_orders': 1}, {'foreign_activity': True},
                                    {'account_name': 'Live'}, {'instrument': 'MNQ DEC26'},
                                    {'reconciliation_current': False}, {'unresolved_execution': True}):
                        runtime._snapshot = replace(safe, **changes)
                        with self.assertRaisesRegex(ValueError, 'SAFE_RECONCILIATION_REQUIRED'):
                            runtime.recover_pending_lockout_evidence('TEST-ONLY-record', 'RECORD', bundle)
                        self.assertTrue(marker.exists())
                    runtime._snapshot = safe
                    runtime.recover_pending_lockout_evidence('TEST-ONLY-record', 'RECORD', bundle)
                with patch.object(runtime, 'operational_paper_readiness', return_value={'ledger': self.proof(ledger)}):
                    runtime.recover_pending_lockout_evidence('TEST-ONLY-resolve', 'RESOLVE', bundle)
                self.assertIsNone(runtime._risk_continuity_fault)
                self.assertTrue(runtime.risk.status()['locked_out'])
                self.assertEqual(runtime.risk.status()['lockout_reason'], 'LEDGER_CAPACITY_INADEQUATE:DEGRADED')
                self.assertTrue(runtime._entries_paused)
                after = runtime.risk_continuity_snapshot()
                for field in ('trade_dates', 'profile_trade_dates', 'entry_execution_ids', 'exit_execution_ids'):
                    self.assertEqual(before.get(field), after.get(field), field)
                self.proof(ledger)
            finally:
                ledger.close()

    def test_resolution_persistence_failure_keeps_active_pending_marker(self):
        with TemporaryDirectory() as directory:
            ledger, marker, bundle = self.fixture(Path(directory))
            try:
                record_pending(ledger, bundle, self.proof(ledger))
                proof = self.proof(ledger)
                with patch.object(ledger, 'append', side_effect=OSError('TEST ONLY resolution disk failure')):
                    with self.assertRaises(OSError):
                        resolve_pending(ledger, bundle, proof)
                self.assertTrue(marker.exists())
                result = resolve_pending(ledger, bundle, proof)
                self.assertFalse(result['lockout_cleared'])
                self.assertFalse(marker.exists())
                self.proof(ledger)
            finally:
                ledger.close()

    def test_pending_restart_preserves_accounting_and_unrelated_lock(self):
        from tests.test_l3g_profile_switch import RiskContinuityTests
        with TemporaryDirectory() as directory:
            path = Path(directory)/'paper.sqlite3'
            ledger = PaperLedger(path)
            context = RiskContinuityTests.context('2026-09-03T14:00:00Z')
            RiskContinuityTests.append_accounted_lifecycle(ledger, context=context,
                entry_id='TEST-ONLY-entry', exit_id='TEST-ONLY-exit', realized='-25')
            runtime = LaneIIIPaperRuntime(ledger)
            runtime.risk.lock_out('FOREIGN_ORDER_ACTIVITY')
            before = runtime.risk_continuity_snapshot()
            marker = Path(str(path)+'.risk-authority-pending.json')
            raw = b'{"TEST_ONLY":"unresolved evidence must not be overwritten"}'
            marker.write_bytes(raw)
            ledger.close()
            ledger = PaperLedger(path)
            try:
                restored = LaneIIIPaperRuntime(ledger)
                after = restored.risk_continuity_snapshot()
                for field in ('trade_dates', 'profile_trade_dates', 'entry_execution_ids', 'exit_execution_ids'):
                    self.assertEqual(before.get(field), after.get(field), field)
                self.assertEqual(restored.risk.status()['lockout_reason'], 'FOREIGN_ORDER_ACTIVITY')
                self.assertEqual(restored._risk_continuity_fault, 'RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED')
                self.assertEqual(marker.read_bytes(), raw)
            finally:
                ledger.close()

    def fixture(self, root):
        ledger = PaperLedger(root / 'paper.sqlite3', epoch_id='L3G-PAPER-EPOCH-TEST-ONLY-PENDING-RECOVERY')
        ledger.append('RISK_EVENT_AUTHORITY_LOCKOUT', {
            'locked_out': True, 'lockout_reason': 'LEDGER_CAPACITY_INADEQUATE:DEGRADED',
            'lockout_trade_date': None, 'effective_trade_date': None, 'effect': 'ENTRY_AUTHORITY_LOCKED',
        }, identity='test-original-old-incident', occurred_at='2026-09-11T12:00:00Z')
        anchor = ledger.record_by_identity('test-original-old-incident')
        marker = Path(str(ledger.path)+'.risk-authority-pending.json')
        document = {'schema': 'lane-iii-risk-lockout-pending-v1', 'ledger_path': str(ledger.path),
            'kind': 'RISK_EVENT_AUTHORITY_LOCKOUT', 'created_at': '2026-09-14T14:46:53Z',
            'payload': {'locked_out': True, 'lockout_reason': 'LEDGER_CAPACITY_INADEQUATE:DEGRADED',
                'lockout_trade_date': None, 'effective_trade_date': None, 'effect': 'ENTRY_AUTHORITY_LOCKED'}}
        original = root / 'IMMUTABLE-TEST-ONLY-original.json'
        raw = json.dumps(document).encode()
        original.write_bytes(raw)
        marker.write_bytes(raw)
        snapshot = {'captured_at': '2026-09-14T15:36:14Z', 'pong': {'loaded_artifact_hash': 'a'*64},
            'paper': {'risk_continuity_fault': 'RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED',
                'paper_account': 'Sim101', 'account_class': 'LOCAL_SIMULATION', 'market_instrument': 'MNQ SEP26',
                'risk_continuity': {'authority_lockout': {'locked_out': True, 'lockout_reason': document['payload']['lockout_reason']},
                    'source_ledger': {'path': str(ledger.path), 'ledger_identity': ledger.ledger_identity,
                        'ledger_epoch': ledger.ledger_epoch, 'risk_boundary_sequence': anchor['ledger_sequence'],
                        'risk_boundary_hash': anchor['record_hash']}}}}
        snapshot_path = root / 'TEST-ONLY-runtime.json'
        snapshot_path.write_text(json.dumps(snapshot), encoding='utf-8')
        bundle = {'schema': RECOVERY_SCHEMA, 'original_path': str(original),
            'original_sha256': hashlib.sha256(raw).hexdigest(), 'runtime_evidence_path': str(snapshot_path),
            'runtime_evidence_sha256': hashlib.sha256(snapshot_path.read_bytes()).hexdigest(),
            'ledger_path': str(ledger.path), 'ledger_identity': ledger.ledger_identity, 'ledger_epoch': ledger.ledger_epoch}
        return ledger, marker, bundle

    def proof(self, ledger):
        report = run_local_verification(ledger.path, ledger.path.parent / 'verification', requested_mode='full')
        self.assertEqual(report['status'], 'PASS', report.get('errors'))
        return {'ledger_trust_state': 'VERIFIED_TO_ARM_SNAPSHOT_TIP', 'unverified_tail_rows': 0,
            'arm_snapshot_tip': report['verified_through_sequence'], 'verified_through_sequence': report['verified_through_sequence'],
            'arm_snapshot_tip_hash': report['tip_hash']}

    def test_two_phase_recovery_and_duplicate_retry_keep_original_lock(self):
        with TemporaryDirectory() as directory:
            ledger, marker, bundle = self.fixture(Path(directory))
            try:
                initial = self.proof(ledger)
                recorded = record_pending(ledger, bundle, initial)
                self.assertTrue(marker.exists())
                with self.assertRaisesRegex(ValueError, 'FULL_VERIFICATION_REQUIRED'):
                    resolve_pending(ledger, bundle, initial)
                proof = self.proof(ledger)
                retry = record_pending(ledger, bundle, proof)
                self.assertEqual(recorded, retry)
                resolved = resolve_pending(ledger, bundle, proof)
                again = resolve_pending(ledger, bundle, self.proof(ledger))
                self.assertFalse(resolved['lockout_cleared'])
                self.assertTrue(again['already_resolved'])
                self.assertFalse(marker.exists())
                resolution_row = ledger.record_by_sequence(resolved['resolution_sequence'])['record']
                self.assertEqual(resolution_row['payload']['evidence_record_hash'], recorded['record_hash'])
                self.assertFalse(resolution_row['payload']['lockout_cleared'])
                self.assertEqual(Path(resolved['resolved_path']).read_bytes(), Path(bundle['original_path']).read_bytes())
                recovered = LaneIIIPaperRuntime(ledger)
                self.assertEqual(recovered.risk.status()['lockout_reason'], 'LEDGER_CAPACITY_INADEQUATE:DEGRADED')
                row = ledger.record_by_sequence(recorded['ledger_sequence'])['record']
                self.assertGreater(row['occurred_at'], '2026-09-14T14:46:53Z')
                self.assertEqual(row['payload']['pending_evidence']['association']['original_incident_at'], '2026-09-14T14:46:53Z')
                self.proof(ledger)
            finally:
                ledger.close()

    def test_missing_tampered_or_conflicting_evidence_never_resolves(self):
        for fault in ('missing', 'tampered_copy', 'tampered_active', 'wrong_epoch', 'wrong_snapshot', 'extra_pending'):
            with self.subTest(fault=fault), TemporaryDirectory() as directory:
                ledger, marker, bundle = self.fixture(Path(directory))
                try:
                    proof = self.proof(ledger)
                    if fault == 'missing':
                        marker.unlink()
                    elif fault == 'tampered_copy':
                        Path(bundle['original_path']).write_text('{}')
                    elif fault == 'tampered_active':
                        marker.write_text('{}')
                    elif fault == 'wrong_epoch':
                        bundle['ledger_epoch'] = 'WRONG'
                    elif fault == 'wrong_snapshot':
                        bundle['runtime_evidence_sha256'] = '0'*64
                    else:
                        Path(str(marker)+'.second').write_text('{}')
                    with self.assertRaises((ValueError, OSError)):
                        record_pending(ledger, bundle, proof)
                    self.assertEqual(len(ledger.recent_kind_records(('RISK_EVENT_AUTHORITY_LOCKOUT',), 10)), 1)
                finally:
                    ledger.close()

    def test_append_failure_and_crash_before_resolution_are_retryable(self):
        with TemporaryDirectory() as directory:
            ledger, marker, bundle = self.fixture(Path(directory))
            try:
                proof = self.proof(ledger)
                with patch.object(ledger, 'append', side_effect=OSError('TEST ONLY disk failure')):
                    with self.assertRaises(OSError):
                        record_pending(ledger, bundle, proof)
                self.assertTrue(marker.exists())
                recorded = record_pending(ledger, bundle, proof)
                proof = self.proof(ledger)
                with patch('src.l3g_paper.pending_lockout.os.rename', side_effect=OSError('TEST ONLY interrupted resolution')):
                    with self.assertRaises(OSError):
                        resolve_pending(ledger, bundle, proof)
                self.assertTrue(marker.exists())
                ledger.close()
                ledger = PaperLedger(Path(directory)/'paper.sqlite3', epoch_id='L3G-PAPER-EPOCH-TEST-ONLY-PENDING-RECOVERY')
                restored = LaneIIIPaperRuntime(ledger)
                self.assertEqual(restored._risk_continuity_fault, 'RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED')
                self.assertTrue(marker.exists())
                self.assertEqual(record_pending(ledger, bundle, self.proof(ledger))['ledger_sequence'], recorded['ledger_sequence'])
                resolve_pending(ledger, bundle, self.proof(ledger))
                self.assertFalse(marker.exists())
            finally:
                ledger.close()

    def test_distinct_same_reason_incidents_do_not_reuse_ledger_identity(self):
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / 'paper.sqlite3')
            try:
                runtime = LaneIIIPaperRuntime(ledger)
                for session in ('first-session', 'second-session'):
                    with patch.object(runtime, '_execution_session_id', return_value=session):
                        runtime._record_authority_lockout(True, 'LEDGER_CAPACITY_INADEQUATE:DEGRADED', None)
                self.assertIsNone(runtime._risk_continuity_fault)
                rows = ledger.recent_kind_records(('RISK_EVENT_AUTHORITY_LOCKOUT',), 10)
                self.assertEqual(len(rows), 2)
                self.assertFalse(Path(str(ledger.path) + '.risk-authority-pending.json').exists())
            finally:
                ledger.close()
