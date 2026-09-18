"""TEST ONLY: bounded risk lookup independent of retained market-row volume."""
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import sqlite3
from src.l3g_paper.ledger import PaperLedger


class RiskLookupCapacityTests(unittest.TestCase):
    def test_actual_risk_boundary_lookup_does_not_scan_market_history(self):
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory)/'TEST-ONLY-paper.sqlite3', persist_high_frequency_records=True)
            try:
                ledger.append('RISK_EVENT_AUTHORITY_LOCKOUT', {
                    'locked_out': True, 'lockout_reason': 'TEST_ONLY_LOCK_RETAINED',
                    'lockout_trade_date': None, 'effective_trade_date': None,
                    'effect': 'ENTRY_AUTHORITY_LOCKED'}, identity='TEST-ONLY-risk')
                expected = ledger.risk_continuity_boundary()
                for index in range(10000):
                    ledger.append_deferred('OBSERVATION', {'test_only': True, 'value': index},
                                           identity=f'TEST-ONLY-market-{index}')
                    if index % 100 == 99:
                        ledger.flush_deferred(timeout_seconds=30)
                ledger.flush_deferred(timeout_seconds=30)
                calls = 0
                def budget():
                    nonlocal calls
                    calls += 1
                    return int(calls > 500)  # deterministic SQLite VM budget, not wall time
                with ledger._lock:
                    # Recreate the exact pre-repair plan on this TEST ONLY
                    # ledger, then prove the same query within the same budget.
                    ledger._connection.execute('DROP INDEX lane_iii_paper_audit_kind_sequence')
                    ledger._connection.set_progress_handler(budget, 100)
                    try:
                        with self.assertRaisesRegex(sqlite3.OperationalError, 'interrupted'):
                            ledger._risk_continuity_boundary_locked()
                    finally:
                        ledger._connection.set_progress_handler(None, 0)
                    ledger._connection.execute('CREATE INDEX lane_iii_paper_audit_kind_sequence '
                                               'ON lane_iii_paper_audit(kind,ledger_sequence,record_hash)')
                    calls = 0
                    ledger._connection.set_progress_handler(budget, 100)
                    try:
                        actual = ledger._risk_continuity_boundary_locked()
                    finally:
                        ledger._connection.set_progress_handler(None, 0)
                self.assertEqual(actual, expected)
                records, boundary = ledger.risk_continuity_evidence()
                self.assertEqual(boundary, expected)
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]['payload']['lockout_reason'], 'TEST_ONLY_LOCK_RETAINED')
            finally:
                ledger.close()


if __name__ == '__main__':
    unittest.main()
