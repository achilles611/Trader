from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import unittest

from src.l3g_paper.ledger import PaperLedger, adopt_legacy_epoch
from src.l3g_paper.verification import run_local_verification


class PaperLedgerTests(unittest.TestCase):
    def test_existing_malformed_image_fails_read_only_before_sidecars_or_schema(self) -> None:
        with TemporaryDirectory() as folder:
            path = Path(folder) / "malformed.sqlite3"
            original = b"sealed malformed epoch evidence"
            path.write_bytes(original)
            with self.assertRaisesRegex(RuntimeError, "existing ledger quick_check failed") as raised:
                PaperLedger(path)
            self.assertIn(str(path.resolve()), str(raised.exception))
            self.assertEqual(path.read_bytes(), original)
            self.assertFalse(Path(str(path.resolve()) + "-wal").exists())
            self.assertFalse(Path(str(path.resolve()) + "-shm").exists())

    def test_health_status_exposes_cached_integrity_and_epoch_metadata(self) -> None:
        with TemporaryDirectory() as folder:
            path = Path(folder) / "Epoch-002" / "paper.sqlite3"
            with PaperLedger(path) as ledger:
                initial = ledger.health_status()
                self.assertEqual(initial["epoch_id"], "L3G-PAPER-EPOCH-002")
                self.assertEqual(initial["quick_check_state"], "not_run_local_verifier_required")
                self.assertTrue(initial["chain_valid"])
                self.assertEqual(initial["highest_sequence"], 0)
                ledger.append("COMMAND", {"command_id": "l3g-health"}, identity="l3g-health")
                current = ledger.health_status()
                self.assertEqual(current["highest_sequence"], 1)
                self.assertIsNotNone(current["last_record_time"])
                self.assertIsNotNone(current["final_record_hash"])
                self.assertGreater(current["file_size"], 0)
                self.assertGreater(current["free_bytes"], 0)

    def test_high_volume_wal_window_is_bounded_above_default_checkpoint(self) -> None:
        with TemporaryDirectory() as folder, PaperLedger(Path(folder) / "paper.sqlite3") as ledger:
            connection = ledger._connection
            self.assertEqual(connection.execute("PRAGMA wal_autocheckpoint").fetchone()[0], 0)
            self.assertEqual(connection.execute("PRAGMA journal_size_limit").fetchone()[0], 134217728)
            self.assertEqual(connection.execute("PRAGMA temp_store").fetchone()[0], 2)

    def test_legacy_epoch_adoption_requires_full_proof_and_explicit_maintenance_confirmation(self) -> None:
        with TemporaryDirectory() as folder:
            path = Path(folder) / "paper.sqlite3"; audit = Path(folder) / "audit"
            with PaperLedger(path) as ledger:
                ledger.append("COMMAND", {"command_id": "adopt"}, identity="adopt")
            self.assertEqual(run_local_verification(path, audit, requested_mode="full")["status"], "PASS")
            with self.assertRaisesRegex(ValueError, "maintenance-window"):
                adopt_legacy_epoch(path, audit, target_epoch="L3G-PAPER-EPOCH-002", operator_id="operator", maintenance_window_confirmed=False)
            adopted = adopt_legacy_epoch(path, audit, target_epoch="L3G-PAPER-EPOCH-002", operator_id="operator", maintenance_window_confirmed=True)
            self.assertTrue(adopted["adopted"])
            self.assertTrue(Path(str(adopted["receipt_path"])).is_file())
            with PaperLedger(path) as ledger:
                self.assertEqual(ledger.health_status()["epoch_id"], "L3G-PAPER-EPOCH-002")

    def test_deferred_records_commit_in_order_before_operational_record(self) -> None:
        with TemporaryDirectory() as folder, PaperLedger(Path(folder) / "paper.sqlite3") as ledger:
            ledger.append_deferred("EVIDENCE", {"evidence_id": "l3g-pe-deferred"}, identity="l3g-pe-deferred")
            ledger.append_deferred("DECISION", {"paper_decision_id": "l3g-pd-deferred"}, identity="l3g-pd-deferred")
            ledger.append("COMMAND", {"command_id": "l3g-pc-after-flush"}, identity="l3g-pc-after-flush")
            rows = ledger._connection.execute(
                "SELECT identity FROM lane_iii_paper_audit ORDER BY ledger_sequence"
            ).fetchall()
            self.assertEqual([row[0] for row in rows], [
                "l3g-pe-deferred", "l3g-pd-deferred", "l3g-pc-after-flush",
            ])
            self.assertEqual(ledger.counts(), {"COMMAND": 1, "DECISION": 1, "EVIDENCE": 1})
            self.assertEqual(ledger.verify_chain(), (True, None))

    def test_close_flushes_deferred_tail(self) -> None:
        with TemporaryDirectory() as folder:
            path = Path(folder) / "paper.sqlite3"
            ledger = PaperLedger(path)
            for index in range(100):
                ledger.append_deferred(
                    "DECISION",
                    {"paper_decision_id": f"l3g-pd-tail-{index}"},
                    identity=f"l3g-pd-tail-{index}",
                )
            ledger.close()
            connection = sqlite3.connect(path)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM lane_iii_paper_audit").fetchone()[0], 100)
            connection.close()

    def test_all_domain_tables_hash_chain_and_idempotence(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            with PaperLedger(path) as ledger:
                first = ledger.append("DECISION", {"paper_decision_id": "l3g-pd-a"}, identity="l3g-pd-a")
                self.assertEqual(
                    first,
                    ledger.append("DECISION", {"paper_decision_id": "l3g-pd-a"}, identity="l3g-pd-a"),
                )
                with self.assertRaisesRegex(ValueError, "identity conflicts"):
                    ledger.append("DECISION", {"different": True}, identity="l3g-pd-a")
                ledger.append("COMMAND", {"command_id": "l3g-pc-a"}, identity="l3g-pc-a")
                self.assertEqual(ledger.chain_status(), (True, None))
                self.assertEqual(ledger.verify_chain(), (True, None))
            connection = sqlite3.connect(path)
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            connection.close()
            self.assertIn("lane_iii_paper_commands", tables)
            self.assertIn("lane_iii_paper_executions", tables)
            self.assertIn("lane_iii_paper_position_snapshots", tables)

    def test_secrets_are_rejected(self) -> None:
        with TemporaryDirectory() as directory, PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
            for payload in ({"hmac_key": "x"}, {"nested": {"password": "x"}}, {"private_key": "x"}):
                with self.assertRaises(ValueError):
                    ledger.append("INCIDENT", payload)

    def test_high_volume_records_use_normal_but_operational_records_restore_full_sync(self) -> None:
        with TemporaryDirectory() as directory, PaperLedger(Path(directory) / "paper.sqlite3") as ledger:
            ledger.append("DECISION", {"paper_decision_id": "l3g-pd-normal"})
            self.assertEqual(ledger._connection.execute("PRAGMA synchronous").fetchone()[0], 1)
            ledger.append("COMMAND", {"command_id": "l3g-pc-full"})
            self.assertEqual(ledger._connection.execute("PRAGMA synchronous").fetchone()[0], 2)
            ledger.append("EVIDENCE", {"evidence_id": "l3g-pe-normal"})
            self.assertEqual(ledger._connection.execute("PRAGMA synchronous").fetchone()[0], 1)
            ledger.append("INCIDENT", {"reason": "critical"})
            self.assertEqual(ledger._connection.execute("PRAGMA synchronous").fetchone()[0], 2)
            self.assertEqual(ledger.verify_chain(), (True, None))


if __name__ == "__main__":
    unittest.main()
