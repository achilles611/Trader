from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import Mock, patch

from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.verification import (
    LocalLedgerVerificationController,
    LocalLedgerVerifier,
    VerificationFailure,
    VerificationPaths,
    _create_lock,
    run_local_verification,
)
from src.ops_scheduler.tasks import ledger_verification, validate_ledger_verification


class LocalLedgerVerificationTests(unittest.TestCase):
    def make_ledger(self, root: Path, *, rows: int = 3, epoch: bool = False) -> Path:
        path = root / "Epoch-002" / "paper.sqlite3" if epoch else root / "paper.sqlite3"
        with PaperLedger(path) as ledger:
            for index in range(rows):
                ledger.append("DECISION", {"paper_decision_id": f"l3g-verifier-{index}"}, identity=f"l3g-verifier-{index}")
        return path

    def full(self, path: Path, root: Path) -> dict[str, object]:
        return run_local_verification(path, root / "runtime" / "audit", requested_mode="full")

    def test_clean_full_verification_writes_machine_readable_artifacts(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=4, epoch=True)
            report = self.full(path, root)
            audit = VerificationPaths(root / "runtime" / "audit")
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["verification_mode"], "full")
            self.assertEqual(report["rows_scanned"], 4)
            self.assertTrue(report["chain_valid"])
            self.assertTrue(audit.latest.exists())
            self.assertTrue(audit.checkpoint.exists())
            self.assertEqual(json.loads(audit.latest.read_text(encoding="utf-8"))["verification_id"], report["verification_id"])
            self.assertTrue(list(audit.reports.glob("*.json")))

    def test_incremental_scans_only_post_checkpoint_records_and_auto_selects_it(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=3)
            self.assertEqual(self.full(path, root)["status"], "PASS")
            with PaperLedger(path) as ledger:
                ledger.append("DECISION", {"paper_decision_id": "tail-1"}, identity="tail-1")
                ledger.append("DECISION", {"paper_decision_id": "tail-2"}, identity="tail-2")
            report = run_local_verification(path, root / "runtime" / "audit", requested_mode="auto")
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["verification_mode"], "incremental")
            self.assertEqual(report["checkpoint_start_sequence"], 3)
            self.assertEqual(report["rows_scanned"], 2)
            self.assertEqual(report["verified_through_sequence"], 5)

    def test_missing_checkpoint_causes_auto_to_run_full(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=2)
            report = run_local_verification(path, root / "runtime" / "audit", requested_mode="auto")
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["verification_mode"], "full")

    def test_incremental_inherits_full_structural_proof_without_running_quick_check(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=2)
            full = self.full(path, root)
            with PaperLedger(path) as ledger:
                ledger.append("DECISION", {"paper_decision_id": "tail"}, identity="tail")
            with patch.object(LocalLedgerVerifier, "_quick_check", side_effect=AssertionError("incremental must not quick_check")) as quick_check:
                report = run_local_verification(path, root / "runtime" / "audit", requested_mode="incremental")
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["quick_check"], "inherited_from_full")
            self.assertEqual(report["last_full_verification_id"], full["verification_id"])
            self.assertEqual(report["timings"]["quick_check_seconds"], 0.0)
            quick_check.assert_not_called()

    def test_v1_checkpoint_upgrades_only_with_current_immutable_full_proof(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=2)
            full = self.full(path, root)
            checkpoint = VerificationPaths(root / "runtime" / "audit").checkpoint
            v1 = json.loads(checkpoint.read_text(encoding="utf-8"))
            v1["schema"] = "beelzebub-ledger-verification-checkpoint-v1"
            for key in ("last_full_verification_id", "last_full_quick_check_at", "last_full_verified_sequence", "last_full_verified_hash"):
                v1.pop(key)
            checkpoint.write_text(json.dumps(v1), encoding="utf-8")
            with PaperLedger(path) as ledger:
                ledger.append("DECISION", {"paper_decision_id": "tail"}, identity="tail")
            report = run_local_verification(path, root / "runtime" / "audit", requested_mode="incremental")
            upgraded = json.loads(checkpoint.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["last_full_verification_id"], full["verification_id"])
            self.assertEqual(upgraded["schema"], "beelzebub-ledger-verification-checkpoint-v2")

    def test_v1_checkpoint_without_immutable_full_proof_fails_closed(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=2)
            self.full(path, root)
            audit = VerificationPaths(root / "runtime" / "audit")
            checkpoint = json.loads(audit.checkpoint.read_text(encoding="utf-8"))
            checkpoint["schema"] = "beelzebub-ledger-verification-checkpoint-v1"
            for key in ("last_full_verification_id", "last_full_quick_check_at", "last_full_verified_sequence", "last_full_verified_hash"):
                checkpoint.pop(key)
            audit.checkpoint.write_text(json.dumps(checkpoint), encoding="utf-8")
            for artifact in audit.reports.glob("*.json"):
                artifact.unlink()
            report = run_local_verification(path, root / "runtime" / "audit", requested_mode="incremental")
            self.assertEqual(report["status"], "FAIL")
            self.assertTrue(report["full_scan_required"])
            self.assertEqual(report["errors"][0]["code"], "CHECKPOINT_FULL_PROVENANCE_MISSING")

    def test_storage_pressure_aborts_only_verifier_and_keeps_checkpoint(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=2)
            self.full(path, root)
            checkpoint = VerificationPaths(root / "runtime" / "audit").checkpoint
            before = checkpoint.read_text(encoding="utf-8")
            with patch.dict(os.environ, {"BEELZEBUB_L3G_VERIFIER_EMERGENCY_FREE_BYTES": str(10**18)}):
                report = run_local_verification(path, root / "runtime" / "audit", requested_mode="incremental")
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["errors"][0]["code"], "STORAGE_PRESSURE_ABORT")
            self.assertEqual(checkpoint.read_text(encoding="utf-8"), before)
            with PaperLedger(path) as ledger:
                ledger.append("DECISION", {"paper_decision_id": "writer-still-runs"}, identity="writer-still-runs")

    def test_historical_mutation_invalidates_checkpoint_before_incremental_tail_scan(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=3)
            self.assertEqual(self.full(path, root)["status"], "PASS")
            connection = sqlite3.connect(path)
            try:
                connection.execute("UPDATE lane_iii_paper_audit SET payload_json = payload_json || ' ' WHERE ledger_sequence = 1")
                connection.commit()
            finally:
                connection.close()
            report = run_local_verification(path, root / "runtime" / "audit", requested_mode="incremental")
            self.assertEqual(report["status"], "FAIL")
            self.assertTrue(report["full_scan_required"])
            self.assertEqual(report["errors"][0]["code"], "HISTORICAL_MUTATION_DETECTED")

    def test_truncation_replacement_wrong_epoch_and_checkpoint_beyond_tip_fail_closed(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=3, epoch=True)
            self.assertEqual(self.full(path, root)["status"], "PASS")
            connection = sqlite3.connect(path)
            try:
                connection.execute("DELETE FROM lane_iii_paper_audit WHERE ledger_sequence = 3")
                connection.commit()
            finally:
                connection.close()
            report = run_local_verification(path, root / "runtime" / "audit", requested_mode="incremental")
            self.assertEqual(report["errors"][0]["code"], "CHECKPOINT_BEYOND_TIP")

        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=2, epoch=True)
            self.assertEqual(self.full(path, root)["status"], "PASS")
            connection = sqlite3.connect(path)
            try:
                connection.execute("UPDATE lane_iii_paper_ledger_metadata SET metadata_value='L3G-PAPER-EPOCH-999' WHERE metadata_key='ledger_epoch'")
                connection.commit()
            finally:
                connection.close()
            report = run_local_verification(path, root / "runtime" / "audit", requested_mode="incremental")
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["errors"][0]["code"], "LEDGER_EPOCH_PATH_MISMATCH")

        with TemporaryDirectory() as folder:
            root = Path(folder); original = self.make_ledger(root / "one", rows=2)
            self.assertEqual(self.full(original, root)["status"], "PASS")
            replacement = self.make_ledger(root / "two", rows=2)
            original.unlink(); replacement.replace(original)
            report = run_local_verification(original, root / "runtime" / "audit", requested_mode="incremental")
            self.assertEqual(report["status"], "FAIL")
            self.assertEqual(report["errors"][0]["code"], "LEDGER_REPLACED")

    def test_corrupted_chain_failure_and_cancelled_run_never_advance_checkpoint(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=3)
            self.assertEqual(self.full(path, root)["status"], "PASS")
            checkpoint = VerificationPaths(root / "runtime" / "audit").checkpoint
            original_checkpoint = checkpoint.read_text(encoding="utf-8")
            connection = sqlite3.connect(path)
            try:
                connection.execute("UPDATE lane_iii_paper_audit SET payload_json = replace(payload_json, 'l3g-verifier-2', 'l3g-corrupted') WHERE ledger_sequence = 3")
                connection.commit()
            finally:
                connection.close()
            failure = run_local_verification(path, root / "runtime" / "audit", requested_mode="full")
            self.assertEqual(failure["status"], "FAIL")
            self.assertEqual(failure["errors"][0]["code"], "CHAIN_INVALID")
            self.assertEqual(checkpoint.read_text(encoding="utf-8"), original_checkpoint)

        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=1)
            self.assertEqual(self.full(path, root)["status"], "PASS")
            checkpoint = VerificationPaths(root / "runtime" / "audit").checkpoint
            original_checkpoint = checkpoint.read_text(encoding="utf-8")
            verifier = LocalLedgerVerifier(path, root / "runtime" / "audit", requested_mode="full")
            verifier._cancel_path.write_text("cancel\n", encoding="utf-8")
            cancelled = verifier.run()
            self.assertEqual(cancelled["status"], "CANCELLED")
            self.assertEqual(checkpoint.read_text(encoding="utf-8"), original_checkpoint)

    def test_controller_launches_exactly_one_detached_process_and_reuses_existing_run(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=1)
            controller = LocalLedgerVerificationController(path, root / "runtime" / "audit")
            process = Mock(pid=424242)
            with patch("src.l3g_paper.verification.subprocess.Popen", return_value=process) as launch, patch("src.l3g_paper.verification._pid_is_running", return_value=True):
                first = controller.start("auto")
                second = controller.start("full")
            self.assertEqual(first["status"], "IN_PROGRESS")
            self.assertEqual(second["status"], "IN_PROGRESS")
            launch.assert_called_once()
            self.assertEqual(second["verification_id"], first["verification_id"])

    def test_controller_process_completes_independently_and_artifact_is_consumable(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=20)
            controller = LocalLedgerVerificationController(path, root / "runtime" / "audit")
            controller.start("full")
            deadline = time.monotonic() + 15
            status = controller.status()
            while status["status"] == "IN_PROGRESS" and time.monotonic() < deadline:
                time.sleep(0.05)
                # A new controller mirrors a browser/API refresh: it reads the
                # artifact only and does not own or terminate the child scan.
                status = LocalLedgerVerificationController(path, root / "runtime" / "audit").status()
            self.assertEqual(status["status"], "PASS")
            self.assertEqual(status["verification_mode"], "full")
            # Reap the detached child handle if its final artifact raced its
            # process exit; this is not needed for artifact consumption.
            for _ in range(20):
                controller.status()
                if not controller._children:
                    break
                time.sleep(0.01)

    def test_lock_rejects_duplicate_direct_verifiers(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=1); paths = VerificationPaths(root / "runtime" / "audit")
            _create_lock(paths, "lv-existing", state="RUNNING")
            with self.assertRaisesRegex(VerificationFailure, "already running"):
                run_local_verification(path, root / "runtime" / "audit", requested_mode="full")

    def test_scheduler_task_only_launches_the_local_process(self) -> None:
        with TemporaryDirectory() as folder:
            root = Path(folder); path = self.make_ledger(root, rows=1)
            controller = LocalLedgerVerificationController(path, root / "runtime" / "audit")
            progress: list[tuple[object, ...]] = []

            class Context:
                configuration = validate_ledger_verification({"mode": "incremental"})
                dependencies = {"ledger_verification_controller": controller}

                def progress(self, *args: object) -> None:
                    progress.append(args)

            with patch.object(controller, "start", return_value={"status": "IN_PROGRESS", "verification_id": "lv-scheduled"}) as start:
                outcome = ledger_verification(Context())
            start.assert_called_once_with("incremental")
            self.assertEqual(outcome.result["verification_id"], "lv-scheduled")
            self.assertEqual(len(progress), 2)


if __name__ == "__main__":
    unittest.main()
