from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.reset import create_clean_reset_genesis


class CleanResetGenesisTests(unittest.TestCase):
    def test_first_record_is_exact_clean_reset_genesis(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = root / "reset-receipt.json"
            receipt.write_text(json.dumps({"schema": "test-reset-receipt"}), encoding="utf-8")
            path = root / "epoch-003" / "lane_iii_paper.sqlite3"
            sha = "a" * 40
            result = create_clean_reset_genesis(
                path,
                epoch_id="L3G-PAPER-EPOCH-003",
                reset_receipt_path=receipt,
                reset_timestamp="2026-09-01T23:30:00Z",
                checkout_sha=sha,
                build_sha=sha,
                runtime_sha=sha,
                addon_source_fingerprint="b" * 64,
                addon_build_fingerprint="c" * 64,
            )
            self.assertEqual(result["genesis_sequence"], 1)
            with PaperLedger(path, epoch_id="L3G-PAPER-EPOCH-003") as ledger:
                records = ledger.recent(5)
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0]["kind"], "SESSION_CLEAN_RESET_GENESIS")
                payload = records[0]["payload"]
                self.assertEqual(payload["account_name"], "Sim101")
                self.assertEqual(payload["account_class"], "LOCAL_SIMULATION")
                self.assertEqual(payload["instrument"], "MNQ SEP26")
                self.assertEqual(payload["maximum_quantity"], 1)
                self.assertEqual(payload["quantity"], 0)
                self.assertEqual(payload["working_owned_orders"], 0)
                self.assertEqual(payload["live_capital"], "DENIED")
                self.assertEqual(payload["reset_receipt_path"], str(receipt.resolve()))

    def test_refuses_existing_target(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = root / "receipt.json"
            receipt.write_text("{}", encoding="utf-8")
            path = root / "epoch-003" / "lane_iii_paper.sqlite3"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"occupied")
            with self.assertRaises(FileExistsError):
                create_clean_reset_genesis(
                    path,
                    epoch_id="L3G-PAPER-EPOCH-003",
                    reset_receipt_path=receipt,
                    reset_timestamp="2026-09-01T23:30:00Z",
                    checkout_sha="a" * 40,
                    build_sha="b" * 40,
                    runtime_sha="a" * 40,
                    addon_source_fingerprint="b" * 64,
                    addon_build_fingerprint="c" * 64,
                )

    def test_refuses_provenance_mismatch(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            receipt = root / "receipt.json"
            receipt.write_text("{}", encoding="utf-8")
            path = root / "epoch-003" / "lane_iii_paper.sqlite3"
            with self.assertRaisesRegex(ValueError, "CHECKOUT_SHA == BUILD_SHA == RUNTIME_SHA"):
                create_clean_reset_genesis(
                    path,
                    epoch_id="L3G-PAPER-EPOCH-003",
                    reset_receipt_path=receipt,
                    reset_timestamp="2026-09-01T23:30:00Z",
                    checkout_sha="a" * 40,
                    build_sha="b" * 40,
                    runtime_sha="a" * 40,
                    addon_source_fingerprint="b" * 64,
                    addon_build_fingerprint="c" * 64,
                )


if __name__ == "__main__":
    unittest.main()
