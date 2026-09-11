from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from src.l3g_paper.contracts import FIVE_MINUTE_PERPETUAL_POLICY
from src.l3g_paper.perpetual_analysis import (
    build_perpetual_analysis,
    export_perpetual_analysis,
)


class PerpetualAnalysisTests(unittest.TestCase):
    @staticmethod
    def _fixture(path: Path) -> tuple[int, str]:
        connection = sqlite3.connect(path)
        connection.executescript("""
            CREATE TABLE lane_iii_paper_audit (
                ledger_sequence INTEGER PRIMARY KEY,
                identity TEXT NOT NULL UNIQUE,
                domain TEXT NOT NULL,
                kind TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                execution_session_id TEXT,
                payload_json TEXT NOT NULL,
                previous_record_hash TEXT,
                record_hash TEXT NOT NULL UNIQUE
            );
            CREATE TABLE lane_iii_paper_ledger_metadata (
                metadata_key TEXT PRIMARY KEY,
                metadata_value TEXT NOT NULL
            );
        """)
        metadata = {
            "ledger_uuid": "l3g-ledger-analysis-test",
            "ledger_epoch": "L3G-PAPER-EPOCH-ANALYSIS-TEST",
            "entry_profile_version": "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
            "paper_policy_hash": FIVE_MINUTE_PERPETUAL_POLICY.configuration_hash,
            "risk_profile_hash": "risk-test",
        }
        connection.executemany(
            "INSERT INTO lane_iii_paper_ledger_metadata VALUES (?,?)",
            metadata.items(),
        )

        def add(seq: int, domain: str, kind: str, at: str, payload: dict[str, object]) -> None:
            envelope = {
                "paper_policy_hash": FIVE_MINUTE_PERPETUAL_POLICY.configuration_hash,
                "payload": payload,
            }
            connection.execute(
                "INSERT INTO lane_iii_paper_audit VALUES (?,?,?,?,?,?,?,?,?)",
                (seq, f"id-{seq}", domain, kind, at, "es-test",
                 json.dumps(envelope), None if seq == 1 else f"hash-{seq-1}", f"hash-{seq}"),
            )

        request = "bounded-analysis-test"
        add(1, "SESSION", "SESSION_OPERATIONAL_PAPER_STARTED", "2026-09-10T23:00:00Z", {
            "request_id": request, "session_id": "ASIA-test", "trade_date": "2026-09-11",
        })
        add(2, "DECISION", "DECISION", "2026-09-10T23:00:30Z", {
            "paper_decision_id": "decision-entry", "created_at": "2026-09-10T23:00:30Z",
            "decision": "LONG", "reason_code": "FIVE_MINUTE_ENTER_LONG",
            "family_summary": {"candle_close_utc": "2026-09-10T23:00:30Z", "target_position": "LONG"},
        })
        add(3, "COMMAND", "COMMAND", "2026-09-10T23:00:30.010Z", {
            "command_id": "command-entry", "decision_id": "decision-entry",
            "action": "ENTER_LONG", "reason_code": "AUTONOMOUS_PAPER_ENTRY",
            "created_at": "2026-09-10T23:00:30.010Z",
        })
        add(4, "EXECUTION", "EXECUTION", "2026-09-10T23:00:30.020Z", {
            "native_execution_id": "execution-entry", "command_id": "command-entry",
            "order_role": "ENTRY", "direction": "LONG", "timestamp": "2026-09-10T23:00:30.020Z",
        })
        add(5, "COMMAND", "COMMAND", "2026-09-10T23:01:00Z", {
            "command_id": "command-exit", "decision_id": "decision-exit",
            "action": "EMERGENCY_FLATTEN", "reason_code": "QUOTE_STALE",
            "created_at": "2026-09-10T23:01:00Z",
        })
        add(6, "EXECUTION", "EXECUTION_REALIZED_PNL", "2026-09-10T23:01:00.020Z", {
            "trade_identity": "trade-1", "entry_command_id": "command-entry",
            "exit_command_id": "command-exit", "entry_decision_id": "decision-entry",
            "entry_execution_id": "execution-entry", "exit_execution_id": "execution-exit",
            "entry_order_id": "order-entry", "exit_order_id": "order-exit",
            "entry_timestamp": "2026-09-10T23:00:30.020Z",
            "exit_timestamp": "2026-09-10T23:01:00.020Z",
            "entry_price": "100", "exit_price": "101", "entry_quantity": 1,
            "realized_pnl": "2", "simulated_fees": "0",
            "pnl_basis": "AUTHENTIC_ENTRY_AND_EXIT_FILLS", "trade_date": "2026-09-11",
        })
        add(7, "SESSION", "SESSION_OPERATIONAL_PAPER_STOPPED", "2026-09-11T00:00:00Z", {
            "request_id": request, "session_id": "ASIA-test", "trade_date": "2026-09-11",
            "started_at": "2026-09-10T23:00:00Z", "reason": "OPERATOR_STOP_TRADING",
            "final_position": "FLAT", "final_quantity": 0, "final_working_order_count": 0,
        })
        connection.commit()
        connection.close()
        return 7, "hash-7"

    def test_builds_verified_native_trade_metrics_and_tables(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            sequence, record_hash = self._fixture(path)
            report = build_perpetual_analysis(
                path, verification_id="lv-test", verified_sequence=sequence,
                verified_hash=record_hash, request_id="bounded-analysis-test",
            )
            self.assertEqual(report["summary"]["closed_trades"], 1)
            self.assertEqual(report["summary"]["gross_realized_pnl"], "2")
            self.assertEqual(report["summary"]["freshness_emergency_exit_count"], 1)
            self.assertEqual(report["run"]["final_position"], "FLAT")
            exported = export_perpetual_analysis(
                path, Path(directory) / "analysis", verification_id="lv-test",
                verified_sequence=sequence, verified_hash=record_hash,
                request_id="bounded-analysis-test",
            )
            self.assertTrue(Path(exported["artifacts"]["trades.csv"]["path"]).is_file())
            self.assertTrue(Path(exported["artifacts"]["report.md"]["path"]).is_file())

    def test_refuses_a_mismatched_verifier_fence(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            sequence, _ = self._fixture(path)
            with self.assertRaisesRegex(RuntimeError, "VERIFIED_LEDGER_FENCE_MISMATCH"):
                build_perpetual_analysis(
                    path, verification_id="lv-test", verified_sequence=sequence,
                    verified_hash="wrong", request_id="bounded-analysis-test",
                )


if __name__ == "__main__":
    unittest.main()
