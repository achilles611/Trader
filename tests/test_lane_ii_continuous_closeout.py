from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from scripts.lane_ii_continuous.closeout import (
    audit_capture_readonly, bind, closeout_capture, closeout_followup, reconcile_cohort_copy, reconcile_profiles,
    stable_database_copy,
)


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def study_fixture(root, *, open_position=False):
    capture = root / "segments/segment-001/study/capture"
    capture.mkdir(parents=True)
    operator = {"pid": 100, "creation_utc": "2026-11-07T16:00:00+00:00", "source_manifest_sha256": "frozen"}
    write(capture.parent / "operator.json", operator)
    entry = {"kind": "PAPER_ENTRY", "event_seq": 1, "data": {"profile": "P", "market": "m", "side": "Up", "quantity": 10, "basis": 5.1, "entry_fee": .1, "authorized_entry_budget": 6, "fills": [{"quantity": 10, "price": .5}]}}
    events = [entry]
    if not open_position:
        events.append({"kind": "PAPER_EXIT", "event_seq": 2, "data": {"profile": "P", "market": "m", "side": "Up", "basis": 5.1, "proceeds": 5.9, "exit_fee": .1, "net": .8, "fills": [{"quantity": 10, "price": .6}]}})
    ledger = capture / "P-ledger.jsonl"
    ledger.write_text("".join(json.dumps(e) + "\n" for e in events))
    receipt = {"profiles": [{"id": "P", "entries": 1, "cash_exits": int(not open_position), "settlements": 0, "unresolved_positions": int(open_position), "unresolved_basis": 5.1 if open_position else 0, "realized_pnl": 0 if open_position else .8, "modeled_fees": .1 if open_position else .2, "virtual_cash": 994.9 if open_position else 1000.8, "virtual_initial_cash": 1000}], "ledger_sha256": {"P": bind(ledger)["sha256"]}, "pid": 100, "source_manifest_sha256": "frozen", "real_orders": 0, "model_api_calls": 0, "authenticated_account_calls": 0, "state": "FAILED", "reason": "TOTAL_HTTP_TRANSPORT_TIMEOUT", "paper_start_utc": "2026-11-07T16:05:00+00:00", "actual_end_utc": "2026-11-07T18:35:00+00:00", "cutoff_utc": "2026-11-08T00:05:00+00:00"}
    write(capture / "receipt.json", receipt)
    write(capture / "COLLECTION_CLOSED.json", {"collection_closed": True, "network_closed": True, "snapshot_publisher_closed": True, "worker_pid": 100, "source_manifest_sha256": "frozen"})
    return capture, receipt


def cohort_fixture(root, *, with_wal=False):
    legacy = root / "segments/segment-001/cohort/capture/legacy"
    legacy.mkdir(parents=True)
    write(legacy.parent.parent / "operator.json", {"pid": 101, "creation_utc": "2026-11-07T16:00:00+00:00"})
    write(legacy / "run_config.json", {"initial_cash_per_wallet_per_sleeve": 1000, "live_orders": 0, "authenticated_requests": 0, "model_api_calls": 0, "started_at_utc": "2026-11-07T16:05:00+00:00"})
    write(legacy / "status.json", {"state": "RUNNING", "updated_at_utc": "2026-11-07T18:35:00+00:00"})
    conn = sqlite3.connect(legacy / "experiment.sqlite")
    if with_wal:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.executescript("""
    CREATE TABLE accounts(wallet,sleeve,cash,realized_pnl);
    CREATE TABLE inventory(wallet,sleeve,token,condition_id,quantity,basis);
    CREATE TABLE paper_fills(id INTEGER PRIMARY KEY,wallet,sleeve,side,quantity,price,fee,cash,realized_pnl);
    CREATE TABLE runtime_state(key,value_json);
    INSERT INTO accounts VALUES('wallet','policy',1004.9,4.9);
    INSERT INTO paper_fills VALUES(1,'wallet','policy','BUY',10,.5,.1,5.1,0);
    INSERT INTO paper_fills VALUES(2,'wallet','policy','SETTLEMENT',10,1,0,10,4.9);
    INSERT INTO runtime_state VALUES('checkpoint','{"at_utc":"2026-11-07T18:36:00+00:00","state":"RUNNING","terminal":false}');
    """)
    conn.commit()
    if not with_wal:
        conn.close()
    return legacy, conn if with_wal else None


class CloseoutTests(unittest.TestCase):
    def test_generic_forward_closeout_preserves_terminal_state_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture, receipt = study_fixture(root / "old")
            run = capture.parent
            result = closeout_capture(run, root / "out", process_probe=lambda _: {"state": "ABSENT"})
            study = result["components"]["study"]
            self.assertEqual(study["execution"]["state"], "failed")
            self.assertTrue(study["closure"]["accounting_reconciled"])
            self.assertEqual(study["closure"]["causal_replay"], "NOT_RUN")
            self.assertFalse(study["closure"]["capture_sealed"])
            self.assertEqual(result, closeout_capture(run, root / "out", process_probe=lambda _: {"state": "ALIVE"}))

    def test_streaming_audit_preserves_failure_and_detects_chain_corruption(self):
        def canonical(value):
            return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture, receipt = study_fixture(root / "old")
            source = root / "source"
            write(source / "SOURCE_MANIFEST.json", {"files": {}})
            receipt["source_manifest_sha256"] = bind(source / "SOURCE_MANIFEST.json")["sha256"]
            write(capture / "COLLECTION_CLOSED.json", {"collection_closed": True, "network_closed": True, "worker_pid": 100, "source_manifest_sha256": receipt["source_manifest_sha256"]})
            ledger = [json.loads(line) for line in (capture / "P-ledger.jsonl").read_text().splitlines()]
            data = [("START", {"manifest_sha256": receipt["source_manifest_sha256"]})]
            data.extend((row["kind"], row["data"]) for row in ledger)
            data.append(("END", {"state": receipt["state"], "reason": receipt["reason"]}))
            previous, lines = "0" * 64, []
            for sequence, (kind, payload) in enumerate(data, 1):
                row = {"seq": sequence, "utc": "2026-11-07T17:00:00+00:00", "monotonic_ns": sequence, "kind": kind, "data": payload, "previous_sha256": previous}
                row["sha256"] = previous = hashlib.sha256(canonical(row)).hexdigest()
                lines.append(canonical(row) + b"\n")
            (capture / "events.jsonl").write_bytes(b"".join(lines))
            for row in ledger:
                row["event_seq"] += 1
            (capture / "P-ledger.jsonl").write_bytes(b"".join(canonical(row) + b"\n" for row in ledger))
            (capture / "websocket-frames.jsonl").write_bytes(b"")
            receipt.update(events=len(lines), chain_sha256=previous, ws_frames=0,
                           events_sha256=bind(capture / "events.jsonl")["sha256"], websocket_sha256=bind(capture / "websocket-frames.jsonl")["sha256"],
                           ledger_sha256={"P": bind(capture / "P-ledger.jsonl")["sha256"]})
            write(capture / "receipt.json", receipt)
            before = {p.name: bind(p) for p in capture.iterdir()}
            report = audit_capture_readonly(capture, source, root / "audit", process_probe=lambda _: {"state": "ABSENT"})
            self.assertEqual(report["status"], "PASS_RETAINED_CAPTURE_INTEGRITY_ONLY")
            self.assertEqual(report["execution"]["state"], "FAILED")
            self.assertFalse(report["full_duration_qualified"])
            self.assertEqual(before, {p.name: bind(p) for p in capture.iterdir()})
            (capture / "events.jsonl").write_bytes((capture / "events.jsonl").read_bytes().replace(b'"monotonic_ns":2', b'"monotonic_ns":1'))
            report = audit_capture_readonly(capture, source, root / "bad-audit", process_probe=lambda _: {"state": "ABSENT"})
            self.assertEqual(report["status"], "FAIL_RETAINED_CAPTURE_INTEGRITY")
            self.assertIn("EVENT_CHAIN", report["problem_counts"])

    def test_financial_reconstruction_keeps_unresolved_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture, receipt = study_fixture(Path(tmp), open_position=True)
            report = reconcile_profiles(capture, receipt)
            self.assertEqual(report["accounts"][0]["unresolved_basis"], 5.1)
            self.assertEqual(report["accounts"][0]["realized_pnl"], 0)
            self.assertEqual(report["causal_replay"], "NOT_RUN")

    def test_changed_ledger_and_duplicate_event_refuse_accounting(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture, receipt = study_fixture(Path(tmp))
            ledger = capture / "P-ledger.jsonl"
            original = ledger.read_text()
            ledger.write_text(original + original.splitlines()[0] + "\n")
            with self.assertRaisesRegex(ValueError, "HASH_MISMATCH"):
                reconcile_profiles(capture, receipt)
            receipt["ledger_sha256"]["P"] = bind(ledger)["sha256"]
            with self.assertRaisesRegex(ValueError, "DUPLICATE"):
                reconcile_profiles(capture, receipt)

    def test_wal_copy_recovers_committed_rows_without_mutating_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            legacy, con = cohort_fixture(root / "source", with_wal=True)
            try:
                before = [bind(p) for p in legacy.glob("experiment.sqlite*")]
                self.assertGreater((legacy / "experiment.sqlite-wal").stat().st_size, 0)
                bindings = stable_database_copy(legacy, root / "copied")
                result = reconcile_cohort_copy(root / "copied/experiment.sqlite", 1000)
                self.assertEqual(result["counts"]["entries"], 1)
                self.assertEqual(result["accounts"][0]["realized_pnl"], 4.9)
                self.assertEqual(before, [bind(p) for p in legacy.glob("experiment.sqlite*")])
                self.assertEqual(len(bindings), 3)
            finally:
                con.close()

    def test_failed_study_and_unsealed_cohort_do_not_become_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            root, output = Path(tmp) / "retained", Path(tmp) / "analysis"
            study_fixture(root)
            cohort_fixture(root)
            write(root / "WINDOW.json", {"paper_seconds": 28800})
            original = {str(p): bind(p) for p in root.rglob("*") if p.is_file()}
            result = closeout_followup(root, output, process_probe=lambda _: {"state": "ABSENT"})
            self.assertEqual(result["status"], "PARTIAL_ACCOUNTING_RECONCILED")
            self.assertEqual(result["components"]["study"]["execution"]["state"], "failed")
            self.assertEqual(result["components"]["study"]["coverage"]["observed_seconds"], 9000)
            self.assertFalse(result["components"]["cohort"]["closure"]["capture_sealed"])
            self.assertFalse(result["full_duration_qualified"])
            self.assertEqual(original, {str(p): bind(p) for p in root.rglob("*") if p.is_file()})
            self.assertEqual(result, closeout_followup(root, output, process_probe=lambda _: {"state": "ALIVE"}))

    def test_alive_owner_prevents_analysis_and_original_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "retained"
            study_fixture(root); cohort_fixture(root)
            write(root / "WINDOW.json", {"paper_seconds": 28800})
            result = closeout_followup(root, Path(tmp) / "out", process_probe=lambda _: {"state": "ALIVE"})
            self.assertEqual(result["status"], "PARTIAL_ANALYSIS_BLOCKED")
            self.assertTrue(all(c["closure"]["accounting"] == "BLOCKED" for c in result["components"].values()))
            with self.assertRaisesRegex(ValueError, "OUTSIDE"):
                closeout_followup(root, root / "bad")

    def test_account_cash_mismatch_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            legacy, _ = cohort_fixture(Path(tmp))
            con = sqlite3.connect(legacy / "experiment.sqlite")
            con.execute("UPDATE accounts SET cash=2000"); con.commit(); con.close()
            with self.assertRaisesRegex(ValueError, "CASH_LEDGER_MISMATCH"):
                reconcile_cohort_copy(legacy / "experiment.sqlite", 1000)


if __name__ == "__main__":
    unittest.main()
