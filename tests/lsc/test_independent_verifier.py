from __future__ import annotations

import ast
from copy import deepcopy
import json
from pathlib import Path
import unittest

from tools.benchmark_lsc_v1 import (
    ACCEPTANCE_RATE,
    BenchmarkError,
    OfflineStageRecorder,
    evaluate_receipt,
)
from tools.verify_lsc_v1 import (
    VerificationError,
    compare_corpora,
    verify_context,
    verify_golden_vectors,
    verify_record,
)


ROOT = Path(__file__).resolve().parents[2]
GOLDEN = ROOT / "tests" / "lsc" / "golden_vectors.json"


def golden_data() -> dict[str, object]:
    return json.loads(GOLDEN.read_text(encoding="utf-8"))


def legacy_row(verified) -> dict[str, object]:
    context = verified.context
    return {
        "schema": "lane-iii-phase-f2-ninjatrader-observation-v1",
        "observation_id": verified.observation_id,
        "session_id": verified.session_id,
        "observation_type": verified.observation_type,
        "ninja_receipt_time": verified.ninja_receipt_time,
        "local_monotonic_sequence": verified.local_sequence,
        "provider_timestamp": verified.provider_timestamp,
        "provider_sequence": verified.provider_sequence,
        "exchange_timestamp": verified.exchange_timestamp,
        "account": None if verified.account_alias is None else {
            "alias": verified.account_alias,
            "class": verified.account_class,
        },
        "payload": dict(verified.payload),
        "payload_hash": verified.payload_hash,
        "wire_hash": verified.wire_hash,
        "source": context.source,
        "provider": context.provider,
        "environment": context.environment,
        "source_identity": context.source_identity,
        "build_identity": context.build_identity,
        "contract_identity": context.contract_identity,
        "contract_id": context.contract_id,
        "generation": context.generation,
    }


def passing_run(run_id: str, run_class: str) -> dict[str, object]:
    expected = 21_110
    return {
        "run_id": run_id,
        "run_class": run_class,
        "target_events_per_second": 2_111,
        "pacing_seconds": 60.0 if run_class == "sustained" else 5.0,
        "expected_events": expected,
        "counts": {stage: expected for stage in ("ingress", "admission", "encode", "durable", "decode", "replay")},
        "seconds": {
            "ingress": 10.0, "admission": 10.0, "encode": 10.0,
            "durable": 10.0, "decode": 10.0, "replay": 10.0,
            "drain": 0.25, "recovery": 0.5, "full_verification": 0.75,
        },
        "latency_ms": {
            "encode": [0.10, 0.20, 0.30, 0.40],
            "durable": [0.50, 0.60, 0.70, 0.80],
        },
        "queue": {
            "start": 0, "end": 0, "high_water": 128, "capacity": 4096,
            "max_residence_ms": 25.0, "max_allowed_residence_ms": 100.0,
        },
        "storage": {
            "legacy_database_bytes": 10_000_000, "legacy_wal_bytes": 1_000_000,
            "lsc_segment_bytes": 2_000_000, "lsc_commitment_bytes": 10_000,
            "raw_record_bytes": 8_000_000, "compressed_segment_bytes": 2_000_000,
            "disk_write_bytes": 2_100_000, "disk_read_bytes": 2_050_000,
            "disk_write_iops": 500, "disk_read_iops": 450,
            "legacy_database_growth_bytes": 10_000_000,
            "legacy_wal_growth_bytes": 1_000_000,
            "lsc_segment_growth_bytes": 2_000_000,
            "lsc_commitment_growth_bytes": 10_000,
        },
        "deltas": {
            "producer_overflow": 0, "transport_failures": 0,
            "sequence_gaps": 0, "unexplained_loss": 0,
        },
        "resources": {"cpu_seconds": 8.0, "peak_memory_bytes": 64_000_000},
        "durability": {
            "segment_chain_verified": True, "commitments_verified": True,
            "recovery_verified": True, "full_verification_passed": True,
            "drained": True, "writer_error": None,
            "final_replay_hash": "a" * 64,
            "durability_method": "same_volume_atomic_publish_and_fsync",
        },
        "configuration_identity": "lsc-v1-fixed-config",
        "corpus_sha256": "b" * 64,
        "source_identity": "source-fixed",
        "build_identity": "build-fixed",
        "contract_identity": "MNQZ6-CME-2026-12",
        "hardware_identity": "atlas-fixed-hardware-state",
        "storage_path_identity": "N-volume-fixed-root",
    }


def passing_receipt() -> dict[str, object]:
    return {
        "schema": "beelzebub-lsc-benchmark-receipt-v1",
        "runs": [
            *(passing_run(f"sustained-{index}", "sustained") for index in range(3)),
            *(passing_run(f"burst-{index}", "burst") for index in range(3)),
        ],
    }


class IndependentVerifierTests(unittest.TestCase):
    def test_verifier_has_no_production_codec_or_store_import(self):
        source = (ROOT / "tools" / "verify_lsc_v1.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.append(node.module or "")
        self.assertFalse(any(name.startswith("src.lsc") for name in imported), imported)
        self.assertNotIn("decode_observation", source)

    def test_independently_verifies_all_immutable_golden_vectors(self):
        report = verify_golden_vectors(GOLDEN)
        self.assertEqual(report["result"], "PASS")
        self.assertEqual(report["record_count"], 3)
        self.assertEqual(set(report["records"]), {"quote", "trade", "depth"})
        trade = report["records"]["trade"]
        self.assertEqual(trade["payload"]["price"], "20000.10")
        depth = report["records"]["depth"]
        self.assertEqual(depth["ninja_receipt_time"], "2026-09-23T00:00:01.0000001Z")
        self.assertEqual(depth["exchange_timestamp"], "2026-09-23T00:00:00.999999999Z")

    def test_context_and_event_corruption_are_refused(self):
        value = golden_data()
        context_bytes = bytearray.fromhex(value["context_hex"])
        context_bytes[-1] ^= 1
        with self.assertRaisesRegex(VerificationError, "context digest mismatch"):
            verify_context(bytes(context_bytes))

        clean_context = bytes.fromhex(value["context_hex"])
        for name in ("quote", "trade", "depth"):
            with self.subTest(name=name):
                record = bytearray.fromhex(value["records"][name]["record_hex"])
                record[len(record) // 2] ^= 1
                with self.assertRaisesRegex(VerificationError, "event digest mismatch"):
                    verify_record(bytes(record), clean_context)

    def test_legacy_lsc_parity_and_typed_first_mismatch(self):
        value = golden_data()
        context_hex = value["context_hex"]
        events = []
        lsc_events = []
        for name in ("quote", "trade", "depth"):
            record_hex = value["records"][name]["record_hex"]
            verified = verify_record(bytes.fromhex(record_hex), bytes.fromhex(context_hex))
            events.append(legacy_row(verified))
            lsc_events.append({"context_hex": context_hex, "record_hex": record_hex})
        proof = {
            "evidence_families": ["family-a"],
            "hypotheses": [{"id": "hypothesis-a", "state": "NO_EFFECT"}],
            "decisions": [{"decision": "NO_TRADE", "reason": "NO_ELIGIBLE_HYPOTHESIS"}],
            "reason_codes": ["NO_ELIGIBLE_HYPOTHESIS"],
            "final_replay_state": {"flat": True, "book": "complete"},
            "final_replay_hash": "c" * 64,
        }
        legacy_path = Path(self._testMethodName + "-legacy.json")
        lsc_path = Path(self._testMethodName + "-lsc.json")
        try:
            legacy_path.write_text(json.dumps({"events": events, **proof}), encoding="utf-8")
            lsc_path.write_text(json.dumps({"events": lsc_events, **proof}), encoding="utf-8")
            passed = compare_corpora(legacy_path, lsc_path)
            self.assertEqual(passed["result"], "PASS")
            self.assertEqual(passed["matched_event_count"], 3)
            self.assertTrue(passed["checks"]["depth_book_checkpoints"])

            changed = deepcopy(events)
            changed[1]["payload"]["price"] = 20000.10
            changed[1].pop("payload_hash")
            changed[1].pop("wire_hash")
            legacy_path.unlink()
            legacy_path.write_text(json.dumps({"events": changed, **proof}), encoding="utf-8")
            failed = compare_corpora(legacy_path, lsc_path)
            self.assertEqual(failed["result"], "FAIL")
            self.assertEqual(failed["first_mismatch"]["path"], "$.events[1].payload.price")
            self.assertEqual(failed["first_mismatch"]["reason"], "TYPE")
        finally:
            legacy_path.unlink(missing_ok=True)
            lsc_path.unlink(missing_ok=True)

    def test_parity_reports_count_and_proof_mismatch(self):
        value = golden_data()
        context_hex = value["context_hex"]
        quote_hex = value["records"]["quote"]["record_hex"]
        verified = verify_record(bytes.fromhex(quote_hex), bytes.fromhex(context_hex))
        legacy_path = Path(self._testMethodName + "-legacy.json")
        lsc_path = Path(self._testMethodName + "-lsc.json")
        try:
            legacy_path.write_text(json.dumps({"events": [legacy_row(verified)], "final_replay_hash": "a" * 64}), encoding="utf-8")
            lsc_path.write_text(json.dumps({"events": [], "final_replay_hash": "b" * 64}), encoding="utf-8")
            report = compare_corpora(legacy_path, lsc_path)
            self.assertEqual(report["first_mismatch"]["reason"], "COUNT")
            self.assertFalse(report["checks"]["count"])
        finally:
            legacy_path.unlink(missing_ok=True)
            lsc_path.unlink(missing_ok=True)

    def test_benchmark_accepts_only_three_sustained_and_three_burst_clean_runs(self):
        report = evaluate_receipt(passing_receipt())
        self.assertTrue(report["accepted"])
        self.assertEqual(report["claim"], "PASS_2111")
        self.assertEqual(report["sustained_run_count"], 3)
        self.assertEqual(report["burst_run_count"], 3)
        for run in report["runs"]:
            self.assertGreaterEqual(run["rates_events_per_second"]["durable"], ACCEPTANCE_RATE)
            self.assertEqual(run["queue"]["growth"], 0)
            self.assertEqual(run["storage"]["compression_ratio"], 4.0)
            self.assertIn("p99", run["latency_ms"]["encode"])

    def test_benchmark_refuses_short_run_set_loss_instability_or_identity_drift(self):
        short = passing_receipt()
        short["runs"] = short["runs"][:5]
        report = evaluate_receipt(short)
        self.assertFalse(report["accepted"])
        self.assertEqual(report["claim"], "NOT_PROVEN_2111")
        self.assertIn("REQUIRES_AT_LEAST_3_SUSTAINED_AND_3_BURST_RUNS", report["blockers"])

        lossy = passing_receipt()
        lossy["runs"][0]["deltas"]["sequence_gaps"] = 1
        report = evaluate_receipt(lossy)
        self.assertFalse(report["accepted"])
        self.assertIn("NONZERO_LOSS_DELTA", report["runs"][0]["reasons"])

        drift = passing_receipt()
        drift["runs"][-1]["build_identity"] = "other-build"
        report = evaluate_receipt(drift)
        self.assertFalse(report["identity_consistent"])
        self.assertIn("LIKE_FOR_LIKE_IDENTITIES_DIFFER", report["blockers"])

    def test_benchmark_requires_raw_stage_and_latency_shapes(self):
        malformed = passing_receipt()
        del malformed["runs"][0]["seconds"]["replay"]
        with self.assertRaisesRegex(BenchmarkError, "exact required shape"):
            evaluate_receipt(malformed)

        recorder = OfflineStageRecorder()
        with recorder.stage("decode"):
            pass
        self.assertGreater(recorder.seconds["decode"], 0)
        self.assertEqual(recorder.measure("decode", lambda: 7), 7)
        self.assertEqual(len(recorder.latency_ms["decode"]), 1)


if __name__ == "__main__":
    unittest.main()
