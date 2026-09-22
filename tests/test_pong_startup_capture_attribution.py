from __future__ import annotations

import hashlib
import json

import pytest

from tools.analyze_pong_startup_capture import analyze, canonical


def fixture_packet(tmp_path, *, changed_identity=False, corrupt=False):
    prior = "0" * 64
    rows = []
    for index, sequence in enumerate((100, 105), 1):
        row = {
            "collector_id": "test", "journal_sequence": index,
            "previous_record_hash": prior,
            "received_by_collector_at": f"2026-09-22T07:42:0{index}Z",
            "observation": {
                "ninja_receipt_time": f"2026-09-22T07:37:0{index}Z",
                "local_monotonic_sequence": sequence, "observation_type": "DEPTH",
                "session_id": "second" if changed_identity and index == 2 else "first",
                "producer": {"source_fingerprint": "source", "build_fingerprint": "build"},
                "payload": {"source_generation": 1, "source_event_sequence": sequence,
                            "observer_queue_overflow_frames": 20 * index,
                            "observer_transport_failed_frames": 10 * index},
            },
        }
        prior = hashlib.sha256(canonical(row)).hexdigest()
        row["record_hash"] = prior
        rows.append(row)
    if corrupt:
        rows[0]["observation"]["payload"]["observer_queue_overflow_frames"] = 0
    journal = tmp_path / "journal.jsonl"
    journal.write_text("".join(json.dumps(row) + "\n" for row in rows))
    status = tmp_path / "status.json"
    status.write_text(json.dumps({"collector": {"collector_id": "test", "started_at": "2026-09-22T07:42:00Z"},
                                 "durability": {"durable_records": 2, "journal_tip_sha256": prior}}))
    return journal, status


def test_historical_counter_deltas_are_attributed_to_native_time(tmp_path):
    report = analyze(*fixture_packet(tmp_path))
    assert report["counter_deltas"]["overflow"] == 20
    assert report["native_span_seconds"] == report["receipt_span_seconds"] == 1
    assert report["minimum_age_ms"] == report["maximum_age_ms"] == 300_000
    assert report["all_retained_native_records_precede_collector_start"]
    assert report["streams"]["DEPTH"]["missing_sequence_values"] == 4
    assert report["sustainable_native_capacity"] == "UNKNOWN"
    assert report["qualified_seconds"] == 0


def test_multiple_producers_cannot_form_counter_delta_or_gap(tmp_path):
    report = analyze(*fixture_packet(tmp_path, changed_identity=True))
    assert report["counter_deltas"]["overflow"] is None
    assert report["streams"]["DEPTH"]["missing_sequence_values"] == 0


def test_changed_evidence_refuses_analysis(tmp_path):
    with pytest.raises(ValueError, match="hash chain"):
        analyze(*fixture_packet(tmp_path, corrupt=True))
