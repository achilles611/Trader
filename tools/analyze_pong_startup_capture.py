"""Read-only attribution of retained native time versus receiver receipt time.

No key, runtime, network or ledger access. Hash-chain checks establish artifact
integrity, not fresh signature verification or qualification. Output is new-only.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
from pathlib import Path


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def time(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timezone missing")
    return result


def analyze(journal: Path, status_path: Path) -> dict:
    status_bytes = status_path.read_bytes()
    status = json.loads(status_bytes)
    started = time(status["collector"]["started_at"])
    rows = []
    digest = hashlib.sha256()
    prior_hash = "0" * 64
    for raw in journal.open("rb"):
        digest.update(raw)
        row = json.loads(raw)
        actual = row.pop("record_hash")
        if (row["previous_record_hash"] != prior_hash
                or row["journal_sequence"] != len(rows) + 1
                or hashlib.sha256(canonical(row)).hexdigest() != actual
                or row["collector_id"] != status["collector"]["collector_id"]):
            raise ValueError("Collector hash chain or identity mismatch")
        prior_hash = actual
        rows.append(row)
    if (not rows or len(rows) != status["durability"]["durable_records"]
            or prior_hash != status["durability"]["journal_tip_sha256"]):
        raise ValueError("Journal does not match final durable status")

    def point(row):
        observation = row["observation"]
        payload = observation["payload"]
        return {
            "native_timestamp": observation["ninja_receipt_time"],
            "collector_receipt": row["received_by_collector_at"],
            "global_sequence": observation["local_monotonic_sequence"],
            "overflow_counter": payload["observer_queue_overflow_frames"],
            "transport_failed_counter": payload["observer_transport_failed_frames"],
        }

    streams = {}
    identities = set()
    ages = []
    prestart = 0
    regressions = 0
    prior_global = {}
    counter_regressions = Counter()
    prior_counters = {}
    for row in rows:
        obs = row["observation"]
        payload = obs["payload"]
        producer = obs["producer"]
        identity = (obs["session_id"], producer["source_fingerprint"], producer["build_fingerprint"], payload.get("source_generation"))
        identities.add(identity)
        native = time(obs["ninja_receipt_time"])
        ages.append((time(row["received_by_collector_at"]) - native).total_seconds() * 1000)
        prestart += native < started
        sequence = obs["local_monotonic_sequence"]
        if identity in prior_global and sequence <= prior_global[identity]:
            regressions += 1
        prior_global[identity] = sequence
        for field in ("observer_queue_overflow_frames", "observer_transport_failed_frames"):
            value = payload[field]
            key = identity, field
            if key in prior_counters and value < prior_counters[key]:
                counter_regressions[field] += 1
            prior_counters[key] = value
        stream = streams.setdefault(obs["observation_type"], {"records": 0, "missing_sequence_values": 0, "gap_jumps": 0, "regressions": 0, "prior": {}})
        stream["records"] += 1
        source_sequence = payload["source_event_sequence"]
        previous = stream["prior"].get(identity)
        if previous is not None:
            if source_sequence > previous + 1:
                stream["missing_sequence_values"] += source_sequence - previous - 1
                stream["gap_jumps"] += 1
            elif source_sequence <= previous:
                stream["regressions"] += 1
        stream["prior"][identity] = source_sequence
    for stream in streams.values():
        del stream["prior"]
    first, last = point(rows[0]), point(rows[-1])
    native_span = (time(last["native_timestamp"]) - time(first["native_timestamp"])).total_seconds()
    receipt_span = (time(last["collector_receipt"]) - time(first["collector_receipt"])).total_seconds()
    return {
        "schema": "pong-startup-capture-attribution-v1",
        "authority": "OFFLINE_DIAGNOSTIC_ONLY",
        "journal": {"path": str(journal.resolve()), "sha256": digest.hexdigest(), "records": len(rows), "chain_valid": True, "tip_hash": prior_hash},
        "status": {"path": str(status_path.resolve()), "sha256": hashlib.sha256(status_bytes).hexdigest()},
        "collector_started_at": status["collector"]["started_at"],
        "first": first, "last": last,
        "native_span_seconds": native_span, "receipt_span_seconds": receipt_span,
        "retained_native_records_before_collector_start": prestart,
        "all_retained_native_records_precede_collector_start": prestart == len(rows),
        "minimum_age_ms": min(ages), "maximum_age_ms": max(ages),
        "source_identities": [dict(zip(("session", "source", "build", "generation"), identity)) for identity in sorted(identities, key=str)],
        "global_sequence_regressions": regressions,
        "counter_regressions": dict(counter_regressions),
        "counter_deltas": {
            "overflow": last["overflow_counter"] - first["overflow_counter"] if len(identities) == 1 else None,
            "transport_failure": last["transport_failed_counter"] - first["transport_failed_counter"] if len(identities) == 1 else None,
            "scope": "NATIVE_TIMESTAMPS_OF_RETAINED_COUNTER_SNAPSHOTS",
        },
        "streams": streams,
        "signature_reverified": False,
        "sustainable_native_capacity": "UNKNOWN",
        "rejected_wire_identity": "UNKNOWN_NOT_RETAINED",
        "qualified_seconds": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--status", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    report = analyze(args.journal, args.status)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
