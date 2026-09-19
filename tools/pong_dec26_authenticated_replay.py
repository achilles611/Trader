#!/usr/bin/env python3
"""Replay an existing DEC26 authenticated journal through the disarmed Pong host.

This is a bounded offline consumer replay.  It verifies the retained outer hash
chain and every native attestation, admits the immutable observations into the
versioned DEC26 host, and records no collector journal or native directive.  A
historical producer fingerprint must be supplied explicitly; the tool never
relabels it as the current observer build.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation  # noqa: E402
from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider  # noqa: E402
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY  # noqa: E402
from src.l3g_paper.pong_production import PongProductionHost  # noqa: E402
from src.l3g_paper.pong_scheduler import PongSchedulingConfig  # noqa: E402
from src.lane_iii.contracts import canonical_hash  # noqa: E402


ZERO_HASH = "0" * 64
PROFILE = "BEELZEBUB_PONG_DISCERNMENT_V1"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def replay(input_path: Path, output_path: Path, expected_source: str) -> dict[str, object]:
    if output_path.exists():
        raise ValueError("output must not already exist")
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    if len(expected_source) != 64 or any(c not in "0123456789abcdef" for c in expected_source):
        raise ValueError("expected-source must be a lowercase SHA-256 fingerprint")
    output_path.mkdir(parents=True)

    native_dispatches: list[object] = []
    runtime = {
        "observed_at": "2026-09-19T16:00:00Z",
        "runtime_active": False,
        "instrument": DEC26_TOPOLOGY.native_instrument,
        "canonical_contract": DEC26_TOPOLOGY.canonical_contract,
        "topology_version": DEC26_TOPOLOGY.version,
        "topology_hash": DEC26_TOPOLOGY.binding_hash,
        "state_namespace": DEC26_TOPOLOGY.state_namespace,
        "account_name": DEC26_TOPOLOGY.account_name,
        "account_class": DEC26_TOPOLOGY.account_class,
        "live_authority": DEC26_TOPOLOGY.live_authority,
        "live_capital": DEC26_TOPOLOGY.live_capital,
        "maximum_quantity": DEC26_TOPOLOGY.maximum_quantity,
        "addon_provenance_valid": False,
        "position_quantity": 0,
        "working_owned_orders": 0,
        "working_entry_orders": 0,
        "unresolved_execution": True,
        "reconciliation_current": False,
        "safety_lockout": True,
    }
    host = PongProductionHost(
        output_path / "host-state",
        selected_profile=PROFILE,
        expected_market_source_fingerprint=expected_source,
        runtime_snapshot=lambda: runtime,
        collector_status=lambda: {},
        controller_dispatch=lambda *_: False,
        native_dispatch=lambda directive, evidence: native_dispatches.append(
            {"directive": type(directive).__name__, "evidence": dict(evidence)}
        ) or {},
        activation_authorized=False,
        history_verification_key=LocalPaperSecretProvider().load_key(),
        scheduling=PongSchedulingConfig(),
        topology=DEC26_TOPOLOGY,
    )
    host.start()
    key = LocalPaperSecretProvider().load_key()
    previous = ZERO_HASH
    total = 0
    verified = 0
    contract_mismatches = 0
    counts: Counter[str] = Counter()
    producer_builds: Counter[str] = Counter()
    input_digest = hashlib.sha256()
    started = time.perf_counter()
    try:
        with input_path.open("rb") as stream:
            for raw_line in stream:
                if not raw_line.strip():
                    continue
                input_digest.update(raw_line)
                row = json.loads(raw_line)
                claimed = row.pop("record_hash", None)
                if (
                    row.get("schema") != "beelzebub-pong-authenticated-observation-record-v1"
                    or row.get("journal_sequence") != total + 1
                    or row.get("previous_record_hash") != previous
                ):
                    raise RuntimeError(f"JOURNAL_CHAIN_AT_{total + 1}")
                if canonical_hash(row) != claimed:
                    raise RuntimeError(f"JOURNAL_HASH_AT_{total + 1}")
                if row.get("wire_hash") != canonical_hash(row["observation"]):
                    raise RuntimeError(f"WIRE_HASH_AT_{total + 1}")
                observation = NinjaTraderObservation.from_wire(
                    json.dumps(row["observation"], sort_keys=True, separators=(",", ":")),
                ).verify_attestation(
                    key, expected_source_fingerprint=expected_source,
                )
                kind = observation.observation_type
                counts[kind] += 1
                producer_builds[observation.producer.build_fingerprint] += 1  # type: ignore[union-attr]
                if observation.payload.get("contract_id") != DEC26_TOPOLOGY.native_instrument:
                    contract_mismatches += 1
                # Keep admission bounded so a fast disk reader cannot turn this
                # offline run into an unbounded queue experiment.
                while host.status()["observation_queue_depth"] > 8192:
                    time.sleep(0.001)
                host.on_observation(observation)
                previous = claimed
                total += 1
                verified += 1
        while host.status()["observation_queue_depth"]:
            time.sleep(0.01)
    finally:
        host.stop(30)
    elapsed = time.perf_counter() - started
    host_status = host.status()
    result = {
        "schema": "beelzebub-pong-dec26-authenticated-replay-v1",
        "status": "PASS" if total == verified and contract_mismatches == 0 else "FAIL",
        "qualification": "NOT_QUALIFIED",
        "authority": "DISARMED",
        "native_runtime_started": False,
        "native_dispatches": len(native_dispatches),
        "input": {
            "path": str(input_path.resolve()),
            "sha256": input_digest.hexdigest(),
            "records": total,
            "verified_records": verified,
            "chain_tip": previous,
            "expected_historical_source_fingerprint": expected_source,
            "types": dict(counts),
            "producer_builds": dict(producer_builds),
            "contract_mismatches": contract_mismatches,
        },
        "topology": DEC26_TOPOLOGY.payload() | {"binding_hash": DEC26_TOPOLOGY.binding_hash},
        "consumer": {
            "host_status": host_status,
            "input_error": host_status.get("input_error"),
            "authoritative_admission_records": host_status["evaluation_schedule"][
                "authoritative_admission_records"
            ],
            "queue_peak": host_status.get("observation_queue_peak"),
        },
        "timing": {
            "wall_seconds": elapsed,
            "verified_records_per_second": total / elapsed if elapsed else None,
        },
        "safety": {
            "live_capital": "DENIED",
            "live_authority": "DISARMED",
            "execution_authority": "DENIED",
            "native_dispatches": len(native_dispatches),
        },
        "fresh_market_data_required_for": [
            "current observer source/build fingerprint congruency",
            "current DEC26 warm-start/history availability",
            "current account/position/order reconciliation",
            "current freshness, latency, queue, and durable-drain qualification",
        ],
        "generated_at": _now(),
    }
    (output_path / "result.json").write_text(
        json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8",
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--expected-source", required=True)
    args = parser.parse_args()
    result = replay(args.input.resolve(), args.output.resolve(), args.expected_source)
    print(json.dumps({
        "status": result["status"],
        "qualification": result["qualification"],
        "records": result["input"]["records"],
        "verified_records": result["input"]["verified_records"],
        "native_dispatches": result["native_dispatches"],
        "input_error": result["consumer"]["input_error"],
        "wall_seconds": result["timing"]["wall_seconds"],
        "result": str((args.output.resolve() / "result.json")),
    }, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
