#!/usr/bin/env python3
"""Compare current DEC26 periodic Pong with and without FHB, fully disarmed.

The retained journal is immutable input.  Both passes authenticate the same
bounded prefix, run the production host synchronously on a deterministic QPC
grid, and deny native dispatch.  This is offline correctness/resource evidence,
never fresh-data qualification or paper authority.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import gc
import hashlib
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import time
import tracemalloc
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation  # noqa: E402
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION  # noqa: E402
from src.l3g_paper.fhb import FHBConfig  # noqa: E402
from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider  # noqa: E402
from src.l3g_paper.pong import PongState  # noqa: E402
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY  # noqa: E402
from src.l3g_paper.pong_production import PongProductionHost  # noqa: E402
from src.l3g_paper.pong_scheduler import (  # noqa: E402
    PongSchedulingConfig,
    PongSchedulingMode,
)
from src.lane_iii.contracts import canonical_hash  # noqa: E402


ZERO_HASH = "0" * 64


class ReplayClock:
    def __init__(self) -> None:
        self.now_ns = 0
        self._origin: datetime | None = None
        self._latest = "2026-09-18T00:00:00Z"

    def qpc(self) -> int:
        return self.now_ns

    def wall_now(self) -> str:
        return self._latest

    def admit(self, receipt: str) -> None:
        value = datetime.fromisoformat(receipt.replace("Z", "+00:00"))
        if self._origin is None:
            self._origin = value
        elapsed = max(0.0, (value - self._origin).total_seconds())
        self.now_ns = int(elapsed * 1_000_000_000)
        self._latest = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_prefix(path: Path, maximum_records: int) -> tuple[list[bytes], dict[str, object]]:
    lines: list[bytes] = []
    digest = hashlib.sha256()
    previous = ZERO_HASH
    producer_sessions: Counter[str] = Counter()
    with path.open("rb") as stream:
        for raw_line in stream:
            if not raw_line.strip():
                continue
            if len(lines) >= maximum_records:
                break
            row = json.loads(raw_line)
            claimed = row.pop("record_hash", None)
            ordinal = len(lines) + 1
            if (
                row.get("schema") != "beelzebub-pong-authenticated-observation-record-v1"
                or row.get("journal_sequence") != ordinal
                or row.get("previous_record_hash") != previous
            ):
                raise RuntimeError(f"JOURNAL_CHAIN_AT_{ordinal}")
            if canonical_hash(row) != claimed:
                raise RuntimeError(f"JOURNAL_HASH_AT_{ordinal}")
            if row.get("wire_hash") != canonical_hash(row["observation"]):
                raise RuntimeError(f"WIRE_HASH_AT_{ordinal}")
            session = str(row["observation"].get("session_id") or "")
            producer_sessions[session] += 1
            digest.update(raw_line)
            lines.append(raw_line)
            previous = str(claimed)
    if not lines:
        raise ValueError("FHB_REPLAY_INPUT_EMPTY")
    return lines, {
        "path": str(path.resolve()),
        "bounded_prefix_records": len(lines),
        "bounded_prefix_sha256": digest.hexdigest(),
        "bounded_prefix_chain_tip": previous,
        "producer_sessions": dict(producer_sessions),
    }


def _runtime(now: str) -> dict[str, object]:
    return {
        "observed_at": now,
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
        "unresolved_execution": False,
        "reconciliation_current": True,
        "safety_lockout": True,
    }


def _run_pass(
    lines: list[bytes],
    *,
    expected_source: str,
    key: bytes,
    focused: bool,
) -> dict[str, object]:
    clock = ReplayClock()
    native_dispatches: list[object] = []
    counts: Counter[str] = Counter()
    with TemporaryDirectory(prefix="pong-fhb-periodic-") as state_root:
        runtime = _runtime(clock.wall_now())
        host = PongProductionHost(
            Path(state_root),
            selected_profile=PONG_ENTRY_PROFILE_VERSION,
            expected_market_source_fingerprint=expected_source,
            runtime_snapshot=lambda: runtime,
            collector_status=lambda: {},
            controller_dispatch=lambda *_: False,
            native_dispatch=lambda *args: native_dispatches.append(args) or {},
            activation_authorized=False,
            fhb_config=FHBConfig.periodic_protected() if focused else None,
            scheduling=PongSchedulingConfig(
                PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
                20,
            ),
            clock_ns=clock.qpc,
            monotonic_clock=lambda: clock.now_ns / 1_000_000_000,
            topology=DEC26_TOPOLOGY,
        )
        host._runtime = runtime
        host._bridge = SimpleNamespace(
            machine=SimpleNamespace(
                state=PongState.FLAT_WAIT_ALIGNMENT,
                actual_quantity=0,
                pending_action=None,
            ),
            status=lambda: {
                "state": "SYNTHETIC_FLAT_NON_DISPATCHING_REPLAY_ONLY",
            },
        )
        # The comparison owns only analytical accounting.  No durable bridge or
        # native execution seam is constructed in this non-dispatching harness.
        host._apply_discernment_update = lambda _update: None  # type: ignore[method-assign]
        verified = 0
        tracemalloc.start()
        wall_started = time.perf_counter()
        cpu_started = time.process_time()
        with patch("src.l3g_paper.pong_production._now", clock.wall_now):
            for raw_line in lines:
                row = json.loads(raw_line)
                observation = NinjaTraderObservation.from_wire(
                    json.dumps(
                        row["observation"],
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                ).verify_attestation(
                    key,
                    expected_source_fingerprint=expected_source,
                )
                clock.admit(observation.ninja_receipt_time)
                runtime["observed_at"] = clock.wall_now()
                host._consume_observation(
                    observation,
                    admitted_monotonic=clock.now_ns / 1_000_000_000,
                )
                host._service_periodic_deadline(clock.now_ns)
                counts[observation.observation_type] += 1
                verified += 1
        cpu_seconds = time.process_time() - cpu_started
        wall_seconds = time.perf_counter() - wall_started
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        status = host.status()
        return {
            "mode": "FHB_TOP10_5S_32T_PERIODIC20" if focused else "CURRENT_PERIODIC20_BASELINE",
            "verified_records": verified,
            "observation_counts": dict(counts),
            "wall_seconds": wall_seconds,
            "cpu_seconds": cpu_seconds,
            "peak_tracemalloc_bytes": peak,
            "records_per_wall_second_diagnostic_only": verified / wall_seconds,
            "input_error": status["input_error"],
            "stream_counts": status["stream_counts"],
            "missing_events": status["missing_events"],
            "out_of_order_events": status["out_of_order_events"],
            "source_identity": status["source_identity"],
            "evaluation_schedule": status["evaluation_schedule"],
            "microstructure_pipeline": status["microstructure_pipeline"],
            "fhb": status["fhb"],
            "fhb_freshness": status["fhb_freshness"],
            "native_dispatches": len(native_dispatches),
            "queue_peak": 0,
            "queue_final": 0,
            "cleanup": "TEMPORARY_STATE_REMOVED_AFTER_STATUS_CAPTURE",
        }


def compare(
    input_path: Path,
    output_path: Path,
    expected_source: str,
    maximum_records: int,
) -> dict[str, object]:
    if output_path.exists():
        raise ValueError("output must not already exist")
    if maximum_records <= 0:
        raise ValueError("maximum-records must be positive")
    lines, input_receipt = _load_prefix(input_path, maximum_records)
    key = LocalPaperSecretProvider().load_key()
    baseline = _run_pass(
        lines,
        expected_source=expected_source,
        key=key,
        focused=False,
    )
    gc.collect()
    focused = _run_pass(
        lines,
        expected_source=expected_source,
        key=key,
        focused=True,
    )
    fhb = focused["fhb"] or {}
    result = {
        "schema": "beelzebub-fhb-current-periodic-comparison-v1",
        "status": "PASS" if (
            baseline["verified_records"] == focused["verified_records"]
            and baseline["stream_counts"] == focused["stream_counts"]
            and baseline["missing_events"] == focused["missing_events"]
            and baseline["out_of_order_events"] == focused["out_of_order_events"]
            and baseline["native_dispatches"] == focused["native_dispatches"] == 0
            and int(fhb.get("eligible_evaluations") or 0) > 0
        ) else "FAIL",
        "qualification": "NOT_QUALIFIED_HISTORICAL_RETAINED_REPLAY",
        "authority": "DISARMED_NON_DISPATCHING_HARNESS",
        "input": input_receipt,
        "topology": DEC26_TOPOLOGY.payload() | {
            "binding_hash": DEC26_TOPOLOGY.binding_hash,
        },
        "effective_configuration": {
            "pong_profile": PONG_ENTRY_PROFILE_VERSION,
            "fhb_profile": FHBConfig.periodic_protected().profile_id,
            "core_levels_per_side": 10,
            "shadow_history_seconds": 5,
            "movement_reserve_ticks": 32,
            "scheduling_mode": "PERIODIC_FLAT_PROTECTED",
            "periodic_interval_ms": 20,
            "fhb_entry_policy_id": None,
            "fhb_entry_evaluation_interval_ms": 0,
            "fhb_qualification_policy_id": (
                FHBConfig.periodic_protected().qualification_policy_id
            ),
            "fhb_minimum_observed_event_rate": "NONE",
            "entry_scoring_owner": "PONG_PERIODIC_DEADLINE",
            "transport_reduction": "NONE",
            "compute_workers": 0,
        },
        "passes": [baseline, focused],
        "comparison": {
            "same_verified_records": baseline["verified_records"] == focused["verified_records"],
            "same_stream_counts": baseline["stream_counts"] == focused["stream_counts"],
            "same_gap_counts": (
                baseline["missing_events"] == focused["missing_events"]
                and baseline["out_of_order_events"] == focused["out_of_order_events"]
            ),
            "same_source_identity": baseline["source_identity"] == focused["source_identity"],
            "native_dispatches_both_zero": (
                baseline["native_dispatches"] == focused["native_dispatches"] == 0
            ),
            "fhb_eligible_evaluations": fhb.get("eligible_evaluations"),
            "fhb_out_of_core_skips": fhb.get("out_of_core_skips"),
            "fhb_rewarm_required_events": fhb.get("rewarm_required_events"),
            "wall_seconds_delta_fhb_minus_baseline": (
                focused["wall_seconds"] - baseline["wall_seconds"]
            ),
            "cpu_seconds_delta_fhb_minus_baseline": (
                focused["cpu_seconds"] - baseline["cpu_seconds"]
            ),
            "peak_memory_delta_bytes_fhb_minus_baseline": (
                focused["peak_tracemalloc_bytes"] - baseline["peak_tracemalloc_bytes"]
            ),
        },
        "limitations": [
            "retained historical input, not fresh native qualification",
            "synchronous deterministic QPC grid, not live scheduler latency",
            "no collector disk writes or paper ledger durability measurement",
            "no runtime bridge or order-capable native process",
        ],
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("xb") as handle:
        handle.write(
            (json.dumps(result, sort_keys=True, indent=2) + "\n").encode("utf-8")
        )
        handle.flush()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--maximum-records", type=int, default=16016)
    args = parser.parse_args()
    result = compare(
        args.input.resolve(),
        args.output.resolve(),
        args.expected_source,
        args.maximum_records,
    )
    print(json.dumps({
        "status": result["status"],
        "qualification": result["qualification"],
        "records": result["input"]["bounded_prefix_records"],
        "fhb_eligible_evaluations": result["comparison"]["fhb_eligible_evaluations"],
        "fhb_out_of_core_skips": result["comparison"]["fhb_out_of_core_skips"],
        "output": str(args.output.resolve()),
    }, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
