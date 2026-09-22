#!/usr/bin/env python3
"""Evaluate like-for-like LSC v1 benchmark receipts without overstating 2,111/s.

This tool is offline and does not import the production codec, segment store,
runtime, or ledger. A benchmark driver records raw counts, elapsed times,
latencies, queue/storage/resource observations, and durability checks. This
tool derives stage rates and percentiles and applies the fixed acceptance gate.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import math
from pathlib import Path
from statistics import mean
import time
from typing import Callable, Iterator, Mapping, TypeVar


SCHEMA = "beelzebub-lsc-benchmark-receipt-v1"
REPORT_SCHEMA = "beelzebub-lsc-benchmark-report-v1"
ACCEPTANCE_RATE = 2_111.0
MINIMUM_SUSTAINED_RUNS = 3
MINIMUM_BURST_RUNS = 3
RATE_STAGES = ("ingress", "admission", "encode", "durable", "decode", "replay")
OTHER_STAGES = ("drain", "recovery", "full_verification")
RUN_CLASSES = frozenset({"sustained", "burst"})
HEX64 = frozenset("0123456789abcdef")
T = TypeVar("T")


class BenchmarkError(ValueError):
    pass


@dataclass
class OfflineStageRecorder:
    """Small instrumentation helper for an explicitly supplied offline action."""

    seconds: dict[str, float] = field(default_factory=dict)
    latency_ms: dict[str, list[float]] = field(default_factory=dict)

    @contextmanager
    def stage(self, name: str) -> Iterator[None]:
        if name in self.seconds:
            raise BenchmarkError(f"stage {name!r} was already measured")
        started = time.perf_counter_ns()
        try:
            yield
        finally:
            elapsed = (time.perf_counter_ns() - started) / 1_000_000_000
            self.seconds[name] = max(elapsed, 1e-9)

    def measure(self, name: str, operation: Callable[[], T]) -> T:
        started = time.perf_counter_ns()
        result = operation()
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        self.latency_ms.setdefault(name, []).append(elapsed_ms)
        return result


def _number(value: object, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BenchmarkError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0 or (positive and result <= 0):
        raise BenchmarkError(f"{label} is outside its finite bound")
    return result


def _integer(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise BenchmarkError(f"{label} must be a non-negative integer")
    return value


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise BenchmarkError(f"{label} must be an object")
    return value


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        raise BenchmarkError("latency sample set is empty")
    rank = max(1, math.ceil(percentile * len(ordered)))
    return ordered[rank - 1]


def _latencies(value: object, label: str) -> tuple[list[float], dict[str, float]]:
    if not isinstance(value, list) or not value:
        raise BenchmarkError(f"{label} must contain raw latency samples")
    samples = [_number(item, f"{label} sample") for item in value]
    return samples, {
        "sample_count": len(samples),
        "mean": mean(samples),
        "p50": _percentile(samples, 0.50),
        "p95": _percentile(samples, 0.95),
        "p99": _percentile(samples, 0.99),
        "maximum": max(samples),
    }


def _hex64(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(item not in HEX64 for item in value):
        raise BenchmarkError(f"{label} must be a lowercase SHA-256")
    return value


def evaluate_run(run: Mapping[str, object]) -> dict[str, object]:
    required = {
        "run_id", "run_class", "target_events_per_second", "pacing_seconds",
        "expected_events", "counts", "seconds", "latency_ms", "queue",
        "storage", "deltas", "resources", "durability", "configuration_identity",
        "corpus_sha256", "source_identity", "build_identity", "contract_identity",
        "hardware_identity", "storage_path_identity",
    }
    if set(run) != required:
        raise BenchmarkError(
            "benchmark run fields differ: missing="
            + ",".join(sorted(required - set(run)))
            + " extra=" + ",".join(sorted(set(run) - required))
        )
    run_id = run["run_id"]
    run_class = run["run_class"]
    if not isinstance(run_id, str) or not run_id or run_class not in RUN_CLASSES:
        raise BenchmarkError("run identity or class invalid")
    target = _number(run["target_events_per_second"], "target rate", positive=True)
    pacing = _number(run["pacing_seconds"], "pacing seconds", positive=True)
    expected = _integer(run["expected_events"], "expected events")
    if expected == 0:
        raise BenchmarkError("expected events must be positive")
    for field_name in (
        "configuration_identity", "source_identity", "build_identity",
        "contract_identity", "hardware_identity", "storage_path_identity",
    ):
        if not isinstance(run[field_name], str) or not run[field_name]:
            raise BenchmarkError(f"{field_name} must be non-empty text")
    corpus_hash = _hex64(run["corpus_sha256"], "corpus_sha256")

    counts = _mapping(run["counts"], "counts")
    seconds = _mapping(run["seconds"], "seconds")
    if set(counts) != set(RATE_STAGES) or set(seconds) != set(RATE_STAGES + OTHER_STAGES):
        raise BenchmarkError("stage counts/seconds do not have the exact required shape")
    normalized_counts = {stage: _integer(counts[stage], f"{stage} count") for stage in RATE_STAGES}
    normalized_seconds = {stage: _number(seconds[stage], f"{stage} seconds", positive=True) for stage in RATE_STAGES + OTHER_STAGES}
    rates = {stage: normalized_counts[stage] / normalized_seconds[stage] for stage in RATE_STAGES}

    latency = _mapping(run["latency_ms"], "latency_ms")
    if set(latency) != {"encode", "durable"}:
        raise BenchmarkError("latency_ms must contain exact encode and durable samples")
    _, encode_latency = _latencies(latency["encode"], "encode latency")
    _, durable_latency = _latencies(latency["durable"], "durable latency")

    queue = _mapping(run["queue"], "queue")
    queue_fields = {"start", "end", "high_water", "capacity", "max_residence_ms", "max_allowed_residence_ms"}
    if set(queue) != queue_fields:
        raise BenchmarkError("queue metrics do not have the exact required shape")
    queue_values = {
        "start": _integer(queue["start"], "queue start"),
        "end": _integer(queue["end"], "queue end"),
        "high_water": _integer(queue["high_water"], "queue high water"),
        "capacity": _integer(queue["capacity"], "queue capacity"),
        "max_residence_ms": _number(queue["max_residence_ms"], "queue residence"),
        "max_allowed_residence_ms": _number(queue["max_allowed_residence_ms"], "queue residence limit", positive=True),
    }

    storage = _mapping(run["storage"], "storage")
    storage_fields = {
        "legacy_database_bytes", "legacy_wal_bytes", "lsc_segment_bytes",
        "lsc_commitment_bytes", "raw_record_bytes", "compressed_segment_bytes",
        "disk_write_bytes", "disk_read_bytes", "disk_write_iops", "disk_read_iops",
        "legacy_database_growth_bytes", "legacy_wal_growth_bytes",
        "lsc_segment_growth_bytes", "lsc_commitment_growth_bytes",
    }
    if set(storage) != storage_fields:
        raise BenchmarkError("storage metrics do not have the exact required shape")
    storage_values = {key: _number(storage[key], f"storage {key}") for key in storage_fields}
    compressed = storage_values["compressed_segment_bytes"]
    compression_ratio = None if compressed == 0 else storage_values["raw_record_bytes"] / compressed
    legacy_total = storage_values["legacy_database_bytes"] + storage_values["legacy_wal_bytes"]
    lsc_total = storage_values["lsc_segment_bytes"] + storage_values["lsc_commitment_bytes"]

    deltas = _mapping(run["deltas"], "deltas")
    delta_fields = {"producer_overflow", "transport_failures", "sequence_gaps", "unexplained_loss"}
    if set(deltas) != delta_fields:
        raise BenchmarkError("loss deltas do not have the exact required shape")
    delta_values = {key: _integer(deltas[key], key) for key in delta_fields}

    resources = _mapping(run["resources"], "resources")
    resource_fields = {"cpu_seconds", "peak_memory_bytes"}
    if set(resources) != resource_fields:
        raise BenchmarkError("resource metrics do not have the exact required shape")
    resource_values = {key: _number(resources[key], key) for key in resource_fields}

    durability = _mapping(run["durability"], "durability")
    durability_fields = {
        "segment_chain_verified", "commitments_verified", "recovery_verified",
        "full_verification_passed", "drained", "writer_error", "final_replay_hash",
        "durability_method",
    }
    if set(durability) != durability_fields:
        raise BenchmarkError("durability metrics do not have the exact required shape")
    booleans = {
        key: durability[key]
        for key in (
            "segment_chain_verified", "commitments_verified", "recovery_verified",
            "full_verification_passed", "drained",
        )
    }
    if any(type(value) is not bool for value in booleans.values()):
        raise BenchmarkError("durability checks must be booleans")
    if durability["writer_error"] is not None and not isinstance(durability["writer_error"], str):
        raise BenchmarkError("writer_error must be null or text")
    final_hash = _hex64(durability["final_replay_hash"], "final_replay_hash")
    if not isinstance(durability["durability_method"], str) or not durability["durability_method"]:
        raise BenchmarkError("durability_method must be non-empty text")

    reasons: list[str] = []
    if target < ACCEPTANCE_RATE:
        reasons.append("TARGET_BELOW_2111")
    if any(value != expected for value in normalized_counts.values()):
        reasons.append("COUNT_OR_LOSS_MISMATCH")
    for stage, rate in rates.items():
        if rate < ACCEPTANCE_RATE:
            reasons.append(f"{stage.upper()}_RATE_BELOW_2111")
    if any(delta_values.values()):
        reasons.append("NONZERO_LOSS_DELTA")
    if queue_values["start"] != 0 or queue_values["end"] != 0:
        reasons.append("QUEUE_NOT_FLAT")
    if queue_values["capacity"] == 0 or queue_values["high_water"] > queue_values["capacity"]:
        reasons.append("QUEUE_CAPACITY_EXCEEDED")
    if queue_values["max_residence_ms"] > queue_values["max_allowed_residence_ms"]:
        reasons.append("QUEUE_RESIDENCE_EXCEEDED")
    if not all(booleans.values()) or durability["writer_error"] is not None:
        reasons.append("DURABILITY_OR_RECOVERY_UNPROVEN")
    if run_class == "sustained" and pacing < 60.0:
        reasons.append("SUSTAINED_DURATION_BELOW_60_SECONDS")

    return {
        "run_id": run_id,
        "run_class": run_class,
        "result": "PASS" if not reasons else "FAIL",
        "reasons": reasons,
        "target_events_per_second": target,
        "pacing_seconds": pacing,
        "expected_events": expected,
        "counts": normalized_counts,
        "rates_events_per_second": rates,
        "stage_seconds": normalized_seconds,
        "latency_ms": {"encode": encode_latency, "durable": durable_latency},
        "queue": {**queue_values, "growth": queue_values["end"] - queue_values["start"]},
        "storage": {
            **storage_values,
            "legacy_total_bytes": legacy_total,
            "lsc_total_bytes": lsc_total,
            "bytes_per_meve": lsc_total / expected,
            "compression_ratio": compression_ratio,
            "disk_write_bytes_per_second": storage_values["disk_write_bytes"] / pacing,
            "disk_read_bytes_per_second": storage_values["disk_read_bytes"] / pacing,
        },
        "deltas": delta_values,
        "resources": resource_values,
        "durability": {**booleans, "writer_error": durability["writer_error"], "final_replay_hash": final_hash, "durability_method": durability["durability_method"]},
        "identities": {
            "configuration": run["configuration_identity"], "corpus_sha256": corpus_hash,
            "source": run["source_identity"], "build": run["build_identity"],
            "contract": run["contract_identity"], "hardware": run["hardware_identity"],
            "storage_path": run["storage_path_identity"],
        },
    }


def evaluate_receipt(value: Mapping[str, object]) -> dict[str, object]:
    if set(value) != {"schema", "runs"} or value.get("schema") != SCHEMA or not isinstance(value.get("runs"), list):
        raise BenchmarkError("benchmark receipt schema or shape invalid")
    if not value["runs"] or not all(isinstance(run, Mapping) for run in value["runs"]):
        raise BenchmarkError("benchmark receipt contains no valid runs")
    evaluated = [evaluate_run(run) for run in value["runs"]]  # type: ignore[arg-type]
    ids = [run["run_id"] for run in evaluated]
    if len(set(ids)) != len(ids):
        raise BenchmarkError("benchmark run IDs are not unique")
    identity_keys = ("configuration", "corpus_sha256", "source", "build", "contract", "hardware", "storage_path")
    identity_consistent = all(
        all(run["identities"][key] == evaluated[0]["identities"][key] for key in identity_keys)  # type: ignore[index]
        for run in evaluated
    )
    sustained = [run for run in evaluated if run["run_class"] == "sustained"]
    burst = [run for run in evaluated if run["run_class"] == "burst"]
    enough = len(sustained) >= MINIMUM_SUSTAINED_RUNS and len(burst) >= MINIMUM_BURST_RUNS
    all_pass = all(run["result"] == "PASS" for run in evaluated)
    acceptance = enough and all_pass and identity_consistent
    blockers: list[str] = []
    if not enough:
        blockers.append("REQUIRES_AT_LEAST_3_SUSTAINED_AND_3_BURST_RUNS")
    if not all_pass:
        blockers.append("ONE_OR_MORE_RUNS_FAILED_ZERO_LOSS_STABILITY_RATE_OR_DURABILITY")
    if not identity_consistent:
        blockers.append("LIKE_FOR_LIKE_IDENTITIES_DIFFER")
    return {
        "schema": REPORT_SCHEMA,
        "acceptance_target_events_per_second": ACCEPTANCE_RATE,
        "claim": "PASS_2111" if acceptance else "NOT_PROVEN_2111",
        "accepted": acceptance,
        "blockers": blockers,
        "run_count": len(evaluated),
        "sustained_run_count": len(sustained),
        "burst_run_count": len(burst),
        "identity_consistent": identity_consistent,
        "runs": evaluated,
    }


def _strict_load(path: Path) -> Mapping[str, object]:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                raise BenchmarkError("duplicate JSON key")
            result[key] = item
        return result
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=hook)
    if not isinstance(value, Mapping):
        raise BenchmarkError("benchmark receipt must be an object")
    return value


def _write_new(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        handle.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        raw = args.receipt.read_bytes()
        report = evaluate_receipt(_strict_load(args.receipt))
        report["receipt_sha256"] = hashlib.sha256(raw).hexdigest()
        _write_new(args.output, report)
        return 0 if report["accepted"] else 2
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"result": "ERROR", "error": type(exc).__name__, "detail": str(exc)}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
