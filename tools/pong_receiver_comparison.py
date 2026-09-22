"""Offline real-socket receiver comparison; never commissions a native producer.

Uses the production dispatch, listener, collector, host, shadow and paper ingest.
The paper execution transport is deliberately absent. Historical input remains
historical, and synthetic input is explicitly incapable of qualification.
"""
from __future__ import annotations

import argparse
from bisect import bisect_left
from collections import Counter
from datetime import datetime, timezone, timedelta
import hashlib
import hmac
import json
import multiprocessing as mp
from pathlib import Path
import socket
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.copytrade.control_center import (
    _dispatch_pong_observation,
    _ObservationIngressMetrics,
    _qualifying_market_fast_path_identity,
)
from src.l3f_provider.ninjatrader_commission import NinjaTraderListenerWorker
from src.l3f_provider.ninjatrader_observation import (
    LoopbackBridgeConfig, NinjaTraderObservation, expected_observer_source_fingerprint,
    NINJATRADER_ATTESTATION_KEY_CONTEXT, NINJATRADER_ATTESTATION_PROTOCOL, L3F3_SCHEMA,
)
from src.l3f_provider.shadow_runtime import LaneIIIShadowRuntime
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION, resolve_paper_profile, paper_policy_for_topology, paper_risk_for_topology
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider
from src.l3g_paper.pong_collection import PongAuthenticHourCollector
from src.l3g_paper.pong_contract import DEC26_TOPOLOGY
from src.l3g_paper.pong_production import PongProductionHost
from src.l3g_paper.pong_qualifying import (
    PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
    PONG_QUALIFYING_INTERVAL_MS,
    PONG_QUALIFYING_SCHEDULING_MODE,
)
from src.l3g_paper.pong_scheduler import PongSchedulingConfig, PongSchedulingMode
from src.l3g_paper.runtime import LaneIIIPaperRuntime, ObservationFanout
from src.lane_iii.contracts import canonical_hash
from tools.pong_periodic_support import BenchmarkLease

TEST_KEY = b"receiver-comparison-offline-only-test-key"
_SOCKET_TIMING_BUCKETS_MS = (
    0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0,
    50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
    10_000.0, 30_000.0,
)


def dump(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def wire(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def new_bounded_timing():
    return {
        "count": 0,
        "total_seconds": 0.0,
        "maximum_seconds": 0.0,
        "histogram_counts": [0] * (len(_SOCKET_TIMING_BUCKETS_MS) + 1),
    }


def record_bounded_timing(timing, elapsed_seconds):
    elapsed = max(0.0, elapsed_seconds)
    timing["count"] += 1
    timing["total_seconds"] += elapsed
    timing["maximum_seconds"] = max(timing["maximum_seconds"], elapsed)
    elapsed_ms = elapsed * 1000.0
    bucket = bisect_left(_SOCKET_TIMING_BUCKETS_MS, elapsed_ms)
    timing["histogram_counts"][bucket] += 1


def bounded_timing_report(timing):
    def percentile_upper_bound(percentile):
        if not timing["count"]:
            return 0.0
        rank = max(1, int(timing["count"] * percentile + 0.999999999))
        cumulative = 0
        for index, count in enumerate(timing["histogram_counts"]):
            cumulative += count
            if cumulative >= rank:
                if index < len(_SOCKET_TIMING_BUCKETS_MS):
                    return _SOCKET_TIMING_BUCKETS_MS[index]
                return round(timing["maximum_seconds"] * 1000.0, 3)
        return round(timing["maximum_seconds"] * 1000.0, 3)

    count = timing["count"]
    return {
        "count": count,
        "total_milliseconds": round(timing["total_seconds"] * 1000.0, 3),
        "mean_milliseconds": round(timing["total_seconds"] * 1000.0 / count, 3) if count else 0.0,
        "maximum_milliseconds": round(timing["maximum_seconds"] * 1000.0, 3),
        "p50_milliseconds": percentile_upper_bound(0.50),
        "p95_milliseconds": percentile_upper_bound(0.95),
        "p99_milliseconds": percentile_upper_bound(0.99),
        "quantile_method": "FIXED_HISTOGRAM_UPPER_BOUND",
        "histogram_upper_bounds_milliseconds": [*_SOCKET_TIMING_BUCKETS_MS, "overflow"],
        "histogram_counts": list(timing["histogram_counts"]),
    }


def corpus(args):
    frames, dues = [], []
    if args.frames:
        frames = args.frames.read_bytes().splitlines(keepends=True)[:args.records]
        source = json.loads(frames[0])["producer"]["source_fingerprint"]
        for raw in frames:
            NinjaTraderObservation.from_wire(raw).verify_attestation(TEST_KEY, expected_source_fingerprint=source)
        return frames, [i/args.rate for i in range(len(frames))], TEST_KEY, source, {"kind": "SYNTHETIC_CLEAN_RECEIVER_SCREEN", "path": str(args.frames), "qualification": "INELIGIBLE", "not_representative_capacity_corpus": True, "fixed_synthetic_timestamps_are_not_fresh_native_evidence": True}
    if args.journal:
        key = LocalPaperSecretProvider().load_key()
        previous = "0" * 64
        source = None
        first = None
        digest = hashlib.sha256()
        with args.journal.open("rb") as stream:
            for index, line in enumerate(stream, 1):
                row = json.loads(line)
                claimed = row.pop("record_hash")
                assert row["journal_sequence"] == index and row["previous_record_hash"] == previous
                assert canonical_hash(row) == claimed
                assert canonical_hash(row["observation"]) == row["wire_hash"]
                source = source or row["observation"]["producer"]["source_fingerprint"]
                raw = wire(row["observation"])
                obs = NinjaTraderObservation.from_wire(raw).verify_attestation(key, expected_source_fingerprint=source)
                at = datetime.fromisoformat(obs.ninja_receipt_time.replace("Z", "+00:00")).timestamp()
                first = at if first is None else first
                dues.append(max(0, at - first))
                frames.append(raw + b"\n")
                digest.update(line)
                previous = claimed
                if len(frames) >= args.records:
                    break
        return frames, dues, key, source, {"kind": "AUTHENTIC_HISTORICAL_FAILED_EPOCH_DIAGNOSTIC", "path": str(args.journal), "prefix_sha256": digest.hexdigest(), "prefix_tip": previous, "qualification": "INELIGIBLE"}
    # Deterministic clean 80/10/10 mix. This is a receiver service screen, not
    # the historical representative capacity corpus or a natural native feed.
    source = expected_observer_source_fingerprint()
    derived = hmac.new(TEST_KEY, NINJATRADER_ATTESTATION_KEY_CONTEXT, hashlib.sha256).digest()
    counters = Counter()
    start = datetime.now(timezone.utc) + timedelta(seconds=10)
    for i in range(args.records):
        kind = "QUOTE" if i % 10 == 0 else "TRADE" if i % 10 == 1 else "DEPTH"
        counters[kind] += 1
        due = (i // 8444) * 4 + (i % 8444) / 4222 if args.mode == "burst" else i / args.rate
        at = (start + timedelta(seconds=due)).isoformat().replace("+00:00", "Z")
        payload = {"contract_id": "MNQ DEC26", "source_generation": 1, "source_event_sequence": counters[kind], "observer_queue_overflow_frames": 0, "observer_transport_failed_frames": 0}
        if kind == "DEPTH":
            policy, stream = "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1", "MARKET_DEPTH"
            payload.update(side="Bid", operation="Update", mutation_price=100, mutation_resolved_price=100, mutation_position=0, mutation_volume=1, is_reset=False, status="AVAILABLE", reason=None,
                bids=[{"price": 100-j*.25, "size": j+1} for j in range(10)], asks=[{"price": 100.25+j*.25, "size": j+1} for j in range(10)])
        elif kind == "QUOTE":
            policy, stream = "EVENT_DRIVEN_EVERY_BID_ASK_CALLBACK_V1", "MARKET_DATA_BID_ASK"
            payload.update(bid=100, ask=100.25, bid_size=1, ask_size=1)
        else:
            policy, stream = "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1", "MARKET_DATA_LAST"
            payload.update(price=100.25, size=1, aggressor_side="BUY", aggressor_source="BID_ASK_CLASSIFICATION", bid_at_trade=100, ask_at_trade=100.25, derivation_quote_observation_id=None)
        payload["publication_policy"] = policy
        value = dict(schema=L3F3_SCHEMA, observation_id=f"offline-receiver-{i+1}", session_id="offline-receiver-test", observation_type=kind, ninja_receipt_time=at, local_monotonic_sequence=i+1, provider_timestamp=at, provider_sequence=None, exchange_timestamp=None, account=None, payload=payload, payload_hash=canonical_hash(payload), producer=dict(protocol_version=NINJATRADER_ATTESTATION_PROTOCOL, source_fingerprint=source, build_fingerprint="e"*64, build_timestamp="2026-09-21T00:00:00Z", source_stream=stream, publication_policy=policy))
        value["signature"] = hmac.new(derived, wire(value), hashlib.sha256).hexdigest()
        frames.append(wire(value)+b"\n")
        dues.append(due)
    return frames, dues, TEST_KEY, source, {"kind": "SYNTHETIC_CLEAN_RECEIVER_SCREEN", "mix": dict(counters), "qualification": "INELIGIBLE", "not_representative_capacity_corpus": True}


def sender(port, frames, dues, ready, go, result_path):
    writes = new_bounded_timing()
    late = new_bounded_timing()
    result = {"sent": 0, "bytes": 0}
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
            conn.settimeout(30)
            ready.set()
            if not go.wait(30):
                raise TimeoutError("sender start")
            epoch = time.perf_counter()
            result["start_monotonic"] = epoch
            for raw, due in zip(frames, dues):
                remaining = epoch + due - time.perf_counter()
                if remaining > 0:
                    time.sleep(remaining)
                before = time.perf_counter()
                record_bounded_timing(late, before-epoch-due)
                conn.sendall(raw)
                record_bounded_timing(writes, time.perf_counter()-before)
                result["sent"] += 1
                result["bytes"] += len(raw)
            result["end_monotonic"] = time.perf_counter()
            result["sender_elapsed_seconds"] = result["end_monotonic"] - epoch
            result["sender_schedule_overrun_seconds"] = max(
                0.0, result["sender_elapsed_seconds"] - dues[-1],
            )
            # Receiver must finish before peer disconnect changes host safety.
            conn.shutdown(socket.SHUT_WR)
    except Exception as exc:
        result["error"] = repr(exc)
    write_report = bounded_timing_report(writes)
    late_report = bounded_timing_report(late)
    result.update(
        socket_write_total_seconds=writes["total_seconds"],
        socket_write_max_seconds=writes["maximum_seconds"],
        release_lateness_max_seconds=late["maximum_seconds"],
        socket_write_distribution=write_report,
        release_lateness_distribution=late_report,
    )
    dump(Path(result_path), result)


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    frames, dues, key, source, provenance = corpus(args)
    (args.output/"input-frames.jsonl").write_bytes(b"".join(frames))
    dump(args.output/"input.json", {**provenance, "records": len(frames), "wire_bytes_sha256": hashlib.sha256(b"".join(frames)).hexdigest(), "native_source": source, "native_sender_metrics": "UNAVAILABLE_OFFLINE_SOCKET_SENDER", "timing_mode": args.mode})
    if args.mode != "natural":
        dues = [(i//8444)*4+(i%8444)/4222 if args.mode == "burst" else i/args.rate for i in range(len(frames))]
    profile = resolve_paper_profile(PONG_ENTRY_PROFILE_VERSION)
    ledger = PaperLedger(args.output/"epoch-offline"/"paper.sqlite3", policy=paper_policy_for_topology(profile.policy, DEC26_TOPOLOGY), risk=paper_risk_for_topology(profile.risk, DEC26_TOPOLOGY))
    fast_path_identity = _qualifying_market_fast_path_identity(
        architecture=PONG_NO_FHB_QUALIFYING_ARCHITECTURE,
        selected_profile=PONG_ENTRY_PROFILE_VERSION,
        paper_topology_version=DEC26_TOPOLOGY.version,
        pong_topology_version=DEC26_TOPOLOGY.version,
        scheduling_mode=PONG_QUALIFYING_SCHEDULING_MODE,
        interval_ms=PONG_QUALIFYING_INTERVAL_MS,
        activation_authorized=False,
    )
    assert fast_path_identity is not None
    paper = LaneIIIPaperRuntime(
        ledger,
        qualifying_market_fast_path=(
            fast_path_identity if args.paper_path == "qualifying-fast" else None
        ),
    )
    shadow = LaneIIIShadowRuntime()
    collector = PongAuthenticHourCollector(args.output/"collector", expected_source_fingerprint=source, topology=DEC26_TOPOLOGY)
    host = PongProductionHost(args.output/"host", selected_profile=PONG_ENTRY_PROFILE_VERSION, expected_market_source_fingerprint=source, runtime_snapshot=paper.pong_controller_snapshot, collector_status=collector.status, controller_dispatch=lambda *_: False, native_dispatch=lambda *_: (_ for _ in ()).throw(RuntimeError("OFFLINE_NATIVE_DISPATCH_FORBIDDEN")), activation_authorized=False, history_verification_key=key, qualifying_architecture=PONG_NO_FHB_QUALIFYING_ARCHITECTURE, scheduling=PongSchedulingConfig(mode=PongSchedulingMode.PERIODIC_FLAT_PROTECTED, interval_ms=20), topology=DEC26_TOPOLOGY)
    stages = {name: {"count": 0, "total_seconds": 0., "max_seconds": 0.} for name in ("shadow", "paper")}
    delivery = {name: hashlib.sha256() for name in ("paper", "receiver")}
    completed = []
    def measured(name, fn):
        def apply(obs):
            before = time.perf_counter()
            fn(obs)
            duration = time.perf_counter()-before
            row = stages[name]
            row["count"] += 1
            row["total_seconds"] += duration
            row["max_seconds"] = max(row["max_seconds"], duration)
            if name in delivery:
                delivery[name].update((obs.wire_hash()+"\n").encode())
        return apply
    paper_ingest = (
        paper.ingest_qualifying_disarmed_pong_market
        if args.paper_path == "qualifying-fast"
        else paper.ingest
    )
    fanout = ObservationFanout(
        shadow_observation=measured("shadow", shadow.ingest),
        shadow_transport=shadow.on_transport_state,
        shadow_rejection=shadow.record_raw_rejection,
        shadow_duplicate=shadow.record_raw_duplicate,
        paper_observation=measured("paper", paper_ingest),
        paper_transport=paper.on_observation_transport_state,
        paper_rejection=paper.on_observation_rejection,
        paper_duplicate=paper.on_observation_duplicate,
        record_failure=paper.record_sink_failure,
    )
    metrics = _ObservationIngressMetrics()
    def dispatch(obs):
        _dispatch_pong_observation(obs, collector=collector, pong_host=host, fanout=fanout, metrics=metrics, independent=False, profile=PONG_ENTRY_PROFILE_VERSION, activation_authorized=False, qualifying_architecture=PONG_NO_FHB_QUALIFYING_ARCHITECTURE if args.route == "candidate" else None)
        delivery["receiver"].update((obs.wire_hash()+"\n").encode())
        completed.append(time.perf_counter())
    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        port = reserve.getsockname()[1]
    listener = NinjaTraderListenerWorker(LoopbackBridgeConfig(port=port), attestation_key=key, require_attestation=True, expected_source_fingerprint=source)
    def transport(state):
        collector.on_transport_state(state)
        fanout.on_transport_state(state)
    def rejection(error):
        collector.on_rejection(error)
        fanout.on_rejection(error)
    def duplicate():
        collector.on_duplicate()
        fanout.on_duplicate()
    listener.set_observation_sinks(on_observation=dispatch, on_local_bridge_state=transport, on_rejection=rejection, on_duplicate=duplicate, fail_on_observation_error=args.route == "candidate")
    collector.start()
    host.start()
    assert listener.start().state == "LISTENING"
    ctx = mp.get_context("spawn")
    ready, go = ctx.Event(), ctx.Event()
    proc = ctx.Process(target=sender, args=(port, frames, dues, ready, go, str(args.output/"sender.json")))
    proc.start()
    if not ready.wait(20):
        raise RuntimeError("OFFLINE_SENDER_NOT_READY")
    receiver_go_monotonic = time.perf_counter()
    go.set()
    samples = []
    deadline = receiver_go_monotonic + dues[-1] + 120
    collector_at_receiver_completion = None
    host_at_receiver_completion = None
    shutdown_timings = {}
    try:
        while len(completed) < len(frames) and time.perf_counter() < deadline:
            now = time.perf_counter()
            # Bounded cheap counters only; no expensive status calls while timed.
            samples.append({"t": now-receiver_go_monotonic, "completed": len(completed), "host_queue": host._observations.qsize(), "collector_queue": collector._queue.qsize()})
            if not proc.is_alive() and (args.output/"sender.json").exists() and json.loads((args.output/"sender.json").read_text()).get("error"):
                break
            time.sleep(.25)
    finally:
        proc.join(35)
        if proc.is_alive():
            raise RuntimeError("OFFLINE_SENDER_SHUTDOWN_UNPROVEN")
        # These snapshots happen only after the timed receiver loop. They make
        # writer/host backlog at receiver completion explicit without adding a
        # status serialization call to any market callback.
        collector_at_receiver_completion = collector.status()
        host_at_receiver_completion = host.status()
        phase_started = time.perf_counter()
        listener_status = listener.stop(timeout_seconds=15).as_dict()
        shutdown_timings["listener_stop"] = time.perf_counter() - phase_started
        phase_started = time.perf_counter()
        host.stop(30)
        shutdown_timings["host_drain_and_stop"] = time.perf_counter() - phase_started
        phase_started = time.perf_counter()
        collector_status = collector.stop(timeout_seconds=30)
        shutdown_timings["collector_drain_and_stop"] = time.perf_counter() - phase_started
        paper_fast_path = paper.pong_controller_snapshot()["qualifying_market_fast_path"]
        phase_started = time.perf_counter()
        paper.stop()
        ledger_close = ledger.close()
        shutdown_timings["paper_and_ledger_close"] = time.perf_counter() - phase_started
    ended = completed[-1] if completed else time.perf_counter()
    expected = hashlib.sha256()
    for frame in frames:
        expected.update((NinjaTraderObservation.from_wire(frame).wire_hash()+"\n").encode())
    retained = hashlib.sha256()
    count = 0
    previous = "0"*64
    for line in collector.journal_path.read_bytes().splitlines():
        row = json.loads(line)
        claimed = row.pop("record_hash")
        assert row["previous_record_hash"] == previous and canonical_hash(row) == claimed
        previous = claimed
        retained.update((row["wire_hash"]+"\n").encode())
        count += 1
    with sqlite3.connect(f"file:{ledger.path.as_posix()}?mode=ro", uri=True) as db:
        kinds = dict(db.execute("SELECT kind,count(*) FROM lane_iii_paper_audit GROUP BY kind"))
    source_hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/"src/copytrade/control_center.py", ROOT/"src/l3f_provider/ninjatrader_commission.py", ROOT/"src/l3g_paper/pong_collection.py", ROOT/"src/l3g_paper/runtime.py", Path(__file__)]}
    sender_result = json.loads((args.output/"sender.json").read_text())
    sender_started = sender_result["start_monotonic"]
    receiver_elapsed = ended - receiver_go_monotonic
    receiver_clock = {
        "clock": "PARENT_PROCESS_PERF_COUNTER",
        "go_to_first_completion_seconds": (
            None if not completed else completed[0] - receiver_go_monotonic
        ),
        "go_to_last_completion_seconds": receiver_elapsed,
        "first_to_last_completion_seconds": (
            0.0 if len(completed) < 2 else completed[-1] - completed[0]
        ),
        "scheduled_span_seconds": dues[-1],
        "schedule_overrun_seconds": max(0.0, receiver_elapsed - dues[-1]),
    }
    result = dict(schema="pong-offline-receiver-comparison-v3", route=args.route, paper_path=args.paper_path, mode=args.mode, offered_rate_per_second=(None if args.mode == "natural" else args.rate), offered_rate_authority="DIAGNOSTIC_ONLY_NOT_AN_ACCEPTANCE_THRESHOLD", qualification="NOT_QUALIFIED", execution_transport="ABSENT", healthy_flat_analysis_proof=False, source_freshness="NOT_CURRENT_NATIVE_EVIDENCE", provenance=provenance, loaded_source_sha256=source_hashes, records=len(frames), completed=len(completed), window_monotonic=[sender_started, ended], elapsed_seconds=ended-sender_started, completion_rate=len(completed)/(ended-sender_started), scheduled_span_seconds=dues[-1], receiver_end_lag_seconds=max(0, ended-sender_started-dues[-1]), receiver_local_timing=receiver_clock, sender=sender_result, consumers=stages, ingress=metrics.snapshot(), listener=listener_status, host=host.status(), host_at_receiver_completion=host_at_receiver_completion, collector=collector_status, collector_at_receiver_completion=collector_at_receiver_completion, shutdown_timings_seconds=shutdown_timings, paper_fast_path=paper_fast_path, ledger_close=ledger_close, ledger_kinds=kinds, samples=samples, ordered_wire_reconciliation=dict(expected=expected.hexdigest(), receiver=delivery["receiver"].hexdigest(), paper=delivery["paper"].hexdigest(), collector=retained.hexdigest(), collector_records=count))
    result["ordered_wire_reconciliation"]["pass"] = len(frames)==len(completed)==count and len({expected.hexdigest(), retained.hexdigest(), delivery["paper"].hexdigest(), delivery["receiver"].hexdigest()}) == 1
    dump(args.output/"result.json", result)
    print(json.dumps({"output": str(args.output), "rate": result["completion_rate"], "lag": result["receiver_end_lag_seconds"], "ordered": result["ordered_wire_reconciliation"]["pass"]}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--route", choices=("baseline", "candidate"), required=True)
    parser.add_argument(
        "--paper-path", choices=("full", "qualifying-fast"), required=True,
        help="Exact paper callback under comparison; this is not a runtime qualification.",
    )
    parser.add_argument("--journal", type=Path)
    parser.add_argument("--frames", type=Path, help="Previously sealed synthetic frames, verified with the offline test key")
    parser.add_argument("--records", type=int, default=10000)
    parser.add_argument("--mode", choices=("natural", "sustained", "burst"), default="sustained")
    parser.add_argument(
        "--rate", type=float,
        help="Explicit diagnostic offered rate for sustained/burst modes; never an acceptance floor.",
    )
    args = parser.parse_args()
    if args.records < 1 or (args.rate is not None and args.rate <= 0):
        parser.error("records and any supplied diagnostic rate must be positive")
    if args.mode == "natural" and not args.journal:
        parser.error("natural timing requires a journal")
    if args.mode != "natural" and args.rate is None:
        parser.error("sustained/burst timing requires an explicit diagnostic --rate")
    with BenchmarkLease(args.output):
        run(args)
