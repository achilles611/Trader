"""Offline natural-receipt Pong cadence measurement; never starts a runtime.

Original signed frames are independently released by a persistent spawned
process. One owner decodes, validates, records and applies every released input.
Collector journal writes/fsync remain enabled. Result latency covers EACH market
record through its first subsequent completed analytical evaluation attempt.
This is neither an actionable-decision latency nor a production certificate.
"""
from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter
from contextlib import ExitStack
import ctypes
from datetime import datetime, timedelta, timezone
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import platform
import queue
import re
import sys
import threading
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.pong_periodic_support import (
    BenchmarkLease, OfflineWorkerShutdownUnproven, independent_release_sender,
    release_progress_guard, RELEASE_COMPLETE, RELEASE_ABORTED,
    RELEASE_CAPACITY_EXHAUSTED, RELEASE_FAILED, system_cpu_times,
)

MARKET = {"DEPTH", "QUOTE", "TRADE"}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def utc(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def timestamp_ns(value):
    """Preserve native 100 ns receipt precision instead of truncating to us."""
    at = utc(value)
    fraction = re.search(r"\.(\d+)(?:Z|[+-]\d\d:\d\d)$", value)
    nanos = int((fraction.group(1)+"000000000")[:9]) if fraction else 0
    seconds = int((at.replace(microsecond=0)-datetime(1970,1,1,tzinfo=timezone.utc)).total_seconds())
    return seconds*1_000_000_000+nanos


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def distribution(values):
    values = sorted(values)
    return dict(count=len(values), p50=values[len(values)//2] if values else None,
        p95=values[min(len(values)-1, int(len(values)*.95))] if values else None,
        p99=values[min(len(values)-1, int(len(values)*.99))] if values else None,
        maximum=values[-1] if values else None)


def history_admission_time(corpus, manifest, key):
    from src.l3g_paper.pong_volume_history import PongVolumeReferenceHistory
    receipt = manifest.get("history_receipt")
    if not receipt:
        raise ValueError("ORIGINAL_AUTHENTICATED_HISTORY_RECEIPT_REQUIRED")
    path = corpus / receipt["path"]
    raw = path.read_bytes()
    if digest(raw) != receipt["sha256"]:
        raise ValueError("HISTORY_RECEIPT_BYTES_CHANGED")
    value = json.loads(raw)
    verified = PongVolumeReferenceHistory(path, root_key=key,
        expected_source=next(iter(manifest["source_fingerprints"])))
    if verified.error or verified.receipt_hash != receipt["receipt_hash"]:
        raise ValueError("HISTORY_RECEIPT_AUTHENTICATION")
    dependencies = (corpus / "source-dependencies.jsonl").read_bytes()
    if digest(dependencies) != manifest["source_dependencies_sha256"]:
        raise ValueError("HISTORY_DEPENDENCY_BYTES_CHANGED")
    if value["records"] != [json.loads(line)["observation"] for line in dependencies.splitlines()]:
        raise ValueError("HISTORY_RECEIPT_DEPENDENCY_MISMATCH")
    if value["received_at"] != manifest["history_admission_at"]:
        raise ValueError("HISTORY_RECEIPT_AVAILABILITY_MISMATCH")
    return utc(value["received_at"])


def verify_line(line, previous, sequence):
    row = json.loads(line)
    if row.get("schema") != "beelzebub-pong-authenticated-observation-record-v1":
        raise ValueError("JOURNAL_SCHEMA")
    claimed = row.pop("record_hash")
    if row["journal_sequence"] != sequence or row["previous_record_hash"] != previous:
        raise ValueError("JOURNAL_SEQUENCE_OR_PREVIOUS_HASH")
    if digest(canonical(row)) != claimed:
        raise ValueError("JOURNAL_RECORD_HASH")
    wire = canonical(row["observation"])
    if digest(wire) != row["wire_hash"] or row["signature_verified"] is not True:
        raise ValueError("JOURNAL_WIRE_HASH_OR_TRUST")
    return wire, claimed


class ReplayClock:
    def __init__(self, initial):
        self.local = threading.local()
        self.initial = initial

    def set(self, at):
        self.local.at = at

    def now(self):
        return getattr(self.local, "at", self.initial)


class ObservedQueue(queue.Queue):
    def __init__(self, maxsize):
        super().__init__(maxsize)
        self.peak = 0

    def _put(self, item):
        super()._put(item)
        self.peak = max(self.peak, self._qsize())


def slope(samples, key):
    if len(samples) < 2:
        return None
    xs = [s["elapsed_seconds"] for s in samples]
    ys = [s[key] for s in samples]
    xm, ym = sum(xs)/len(xs), sum(ys)/len(ys)
    divisor = sum((x-xm)**2 for x in xs)
    return sum((x-xm)*(y-ym) for x,y in zip(xs,ys))/divisor if divisor else None


def process_memory(pid):
    """Windows process counters without requiring an optional package."""
    if os.name != "nt":
        return None
    from ctypes import wintypes
    class Counters(ctypes.Structure):
        _fields_ = [("cb",wintypes.DWORD),("faults",wintypes.DWORD)]+[
            (name,ctypes.c_size_t) for name in ("peak_working_set","working_set",
                "peak_paged_pool","paged_pool","peak_nonpaged_pool","nonpaged_pool",
                "pagefile","peak_pagefile","private_bytes")]
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD,wintypes.BOOL,wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE,ctypes.POINTER(Counters),wintypes.DWORD]
    handle = kernel.OpenProcess(0x1000,False,pid)
    if not handle:
        return None
    try:
        value = Counters()
        value.cb = ctypes.sizeof(value)
        if not psapi.GetProcessMemoryInfo(handle,ctypes.byref(value),value.cb):
            return None
        return dict(rss=value.working_set,peak_rss=value.peak_working_set,private_bytes=value.private_bytes)
    finally:
        kernel.CloseHandle(handle)


def replay(corpus, output, key, *, interval_ms=0, phase_ms=0, mode="natural",
           rate=2111, limit=900, record_capacity=262144, byte_capacity=536870912):
    with BenchmarkLease(output) as lease:
        return _replay(corpus, output, key, interval_ms=interval_ms, phase_ms=phase_ms,
            mode=mode, rate=rate, limit=limit, record_capacity=record_capacity,
            byte_capacity=byte_capacity, lease=lease.metadata)


def _replay(corpus, output, key, *, interval_ms, phase_ms, mode, rate, limit,
            record_capacity, byte_capacity, lease):
    from src.l3f_provider.ninjatrader_commission import NinjaTraderCommissioningHarness
    from src.l3g_paper import pong_production as production, pong_collection as collection
    from src.l3g_paper.pong_periodic import OfflinePeriodicPong

    manifest = json.loads((corpus / "manifest.json").read_bytes())
    raw = (corpus / "frames.jsonl").read_bytes()
    if digest(raw) != manifest["frames_sha256"]:
        raise ValueError("CORPUS_DIGEST_CHANGED")
    frames = tuple(raw.splitlines())
    del raw
    if len(frames) != manifest["records"]:
        raise ValueError("CORPUS_COUNT_CHANGED")
    if len(manifest["source_fingerprints"]) != 1:
        raise ValueError("CORPUS_SOURCE_AMBIGUOUS")
    history_at = history_admission_time(corpus, manifest, key)
    source_id = next(iter(manifest["source_fingerprints"]))
    first_market = utc(manifest.get("first_market_receipt_at", manifest["first_receipt_at"]))
    clock = ReplayClock(first_market)
    kinds, receipts, observation_ids, indices = [], [], [], {}
    receipt_ns, source_ns, receipt_text = [], [], []
    dues = []
    for index, frame in enumerate(frames):
        row = json.loads(frame)
        identity = row["observation_id"]
        if identity in indices:
            raise ValueError("CORPUS_DUPLICATE_OBSERVATION_ID")
        indices[identity] = index
        observation_ids.append(identity)
        kinds.append(row["observation_type"])
        receipts.append(utc(row["ninja_receipt_time"]))
        receipt_text.append(row["ninja_receipt_time"])
        receipt_ns.append(timestamp_ns(row["ninja_receipt_time"]))
        source_ns.append(timestamp_ns(row.get("exchange_timestamp") or row.get("provider_timestamp") or row["ninja_receipt_time"]))
        if mode != "natural":
            burst_size = rate*4
            dues.append((index//burst_size)*4+(index%burst_size)/(2*rate))
    if mode == "natural":
        origin_index = next(i for i,kind in enumerate(kinds) if kind in MARKET)
        origin_receipt_ns = receipt_ns[origin_index]
        first_market = receipts[origin_index]
        for native_ns in receipt_ns:
            # Pre-window history is delivered at zero with its separate original
            # authenticated availability clock; native market intervals are exact.
            dues.append(max(dues[-1] if dues else 0., (native_ns-origin_receipt_ns)/1e9))
    if history_at > first_market:
        raise ValueError("HISTORY_NOT_AVAILABLE_BEFORE_REPLAY_MARKET_ORIGIN")
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "performance-lease.json", lease)

    context = multiprocessing.get_context("spawn")
    go, abort = context.Event(), context.Event()
    epoch = context.RawValue("d", 0.)
    released, admitted_shared = context.RawValue("q", 0), context.RawValue("q", 0)
    state = context.RawValue("i", 0)
    progress_lock = context.Lock()
    producer_cpu = context.RawValue("q", 0)
    release_started = context.RawArray("d", len(frames))
    release_completed = context.RawArray("d", len(frames))
    outstanding_peaks = context.RawArray("q", 2)
    read_pipe, write_pipe = context.Pipe(duplex=False)
    producer = context.Process(target=independent_release_sender,
        name="PONG_PERIODIC_INDEPENDENT_PRODUCER", args=(str(corpus / "frames.jsonl"),
        manifest["frames_sha256"], dues, write_pipe, go, abort, epoch,
        release_started, release_completed, released, admitted_shared, state,
        progress_lock, producer_cpu, outstanding_peaks, record_capacity, byte_capacity))

    failures, samples, host_load_samples, timer_observations = [], [], [], []
    pending_indices = []
    first_result_elapsed = [None]*len(frames)
    admission_elapsed = [None]*len(frames)
    coverage, first_blockers = Counter(), {}
    evaluation_count = evaluation_cpu_ns = evaluation_wall_ns = 0
    current_index = -1
    processed = 0
    admitted = 0
    started = 0.
    source_manifest = {}
    rss_peak = {"owner_bytes": None, "producer_bytes": None}

    def deny(*args, **kwargs):
        failures.append("EXECUTION_OR_RUNTIME_CALLBACK_ATTEMPTED")
        raise AssertionError("Offline measurement has no execution capability")

    class Collector(collection.PongAuthenticHourCollector):
        def _observation_line(self, record, enqueued_at):
            clock.set(utc(enqueued_at))
            return super()._observation_line(record, enqueued_at)

    class Host(production.PongProductionHost):
        def _evaluate_microstructure(self, at):
            nonlocal evaluation_count, evaluation_cpu_ns, evaluation_wall_ns
            cpu_before, wall_before = time.thread_time_ns(), time.perf_counter_ns()
            super()._evaluate_microstructure(at)
            evaluation_cpu_ns += time.thread_time_ns()-cpu_before
            evaluation_wall_ns += time.perf_counter_ns()-wall_before
            evaluation_count += 1
            completed = time.perf_counter()-started
            # Every pending market record receives its own latency observation.
            # No batching by newest-event age and no actionable-entry inference.
            for index in pending_indices:
                first_result_elapsed[index] = completed
            pending_indices.clear()
            blockers = tuple(self._microstructure_blockers)
            coverage["complete_unblocked_evaluations"] += int(not blockers)
            for blocker in blockers:
                first_blockers.setdefault(blocker, dict(index=current_index,
                    elapsed_seconds=completed, as_of=at.isoformat()))

    with ExitStack() as stack:
        stack.enter_context(patch.object(collection, "_utc_now", clock.now))
        stack.enter_context(patch.object(production, "_now", lambda: clock.now().isoformat().replace("+00:00", "Z")))
        collector = Collector(output / "collector", expected_source_fingerprint=source_id,
            independent_market_collection=True,
            now=first_market.replace(minute=0, second=0, microsecond=0)-timedelta(seconds=1))
        collector._queue = ObservedQueue(collector._queue.maxsize)
        host = Host(output / "host", selected_profile="BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2",
            expected_market_source_fingerprint=source_id, runtime_snapshot=deny,
            collector_status=deny, controller_dispatch=deny, native_dispatch=deny,
            activation_authorized=False, history_verification_key=key, compute_workers=0)
        adapter = None

        def admit(record):
            nonlocal admitted, processed
            index = indices[record.observation_id]
            if index != current_index:
                raise ValueError("RECEIVER_ORDER_CHANGED")
            admission_elapsed[index] = time.perf_counter()-started
            available = history_at if kinds[index] == "VOLUME_REFERENCE_HISTORY" else receipts[index]
            clock.set(available)
            collector.ingest(record)
            if kinds[index] in MARKET:
                pending_indices.append(index)
            adapter.consume(record)
            processed += 1
            admitted += 1
            with release_progress_guard(progress_lock):
                admitted_shared.value = admitted

        harness = NinjaTraderCommissioningHarness(attestation_key=key, require_attestation=True,
            expected_source_fingerprint=source_id, on_observation=admit,
            on_rejection=collector.on_rejection, on_duplicate=collector.on_duplicate)
        for module in tuple(sys.modules.values()):
            file = getattr(module, "__file__", None)
            if file and Path(file).suffix == ".py" and Path(file).resolve().is_relative_to(ROOT):
                path = Path(file).resolve()
                source_manifest[str(path.relative_to(ROOT))] = digest(path.read_bytes())
        write_json(output / "loaded-source-manifest.json", source_manifest)
        write_json(output / "run-plan.json", dict(interval_ms=interval_ms, phase_ms=phase_ms,
            mode=mode, records=len(frames), scheduled_duration_seconds=dues[-1],
            frames_sha256=manifest["frames_sha256"], native_runtime_started=False,
            execution_authority="DENIED", target_events_per_second=2111))
        preflight_cpu = system_cpu_times()
        time.sleep(.5)
        preflight_end = system_cpu_times()
        preflight_total = preflight_end[1]-preflight_cpu[1]
        preflight_host = dict(aggregate_cpu_busy_percent=100*(1-(preflight_end[0]-preflight_cpu[0])/preflight_total)
            if preflight_total else None, owner_memory=process_memory(os.getpid()),
            sample_seconds=.5, disk_contention="NOT_MEASURED_BY_RUNNER_REQUIRES_EXTERNAL_HOST_CHECK",
            active_benchmarks="EXCLUDED_BY_GLOBAL_MUTEX")
        write_json(output / "preflight-host.json", preflight_host)
        cpu_before = time.process_time_ns()
        cpu_counters = system_cpu_times()
        next_sample = 0.
        next_load = 1.
        collector_status = None
        sender_identity = None
        elapsed_to_drain = None
        try:
            collector.start()
            producer.start()
            write_pipe.close()
            if not read_pipe.poll(30):
                raise RuntimeError("PRODUCER_PRELOAD_TIMEOUT")
            sender_identity = json.loads(read_pipe.recv_bytes())
            read_pipe.close()
            if (sender_identity.get("status") != "READY" or
                    sender_identity.get("frames_sha256") != manifest["frames_sha256"] or
                    sender_identity.get("frame_count") != len(frames)):
                raise RuntimeError("PRODUCER_PRELOAD_IDENTITY_MISMATCH")
            started = time.perf_counter()
            epoch.value = started
            adapter = OfflinePeriodicPong(host, interval_ms=interval_ms,
                phase_ns=int(phase_ms*1e6), origin_ns=time.monotonic_ns())
            go.set()
            while True:
                now = time.perf_counter()
                elapsed = now-started
                with release_progress_guard(progress_lock):
                    published, sender_state = released.value, state.value
                if sender_state in (RELEASE_ABORTED, RELEASE_CAPACITY_EXHAUSTED, RELEASE_FAILED):
                    failures.append("PRODUCER_STATE_"+str(sender_state))
                    break
                # Bounded ownership batch prevents an expanding ingress queue
                # from starving the timer. Preserve a published equal-receipt
                # group across the bound so its capture order precedes a tick.
                target = min(published, processed+64)
                while target < published and target and receipt_ns[target] == receipt_ns[target-1]:
                    target += 1
                while processed < target:
                    current_index = processed
                    harness.ingest((frames[current_index],))
                    if admitted != current_index+1:
                        failures.append("SIGNED_INPUT_REJECTED_OR_DUPLICATED")
                        break
                if failures:
                    break
                if interval_ms and (processed < len(frames) or pending_indices):
                    mono = time.monotonic_ns()
                    if mono >= adapter.next_deadline_ns:
                        # Cutoff uses elapsed wall time, including owner backlog
                        # and service; unavailable future records never enter it.
                        tick_time = first_market+timedelta(seconds=time.perf_counter()-started)
                        if mode == "burst" and current_index >= 0:
                            # Synthetic stress compresses arrival spacing only.
                            # Its source-time axis cannot precede applied signed
                            # input; no claim of natural-clock latency follows.
                            tick_time = max(tick_time, receipts[current_index])
                            if adapter.last_view:
                                tick_time = max(tick_time, utc(adapter.last_view.available_cutoff))
                        clock.set(tick_time)
                        tick = adapter.tick(mono, tick_time)
                        if tick:
                            timer_observations.append(dict(evaluation_sequence=tick.view.evaluation_sequence,
                                cutoff=tick.view.available_cutoff,input_record_count=tick.view.input_record_count,
                                watermark=tick.view.watermark,lateness_ns=tick.lateness_ns,
                                missed_deadlines=tick.missed_deadlines,snapshot_cpu_ns=tick.snapshot_cpu_ns,
                                snapshot_wall_ns=tick.snapshot_wall_ns,evaluation_cpu_ns=tick.evaluation_cpu_ns,
                                evaluation_wall_ns=tick.evaluation_wall_ns,lock_hold_ns=tick.lock_hold_ns,
                                evaluated=tick.evaluated,blockers=tick.blockers))
                elapsed = time.perf_counter()-started
                if elapsed >= next_sample:
                    durable = collector._durable_records
                    due = bisect_right(dues, elapsed)
                    samples.append(dict(elapsed_seconds=elapsed, due=due, released=published,
                        admitted=admitted, processed=processed, durable=durable,
                        ingress_queue=published-admitted,
                        collector_queue=collector._queue.qsize(),
                        due_to_processed_backlog=due-processed,
                        due_to_durable_backlog=due-durable,
                        pending_result_records=len(pending_indices)))
                    next_sample = elapsed+.1
                if elapsed >= next_load:
                    counters = system_cpu_times()
                    total_delta, idle_delta = counters[1]-cpu_counters[1], counters[0]-cpu_counters[0]
                    host_load_samples.append(dict(elapsed_seconds=elapsed,
                        aggregate_cpu_busy_percent=100*(1-idle_delta/total_delta) if total_delta else None))
                    cpu_counters = counters
                    next_load = elapsed+1
                    for role,pid in (("owner_bytes", os.getpid()), ("producer_bytes", producer.pid)):
                        memory = process_memory(pid)
                        if memory:
                            rss_peak[role] = max(rss_peak[role] or 0, memory["peak_rss"])
                if host._input_error or collector._state == "FAILED":
                    failures.append(host._input_error or "COLLECTOR_FAILED")
                    break
                if processed == len(frames) and sender_state == RELEASE_COMPLETE:
                    if collector._durable_records == admitted and (not interval_ms or not pending_indices):
                        break
                if elapsed > limit:
                    failures.append("DRAIN_TIMEOUT")
                    break
                if processed == published:
                    sleep_seconds = .0005
                    if interval_ms:
                        sleep_seconds = min(sleep_seconds, max(0., (adapter.next_deadline_ns-time.monotonic_ns())/1e9))
                    if sleep_seconds:
                        time.sleep(sleep_seconds)
        except BaseException as error:
            failures.append(type(error).__name__+":"+str(error))
        finally:
            elapsed_to_drain = time.perf_counter()-started if started else None
            abort.set()
            if producer.pid is not None:
                try:
                    producer.join(5)
                except Exception as error:
                    failures.append("PRODUCER_JOIN_"+type(error).__name__)
            try:
                collector_status = collector.stop(timeout_seconds=10)
            except Exception as error:
                failures.append("COLLECTOR_STOP_"+type(error).__name__)
            clock.set(first_market+timedelta(seconds=elapsed_to_drain or 0))
            owner_cpu_ns = time.process_time_ns()-cpu_before
            collector_alive = bool(collector._thread and collector._thread.is_alive())
            workers_stopped = not producer.is_alive() and not collector_alive
            if not workers_stopped:
                write_json(output / "shutdown-failure.json", dict(failures=failures,
                    producer_alive=producer.is_alive(), collector_alive=collector_alive))
                raise OfflineWorkerShutdownUnproven("OFFLINE_WORKER_SHUTDOWN_UNPROVEN")
            if collector_status is None:
                collector_status = collector.status()
            lifecycle_seconds = time.perf_counter()-started if started else None

    previous, compared = "0"*64, 0
    try:
        with collector.journal_path.open("rb") as stream:
            for index, line in enumerate(stream):
                wire, previous = verify_line(line, previous, index+1)
                if wire != frames[index]:
                    raise ValueError("OUTPUT_SIGNED_WIRE_PARITY")
                compared += 1
        journal_ok = compared == admitted and previous == collector_status["durability"]["journal_tip_sha256"]
    except Exception as error:
        failures.append("OUTPUT_JOURNAL_VERIFICATION_"+type(error).__name__+":"+str(error))
        journal_ok = False
    market_indices = [i for i,kind in enumerate(kinds) if kind in MARKET]
    covered = [i for i in market_indices if first_result_elapsed[i] is not None]
    scheduled_latencies = [first_result_elapsed[i]-dues[i] for i in covered]
    release_latencies = [first_result_elapsed[i]-(release_completed[i]-started) for i in covered]
    admission_latencies = [first_result_elapsed[i]-admission_elapsed[i] for i in covered]
    published = released.value
    lateness = [release_completed[i]-started-dues[i] for i in range(published)]
    publication = [release_completed[i]-release_started[i] for i in range(published)]
    source_lags = [(receipt_ns[i]-source_ns[i])/1e9 for i in market_indices]
    producer_end = release_completed[published-1]-started if published else None
    try:
        source_unchanged = all(digest((ROOT/name).read_bytes()) == value for name,value in source_manifest.items())
    except OSError as error:
        source_unchanged = False
        failures.append("LOADED_SOURCE_REVALIDATION_"+type(error).__name__)
    integrity = (not failures and journal_ok and admitted == processed == published == len(frames)
        and not harness.summary.rejected and not harness.summary.duplicates
        and not collector_status["durability"]["queue_overflow_records"] and source_unchanged)
    arrivals = [s for s in samples if s["elapsed_seconds"] <= dues[-1]]
    tail = arrivals[len(arrivals)//2:]
    with (output / "event-result-latencies.jsonl").open("x",encoding="utf-8") as trace:
        for index in market_indices:
            trace.write(json.dumps(dict(index=index,observation_id=observation_ids[index],
                kind=kinds[index],receipt_at=receipt_text[index],
                scheduled_seconds=dues[index],release_seconds=release_completed[index]-started if release_completed[index] else None,
                admission_seconds=admission_elapsed[index],first_result_seconds=first_result_elapsed[index]),
                separators=(",",":"))+"\n")
    with (output / "timer-observations.jsonl").open("x",encoding="utf-8") as trace:
        for tick in timer_observations:
            trace.write(json.dumps(tick,separators=(",",":"))+"\n")
    result = dict(schema="pong-periodic-independent-paced-v1", test_only=True,
        qualification="NOT_QUALIFIED", interval_ms=interval_ms, phase_ms=phase_ms,
        mode=mode, target_events_per_second=2111, output_root=str(output),
        corpus_path=str(corpus), corpus_manifest=manifest, records=len(frames),
        market_records=len(market_indices), scheduled_duration_seconds=dues[-1],
        actual_evaluations=evaluation_count, evaluation_cpu_ns=evaluation_cpu_ns,
        evaluation_wall_ns=evaluation_wall_ns, adapter_metrics=adapter.metrics() if adapter else None,
        timer_lateness_seconds=distribution([row["lateness_ns"]/1e9 for row in timer_observations]),
        snapshot_cost_seconds=distribution([row["snapshot_wall_ns"]/1e9 for row in timer_observations]),
        evaluation_cost_seconds=distribution([row["evaluation_wall_ns"]/1e9 for row in timer_observations]),
        lock_hold_seconds=distribution([row["lock_hold_ns"]/1e9 for row in timer_observations]),
        owner_process_cpu_ns=owner_cpu_ns, producer_process_cpu_ns=producer_cpu.value,
        total_process_cpu_ns=owner_cpu_ns+producer_cpu.value,
        cpu_scope="Owner after its preload through collector startup/shutdown; producer target function after module import including its independent preload. Python child startup before target, owner preload, verification and report writing excluded",
        end_to_end_seconds=elapsed_to_drain, lifecycle_seconds=lifecycle_seconds, producer_seconds=producer_end,
        drain_seconds=max(0., elapsed_to_drain-producer_end) if elapsed_to_drain is not None and producer_end is not None else None,
        completed_result_rate_per_second=evaluation_count/elapsed_to_drain if elapsed_to_drain else None,
        released=published, admitted=admitted, processed=processed, durable=collector._durable_records,
        receipt_to_first_result_seconds=distribution(scheduled_latencies),
        actual_release_to_first_result_seconds=distribution(release_latencies),
        authenticated_admission_to_first_result_seconds=distribution(admission_latencies),
        signed_source_to_receipt_seconds=distribution(source_lags),
        clock_precision="Original 100 ns wire receipt precision retained for release schedule/source-age; analytical timestamp parsing retains unchanged production Python datetime microsecond semantics",
        first_result_latency_semantics="Every market event is assigned the first analytical attempt completed after its ordered application. Blocked/incomplete evaluations count as attempts. This is NOT actionable decision delay.",
        first_result_uncovered_market_records=len(market_indices)-len(covered),
        producer_schedule_lateness_seconds=distribution(lateness),
        producer_release_publication_seconds=distribution(publication),
        producer_exitcode=producer.exitcode, sender_identity=sender_identity,
        queue_peaks=dict(ingress_records=outstanding_peaks[0], ingress_wire_bytes=outstanding_peaks[1],
            collector_records=collector._queue.peak,
            due_to_processed_sampled=max((s["due_to_processed_backlog"] for s in samples),default=0),
            due_to_durable_sampled=max((s["due_to_durable_backlog"] for s in samples),default=0)),
        queue_slopes_records_per_second=dict(arrival_period_processed=slope(arrivals,"due_to_processed_backlog"),
            last_half_arrival_processed=slope(tail,"due_to_processed_backlog"),
            arrival_period_durable=slope(arrivals,"due_to_durable_backlog")),
        final_queues=dict(ingress=published-admitted, collector=collector._queue.qsize(),pending_result=len(pending_indices)),
        samples=samples, host_load_samples=host_load_samples, memory_sampled_peak_rss=rss_peak,
        preflight_host=preflight_host,
        receiver_rejected=harness.summary.rejected, receiver_duplicates=harness.summary.duplicates,
        source_gaps=host._stream_missing, out_of_order=host._stream_out_of_order,
        first_blockers=first_blockers, coverage=dict(coverage), failures=failures,
        actionable_decision_delay_seconds=None, decision_fidelity="UNPROVEN_NO_ORDER_CAPABLE_STRATEGY_ATTACHED",
        native_runtime_started=False, controller_attached=host._controller is not None,
        bridge_attached=host._bridge is not None, execution_authority="DENIED",
        operational_writes_enabled=["authenticated collector journal", "journal flush and fsync", "collector status", "authenticated volume history"],
        timing_scope="COLLECTOR_PLUS_ANALYTICAL_OWNER_ONLY_NOT_FULL_OPERATIONAL_PIPELINE",
        operational_scope_note="The production host.start loop, bridge, controller, risk and protection are deliberately absent. Their periodic persistence and control costs are NOT represented. This experiment cannot claim full-pipeline operational speedup or safety readiness.",
        unexercised_production_scope=["native socket producer", "paper runtime", "active strategy entries and exits", "order protection", "controller/risk durability"],
        detailed_profiling=False, measurement_hooks="Two evaluation clocks and count, per-event completion timestamps; same across modes",
        admission_batch_limit=64,same_timestamp_order="Published equal-receipt groups retain full capture order before a tick; a group may extend the batch bound",
        snapshot_availability_scope="Cutoff includes only the admitted ordered prefix; released ingress backlog remains separately measured and is never relabelled as applied data",
        journal_verification="PASS" if journal_ok else "FAIL", collector=collector_status,
        integrity_pass=integrity, workers_stopped=workers_stopped,
        source_unchanged_during_run=source_unchanged,
        environment=dict(python=sys.version,platform=platform.platform(),logical_cpu_count=os.cpu_count()))
    write_json(output / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interval-ms", type=int, choices=(0,10,20,25), default=0)
    parser.add_argument("--phase-ms", type=float, default=0.)
    parser.add_argument("--mode", choices=("natural","burst"), default="natural")
    parser.add_argument("--rate", type=int, default=2111, help="Synthetic burst base rate only; acceptance floor remains 2111/s")
    parser.add_argument("--limit-seconds", type=float, default=900.)
    args = parser.parse_args()
    if args.phase_ms < 0 or (args.interval_ms and args.phase_ms >= args.interval_ms) or (not args.interval_ms and args.phase_ms):
        parser.error("Phase must be nonnegative and less than one interval")
    if args.rate <= 0 or args.limit_seconds <= 0:
        parser.error("Rate and limit must be positive")
    from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider
    key = LocalPaperSecretProvider().load_key()  # Never print or persist secrets.
    result = replay(args.corpus.resolve(), args.output.resolve(), key,
        interval_ms=args.interval_ms, phase_ms=args.phase_ms, mode=args.mode,
        rate=args.rate, limit=args.limit_seconds)
    print(json.dumps({k:result[k] for k in ("output_root","integrity_pass","interval_ms",
        "actual_evaluations","total_process_cpu_ns","receipt_to_first_result_seconds",
        "producer_schedule_lateness_seconds","drain_seconds","failures")}, sort_keys=True))
    return 0 if result["integrity_pass"] else 2


if __name__ == "__main__":
    multiprocessing.freeze_support()
    raise SystemExit(main())
