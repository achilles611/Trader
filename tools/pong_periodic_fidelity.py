"""Offline deterministic periodic screen; never creates an execution bridge.

Reference hosts ingest every authenticated record independently and invoke the
unchanged calculation at candidate cutoffs. No floating tolerance is allowed.
The ordinary event-driven host advances its own independent strategy state.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.l3g_paper import pong_production as production
from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider
from src.l3g_paper.pong import PongStateMachine
from src.l3g_paper.pong_periodic import OfflinePeriodicPong
from src.l3g_paper.pong_volume_history import PongVolumeReferenceHistory
from tests.test_l3g_pong_production import runtime_snapshot, pending_collection


def utc(text):
    return datetime.fromisoformat(text.replace('Z', '+00:00'))


def stamp(at):
    return at.isoformat().replace('+00:00', 'Z')


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def file_sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for data in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(data)
    return h.hexdigest()


def verify_corpus(corpus, key):
    manifest = json.loads((corpus / 'manifest.json').read_bytes())
    if file_sha(corpus / 'frames.jsonl') != manifest['frames_sha256']:
        raise ValueError('CORPUS_DIGEST_CHANGED')
    if len(manifest['source_fingerprints']) != 1:
        raise ValueError('AMBIGUOUS_SOURCE')
    source = next(iter(manifest['source_fingerprints']))
    receipt = manifest['history_receipt']
    path = corpus / receipt['path']
    if file_sha(path) != receipt['sha256']:
        raise ValueError('HISTORY_RECEIPT_BYTES_CHANGED')
    history = PongVolumeReferenceHistory(path, root_key=key, expected_source=source)
    if history.error or history.receipt_hash != receipt['receipt_hash']:
        raise ValueError('HISTORY_RECEIPT_AUTHENTICATION')
    value = json.loads(path.read_bytes())
    dependencies = [json.loads(line)['observation'] for line in
                    (corpus / 'source-dependencies.jsonl').read_bytes().splitlines()]
    if value['records'] != dependencies or value['received_at'] != manifest['history_admission_at']:
        raise ValueError('HISTORY_RECEIPT_DEPENDENCY_OR_AVAILABILITY_MISMATCH')
    return manifest, source, utc(value['received_at'])


def deny(*args, **kwargs):
    raise AssertionError('OFFLINE_EXECUTION_DENIED')


class Capture:
    def __init__(self, host, path, *, trace=True):
        self.host, self.artifacts = host, {}
        self.counts = Counter()
        self.machine = PongStateMachine(discernment_policy=host._discernment.policy)
        self.context = {'source_index': -1, 'source_sequence': None}
        self.stream = path.open('xb') if trace else None
        self.opportunities = []
        self.switches = []
        self.last_update = None
        self.evaluation_cpu_ns = 0
        self.evaluation_wall_ns = 0

    def install(self, stack):
        for owner, name, stage in ((self.host._flow, 'evaluate', 'flow'),
                                    (self.host._volume, 'evaluate', 'volume'),
                                    (self.host._mapper, 'map_frame', 'mapper')):
            original = getattr(owner, name)
            def wrapper(*args, _original=original, _stage=stage, **kwargs):
                result = _original(*args, **kwargs)
                self.artifacts[_stage] = result
                return result
            stack.enter_context(patch.object(owner, name, wrapper))
        original_liquidity = self.host._liquidity.evaluate
        def liquidity(values, **kwargs):
            result = original_liquidity(values, **kwargs)
            self.artifacts[values[0].side.value] = result
            return result
        stack.enter_context(patch.object(self.host._liquidity, 'evaluate', liquidity))
        original = self.host._evaluate_microstructure
        def evaluate(at):
            self.artifacts = {}
            wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
            original(at)
            self.evaluation_cpu_ns += time.thread_time_ns() - cpu
            self.evaluation_wall_ns += time.perf_counter_ns() - wall
            self.counts['evaluations'] += 1
            self.counts['complete_frames'] += int('mapper' in self.artifacts)
            self.counts['valid_frames'] += int(not self.host._microstructure_blockers)
            snapshot = self.host._discernment.last_snapshot
            row = dict(self.context, cutoff=stamp(at), blockers=self.host._microstructure_blockers,
                       artifact_stages=sorted(self.artifacts),
                       snapshot_hash=None if snapshot is None else snapshot.result_hash,
                       score=None if snapshot is None else snapshot.effective_long_score,
                       evidence_revision=self.host._discernment.evidence_revision)
            if self.stream:
                self.stream.write(canonical(row) + b'\n')
        stack.enter_context(patch.object(self.host, '_evaluate_microstructure', evaluate))
        def apply(update):
            self.last_update = dict(snapshot=update.snapshot.payload(),
                switch=None if update.switch is None else update.switch.payload(),
                admitted=update.admitted, reason_code=update.reason_code)
            self.counts['discernment_updates'] += 1
            machine = self.machine
            if machine.shadow_direction is None and self.host._discernment.latched_direction is not None:
                result = machine.observe_startup_seed(self.host._discernment.latched_direction, update.snapshot)
            elif update.switch is None:
                result = machine.on_snapshot_update(update.snapshot)
            else:
                self.switches.append(dict(self.context, at=update.switch.available_at,
                    direction=update.switch.direction.value, score=update.snapshot.effective_long_score,
                    setup=update.snapshot.policy_hash, event=update.switch.payload()))
                # Preserve unconfigured bias and disarmed gates; never invent authority.
                result = machine.on_switch(update.switch, self.host._daily_bias, entry_gates_pass=False)
            for action in result.actions:
                self.opportunities.append(dict(self.context, at=update.snapshot.available_at,
                    instrument=update.snapshot.instrument, direction=action.expected_direction.value,
                    setup=update.snapshot.policy_hash + ':' + action.kind.value,
                    kind=action.kind.value, score=update.snapshot.effective_long_score))
        stack.enter_context(patch.object(self.host, '_apply_discernment_update', apply))

    def close(self):
        if self.stream:
            self.stream.close()


def host_state(host):
    snap = host._discernment.last_snapshot
    return dict(input_error=host._input_error, blockers=host._microstructure_blockers,
        streams=host._stream_sequences, counts=host._stream_counts, gaps=host._stream_missing,
        out_of_order=host._stream_out_of_order, producer=host._producer,
        generation=host._source_generation, trades=tuple(host._trades),
        bid=tuple(host._depth[production.BookSide.BID]),
        ask=tuple(host._depth[production.BookSide.ASK]),
        volume=host._volume_buckets, classified=host._classified_hour,
        quote=host._last_quote_record,
        evidence_revision=host._discernment.evidence_revision,
        snapshot=None if snap is None else snap.payload(),
        direction=host._discernment.latched_direction)


def match(baseline, candidate, allowance_seconds):
    """FIFO by instrument/direction/setup, causal nonnegative delay within bound."""
    unmatched = list(candidate)
    rows = []
    for event in baseline:
        found = next((e for e in unmatched if all(e[k] == event[k] for k in
                     ('instrument', 'direction', 'setup')) and
                     0 <= (utc(e['at'])-utc(event['at'])).total_seconds() <= allowance_seconds), None)
        if found:
            unmatched.remove(found)
            rows.append(dict(type='MATCHED', baseline=event, candidate=found,
                             delay_seconds=(utc(found['at'])-utc(event['at'])).total_seconds()))
        else:
            rows.append(dict(type='LOST_OR_SUPPRESSED', baseline=event))
    rows.extend(dict(type='NEW_OR_REVERSED', candidate=event) for event in unmatched)
    return rows


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    key = LocalPaperSecretProvider().load_key()
    manifest, source, history_at = verify_corpus(args.corpus, key)
    first = utc(manifest['first_market_receipt_at'])
    mode_specs = [(int(x.split(':')[0]), float(x.split(':')[1]) if ':' in x else 0.0)
                  for x in args.intervals.split(',')]
    clock = [history_at]
    virtual_ns = [0]
    captures = []
    errors = []
    comparisons = Counter()
    source_counts = Counter()
    source_age_max = 0.0
    started, cpu_start = time.perf_counter(), time.process_time()
    with ExitStack() as stack:
        stack.enter_context(patch.object(production, '_now', lambda: stamp(clock[0])))
        def make(name, trace=True):
            host = production.PongProductionHost(output / name,
                selected_profile='BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2',
                expected_market_source_fingerprint=source, runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection, controller_dispatch=deny, native_dispatch=deny,
                activation_authorized=False, history_verification_key=key, compute_workers=0)
            capture = Capture(host, output / (name + '-trace.jsonl'), trace=trace)
            capture.install(stack)
            captures.append(capture)
            return host, capture
        baseline, base_capture = make('event-driven')
        modes = []
        for interval, phase in mode_specs:
            label = str(interval) + 'ms-phase-' + str(phase)
            host, capture = make(label)
            reference, oracle = make(label + '-oracle', trace=False)
            scheduler = OfflinePeriodicPong(host, interval_ms=interval,
                origin_ns=0, phase_ns=int(phase * 1e6), clock_ns=lambda: virtual_ns[0])
            modes.append((label, scheduler, capture, reference, oracle))
        cutoff_file = (output / 'shared-cutoff-checks.jsonl').open('xb')
        stack.callback(cutoff_file.close)
        def ticks_before(bound_ns, inclusive=False):
            for label, scheduler, capture, reference, oracle in modes:
                while scheduler.next_deadline_ns < bound_ns or (inclusive and scheduler.next_deadline_ns == bound_ns):
                    due = scheduler.next_deadline_ns
                    if due < 0:
                        raise AssertionError('NEGATIVE_DEADLINE')
                    virtual_ns[0] = due
                    at = first + timedelta(microseconds=due // 1000)
                    clock[0] = at
                    result = scheduler.tick(due, at)
                    if result is None:
                        raise AssertionError('DUE_TICK_NOT_EVALUATED')
                    if reference._input_error is None:
                        reference._evaluate_microstructure(at)
                    else:
                        reference._microstructure_blockers = (reference._input_error,)
                    # Match the explicitly scheduled expiry maintenance, if used.
                    if getattr(result, 'expiry_maintenance', False):
                        reference._apply_discernment_update(reference._discernment.on_expiry(stamp(at)))
                    fields = []
                    candidate_state, reference_state = host_state(scheduler.host), host_state(reference)
                    for field in candidate_state:
                        if candidate_state[field] != reference_state[field]:
                            fields.append(field)
                    if capture.artifacts != oracle.artifacts:
                        fields.append('full_artifact_payloads')
                    if capture.last_update != oracle.last_update:
                        fields.append('discernment_update')
                    comparisons[label] += 1
                    row = dict(mode=label, cutoff=stamp(at), source_index=capture.context['source_index'],
                               pass_exact=not fields, differing_fields=fields)
                    cutoff_file.write(canonical(row) + b'\n')
                    if fields:
                        errors.append(row)
                        if len(errors) <= 3:
                            print('PARITY_FAILURE', row, flush=True)
        count = 0
        last_ns = 0
        with (args.corpus / 'frames.jsonl').open('rb') as stream:
            for index, frame in enumerate(stream):
                record = NinjaTraderObservation.from_wire(frame.decode()).verify_attestation(
                    key, expected_source_fingerprint=source)
                is_market = record.observation_type in ('DEPTH', 'TRADE', 'QUOTE')
                at = history_at if record.observation_type == 'VOLUME_REFERENCE_HISTORY' else utc(record.ninja_receipt_time)
                now_ns = round((at-first).total_seconds() * 1e9)
                if now_ns >= 0:
                    ticks_before(now_ns)
                    last_ns = now_ns
                clock[0], virtual_ns[0] = at, max(now_ns, 0)
                context = dict(source_index=index, source_sequence=record.local_monotonic_sequence)
                for capture in captures:
                    capture.context = context
                baseline._consume_observation(record)
                for _, scheduler, _, reference, _ in modes:
                    scheduler.consume(record)
                    reference._process_observation(record)
                source_counts[record.observation_type] += 1
                if is_market:
                    source_age_max = max(source_age_max, (utc(record.ninja_receipt_time) - utc(
                        record.provider_timestamp or record.exchange_timestamp or record.ninja_receipt_time)).total_seconds())
                count += 1
                if count % 25000 == 0:
                    print(json.dumps(dict(records=count, wall_seconds=time.perf_counter()-started,
                                          errors=len(errors))), flush=True)
        ticks_before(last_ns, inclusive=True)
        if count != manifest['records']:
            raise ValueError('CORPUS_RECORD_COUNT_CHANGED')
        results = []
        for label, scheduler, capture, reference, oracle in modes:
            interval = scheduler.interval_ns / 1e9
            matched = match(base_capture.opportunities, capture.opportunities, interval)
            with (output / (label + '-switches.jsonl')).open('xb') as stream:
                for row in capture.switches:
                    stream.write(canonical(row) + b'\n')
            with (output / (label + '-divergences.jsonl')).open('xb') as stream:
                for row in matched:
                    stream.write(canonical(row) + b'\n')
            actions = Counter(row['type'] for row in matched)
            no_entries = not any('REQUEST_ENTRY' in e['kind'] for e in base_capture.opportunities + capture.opportunities)
            results.append(dict(mode=label, correctness='PASS' if not any(e['mode']==label for e in errors) else 'FAIL',
                shared_cutoffs=comparisons[label], counts=dict(capture.counts),
                evaluation_reduction=1-capture.counts['evaluations']/max(1,base_capture.counts['evaluations']),
                evaluation_cpu_ns=capture.evaluation_cpu_ns, metrics=scheduler.metrics(),
                entry_fidelity='UNPROVEN_NO_VALID_BIAS_OR_ENABLED_ACTIONABLE_PATH' if no_entries else ('FAIL' if any(k!='MATCHED' for k in actions) else 'PASS'),
                protective_fidelity='UNPROVEN_NO_HELD_POSITION_MARKET_PATH',
                divergence_counts=dict(actions), switches=len(capture.switches),
                input_error=scheduler.host._input_error, source_gaps=scheduler.host._stream_missing))
        result = dict(schema='pong-periodic-fidelity-v1', disposition='BLOCKED',
            reasons=['DECISION_FIDELITY_REQUIRES_ACTIONABLE_MARKET_EVIDENCE',
                     'ANALYTICAL_PROTECTIVE_OUTCOMES_NOT_SEPARATED'],
            test_only=True, execution_authority='DENIED',
            fixed_ingress_acceptance='RETIRED_NO_NUMERIC_THRESHOLD_AUTHORIZED',
            corpus=str(args.corpus), manifest_sha256=file_sha(args.corpus/'manifest.json'),
            frames_sha256=manifest['frames_sha256'], records=count, source_counts=dict(source_counts),
            evidence_seconds=(utc(manifest['last_receipt_at'])-first).total_seconds(),
            source_to_receipt_max_seconds=source_age_max,
            history_admission_at=stamp(history_at), same_timestamp_order='All original records at cutoff before timer; original file order breaks ties.',
            tolerance='Exact equality; no numeric tolerance; cutoff times identical within each pair.',
            reference='Independent per-event owner using unchanged production calculation at same periodic cutoff and same maintenance.',
            timing_note='Instrumented deterministic diagnostic; CPU comparisons and service deadlines require separate paced runs.',
            matching_rule='FIFO by instrument, direction and policy-hash/action-kind setup; candidate at or after reference within cadence. Missing pairs retained in full.',
            baseline=dict(counts=dict(base_capture.counts), actions=base_capture.opportunities,
                          switches=base_capture.switches, input_error=baseline._input_error,
                          evaluation_cpu_ns=base_capture.evaluation_cpu_ns),
            modes=results, errors=errors, wall_seconds=time.perf_counter()-started,
            process_cpu_seconds=time.process_time()-cpu_start,
            stage2='DEFERRED_NO_PERIODIC_CANDIDATE_PASSES_DECISION_AND_PROTECTION_SCREEN')
        for capture in captures:
            capture.close()
        (output / 'result.json').write_bytes(json.dumps(result, indent=2, default=str).encode()+b'\n')
        print(json.dumps({k:result[k] for k in ('records','disposition','wall_seconds','stage2')}), flush=True)
        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--corpus', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--intervals', default='10,20,25')
    run(parser.parse_args())
