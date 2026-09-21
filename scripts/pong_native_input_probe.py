"""Bounded read-only source diagnostic; never creates a paper runtime or grant."""
import argparse
from collections import Counter
from dataclasses import fields
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time

from src.l3f_provider.ninjatrader_commission import NinjaTraderListenerWorker
from src.l3g_paper.ninjatrader_transport import LocalPaperSecretProvider


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--expected-source-fingerprint')
    parser.add_argument('--duration-seconds', type=float, default=20.0)
    args = parser.parse_args()
    if args.duration_seconds <= 0 or args.duration_seconds > 120:
        parser.error('--duration-seconds must be in (0, 120].')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    counts = Counter()
    first = {}
    errors = []
    producer_identity = None
    producer_identity_changes = []
    source_generations = set()
    latest_source_sequences = {}
    producer_counters = {
        'observer_queue_overflow_frames': None,
        'observer_transport_failed_frames': None,
    }
    started = datetime.now(timezone.utc).isoformat()
    journal_path = args.output / 'native-observations.jsonl'
    with journal_path.open('x', encoding='utf-8') as stream:
        def observe(observation):
            nonlocal producer_identity
            payload = {field.name: getattr(observation, field.name) for field in fields(observation)}
            payload['payload'] = dict(observation.payload)
            counts[observation.observation_type] += 1
            first.setdefault(observation.observation_type, payload)
            identity = {
                'session_id': observation.session_id,
                'source_fingerprint': observation.producer.source_fingerprint,
                'build_fingerprint': observation.producer.build_fingerprint,
                'build_timestamp': observation.producer.build_timestamp,
            }
            if producer_identity is None:
                producer_identity = identity
            elif identity != producer_identity and identity not in producer_identity_changes:
                producer_identity_changes.append(identity)
            generation = observation.payload.get('source_generation')
            source_sequence = observation.payload.get('source_event_sequence')
            if type(generation) is int and generation > 0:
                source_generations.add(generation)
            if type(source_sequence) is int and source_sequence > 0:
                latest_source_sequences[observation.observation_type] = source_sequence
            for key in producer_counters:
                value = observation.payload.get(key)
                if type(value) is int and value >= 0:
                    current = producer_counters[key]
                    producer_counters[key] = value if current is None else max(current, value)
            stream.write(json.dumps({'recorded_at': datetime.now(timezone.utc).isoformat(),
                                     'observation': payload, 'wire_hash': observation.wire_hash()},
                                    default=str, separators=(',', ':')) + '\n')
        listener = NinjaTraderListenerWorker(
            attestation_key=(
                LocalPaperSecretProvider().load_key()
                if args.expected_source_fingerprint else None
            ),
            require_attestation=bool(args.expected_source_fingerprint),
            expected_source_fingerprint=args.expected_source_fingerprint,
            on_observation=observe,
            on_rejection=lambda error: errors.append(str(error)),
        )
        try:
            listener.start()
            time.sleep(args.duration_seconds)
        finally:
            final = listener.stop().as_dict()
    result = {'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
              'authority': 'OBSERVE_ONLY', 'commands': 0, 'counts': dict(counts),
              'first_by_type': first, 'rejections': errors, 'listener': final}
    with journal_path.open('rb') as source:
        journal_sha256 = hashlib.file_digest(source, 'sha256').hexdigest()
    result['journal'] = {
        'path': str(journal_path),
        'bytes': journal_path.stat().st_size,
        'sha256': journal_sha256,
    }
    result['producer_boundary'] = {
        'expected_source_fingerprint': args.expected_source_fingerprint,
        'identity': producer_identity,
        'identity_changes': producer_identity_changes,
        'source_generations': sorted(source_generations),
        'latest_source_sequences': dict(sorted(latest_source_sequences.items())),
        'producer_counters': producer_counters,
        'accepted_observations': sum(counts.values()),
        'rejected_observations': len(errors),
        'old_epoch_tail_disposition': 'FAILED_UNKNOWN_PENDING_TAIL_NOT_DRAIN_PROOF',
    }
    (args.output / 'summary.json').write_text(json.dumps(result, default=str, indent=2), encoding='utf-8')
    print(json.dumps({'started_at': started, 'finished_at': result['finished_at'],
                      'counts': dict(counts), 'rejections': errors,
                      'attachment': final['observer_attachment'], 'path': str(args.output)}, default=str))


if __name__ == '__main__':
    main()
