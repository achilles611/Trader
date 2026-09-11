"""Bounded read-only source diagnostic; never creates a paper runtime or grant."""
import argparse
from collections import Counter
from dataclasses import fields
from datetime import datetime, timezone
import json
from pathlib import Path
import time

from src.l3f_provider.ninjatrader_commission import NinjaTraderListenerWorker


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    counts = Counter()
    first = {}
    errors = []
    started = datetime.now(timezone.utc).isoformat()
    with (args.output / 'native-observations.jsonl').open('x', encoding='utf-8') as stream:
        def observe(observation):
            payload = {field.name: getattr(observation, field.name) for field in fields(observation)}
            payload['payload'] = dict(observation.payload)
            counts[observation.observation_type] += 1
            first.setdefault(observation.observation_type, payload)
            stream.write(json.dumps({'recorded_at': datetime.now(timezone.utc).isoformat(),
                                     'observation': payload, 'wire_hash': observation.wire_hash()},
                                    default=str, separators=(',', ':')) + '\n')
        listener = NinjaTraderListenerWorker(on_observation=observe,
                    on_rejection=lambda error: errors.append(str(error)))
        try:
            listener.start()
            time.sleep(20)
        finally:
            final = listener.stop().as_dict()
    result = {'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
              'authority': 'OBSERVE_ONLY', 'commands': 0, 'counts': dict(counts),
              'first_by_type': first, 'rejections': errors, 'listener': final}
    (args.output / 'summary.json').write_text(json.dumps(result, default=str, indent=2), encoding='utf-8')
    print(json.dumps({'started_at': started, 'finished_at': result['finished_at'],
                      'counts': dict(counts), 'rejections': errors,
                      'attachment': final['observer_attachment'], 'path': str(args.output)}, default=str))


if __name__ == '__main__':
    main()
