"""Preregister and capture one fixed BTC five-minute P2/P5 paper window."""
from __future__ import annotations

import argparse
from collections import deque
import math
from pathlib import Path
import subprocess
import sys
import time

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.lane_ii_continuous.capture import _patch_helper, _replace, capture_state
from scripts.lane_ii_continuous.common import (application_lock, atomic_json, binding, file_hash,
    fresh_coordinator_clock, process_identity, read_json, same_process, utc)
from scripts.lane_ii_continuous.long_qualification import set_window_wake_lease
from scripts.lane_ii_continuous.qualification import _epoch, _must, bound_json
from scripts.lane_ii_continuous.resources import Guard, admission, sample
from scripts.lane_ii_continuous.worker import worker_environment


SCHEMA = 'LANE_II_P2_P5_FRESH_PREREGISTRATION_V1'
SOURCE = '4468d1c2d75c55c24f1b96ac88ec7ee028d6aedb2ed9afdbcf2608d3dfb9962c'
CONTROLS = ('5m-P2', '5m-P5__confluence')
DURATION = 14400
CHECKPOINT = 900

ROSTER = """from strategy_roster import *
from strategy_roster import profile_specs as _all_specs

def profile_specs():
    rows = [row for row in _all_specs() if row['id'] in ('5m-P2', '5m-P5__confluence')]
    if {row['id'] for row in rows} != {'5m-P2', '5m-P5__confluence'} or len(rows) != 2:
        raise ValueError('FRESH_WAVE_EXACT_BASELINES_REQUIRED')
    return rows
"""


def patch_study(text):
    text = text.replace('\r\n', '\n')
    text = _replace(text, 'from next12_imports import bootstrap',
        'import continuous_clock\nimport continuous_transport\nfrom next12_imports import bootstrap')
    text = _replace(text, "'maximum_paper_seconds':43200", f"'maximum_paper_seconds':{DURATION}")
    text = _replace(text, '  self.ledgers={};self.profiles=[]',
        '  self.transport_control=continuous_transport.RetryController()\n  self.ledgers={};self.profiles=[]')
    text = _replace(text, "  # One shared transport, bounded cadence; no tenfold duplicate venue requests.",
        "  # One fixed-cadence, two-sided public quote stream for all shadow accounts.")
    text = _replace(text, '  for horizon in (300,900):', '  for horizon in (300,):')
    start = text.index('      for profile in self.profiles:', text.index(' def schedule(self,now):'))
    end = text.index('      pending_key=', start)
    text = text[:start] + text[end:]
    text = _replace(text, " or priority<0 and pending_key in self.pending", "")
    text = _replace(text, "and any(key in p.positions for p in self.profiles) and",
        "and now-m['end']<=900 and")
    text = _replace(text, ' def http_receiver(self):',
        ' def clock_ready(self):return continuous_clock.decision_allowed()\n\n def http_receiver(self):')
    start, end = text.index(' def http_receiver(self):'), text.index(' def ws_receiver(self):')
    text = text[:start] + ' def http_receiver(self):\n  continuous_transport.http_receiver(self,fetch,POLICY)\n\n' + text[end:]
    text = _replace(text, "  job=data['job'];self.pending.discard(job['key'])",
        "  job=data['job']\n  if data.get('attempt_terminal',True):self.pending.discard(job['key'])")
    text = _replace(text, "'market':m['id'],'side':info['side'],'raw_ref':ref,'received_wall':received,",
        "'market':m['id'],'side':info['side'],'raw_ref':ref,'received_wall':received,'evaluation_wall':now,")
    text = _replace(text, "'profiles':self.roster,", "'profiles':self.roster,'wave_preregistration_sha256':os.environ['LANE_II_WAVE_PREREG_SHA256'],")
    start = text.index("   'participation':")
    end = text.index("   'money':", start)
    text = text[:start] + "   'participation':'Exact P2 and P5 baselines. Both tokens follow the same fixed public sampling cadence.',\n" + text[end:]
    text = text.replace('All60 comparison profiles revalidate', 'Both baseline profiles revalidate')
    text = _replace(text, "'comparison':{'arms':5,'profiles':60",
        "'comparison':{'arms':2,'profiles':2")
    text = _replace(text, "'UNION_OF_ALL_ARMS_PENDING_REQUEST_PRIORITIES'", "'FIXED_TWO_SIDED_PUBLIC_CADENCE'")
    text = _replace(text, "'owned_job_before_startup_gate':True}",
        "'owned_job_before_startup_gate':True,'wave_preregistration_sha256':os.environ['LANE_II_WAVE_PREREG_SHA256']}")
    text = _replace(text, "  return Study(args.run_dir,args.paper_start_utc,args.cutoff_utc).run()",
        "  continuous_clock.install()\n  continuous_transport.install_boundary('worker',HERE/'http_helper.py')\n"
        "  return Study(args.run_dir,args.paper_start_utc,args.cutoff_utc).run()")
    return text.replace('# 60-profile ten-hour paper result', '# P2/P5 fresh baseline paper result')


def freeze(source, target, experiment):
    source, target, experiment = (Path(x).resolve() for x in (source, target, experiment))
    _must(not target.exists() and not target.is_relative_to(source), 'NEW_WAVE_CANDIDATE_REQUIRED')
    original = source / 'SOURCE_MANIFEST.json'
    _must(file_hash(original) == SOURCE, 'FROZEN_OVERNIGHT_SOURCE_CHANGED')
    manifest = read_json(original)
    spec = read_json(experiment)
    _must(spec['schema'] == 'LANE_II_P2_P5_EXPERIMENT_V1'
          and {row['parent_id'] for row in spec['models']} == set(CONTROLS)
          and sum(len(row['arms']) for row in spec['models']) == 50, 'EXACT_WAVE_01_REQUIRED')
    files = {}
    for name, sha in manifest['files'].items():
        path = (source / name).resolve()
        _must(path.is_relative_to(source) and file_hash(path) == sha, 'BASE_SOURCE_CHANGED:' + name)
        files[name] = path.read_bytes()
    files['study.py'] = patch_study(files['study.py'].decode()).encode()
    files['roster.py'] = ROSTER.encode()
    files['http_helper.py'] = _patch_helper(files['http_helper.py'].decode()).encode()
    for name in ('clock', 'transport'):
        files['continuous_' + name + '.py'] = Path(__file__).with_name(name + '.py').read_bytes()
    for name, raw in files.items():
        if name.endswith('.py'):
            compile(raw, name, 'exec')
    target.mkdir(parents=True, exist_ok=False)
    for name, raw in files.items():
        path = target / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    manifest.update(files={name: file_hash(target / name) for name in sorted(files)},
        fresh_wave={'base_sha256': SOURCE, 'wave_manifest_sha256': file_hash(experiment),
                    'controls': CONTROLS, 'shadow_variants': 50, 'public_only': True, 'duration_seconds': DURATION})
    atomic_json(target / 'SOURCE_MANIFEST.json', manifest)
    return binding(target / 'SOURCE_MANIFEST.json')


def register(root, candidate, experiment, clock_file, interpreter):
    root, candidate, experiment, clock_file, interpreter = (Path(x).resolve() for x in
        (root, candidate, experiment, clock_file, interpreter))
    target = root / 'PREREGISTRATION.json'
    _must(not target.exists(), 'WAVE_WINDOW_ALREADY_PREREGISTERED')
    source = candidate / 'SOURCE_MANIFEST.json'
    wave = read_json(source)['fresh_wave']
    _must(wave['wave_manifest_sha256'] == file_hash(experiment), 'FROZEN_WAVE_MANIFEST_CHANGED')
    clock = read_json(clock_file)
    _must(fresh_coordinator_clock(clock), 'FRESH_ACCEPTED_CLOCK_REQUIRED')
    capacity = admission([sample(root, cpu_interval=.5) for _ in range(8)], DURATION)
    _must(capacity['admitted'], 'RESOURCE:' + ','.join(capacity['reasons']))
    now = time.time()
    start = math.ceil((now + 180) / 300) * 300
    prereg = {'schema': SCHEMA, 'registered_utc': utc(now), 'paper_start_utc': utc(start),
              'cutoff_utc': utc(start + DURATION), 'wave_manifest_sha256': file_hash(experiment),
              'candidate_manifest_sha256': file_hash(source), 'candidate': binding(source),
              'experiment': binding(experiment), 'interpreter': binding(interpreter), 'clock_file': str(clock_file),
              'controls': CONTROLS, 'shadow_variants': 50, 'capacity': capacity, 'paper_only': True,
              'evidence_role': 'FRESH_SINGLE_WINDOW_VALIDATION', 'cutoff_extended_after_interruption': False,
              'comparison': 'REALIZED_NET_AFTER_MODELED_FEES_VS_MATCHED_BASELINE_WITH_CENSORING'}
    atomic_json(target, prereg)
    return prereg


def run(root):
    root = Path(root).resolve()
    prereg_path = root / 'PREREGISTRATION.json'
    prereg_ref = binding(prereg_path)
    prereg = bound_json(prereg_ref)
    candidate = Path(prereg['candidate']['path']).parent
    run_dir = root / 'captures' / 'attempt-001'
    start, cutoff = (_epoch(prereg[key]) for key in ('paper_start_utc', 'cutoff_utc'))
    _must(prereg['schema'] == SCHEMA and prereg['paper_only'] is True and cutoff - start == DURATION
          and _epoch(prereg['registered_utc']) < time.time() < start - 90, 'EXACT_PREREGISTERED_WINDOW_REQUIRED')
    _must(all(binding(prereg[k]['path']) == prereg[k] for k in ('candidate', 'experiment', 'interpreter')),
          'WAVE_PREREGISTRATION_SOURCE_CHANGED')
    clock = read_json(prereg['clock_file'])
    _must(fresh_coordinator_clock(clock), 'FRESH_ACCEPTED_CLOCK_REQUIRED')
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    (root / 'temp').mkdir(exist_ok=True)
    environment = worker_environment(root)
    environment.update(LANE_II_CLOCK_CONTROL=prereg['clock_file'], LANE_II_BOOT_ID=str(clock['boot_id']),
                       LANE_II_WAVE_PREREG_SHA256=prereg_ref['sha256'])
    command = [prereg['interpreter']['path'], '-I', '-S', '-B', str(candidate / 'study.py'), 'supervise',
               '--run-dir', str(run_dir), '--paper-start-utc', prereg['paper_start_utc'], '--cutoff-utc', prereg['cutoff_utc']]
    guard = Guard()
    status, control = root / 'STATUS.json', root / 'HEALTH_STOP.json'
    with application_lock(root / 'controller.lock') as acquired, (root / 'controller.log').open('xb') as output:
        _must(acquired and not run_dir.exists(), 'EXACT_SINGLE_WAVE_OWNER_REQUIRED')
        set_window_wake_lease(True)
        child = None
        try:
            child = subprocess.Popen(command, cwd=candidate, env=environment, stdout=output, stderr=output,
                                     creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            owner = process_identity(child.pid)
            stop = None
            while child.poll() is None:
                now = time.time()
                resource = sample(root)
                reason = guard.check(resource, max(0, cutoff - now))
                if now < cutoff and not fresh_coordinator_clock(read_json(prereg['clock_file'])):
                    reason = 'CLOCK_ADMISSION'
                packet = capture_state(run_dir)
                if reason and not stop and (run_dir / 'capture' / 'preregistration.json').exists() and now < cutoff:
                    stop = {'utc': utc(), 'reason': reason}
                    atomic_json(control, stop)
                    atomic_json(run_dir / 'capture' / 'STOP', stop)
                snap = packet.get('snapshot') or packet.get('receipt') or {}
                atomic_json(status, {'utc': utc(), 'state': 'PREWARM' if now < start else 'PAPER' if now < cutoff else 'CLOSEOUT',
                    'supervisor': owner, 'preregistration': prereg_ref, 'snapshot_utc': snap.get('utc'),
                    'profiles': snap.get('profiles', []), 'ticks': snap.get('ticks'), 'gaps': snap.get('gaps'),
                    'stop': stop, 'resources': resource})
                time.sleep(2)
            result = run_dir / 'PAPER_RUN_RESULT.json'
            atomic_json(status, {'utc': utc(), 'state': 'SEALED' if child.returncode == 0 else 'HELD',
                'supervisor': owner, 'supervisor_exit_code': child.returncode, 'preregistration': prereg_ref,
                'result': binding(result) if result.is_file() else None, 'stop': stop})
            return child.returncode
        finally:
            if child is not None and child.poll() is None:
                child.terminate()
                child.wait(timeout=10)
            set_window_wake_lease(False)


def checkpoint(root):
    root = Path(root).resolve()
    registration = read_json(root / 'PREREGISTRATION.json')
    start, cutoff = (_epoch(registration[k]) for k in ('paper_start_utc', 'cutoff_utc'))
    now = time.time()
    slot = min(int(cutoff), int(start + math.floor((now - start) / CHECKPOINT) * CHECKPOINT))
    status = read_json(root / 'STATUS.json')
    snapshot = status.get('snapshot_utc')
    owner = root / 'captures' / 'attempt-001' / 'operator.json'
    if owner.is_file():
        worker = read_json(owner)
        worker = {'pid': worker['pid'], 'live': (root / 'captures' / 'attempt-001' / 'exit.json').exists() is False
                  and any(row['pid'] == worker['pid'] for row in (status.get('resources') or {}).get('owned_processes', []))}
    else:
        worker = None
    record = {'observer': 'codex', 'observed_utc': utc(now), 'scheduled_epoch': slot, 'state': status['state'],
              'supervisor_live': same_process(status['supervisor']), 'worker': worker,
              'snapshot_age_seconds': round(now - _epoch(snapshot), 3) if snapshot else None,
              'status_age_seconds': round(now - _epoch(status['utc']), 3),
              'ticks': status.get('ticks'), 'stop': status.get('stop'), 'profiles': status.get('profiles', []),
              'resources': status.get('resources'), 'preregistration': status['preregistration']}
    path = root / 'health' / f'{slot}.json'
    if start <= slot <= cutoff and now < slot + CHECKPOINT and not path.exists():
        atomic_json(path, record)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('freeze', 'register', 'run', 'checkpoint'))
    for name in ('root', 'candidate', 'experiment', 'source', 'clock-file', 'interpreter'):
        parser.add_argument('--' + name, type=Path)
    args = parser.parse_args()
    if args.command == 'freeze':
        print(freeze(args.source, args.candidate, args.experiment))
    elif args.command == 'register':
        print(register(args.root, args.candidate, args.experiment, args.clock_file, args.interpreter))
    elif args.command == 'checkpoint':
        import json
        print(json.dumps(checkpoint(args.root)))
    else:
        raise SystemExit(run(args.root))


if __name__ == '__main__':
    main()
