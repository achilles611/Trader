"""Owned fixed-window paper qualification with a persistent offline consumer."""
from __future__ import annotations

import argparse
from collections import deque
import json
import math
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.lane_ii_continuous.capture import build_forward_command, capture_state, read_terminal, request_stop
from scripts.lane_ii_continuous.clock import calibrate_windows, project
from scripts.lane_ii_continuous.common import (BOOT_EPOCH_TOLERANCE_SECONDS, application_lock, atomic_json,
    binding, digest, file_hash, fresh_coordinator_clock, process_identity, read_json, same_process, utc)
from scripts.lane_ii_continuous.long_gate import (assess_window, campaign_state, require_flat_predecessor,
    short_predecessor, verified_hours, verify_long_candidate)
from scripts.lane_ii_continuous.long_status import CADENCE, dashboard, health, serve
from scripts.lane_ii_continuous.paper_risk import snapshot, validate_closure
from scripts.lane_ii_continuous.qualification import PHASE_SECONDS, _epoch, _must, bound_json
from scripts.lane_ii_continuous.resources import Guard, admission, sample
from scripts.lane_ii_continuous.service import load_config
from scripts.lane_ii_continuous.worker import deny_python_network, exited_owned_descendants, worker_environment

SCHEMA = 'LANE_II_LONG_QUALIFICATION_V1'
RECOVERY_LIMIT = 2
STUDY = 'collector'
REPLAY = 'hourly-replay'
RESEARCH_SECONDS = 480


def fixed_plan(phase, now, prewarm, predecessor):
    _must(phase in {'eight-hour', 'twenty-four-hour'}, 'LONG_PHASE_REQUIRED')
    start = math.ceil((now + prewarm) / CADENCE) * CADENCE
    cutoff = start + PHASE_SECONDS[phase]
    return {'schema': SCHEMA, 'phase': phase, 'start_epoch': start, 'cutoff_epoch': cutoff,
            'paper_start_utc': utc(start), 'cutoff_utc': utc(cutoff), 'predecessor': predecessor}


def continuation_start(plan, now, prewarm):
    start = max(plan['start_epoch'], math.ceil((now + prewarm) / 300) * 300)
    return start if start + 300 <= plan['cutoff_epoch'] else None


def set_window_wake_lease(required):
    if sys.platform != 'win32':
        return
    import ctypes

    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.SetThreadExecutionState.argtypes = [ctypes.c_uint32]
    kernel.SetThreadExecutionState.restype = ctypes.c_uint32
    flags = 0x80000000 | (1 if required else 0)
    if not kernel.SetThreadExecutionState(flags):
        raise OSError(ctypes.get_last_error(), 'WINDOWS_WINDOW_WAKE_LEASE_UNAVAILABLE')


def verify_release(config):
    release = bound_json(config['hourly_release'])
    _must(release['schema'] == 'LANE_II_HOURLY_VERIFICATION_V1' and release['status'] == 'PASS'
          and release['split_vs_contiguous_parity'] is True and release['no_capture_mutation'] is True
          and release['preflight_test_exit_code'] == 0, 'VERIFIED_HOURLY_RELEASE_REQUIRED')
    for name, expected in release['source_sha256'].items():
        _must(Path(name).name == name, 'RELEASE_SOURCE_MUST_BE_LOCAL')
        _must(file_hash(Path(config['app']) / 'scripts/lane_ii_continuous' / name) == expected,
              'TESTED_HOURLY_SOURCE_CHANGED:' + name)
    _must({'hourly_replay.py', 'long_qualification.py', 'long_gate.py', 'capture_checkpoint.py',
           'long_closeout.py', 'long_status.py', 'long_dashboard.html', 'capture.py', 'paper_risk.py',
           'qualification.py', 'qualification_replay.py', 'install_long.ps1', 'long_research.py'}
          <= release['source_sha256'].keys(), 'EXACT_TESTED_LONG_SOURCES_REQUIRED')
    _must({'sealed_replay_verification', 'preflight_tests'} <= release['evidence'].keys()
          and release['candidate_sha256'] == config['candidate']['source_manifest_sha256']
          and release['risk_manifest_sha256'] == config['paper_risk_manifest']['sha256'],
          'VERIFICATION_CANDIDATE_OR_CAMPAIGN_CHANGED')
    for ref in release['evidence'].values():
        _must(binding(ref['path']) == ref, 'HOURLY_VERIFICATION_ARTIFACT_CHANGED')
    review = bound_json(config['release_review'])
    _must(review['schema'] == 'LANE_II_LONG_RELEASE_REVIEW_V1' and review['decision'] == 'ACCEPT_EIGHT_HOUR_PAPER'
          and review['holds'] == [] and review['release'] == config['hourly_release']
          and review['candidate_sha256'] == release['candidate_sha256']
          and review['risk_manifest_sha256'] == release['risk_manifest_sha256'], 'INDEPENDENT_LONG_RELEASE_REVIEW_REQUIRED')
    return release


def prepare(root, repo, candidate_path, observation, review, release, release_review, *, research=None, port=8889):
    root, repo = Path(root).resolve(), Path(repo).resolve()
    _must(not root.exists(), 'NEW_EXTERNAL_LONG_RUNTIME_REQUIRED')
    observation, review = binding(observation), binding(review)
    original, terminal = short_predecessor(observation, review)
    candidate = bound_json(binding(candidate_path))
    verify_long_candidate(original['candidate'], candidate)
    require_flat_predecessor(original, terminal)
    _must(root.drive == Path(original['paper_risk_manifest']['path']).drive, 'USE_CAMPAIGN_RUNTIME_VOLUME')
    app = root / 'app'
    for part in ('lane_ii_continuous', 'slim_paper_operator', 'overnight_paper', 'btc_next_paper'):
        relative = Path('scripts') / part
        shutil.copytree(repo / relative, app / relative, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    source = {'schema': 'LANE_II_FROZEN_APPLICATION_V1',
              'files': [binding(path) for path in sorted(app.rglob('*')) if path.is_file()]}
    config = {key: original[key] for key in ('interpreter', 'interpreter_sha256', 'interpreter_windows', 'limits',
              'paper_only', 'model_enrichment_enabled', 'maximum_replay_seconds', 'closeout_wait_seconds',
              'prewarm_seconds', 'campaign_id', 'paper_risk_manifest')}
    config.update({'schema': SCHEMA, 'root': str(root), 'app': str(app), 'source_sha256': digest(source),
                   'candidate': candidate, 'port': port, 'short_observation': observation, 'short_review': review,
                   'short_predecessor': terminal, 'hourly_release': binding(release), 'release_review': binding(release_review),
                   'research': binding(research) if research else None, 'promote_after_qualified_eight_hour': True})
    verify_release(config)
    atomic_json(root / 'SOURCE.json', source)
    atomic_json(root / 'CONFIG.json', config)
    (root / 'temp').mkdir()
    atomic_json(root / 'CONTROL.json', {'mode': 'run', 'utc': utc()})
    return config


class OwnedChild:
    def __init__(self, argv, cwd, environment, folder, *, release, source_sha256, segment):
        from scripts.slim_paper_operator.owned_job import OwnedJob

        folder.mkdir(parents=True, exist_ok=True)
        self.folder = folder
        self.log = (folder / 'output.log').open('xb', buffering=0)
        self.job = None
        try:
            self.process = subprocess.Popen(argv, cwd=cwd, env=environment, stdin=subprocess.PIPE,
                stdout=self.log, stderr=self.log, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            self.job = OwnedJob(self.process)
            self.identity = process_identity(self.process.pid)
            self.source = source_sha256
            atomic_json(folder / 'operator.json', {'pid': self.process.pid,
                'creation_utc': utc(self.identity['creation_epoch']), **self.identity, 'command': argv,
                'source_manifest_sha256': source_sha256, 'segment_id': segment, 'owned_job_before_startup_gate': True})
            self.process.stdin.write(release)
            self.process.stdin.close()
        except BaseException:
            if self.job is not None:
                self.job.close()
            elif getattr(self, 'process', None) is not None and self.process.poll() is None:
                self.process.kill()
            if getattr(self, 'process', None) is not None:
                self.process.wait(timeout=15)
            self.log.close()
            raise

    def close(self, *, force=False, reason=None):
        running = self.process.poll() is None
        _must(force or not running, 'LIVE_OWNED_PROCESS_REQUIRES_EXPLICIT_STOP')
        children = [] if running else sorted(exited_owned_descendants(self.job, self.process))
        self.job.close()
        code = self.process.wait(timeout=15)
        self.log.close()
        receipt = {'pid': self.identity['pid'], 'creation_epoch': self.identity['creation_epoch'],
                   'boot_epoch': self.identity['boot_epoch'], 'executable_sha256': self.identity['executable_sha256'],
                   'exit_code': code, 'observed_exit_utc': utc(), 'observed_exit_monotonic': time.monotonic(),
                   'owned_children_at_parent_exit': children, 'owned_job_closed': True,
                   'retained_handle_exit_observed': True, 'forced_closure': bool(running or children),
                   'reason': reason, 'source_manifest_sha256': self.source}
        atomic_json(self.folder / 'exit.json', receipt)
        return receipt


class LongRun:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.config = load_config(self.root)
        _must(self.config['schema'] == SCHEMA, 'FROZEN_LONG_RUNTIME_REQUIRED')
        verify_release(self.config)
        original, terminal = short_predecessor(self.config['short_observation'], self.config['short_review'])
        _must(terminal == self.config['short_predecessor'] and original['paper_risk_manifest'] == self.config['paper_risk_manifest'],
              'ORIGINAL_SHORT_OR_CAMPAIGN_CHANGED')
        self.owner = process_identity()
        self.configuration = digest(self.config)
        self.state_path = self.root / 'STATE.json'
        self.data = (read_json(self.state_path) if self.state_path.exists() else
                     {'config_sha256': self.configuration, 'phase': None, 'plans': {}, 'attempts': {}, 'crashes': {},
                      'assessments': {}, 'hold': None, 'state': 'PRELAUNCH'})
        _must(self.data['config_sha256'] == self.configuration, 'PERSISTED_LONG_CONFIGURATION_CHANGED')
        self.collector = self.replayer = self.auditor = self.research = None
        self.guard = Guard(self.config['limits'])
        self.samples = deque(maxlen=60)
        self.clock = None
        self.next_calibration = 0.0
        self.next_status = 0.0
        self.last_size = self.last_sample = 0
        self.last_published_state = None

    @property
    def plan(self):
        phase = self.data['phase']
        return bound_json(self.data['plans'][phase]) if phase else None

    @property
    def attempts(self):
        return self.data['attempts'][self.data['phase']]

    @property
    def current(self):
        return self.attempts[-1] if self.plan and self.attempts else None

    def save(self):
        atomic_json(self.state_path, self.data)

    def hold(self, reason, *, state='ADMISSION_HELD'):
        self.data.update(state=state, hold=reason)
        self.save()

    def now(self):
        if self.clock and self.clock.get('state') == 'accepted':
            return project(self.clock['anchor'])
        return time.time()

    def calibrate(self):
        path = self.root / 'CLOCK_CONTROL.json'
        previous = self.clock or (read_json(path) if path.is_file() else {})
        anchor = previous.get('anchor') or previous.get('last_accepted_anchor')
        boot = self.owner['boot_epoch']
        if anchor and abs(anchor['boot_id'] - boot) <= BOOT_EPOCH_TOLERANCE_SECONDS:
            boot = anchor['boot_id']
        else:
            anchor = None
        self.clock = calibrate_windows(boot_id=boot, prior_anchor=anchor)
        if self.clock.get('state') != 'accepted' and anchor:
            self.clock['last_accepted_anchor'] = anchor
        atomic_json(path, self.clock)
        self.next_calibration = time.monotonic() + (300 if self.clock['state'] == 'accepted' else 60)

    def new_plan(self, phase, predecessor):
        require_flat_predecessor(self.config, predecessor)
        if phase == 'twenty-four-hour':
            prior = bound_json(self.data['assessments']['eight-hour'])
            _must(prior['status'] == 'QUALIFIED' and prior['terminal_receipt'] == predecessor
                  and prior['config_sha256'] == self.configuration, 'QUALIFIED_EIGHT_HOUR_PREDECESSOR_REQUIRED')
        path = self.root / phase / 'PLAN.json'
        planned = read_json(path) if path.is_file() else fixed_plan(phase, self.now(), self.config['prewarm_seconds'], predecessor)
        _must(planned['predecessor'] == predecessor and planned['phase'] == phase
              and planned['cutoff_epoch'] - planned['start_epoch'] == PHASE_SECONDS[phase], 'FIXED_PLAN_CHANGED')
        if not path.exists():
            atomic_json(path, planned)
        self.data['phase'] = phase
        self.data['plans'][phase] = binding(path)
        self.data['attempts'].setdefault(phase, [])
        self.data['crashes'].setdefault(phase, [])
        self.data.update(state='PLANNED', hold=None)
        self.save()

    def start_collector(self, predecessor, start):
        plan = self.plan
        require_flat_predecessor(self.config, predecessor)
        _must(start < plan['cutoff_epoch'] and start >= plan['start_epoch'], 'ATTEMPT_OUTSIDE_FIXED_WINDOW')
        count = len(self.attempts) + 1
        _must(count <= RECOVERY_LIMIT + 1, 'PAPER_RESTART_BUDGET_EXHAUSTED')
        folder = self.root / plan['phase'] / f'attempt-{count:03}'
        _must(not folder.exists(), 'ATTEMPT_PATH_ALREADY_USED')
        segment = f'{plan["phase"]}-{count:03}'
        remaining = plan['cutoff_epoch'] - self.now()
        admitted = admission(list(self.samples), remaining, limits=self.config['limits'])
        _must(admitted['admitted'], 'RESOURCE:' + ','.join(admitted['reasons']))
        attempt = {'ordinal': count, 'segment_id': segment, 'path': str(folder), 'start_epoch': start,
                   'cutoff_epoch': plan['cutoff_epoch'], 'predecessor': predecessor, 'state': 'LAUNCHING',
                   'offline_attempts': [], 'created_utc': utc()}
        self.attempts.append(attempt)
        self.data.update(state='STARTING', hold=None)
        self.save()
        candidate = self.config['candidate']
        command = build_forward_command(candidate, folder, utc(start), plan['cutoff_utc'])
        command[0] = self.config['interpreter']
        environment = worker_environment(self.root)
        prior = bound_json(predecessor)
        if prior.get('schema') == 'LANE_II_CRASH_HANDOFF_V1':
            _must(start >= prior['restart_not_before_epoch'] and plan['cutoff_epoch'] == prior['fixed_cutoff_epoch'],
                  'CRASH_HANDOFF_FIXED_WINDOW_REQUIRED')
        token = ('CRASH:' if prior.get('schema') == 'LANE_II_CRASH_HANDOFF_V1' else '') + predecessor['sha256']
        manifest = self.config['paper_risk_manifest']
        environment.update(LANE_II_CLOCK_CONTROL=str(self.root / 'CLOCK_CONTROL.json'), LANE_II_BOOT_ID=str(self.clock['boot_id']),
                           LANE_II_PAPER_RISK_MANIFEST=manifest['path'], LANE_II_PAPER_RISK_MANIFEST_SHA256=manifest['sha256'],
                           LANE_II_PAPER_SEGMENT_ID=segment, LANE_II_PAPER_PREDECESSOR_RECEIPT_SHA256=token,
                           LANE_II_CAPTURE_CHECKPOINT_SECONDS=str(CADENCE), LANE_II_CAPTURE_RECOVERY_SYNC='1',
                           LANE_II_CAPTURE_CHECKPOINT_ORIGIN_EPOCH=str(plan['start_epoch']))
        atomic_json(folder / 'ADMISSION.json', {'schema': SCHEMA, 'utc': utc(), 'plan': self.data['plans'][plan['phase']],
                    'start_utc': utc(start), 'cutoff_utc': plan['cutoff_utc'], 'segment_id': segment, 'clock': self.clock,
                    'capacity': admitted, 'paper_risk_manifest': manifest, 'predecessor': predecessor,
                    'source_sha256': self.config['source_sha256'], 'candidate_sha256': candidate['source_manifest_sha256']})
        self.collector = OwnedChild(command, candidate['candidate_dir'], environment, folder,
                                    release=candidate['startup_release'].encode(),
                                    source_sha256=candidate['source_manifest_sha256'], segment=segment)
        attempt['state'] = 'COLLECTING'
        self.data['state'] = 'COLLECTING'
        self.save()

    def offline(self, module, arguments, folder, segment):
        directory = Path(self.config['app']) / 'scripts/lane_ii_continuous'
        argv = [self.config['interpreter'], '-I', '-B', str(directory / 'offline.py'), '--startup-gate', '--',
                str(directory / module), *map(str, arguments)]
        child = OwnedChild(argv, self.config['app'], worker_environment(self.root), folder,
                           release=b'LANE_II_OFFLINE_START\n', source_sha256=self.config['source_sha256'], segment=segment)
        try:
            import psutil
            if sys.platform == 'win32':
                psutil.Process(child.identity['pid']).nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            pass
        return child

    def start_replay(self):
        attempt, plan = self.current, self.plan
        run = Path(attempt['path'])
        candidate = self.config['candidate']
        count = len(attempt['offline_attempts']) + 1
        _must(count <= RECOVERY_LIMIT + 1, 'OFFLINE_REPLAY_RESTART_BUDGET_EXHAUSTED')
        folder = run / f'replay-worker-{count:03}'
        attempt['offline_attempts'].append(str(folder))
        self.save()
        seconds = max(60, int(plan['cutoff_epoch'] - self.now() + 1800))
        self.replayer = self.offline('hourly_replay.py', ['--candidate', candidate['candidate_dir'], '--run-dir', run,
            '--manifest-sha256', candidate['source_manifest_sha256'], '--output', self.evidence_dir() / 'replay',
            '--final-inputs', run / 'FINAL_INPUTS.json',
            '--maximum-input-bytes', 16 * 2**30, '--maximum-events', 8_000_000, '--maximum-rss-bytes', 256 * 2**20,
            '--maximum-seconds', seconds, '--restart-policy', 'rebuild' if count > 1 else 'hold'], folder, attempt['segment_id'])

    def evidence_dir(self, attempt=None):
        run = Path((attempt or self.current)['path'])
        return run.parent / 'verification' / run.name

    def verified_hour_epochs(self):
        epochs = set()
        for attempt in self.attempts:
            directory = self.evidence_dir(attempt) / 'replay' / 'hours'
            epochs.update(int(path.stem) for path in directory.glob('*.json')
                          if path.stem.isdecimal() and int(path.stem) - 3600 >= attempt['start_epoch'])
        return epochs

    def service_research(self, control):
        plan = self.plan
        reference = self.config['research']
        if not reference or not plan or self.data['state'] == 'OWNERSHIP_HELD':
            return
        status = self.root / 'RESEARCH_STATUS.json'
        folder = self.root / 'offline-research' / 'worker'
        if self.research is not None:
            if self.research.process.poll() is not None:
                result = self.research.close()
                self.research = None
                prior = read_json(status) if status.is_file() else {}
                if result['exit_code'] != 0 and prior.get('state') != 'HELD':
                    atomic_json(status, {'state': 'HELD', 'reason': 'OPTIONAL_WORKER_EXIT', 'utc': utc()})
            elif control != 'run' or self.now() >= plan['start_epoch'] + CADENCE - 60:
                self.stop_research('QUALIFICATION_HAS_PRIORITY')
            return
        if status.is_file() or folder.exists() or plan['phase'] != 'eight-hour' or control != 'run':
            return
        if self.now() + RESEARCH_SECONDS >= plan['start_epoch'] + CADENCE - 90:
            return
        if not admission(list(self.samples), plan['cutoff_epoch'] - self.now(), limits=self.config['limits'], heavy=True)['admitted']:
            return
        try:
            bound_json(reference)
            self.research = self.offline('long_research.py', ['run', '--plan', reference['path'], '--output', folder.parent / 'results',
                '--status', status, '--maximum-seconds', RESEARCH_SECONDS], folder, 'retained-research')
        except (OSError, KeyError, ValueError, TypeError) as exc:
            atomic_json(status, {'state': 'HELD', 'reason': str(exc)[:180], 'utc': utc()})

    def stop_research(self, reason):
        self.research.close(force=True, reason=reason)
        self.research = None
        atomic_json(self.root / 'RESEARCH_STATUS.json', {'state': 'HELD', 'reason': reason, 'utc': utc()})

    def crash(self, component, reason, *, epoch=None, stamp_kind='observed_exit', attempt=None):
        plan = self.plan
        if plan is None:
            return
        rows = self.data['crashes'][plan['phase']]
        identity = f'{component}:{attempt}'
        if any(row['key'] == identity for row in rows):
            return
        event = {'key': identity, 'component': component, 'reason': reason, 'utc': utc(epoch),
                 'time_basis': stamp_kind, 'attempt': attempt, 'count': len(rows) + 1}
        path = self.root / plan['phase'] / 'crashes' / f'{event["count"]:03}.json'
        _must(not path.exists(), 'CRASH_EVENT_ALREADY_RECORDED')
        atomic_json(path, event)
        rows.append(event)
        self.save()

    def reconcile_crashes(self):
        for phase, previous in self.data['crashes'].items():
            paths = sorted((self.root / phase / 'crashes').glob('*.json'))
            events = [read_json(path) for path in paths]
            _must(len(events) <= 32 and events[:len(previous)] == previous
                  and [event['count'] for event in events] == list(range(1, len(events) + 1)), 'CRASH_HISTORY_CHANGED')
            self.data['crashes'][phase] = events
        self.save()

    def observed_attempt_end(self, exit_receipt):
        attempt = self.current
        run = Path(attempt['path'])
        native = read_terminal(run)
        terminal = native['receipt'] if native else None
        complete = (terminal and terminal['state'] == 'COMPLETE' and terminal['reason'] == 'DEADLINE'
                    and terminal['cutoff_utc'] == self.plan['cutoff_utc']
                    and exit_receipt['exit_code'] == 0 and not exit_receipt['forced_closure'])
        attempt['exit'] = binding(run / 'exit.json')
        if complete:
            attempt['state'] = 'CLOSING'
            self.data['state'] = 'CLOSING'
        else:
            attempt['state'] = 'INTERRUPTED'
            reason = exit_receipt.get('reason') or (terminal['reason'] if terminal else 'COLLECTOR_EXIT_WITHOUT_SEAL')
            attempt['reason'] = reason
            if reason != 'OPERATOR_STOP':
                self.crash(STUDY, reason, epoch=_epoch(exit_receipt['observed_exit_utc']), attempt=attempt['ordinal'])
            if self.replayer is not None:
                self.replayer.close(force=True, reason='COLLECTOR_INTERRUPTED')
                self.replayer = None
            self.data.update(state='RECOVERING' if reason != 'OPERATOR_STOP' else 'OPERATOR_STOPPED', hold=reason)
        self.save()

    def reconcile_manager_restart(self):
        attempt = self.current
        if not attempt:
            return
        run = Path(attempt['path'])
        for folder in (run, *(Path(path) for path in attempt['offline_attempts']),
                       *(Path(path) for path in attempt.get('audit_workers', []))):
            owner_path = folder / 'operator.json'
            if owner_path.is_file() and same_process(read_json(owner_path)) is not False:
                self.hold('PRIOR_OWNED_PROCESS_ABSENCE_UNPROVEN', state='OWNERSHIP_HELD')
                return
        if attempt['state'] not in {'COLLECTING', 'LAUNCHING'}:
            if self.data['state'] == 'OWNERSHIP_HELD':
                self.data.update(state='RECOVERING' if attempt['state'] == 'INTERRUPTED' else attempt['state'], hold=None)
                self.save()
            return
        exit_path = run / 'exit.json'
        if exit_path.is_file():
            self.observed_attempt_end(read_json(exit_path))
            return
        attempt.update(state='INTERRUPTED', reason='SUPERVISOR_RESTART_WITHOUT_RETAINED_EXIT')
        self.crash('supervisor', attempt['reason'], stamp_kind='detected_after_restart', attempt=attempt['ordinal'])
        self.hold(attempt['reason'], state='RECOVERING')

    def restart_predecessor(self):
        attempt = self.current
        run = Path(attempt['path'])
        state = campaign_state(self.config['paper_risk_manifest'])
        if state['phase'] == 'CLOSED':
            endpoint = state['last_terminal']
            crashed = endpoint.get('kind') == 'CRASH_HANDOFF'
            endpoint_path = Path(endpoint['handoff_path'] if crashed else endpoint['receipt_path']).resolve()
            if endpoint.get('handoff_sha256' if crashed else 'receipt_sha256') == attempt['predecessor']['sha256']:
                return attempt['predecessor']
            if crashed and endpoint_path == run / 'CRASH_HANDOFF.json':
                return binding(endpoint_path)
            if endpoint_path == run / 'capture' / 'receipt.json' and (run / 'RISK_CLOSURE.json').is_file():
                result = validate_closure(run, self.config['paper_risk_manifest']['sha256'])
                terminal = read_terminal(run)
                _must(result['flat'] and terminal['closure']['capture_sealed'] and (run / 'exit.json').is_file(),
                      'INTERRUPTED_ATTEMPT_HAS_OPEN_OR_UNPROVEN_EXPOSURE')
                return binding(endpoint_path)
        from scripts.lane_ii_continuous.capture_checkpoint import recover_flat_attempt
        recovery = recover_flat_attempt(run, self.config['paper_risk_manifest'], attempt['segment_id'])
        return recovery['handoff']

    def poll_replay(self):
        if self.replayer is None or self.replayer.process.poll() is None:
            return
        attempt = self.current
        replay = self.evidence_dir() / 'replay'
        child = self.replayer
        self.replayer = None
        result = child.close()
        if result['exit_code'] == 0 and not result['forced_closure'] and (replay / 'RESULT.json').is_file():
            attempt['replay_complete'] = binding(replay / 'RESULT.json')
        else:
            status = replay / 'STATUS.json'
            detail = read_json(status) if status.is_file() else {}
            reason = str(detail.get('hold') or detail.get('reason') or f'REPLAY_EXIT_{result["exit_code"]}')[:200]
            self.crash(REPLAY, reason, epoch=_epoch(result['observed_exit_utc']),
                       attempt=f'{attempt["ordinal"]}.{len(attempt["offline_attempts"])}')
            attempt['replay_retry_epoch'] = self.now() + 60
            if len(attempt['offline_attempts']) > RECOVERY_LIMIT or detail.get('state') == 'HELD':
                attempt['replay_hold'] = reason
        self.save()

    def service_replay(self):
        attempt = self.current
        if not attempt or attempt['state'] not in {'COLLECTING', 'CLOSING'}:
            return
        self.poll_replay()
        if self.replayer or self.auditor or attempt.get('replay_hold') or attempt.get('replay_complete'):
            return
        if self.now() < max(attempt.get('replay_retry_epoch', 0), attempt.get('closeout_retry_epoch', 0)):
            return
        run = Path(attempt['path'])
        if not (run / 'capture' / 'preregistration.json').is_file():
            return
        if len(attempt['offline_attempts']) > RECOVERY_LIMIT:
            attempt['replay_hold'] = 'OFFLINE_REPLAY_RESTART_BUDGET_EXHAUSTED'
            self.save()
            return
        if not admission(list(self.samples), max(0, self.plan['cutoff_epoch'] - self.now()), limits=self.config['limits'])['admitted']:
            return
        self.start_replay()

    def start_final_audit(self):
        attempt = self.current
        candidate = self.config['candidate']
        run = Path(attempt['path'])
        workers = attempt.setdefault('audit_workers', [])
        folder = run / f'audit-worker-{len(workers) + 1:03}'
        _must(len(workers) <= RECOVERY_LIMIT, 'TERMINAL_AUDIT_RESTART_BUDGET_EXHAUSTED')
        workers.append(str(folder))
        self.save()
        self.auditor = self.offline('long_closeout.py', ['--candidate', candidate['candidate_dir'], '--run-dir', run,
            '--manifest-sha256', candidate['source_manifest_sha256'], '--output', self.evidence_dir() / folder.name],
            folder, attempt['segment_id'])

    def assess(self, status, reason=None, *, evidence=None):
        plan = self.plan
        path = self.root / plan['phase'] / 'ASSESSMENT.json'
        value = {'schema': SCHEMA, 'phase': plan['phase'], 'status': status, 'holds': [reason] if reason else [],
                 'utc': utc(), 'plan': self.data['plans'][plan['phase']], 'config_sha256': self.configuration,
                 'campaign_id': self.config['campaign_id'], 'risk_manifest_sha256': self.config['paper_risk_manifest']['sha256'],
                 'crash_events': self.data['crashes'][plan['phase']], 'paper_only': True,
                 'funded_authority': False, 'profitability_qualified': False, **(evidence or {})}
        if path.is_file():
            prior = read_json(path)
            _must({k: v for k, v in prior.items() if k != 'utc'} == {k: v for k, v in value.items() if k != 'utc'},
                  'PHASE_ASSESSMENT_ALREADY_SEALED')
        else:
            atomic_json(path, value)
        self.data['assessments'][plan['phase']] = binding(path)
        self.data.update(state='QUALIFIED' if status == 'QUALIFIED' else 'FINISHED_HELD', hold=reason)
        self.save()

    def final_assessment(self):
        from scripts.lane_ii_continuous.research import analyze_closeout

        plan, attempt = self.plan, self.current
        run = Path(attempt['path'])
        terminal_ref = binding(run / 'capture' / 'receipt.json')
        terminal = bound_json(terminal_ref)
        risk = validate_closure(run, self.config['paper_risk_manifest']['sha256'])
        require_flat_predecessor(self.config, terminal_ref)
        replay = bound_json(attempt['replay_complete'])
        final_ref = binding(run / 'FINAL_INPUTS.json')
        final = bound_json(final_ref)['bindings']
        account = read_json(run / 'TERMINAL_ACCOUNTING.json')
        audit = bound_json(final['raw_audit'])
        supervisor = bound_json(final['supervisor_result'])
        _must(replay['final_inputs'] == final_ref and audit['status'] == 'PASS_RETAINED_CAPTURE_INTEGRITY_ONLY'
              and audit['problem_counts'] == {} and supervisor['bindings']['raw_audit'] == final['raw_audit']
              and supervisor['bindings']['receipt'] == terminal_ref and supervisor['receipt'] == terminal
              and supervisor['worker_exit_code'] == 0 and supervisor['forced_closure'] is False,
              'BOUND_TERMINAL_RAW_AND_EXIT_PROOF_REQUIRED')
        accounting = bound_json(account)['components']['study']['accounting']
        _must(accounting['status'] == 'PASS_RETAINED_PROFILE_ACCOUNTING'
              and accounting['counts']['unresolved_positions'] == 0, 'TERMINAL_ACCOUNTING_REQUIRED')
        hourly = verified_hours(plan, replay, self.evidence_dir() / 'replay')
        outages = [row for row in self.data['crashes'][plan['phase']] if row['component'] != REPLAY]
        evaluated = assess_window(plan, terminal, replay, risk, hourly, attempts=len(self.attempts), crashes=len(outages))
        report = analyze_closeout({'artifacts': {'terminal_receipt': terminal_ref, 'raw_audit': final['raw_audit'],
            'accounting': account, 'prospective_replay': attempt['replay_complete']},
            'source_sha256': self.config['source_sha256'], 'config_sha256': self.configuration}, self.evidence_dir() / 'report')
        _must(not report['result']['rejected_reasons'] and report['result']['components']['study']['closure']['replay']
              == 'PASS_PROSPECTIVE_CAUSAL_PARITY', 'AUTOMATIC_CLOSEOUT_REPORT_REQUIRED')
        self.assess(evaluated['status'], ','.join(evaluated['holds']) or None, evidence={**evaluated,
            'terminal_receipt': terminal_ref, 'risk_closure': binding(run / 'RISK_CLOSURE.json'),
            'replay': attempt['replay_complete'], 'raw_audit': final['raw_audit'], 'supervisor': final['supervisor_result'],
            'accounting': account, 'report': report['artifacts']})

    def finish_if_ready(self):
        attempt = self.current
        run = Path(attempt['path'])
        final_path = run / 'FINAL_INPUTS.json'
        if self.auditor and self.auditor.process.poll() is not None:
            result = self.auditor.close()
            self.auditor = None
            if result['exit_code'] != 0 or result['forced_closure'] or not final_path.is_file():
                self.assess('HOLD', 'TERMINAL_AUDIT_FAILED')
                return
        status_file = self.evidence_dir() / 'replay' / 'STATUS.json'
        replay_state = read_json(status_file).get('state') if status_file.is_file() else None
        ready = replay_state == 'AWAITING_FINAL_INPUTS' or attempt.get('replay_hold') or not self.replayer
        if (not final_path.is_file() and not self.auditor and ready
                and self.now() >= attempt.get('closeout_retry_epoch', 0)
                and admission(list(self.samples), 0, limits=self.config['limits'], heavy=True)['admitted']):
            self.start_final_audit()
        if final_path.is_file() and attempt.get('replay_complete') and not self.auditor:
            self.final_assessment()
        elif final_path.is_file() and attempt.get('replay_hold') and not self.auditor:
            self.assess('HOLD', 'OFFLINE_REPLAY:' + attempt['replay_hold'])

    def guard_closeout(self):
        if not (self.replayer or self.auditor):
            return False
        reason = self.guard.check(self.samples[-1], 0)
        if not reason:
            return False
        attempt = self.current
        if self.replayer:
            exit_receipt = self.replayer.close(force=True, reason=reason)
            self.replayer = None
            self.crash(REPLAY, reason, epoch=_epoch(exit_receipt['observed_exit_utc']),
                       attempt=f'{attempt["ordinal"]}.{len(attempt["offline_attempts"])}')
        if self.auditor:
            self.auditor.close(force=True, reason=reason)
            self.auditor = None
        attempt['closeout_retry_epoch'] = self.now() + 60
        self.data['hold'] = 'CLOSEOUT_RESOURCE:' + reason
        self.save()
        return True

    def collector_snapshot(self):
        if not self.current:
            return {}
        run = Path(self.current['path'])
        native = capture_state(run)
        return native.get('snapshot') or native.get('receipt') or {}

    def publish_status(self, *, immediate=False):
        plan = self.plan
        attempt = self.current
        snap = self.collector_snapshot()
        before = bound_json(attempt['predecessor'])['account_carryover'] if attempt else {}
        profiles = []
        for profile in snap.get('profiles', []):
            prior = before.get(profile['id'], {})
            profiles.append({'id': profile['id'], 'realized_net': prior.get('cumulative_realized', 0) + profile['realized_pnl'],
                'entries': prior.get('cumulative_entries', 0) + profile['entries'],
                'open_positions': profile['unresolved_positions']})
        try:
            risk = snapshot(campaign_state(self.config['paper_risk_manifest']))
        except (OSError, ValueError, KeyError, sqlite3.Error):
            risk = None
        replay_state = {}
        if attempt:
            replay = self.evidence_dir() / 'replay'
            path = replay / 'STATUS.json'
            if path.is_file():
                replay_state = read_json(path)
            if attempt.get('replay_hold'):
                replay_state.update(state='HOLD', reason=attempt['replay_hold'])
            replay_state['hours'] = len(self.verified_hour_epochs())
        crashes = self.data['crashes'][plan['phase']] if plan else []
        observations = self.samples[-1] if self.samples else None
        value = {'schema': SCHEMA, 'as_of_utc': utc(), 'owner': self.owner, 'plan': plan, 'state': self.data['state'],
                 'hold': self.data['hold'], 'collector': {'owner': self.collector.identity if self.collector else None,
                     'snapshot_utc': snap.get('utc'), 'attempt': attempt['ordinal']} if attempt else None,
                 'replay': replay_state, 'profiles': profiles, 'risk': risk, 'crashes': crashes, 'crash_count': len(crashes),
                 'collector_crash_count': sum(row['component'] != REPLAY for row in crashes),
                 'ticks': snap.get('ticks'), 'resource': observations, 'clock_state': (self.clock or {}).get('state'),
                 'research': read_json(self.root / 'RESEARCH_STATUS.json') if (self.root / 'RESEARCH_STATUS.json').is_file() else None,
                 'qualification': self.data['assessments'], 'paper_only': True}
        atomic_json(self.root / 'STATUS.json', value)
        signature = (plan and plan['phase'], value['state'], len(crashes))
        if plan and (immediate or signature != self.last_published_state):
            dashboard(self.root, immediate=True)
        self.last_published_state = signature
        self.next_status = time.monotonic() + 5

    def guard_collector(self, control):
        attempt = self.current
        run = Path(attempt['path'])
        process = self.collector.process
        if process.poll() is not None:
            exit_receipt = self.collector.close(reason=attempt.get('stop_reason'))
            self.collector = None
            self.observed_attempt_end(exit_receipt)
            return
        current_mono = time.monotonic()
        tape = run / 'capture' / 'events.jsonl'
        size = tape.stat().st_size if tape.is_file() else 0
        rate = max(0, size - self.last_size) / max(1, current_mono - self.last_sample)
        self.last_sample, self.last_size = current_mono, size
        now = self.now()
        remaining = max(0, self.plan['cutoff_epoch'] - now)
        reason = self.guard.check(self.samples[-1], remaining, rate)
        if self.research and reason in {'CPU_PERSISTENT', 'RAM_PERSISTENT', 'OWNED_RSS_CEILING', 'OWNED_PROCESS_CEILING'}:
            self.stop_research(reason)
            reason = None
        if control == 'stop':
            reason = 'OPERATOR_STOP'
        if now >= self.plan['cutoff_epoch'] + 20:
            marker = run / 'capture' / 'COLLECTION_CLOSED.json'
            if not marker.is_file() or now >= self.plan['cutoff_epoch'] + 300:
                reason = 'FIXED_CUTOFF_CLOSE_TIMEOUT'
        if now < self.plan['cutoff_epoch'] and not reason and time.monotonic() >= self.next_status:
            snap = self.collector_snapshot()
            updated = _epoch(snap['utc']) if snap.get('utc') else _epoch(attempt['created_utc'])
            if now - updated > 35:
                reason = 'COLLECTOR_HEARTBEAT_STALE'
        if reason and not attempt.get('stop_reason'):
            attempt.update(stop_reason=reason, stop_requested_epoch=now)
            self.save()
        if (attempt.get('stop_reason') and (run / 'capture' / 'preregistration.json').is_file()
                and not (run / 'capture' / 'STOP').is_file()):
            request_stop(run, attempt['stop_reason'])
        if (attempt.get('stop_reason') and now >= attempt['stop_requested_epoch'] + self.config['closeout_wait_seconds']):
            exit_receipt = self.collector.close(force=True, reason=attempt['stop_reason'])
            self.collector = None
            self.observed_attempt_end(exit_receipt)

    def advance(self):
        control = read_json(self.root / 'CONTROL.json')['mode']
        phase, plan = self.data['phase'], self.plan
        if self.collector:
            self.guard_collector(control)
        self.service_research(control)
        if self.data['state'] == 'OWNERSHIP_HELD':
            self.reconcile_manager_restart()
            if self.data['state'] == 'OWNERSHIP_HELD':
                return
        if self.current and self.current['state'] in {'COLLECTING', 'CLOSING'}:
            self.service_replay()
        if phase and phase in self.data['assessments']:
            assessed = bound_json(self.data['assessments'][phase])
            if (control == 'run' and phase == 'eight-hour' and assessed['status'] == 'QUALIFIED'
                    and self.config['promote_after_qualified_eight_hour'] and self.ready_for('twenty-four-hour')):
                self.new_plan('twenty-four-hour', assessed['terminal_receipt'])
            return
        if self.current and self.current['state'] == 'CLOSING':
            if self.now() > plan['cutoff_epoch'] + 1800:
                if self.replayer:
                    self.replayer.close(force=True, reason='CLOSEOUT_TIME_BOUND')
                    self.replayer = None
                if self.auditor:
                    self.auditor.close(force=True, reason='CLOSEOUT_TIME_BOUND')
                    self.auditor = None
                self.assess('HOLD', 'BOUNDED_CLOSEOUT_TIMEOUT')
            elif not self.guard_closeout():
                self.finish_if_ready()
            return
        if self.collector:
            return
        if control == 'stop':
            if plan:
                self.assess('HOLD', 'OPERATOR_STOP')
            else:
                self.hold('OPERATOR_STOP', state='OPERATOR_STOPPED')
            return
        if plan is None:
            if control == 'run' and self.ready_for('eight-hour'):
                self.new_plan('eight-hour', self.config['short_predecessor'])
            return
        if self.now() >= plan['cutoff_epoch']:
            self.assess('HOLD', self.data['hold'] or 'FIXED_WINDOW_ENDED_WITHOUT_FULL_QUALIFICATION')
            return
        if control != 'run' or not self.ready_for(phase, remaining=plan['cutoff_epoch'] - self.now()):
            return
        if not self.current:
            if plan['start_epoch'] - self.now() < 30:
                self.hold('ORIGINAL_START_NO_LONGER_ADMISSIBLE')
                return
            self.start_collector(plan['predecessor'], plan['start_epoch'])
        elif self.current['state'] == 'INTERRUPTED' and not self.current.get('recovery_hold'):
            if len(self.attempts) > RECOVERY_LIMIT:
                self.hold('PAPER_RESTART_BUDGET_EXHAUSTED')
                return
            if not self.current.get('recovery_eligible_epoch'):
                self.current['recovery_eligible_epoch'] = self.now() + (300 if self.current.get('stop_reason') else 10)
                self.save()
            if self.now() < self.current['recovery_eligible_epoch']:
                return
            try:
                predecessor = self.restart_predecessor()
            except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                self.current['recovery_hold'] = str(exc)[:180]
                self.hold('CONTINUITY:' + self.current['recovery_hold'], state='RISK_HELD')
                return
            restart_now = self.now()
            prior = bound_json(predecessor)
            if prior.get('schema') == 'LANE_II_CRASH_HANDOFF_V1':
                restart_now = max(restart_now, prior['restart_not_before_epoch'] - self.config['prewarm_seconds'])
            start = continuation_start(plan, restart_now, self.config['prewarm_seconds'])
            if start is None:
                self.hold('TOO_LITTLE_TIME_TO_RESUME_BEFORE_ORIGINAL_CUTOFF')
                return
            self.start_collector(predecessor, start)

    def ready_for(self, phase, *, remaining=None):
        if len(self.samples) < 8 or not fresh_coordinator_clock(self.clock):
            return False
        seconds = PHASE_SECONDS[phase] if remaining is None else remaining
        result = admission(list(self.samples), seconds, limits=self.config['limits'])
        if not result['admitted']:
            self.data['hold'] = 'RESOURCE:' + ','.join(result['reasons'])
        return result['admitted']

    def run(self):
        with application_lock(self.root / 'long-run.lock') as acquired:
            if not acquired:
                return
            if self.state_path.exists():
                self.data = read_json(self.state_path)
                _must(self.data['config_sha256'] == self.configuration, 'PERSISTED_LONG_CONFIGURATION_CHANGED')
            self.reconcile_crashes()
            self.reconcile_manager_restart()
            deny_python_network()
            server = serve(self.root, self.config['port'])
            wake_lease = False
            try:
                while True:
                    if time.monotonic() >= self.next_calibration:
                        self.calibrate()
                    self.samples.append(sample(self.root))
                    try:
                        plan = self.plan
                        closing = self.current and self.current['state'] == 'CLOSING'
                        awake = bool(plan and self.data['phase'] not in self.data['assessments']
                                     and self.now() < plan['cutoff_epoch'] + (1800 if closing else 0))
                        if awake != wake_lease:
                            set_window_wake_lease(awake)
                            wake_lease = awake
                        self.advance()
                    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
                        reason = f'{type(exc).__name__}:{str(exc)[:180]}'
                        if self.current and self.current['state'] == 'CLOSING':
                            self.assess('HOLD', reason)
                        elif self.current and self.current['state'] == 'LAUNCHING' and not self.collector:
                            self.current.update(state='INTERRUPTED', reason=reason)
                            self.crash(STUDY, reason, stamp_kind='launch_error', attempt=self.current['ordinal'])
                            self.hold(reason, state='RECOVERING')
                        else:
                            self.hold(reason)
                    if time.monotonic() >= self.next_status:
                        self.publish_status()
                    time.sleep(1)
            finally:
                try:
                    for child in (self.research, self.replayer, self.auditor, self.collector):
                        if child:
                            child.close(force=True, reason='SUPERVISOR_EXIT')
                finally:
                    if wake_lease:
                        set_window_wake_lease(False)
                    server.shutdown()
                    server.server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('prepare', 'run', 'status', 'checkpoint', 'dashboard', 'stop', 'pause', 'resume'))
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--candidate', type=Path)
    parser.add_argument('--short-observation', type=Path)
    parser.add_argument('--short-review', type=Path)
    parser.add_argument('--release', type=Path)
    parser.add_argument('--release-review', type=Path)
    parser.add_argument('--research', type=Path)
    parser.add_argument('--observer', choices=('host', 'codex'), default='host')
    parser.add_argument('--port', type=int, default=8889)
    args = parser.parse_args()
    result = None
    if args.command == 'prepare':
        for key in ('candidate', 'short_observation', 'short_review', 'release', 'release_review'):
            if getattr(args, key) is None:
                parser.error('prepare requires --' + key.replace('_', '-'))
        config = prepare(args.root, args.repo, args.candidate, args.short_observation, args.short_review, args.release, args.release_review,
                         research=args.research, port=args.port)
        result = {'root': config['root'], 'source_sha256': config['source_sha256'], 'config_sha256': digest(config)}
    elif args.command == 'run':
        LongRun(args.root).run()
    elif args.command in {'stop', 'pause', 'resume'}:
        load_config(args.root)
        result = {'mode': 'run' if args.command == 'resume' else args.command, 'utc': utc()}
        atomic_json(args.root / 'CONTROL.json', result)
    elif (args.root / 'STATUS.json').is_file():
        if args.command == 'checkpoint':
            result = health(args.root, args.observer)
        elif args.command == 'dashboard':
            result = dashboard(args.root)
        else:
            result = read_json(args.root / 'STATUS.json')
    if sys.stdout and result is not None:
        print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
