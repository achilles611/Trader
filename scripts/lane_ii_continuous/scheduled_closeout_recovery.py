"""One scheduled retry of analysis for an already sealed short. No forward job."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.lane_ii_continuous.capture import verify_candidate
from scripts.lane_ii_continuous.common import application_lock, atomic_json, binding, digest, read_json, same_process, utc
from scripts.lane_ii_continuous.paper_risk import validate_closure
from scripts.lane_ii_continuous.qualification import (
    PROFILES, _artifact, _epoch, _job_output, _must, _same_bytes, bound_json, identities, prospective_replay_pass,
)
from scripts.lane_ii_continuous.queue import Registry
from scripts.lane_ii_continuous.research import analyze_closeout, assess_replay_results
from scripts.lane_ii_continuous.resources import Guard, admission, sample
from scripts.lane_ii_continuous.service import load_config
from scripts.lane_ii_continuous.worker import deny_python_network, exited_owned_descendants, worker_environment

SCHEMA = 'LANE_II_SCHEDULED_SHORT_CLOSEOUT_RECOVERY_V1'


def prepare_recovery(root, original_root, repo):
    root, original_root, repo = (Path(p).resolve() for p in (root, original_root, repo))
    _must(not root.exists() and not root.is_relative_to(original_root), 'NEW_EXTERNAL_RECOVERY_ROOT_REQUIRED')
    original = load_config(original_root); app = root / 'app'
    shutil.copytree(original['app'], app)
    relative = Path('scripts/lane_ii_continuous')
    for name in ('offline.py', 'qualification_replay.py', 'scheduled_closeout_recovery.py'):
        shutil.copyfile(repo/relative/name, app/relative/name)
    (root/'temp').mkdir()
    source = {'schema':'LANE_II_FROZEN_APPLICATION_V1','files':[binding(p) for p in sorted(app.rglob('*')) if p.is_file()]}
    config = {**original,'root':str(root),'app':str(app),'cli':str(app/relative/'cli.py'),'source_sha256':digest(source),
              'recovery_only':True,'sealed_short_recovery':{'config':binding(original_root/'CONFIG.json'),
                  'phase':binding(original_root/'qualification/short.json')}}
    atomic_json(root/'SOURCE.json', source); atomic_json(root/'CONFIG.json', config)
    _original_inputs(root, config)
    return {'root':str(root),'source_sha256':digest(source),'config_sha256':digest(config),'capture_started':False}


def _original_inputs(root, config):
    previous = config['sealed_short_recovery']
    original = bound_json(previous['config'])
    original_root = Path(original['root']); config_path = original_root / 'CONFIG.json'
    _must(previous['config'] == binding(config_path) and original == load_config(original_root)
          and original_root != root, 'EXACT_ORIGINAL_CONFIGURATION_REQUIRED')
    _must(config['candidate'] == original['candidate'] and config['paper_risk_manifest'] == original['paper_risk_manifest'],
          'RECOVERY_CANNOT_REPLACE_CANDIDATE_OR_CAMPAIGN')
    status = read_json(original_root / 'STATUS.json')
    _must(status['workers'] == [], 'ORIGINAL_SERVICE_MUST_BE_IDLE')
    with sqlite3.connect((original_root / 'jobs.sqlite').as_uri() + '?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        rows = [Registry._decode(row) for row in db.execute('SELECT * FROM jobs ORDER BY created')]
    _must(all(j['state'] in {'completed', 'failed', 'cancelled', 'admission-held'} for j in rows),
          'NO_ACTIVE_OR_PENDING_JOB_DURING_SEALED_RECOVERY')
    forwards = [j for j in rows if j['type'] == 'forward']
    _must(len(forwards) == 1 and forwards[0]['payload']['phase'] == 'short', 'ONE_ORIGINAL_SHORT_REQUIRED')
    forward = forwards[0]; run = Path(forward['payload']['run_dir'])
    _must(run.is_relative_to(original_root / 'runs') and not root.is_relative_to(original_root),
          'SEPARATE_RECOVERY_OUTPUT_REQUIRED')
    _must(same_process(read_json(run/'operator.json')) is False, 'ORIGINAL_COLLECTOR_EXIT_NOT_PROVEN')
    _must(previous['phase'] == binding(original_root / 'qualification/short.json'), 'ORIGINAL_PHASE_BINDING_REQUIRED')
    phase = bound_json(previous['phase'])
    _must(phase['status'] == 'HOLD' and phase['identities'] == identities(original)
          and phase['holds'] == ['replay:GENUINE_PROSPECTIVE_SUPERVISOR_PROOF_REQUIRED']
          and phase['evidence']['forward'] == forward['artifact_manifest'], 'SUPPORTED_OFFLINE_CLOSEOUT_HOLD_REQUIRED')
    terminal = _job_output(forward, original)
    stages = {kind: [j for j in rows if j['type'] == kind and j['parent_id'] == forward['id'] and j['state'] == 'completed']
              for kind in ('seal', 'integrity', 'accounting')}
    _must(all(len(v) == 1 for v in stages.values()), 'UNIQUE_ORIGINAL_CLOSEOUT_STAGES_REQUIRED')
    stages = {kind: _job_output(jobs[0], original) for kind, jobs in stages.items()}
    ref = binding(run / 'capture/receipt.json'); receipt = bound_json(ref)
    start, cutoff, end = (_epoch(receipt[k]) for k in ('paper_start_utc', 'cutoff_utc', 'actual_end_utc'))
    _must(receipt == terminal['result']['receipt'] and _same_bytes(ref, _artifact(stages['seal'], 'receipt.json')[0])
          and (receipt['state'], receipt['reason']) == ('COMPLETE', 'DEADLINE') and end >= cutoff
          and cutoff - start == 900 and start % 300 == 0 and receipt['snapshot_publish_gaps'] == 0
          and all(type(receipt.get(k)) is int and receipt[k] == 0 for k in
                  ('real_orders', 'model_api_calls', 'authenticated_account_calls')),
          'GENUINE_COMPLETE_SHORT_RECEIPT_REQUIRED')
    risk_ref = binding(run / 'RISK_CLOSURE.json')
    risk = validate_closure(run, config['paper_risk_manifest']['sha256'])
    _must(risk_ref == terminal['result']['risk_continuity'] and risk['flat'] is True
          and risk['receipt_sha256'] == ref['sha256'], 'ORIGINAL_FLAT_CAMPAIGN_CLOSURE_REQUIRED')
    raw_ref, raw = _artifact(stages['integrity'], 'RAW_CAPTURE_AUDIT.json')
    accounting_ref, accounting = _artifact(stages['accounting'], 'CLOSEOUT.json')
    a = accounting['components']['study']; counts = a['accounting']['counts']
    _must(raw['status'] == 'PASS_RETAINED_CAPTURE_INTEGRITY_ONLY' and raw['problem_counts'] == {}
          and raw['events_checked'] == receipt['events'] and ref in raw['input_bindings']
          and ref in accounting['input_bindings'] and a['closure']['supervisor_exit_verified'] is True
          and a['accounting']['status'] == 'PASS_RETAINED_PROFILE_ACCOUNTING' and counts['unresolved_positions'] == 0
          and {p['id'] for p in a['accounting']['accounts']} == PROFILES, 'ORIGINAL_RAW_AND_ACCOUNTING_PROOF_REQUIRED')
    verify_candidate(config['candidate'])
    return run, receipt, risk, {'terminal_receipt': ref, 'raw_audit': raw_ref, 'accounting': accounting_ref}, counts


def _offline_stage(root, config, name, args):
    from scripts.slim_paper_operator.owned_job import OwnedJob

    scripts = Path(config['cli']).parent
    argv = [config['interpreter'], '-I', '-B', str(scripts/'offline.py'), '--startup-gate', '--',
            str(scripts/name), *map(str, args)]
    guard = Guard(config['limits']); started = time.monotonic()
    with (root/(name+'.log')).open('xb', buffering=0) as log:
        p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=log, stderr=log, cwd=config['app'],
                             env=worker_environment(root), creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        owned = None; children = None
        try:
            owned = OwnedJob(p)
            p.stdin.write(b'LANE_II_OFFLINE_START\n'); p.stdin.close()
            while p.poll() is None:
                reason = guard.check(sample(root), 0)
                if reason or time.monotonic() - started > config['maximum_replay_seconds']:
                    raise RuntimeError('RECOVERY_RESOURCE_OR_TIME_HOLD:'+str(reason))
                time.sleep(.4)
            code = p.wait(timeout=2)
            children = exited_owned_descendants(owned, p)
            _must(code == 0 and not children, 'RECOVERY_STAGE_FAILED:'+name+':'+str(code))
        finally:
            still_running = p.poll() is None
            if owned is not None:
                owned.close()
            elif still_running:
                p.kill()
            exit_path = root/(name+'.exit.json')
            try:
                p.wait(timeout=10)
            finally:
                atomic_json(exit_path, {'stage':name,'utc':utc(),'pid':p.pid,'exit_code':p.poll(),
                    'retained_handle_exit_observed':p.poll() is not None,
                    'forced_closure':True if still_running or children else (False if children is not None else None),
                    'owned_children_after_root_exit':None if children is None else sorted(children)})
    return {'stage': name, 'command': argv, 'exit_code': code, 'unforced': True,
            'exit':binding(exit_path), 'log': binding(root/(name+'.log'))}


def run_recovery(root):
    root = Path(root).resolve(); config = load_config(root)
    _must(config.get('recovery_only') is True and 'sealed_short_recovery' in config,
          'FROZEN_SEALED_RECOVERY_CONFIGURATION_REQUIRED')
    with application_lock(root / 'recovery.lock') as acquired:
        _must(acquired and not (root/'ATTEMPT.json').exists(), 'ONE_SCHEDULED_RECOVERY_ATTEMPT_REQUIRED')
        run, terminal, risk, refs, counts = _original_inputs(root, config)
        from scripts.lane_ii_continuous.common import process_identity
        resource = admission([sample(root, cpu_interval=.25) for _ in range(8)], 0, limits=config['limits'], heavy=True)
        attempt = {'schema': SCHEMA, 'started_utc': utc(), 'owner': process_identity(),
                   'correction_source_sha256': config['source_sha256'], 'correction_config_sha256': digest(config),
                   'original': config['sealed_short_recovery'], 'candidate_sha256': config['candidate']['source_manifest_sha256'],
                   'admission': resource, 'inputs': refs, 'forward_relaunched': False}
        atomic_json(root/'ATTEMPT.json', attempt)
        try:
            _must(resource['admitted'], 'RECOVERY_ADMISSION_HOLD:'+','.join(resource['reasons']))
            deny_python_network()
            candidate = config['candidate']; supervisor_path = root/'PAPER_RUN_RESULT.json'; replay_dir = root/'replay'
            args = ['--candidate', candidate['candidate_dir'], '--run-dir', run, '--raw-audit', refs['raw_audit']['path'],
                    '--manifest-sha256', candidate['source_manifest_sha256']]
            audits = _offline_stage(root, config, 'prospective_supervisor.py', [*args, '--output', supervisor_path])
            replayed = _offline_stage(root, config, 'qualification_replay.py',
                                      [*args, '--supervisor-result', supervisor_path, '--output', replay_dir])
            supervisor_ref = binding(supervisor_path); supervisor = bound_json(supervisor_ref)
            replay_ref = binding(replay_dir/'RESULT.json'); replay = bound_json(replay_ref)
            _must(supervisor['receipt'] == terminal and supervisor['worker_exit_code'] == 0
                  and supervisor['forced_closure'] is False and supervisor['bindings']['receipt'] == refs['terminal_receipt']
                  and supervisor['bindings']['raw_audit'] == refs['raw_audit']
                  and supervisor['capture_audit']['status'] == 'PASS_RETAINED_NEXT12_CAPTURE_INTEGRITY_ONLY',
                  'RECOVERY_SUPERVISOR_INPUT_MISMATCH')
            parity = assess_replay_results(replay)
            _must(prospective_replay_pass(replay) and parity['all_requested_parity_passed'] is True
                  and replay['source_state'] == 'COMPLETE' and replay['source_events_sha256'] == terminal['events_sha256']
                  and replay['risk_parity']['immutable_closure'] == risk and replay['risk_parity']['flat'] is True
                  and all(replay['risk_parity'][key] == [] for key in ('open_reservation_ids','unknown_reservation_ids','working_reservation_ids')),
                  'RECOVERY_FULL_CAUSAL_PARITY_REQUIRED')
            inputs = bound_json(replay['input_manifest'])
            _must(inputs['sources']['supervisor_result'] == supervisor_ref
                  and inputs['sources']['receipt'] == refs['terminal_receipt']
                  and inputs['sources']['raw_audit'] == refs['raw_audit']
                  and all(binding(row['path']) == row and Path(row['path']).is_relative_to(config['app'])
                          for row in inputs['drivers']), 'CORRECTED_REPLAY_SOURCE_OR_INPUT_MISMATCH')
            coverage = replay['coverage']; start = int(_epoch(terminal['paper_start_utc'])); windows = list(range(start,start+900,300))
            _must(coverage['expected_scheduled_tick_seconds'] == coverage['distinct_paper_scheduled_tick_seconds'] == 900
                  and coverage['missing_scheduled_tick_seconds'] == 0 and coverage['expected_5m_window_starts'] == windows
                  and coverage['shared_fresh_eligible_window_starts'] == windows and set(coverage['per_profile']) == PROFILES
                  and coverage['trade_quota_used'] is False and coverage['missing_data_treated_as_abstention'] is False,
                  'RECOVERY_PAPER_TICK_OR_MATCHED_WINDOW_GAP')
            report = analyze_closeout({'artifacts': {**refs, 'prospective_replay': replay_ref},
                                       'source_sha256': config['source_sha256'], 'config_sha256': digest(config)},root/'analysis')
            _must(report['result']['rejected_reasons'] == []
                  and report['result']['components']['study']['closure']['replay'] == 'PASS_PROSPECTIVE_CAUSAL_PARITY',
                  'RECOVERED_AUTOMATIC_REPORT_REQUIRED')
            _, _, _, current_refs, _ = _original_inputs(root, config)
            _must(current_refs == refs and load_config(root) == config, 'SOURCE_OR_CAPTURE_CHANGED_DURING_RECOVERY')
            result = {'schema': SCHEMA, 'utc': utc(), 'status': 'QUALIFIED', 'basis': 'SCHEDULED_RECOVERY_OF_SEALED_SHORT',
                      'attempt': binding(root/'ATTEMPT.json'), 'original_first_pass': 'HOLD',
                      'terminal_receipt': refs['terminal_receipt'], 'original_phase': config['sealed_short_recovery']['phase'],
                      'supervisor': supervisor_ref, 'replay': replay_ref, 'analysis': report['artifacts'],
                      'stages': [audits,replayed], 'coverage': coverage, 'accounting_counts': counts,
                      'flat_risk': True, 'forward_relaunched': False, 'hourly_replay_qualified': False,
                      'eight_hour_qualified': False, 'twenty_four_hour_qualified': False,
                      'profitability_qualified': False, 'funded_authority': False, 'runtime_model_calls': 0}
            atomic_json(root/'RECOVERED_SHORT_QUALIFICATION.json',result)
            return result
        except (ValueError, RuntimeError, OSError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
            atomic_json(root/'RECOVERY_HOLD.json',{'schema':SCHEMA,'utc':utc(),'status':'HOLD',
                        'reason':type(exc).__name__+':'+str(exc)[:240], 'attempt':binding(root/'ATTEMPT.json'),
                        'forward_relaunched':False,'original_first_pass':'HOLD'})
            raise


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    args = p.parse_args()
    result = run_recovery(args.root)
    if sys.stdout:
        print(json.dumps({'status':result['status'],'basis':result['basis']}))


if __name__=='__main__':
    main()
