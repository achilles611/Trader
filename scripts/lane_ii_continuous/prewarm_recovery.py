"""One source-bound replacement for an owned, flat failure before paper start."""
from __future__ import annotations

from pathlib import Path

from .capture import verify_candidate
from .common import atomic_json, binding, digest, read_json, same_process, utc
from .paper_risk import decimal, validate_closure
from .qualification import PROFILES, _epoch, _must, bound_json

SCHEMA = 'LANE_II_PREWARM_REPLACEMENT_V1'
ROLES = {'prior_config', 'service_exit', 'assessment', 'forward_output', 'raw_audit', 'accounting',
         'receipt', 'operator', 'exit', 'risk_closure'}


def _zero_paper(receipt):
    _must((receipt['state'], receipt['reason']) == ('FAILED', 'DETERMINISTIC_TRANSPORT_FAILURE')
          and _epoch(receipt['actual_end_utc']) < _epoch(receipt['paper_start_utc'])
          and _epoch(receipt['cutoff_utc']) - _epoch(receipt['paper_start_utc']) == 900
          and all(type(receipt.get(k)) is int and receipt[k] == 0 for k in
                  ('ticks', 'missed_ticks', 'real_orders', 'model_api_calls', 'authenticated_account_calls')),
          'ONLY_ZERO_TICK_PREWARM_TRANSPORT_FAILURE_CAN_BE_REPLACED')
    rows = receipt['profiles']
    _must(len(rows) == 2 and {p['id'] for p in rows} == PROFILES
          and all(p['virtual_initial_cash'] == p['virtual_cash'] and all(p[k] == 0 for k in
                  ('entries', 'cash_exits', 'settlements', 'unresolved_positions', 'realized_pnl', 'modeled_fees',
                   'unresolved_basis')) for p in rows), 'PREWARM_ACCOUNTS_MUST_BE_UNCHANGED_AND_FLAT')


def validate_replacement(config, reference):
    packet = bound_json(reference)
    refs = packet['evidence']
    _must(packet['schema'] == SCHEMA and set(refs) == ROLES and packet['replacement_segment'] == 'short-002',
          'EXACT_PREWARM_REPLACEMENT_EVIDENCE_REQUIRED')
    docs = {k: bound_json(ref) for k, ref in refs.items()}
    old = docs['prior_config']; new_candidate = config['candidate']; old_candidate = old['candidate']
    _must(old['campaign_id'] == config['campaign_id'] and old['paper_risk_manifest'] == config['paper_risk_manifest']
          and old.get('prewarm_recovery') is None and old_candidate['roster_sha256'] == new_candidate['roster_sha256']
          and old_candidate['base_manifest_sha256'] == new_candidate['base_manifest_sha256'],
          'PREWARM_REPLACEMENT_MUST_REUSE_CAMPAIGN_AND_STRATEGY')
    for candidate in (old_candidate, new_candidate):
        verify_candidate(candidate)
    originals = [read_json(Path(c['candidate_dir']) / 'SOURCE_MANIFEST.json') for c in (old_candidate, new_candidate)]
    left, right = (doc['files'] for doc in originals)
    _must(originals[0]['config'] == originals[1]['config'] and set(left) == set(right)
          and {k for k in left if left[k] != right[k]} == {'CAPTURE_PATCHES.json', 'continuous_transport.py'},
          'PREWARM_REPLACEMENT_COLLECTOR_CHANGES_MUST_BE_TRANSPORT_ONLY')
    root = Path(old['root']); run = root / 'runs' / 'prospective-short-001'
    expected = {'prior_config': root / 'CONFIG.json', 'service_exit': root / 'SERVICE_EXIT.json',
                'assessment': root / 'qualification/short.json', 'receipt': run / 'capture/receipt.json',
                'operator': run / 'operator.json', 'exit': run / 'exit.json', 'risk_closure': run / 'RISK_CLOSURE.json'}
    _must(all(Path(refs[k]['path']) == v for k, v in expected.items()), 'PREWARM_REPLACEMENT_PATH_BINDINGS')
    service_exit = docs['service_exit']; assessment = docs['assessment']; forward = docs['forward_output']
    _must(service_exit['service_execution'] == 'stopped' and service_exit['control'] == 'stop'
          and service_exit['workers'] == [] and same_process(service_exit['owner']) is False
          and service_exit['source_sha256'] == old['source_sha256'] and service_exit['config_sha256'] == digest(old)
          and assessment['status'] == 'HOLD' and assessment['phase'] == 'short'
          and assessment['evidence']['forward'] == refs['forward_output']
          and forward['execution_state'] == 'failed' and forward['code_hash'] == old['source_sha256']
          and forward['config_hash'] == digest(old), 'PREWARM_PRIOR_OWNER_OR_TERMINAL_JOB_UNPROVEN')
    receipt, operator, exited = (docs[k] for k in ('receipt', 'operator', 'exit'))
    _zero_paper(receipt)
    _must(forward['result']['receipt'] == receipt and forward['result']['risk_continuity'] == refs['risk_closure']
          and forward['result']['supervisor_exit'] == refs['exit'] and receipt['pid'] == operator['pid'] == exited['pid']
          and exited['exit_code'] == 2 and exited['forced_closure'] is False and exited['retained_handle_exit_observed'] is True
          and exited['owned_children_at_parent_exit'] == [] and same_process(operator) is False
          and all(exited[k] == operator[k] for k in ('creation_epoch', 'boot_epoch', 'executable_sha256')),
          'PREWARM_REPLACEMENT_REQUIRES_GENUINE_EXIT_AND_RECEIPT')
    audit = docs['raw_audit']; account = docs['accounting']; study = account['components']['study']
    _must(audit['status'] == 'PASS_RETAINED_CAPTURE_INTEGRITY_ONLY' and audit['problem_counts'] == {}
          and audit['events_checked'] == receipt['events'] and refs['receipt'] in audit['input_bindings']
          and refs['receipt'] in account['input_bindings'] and study['closure']['supervisor_exit_verified'] is True
          and study['accounting']['status'] == 'PASS_RETAINED_PROFILE_ACCOUNTING'
          and all(study['accounting']['counts'][k] == 0 for k in ('entries', 'cash_exits', 'settlements', 'unresolved_positions')),
          'PREWARM_REPLACEMENT_REQUIRES_RAW_AUDIT_AND_ACCOUNTING')
    risk = validate_closure(run, expected_anchor_sha256=config['paper_risk_manifest']['sha256'])
    _must(risk['flat'] is True and risk['campaign_id'] == config['campaign_id'] and risk['segment_id'] == 'short-001'
          and risk['receipt_sha256'] == refs['receipt']['sha256'] and risk['snapshot']['holds'] == []
          and decimal(risk['snapshot']['consumed_or_reserved']) == 0, 'PREWARM_RISK_MUST_BE_CLOSED_FLAT_AND_UNCHANGED')
    return refs['receipt']


def prepare_replacement(prior_root, output):
    from .queue import Registry
    from .service import load_config

    root = Path(prior_root).resolve(); run = root / 'runs' / 'prospective-short-001'; output = Path(output).resolve()
    _must(not output.exists() and not output.is_relative_to(root), 'NEW_EXTERNAL_PREWARM_REPLACEMENT_PACKET_REQUIRED')
    load_config(root)
    registry = Registry(root / 'jobs.sqlite')
    forwards = [j for j in registry.jobs() if j['type'] == 'forward']
    _must(len(forwards) == 1 and forwards[0]['segment_id'] == 'short-001' and forwards[0]['attempts'] == 1
          and forwards[0]['state'] == 'failed', 'ONE_PRIOR_PREWARM_ATTEMPT_REQUIRED')
    forward = forwards[0]
    children = {j['type']: bound_json(j['artifact_manifest']) for j in registry.jobs({'completed'})
                if j['parent_id'] == forward['id'] and j['type'] in {'accounting', 'integrity'}}
    def artifact(kind, name):
        rows = [a for a in children[kind]['artifacts'] if a['name'] == name]
        _must(len(rows) == 1, 'UNIQUE_PREWARM_PROOF_REQUIRED')
        return {k: v for k, v in rows[0].items() if k != 'name'}
    files = {'prior_config': root / 'CONFIG.json', 'service_exit': root / 'SERVICE_EXIT.json',
             'assessment': root / 'qualification/short.json', 'receipt': run / 'capture/receipt.json',
             'operator': run / 'operator.json', 'exit': run / 'exit.json', 'risk_closure': run / 'RISK_CLOSURE.json'}
    refs = {k: binding(v) for k, v in files.items()}
    refs.update(forward_output=forward['artifact_manifest'], raw_audit=artifact('integrity', 'RAW_CAPTURE_AUDIT.json'),
                accounting=artifact('accounting', 'CLOSEOUT.json'))
    atomic_json(output, {'schema': SCHEMA, 'utc': utc(), 'replacement_segment': 'short-002', 'evidence': refs})
    return binding(output)
