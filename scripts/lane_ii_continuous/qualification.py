"""Evidence-bound release and promotion for the prospective paper campaign."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .common import atomic_json, binding, digest, read_json, utc

PHASE_SECONDS = {'short': 900, 'eight-hour': 28800, 'twenty-four-hour': 86400}
PREVIOUS = {'eight-hour': 'short', 'twenty-four-hour': 'eight-hour'}
POLICY = {'schema': 'LANE_II_PROSPECTIVE_POLICY_V1', 'phase_seconds': PHASE_SECONDS,
          'alignment_seconds': 300, 'missed_paper_ticks_allowed': 0,
          'require_all_expected_market_windows': True, 'minimum_entries': 0,
          'maximum_campaign_loss_usd': '25.00', 'profitability_required': False,
          'require_unresolved_exposure_zero_for_promotion': True, 'paper_only': True}
RELEASE = 'LANE_II_SHORT_RELEASE_V1'
REVIEW = 'LANE_II_SHORT_REVIEW_V1'
TESTS = 'LANE_II_QUALIFICATION_TESTS_V1'
ASSESSMENT = 'LANE_II_PHASE_QUALIFICATION_V1'
PROFILES = {'5m-P13', '5m-P2'}
STAGES = ('seal', 'integrity', 'accounting', 'replay', 'analysis', 'decision', 'report')


def bound_json(reference):
    path = Path(reference['path'])
    if path.stat().st_size > 8 * 2**20 or binding(path) != reference:
        raise ValueError('QUALIFICATION_EVIDENCE_CHANGED')
    return read_json(path)


def identities(config):
    return {'source_sha256': config['source_sha256'], 'config_sha256': digest(config),
            'candidate_sha256': config['candidate']['source_manifest_sha256'],
            'risk_manifest_sha256': config['paper_risk_manifest']['sha256']}


def phase_segment(config, phase):
    return 'short-002' if phase == 'short' and config.get('prewarm_recovery') else phase + '-001'


def phase_payload(config, phase, *, predecessor=None):
    if phase not in PHASE_SECONDS:
        raise ValueError('UNKNOWN_QUALIFICATION_PHASE')
    return {'phase': phase, 'seconds': PHASE_SECONDS[phase], 'candidate': config['candidate'],
            'run_dir': str(Path(config['root']) / 'runs' / ('prospective-' + phase_segment(config, phase))),
            'predecessor': predecessor}


def validate_short_release(config, release):
    if release.get('schema') != RELEASE or release.get('identities') != identities(config):
        raise ValueError('SHORT_RELEASE_SOURCE_OR_CONFIGURATION_MISMATCH')
    if config.get('qualification_policy') != POLICY or config.get('paper_only') is not True:
        raise ValueError('FROZEN_PAPER_QUALIFICATION_POLICY_REQUIRED')
    bound_json(config['paper_risk_manifest'])
    if config.get('prewarm_recovery'):
        from .prewarm_recovery import validate_replacement
        validate_replacement(config, config['prewarm_recovery'])
    inputs = release['evidence']
    if set(inputs) != {'review', 'tests', 'historical_replay'}:
        raise ValueError('EXACT_PRELAUNCH_EVIDENCE_REQUIRED')
    review, tests, replay = (bound_json(inputs[k]) for k in ('review', 'tests', 'historical_replay'))
    if (review.get('schema') != REVIEW or review.get('identities') != identities(config)
            or review.get('decision') != 'ACCEPT_SHORT_PAPER_ONLY' or review.get('holds') != []):
        raise ValueError('SHORT_REVIEW_ACCEPTANCE_REQUIRED')
    if (tests.get('schema') != TESTS or tests.get('identities') != identities(config)
            or tests.get('exit_code') != 0 or not tests.get('tests_run')
            or tests.get('prospective_capture_started') is not False):
        raise ValueError('BOUND_PRELAUNCH_TESTS_REQUIRED')
    assessment = replay.get('assessment', replay)
    if (assessment.get('all_requested_parity_passed') is not True
            or assessment.get('study_parity') != 'PASS' or assessment.get('cohort_parity') != 'PASS'
            or assessment.get('replayed_profile_count') != 60):
        raise ValueError('SEALED_HISTORICAL_PARITY_EVIDENCE_REQUIRED')
    return {'engineering_reviewed': True, 'paper_scope_reviewed': True, 'replay_reproduced': True,
            'short_pipeline_qualified': False, 'eight_hour_qualified': False,
            'twenty_four_hour_qualified': False, 'profitability_qualified': False,
            'reason': 'Source-bound paper review, prelaunch tests and retained historical parity; prospective qualification pending'}


def register_short_release(config, registry, path):
    path = Path(path).resolve()
    reference = binding(path)
    release = bound_json(reference)
    gates = validate_short_release(config, release)
    prior = registry.fact('short_release')
    if prior:
        if prior != reference:
            raise ValueError('IMMUTABLE_SHORT_RELEASE_CHANGED')
        return registry.fact('qualification')
    registry.fact('qualification', gates, set_value=True)
    registry.fact('short_release', reference, set_value=True)
    return gates


def launch_reasons(config, registry, job):
    try:
        payload = job['payload']
        phase = payload['phase']
        if phase not in PHASE_SECONDS or payload != phase_payload(config, phase, predecessor=payload.get('predecessor')):
            return ['UNFROZEN_PHASE_PAYLOAD']
        if job.get('experiment_id') != config['campaign_id'] or job.get('segment_id') != phase_segment(config, phase):
            return ['UNFROZEN_CAMPAIGN_OR_SEGMENT']
        reference = registry.fact('short_release')
        if reference is None:
            return ['BOUND_SHORT_RELEASE_REQUIRED']
        validate_short_release(config, bound_json(reference))
        # One immutable segment of each phase in the same campaign. Long phases
        # require a source-bound, complete preceding qualification and risk seal.
        peers = [row for row in registry.jobs() if row['type'] == 'forward' and row['id'] != job['id']]
        if any(row.get('segment_id') == job['segment_id'] for row in peers):
            return ['DUPLICATE_PROSPECTIVE_SEGMENT']
        if phase == 'short':
            expected = None
            if config.get('prewarm_recovery'):
                from .prewarm_recovery import validate_replacement
                expected = validate_replacement(config, config['prewarm_recovery'])
            return [] if payload['predecessor'] == expected else ['UNEXPECTED_SHORT_PREDECESSOR']
        previous = PREVIOUS[phase]
        seal_reference = registry.fact('phase_qualification:' + previous)
        if not seal_reference:
            return ['COMPLETE_' + previous.upper().replace('-', '_') + '_QUALIFICATION_REQUIRED']
        seal = bound_json(seal_reference)
        if (seal.get('schema') != ASSESSMENT or seal.get('phase') != previous
                or seal.get('status') != 'QUALIFIED' or seal.get('identities') != identities(config)
                or seal.get('terminal_receipt') != payload['predecessor'] or not seal.get('risk_continuity')
                or not payload['predecessor']):
            return ['PREDECESSOR_EVIDENCE_OR_CONTINUITY_CHANGED']
        bound_json(payload['predecessor'])
        bound_json(seal['risk_continuity'])
        # This build only commissions the short path. Byte partitioning alone
        # does not demonstrate incremental replay/accounting for multi-hour work.
        return ['HOURLY_CAUSAL_REPLAY_NOT_QUALIFIED']
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return ['QUALIFICATION_BINDING:' + str(exc).split(':', 1)[0][:80]]


def write_phase_assessment(config, registry, phase, result):
    """Persist one immutable terminal assessment; a HOLD never admits a later phase."""
    if phase not in PHASE_SECONDS or result['status'] not in {'QUALIFIED', 'HOLD'}:
        raise ValueError('INVALID_PHASE_ASSESSMENT')
    data = {'schema': ASSESSMENT, 'utc': utc(), 'phase': phase, 'identities': identities(config), **result}
    path = Path(config['root']) / 'qualification' / (phase + '.json')
    if path.exists():
        old = read_json(path)
        if {k: v for k, v in old.items() if k != 'utc'} != {k: v for k, v in data.items() if k != 'utc'}:
            raise ValueError('PHASE_ASSESSMENT_ALREADY_BOUND')
    else:
        atomic_json(path, data)
    reference = binding(path)
    registry.fact('phase_qualification:' + phase, reference, set_value=True)
    return reference


def _must(condition, reason):
    if not condition:
        raise ValueError(reason)


def _job_output(job, config):
    expected = {'schema': 'LANE_II_JOB_OUTPUT_V1', 'job_id': job['id'],
                'input_hash': digest(job['payload']), 'code_hash': config['source_sha256'],
                'config_hash': digest(config), 'execution_state': 'completed'}
    _must(job['state'] == 'completed' and job.get('artifact_manifest'), 'JOB_NOT_VERIFIED_COMPLETE')
    output = bound_json(job['artifact_manifest'])
    _must(all(output.get(k) == v for k, v in expected.items()), 'PIPELINE_JOB_OUTPUT_IDENTITY_MISMATCH')
    return output


def _artifact(output, name):
    matches = [{k: v for k, v in row.items() if k != 'name'}
               for row in output['artifacts'] if row['name'] == name]
    _must(len(matches) == 1, 'MISSING_UNIQUE_PIPELINE_ARTIFACT:' + name)
    return matches[0], bound_json(matches[0])


def _same_bytes(left, right):
    return all(left[k] == right[k] for k in ('bytes', 'sha256'))


def prospective_replay_pass(result):
    exact_schema = (result.get('schema') == 'LANE_II_PROSPECTIVE_REPLAY_RESULT_V1' or
                    (result.get('schema') == 'LANE_II_HOURLY_REPLAY_RESULT_V1'
                     and result.get('sealed_retrospective_validation_only') is False))
    return (exact_schema and result.get('pass') is True
            and set(result.get('parity', {})) == PROFILES
            and all(result.get(key, {}).get('pass') is True
                    for key in ('admission_parity', 'raw_feed_reconstruction', 'risk_parity'))
            and result['admission_parity'].get('exact_input_and_terminal_state_hashes') is True
            and result['risk_parity'].get('initial_state_and_predecessor_verified') is True
            and result['risk_parity'].get('terminal_state_verified') is True
            and result['risk_parity'].get('account_carryover_verified') is True
            and result['risk_parity'].get('unowned_liabilities') == []
            and all(type(result.get(k)) is int and result[k] == 0
                    for k in ('network_requests', 'real_orders', 'model_api_calls')))


def _epoch(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    _must(stamp.utcoffset() is not None, 'QUALIFICATION_TIMESTAMP_MUST_HAVE_OFFSET')
    return stamp.timestamp()


def _phase_packet(config, forward, stages):
    from .paper_risk import validate_closure

    p = forward['payload']; run = Path(p['run_dir']); duration = PHASE_SECONDS[p['phase']]
    forward_output = _job_output(forward, config)
    outputs = {kind: _job_output(job, config) for kind, job in stages.items()}
    terminal_ref = binding(run / 'capture' / 'receipt.json')
    terminal = bound_json(terminal_ref)
    expected_start, cutoff, end = (_epoch(terminal[k]) for k in ('paper_start_utc', 'cutoff_utc', 'actual_end_utc'))
    _must(terminal['state'] == 'COMPLETE' and terminal['reason'] == 'DEADLINE' and end >= cutoff
          and cutoff - expected_start == duration and expected_start % POLICY['alignment_seconds'] == 0
          and terminal['source_manifest_sha256'] == config['candidate']['source_manifest_sha256']
          and set(row['id'] for row in terminal['profiles']) == PROFILES and len(terminal['profiles']) == len(PROFILES)
          and all(type(terminal.get(k)) is int and terminal[k] == 0
                  for k in ('real_orders', 'model_api_calls', 'authenticated_account_calls')),
          'GENUINE_FULL_PAPER_INTERVAL_REQUIRED')
    _must(terminal.get('snapshot_publish_gaps') == 0, 'SNAPSHOT_PUBLICATION_GAPS')
    _must(_same_bytes(terminal_ref, _artifact(outputs['seal'], 'receipt.json')[0])
          and forward_output['result']['receipt'] == terminal, 'TERMINAL_RECEIPT_CHANGED')
    admission_ref, admission = _artifact(forward_output, 'ADMISSION.json')
    _must(admission['schema'] == 'LANE_II_FORWARD_ADMISSION_V1' and admission['resources']['admitted'] is True
          and admission['clock']['state'] == 'accepted' and admission['job_id'] == forward['id']
          and admission['segment_id'] == forward['segment_id'] and admission['qualification_phase'] == p['phase']
          and admission['paper_risk_manifest'] == config['paper_risk_manifest'] and admission['predecessor'] == p['predecessor']
          and admission['config_sha256'] == digest(config) and admission['source_sha256'] == config['source_sha256']
          and admission['candidate_sha256'] == terminal['source_manifest_sha256']
          and _epoch(admission['paper_start_utc']) == expected_start and _epoch(admission['cutoff_utc']) == cutoff,
          'LAUNCH_ADMISSION_NOT_BOUND')
    raw_ref, raw = _artifact(outputs['integrity'], 'RAW_CAPTURE_AUDIT.json')
    supervisor_ref, supervisor = _artifact(outputs['integrity'], 'PAPER_RUN_RESULT.json')
    _must(raw['status'] == 'PASS_RETAINED_CAPTURE_INTEGRITY_ONLY' and raw['problem_counts'] == {}
          and terminal_ref in raw['input_bindings'] and supervisor['receipt'] == terminal
          and supervisor['worker_exit_code'] == 0 and supervisor['forced_closure'] is False
          and supervisor['bindings']['receipt'] == terminal_ref and _same_bytes(raw_ref, supervisor['bindings']['raw_audit'])
          and supervisor['capture_audit']['status'] == 'PASS_RETAINED_NEXT12_CAPTURE_INTEGRITY_ONLY',
          'RAW_INTEGRITY_OR_GENUINE_EXIT_FAILED')
    risk_ref = forward_output['result']['risk_continuity']
    risk_published, _ = _artifact(forward_output, 'RISK_CLOSURE.json')
    risk = validate_closure(run, expected_anchor_sha256=config['paper_risk_manifest']['sha256'])
    _must(risk['valid'] is True and risk['closure_sha256'] == risk_ref['sha256']
          and risk_ref == binding(run / 'RISK_CLOSURE.json') and _same_bytes(risk_published, risk_ref)
          and risk['campaign_id'] == config['campaign_id'] and risk['segment_id'] == forward['segment_id']
          and risk['receipt_sha256'] == terminal_ref['sha256'], 'UNRESOLVED_OR_UNPROVEN_CAMPAIGN_EXPOSURE')
    _, account = _artifact(outputs['accounting'], 'CLOSEOUT.json')
    study = account['components']['study']
    balances = study['accounting']
    _must(study['closure']['supervisor_exit_verified'] is True and terminal_ref in account['input_bindings']
          and balances['status'] == 'PASS_RETAINED_PROFILE_ACCOUNTING'
          and {row['id'] for row in balances['accounts']} == PROFILES, 'CLOSED_ACCOUNTING_REQUIRED')
    replay_ref, replay = _artifact(outputs['replay'], 'RESULT.json')
    replay_output = outputs['replay']['result']
    _must(replay_output['driver_exit_code'] == 0 and _same_bytes(replay_ref, replay_output['driver_result'])
          and replay_output['parity']['all_requested_parity_passed'] is True and prospective_replay_pass(replay)
          and replay['source_state'] == terminal['state'] and replay['source_events_sha256'] == terminal['events_sha256']
          and replay['risk_parity']['immutable_closure'] == risk, 'PROSPECTIVE_CAUSAL_PARITY_REQUIRED')
    holds = []
    if (risk['flat'] is not True or replay['risk_parity']['flat'] is not True
            or balances['counts']['unresolved_positions'] != 0
            or any(replay['risk_parity'][key] != [] for key in
                   ('open_reservation_ids', 'unknown_reservation_ids', 'working_reservation_ids'))):
        holds.append('OPEN_WORKING_OR_UNKNOWN_RISK')
    coverage = replay['coverage']; windows = list(range(int(expected_start), int(cutoff), 300))
    if not (coverage['expected_scheduled_tick_seconds'] == coverage['distinct_paper_scheduled_tick_seconds'] == duration
            and coverage['missing_scheduled_tick_seconds'] == 0 and coverage['expected_5m_window_starts'] == windows
            and coverage['shared_fresh_eligible_window_starts'] == windows and set(coverage['per_profile']) == PROFILES
            and coverage['missing_data_treated_as_abstention'] is False and coverage['trade_quota_used'] is False):
        holds.append('PAPER_TICK_OR_MATCHED_MARKET_WINDOW_COVERAGE_GAP')
    analysis_ref, analysis = _artifact(outputs['analysis'], 'ANALYSIS.json')
    _must(analysis['rejected_reasons'] == [] and analysis['funded_authority'] is False
          and analysis['components']['study']['closure']['accounting'] == 'RECONCILED'
          and analysis['components']['study']['closure']['replay'] == 'PASS_PROSPECTIVE_CAUSAL_PARITY'
          and any(row['role'] == 'prospective_replay' and row['sha256'] == replay_ref['sha256']
                  for row in analysis['input_bindings'])
          and outputs['decision']['result']['capital_review_open'] is False,
          'BOUND_DESCRIPTIVE_REPORT_REQUIRED')
    report_ref, report = _artifact(outputs['report'], 'CLOSEOUT_STATUS.json')
    _must(report['forward'] == 'completed' and report['stages'] == stages['report']['payload']['closure_snapshot'],
          'FINAL_AUTOMATIC_REPORT_REQUIRED')
    return {'status': 'HOLD' if holds else 'QUALIFIED', 'holds': holds,
            'terminal_receipt': terminal_ref, 'risk_continuity': risk_ref,
            'evidence': {'forward': forward['artifact_manifest'], 'admission': admission_ref, 'raw_audit': raw_ref,
                         'supervisor': supervisor_ref, 'replay': replay_ref, 'analysis': analysis_ref, 'report': report_ref,
                         'pipeline': {k: j['artifact_manifest'] for k, j in stages.items()}},
            'coverage': coverage, 'entries': balances['counts']['entries'],
            'profitability_qualified': False, 'funded_authority': False}


def reconcile_phases(config, registry):
    """Project immutable terminal qualification from completed scheduler jobs."""
    rows = registry.jobs()
    for forward in (j for j in rows if j['type'] == 'forward' and j['state'] in {'completed', 'failed', 'cancelled'}):
        phase = forward['payload']['phase']
        if registry.fact('phase_qualification:' + phase):
            continue
        children = [j for j in rows if j['parent_id'] == forward['id']]
        stages = {j['type']: j for j in children if j['type'] in STAGES and j['type'] != 'report'}
        if set(stages) != set(STAGES) - {'report'}:
            continue
        pending = {'queued', 'leased', 'running'}
        if any(j['state'] in pending or (j['state'] == 'admission-held'
               and (j.get('terminal_reason') or '').startswith('RESOURCE:')) for j in stages.values()):
            continue
        reports = [j for j in children if j['type'] == 'report' and j['state'] == 'completed'
                   and {r['type']: (r['id'], r['state'], r['output']) for r in j['payload']['closure_snapshot']}
                   == {k: (v['id'], v['state'], v['artifact_manifest']) for k, v in stages.items()}]
        if not reports:
            continue
        stages['report'] = reports[-1]
        failed = [k + ':' + str(j['terminal_reason']) for k, j in stages.items() if j['state'] != 'completed']
        if forward['state'] != 'completed':
            forward_reason = forward['terminal_reason']
            if forward.get('artifact_manifest'):
                result = bound_json(forward['artifact_manifest'])['result']
                forward_reason = result.get('risk_hold') or result.get('execution', {}).get('reason') or forward_reason
            failed.insert(0, 'forward:' + str(forward_reason))
        if not failed:
            try:
                result = _phase_packet(config, forward, stages)
            except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
                failed.append(type(exc).__name__ + ':' + str(exc)[:200])
        if failed:
            result = {'status': 'HOLD', 'holds': failed, 'profitability_qualified': False, 'funded_authority': False,
                      'evidence': {k: j['artifact_manifest'] for k, j in {'forward': forward, **stages}.items()}}
        write_phase_assessment(config, registry, phase, result)
    gates = registry.fact('qualification') or {}
    keys = {'short': 'short_pipeline_qualified', 'eight-hour': 'eight_hour_qualified', 'twenty-four-hour': 'twenty_four_hour_qualified'}
    assessments = {p: registry.fact('phase_qualification:' + p) for p in PHASE_SECONDS}
    for p, ref in assessments.items():
        if ref:
            gates[keys[p]] = bound_json(ref)['status'] == 'QUALIFIED'
    if any(assessments.values()):
        gates['phase_assessments'] = {p: ref for p, ref in assessments.items() if ref}
        gates['reason'] = 'See sealed phase assessments for coverage, closeout and holds'
        if gates != registry.fact('qualification'):
            registry.fact('qualification', gates, set_value=True)
