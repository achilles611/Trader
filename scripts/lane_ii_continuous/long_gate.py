"""Evidence gates for the two longer runs of the same paper campaign."""
from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path
import sqlite3

from .capture import verify_candidate
from .common import binding, read_json
from .paper_risk import validate_closure
from .qualification import PHASE_SECONDS, PROFILES, _epoch, _must, bound_json, prospective_replay_pass


def short_predecessor(observation, review):
    observed = bound_json(observation)
    accepted = bound_json(review)
    _must(observed['scheduled_launch_observed'] is True and observed['last_task_result'] == 0
          and observed['owner_absent'] is True and observed['campaign_unchanged_from_sealed_closeout'] is True
          and observed['forward_relaunched'] is False, 'VERIFIED_SCHEDULED_SHORT_RECOVERY_REQUIRED')
    result = bound_json(observed['result'])
    _must(result['status'] == 'QUALIFIED' and result['basis'] == 'SCHEDULED_RECOVERY_OF_SEALED_SHORT'
          and result['original_phase'] == observed['original_phase']
          and result['coverage']['distinct_paper_scheduled_tick_seconds'] == 900
          and result['coverage']['missing_scheduled_tick_seconds'] == 0
          and result['coverage']['shared_fresh_eligible_5m_windows'] == 3 and result['flat_risk'] is True
          and accepted['schema'] == 'LANE_II_SHORT_COMPLETION_REVIEW_V1'
          and accepted['decision'] == 'ACCEPT_RECOVERED_SHORT_PAPER_ONLY' and accepted['short_qualified'] is True
          and accepted['short_holds'] == [] and accepted['artifact_bindings']['recovered_qualification'] == observed['result']
          and accepted['artifact_bindings']['task_observed'] == observation, 'ACCEPTED_SHORT_PREDECESSOR_REQUIRED')
    attempt = bound_json(result['attempt'])
    original = bound_json(attempt['original']['config'])
    replay = bound_json(result['replay'])
    _must(prospective_replay_pass(replay) and replay['risk_parity']['flat'] is True
          and attempt['original']['phase'] == result['original_phase'], 'SHORT_REPLAY_BINDING_REQUIRED')
    terminal = result['terminal_receipt']
    capture = Path(terminal['path']).parent
    risk = validate_closure(capture.parent, original['paper_risk_manifest']['sha256'])
    _must(risk['flat'] and risk['receipt_sha256'] == terminal['sha256']
          and risk['campaign_id'] == original['campaign_id'], 'SHORT_RISK_CLOSURE_REQUIRED')
    return original, terminal


def campaign_state(reference):
    """Read the existing SQLite campaign. The collector is its sole writer."""
    anchor = bound_json(reference)
    db_path = Path(anchor['db_path']).resolve()
    info = db_path.stat()
    _must(anchor['db_file_identity'] == {'device': info.st_dev, 'inode': info.st_ino}
          and anchor['maximum_loss'] == '25.00', 'ORIGINAL_25_DOLLAR_CAMPAIGN_REQUIRED')
    with closing(sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True, timeout=3)) as db:
        db.execute('PRAGMA query_only=ON')
        state = json.loads(db.execute('SELECT value_json FROM state WHERE singleton=1').fetchone()[0])
    _must(all(state[key] == anchor[key] for key in ('campaign_id', 'database_id', 'roster_sha256', 'policy_sha256',
                                                 'maximum_loss')), 'CAMPAIGN_STATE_IDENTITY_MISMATCH')
    return state


def require_flat_predecessor(config, terminal):
    """Apply the same closed-state binding the collector will enforce at open."""
    prior = bound_json(terminal)
    state = campaign_state(config['paper_risk_manifest'])
    endpoint = state['last_terminal']
    crashed = prior.get('schema') == 'LANE_II_CRASH_HANDOFF_V1'
    path_key, hash_key = ('handoff_path', 'handoff_sha256') if crashed else ('receipt_path', 'receipt_sha256')
    _must(state['phase'] == 'CLOSED' and not state['reservations'] and not state['holds']
          and state['campaign_id'] == config['campaign_id'] and endpoint[hash_key] == terminal['sha256']
          and Path(endpoint[path_key]).resolve() == Path(terminal['path']).resolve()
          and not any(row['positions'] for row in prior['account_carryover'].values()), 'FLAT_CAMPAIGN_PREDECESSOR_REQUIRED')
    expected = {**prior['paper_risk_terminal_state'], 'phase': 'CLOSED', 'last_terminal': endpoint,
                'carry_accounts': prior['account_carryover']}
    _must(state == expected, 'CAMPAIGN_BALANCE_OR_BUDGET_CONTINUITY_FAILED')
    if crashed:
        from .capture_checkpoint import validate_crash_handoff

        proof = validate_crash_handoff(terminal['path'], config['paper_risk_manifest']['sha256'])
        _must(proof['closed_state'] == state, 'CRASH_PREDECESSOR_PROOF_CHANGED')
    return state


def verify_long_candidate(previous, candidate):
    for source in (previous, candidate):
        verify_candidate(source)
    old = read_json(Path(previous['candidate_dir']) / 'SOURCE_MANIFEST.json')
    new = read_json(Path(candidate['candidate_dir']) / 'SOURCE_MANIFEST.json')
    _must(candidate['roster_sha256'] == previous['roster_sha256']
          and candidate['base_manifest_sha256'] == previous['base_manifest_sha256']
          and candidate['maximum_paper_seconds'] >= PHASE_SECONDS['twenty-four-hour'], 'FROZEN_LONG_ROSTER_REQUIRED')
    allowed = {'study.py', 'CAPTURE_PATCHES.json', 'continuous_capture_checkpoint.py', 'continuous_paper_risk.py',
               'continuous_decision_capture.py'}
    changed = {name for name in set(old['files']) | set(new['files']) if old['files'].get(name) != new['files'].get(name)}
    _must(changed <= allowed and 'continuous_capture_checkpoint.py' in changed,
          'LONG_CANDIDATE_CHANGED_FROZEN_ECONOMICS_OR_LACKS_BARRIERS')


def verified_hours(plan, replay, directory):
    directory = Path(directory).resolve()
    refs = replay['hourly_bindings']
    epochs = replay['hourly_epochs']
    scheduled = range(plan['start_epoch'] + 3600, plan['cutoff_epoch'] + 1, 3600)
    _must([row['epoch'] for row in refs] == epochs == sorted(set(epochs)) and all(epoch in scheduled for epoch in epochs),
          'INVALID_HOURLY_REPLAY_SEQUENCE')
    for reference in refs:
        epoch, ref = reference['epoch'], reference['binding']
        _must(epoch == int(epoch) and Path(ref['path']).resolve() == directory / 'hours' / f'{int(epoch)}.json',
              'HOUR_RECEIPT_WRONG_ATTEMPT')
        hour = bound_json(ref)
        _must(hour['schema'] == 'LANE_II_HOURLY_REPLAY_HOUR_V1' and hour['epoch'] == epoch and hour['quarters'] == 4
              and hour['semantic_prefix_pass'] is True and hour['accounts_nonadditive'] is True
              and set(hour['accounting']) == PROFILES and len(hour['range_receipts']) == 4, 'HOURLY_ACCOUNTING_REQUIRED')
        previous = None
        for due, interval in zip(range(int(epoch) - 2700, int(epoch) + 1, 900), hour['range_receipts'], strict=True):
            _must(Path(interval['path']).resolve().parent == directory / 'ranges', 'RANGE_RECEIPT_WRONG_ATTEMPT')
            prefix = bound_json(interval)
            _must(prefix['schema'] == 'LANE_II_INCREMENTAL_REPLAY_RANGE_V1' and prefix['semantic_prefix_pass'] is True
                  and prefix['coverage']['planned_through_epoch'] == due and set(prefix['accounting']) == PROFILES
                  and (previous is None or prefix['previous_receipt_sha256'] == previous), 'HOURLY_RANGE_PARITY_REQUIRED')
            previous = interval['sha256']
        _must(prefix['accounting'] == hour['accounting'], 'HOUR_ACCOUNTING_AND_PREFIX_DIFFER')
    return epochs


def assess_window(plan, terminal, replay, risk, hourly_epochs, *, attempts, crashes):
    """The phase clock stays fixed across attempts. Coverage never fills outages."""
    start, cutoff = plan['start_epoch'], plan['cutoff_epoch']
    duration = PHASE_SECONDS[plan['phase']]
    _must(cutoff - start == duration and start % 900 == 0, 'FIXED_PHASE_INTERVAL_REQUIRED')
    coverage = replay.get('coverage', {})
    holds = []
    if attempts != 1 or crashes:
        holds.append('DISCONTINUOUS_ATTEMPT_HISTORY')
    if (terminal.get('state'), terminal.get('reason')) != ('COMPLETE', 'DEADLINE') or not (
            _epoch(terminal['paper_start_utc']) == start and _epoch(terminal['cutoff_utc']) == cutoff
            and _epoch(terminal['actual_end_utc']) >= cutoff and terminal.get('snapshot_publish_gaps') == 0):
        holds.append('ORIGINAL_WINDOW_OR_GENUINE_COMPLETION_UNPROVEN')
    if not (prospective_replay_pass(replay) and replay.get('source_events_sha256') == terminal['events_sha256']
            and replay['risk_parity']['immutable_closure'] == risk):
        holds.append('EXACT_CAUSAL_AND_ACCOUNTING_PARITY_REQUIRED')
    windows = list(range(start, cutoff, 300))
    if not (coverage.get('distinct_paper_scheduled_tick_seconds') == duration
            and coverage.get('expected_scheduled_tick_seconds') == duration
            and coverage.get('missing_scheduled_tick_seconds') == 0
            and coverage.get('expected_5m_window_starts') == windows
            and coverage.get('shared_fresh_eligible_window_starts') == windows
            and coverage.get('missing_data_treated_as_abstention') is False and coverage.get('trade_quota_used') is False
            and set(coverage.get('per_profile', {})) == PROFILES):
        holds.append('PAPER_TICK_OR_MATCHED_MARKET_WINDOW_GAP')
    if hourly_epochs != list(range(start + 3600, cutoff + 1, 3600)):
        holds.append('MISSING_HOURLY_INCREMENTAL_REPLAY_OR_ACCOUNTING')
    if not (risk.get('flat') is True and replay.get('risk_parity', {}).get('flat') is True):
        holds.append('OPEN_OR_UNPROVEN_TERMINAL_EXPOSURE')
    if any(type(terminal.get(k)) is not int or terminal[k] != 0
           for k in ('real_orders', 'model_api_calls', 'authenticated_account_calls')):
        holds.append('PAPER_BOUNDARY_UNPROVEN')
    return {'status': 'HOLD' if holds else 'QUALIFIED', 'holds': holds,
            'phase': plan['phase'], 'start_epoch': start, 'cutoff_epoch': cutoff, 'coverage': coverage,
            'hourly_verified_epochs': hourly_epochs, 'attempts': attempts, 'crashes': crashes,
            'flat_risk': risk.get('flat') is True, 'profitability_qualified': False, 'funded_authority': False}
