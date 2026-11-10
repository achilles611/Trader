"""Bounded P-model sensitivity study against the retained five-minute tape."""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import math
import os
from pathlib import Path
import re
import sys
import time

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.lane_ii_continuous.common import atomic_json, binding, utc
from scripts.lane_ii_continuous.qualification import _must, bound_json
from scripts.lane_ii_continuous.qualification_replay import offline_boundary
from scripts.overnight_paper import replay_study as retained


SCHEMA = 'LANE_II_P_MODEL_SENSITIVITY_V1'
BUDGET = ('budget', 'high_confidence_budget', 'high_volatility_budget')
P2_P5_SCHEMA = 'LANE_II_P2_P5_EXPERIMENT_V1'
P2_P5_SCOPE = 'RETAINED_P2_P5_ONLY'
P2_P5_PARENTS = {'P2': '5m-P2', 'P5': '5m-P5__confluence'}
ARM_ID = re.compile(r'[a-z0-9][a-z0-9-]{0,47}')
SHARED_BOUNDS = {
    'budget': (.01, 25), 'depth_fraction': (.01, .5), 'spread_max': (.001, .10),
    retained.FRICTION_CAP: (.01, .5), 'profit_target_fraction': (.05, 2), 'stop': (.01, .75),
    'history_seconds': (2, 60), 'agreement_seconds': (1, 60),
    'minimum_score_magnitude': (.01, 1), 'soft_exit_min_hold_seconds': (0, 120),
    'soft_exit_persistence_seconds': (1, 60),
}
FAMILY_BOUNDS = {
    'P2': {
        'entry_elapsed_min': (0, 299), 'entry_elapsed_max': (1, 299), 'minimum_aligned_inputs': (1, 4),
        'minimum_book_momentum': (0, 2), 'minimum_spot30': (0, 2), 'minimum_net_target_fraction': (0, 1),
        'target_cash_headroom_fraction': (.01, 1),
    },
    'P5': {
        'entry_remaining_min': (1, 299), 'entry_remaining_max': (1, 299),
        'high_confidence_budget': (.01, 25), 'high_confidence_min_score': (.01, 1),
        'high_confidence_min_aligned': (1, 4), 'sustained_component_minimum': (0, 1),
        'minimum_proxy_margin': (0, .5), 'minimum_total_payout_multiple': (1, 5),
    },
}


def select_profiles(baseline, specs):
    active, controls = {}, {}
    for spec in specs:
        identity, family = spec['id'], spec['model']['id']
        if spec['horizon_seconds'] != 300 or not family.removeprefix('P').isdigit():
            continue
        row = baseline['profiles'][identity]
        if (row['unresolved_positions'] or not baseline['parity'][identity]['pass']
                or not baseline['recorded_decision_parity'][identity]['pass']):
            continue
        selected = {'family': family, 'id': identity, 'realized_pnl': row['realized_pnl'],
                    'entries': row['entries'], 'closed_entries': row['cash_exits'] + row['settlements'],
                    'exploratory': row['entries'] == 0}
        if not row['entries'] and identity == f'5m-{family}':
            controls[family] = selected
        elif selected['closed_entries'] == row['entries'] and row['entries']:
            prior = active.get(family)
            if prior is None or (-selected['realized_pnl'], identity) < (-prior['realized_pnl'], prior['id']):
                active[family] = selected
    ranked = sorted(active.values(), key=lambda row: (-row['realized_pnl'], row['id']))
    exploratory = [controls[family] for family in sorted(controls, key=lambda key: int(key[1:]))
                   if family not in active]
    _must(len(ranked) + len(exploratory) >= 5, 'FIVE_EXACT_RETAINED_FAMILIES_REQUIRED')
    return (ranked + exploratory)[:5]


def variants(spec):
    model = spec['model']
    target = 'profit_target_fraction' if 'profit_target_fraction' in model else 'target'
    factors = [('stake', BUDGET, (0.50, 0.65, 0.75, 0.90)), ('target', (target,), (0.75, 1.25)),
               ('spread', ('spread_max',), (0.60, 0.80)),
               ('friction', (retained.FRICTION_CAP,), (0.60, 0.80))]
    result = []
    for dimension, keys, scales in factors:
        before = {key: model[key] for key in keys if key in model}
        _must(before and all(0 < value for value in before.values()), 'RETAINED_VARIANT_PARAMETER_REQUIRED')
        for factor in scales:
            result.append({'id': f'{spec["id"]}__offline_{dimension}_{round(factor * 100):03}',
                           'parent': spec['id'], 'dimension': dimension, 'factor': factor, 'before': before,
                           'after': {key: round(value * factor, 8) for key, value in before.items()}})
    return result


def retained_roster_bound(baseline):
    count = baseline['source_profile_count']
    _must(baseline['schema'] == 'OVERNIGHT_STUDY_REPLAY_RESULT_V1'
          and type(count) is int and count > 0 and count == baseline['source_profiles_with_financial_parity']
          == baseline['source_profiles_with_recorded_decision_parity'], 'EXACT_RETAINED_BASELINE_REQUIRED')
    return count


def experiment_scope(experiment, baseline, profiles):
    count = retained_roster_bound(baseline)
    models = experiment.get('models')
    _must(experiment.get('schema') == P2_P5_SCHEMA and isinstance(experiment.get('id'), str)
          and 0 < len(experiment['id']) <= 80 and isinstance(experiment.get('protocol'), dict)
          and isinstance(models, list) and len(models) == 2 and all(isinstance(row, dict) for row in models)
          and {row.get('family') for row in models} == P2_P5_PARENTS.keys(), 'EXACT_P2_P5_EXPERIMENT_REQUIRED')
    specs = {spec['id']: spec for spec in profiles}
    _must(len(specs) == len(profiles) == count, 'RESEARCH_ROSTER_CHANGED')
    selected, variations = [], []
    for row in sorted(models, key=lambda item: item['family']):
        family = row['family']
        parent = P2_P5_PARENTS[family]
        spec = specs.get(parent)
        _must(row.get('parent_id') == parent and spec and spec['horizon_seconds'] == 300
              and spec['model']['id'] == family, 'FROZEN_P2_P5_PARENT_REQUIRED:' + family)
        result = baseline['profiles'][parent]
        closed = result['cash_exits'] + result['settlements']
        _must(baseline['parity'][parent]['pass'] is True and baseline['recorded_decision_parity'][parent]['pass'] is True
              and result['unresolved_positions'] == 0 and closed == result['entries'], 'P2_P5_PARENT_PARITY:' + family)
        selected.append({'family': family, 'id': parent, 'realized_pnl': result['realized_pnl'],
                         'entries': result['entries'], 'closed_entries': closed, 'exploratory': closed == 0})
        arms = row.get('arms')
        _must(isinstance(arms, list) and 0 < len(arms) <= count - len(P2_P5_PARENTS),
              'P2_P5_ARMS_REQUIRED:' + family)
        original = spec['model']
        bounds = SHARED_BOUNDS | FAMILY_BOUNDS[family]
        seen_ids, seen_overrides = set(), set()
        for arm in arms:
            _must(isinstance(arm, dict) and isinstance(arm.get('id'), str) and ARM_ID.fullmatch(arm['id']),
                  'INVALID_P2_P5_ARM_ID')
            name = arm['id']
            overrides = arm.get('overrides')
            _must(isinstance(overrides, dict) and overrides and overrides.keys() <= bounds.keys()
                  and overrides.keys() <= original.keys(), 'UNSUPPORTED_P2_P5_OVERRIDE:' + family + ':' + name)
            for key, value in overrides.items():
                lo, hi = bounds[key]
                _must(type(value) in (int, float) and lo <= value <= hi and math.isfinite(value)
                      and (type(original[key]) is not int or type(value) is int),
                      'P2_P5_PARAMETER_BOUND:' + family + ':' + name + ':' + key)
                _must(value != original[key], 'P2_P5_NOOP_OVERRIDE:' + family + ':' + name + ':' + key)
            tuned = original | overrides
            window = 'entry_elapsed_' if family == 'P2' else 'entry_remaining_'
            _must(tuned['agreement_seconds'] <= tuned['history_seconds'] and tuned[window + 'min'] <= tuned[window + 'max']
                  and (family != 'P5' or tuned['budget'] <= tuned['high_confidence_budget']),
                  'P2_P5_INCONSISTENT_PARAMETERS:' + family + ':' + name)
            override_key = tuple(sorted(overrides.items()))
            _must(name not in seen_ids and override_key not in seen_overrides, 'DUPLICATE_P2_P5_ARM:' + family + ':' + name)
            seen_ids.add(name)
            seen_overrides.add(override_key)
            identity = f'{parent}__p2p5_{name}'
            _must(identity not in specs, 'P2_P5_ARM_COLLIDES_WITH_RETAINED_ID:' + identity)
            variations.append({'id': identity, 'parent': parent, 'arm': name,
                               'before': {key: original[key] for key in overrides}, 'after': overrides})
    _must(len(selected) + len(variations) <= count, 'RESEARCH_EXCEEDS_MEASURED_ROSTER')
    return selected, variations


def prepare(baseline, output, cohort=None, wallet_registry=None, *, experiment=None):
    reference = binding(baseline)
    baseline = bound_json(reference)
    inputs = bound_json(baseline['inputs'])
    count = retained_roster_bound(baseline)
    if experiment is None:
        _must(cohort is not None and wallet_registry is not None, 'RETAINED_RESEARCH_COHORT_AND_WALLETS_REQUIRED')
        selected = select_profiles(baseline, inputs['profiles'])
        specs = {spec['id']: spec for spec in inputs['profiles']}
        variations = [variation for parent in selected for variation in variants(specs[parent['id']])]
        scope = {'scope': 'RETAINED_RESEARCH_ONLY', 'cohort_reference': binding(cohort),
                 'wallet_registry': binding(wallet_registry), 'current_wallet_admission': 'SOURCE_UNOBSERVED'}
    else:
        _must(cohort is None and wallet_registry is None, 'P2_P5_EXPERIMENT_EXCLUDES_COHORT_AND_WALLETS')
        experiment_ref = binding(experiment)
        selected, variations = experiment_scope(bound_json(experiment_ref), baseline, inputs['profiles'])
        scope = {'scope': P2_P5_SCOPE, 'experiment': experiment_ref}
    _must(len(selected) + len(variations) <= count, 'RESEARCH_EXCEEDS_MEASURED_ROSTER')
    _must(not Path(output).exists(), 'RESEARCH_PLAN_ALREADY_EXISTS')
    atomic_json(output, {'schema': SCHEMA, 'created_utc': utc(), 'baseline': reference,
                         'selected': selected, 'variants': variations, 'retained_roster_bound': count, **scope})


@offline_boundary()
def run(plan, output, status, maximum_seconds):
    import psutil

    plan = bound_json(binding(plan))
    baseline = bound_json(plan['baseline'])
    inputs = bound_json(baseline['inputs'])
    sources = inputs['sources']
    scope = plan['scope']
    count = retained_roster_bound(baseline)
    _must(plan['schema'] == SCHEMA and plan['retained_roster_bound'] == count
          and scope in ('RETAINED_RESEARCH_ONLY', P2_P5_SCOPE), 'FROZEN_RESEARCH_SCOPE_REQUIRED')
    if scope == P2_P5_SCOPE:
        _must(type(maximum_seconds) is int and 0 < maximum_seconds <= 1800, 'P2_P5_WALL_TIME_BOUND_REQUIRED')
        _must(not ({'cohort_reference', 'wallet_registry'} & plan.keys()), 'P2_P5_EXPERIMENT_EXCLUDES_COHORT_AND_WALLETS')
        selected, expected = experiment_scope(bound_json(plan['experiment']), baseline, inputs['profiles'])
    else:
        _must('experiment' not in plan, 'FROZEN_RESEARCH_SCOPE_REQUIRED')
        selected = select_profiles(baseline, inputs['profiles'])
        specs = {spec['id']: spec for spec in inputs['profiles']}
        expected = [variant for parent in selected for variant in variants(specs[parent['id']])]
    _must(selected == plan['selected'] and expected == plan['variants']
          and len(expected) + len(selected) <= count, 'RESEARCH_VARIANTS_CHANGED')
    candidate = Path(sources['candidate_manifest']['path']).parent
    study = Path(sources['receipt']['path']).parent.parent
    output = output.resolve()
    capture, receipt, prereg, actual = retained.sealed_input(study, candidate, output)
    _must(actual == sources, 'EXACT_RETAINED_RESEARCH_SOURCE_REQUIRED')
    shim, helpers, market_types = retained.load_runtime(candidate)
    registry = shim.bound_module('replay_study_roster', candidate / 'roster.py')
    frozen = registry.profile_specs()
    _must(frozen == prereg['profiles'] == inputs['profiles'], 'RESEARCH_ROSTER_CHANGED')
    identities = {row['id'] for row in selected}
    chosen = {spec['id']: spec for spec in frozen if spec['id'] in identities}
    observed_rows = {row['id']: row for row in receipt['profiles'] if row['id'] in identities}
    observed = {identity: retained.capture_ledger(capture / f'{identity}-ledger.jsonl',
                                                 receipt['ledger_sha256'][identity]) for identity in identities}
    start, cutoff = retained.stamp(receipt['paper_start_utc']), retained.stamp(receipt['cutoff_utc'])
    decision_bound, observed_wall = inputs['decision_bound_epoch'], retained.stamp(receipt['utc'])
    _must(start < decision_bound <= min(cutoff, observed_wall), 'RETAINED_DECISION_CLOCK_REQUIRED')
    entry_events = []

    def record_entry(row):
        kind, data = row['kind'], row['data']
        if kind not in ('PAPER_ENTRY_INTENT', 'PAPER_ENTRY_INTENT_CANCELLED', 'PAPER_ENTRY'):
            return
        _must(len(entry_events) < 20_000, 'P2_P5_ENTRY_TRACE_BOUND')
        signal = data.get('cancelled_signal', data.get('signal', data))
        entry_events.append({'profile_id': row['profile_id'], 'kind': kind, 'market': data['market'],
                             'side': data.get('side') or signal.get('decision', {}).get('side'),
                             'signal_wall': signal.get('signal_wall'), 'available_after': signal.get('available_after'),
                             'evaluation_wall': row['evaluation_wall'], 'source_event_seq': row['trigger_source_event_seq'],
                             'reason': data.get('reason')})

    engine = retained.StudyReplay(capture, start, cutoff, decision_bound, chosen.values(), registry, helpers,
                                  market_types, record_entry if scope == P2_P5_SCOPE else lambda row: None, None, None)
    for variant in expected:
        profile = engine.add_profile(chosen[variant['parent']], variant['id'])
        profile.model = deepcopy(profile.model)
        profile.model.update(variant['after'])
    output.mkdir(parents=True, exist_ok=False)
    deadline = time.monotonic() + maximum_seconds
    process, peak = psutil.Process(os.getpid()), 0
    previous, mono, sequence = '0' * 64, 0, 0
    tape = hashlib.sha256()
    atomic_json(status, {'state': 'REPLAYING', 'source_events': 0, 'utc': utc()})
    with (capture / 'events.jsonl').open('rb') as journal:
        for sequence, line in enumerate(journal, 1):
            tape.update(line)
            row = retained.parse_event(line)
            _must(row['seq'] == sequence and row['previous_sha256'] == previous and row['monotonic_ns'] >= mono,
                  'RESEARCH_JOURNAL_CHAIN_OR_CLOCK')
            if sequence == 1:
                _must(row['kind'] == 'START' and row['data']['preregistration_sha256'] == receipt['preregistration_sha256']
                      and row['data']['manifest_sha256'] == receipt['source_manifest_sha256'], 'RESEARCH_START_BINDING')
            previous, mono = row['sha256'], row['monotonic_ns']
            engine.consume(row)
            if sequence % 1000 == 0:
                peak = max(peak, process.memory_info().rss)
                _must(time.monotonic() <= deadline, 'RESEARCH_WALL_TIME_BOUND')
                _must(peak <= 256 * 2**20, 'RESEARCH_MEMORY_BOUND')
            if sequence % 10000 == 0:
                atomic_json(status, {'state': 'REPLAYING', 'source_events': sequence, 'peak_rss_bytes': peak, 'utc': utc()})
    if scope == P2_P5_SCOPE:
        peak = max(peak, process.memory_info().rss)
        _must(time.monotonic() <= deadline, 'RESEARCH_WALL_TIME_BOUND')
        _must(peak <= 256 * 2**20, 'RESEARCH_MEMORY_BOUND')
    _must(sequence == receipt['events'] == baseline['source_events'] and previous == receipt['chain_sha256']
          and tape.hexdigest() == receipt['events_sha256'] == baseline['source_events_sha256']
          and row['kind'] == 'END' and row['data']['state'] == receipt['state'] and row['data']['reason'] == receipt['reason'],
          'RESEARCH_END_BINDING')
    replayed = engine.results(observed_wall)
    parity = {}
    for identity in sorted(identities):
        financial = retained.compare_parity(observed[identity]['financial'], engine.financial[identity],
                                            observed_rows[identity], replayed[identity])
        decision = retained.compare_events(observed[identity]['decisions'], engine.decisions[identity])
        inventory = retained.inventory(receipt['terminal_censoring'][identity]) == replayed[identity]['unresolved_inventory']
        parity[identity] = {'financial': financial, 'decisions': decision, 'inventory_matches': inventory}
        _must(financial['pass'] and decision['pass'] and inventory, 'RESEARCH_PARENT_PARITY:' + identity)
    _must(all(binding(ref['path']) == ref for ref in actual.values()), 'RESEARCH_BINDING_CHANGED_DURING_READ')
    retained.verify_candidate(candidate, receipt['source_manifest_sha256'])
    result = output / 'RESULT.json'
    if scope == P2_P5_SCOPE:
        _must(binding(plan['experiment']['path']) == plan['experiment'], 'P2_P5_EXPERIMENT_CHANGED_DURING_REPLAY')
        trace = output / 'ENTRY_EVENTS.json'
        atomic_json(trace, {'schema': 'LANE_II_P2_P5_ENTRY_TRACE_V1', 'events': entry_events,
                            'source_events_sha256': tape.hexdigest(), 'experiment': plan['experiment']})
        experiment_result = {'scope': scope, 'experiment': plan['experiment'], 'entry_events': binding(trace)}
    else:
        experiment_result = {'cohort_reference': plan['cohort_reference']}
    atomic_json(result, {'schema': SCHEMA, 'status': 'COMPLETE_RETAINED_RESEARCH', 'source_events': sequence,
                         'source_events_sha256': tape.hexdigest(), 'source_state': receipt['state'],
                         'observed_seconds': decision_bound - start, 'peak_rss_bytes': peak, 'parent_parity': parity,
                         'parents': {identity: replayed[identity] for identity in identities},
                         'variants': [{**item, 'result': replayed[item['id']], 'delta': retained.comparison_delta(
                             replayed[item['id']], replayed[item['parent']])} for item in expected],
                         **experiment_result, 'network_calls': 0, 'real_orders': 0,
                         'campaign_mutations': 0, 'profitability_qualified': False})
    atomic_json(status, {'state': 'COMPLETE_RETAINED_RESEARCH', 'source_events': sequence, 'variants': len(expected),
                         'parents': len(identities), 'result': binding(result), 'utc': utc()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('prepare', 'run'))
    for flag in ('baseline', 'output', 'plan', 'status', 'cohort', 'wallet-registry', 'experiment'):
        parser.add_argument('--' + flag, type=Path)
    parser.add_argument('--maximum-seconds', type=int, default=480)
    args = parser.parse_args()
    if args.command == 'prepare':
        if args.baseline is None or args.output is None:
            parser.error('prepare requires --baseline and --output')
        prepare(args.baseline, args.output, args.cohort, args.wallet_registry, experiment=args.experiment)
    else:
        if args.plan is None or args.output is None or args.status is None or args.experiment is not None:
            parser.error('run requires --plan, --output and --status and uses the bound experiment')
        try:
            run(args.plan, args.output, args.status, args.maximum_seconds)
        except (OSError, KeyError, ValueError, TypeError) as exc:
            atomic_json(args.status, {'state': 'HELD', 'reason': str(exc)[:200], 'utc': utc()})
            raise


if __name__ == '__main__':
    main()
