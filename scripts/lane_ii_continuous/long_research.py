"""Bounded P-model sensitivity study against the retained five-minute tape."""
from __future__ import annotations

import argparse
from copy import deepcopy
import hashlib
import os
from pathlib import Path
import sys
import time

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.lane_ii_continuous.common import atomic_json, binding, utc
from scripts.lane_ii_continuous.qualification import _must, bound_json
from scripts.overnight_paper import replay_study as retained


SCHEMA = 'LANE_II_P_MODEL_SENSITIVITY_V1'
BUDGET = ('budget', 'high_confidence_budget', 'high_volatility_budget')


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


def prepare(baseline, output, cohort, wallet_registry):
    reference = binding(baseline)
    baseline = bound_json(reference)
    inputs = bound_json(baseline['inputs'])
    count = baseline['source_profile_count']
    _must(baseline['schema'] == 'OVERNIGHT_STUDY_REPLAY_RESULT_V1'
          and count == baseline['source_profiles_with_financial_parity']
          == baseline['source_profiles_with_recorded_decision_parity'], 'EXACT_RETAINED_BASELINE_REQUIRED')
    selected = select_profiles(baseline, inputs['profiles'])
    specs = {spec['id']: spec for spec in inputs['profiles']}
    variations = [variation for parent in selected for variation in variants(specs[parent['id']])]
    _must(len(selected) + len(variations) <= count, 'RESEARCH_EXCEEDS_MEASURED_ROSTER')
    cohort_ref, wallet_ref = binding(cohort), binding(wallet_registry)
    _must(not Path(output).exists(), 'RESEARCH_PLAN_ALREADY_EXISTS')
    atomic_json(output, {'schema': SCHEMA, 'created_utc': utc(), 'baseline': reference,
                         'selected': selected, 'variants': variations, 'retained_roster_bound': count,
                         'cohort_reference': cohort_ref, 'wallet_registry': wallet_ref,
                         'current_wallet_admission': 'SOURCE_UNOBSERVED', 'scope': 'RETAINED_RESEARCH_ONLY'})


def run(plan, output, status, maximum_seconds):
    import psutil

    plan = bound_json(binding(plan))
    baseline = bound_json(plan['baseline'])
    inputs = bound_json(baseline['inputs'])
    sources = inputs['sources']
    _must(plan['schema'] == SCHEMA and plan['scope'] == 'RETAINED_RESEARCH_ONLY'
          and plan['selected'] == select_profiles(baseline, inputs['profiles']), 'FROZEN_RESEARCH_SCOPE_REQUIRED')
    candidate = Path(sources['candidate_manifest']['path']).parent
    study = Path(sources['receipt']['path']).parent.parent
    output = output.resolve()
    capture, receipt, prereg, actual = retained.sealed_input(study, candidate, output)
    _must(actual == sources, 'EXACT_RETAINED_RESEARCH_SOURCE_REQUIRED')
    shim, helpers, market_types = retained.load_runtime(candidate)
    registry = shim.bound_module('replay_study_roster', candidate / 'roster.py')
    frozen = registry.profile_specs()
    _must(frozen == prereg['profiles'] == inputs['profiles'], 'RESEARCH_ROSTER_CHANGED')
    identities = {row['id'] for row in plan['selected']}
    chosen = {spec['id']: spec for spec in frozen if spec['id'] in identities}
    expected = [variant for parent in plan['selected'] for variant in variants(chosen[parent['id']])]
    _must(expected == plan['variants'] and len(expected) + len(chosen) <= len(frozen), 'RESEARCH_VARIANTS_CHANGED')
    observed_rows = {row['id']: row for row in receipt['profiles'] if row['id'] in identities}
    observed = {identity: retained.capture_ledger(capture / f'{identity}-ledger.jsonl',
                                                 receipt['ledger_sha256'][identity]) for identity in identities}
    start, cutoff = retained.stamp(receipt['paper_start_utc']), retained.stamp(receipt['cutoff_utc'])
    decision_bound, observed_wall = inputs['decision_bound_epoch'], retained.stamp(receipt['utc'])
    _must(start < decision_bound <= min(cutoff, observed_wall), 'RETAINED_DECISION_CLOCK_REQUIRED')
    engine = retained.StudyReplay(capture, start, cutoff, decision_bound, chosen.values(), registry, helpers,
                                  market_types, lambda row: None, None, None)
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
    atomic_json(result, {'schema': SCHEMA, 'status': 'COMPLETE_RETAINED_RESEARCH', 'source_events': sequence,
                         'source_events_sha256': tape.hexdigest(), 'source_state': receipt['state'],
                         'observed_seconds': decision_bound - start, 'peak_rss_bytes': peak, 'parent_parity': parity,
                         'parents': {identity: replayed[identity] for identity in identities},
                         'variants': [{**item, 'result': replayed[item['id']], 'delta': retained.comparison_delta(
                             replayed[item['id']], replayed[item['parent']])} for item in expected],
                         'cohort_reference': plan['cohort_reference'], 'network_calls': 0, 'real_orders': 0,
                         'campaign_mutations': 0, 'profitability_qualified': False})
    atomic_json(status, {'state': 'COMPLETE_RETAINED_RESEARCH', 'source_events': sequence, 'variants': len(expected),
                         'parents': len(identities), 'result': binding(result), 'utc': utc()})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('prepare', 'run'))
    for flag in ('baseline', 'output', 'plan', 'status', 'cohort', 'wallet-registry'):
        parser.add_argument('--' + flag, type=Path)
    parser.add_argument('--maximum-seconds', type=int, default=480)
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args.baseline, args.output, args.cohort, args.wallet_registry)
    else:
        try:
            run(args.plan, args.output, args.status, args.maximum_seconds)
        except (OSError, KeyError, ValueError, TypeError) as exc:
            atomic_json(args.status, {'state': 'HELD', 'reason': str(exc)[:200], 'utc': utc()})
            raise


if __name__ == '__main__':
    main()
