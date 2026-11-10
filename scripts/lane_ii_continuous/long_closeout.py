"""Verify a stopped collector and hand its immutable inputs to hourly replay."""
from __future__ import annotations

import argparse
import math
from pathlib import Path
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.lane_ii_continuous.closeout import MIB, audit_capture_readonly, closeout_capture
from scripts.lane_ii_continuous.common import atomic_json, binding, read_json
from scripts.lane_ii_continuous.prospective_supervisor import build_supervisor_result
from scripts.lane_ii_continuous.qualification import _epoch, _must, bound_json


def ledger_budget(receipt):
    seconds = _epoch(receipt['cutoff_utc']) - _epoch(receipt['paper_start_utc'])
    _must(0 < seconds <= 86400, 'LONG_LEDGER_DURATION_BOUND')
    return math.ceil(seconds / 3600) * 64 * MIB


def prepare_final_inputs(run, candidate, expected_manifest, output, *, rehearsal=False):
    run, candidate, output = (Path(path).resolve() for path in (run, candidate, output))
    _must(not output.is_relative_to(run) and not output.is_relative_to(candidate), 'EXTERNAL_LONG_AUDIT_OUTPUT_REQUIRED')
    handoff_dir = output if rehearsal else run
    final_inputs = handoff_dir / 'FINAL_INPUTS.json'
    if final_inputs.is_file():
        existing = bound_json(binding(final_inputs))
        _must(existing['schema'] == 'LANE_II_HOURLY_REPLAY_FINAL_INPUTS_V1'
              and set(existing['bindings']) == {'raw_audit', 'supervisor_result'}, 'FINAL_INPUTS_SCHEMA')
        for reference in existing['bindings'].values():
            bound_json(reference)
        bound_json(bound_json(binding(handoff_dir / 'TERMINAL_ACCOUNTING.json')))
        return existing
    capture = run / 'capture'
    max_ledger_bytes = ledger_budget(read_json(capture / 'receipt.json'))
    audit = audit_capture_readonly(capture, candidate, output / 'raw', maximum_ledger_bytes=max_ledger_bytes)
    _must(audit['status'] == 'PASS_RETAINED_CAPTURE_INTEGRITY_ONLY' and not audit['problem_counts'],
          'LONG_RAW_CAPTURE_INTEGRITY_FAILED')
    raw_path = output / 'raw' / 'RAW_CAPTURE_AUDIT.json'
    supervisor_path = output / 'PAPER_RUN_RESULT.json'
    build_supervisor_result(candidate, run, raw_path, supervisor_path, expected_manifest_sha256=expected_manifest)
    closeout_capture(run, output / 'accounting', source_dir=candidate, max_ledger_bytes=max_ledger_bytes)
    inputs = {'schema': 'LANE_II_HOURLY_REPLAY_FINAL_INPUTS_V1',
              'bindings': {'raw_audit': binding(raw_path), 'supervisor_result': binding(supervisor_path)}}
    atomic_json(handoff_dir / 'TERMINAL_ACCOUNTING.json', binding(output / 'accounting' / 'CLOSEOUT.json'))
    atomic_json(final_inputs, inputs)
    return inputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--rehearsal', action='store_true')
    args = parser.parse_args()
    prepare_final_inputs(args.run_dir, args.candidate, args.manifest_sha256, args.output, rehearsal=args.rehearsal)


if __name__ == '__main__':
    main()
