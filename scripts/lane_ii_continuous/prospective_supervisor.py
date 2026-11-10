"""Bind an actual owned exit and the frozen study's cold audit to a new capture.

Run inside the offline child boundary. This adapter never fills in missing
historical receipts or writes into the capture or candidate.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import importlib.util
from pathlib import Path
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.lane_ii_continuous.capture import verify_candidate
from scripts.lane_ii_continuous.closeout import owner_observation
from scripts.lane_ii_continuous.common import atomic_json, binding, read_json, utc

SCHEMA = 'LANE_II_PROSPECTIVE_SUPERVISOR_RESULT_V1'


def _require(condition, reason):
    if not condition:
        raise ValueError(reason)


def _stamp(value):
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    _require(parsed.tzinfo is not None, 'TIMEZONE_REQUIRED')
    return parsed.timestamp()


def _metadata(path):
    _require(path.stat().st_size <= 8 * 2**20, 'SUPERVISOR_METADATA_TOO_LARGE')
    return read_json(path)


def _cold_audit(candidate, capture):
    spec = importlib.util.spec_from_file_location('lane_ii_bound_study', candidate / 'study.py')
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(candidate))
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
        return module.cold_audit(capture)
    finally:
        sys.modules.pop(spec.name, None)
        sys.path.remove(str(candidate))


def build_supervisor_result(candidate, run_dir, raw_audit, output, *, expected_manifest_sha256,
                            process_probe=owner_observation):
    candidate, run, audit_path, output = map(lambda p: Path(p).resolve(), (candidate, run_dir, raw_audit, output))
    capture = run / 'capture'
    _require(not output.is_relative_to(run) and not output.is_relative_to(candidate) and not output.exists(),
             'NEW_EXTERNAL_SUPERVISOR_RESULT_REQUIRED')
    paths = {'receipt': capture / 'receipt.json', 'preregistration': capture / 'preregistration.json',
             'collection_closed': capture / 'COLLECTION_CLOSED.json', 'worker_exit': run / 'exit.json',
             'worker_identity': run / 'operator.json', 'raw_audit': audit_path,
             'source_manifest': candidate / 'SOURCE_MANIFEST.json'}
    values = {key: _metadata(path) for key, path in paths.items()}
    binds = {key: binding(path) for key, path in paths.items()}
    manifest = {'candidate_dir': str(candidate), 'source_manifest_sha256': expected_manifest_sha256}
    verify_candidate(manifest)
    receipt, prereg, marker, exited, owner, audit = (values[k] for k in
        ('receipt', 'preregistration', 'collection_closed', 'worker_exit', 'worker_identity', 'raw_audit'))
    _require((receipt.get('state'), receipt.get('reason')) in {('COMPLETE', 'DEADLINE'), ('STOPPED', 'USER_STOP')},
             'NO_NORMAL_REPLAYABLE_EXECUTION')
    _require(all(type(receipt.get(key)) is int and receipt[key] == 0 for key in
                 ('real_orders', 'authenticated_account_calls', 'model_api_calls')), 'PAPER_ONLY_RECEIPT_REQUIRED')
    _require(all(values[key].get('source_manifest_sha256') == expected_manifest_sha256 for key in
                 ('receipt', 'preregistration', 'collection_closed', 'worker_exit', 'worker_identity')),
             'SUPERVISOR_SOURCE_IDENTITY_MISMATCH')
    _require(prereg.get('paper_start_utc') and _stamp(prereg['paper_start_utc']) == _stamp(receipt['paper_start_utc'])
             and _stamp(prereg['cutoff_utc']) == _stamp(receipt['cutoff_utc']) == _stamp(marker['fixed_cutoff_utc'])
             and receipt.get('preregistration_sha256') == binds['preregistration']['sha256'], 'FIXED_WINDOW_BINDING')
    _require(type(owner.get('pid')) is int and owner['pid'] == exited.get('pid') == receipt.get('pid') == marker.get('worker_pid')
             and type(owner.get('creation_epoch')) in {int, float}
             and owner['creation_epoch'] == exited.get('creation_epoch')
             and owner.get('boot_epoch') == exited.get('boot_epoch')
             and owner.get('executable_sha256') == exited.get('executable_sha256')
             and owner.get('owned_job_before_startup_gate') is True
             and exited.get('retained_handle_exit_observed') is True and exited.get('owned_job_closed') is True
             and exited.get('owned_children_at_parent_exit') == [] and exited.get('forced_closure') is False
             and type(exited.get('exit_code')) is int and exited['exit_code'] == 0, 'GENUINE_OWNED_EXIT_REQUIRED')
    _require(process_probe(owner).get('state') in {'ABSENT', 'PID_REUSED'}, 'RETAINED_OWNER_NOT_ABSENT')
    _require(marker.get('schema') == 'BTC_NEXT12_COLLECTION_CLOSED_V1'
             and marker.get('state_at_collection_close') == receipt['state']
             and all(marker.get(k) is True for k in ('collection_closed', 'network_closed', 'producers_exited',
                 'snapshot_publisher_closed', 'finalization_only_no_more_http')), 'GENUINE_COLLECTION_CLOSURE_REQUIRED')
    command = owner.get('command', [])
    _require(len(command) == 13 and command[1:4] == ['-I', '-S', '-B']
             and Path(command[0]).resolve() == Path(sys.executable).resolve()
             and Path(command[4]).resolve() == candidate / 'study.py'
             and command[5:7] == ['worker', '--run-dir'] and Path(command[7]).resolve() == capture
             and command[8] == '--paper-start-utc' and _stamp(command[9]) == _stamp(receipt['paper_start_utc'])
             and command[10] == '--cutoff-utc' and _stamp(command[11]) == _stamp(receipt['cutoff_utc'])
             and command[12] == '--startup-gate'
             and binding(command[0])['sha256'] == owner['executable_sha256'], 'EXACT_COLLECTOR_COMMAND_REQUIRED')
    _require(audit.get('schema') == 'LANE_II_READONLY_CAPTURE_AUDIT_V1'
             and audit.get('status') == 'PASS_RETAINED_CAPTURE_INTEGRITY_ONLY' and audit.get('problem_counts') == {}
             and audit.get('events_checked') == receipt.get('events')
             and Path(audit.get('capture_dir', '')).resolve() == capture
             and Path(audit.get('source_dir', '')).resolve() == candidate
             and all(binds[k] in audit.get('input_bindings', []) for k in ('receipt', 'collection_closed', 'source_manifest', 'preregistration')),
             'INDEPENDENT_RAW_AUDIT_BINDING_REQUIRED')
    if receipt['state'] == 'STOPPED':
        stop = capture / 'STOP'
        _require(_metadata(stop).get('reason'), 'GENUINE_STOP_PROOF_REQUIRED')
        binds['stop_request'] = binding(stop)
        paths['stop_request'] = stop
    audit_result = _cold_audit(candidate, capture)
    _require(audit_result.get('status') == 'PASS_RETAINED_NEXT12_CAPTURE_INTEGRITY_ONLY'
             and audit_result.get('problems') == [] and audit_result.get('events_checked') == receipt['events'],
             'FROZEN_COLD_AUDIT_FAILED')
    _require(all(binding(path) == binds[key] for key, path in paths.items()), 'SUPERVISOR_INPUT_CHANGED')
    verify_candidate(manifest)
    value = {'schema': SCHEMA, 'utc': utc(), 'receipt': receipt, 'worker_exit_code': exited['exit_code'],
             'forced_closure': exited['forced_closure'], 'capture_audit': audit_result, 'bindings': binds}
    atomic_json(output, value)
    return value


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for field in ('candidate', 'run-dir', 'raw-audit', 'output', 'manifest-sha256'):
        p.add_argument('--' + field, required=True)
    args = p.parse_args()
    build_supervisor_result(args.candidate, args.run_dir, args.raw_audit, args.output,
                            expected_manifest_sha256=args.manifest_sha256)


if __name__ == '__main__':
    main()
