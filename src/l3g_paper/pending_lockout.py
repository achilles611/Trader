"""Two-phase recovery of a preserved, unrecorded capacity-lock incident.

This records local runtime evidence, never native signatures or a risk clear.
The immutable original and its hash remain available after pending resolution.
The owner must supply fresh disarmed reconciliation and a verified ledger fence.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Mapping

from src.lane_iii.contracts import canonical_hash, normalized_utc
from .ledger import PaperLedger

PENDING_FAULT = 'RISK_LOCKOUT_EVIDENCE_PERSISTENCE_FAILED'
RECOVERY_SCHEMA = 'lane-iii-pending-lockout-evidence-recovery-v1'
_FIELDS = {
    'schema', 'original_path', 'original_sha256', 'runtime_evidence_path',
    'runtime_evidence_sha256', 'ledger_path', 'ledger_identity', 'ledger_epoch',
}


def _time(value: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, 'Pending incident time').replace('Z', '+00:00'))


def _read_exact(path: object, expected_hash: object) -> bytes:
    if not isinstance(path, str) or not Path(path).is_absolute():
        raise ValueError('PENDING_LOCKOUT_EVIDENCE_PATH_INVALID')
    if not isinstance(expected_hash, str) or re.fullmatch('[0-9a-f]{64}', expected_hash) is None:
        raise ValueError('PENDING_LOCKOUT_EVIDENCE_HASH_INVALID')
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_hash:
        raise ValueError('PENDING_LOCKOUT_EVIDENCE_HASH_MISMATCH')
    return raw


def validate_bundle(ledger: PaperLedger, bundle: Mapping[str, object]) -> dict[str, object]:
    if set(bundle) != _FIELDS or bundle.get('schema') != RECOVERY_SCHEMA:
        raise ValueError('PENDING_LOCKOUT_BUNDLE_INVALID')
    if (Path(str(bundle['ledger_path'])).resolve() != ledger.path.resolve()
            or bundle['ledger_identity'] != ledger.ledger_identity
            or bundle['ledger_epoch'] != ledger.ledger_epoch):
        raise ValueError('PENDING_LOCKOUT_LEDGER_IDENTITY_MISMATCH')
    raw = _read_exact(bundle['original_path'], bundle['original_sha256'])
    original = json.loads(raw)
    if not isinstance(original, dict):
        raise ValueError('PENDING_LOCKOUT_ORIGINAL_INVALID')
    base_fields = {'schema', 'ledger_path', 'kind', 'payload', 'created_at'}
    if original.get('schema') == 'lane-iii-risk-lockout-pending-v1':
        valid_fields = base_fields
    elif original.get('schema') == 'lane-iii-risk-lockout-pending-v2':
        valid_fields = base_fields | {'ledger_identity', 'ledger_epoch', 'incident_id',
            'account', 'account_class', 'instrument', 'execution_session_id'}
        if (original.get('ledger_identity') != ledger.ledger_identity
                or original.get('ledger_epoch') != ledger.ledger_epoch
                or (original.get('account'), original.get('account_class'), original.get('instrument'))
                != ('Sim101', 'LOCAL_SIMULATION', 'MNQ SEP26')):
            raise ValueError('PENDING_LOCKOUT_ORIGINAL_BINDING_MISMATCH')
    else:
        raise ValueError('PENDING_LOCKOUT_SCHEMA_INVALID')
    payload = original.get('payload')
    if (set(original) != valid_fields
            or Path(str(original['ledger_path'])).resolve() != ledger.path.resolve()
            or original['kind'] != 'RISK_EVENT_AUTHORITY_LOCKOUT'
            or not isinstance(payload, dict)
            or set(payload) != {'locked_out', 'lockout_reason', 'lockout_trade_date', 'effective_trade_date', 'effect'}
            or payload.get('locked_out') is not True
            or payload.get('effect') != 'ENTRY_AUTHORITY_LOCKED'
            or payload.get('lockout_trade_date') is not None
            or payload.get('effective_trade_date') is not None
            or not str(payload.get('lockout_reason', '')).startswith('LEDGER_CAPACITY_INADEQUATE:')):
        raise ValueError('PENDING_LOCKOUT_ORIGINAL_SCOPE_INVALID')
    snapshot_raw = _read_exact(bundle['runtime_evidence_path'], bundle['runtime_evidence_sha256'])
    snapshot = json.loads(snapshot_raw)
    paper = snapshot['paper']
    source = paper['risk_continuity']['source_ledger']
    archived_lock = paper['risk_continuity']['authority_lockout']
    if (Path(source['path']).resolve() != ledger.path.resolve()
            or source['ledger_identity'] != ledger.ledger_identity
            or source['ledger_epoch'] != ledger.ledger_epoch
            or archived_lock.get('lockout_reason') != payload['lockout_reason']
            or archived_lock.get('locked_out') is not True
            or paper.get('risk_continuity_fault') != PENDING_FAULT
            or (paper.get('paper_account'), paper.get('account_class'), paper.get('market_instrument'))
            != ('Sim101', 'LOCAL_SIMULATION', 'MNQ SEP26')
            or _time(original['created_at']) > _time(snapshot['captured_at'])):
        raise ValueError('PENDING_LOCKOUT_RUNTIME_ASSOCIATION_INVALID')
    anchor = ledger.record_by_sequence(source['risk_boundary_sequence'])
    if anchor is None or anchor['record_hash'] != source['risk_boundary_hash']:
        raise ValueError('PENDING_LOCKOUT_HISTORICAL_ANCHOR_INVALID')
    loaded = snapshot.get('pong', {}).get('loaded_artifact_hash')
    if not isinstance(loaded, str) or re.fullmatch('[0-9a-f]{64}', loaded) is None:
        raise ValueError('PENDING_LOCKOUT_RUNTIME_IDENTITY_MISSING')
    return {'original': original, 'raw': raw, 'bundle': dict(bundle),
        'payload': payload, 'identity': 'l3g-pending-lockout-evidence-' + str(bundle['original_sha256']),
        'association': {'ledger_identity': ledger.ledger_identity, 'ledger_epoch': ledger.ledger_epoch,
            'original_incident_at': original['created_at'], 'archived_runtime_observed_at': snapshot['captured_at'],
            'archived_runtime_artifact': loaded, 'risk_anchor_sequence': source['risk_boundary_sequence'],
            'risk_anchor_hash': source['risk_boundary_hash'], 'account': 'Sim101',
            'account_class': 'LOCAL_SIMULATION', 'instrument': 'MNQ SEP26'}}


def require_verified_tip(ledger: PaperLedger, proof: Mapping[str, object], *, minimum: int = 1) -> None:
    tip = proof.get('arm_snapshot_tip')
    if (proof.get('ledger_trust_state') != 'VERIFIED_TO_ARM_SNAPSHOT_TIP'
            or proof.get('unverified_tail_rows') != 0 or type(tip) is not int
            or tip < minimum or proof.get('verified_through_sequence') != tip):
        raise ValueError('PENDING_LOCKOUT_FULL_VERIFICATION_REQUIRED')
    record = ledger.record_by_sequence(tip)
    if record is None or record['record_hash'] != proof.get('arm_snapshot_tip_hash'):
        raise ValueError('PENDING_LOCKOUT_VERIFIED_REFERENCE_INVALID')
    current = ledger.commissioning_tail_snapshot(tip, last_full_verified_sequence=tip)
    if current['arm_snapshot_tip'] != tip or current['arm_snapshot_tip_hash'] != record['record_hash']:
        raise ValueError('PENDING_LOCKOUT_VERIFIED_TIP_ADVANCED')


def _paths(ledger: PaperLedger, bundle: Mapping[str, object]) -> tuple[Path, Path]:
    pending = Path(str(ledger.path) + '.risk-authority-pending.json')
    resolved = Path(str(ledger.path) + '.risk-authority-resolved-' + str(bundle['original_sha256']) + '.json')
    if Path(str(bundle['original_path'])).resolve() in {pending.resolve(), resolved.resolve()}:
        raise ValueError('PENDING_LOCKOUT_IMMUTABLE_COPY_REQUIRED')
    return pending, resolved


def _check_sidecars(pending: Path, resolved: Path, raw: bytes) -> None:
    if any(pending.parent.glob(pending.name + '.*')):
        raise ValueError('PENDING_LOCKOUT_ADDITIONAL_INCIDENT_UNRESOLVED')
    if not pending.exists() and not resolved.exists():
        raise ValueError('PENDING_LOCKOUT_ACTIVE_EVIDENCE_MISSING')
    for path in (pending, resolved):
        if path.exists() and path.read_bytes() != raw:
            raise ValueError('PENDING_LOCKOUT_ACTIVE_EVIDENCE_CONFLICT')


def _validate_record(record: Mapping[str, object], evidence: Mapping[str, object]) -> None:
    row = record['record']
    payload = row['payload']
    original_payload = evidence['payload']
    if (row.get('kind') != 'RISK_EVENT_AUTHORITY_LOCKOUT'
            or set(payload) - {'session_family'} != set(original_payload) | {'pending_evidence'}
            or any(payload.get(key) != value for key, value in original_payload.items())
            or payload.get('pending_evidence') != {
                'schema': RECOVERY_SCHEMA, 'bundle': evidence['bundle'],
                'original_document': evidence['original'], 'association': evidence['association'],
                'evidence_semantics': 'LOCAL_RUNTIME_INCIDENT_NOT_NATIVE_SIGNATURE',
                'effect': 'ORIGINAL_LOCKOUT_PRESERVED_NOT_CLEARED'}):
        raise ValueError('PENDING_LOCKOUT_DURABLE_EVIDENCE_CONFLICT')


def record_pending(ledger: PaperLedger, bundle: Mapping[str, object], proof: Mapping[str, object]) -> dict[str, object]:
    """Phase 1: durable incident only; sidecar and all authority remain locked."""
    evidence = validate_bundle(ledger, bundle)
    pending, resolved = _paths(ledger, bundle)
    _check_sidecars(pending, resolved, evidence['raw'])
    require_verified_tip(ledger, proof)
    identity = evidence['identity']
    record = ledger.record_by_identity(identity)
    if record is None:
        if not pending.exists() or resolved.exists():
            raise ValueError('PENDING_LOCKOUT_RESOLUTION_WITHOUT_RECORD')
        recent = ledger.recent_kind_records(('RISK_EVENT_AUTHORITY_LOCKOUT', 'RISK_EVENT_AUTHORITY_LOCKOUT_CLEARED'), 1)
        if recent and _time(recent[0]['record']['occurred_at']) > _time(evidence['original']['created_at']):
            raise ValueError('PENDING_LOCKOUT_NEWER_AUTHORITY_INCIDENT')
        payload = {**evidence['payload'], 'pending_evidence': {
            'schema': RECOVERY_SCHEMA, 'bundle': evidence['bundle'],
            'original_document': evidence['original'], 'association': evidence['association'],
            'evidence_semantics': 'LOCAL_RUNTIME_INCIDENT_NOT_NATIVE_SIGNATURE',
            'effect': 'ORIGINAL_LOCKOUT_PRESERVED_NOT_CLEARED'}}
        ledger.append('RISK_EVENT_AUTHORITY_LOCKOUT', payload, identity=identity)
        record = ledger.record_by_identity(identity)
        if record is None:
            raise RuntimeError('PENDING_LOCKOUT_EVIDENCE_NOT_DURABLE')
    _validate_record(record, evidence)
    return {'phase': 'EVIDENCE_RECORDED_PENDING_FULL_VERIFICATION',
        'ledger_sequence': record['ledger_sequence'], 'record_hash': record['record_hash'],
        'identity': identity, 'lockout_reason': evidence['payload']['lockout_reason'],
        'lockout_cleared': False, 'new_full_verification_required': True}


def resolve_pending(ledger: PaperLedger, bundle: Mapping[str, object], proof: Mapping[str, object]) -> dict[str, object]:
    """Phase 2: resolve the pending fence only after independent verification.

    Atomic rename preserves the original bytes; the separate immutable copy
    and ledger-embedded original remain. A retry recognizes either crash side.
    """
    evidence = validate_bundle(ledger, bundle)
    pending, resolved = _paths(ledger, bundle)
    _check_sidecars(pending, resolved, evidence['raw'])
    record = ledger.record_by_identity(evidence['identity'])
    if record is None:
        raise ValueError('PENDING_LOCKOUT_DURABLE_EVIDENCE_MISSING')
    _validate_record(record, evidence)
    require_verified_tip(ledger, proof, minimum=record['ledger_sequence'])
    already_resolved = resolved.exists() and not pending.exists()
    resolution_identity = evidence['identity'] + '-resolution'
    resolution = ledger.record_by_identity(resolution_identity)
    resolution_payload = {
        'schema': RECOVERY_SCHEMA,
        'effect': 'PENDING_EVIDENCE_RESOLUTION_COMMITTED_LOCKOUT_RETAINED',
        'original_sha256': bundle['original_sha256'],
        'original_incident_at': evidence['original']['created_at'],
        'evidence_identity': evidence['identity'],
        'evidence_sequence': record['ledger_sequence'],
        'evidence_record_hash': record['record_hash'],
        'resolved_path': str(resolved), 'lockout_cleared': False,
    }
    if resolution is None:
        if already_resolved:
            raise ValueError('PENDING_LOCKOUT_RESOLUTION_RECORD_MISSING')
        ledger.append('INCIDENT_PENDING_LOCKOUT_EVIDENCE_RESOLUTION',
                      resolution_payload, identity=resolution_identity)
        resolution = ledger.record_by_identity(resolution_identity)
        if resolution is None:
            raise RuntimeError('PENDING_LOCKOUT_RESOLUTION_NOT_DURABLE')
    stored = resolution['record']
    if (stored.get('kind') != 'INCIDENT_PENDING_LOCKOUT_EVIDENCE_RESOLUTION'
            or {k: v for k, v in stored['payload'].items() if k != 'session_family'} != resolution_payload):
        raise ValueError('PENDING_LOCKOUT_RESOLUTION_RECORD_CONFLICT')
    if pending.exists():
        if resolved.exists():
            raise ValueError('PENDING_LOCKOUT_AMBIGUOUS_SIDECAR_STATE')
        os.rename(pending, resolved)  # Windows refuses an existing target.
    return {'phase': 'PENDING_EVIDENCE_RESOLVED_LOCKOUT_RETAINED',
        'already_resolved': already_resolved, 'resolved_path': str(resolved),
        'ledger_sequence': record['ledger_sequence'], 'record_hash': record['record_hash'],
        'resolution_sequence': resolution['ledger_sequence'], 'resolution_record_hash': resolution['record_hash'],
        'new_full_verification_required': True,
        'lockout_reason': evidence['payload']['lockout_reason'], 'lockout_cleared': False,
        'original_sha256': bundle['original_sha256']}
