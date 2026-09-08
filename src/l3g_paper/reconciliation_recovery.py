"""Incident-8631-only, authenticated Sim101 reconciliation recovery."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import secrets
import threading
import time
from typing import Callable, Mapping

from src.lane_iii.contracts import canonical_hash

from .ledger import PaperLedger
from .ninjatrader_reconciliation_probe import (
    probe_flat_sim101_reconciliation,
    validate_flat_sim101_reconciliation_proof,
)
from .ninjatrader_transport import PaperExecutionTransport
from .runtime import LaneIIIPaperRuntime


RECONCILIATION_RECOVERY_SCHEMA = "lane-iii-reconciliation-recovery-v1"
RECONCILIATION_RECOVERY_ACTION_HEADER = "X-Beelzebub-Reconciliation-Recovery-Action"
RECONCILIATION_RECOVERY_ACTION_VALUE = "sim101-incident-8631-recovery-v1"
RECONCILIATION_RECOVERY_TOKEN_HEADER = "X-Beelzebub-Reconciliation-Recovery-Token"
_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$")

INCIDENT = {
    "ledger_sequence": 8631,
    "record_hash": "f7fd21e13621690a8d78f36abc1feb15df8591d06aa601da68802c7bda4a372d",
    "identity": "l3g-safety-8051c04030c94cb9b342c6371b0ba9d4",
    "reason": "FOREIGN_ORDER_ACTIVITY",
}
LOCKOUTS = (
    {
        "ledger_sequence": 8632,
        "record_hash": "8429e493e53325419e190986eeb8add14eef9e22013be5aa981c300b2fcf4f3a",
        "lockout_reason": "NINJATRADER_SAFETY_EVENT:FOREIGN_ORDER_ACTIVITY",
    },
    {
        "ledger_sequence": 8636,
        "record_hash": "bdd238bacb665ce9be62719526fb1e1bd9400f50840fbc15722ec102d387f9f4",
        "lockout_reason": "RECONCILIATION_BLOCKED",
    },
)
_ORDER_RECORDS = (
    {
        "native_order_id": "18f43205557f428aadd20fbc8d54f8bd",
        "native_execution_id": "4a3e10cc72554417a41e513e703c280f",
        "ninjatrader_local_time": "2026-09-07 10:59:18",
        "account": "Sim101", "instrument": "MNQ SEP26", "name": "",
        "action": "Buy", "order_type": "Market", "quantity": 1,
        "terminal_state": "Filled", "filled_quantity": 1, "average_fill_price": "29606.5",
        "trace_lines": [972, 982], "log_lines": [29, 33],
    },
    {
        "native_order_id": "e8996c065ceb47bcb6c7df5d8b3eb03d",
        "native_execution_id": "bc402e6804ca49bfa97c8550cfa765fc",
        "ninjatrader_local_time": "2026-09-07 10:59:55",
        "account": "Sim101", "instrument": "MNQ SEP26", "name": "",
        "action": "Sell", "order_type": "Market", "quantity": 1,
        "terminal_state": "Filled", "filled_quantity": 1, "average_fill_price": "29604.25",
        "trace_lines": [1130, 1140], "log_lines": [36, 40],
    },
    {
        "native_order_id": "de3259dab0d34d1dab120c4a3778f166",
        "native_execution_id": "5e56ddfc4fa04f2789e2f873ce83d4f8",
        "ninjatrader_local_time": "2026-09-07 11:02:51",
        "account": "Sim101", "instrument": "MNQ SEP26", "name": "",
        "action": "Buy", "order_type": "Market", "quantity": 1,
        "terminal_state": "Filled", "filled_quantity": 1, "average_fill_price": "29604.25",
        "trace_lines": [1147, 1157], "log_lines": [43, 47],
    },
    {
        "native_order_id": "ff3a2b0ea07540848e93fb7df77387cb",
        "native_execution_id": "4fe59b2c2dd846fa80faa52ef4296a65",
        "ninjatrader_local_time": "2026-09-07 11:03:15",
        "account": "Sim101", "instrument": "MNQ SEP26", "name": "Close",
        "action": "Sell", "order_type": "Market", "quantity": 1,
        "terminal_state": "Filled", "filled_quantity": 1, "average_fill_price": "29603.25",
        "trace_lines": [1252, 1265], "log_lines": [50, 54],
    },
)
ORDER_EVIDENCE = tuple({**record, "record_fingerprint": canonical_hash(record)} for record in _ORDER_RECORDS)
ORDER_IDENTITIES = tuple(str(record["native_order_id"]) for record in ORDER_EVIDENCE)
TRACE_EVIDENCE = {
    "trace_sha256": "65df75fc3ee4a5f64e5f4dd19f3e82b14eb47b1c2b24ef058ad9836942c9af74",
    "log_sha256": "06a2c0e1065536fb65113c139c16d339ac9d4299094ed7b7c933debc65940488",
    "position_close_trace_line": 1252,
    "technical_origin": "UNPROVEN",
}
OPERATOR = "Joseph"
OPERATOR_STATEMENT = "I was trading on Tradovate."
EXPECTED_LOCKOUT_REASON = "RECONCILIATION_BLOCKED"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _append_jsonl(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(dict(payload), sort_keys=True, separators=(",", ":")) + "\n"
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(descriptor, encoded.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class ReconciliationRecoveryService:
    """Own one process-local recovery lease and durable request replay."""

    def __init__(
        self,
        *,
        runtime_provider: Callable[[], object | None],
        transport_provider: Callable[[], object | None],
        ledger_provider: Callable[[], object | None],
        audit_path: str | Path,
        probe: Callable[..., dict[str, object]] = probe_flat_sim101_reconciliation,
        reconnect_timeout_seconds: float = 30.0,
        poll_seconds: float = 0.1,
    ) -> None:
        if reconnect_timeout_seconds <= 0 or poll_seconds <= 0:
            raise ValueError("Reconciliation recovery timeouts must be positive.")
        self._runtime_provider = runtime_provider
        self._transport_provider = transport_provider
        self._ledger_provider = ledger_provider
        self._audit_path = Path(audit_path).resolve()
        self._probe = probe
        self._reconnect_timeout_seconds = reconnect_timeout_seconds
        self._poll_seconds = poll_seconds
        self._action_token = secrets.token_urlsafe(32)
        self._lock = threading.Lock()

    @property
    def action_token(self) -> str:
        return self._action_token

    def _components(self) -> tuple[LaneIIIPaperRuntime, PaperExecutionTransport, PaperLedger]:
        runtime = self._runtime_provider()
        transport = self._transport_provider()
        ledger = self._ledger_provider()
        if type(runtime) is not LaneIIIPaperRuntime:
            raise RuntimeError("RECONCILIATION_RECOVERY_RUNTIME_UNAVAILABLE")
        if type(transport) is not PaperExecutionTransport:
            raise RuntimeError("RECONCILIATION_RECOVERY_TRANSPORT_UNAVAILABLE")
        if type(ledger) is not PaperLedger or runtime.ledger is not ledger or transport.ledger is not ledger:
            raise RuntimeError("RECONCILIATION_RECOVERY_LEDGER_BINDING_MISMATCH")
        return runtime, transport, ledger

    @staticmethod
    def request_template(authority_state_hash: object) -> dict[str, object]:
        return {
            "request_id": "incident-8631-recovery-<unique>",
            "operator": OPERATOR,
            "incident_sequence": INCIDENT["ledger_sequence"],
            "incident_hash": INCIDENT["record_hash"],
            "lockout_sequences": [value["ledger_sequence"] for value in LOCKOUTS],
            "order_identities": list(ORDER_IDENTITIES),
            "account": "Sim101",
            "environment": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "operator_statement": OPERATOR_STATEMENT,
            "expected_lockout_reason": EXPECTED_LOCKOUT_REASON,
            "authority_state_hash": authority_state_hash,
        }

    def status(self) -> dict[str, object]:
        authority: dict[str, object] = {}
        error: str | None = None
        recovered: dict[str, object] | None = None
        try:
            runtime, _, ledger = self._components()
            authority = runtime.reconciliation_recovery_authority()
            clear_rows = ledger.recent_kind_records(("RISK_EVENT_RECONCILIATION_LOCKOUT_CLEARED",), 1)
            if clear_rows:
                recovered = {
                    "ledger_sequence": clear_rows[0]["ledger_sequence"],
                    "record_hash": clear_rows[0]["record_hash"],
                }
        except RuntimeError as exc:
            error = str(exc)
        return {
            "schema": RECONCILIATION_RECOVERY_SCHEMA,
            "action_token": self._action_token,
            "authenticated_operator": OPERATOR,
            "authority": authority,
            "request_template": self.request_template(authority.get("authority_state_hash")),
            "incident": dict(INCIDENT),
            "lockouts": [dict(value) for value in LOCKOUTS],
            "order_evidence": [dict(value) for value in ORDER_EVIDENCE],
            "trace_evidence": dict(TRACE_EVIDENCE),
            "recovered": recovered,
            "error": error,
            "live_capital": "DENIED",
        }

    @staticmethod
    def _validated_request(body: Mapping[str, object]) -> tuple[dict[str, object], str]:
        expected_keys = {
            "request_id", "operator", "incident_sequence", "incident_hash",
            "lockout_sequences", "order_identities", "account", "environment",
            "instrument", "operator_statement", "expected_lockout_reason",
            "authority_state_hash",
        }
        if not isinstance(body, Mapping) or set(body) != expected_keys:
            raise ValueError("Reconciliation recovery request shape is invalid.")
        request = dict(body)
        request_id = request.get("request_id")
        if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
            raise ValueError("Reconciliation recovery request ID is invalid.")
        exact = {
            "operator": OPERATOR,
            "incident_sequence": INCIDENT["ledger_sequence"],
            "incident_hash": INCIDENT["record_hash"],
            "lockout_sequences": [8632, 8636],
            "order_identities": list(ORDER_IDENTITIES),
            "account": "Sim101",
            "environment": "LOCAL_SIMULATION",
            "instrument": "MNQ SEP26",
            "operator_statement": OPERATOR_STATEMENT,
            "expected_lockout_reason": EXPECTED_LOCKOUT_REASON,
        }
        if any(request.get(key) != value for key, value in exact.items()):
            raise ValueError("Reconciliation recovery request evidence does not match incident 8631.")
        authority_hash = request.get("authority_state_hash")
        if not isinstance(authority_hash, str) or re.fullmatch(r"[0-9a-f]{64}", authority_hash) is None:
            raise ValueError("Reconciliation recovery authority-state hash is invalid.")
        return request, canonical_hash(request)

    @staticmethod
    def _coordinate(record: Mapping[str, object], identity: str) -> dict[str, object]:
        return {
            "identity": identity,
            "ledger_sequence": record["ledger_sequence"],
            "record_hash": record["record_hash"],
        }

    @staticmethod
    def _require_incident_chain(ledger: PaperLedger) -> None:
        for expected in (INCIDENT, *LOCKOUTS):
            record = ledger.record_by_sequence(int(expected["ledger_sequence"]))
            envelope = None if record is None else record.get("record")
            payload = envelope.get("payload") if isinstance(envelope, Mapping) else None
            if record is None or record.get("record_hash") != expected["record_hash"] or not isinstance(payload, Mapping):
                raise RuntimeError("RECONCILIATION_RECOVERY_INCIDENT_COORDINATE_MISMATCH")
            if expected is INCIDENT and (
                envelope.get("kind") != "INCIDENT_SAFETY_EVENT"
                or payload.get("reason_code") != "FOREIGN_ORDER_ACTIVITY"
                or envelope.get("identity") != INCIDENT["identity"]
            ):
                raise RuntimeError("RECONCILIATION_RECOVERY_INCIDENT_EVIDENCE_MISMATCH")
            if expected is not INCIDENT and (
                envelope.get("kind") != "RISK_EVENT_AUTHORITY_LOCKOUT"
                or payload.get("lockout_reason") != expected["lockout_reason"]
                or payload.get("locked_out") is not True
            ):
                raise RuntimeError("RECONCILIATION_RECOVERY_LOCKOUT_EVIDENCE_MISMATCH")
        latest_safety = ledger.recent_kind_records(("INCIDENT_SAFETY_EVENT",), 1)
        latest_lockout = ledger.recent_kind_records(("RISK_EVENT_AUTHORITY_LOCKOUT",), 1)
        if (
            len(latest_safety) != 1
            or latest_safety[0]["ledger_sequence"] != 8631
            or latest_safety[0]["record_hash"] != INCIDENT["record_hash"]
            or len(latest_lockout) != 1
            or latest_lockout[0]["ledger_sequence"] != 8636
            or latest_lockout[0]["record_hash"] != LOCKOUTS[-1]["record_hash"]
        ):
            raise RuntimeError("RECONCILIATION_RECOVERY_NEWER_SAFETY_AUTHORITY")

    def _audit(self, event: str, **details: object) -> None:
        _append_jsonl(self._audit_path, {
            "schema": RECONCILIATION_RECOVERY_SCHEMA,
            "event": event,
            "recorded_at": _utc_now(),
            "authenticated_operator": OPERATOR,
            "live_capital": "DENIED",
            **details,
        })

    @staticmethod
    def _replay(ledger: PaperLedger, request_id: str, request_hash: str) -> dict[str, object] | None:
        clear_identity = "l3g-reconciliation-lockout-clear-" + canonical_hash({"request_id": request_id})
        clear = ledger.record_by_identity(clear_identity)
        if clear is None:
            return None
        envelope = clear.get("record")
        payload = envelope.get("payload") if isinstance(envelope, Mapping) else None
        if not isinstance(payload, Mapping) or payload.get("request_hash") != request_hash:
            raise RuntimeError("RECONCILIATION_RECOVERY_REQUEST_REPLAY_CONFLICT")
        return {
            "schema": RECONCILIATION_RECOVERY_SCHEMA,
            "status": "RECOVERED",
            "request_id": request_id,
            "request_hash": request_hash,
            "idempotent_replay": True,
            "acknowledgement": dict(payload["acknowledgement"]),
            "reconciliation": dict(payload["reconciliation"]),
            "clear": {
                "identity": clear_identity,
                "ledger_sequence": clear["ledger_sequence"],
                "record_hash": clear["record_hash"],
            },
            "cleared_lockout_reason": "RECONCILIATION_BLOCKED",
            "preserved_incident": dict(payload["incident"]),
            "risk_locked_out": False,
            "transport_command_delta": 0,
            "live_capital": "DENIED",
        }

    def recover(self, body: Mapping[str, object]) -> dict[str, object]:
        request, request_hash = self._validated_request(body)
        request_id = str(request["request_id"])
        lease_id = "l3g-reconciliation-recovery-lease-" + canonical_hash({"request_id": request_id})
        with self._lock:
            runtime, transport, ledger = self._components()
            replay = self._replay(ledger, request_id, request_hash)
            if replay is not None:
                return replay
            self._require_incident_chain(ledger)
            lease = runtime.begin_reconciliation_recovery_lease(
                lease_id, str(request["authority_state_hash"]),
            )
            commands_before = int(lease["transport_commands_sent"])
            acknowledgement_identity = "l3g-reconciliation-recovery-ack-" + canonical_hash(
                {"request_id": request_id}
            )
            acknowledgement_payload = {
                "schema": RECONCILIATION_RECOVERY_SCHEMA,
                "request_id": request_id,
                "request_hash": request_hash,
                "authenticated_operator": OPERATOR,
                "operator_statement": OPERATOR_STATEMENT,
                "incident": dict(INCIDENT),
                "lockouts": [dict(value) for value in LOCKOUTS],
                "order_evidence": [dict(value) for value in ORDER_EVIDENCE],
                "trace_evidence": dict(TRACE_EVIDENCE),
                "binding": {
                    "account": "Sim101", "environment": "LOCAL_SIMULATION",
                    "instrument": "MNQ SEP26", "maximum_quantity": 1,
                    "live_capital": "DENIED",
                },
                "expected_active_lockout_reason": EXPECTED_LOCKOUT_REASON,
                "authority_state_hash": request["authority_state_hash"],
                "technical_origin_claimed": False,
                "effect": "OPERATOR_CONTEXT_ONLY_NO_ORDER_AUTHORITY",
            }
            transport_stopped = False
            try:
                ledger.append(
                    "RISK_EVENT_OPERATOR_INCIDENT_ACKNOWLEDGEMENT", acknowledgement_payload,
                    identity=acknowledgement_identity,
                )
                stored_ack = ledger.record_by_identity(acknowledgement_identity)
                if stored_ack is None:
                    raise RuntimeError("RECONCILIATION_RECOVERY_ACKNOWLEDGEMENT_NOT_DURABLE")
                acknowledgement = self._coordinate(stored_ack, acknowledgement_identity)
                self._audit(
                    "ACKNOWLEDGEMENT_DURABLE", request_id=request_id,
                    request_hash=request_hash, acknowledgement=acknowledgement,
                )

                stopped = transport.stop()
                transport_stopped = True
                if stopped.state not in {"STOPPED", "DISABLED"}:
                    raise RuntimeError("RECONCILIATION_RECOVERY_TRANSPORT_STOP_FAILED")
                proof = self._probe(
                    paper_policy_hash=runtime.policy.artifact.configuration_hash,
                    risk_profile_hash=runtime.risk.profile.configuration_hash,
                )
                validate_flat_sim101_reconciliation_proof(
                    proof,
                    expected_policy_hash=runtime.policy.artifact.configuration_hash,
                    expected_risk_hash=runtime.risk.profile.configuration_hash,
                )
                started = transport.start()
                transport_stopped = False
                if started.state == "FAULTED":
                    raise RuntimeError("RECONCILIATION_RECOVERY_TRANSPORT_RESTART_FAILED")
                deadline = time.monotonic() + self._reconnect_timeout_seconds
                while True:
                    authority = runtime.reconciliation_recovery_authority()
                    if (
                        authority.get("runtime_state") == "READY_DISARMED"
                        and authority.get("transport_authenticated") is True
                        and authority.get("transport_reconciled") is True
                        and authority.get("reconciliation_current") is True
                    ):
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError("RECONCILIATION_RECOVERY_ACTIVE_RECONCILIATION_TIMEOUT")
                    time.sleep(self._poll_seconds)
                validate_flat_sim101_reconciliation_proof(
                    proof,
                    expected_policy_hash=runtime.policy.artifact.configuration_hash,
                    expected_risk_hash=runtime.risk.profile.configuration_hash,
                )
                evidence_digest = canonical_hash(proof)
                result = runtime.complete_reconciliation_recovery(
                    lease_id=lease_id,
                    request_id=request_id,
                    request_hash=request_hash,
                    incident=INCIDENT,
                    lockouts=LOCKOUTS,
                    acknowledgement=acknowledgement,
                    proof=proof,
                    evidence_digest=evidence_digest,
                    commands_before=commands_before,
                )
                self._audit(
                    "RECOVERY_COMPLETED", request_id=request_id,
                    request_hash=request_hash, result=result,
                )
                return result
            except Exception as exc:
                if transport_stopped:
                    try:
                        transport.start()
                    except Exception:
                        pass
                runtime.abort_reconciliation_recovery_lease(lease_id)
                try:
                    self._audit(
                        "RECOVERY_BLOCKED", request_id=request_id,
                        request_hash=request_hash, blocker=f"{type(exc).__name__}:{exc}",
                    )
                except OSError:
                    pass
                raise
