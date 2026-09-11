"""Durable Pong-to-native execution bridge contracts.

The bridge does not create a second venue adapter.  It translates admitted
``PongAction`` objects into narrow directives which the existing Lane III
runtime must execute through its authenticated Sim101 transport.  Verified
native callbacks are folded back into ``PongStateMachine`` and checkpointed
before any follow-up directive is exposed.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from enum import StrEnum
import hashlib
import json
import os
from pathlib import Path
import uuid

from src.l3g_paper.daily_bias import DailyBiasResult
from src.l3g_paper.discernment import DiscernmentSnapshot
from src.l3g_paper.pong import (
    PongAction,
    PongActionKind,
    PongDirection,
    PongStateMachine,
    PongTransition,
    deserialize_pong_checkpoint,
    serialize_pong_checkpoint,
)
from src.l3g_paper.pong_quantity import PongQuantityPolicy, PongReconciliationProof


PONG_RUNTIME_BRIDGE_SCHEMA = "beelzebub-pong-native-runtime-bridge-v1"
PONG_RUNTIME_IDENTITY_SCHEMA = "beelzebub-pong-loaded-runtime-identity-v1"


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Pong native event time must include an offset.")
    return parsed.astimezone(timezone.utc)


class PongRuntimeBridgeIntegrityError(RuntimeError):
    """The durable bridge checkpoint cannot be trusted."""


class PongNativeDirectiveKind(StrEnum):
    ENTER_LONG = "ENTER_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    VERIFY_NATIVE_PROTECTION = "VERIFY_NATIVE_PROTECTION"
    CANCEL_PENDING_ENTRY = "CANCEL_PENDING_ENTRY"
    CANCEL_PROTECTIVE_STOP = "CANCEL_PROTECTIVE_STOP"
    EXIT = "EXIT"
    EMERGENCY_FLATTEN = "EMERGENCY_FLATTEN"


class PongNativeDirectiveStatus(StrEnum):
    REQUESTED = "REQUESTED"
    DISPATCHED = "DISPATCHED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class PongLoadedRuntimeIdentity:
    """Authenticated identity of the AddOn that is actually loaded now."""

    execution_session_id: str
    addon_protocol_version: str
    addon_source_fingerprint: str
    addon_build_fingerprint: str
    verified_at: str
    attestation_verified: bool
    schema: str = PONG_RUNTIME_IDENTITY_SCHEMA
    account_name: str = "Sim101"
    account_class: str = "LOCAL_SIMULATION"
    instrument: str = "MNQ SEP26"
    canonical_contract: str = "MNQU6"
    live_authority: str = "DISARMED"

    def __post_init__(self) -> None:
        _utc(self.verified_at)
        digests = (
            self.addon_source_fingerprint, self.addon_build_fingerprint,
        )
        if (
            self.schema != PONG_RUNTIME_IDENTITY_SCHEMA
            or not self.execution_session_id.startswith("l3g-es-")
            or not self.addon_protocol_version
            or type(self.attestation_verified) is not bool
            or not self.attestation_verified
            or any(
                len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
                for value in digests
            )
            or (
                self.account_name, self.account_class, self.instrument,
                self.canonical_contract, self.live_authority,
            ) != (
                "Sim101", "LOCAL_SIMULATION", "MNQ SEP26", "MNQU6", "DISARMED",
            )
        ):
            raise ValueError("Pong loaded runtime identity is invalid.")

    @property
    def identity_hash(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True)
class PongNativeDirective:
    directive_id: str
    sequence: int
    action_id: str
    action_kind: PongActionKind
    kind: PongNativeDirectiveKind
    source_switch_id: str
    source_switch_sequence: int
    expected_direction: PongDirection
    quantity: int
    score_desired_quantity: int
    admitted_total_quantity: int
    leg_index: int
    target_action_id: str | None
    admission_id: str | None
    reason_code: str
    status: PongNativeDirectiveStatus
    filled_quantity: int = 0

    def __post_init__(self) -> None:
        if (
            not self.directive_id.startswith("pong-native-directive-")
            or type(self.sequence) is not int
            or self.sequence <= 0
            or not self.action_id.startswith("pong-action-")
            or type(self.action_kind) is not PongActionKind
            or type(self.kind) is not PongNativeDirectiveKind
            or not self.source_switch_id
            or type(self.source_switch_sequence) is not int
            or self.source_switch_sequence < 0
            or type(self.expected_direction) is not PongDirection
            or type(self.quantity) is not int
            or self.quantity not in {0, 1}
            or type(self.score_desired_quantity) is not int
            or not 0 <= self.score_desired_quantity <= 2
            or type(self.admitted_total_quantity) is not int
            or not 0 <= self.admitted_total_quantity <= 2
            or type(self.leg_index) is not int
            or self.leg_index <= 0
            or not self.reason_code
            or type(self.status) is not PongNativeDirectiveStatus
            or type(self.filled_quantity) is not int
            or not 0 <= self.filled_quantity <= max(1, self.quantity)
        ):
            raise ValueError("Pong native directive is invalid.")
        zero_quantity = self.kind in {
            PongNativeDirectiveKind.CANCEL_PENDING_ENTRY,
            PongNativeDirectiveKind.CANCEL_PROTECTIVE_STOP,
        }
        if self.quantity != (0 if zero_quantity else 1):
            raise ValueError("Pong native directive quantity is invalid.")
        if self.kind in {
            PongNativeDirectiveKind.ENTER_LONG,
            PongNativeDirectiveKind.ENTER_SHORT,
        }:
            if not self.admission_id or self.quantity != 1:
                raise ValueError("Pong native entry requires a sealed admission.")
            if self.score_desired_quantity not in {1, 2}:
                raise ValueError("Pong desired entry quantity must remain visible.")
        if self.kind is PongNativeDirectiveKind.CANCEL_PROTECTIVE_STOP:
            if not self.target_action_id:
                raise ValueError("Pong protective cancellation requires a target action.")
        elif self.target_action_id is not None:
            raise ValueError("Only Pong protective cancellation binds a target action.")


@dataclass(frozen=True)
class PongNativeEventProof:
    receipt_id: str
    observed_at: str
    source_fingerprint: str
    build_fingerprint: str
    execution_session_id: str
    signature_verified: bool
    account_name: str = "Sim101"
    account_class: str = "LOCAL_SIMULATION"
    instrument: str = "MNQ SEP26"
    canonical_contract: str = "MNQU6"
    live_authority: str = "DISARMED"

    def __post_init__(self) -> None:
        _utc(self.observed_at)
        if (
            not self.receipt_id
            or type(self.signature_verified) is not bool
            or not self.source_fingerprint
            or not self.build_fingerprint
            or not self.execution_session_id
        ):
            raise ValueError("Pong native event proof is incomplete.")


@dataclass(frozen=True)
class PongBridgeUpdate:
    transition: PongTransition
    directives: tuple[PongNativeDirective, ...]
    duplicate_receipt: bool = False


_DIRECTIVE_KEYS = frozenset(PongNativeDirective.__dataclass_fields__)
_BRIDGE_KEYS = frozenset({
    "schema", "bridge_id", "state_path", "directive_sequence",
    "runtime_identity", "runtime_identity_hash",
    "machine_checkpoint", "directives", "receipt_ids",
    "last_reconciliation_hash", "safety_blocker", "checkpoint_hash",
})


def _directive_payload(value: PongNativeDirective) -> dict[str, object]:
    payload = asdict(value)
    payload["action_kind"] = value.action_kind.value
    payload["kind"] = value.kind.value
    payload["expected_direction"] = value.expected_direction.value
    payload["status"] = value.status.value
    return payload


def _directive_from_payload(value: object) -> PongNativeDirective:
    if type(value) is not dict or set(value) != _DIRECTIVE_KEYS:
        raise ValueError("Pong native directive fields are invalid.")
    return PongNativeDirective(
        **{
            **value,
            "action_kind": PongActionKind(value["action_kind"]),
            "kind": PongNativeDirectiveKind(value["kind"]),
            "expected_direction": PongDirection(value["expected_direction"]),
            "status": PongNativeDirectiveStatus(value["status"]),
        },
    )


class PongRuntimeBridge:
    """Exact-once adapter boundary around one Pong state machine."""

    def __init__(
        self,
        machine: PongStateMachine,
        path: Path,
        *,
        bridge_id: str,
        runtime_identity: PongLoadedRuntimeIdentity,
        directive_sequence: int = 0,
        directives: tuple[PongNativeDirective, ...] = (),
        receipt_ids: tuple[str, ...] = (),
        last_reconciliation_hash: str | None = None,
        safety_blocker: str | None = None,
    ) -> None:
        self.machine = machine
        self.path = path.resolve()
        self.bridge_id = bridge_id
        if type(runtime_identity) is not PongLoadedRuntimeIdentity:
            raise ValueError("Pong bridge requires a verified loaded runtime identity.")
        self.runtime_identity = runtime_identity
        self.directive_sequence = directive_sequence
        self._directives = {item.directive_id: item for item in directives}
        self._action_directives: dict[str, list[str]] = {}
        for item in directives:
            self._action_directives.setdefault(item.action_id, []).append(
                item.directive_id,
            )
        self._receipt_ids = set(receipt_ids)
        self.last_reconciliation_hash = last_reconciliation_hash
        self.safety_blocker = safety_blocker

    @classmethod
    def create(
        cls,
        machine: PongStateMachine,
        path: str | Path,
        *,
        runtime_identity: PongLoadedRuntimeIdentity,
        bridge_id: str | None = None,
    ) -> "PongRuntimeBridge":
        if type(machine) is not PongStateMachine:
            raise ValueError("Pong runtime bridge requires the exact state machine.")
        if (
            machine.quantity_policy.reconciliation_source_fingerprint
            != runtime_identity.addon_source_fingerprint
            or machine.quantity_policy.reconciliation_build_fingerprint
            != runtime_identity.addon_build_fingerprint
        ):
            raise ValueError("PONG_LOADED_RUNTIME_IDENTITY_NOT_BOUND_TO_RECONCILIATION_POLICY")
        target = Path(path).resolve()
        if target.exists():
            raise FileExistsError("PONG_RUNTIME_BRIDGE_CHECKPOINT_ALREADY_EXISTS")
        identity = bridge_id or "pong-runtime-bridge-" + uuid.uuid4().hex
        if not identity.startswith("pong-runtime-bridge-"):
            raise ValueError("Pong runtime bridge identity is invalid.")
        bridge = cls(
            machine, target, bridge_id=identity,
            runtime_identity=runtime_identity,
        )
        bridge._save()
        return bridge

    @classmethod
    def restore(
        cls,
        path: str | Path,
        *,
        current_snapshot: DiscernmentSnapshot | None,
        current_bias: DailyBiasResult,
        reconciliation: PongReconciliationProof,
        current_runtime_identity: PongLoadedRuntimeIdentity,
        quantity_policy: PongQuantityPolicy,
        at: str,
    ) -> "PongRuntimeBridge":
        target = Path(path).resolve()
        try:
            raw = target.read_bytes()
            payload = json.loads(raw)
            if type(payload) is not dict or set(payload) != _BRIDGE_KEYS:
                raise ValueError("bridge fields")
            expected_hash = _digest({
                key: value for key, value in payload.items()
                if key != "checkpoint_hash"
            })
            if payload["checkpoint_hash"] != expected_hash or raw != _canonical(payload):
                raise ValueError("bridge hash or canonical form")
            if (
                payload["schema"] != PONG_RUNTIME_BRIDGE_SCHEMA
                or Path(payload["state_path"]).resolve() != target
                or not str(payload["bridge_id"]).startswith("pong-runtime-bridge-")
                or type(payload["directive_sequence"]) is not int
                or payload["directive_sequence"] < 0
                or type(payload["directives"]) is not list
                or type(payload["receipt_ids"]) is not list
                or len(payload["receipt_ids"]) != len(set(payload["receipt_ids"]))
            ):
                raise ValueError("bridge identity")
            runtime_identity = PongLoadedRuntimeIdentity(**payload["runtime_identity"])
            if (
                payload["runtime_identity_hash"] != runtime_identity.identity_hash
                or type(current_runtime_identity) is not PongLoadedRuntimeIdentity
                or current_runtime_identity.identity_hash != runtime_identity.identity_hash
                or type(quantity_policy) is not PongQuantityPolicy
                or quantity_policy.reconciliation_source_fingerprint
                != runtime_identity.addon_source_fingerprint
                or quantity_policy.reconciliation_build_fingerprint
                != runtime_identity.addon_build_fingerprint
            ):
                raise ValueError("loaded runtime identity changed")
            checkpoint = deserialize_pong_checkpoint(payload["machine_checkpoint"])
            machine = PongStateMachine.restore_checkpoint(
                checkpoint,
                current_snapshot=current_snapshot,
                current_bias=current_bias,
                reconciliation=reconciliation,
                at=at,
                quantity_policy=quantity_policy,
            )
            directives = tuple(
                _directive_from_payload(item) for item in payload["directives"]
            )
            bridge = cls(
                machine,
                target,
                bridge_id=payload["bridge_id"],
                runtime_identity=runtime_identity,
                directive_sequence=payload["directive_sequence"],
                directives=directives,
                receipt_ids=tuple(payload["receipt_ids"]),
                last_reconciliation_hash=payload["last_reconciliation_hash"],
                safety_blocker=payload["safety_blocker"],
            )
            if any(
                item.status in {
                    PongNativeDirectiveStatus.DISPATCHED,
                    PongNativeDirectiveStatus.ACKNOWLEDGED,
                }
                for item in directives
            ):
                bridge.safety_blocker = "PONG_NATIVE_RESTART_RECONCILIATION_REQUIRED"
            bridge._save()
            return bridge
        except (
            OSError, KeyError, TypeError, ValueError, json.JSONDecodeError,
        ) as error:
            raise PongRuntimeBridgeIntegrityError(
                "PONG_RUNTIME_BRIDGE_CHECKPOINT_INTEGRITY_FAILED",
            ) from error

    def _payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema": PONG_RUNTIME_BRIDGE_SCHEMA,
            "bridge_id": self.bridge_id,
            "state_path": str(self.path),
            "directive_sequence": self.directive_sequence,
            "runtime_identity": asdict(self.runtime_identity),
            "runtime_identity_hash": self.runtime_identity.identity_hash,
            "machine_checkpoint": serialize_pong_checkpoint(
                self.machine.checkpoint(),
            ),
            "directives": [
                _directive_payload(self._directives[key])
                for key in sorted(
                    self._directives,
                    key=lambda item: self._directives[item].sequence,
                )
            ],
            "receipt_ids": sorted(self._receipt_ids),
            "last_reconciliation_hash": self.last_reconciliation_hash,
            "safety_blocker": self.safety_blocker,
        }
        payload["checkpoint_hash"] = _digest(payload)
        return payload

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        raw = _canonical(self._payload())
        temporary = self.path.with_name(
            f".{self.path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp",
        )
        try:
            with temporary.open("xb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            if temporary.exists():
                temporary.unlink()

    def _proof_reason(self, proof: PongNativeEventProof) -> str | None:
        if type(proof) is not PongNativeEventProof:
            return "PONG_NATIVE_EVENT_PROOF_INVALID"
        if not proof.signature_verified:
            return "PONG_NATIVE_EVENT_SIGNATURE_INVALID"
        if (
            proof.account_name,
            proof.account_class,
            proof.instrument,
            proof.canonical_contract,
            proof.live_authority,
            proof.source_fingerprint,
            proof.build_fingerprint,
            proof.execution_session_id,
        ) != (
            self.runtime_identity.account_name,
            self.runtime_identity.account_class,
            self.runtime_identity.instrument,
            self.runtime_identity.canonical_contract,
            self.runtime_identity.live_authority,
            self.runtime_identity.addon_source_fingerprint,
            self.runtime_identity.addon_build_fingerprint,
            self.runtime_identity.execution_session_id,
        ):
            return "PONG_NATIVE_EVENT_IDENTITY_MISMATCH"
        return None

    def _claim(self, proof: PongNativeEventProof) -> bool:
        reason = self._proof_reason(proof)
        if reason is not None:
            self.safety_blocker = reason
            transition = self.machine.on_safety_exit(reason=reason)
            self._queue_transition(transition)
            self._save()
            raise ValueError(reason)
        if proof.receipt_id in self._receipt_ids:
            return False
        self._receipt_ids.add(proof.receipt_id)
        return True

    def _informational_transition(self, reason: str) -> PongTransition:
        return PongTransition(self.machine.state, (), reason, False)

    @staticmethod
    def _directive_kind(action: PongAction) -> PongNativeDirectiveKind:
        return {
            PongActionKind.REQUEST_ENTRY_LONG: PongNativeDirectiveKind.ENTER_LONG,
            PongActionKind.REQUEST_ENTRY_SHORT: PongNativeDirectiveKind.ENTER_SHORT,
            PongActionKind.REQUEST_PROTECTIVE_STOP: PongNativeDirectiveKind.VERIFY_NATIVE_PROTECTION,
            PongActionKind.REQUEST_EXIT: PongNativeDirectiveKind.EXIT,
            PongActionKind.REQUEST_SAFETY_EXIT: PongNativeDirectiveKind.EMERGENCY_FLATTEN,
            PongActionKind.CANCEL_PENDING_ENTRY: PongNativeDirectiveKind.CANCEL_PENDING_ENTRY,
            PongActionKind.CANCEL_PROTECTIVE_STOP: PongNativeDirectiveKind.CANCEL_PROTECTIVE_STOP,
        }[action.kind]

    def _queue_action(self, action: PongAction) -> tuple[PongNativeDirective, ...]:
        existing_ids = self._action_directives.get(action.action_id, [])
        if existing_ids:
            return tuple(self._directives[item] for item in existing_ids)
        kind = self._directive_kind(action)
        if kind in {
            PongNativeDirectiveKind.ENTER_LONG,
            PongNativeDirectiveKind.ENTER_SHORT,
        } and action.admission_id is None:
            raise ValueError("PONG_NATIVE_ENTRY_NOT_ADMITTED")
        # Desired two-MNQ sizing remains immutable evidence, but the currently
        # loaded native capability is a one-MNQ fence.  Never create a second
        # entry leg here even if an upstream action still displays two.
        legs = (
            1 if kind in {
                PongNativeDirectiveKind.ENTER_LONG,
                PongNativeDirectiveKind.ENTER_SHORT,
            } else max(1, action.requested_total_quantity)
        )
        if kind in {
            PongNativeDirectiveKind.CANCEL_PENDING_ENTRY,
            PongNativeDirectiveKind.CANCEL_PROTECTIVE_STOP,
        }:
            legs = 1
        created: list[PongNativeDirective] = []
        for leg_index in range(1, legs + 1):
            self.directive_sequence += 1
            identity = {
                "bridge_id": self.bridge_id,
                "sequence": self.directive_sequence,
                "action_id": action.action_id,
                "leg_index": leg_index,
                "kind": kind.value,
            }
            directive = PongNativeDirective(
                directive_id="pong-native-directive-" + _digest(identity)[:32],
                sequence=self.directive_sequence,
                action_id=action.action_id,
                action_kind=action.kind,
                kind=kind,
                source_switch_id=action.source_switch_id,
                source_switch_sequence=action.source_switch_sequence,
                expected_direction=action.expected_direction,
                quantity=(
                    0 if kind in {
                        PongNativeDirectiveKind.CANCEL_PENDING_ENTRY,
                        PongNativeDirectiveKind.CANCEL_PROTECTIVE_STOP,
                    } else 1
                ),
                score_desired_quantity=action.score_authorized_quantity,
                admitted_total_quantity=action.requested_total_quantity,
                leg_index=leg_index,
                target_action_id=action.target_action_id,
                admission_id=action.admission_id,
                reason_code=action.reason_code,
                status=PongNativeDirectiveStatus.REQUESTED,
            )
            self._directives[directive.directive_id] = directive
            self._action_directives.setdefault(action.action_id, []).append(
                directive.directive_id,
            )
            created.append(directive)
        return tuple(created)

    def _queue_transition(
        self, transition: PongTransition,
    ) -> tuple[PongNativeDirective, ...]:
        if type(transition) is not PongTransition:
            raise ValueError("Pong runtime bridge requires an exact transition.")
        directives: list[PongNativeDirective] = []
        for action in transition.actions:
            directives.extend(self._queue_action(action))
        return tuple(directives)

    def apply_transition(self, transition: PongTransition) -> PongBridgeUpdate:
        directives = self._queue_transition(transition)
        self._save()
        return PongBridgeUpdate(transition, directives)

    def mark_dispatched(self, directive_id: str) -> PongNativeDirective:
        directive = self._require_directive(directive_id)
        if directive.status is PongNativeDirectiveStatus.REQUESTED:
            directive = replace(
                directive, status=PongNativeDirectiveStatus.DISPATCHED,
            )
            self._directives[directive_id] = directive
            self._save()
        return directive

    def on_command_ack(
        self, directive_id: str, proof: PongNativeEventProof,
    ) -> PongBridgeUpdate:
        directive = self._require_directive(directive_id)
        if not self._claim(proof):
            return PongBridgeUpdate(
                self._informational_transition("DUPLICATE_NATIVE_RECEIPT_IGNORED"),
                (),
                True,
            )
        if directive.status not in {
            PongNativeDirectiveStatus.DISPATCHED,
            PongNativeDirectiveStatus.ACKNOWLEDGED,
        }:
            return self._native_fault("PONG_NATIVE_ACK_STATE_INVALID")
        self._directives[directive_id] = replace(
            directive, status=PongNativeDirectiveStatus.ACKNOWLEDGED,
        )
        transition = self._informational_transition("PONG_NATIVE_COMMAND_ACKNOWLEDGED")
        self._save()
        return PongBridgeUpdate(transition, ())

    def on_command_rejected(
        self,
        directive_id: str,
        proof: PongNativeEventProof,
        *,
        reason: str,
    ) -> PongBridgeUpdate:
        directive = self._require_directive(directive_id)
        if not self._claim(proof):
            return PongBridgeUpdate(
                self._informational_transition("DUPLICATE_NATIVE_RECEIPT_IGNORED"),
                (), True,
            )
        self._directives[directive_id] = replace(
            directive, status=PongNativeDirectiveStatus.REJECTED,
        )
        blocker = "PONG_NATIVE_COMMAND_REJECTED:" + (reason or "UNKNOWN")
        self.safety_blocker = blocker
        if directive.kind in {
            PongNativeDirectiveKind.ENTER_LONG,
            PongNativeDirectiveKind.ENTER_SHORT,
        }:
            transition = self.machine.on_entry_rejected(action_id=directive.action_id)
        else:
            transition = self.machine.on_safety_exit(reason=blocker)
        follow_up = self._queue_transition(transition)
        self._save()
        return PongBridgeUpdate(transition, follow_up)

    def on_entry_fill(
        self,
        directive_id: str,
        proof: PongNativeEventProof,
        *,
        quantity: int,
        fill_id: str,
    ) -> PongBridgeUpdate:
        directive = self._require_directive(directive_id)
        if directive.kind not in {
            PongNativeDirectiveKind.ENTER_LONG,
            PongNativeDirectiveKind.ENTER_SHORT,
        }:
            return self._native_fault("PONG_NATIVE_ENTRY_FILL_ROLE_INVALID")
        if not self._claim(proof):
            return PongBridgeUpdate(
                self._informational_transition("DUPLICATE_NATIVE_RECEIPT_IGNORED"),
                (), True,
            )
        transition = self.machine.on_entry_fill(
            action_id=directive.action_id,
            direction=directive.expected_direction,
            quantity=quantity,
            fill_id=fill_id,
        )
        self._directives[directive_id] = replace(
            directive,
            status=PongNativeDirectiveStatus.COMPLETED,
            filled_quantity=min(1, directive.filled_quantity + quantity),
        )
        follow_up = self._queue_transition(transition)
        self._save()
        return PongBridgeUpdate(transition, follow_up)

    def on_protection_working(
        self,
        directive_id: str,
        proof: PongNativeEventProof,
        *,
        quantity: int,
    ) -> PongBridgeUpdate:
        directive = self._require_kind(
            directive_id, PongNativeDirectiveKind.VERIFY_NATIVE_PROTECTION,
        )
        if not self._claim(proof):
            return PongBridgeUpdate(
                self._informational_transition("DUPLICATE_NATIVE_RECEIPT_IGNORED"),
                (), True,
            )
        transition = self.machine.on_protection_update(
            action_id=directive.action_id, quantity=quantity, signed=True,
        )
        self._directives[directive_id] = replace(
            directive, status=PongNativeDirectiveStatus.COMPLETED,
        )
        follow_up = self._queue_transition(transition)
        self._save()
        return PongBridgeUpdate(transition, follow_up)

    def on_protective_cancelled(
        self, directive_id: str, proof: PongNativeEventProof,
    ) -> PongBridgeUpdate:
        directive = self._require_kind(
            directive_id, PongNativeDirectiveKind.CANCEL_PROTECTIVE_STOP,
        )
        if not self._claim(proof):
            return PongBridgeUpdate(
                self._informational_transition("DUPLICATE_NATIVE_RECEIPT_IGNORED"),
                (), True,
            )
        transition = self.machine.on_protective_stop_cancelled(
            action_id=directive.target_action_id or "",
        )
        self._directives[directive_id] = replace(
            directive, status=PongNativeDirectiveStatus.COMPLETED,
        )
        follow_up = self._queue_transition(transition)
        self._save()
        return PongBridgeUpdate(transition, follow_up)

    def on_reducing_fill(
        self,
        directive_id: str,
        proof: PongNativeEventProof,
        *,
        quantity: int,
        fill_id: str,
    ) -> PongBridgeUpdate:
        directive = self._require_directive(directive_id)
        if directive.kind not in {
            PongNativeDirectiveKind.EXIT,
            PongNativeDirectiveKind.EMERGENCY_FLATTEN,
            PongNativeDirectiveKind.VERIFY_NATIVE_PROTECTION,
        }:
            return self._native_fault("PONG_NATIVE_REDUCING_FILL_ROLE_INVALID")
        if not self._claim(proof):
            return PongBridgeUpdate(
                self._informational_transition("DUPLICATE_NATIVE_RECEIPT_IGNORED"),
                (), True,
            )
        if directive.kind is PongNativeDirectiveKind.EXIT:
            transition = self.machine.on_exit_fill(
                action_id=directive.action_id, quantity=quantity, fill_id=fill_id,
            )
        elif directive.kind is PongNativeDirectiveKind.EMERGENCY_FLATTEN:
            transition = self.machine.on_safety_exit_fill(
                action_id=directive.action_id, quantity=quantity, fill_id=fill_id,
            )
        else:
            transition = self.machine.on_protective_stop_fill(
                action_id=directive.action_id, quantity=quantity, fill_id=fill_id,
            )
        self._directives[directive_id] = replace(
            directive,
            status=PongNativeDirectiveStatus.COMPLETED,
            filled_quantity=min(1, directive.filled_quantity + quantity),
        )
        follow_up = self._queue_transition(transition)
        self._save()
        return PongBridgeUpdate(transition, follow_up)

    def on_signed_flat_reconciliation(
        self,
        reconciliation: PongReconciliationProof,
        proof: PongNativeEventProof,
        *,
        at: str,
    ) -> PongBridgeUpdate:
        if not self._claim(proof):
            return PongBridgeUpdate(
                self._informational_transition("DUPLICATE_NATIVE_RECEIPT_IGNORED"),
                (), True,
            )
        if (
            type(reconciliation) is not PongReconciliationProof
            or reconciliation.validation_reason(
                at=at, policy=self.machine.quantity_policy,
            ) is not None
        ):
            return self._native_fault("PONG_SIGNED_RECONCILIATION_INVALID")
        self.last_reconciliation_hash = reconciliation.result_hash
        transition = self.machine.on_signed_flat_reconciliation(
            reconciliation=reconciliation, at=at,
        )
        follow_up = self._queue_transition(transition)
        self._save()
        return PongBridgeUpdate(transition, follow_up)

    def _native_fault(self, reason: str) -> PongBridgeUpdate:
        self.safety_blocker = reason
        transition = self.machine.on_safety_exit(reason=reason)
        follow_up = self._queue_transition(transition)
        self._save()
        return PongBridgeUpdate(transition, follow_up)

    def _require_directive(self, directive_id: str) -> PongNativeDirective:
        try:
            return self._directives[directive_id]
        except KeyError as error:
            raise ValueError("PONG_NATIVE_DIRECTIVE_UNKNOWN") from error

    def _require_kind(
        self, directive_id: str, kind: PongNativeDirectiveKind,
    ) -> PongNativeDirective:
        directive = self._require_directive(directive_id)
        if directive.kind is not kind:
            raise ValueError("PONG_NATIVE_DIRECTIVE_KIND_MISMATCH")
        return directive

    def undispatched_directives(self) -> tuple[PongNativeDirective, ...]:
        """Only never-sent directives are safe to expose after restart."""
        return tuple(
            sorted(
                (
                    item for item in self._directives.values()
                    if item.status is PongNativeDirectiveStatus.REQUESTED
                ),
                key=lambda item: item.sequence,
            ),
        )

    def status(self) -> dict[str, object]:
        unresolved = [
            item for item in self._directives.values()
            if item.status not in {
                PongNativeDirectiveStatus.COMPLETED,
                PongNativeDirectiveStatus.REJECTED,
            }
        ]
        return {
            "schema": PONG_RUNTIME_BRIDGE_SCHEMA,
            "bridge_id": self.bridge_id,
            "runtime_identity_hash": self.runtime_identity.identity_hash,
            "state_path": str(self.path),
            "machine_state": self.machine.state.value,
            "machine_checkpoint_hash": self.machine.checkpoint().checkpoint_hash,
            "directive_sequence": self.directive_sequence,
            "unresolved_directive_ids": [
                item.directive_id for item in sorted(
                    unresolved, key=lambda value: value.sequence,
                )
            ],
            "undispatched_directive_ids": [
                item.directive_id for item in self.undispatched_directives()
            ],
            "last_reconciliation_hash": self.last_reconciliation_hash,
            "safety_blocker": self.safety_blocker,
            "entry_authority": "DENIED" if self.safety_blocker else "EXTERNAL_GATES_REQUIRED",
            "risk_reduction_authority": "RETAINED",
            "native_execution_cap": 1,
            "live_authority": "DISARMED",
        }
