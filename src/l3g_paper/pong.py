"""Pure, execution-disabled Pong state machine driven by Discernment switches.

Pong receives only causal, distinct logical transitions from the independent
Discernment engine. It has no timer, runtime registration, account binding,
transport, sender, or persistence side effect. All actions are abstract review
artifacts and cannot place an order.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
import hashlib
import json
import math

from src.l3g_paper.daily_bias import DailyBiasResult, default_unconfigured_bias
from src.l3g_paper.discernment import (
    DISCERNMENT_CONTRACT,
    DISCERNMENT_INSTRUMENT,
    DISCERNMENT_PRODUCTION_CONFIGURED,
    EXPERIMENTAL_DIRECTIONAL_SUPPORT,
    PONG_RESEARCH_POLICY,
    STRONG_THRESHOLD_UNREACHABLE,
    DiscernmentDirection,
    DiscernmentPolicy,
    DiscernmentSnapshot,
    DiscernmentSwitch,
)
from src.l3g_paper.pong_quantity import (
    PONG_LIVE_AUTHORITY,
    PONG_MODE,
    PONG_QUANTITY_POLICY,
    PongQuantityPolicy,
    PongQuantityDirection,
    PongReconciliationProof,
    assess_quantity_admission,
    score_authorized_quantity,
)


PONG_EXECUTION_ENABLED = False
PONG_PROFILE_REGISTRATION = "NOT_REGISTERED"
PONG_PROFILE_VERSION = "BEELZEBUB_PONG_DISCERNMENT_V1"
PONG_SIGNAL_SOURCE = "CAUSAL_DISCERNMENT_THRESHOLD_TRANSITIONS"
PONG_INSTRUMENT = DISCERNMENT_INSTRUMENT
PONG_CONTRACT = DISCERNMENT_CONTRACT
PONG_CHECKPOINT_SCHEMA = "beelzebub-pong-discernment-checkpoint-v1"


class PongDirection(StrEnum):
    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"


class PongState(StrEnum):
    FLAT_WAIT_ALIGNMENT = "FLAT_WAIT_ALIGNMENT"
    ENTRY_PENDING_LONG = "ENTRY_PENDING_LONG"
    ENTRY_PENDING_SHORT = "ENTRY_PENDING_SHORT"
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT_PENDING = "EXIT_PENDING"
    FLAT_WAIT_BEAT = "FLAT_WAIT_BEAT"
    SAFETY_BLOCKED = "SAFETY_BLOCKED"


class PongActionKind(StrEnum):
    REQUEST_ENTRY_LONG = "REQUEST_ENTRY_LONG"
    REQUEST_ENTRY_SHORT = "REQUEST_ENTRY_SHORT"
    REQUEST_PROTECTIVE_STOP = "REQUEST_PROTECTIVE_STOP"
    REQUEST_EXIT = "REQUEST_EXIT"
    REQUEST_SAFETY_EXIT = "REQUEST_SAFETY_EXIT"
    CANCEL_PENDING_ENTRY = "CANCEL_PENDING_ENTRY"
    CANCEL_PROTECTIVE_STOP = "CANCEL_PROTECTIVE_STOP"


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Pong timestamps must include a UTC offset.")
    return parsed


def _pong_direction(value: DiscernmentDirection) -> PongDirection:
    if type(value) is not DiscernmentDirection:
        raise ValueError("Pong requires a Discernment LONG or SHORT direction.")
    return PongDirection(value.value)


def _pong_bias_eligibility(
    bias: DailyBiasResult,
    *,
    at: str,
    direction: PongDirection,
) -> str:
    """Keep the shared daily-bias contract stable while naming Pong's source."""
    reason = bias.eligibility(at=at, direction=direction)
    if reason == "PERPETUAL_SWITCH_NOT_ALIGNED_WITH_DAILY_BIAS":
        return "DISCERNMENT_SWITCH_NOT_ALIGNED_WITH_DAILY_BIAS"
    return reason


@dataclass(frozen=True)
class PongAction:
    action_id: str
    kind: PongActionKind
    source_switch_id: str
    source_switch_sequence: int
    expected_direction: PongDirection
    reason_code: str
    requested_total_quantity: int
    score_authorized_quantity: int
    position_side_support: float | None
    quantity_policy_hash: str
    discernment_policy_hash: str
    discernment_model_hash: str
    snapshot_hash: str | None
    bias_result_hash: str | None
    reconciliation_hash: str | None
    blockers: tuple[str, ...] = ()
    admission_id: str | None = None
    target_action_id: str | None = None

    def __post_init__(self) -> None:
        if (
            type(self.kind) is not PongActionKind
            or type(self.expected_direction) is not PongDirection
            or not isinstance(self.source_switch_id, str)
            or not self.source_switch_id
            or type(self.source_switch_sequence) is not int
            or self.source_switch_sequence < 0
            or not isinstance(self.reason_code, str)
            or not self.reason_code
            or type(self.requested_total_quantity) is not int
            or self.requested_total_quantity < 0
            or type(self.score_authorized_quantity) is not int
            or not 0 <= self.score_authorized_quantity <= 2
            or type(self.blockers) is not tuple
            or any(not isinstance(reason, str) or not reason for reason in self.blockers)
        ):
            raise ValueError("Pong action contract is invalid.")
        if self.position_side_support is not None and (
            isinstance(self.position_side_support, bool)
            or not isinstance(self.position_side_support, (int, float))
            or not math.isfinite(float(self.position_side_support))
            or not 0 <= float(self.position_side_support) <= 1
        ):
            raise ValueError("Pong action position-side support is invalid.")
        for digest in (
            self.quantity_policy_hash,
            self.discernment_policy_hash,
            self.discernment_model_hash,
        ):
            if not isinstance(digest, str) or len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError("Pong action policy identity is invalid.")
        for digest in (
            self.snapshot_hash,
            self.bias_result_hash,
            self.reconciliation_hash,
        ):
            if digest is not None and (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError("Pong action proof identity is invalid.")
        if self.admission_id is not None and not self.admission_id.startswith(
            "pong-admission-",
        ):
            raise ValueError("Pong action admission identity is invalid.")
        if self.target_action_id is not None and not self.target_action_id.startswith(
            "pong-action-",
        ):
            raise ValueError("Pong target action identity is invalid.")
        if self.kind in {
            PongActionKind.REQUEST_ENTRY_LONG,
            PongActionKind.REQUEST_ENTRY_SHORT,
        } and self.requested_total_quantity not in {1, 2}:
            raise ValueError("Pong entry action quantity must be one or two MNQ.")
        if (
            self.kind is PongActionKind.CANCEL_PENDING_ENTRY
            and self.requested_total_quantity != 0
        ):
            raise ValueError("Pong cancellation action cannot request exposure.")
        if self.kind is PongActionKind.CANCEL_PROTECTIVE_STOP:
            if self.requested_total_quantity != 0 or self.target_action_id is None:
                raise ValueError("Protective cancellation must bind one target without exposure.")
        elif self.target_action_id is not None:
            raise ValueError("Only protective cancellation may bind a target action.")
        if self.kind in {
            PongActionKind.REQUEST_PROTECTIVE_STOP,
            PongActionKind.REQUEST_EXIT,
            PongActionKind.REQUEST_SAFETY_EXIT,
        } and self.requested_total_quantity <= 0:
            raise ValueError("Pong risk-reducing action quantity must be positive.")
        if self.kind in {
            PongActionKind.REQUEST_PROTECTIVE_STOP,
            PongActionKind.REQUEST_EXIT,
            PongActionKind.REQUEST_SAFETY_EXIT,
        } and self.expected_direction not in {
            PongDirection.LONG,
            PongDirection.SHORT,
        }:
            raise ValueError("Pong risk-reducing action must bind the position side.")
        if self.action_id != "pong-action-" + hashlib.sha256(
            json.dumps(
                _action_identity_payload(self),
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        ).hexdigest()[:32]:
            raise ValueError("Pong action identity mismatch.")


@dataclass(frozen=True)
class PongTransition:
    state: PongState
    actions: tuple[PongAction, ...]
    reason_code: str
    switch_consumed: bool


@dataclass(frozen=True)
class PongCheckpoint:
    """Immutable logical continuity record; it carries no execution authority."""

    schema: str
    profile: str
    mode: str
    account_name: str
    account_class: str
    instrument: str
    canonical_contract: str
    live_authority: str
    signal_source: str
    quantity_policy_hash: str
    discernment_policy_hash: str
    discernment_model_hash: str
    state: PongState
    shadow_direction: PongDirection | None
    actual_position: PongDirection
    actual_quantity: int
    protected_quantity: int
    last_switch_id: str | None
    last_switch_sequence: int
    last_consumed_switch_id: str | None
    last_snapshot_hash: str | None
    last_snapshot_continuity_domain: str | None
    last_snapshot_revision: int | None
    pending_action: PongAction | None
    reducing_actions: tuple[tuple[PongAction, int], ...]
    protective_actions: tuple[tuple[PongAction, int], ...]
    retired_reducing_actions: tuple[PongAction, ...]
    deferred_exit_action: PongAction | None
    pending_protective_cancel_ids: tuple[str, ...]
    confirmed_flat_switch_sequence: int | None
    waiting_leg_completed: bool
    entry_admitted: bool
    entry_cancel_requested: bool
    entry_target_quantity: int
    entry_filled_quantity: int
    exit_target_quantity: int
    exit_filled_quantity: int
    daily_bias_result_hash: str
    safety_reason: str | None
    recovery_blockers: tuple[str, ...]
    score_authorized_quantity: int
    risk_admitted_quantity: int
    quantity_blockers: tuple[str, ...]
    entry_fill_ids: tuple[str, ...]
    exit_fill_ids: tuple[str, ...]
    safety_fill_ids: tuple[str, ...]
    checkpoint_hash: str


def _action_identity_payload(action: PongAction) -> dict[str, object]:
    return {
        "profile": PONG_PROFILE_VERSION,
        "signal_source": PONG_SIGNAL_SOURCE,
        "kind": action.kind.value,
        "switch": action.source_switch_id,
        "sequence": action.source_switch_sequence,
        "expected": action.expected_direction.value,
        "reason": action.reason_code,
        "requested_total_quantity": action.requested_total_quantity,
        "score_authorized_quantity": action.score_authorized_quantity,
        "position_side_support": action.position_side_support,
        "quantity_policy_hash": action.quantity_policy_hash,
        "discernment_policy_hash": action.discernment_policy_hash,
        "discernment_model_hash": action.discernment_model_hash,
        "snapshot_hash": action.snapshot_hash,
        "bias_result_hash": action.bias_result_hash,
        "reconciliation_hash": action.reconciliation_hash,
        "blockers": list(action.blockers),
        "admission_id": action.admission_id,
        "target_action_id": action.target_action_id,
    }


def _action_payload(action: PongAction | None) -> dict[str, object] | None:
    if action is None:
        return None
    return {
        "action_id": action.action_id,
        "kind": action.kind.value,
        "source_switch_id": action.source_switch_id,
        "source_switch_sequence": action.source_switch_sequence,
        "expected_direction": action.expected_direction.value,
        "reason_code": action.reason_code,
        "requested_total_quantity": action.requested_total_quantity,
        "score_authorized_quantity": action.score_authorized_quantity,
        "position_side_support": action.position_side_support,
        "quantity_policy_hash": action.quantity_policy_hash,
        "discernment_policy_hash": action.discernment_policy_hash,
        "discernment_model_hash": action.discernment_model_hash,
        "snapshot_hash": action.snapshot_hash,
        "bias_result_hash": action.bias_result_hash,
        "reconciliation_hash": action.reconciliation_hash,
        "blockers": list(action.blockers),
        "admission_id": action.admission_id,
        "target_action_id": action.target_action_id,
    }


def _checkpoint_payload(checkpoint: PongCheckpoint) -> dict[str, object]:
    return {
        "schema": checkpoint.schema,
        "profile": checkpoint.profile,
        "mode": checkpoint.mode,
        "account_name": checkpoint.account_name,
        "account_class": checkpoint.account_class,
        "instrument": checkpoint.instrument,
        "canonical_contract": checkpoint.canonical_contract,
        "live_authority": checkpoint.live_authority,
        "signal_source": checkpoint.signal_source,
        "quantity_policy_hash": checkpoint.quantity_policy_hash,
        "discernment_policy_hash": checkpoint.discernment_policy_hash,
        "discernment_model_hash": checkpoint.discernment_model_hash,
        "state": checkpoint.state.value,
        "shadow_direction": (
            None if checkpoint.shadow_direction is None
            else checkpoint.shadow_direction.value
        ),
        "actual_position": checkpoint.actual_position.value,
        "actual_quantity": checkpoint.actual_quantity,
        "protected_quantity": checkpoint.protected_quantity,
        "last_switch_id": checkpoint.last_switch_id,
        "last_switch_sequence": checkpoint.last_switch_sequence,
        "last_consumed_switch_id": checkpoint.last_consumed_switch_id,
        "last_snapshot_hash": checkpoint.last_snapshot_hash,
        "last_snapshot_continuity_domain": checkpoint.last_snapshot_continuity_domain,
        "last_snapshot_revision": checkpoint.last_snapshot_revision,
        "pending_action": _action_payload(checkpoint.pending_action),
        "reducing_actions": [
            {"action": _action_payload(action), "remaining_quantity": remaining}
            for action, remaining in checkpoint.reducing_actions
        ],
        "protective_actions": [
            {"action": _action_payload(action), "remaining_quantity": remaining}
            for action, remaining in checkpoint.protective_actions
        ],
        "retired_reducing_actions": [
            _action_payload(action) for action in checkpoint.retired_reducing_actions
        ],
        "deferred_exit_action": _action_payload(checkpoint.deferred_exit_action),
        "pending_protective_cancel_ids": list(
            checkpoint.pending_protective_cancel_ids
        ),
        "confirmed_flat_switch_sequence": checkpoint.confirmed_flat_switch_sequence,
        "waiting_leg_completed": checkpoint.waiting_leg_completed,
        "entry_admitted": checkpoint.entry_admitted,
        "entry_cancel_requested": checkpoint.entry_cancel_requested,
        "entry_target_quantity": checkpoint.entry_target_quantity,
        "entry_filled_quantity": checkpoint.entry_filled_quantity,
        "exit_target_quantity": checkpoint.exit_target_quantity,
        "exit_filled_quantity": checkpoint.exit_filled_quantity,
        "daily_bias_result_hash": checkpoint.daily_bias_result_hash,
        "safety_reason": checkpoint.safety_reason,
        "recovery_blockers": list(checkpoint.recovery_blockers),
        "score_authorized_quantity": checkpoint.score_authorized_quantity,
        "risk_admitted_quantity": checkpoint.risk_admitted_quantity,
        "quantity_blockers": list(checkpoint.quantity_blockers),
        "entry_fill_ids": list(checkpoint.entry_fill_ids),
        "exit_fill_ids": list(checkpoint.exit_fill_ids),
        "safety_fill_ids": list(checkpoint.safety_fill_ids),
    }


def _checkpoint_hash(checkpoint: PongCheckpoint) -> str:
    material = json.dumps(
        _checkpoint_payload(checkpoint), sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


_ACTION_PAYLOAD_KEYS = frozenset({
    "action_id", "kind", "source_switch_id", "source_switch_sequence",
    "expected_direction", "reason_code", "requested_total_quantity",
    "score_authorized_quantity", "position_side_support",
    "quantity_policy_hash", "discernment_policy_hash",
    "discernment_model_hash", "snapshot_hash", "bias_result_hash",
    "reconciliation_hash", "blockers", "admission_id",
    "target_action_id",
})
_CHECKPOINT_PAYLOAD_KEYS = frozenset({
    "schema", "profile", "mode", "account_name", "account_class", "instrument",
    "canonical_contract", "live_authority", "signal_source", "quantity_policy_hash",
    "discernment_policy_hash", "discernment_model_hash", "state",
    "shadow_direction", "actual_position", "actual_quantity",
    "protected_quantity", "last_switch_id", "last_switch_sequence",
    "last_consumed_switch_id", "last_snapshot_hash",
    "last_snapshot_continuity_domain", "last_snapshot_revision",
    "pending_action", "reducing_actions", "protective_actions",
    "retired_reducing_actions", "deferred_exit_action",
    "pending_protective_cancel_ids",
    "confirmed_flat_switch_sequence",
    "waiting_leg_completed", "entry_admitted", "entry_cancel_requested",
    "entry_target_quantity",
    "entry_filled_quantity", "exit_target_quantity", "exit_filled_quantity",
    "daily_bias_result_hash", "safety_reason", "recovery_blockers",
    "score_authorized_quantity", "risk_admitted_quantity",
    "quantity_blockers", "entry_fill_ids", "exit_fill_ids",
    "safety_fill_ids", "checkpoint_hash",
})


def _strict_keys(
    value: object,
    expected: frozenset[str],
    field: str,
) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise ValueError(f"{field} fields are missing or unknown.")
    return value


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if type(value) is not list or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise ValueError(f"{field} must be a list of non-empty strings.")
    return tuple(value)


def _action_from_payload(value: object) -> PongAction | None:
    if value is None:
        return None
    payload = _strict_keys(value, _ACTION_PAYLOAD_KEYS, "Pong action")
    return PongAction(
        action_id=payload["action_id"],  # type: ignore[arg-type]
        kind=PongActionKind(payload["kind"]),
        source_switch_id=payload["source_switch_id"],  # type: ignore[arg-type]
        source_switch_sequence=payload["source_switch_sequence"],  # type: ignore[arg-type]
        expected_direction=PongDirection(payload["expected_direction"]),
        reason_code=payload["reason_code"],  # type: ignore[arg-type]
        requested_total_quantity=payload["requested_total_quantity"],  # type: ignore[arg-type]
        score_authorized_quantity=payload["score_authorized_quantity"],  # type: ignore[arg-type]
        position_side_support=payload["position_side_support"],  # type: ignore[arg-type]
        quantity_policy_hash=payload["quantity_policy_hash"],  # type: ignore[arg-type]
        discernment_policy_hash=payload["discernment_policy_hash"],  # type: ignore[arg-type]
        discernment_model_hash=payload["discernment_model_hash"],  # type: ignore[arg-type]
        snapshot_hash=payload["snapshot_hash"],  # type: ignore[arg-type]
        bias_result_hash=payload["bias_result_hash"],  # type: ignore[arg-type]
        reconciliation_hash=payload["reconciliation_hash"],  # type: ignore[arg-type]
        blockers=_string_tuple(payload["blockers"], "Pong action blockers"),
        admission_id=payload["admission_id"],  # type: ignore[arg-type]
        target_action_id=payload["target_action_id"],  # type: ignore[arg-type]
    )


def serialize_pong_checkpoint(checkpoint: PongCheckpoint) -> str:
    """Return the canonical, hash-bound checkpoint envelope without I/O."""
    if (
        type(checkpoint) is not PongCheckpoint
        or checkpoint.checkpoint_hash != _checkpoint_hash(checkpoint)
    ):
        raise ValueError("PONG_CHECKPOINT_INTEGRITY_FAILED")
    payload = _checkpoint_payload(checkpoint)
    payload["checkpoint_hash"] = checkpoint.checkpoint_hash
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def deserialize_pong_checkpoint(encoded: str | bytes) -> PongCheckpoint:
    """Strictly decode a complete canonical checkpoint; partial input is refused."""
    if type(encoded) is bytes:
        try:
            encoded = encoded.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValueError("PONG_CHECKPOINT_ENCODING_INVALID") from exc
    if type(encoded) is not str or not encoded:
        raise ValueError("PONG_CHECKPOINT_ENCODING_INVALID")
    try:
        value = json.loads(encoded)
    except json.JSONDecodeError as exc:
        raise ValueError("PONG_CHECKPOINT_ENCODING_INVALID") from exc
    payload = _strict_keys(value, _CHECKPOINT_PAYLOAD_KEYS, "Pong checkpoint")

    reducing_value = payload["reducing_actions"]
    if type(reducing_value) is not list:
        raise ValueError("Pong checkpoint reducing actions must be a list.")
    reducing_actions: list[tuple[PongAction, int]] = []
    for item in reducing_value:
        row = _strict_keys(
            item, frozenset({"action", "remaining_quantity"}),
            "Pong reducing action",
        )
        action = _action_from_payload(row["action"])
        if action is None:
            raise ValueError("Pong reducing action cannot be null.")
        reducing_actions.append((action, row["remaining_quantity"]))  # type: ignore[arg-type]

    protective_value = payload["protective_actions"]
    if type(protective_value) is not list:
        raise ValueError("Pong protective actions must be a list.")
    protective_actions: list[tuple[PongAction, int]] = []
    for item in protective_value:
        row = _strict_keys(
            item, frozenset({"action", "remaining_quantity"}),
            "Pong protective action",
        )
        action = _action_from_payload(row["action"])
        if action is None:
            raise ValueError("Pong protective action cannot be null.")
        protective_actions.append((action, row["remaining_quantity"]))  # type: ignore[arg-type]

    retired_value = payload["retired_reducing_actions"]
    if type(retired_value) is not list:
        raise ValueError("Pong retired reducing actions must be a list.")
    retired_reducing_actions: list[PongAction] = []
    for item in retired_value:
        action = _action_from_payload(item)
        if action is None:
            raise ValueError("Pong retired reducing action cannot be null.")
        retired_reducing_actions.append(action)

    shadow_value = payload["shadow_direction"]
    checkpoint = PongCheckpoint(
        schema=payload["schema"],  # type: ignore[arg-type]
        profile=payload["profile"],  # type: ignore[arg-type]
        mode=payload["mode"],  # type: ignore[arg-type]
        account_name=payload["account_name"],  # type: ignore[arg-type]
        account_class=payload["account_class"],  # type: ignore[arg-type]
        instrument=payload["instrument"],  # type: ignore[arg-type]
        canonical_contract=payload["canonical_contract"],  # type: ignore[arg-type]
        live_authority=payload["live_authority"],  # type: ignore[arg-type]
        signal_source=payload["signal_source"],  # type: ignore[arg-type]
        quantity_policy_hash=payload["quantity_policy_hash"],  # type: ignore[arg-type]
        discernment_policy_hash=payload["discernment_policy_hash"],  # type: ignore[arg-type]
        discernment_model_hash=payload["discernment_model_hash"],  # type: ignore[arg-type]
        state=PongState(payload["state"]),
        shadow_direction=(
            None if shadow_value is None else PongDirection(shadow_value)
        ),
        actual_position=PongDirection(payload["actual_position"]),
        actual_quantity=payload["actual_quantity"],  # type: ignore[arg-type]
        protected_quantity=payload["protected_quantity"],  # type: ignore[arg-type]
        last_switch_id=payload["last_switch_id"],  # type: ignore[arg-type]
        last_switch_sequence=payload["last_switch_sequence"],  # type: ignore[arg-type]
        last_consumed_switch_id=payload["last_consumed_switch_id"],  # type: ignore[arg-type]
        last_snapshot_hash=payload["last_snapshot_hash"],  # type: ignore[arg-type]
        last_snapshot_continuity_domain=payload["last_snapshot_continuity_domain"],  # type: ignore[arg-type]
        last_snapshot_revision=payload["last_snapshot_revision"],  # type: ignore[arg-type]
        pending_action=_action_from_payload(payload["pending_action"]),
        reducing_actions=tuple(reducing_actions),
        protective_actions=tuple(protective_actions),
        retired_reducing_actions=tuple(retired_reducing_actions),
        deferred_exit_action=_action_from_payload(payload["deferred_exit_action"]),
        pending_protective_cancel_ids=_string_tuple(
            payload["pending_protective_cancel_ids"],
            "Pong pending protective cancellation IDs",
        ),
        confirmed_flat_switch_sequence=payload["confirmed_flat_switch_sequence"],  # type: ignore[arg-type]
        waiting_leg_completed=payload["waiting_leg_completed"],  # type: ignore[arg-type]
        entry_admitted=payload["entry_admitted"],  # type: ignore[arg-type]
        entry_cancel_requested=payload["entry_cancel_requested"],  # type: ignore[arg-type]
        entry_target_quantity=payload["entry_target_quantity"],  # type: ignore[arg-type]
        entry_filled_quantity=payload["entry_filled_quantity"],  # type: ignore[arg-type]
        exit_target_quantity=payload["exit_target_quantity"],  # type: ignore[arg-type]
        exit_filled_quantity=payload["exit_filled_quantity"],  # type: ignore[arg-type]
        daily_bias_result_hash=payload["daily_bias_result_hash"],  # type: ignore[arg-type]
        safety_reason=payload["safety_reason"],  # type: ignore[arg-type]
        recovery_blockers=_string_tuple(
            payload["recovery_blockers"], "Pong recovery blockers",
        ),
        score_authorized_quantity=payload["score_authorized_quantity"],  # type: ignore[arg-type]
        risk_admitted_quantity=payload["risk_admitted_quantity"],  # type: ignore[arg-type]
        quantity_blockers=_string_tuple(
            payload["quantity_blockers"], "Pong quantity blockers",
        ),
        entry_fill_ids=_string_tuple(payload["entry_fill_ids"], "Pong entry fill IDs"),
        exit_fill_ids=_string_tuple(payload["exit_fill_ids"], "Pong exit fill IDs"),
        safety_fill_ids=_string_tuple(payload["safety_fill_ids"], "Pong safety fill IDs"),
        checkpoint_hash=payload["checkpoint_hash"],  # type: ignore[arg-type]
    )
    if checkpoint.checkpoint_hash != _checkpoint_hash(checkpoint):
        raise ValueError("PONG_CHECKPOINT_INTEGRITY_FAILED")
    if serialize_pong_checkpoint(checkpoint) != encoded:
        raise ValueError("PONG_CHECKPOINT_ENCODING_NOT_CANONICAL")
    return checkpoint


class PongStateMachine:
    """Deterministic candidate lifecycle with no execution capability."""

    def __init__(
        self,
        quantity_policy: PongQuantityPolicy = PONG_QUANTITY_POLICY,
        discernment_policy: DiscernmentPolicy = PONG_RESEARCH_POLICY,
    ) -> None:
        if type(quantity_policy) is not PongQuantityPolicy:
            raise ValueError("Pong requires an exact quantity policy contract.")
        self.quantity_policy = quantity_policy
        if type(discernment_policy) is not DiscernmentPolicy:
            raise ValueError("Pong requires an exact Discernment policy contract.")
        self.discernment_policy = discernment_policy
        self.state = PongState.FLAT_WAIT_ALIGNMENT
        self.shadow_direction: PongDirection | None = None
        self.actual_position = PongDirection.FLAT
        self.actual_quantity = 0
        self.protected_quantity = 0
        self.last_switch_id: str | None = None
        self.last_switch_sequence = 0
        self.last_consumed_switch_id: str | None = None
        self.last_snapshot: DiscernmentSnapshot | None = None
        self._restored_snapshot_proof: tuple[
            str | None, str | None, int | None,
        ] = (None, None, None)
        self.pending_action: PongAction | None = None
        self.confirmed_flat_switch_sequence: int | None = None
        self.waiting_leg_completed = False
        self.entry_admitted = False
        self.entry_cancel_requested = False
        self.entry_target_quantity = 0
        self.entry_filled_quantity = 0
        self.exit_target_quantity = 0
        self.exit_filled_quantity = 0
        self.daily_bias = default_unconfigured_bias()
        self._restored_daily_bias_result_hash: str | None = None
        self.safety_reason: str | None = None
        self.recovery_blockers: tuple[str, ...] = ()
        self.score_authorized_quantity = 0
        self.risk_admitted_quantity = 0
        self.quantity_blockers: tuple[str, ...] = ()
        self._entry_fill_ids: set[str] = set()
        self._exit_fill_ids: set[str] = set()
        self._safety_fill_ids: set[str] = set()
        self._reducing_actions: dict[str, PongAction] = {}
        self._reducing_remaining: dict[str, int] = {}
        self._protective_actions: dict[str, PongAction] = {}
        self._protective_remaining: dict[str, int] = {}
        self._retired_reducing_actions: dict[str, PongAction] = {}
        self._deferred_exit_action: PongAction | None = None
        self._pending_protective_cancel_ids: set[str] = set()

    def _snapshot_reason(
        self,
        snapshot: DiscernmentSnapshot,
        *,
        at: str | None = None,
    ) -> str | None:
        if type(snapshot) is not DiscernmentSnapshot:
            return "DISCERNMENT_SNAPSHOT_TYPE_INVALID"
        if (
            snapshot.instrument != self.quantity_policy.instrument
            or snapshot.contract != self.quantity_policy.canonical_contract
        ):
            return "DISCERNMENT_SOURCE_IDENTITY_MISMATCH"
        if snapshot.execution_enabled:
            return "DISCERNMENT_EXECUTION_AUTHORITY_INVALID"
        if self.discernment_policy.evidence_mapping_hash is None:
            return "DISCERNMENT_EVIDENCE_MAPPING_UNBOUND"
        verification_error = snapshot.verification_error(
            self.discernment_policy, at=at,
        )
        if verification_error is not None:
            return verification_error
        if not snapshot.continuity_valid:
            return "DISCERNMENT_SWITCH_CONTINUITY_GAP"
        return None

    def _action(
        self,
        kind: PongActionKind,
        *,
        source_switch_id: str,
        source_sequence: int,
        expected: PongDirection,
        reason: str,
        requested_quantity: int,
        score_quantity: int = 0,
        support: float | None = None,
        blockers: tuple[str, ...] = (),
        snapshot_hash: str | None = None,
        bias_result_hash: str | None = None,
        reconciliation_hash: str | None = None,
        admission_id: str | None = None,
        target_action_id: str | None = None,
    ) -> PongAction:
        if type(requested_quantity) is not int or requested_quantity < 0:
            raise ValueError("Pong action quantity must be a non-negative integer.")
        if kind in {
            PongActionKind.REQUEST_ENTRY_LONG, PongActionKind.REQUEST_ENTRY_SHORT,
        } and requested_quantity not in {1, 2}:
            raise ValueError("Pong entries are bounded to one or two MNQ total.")
        if kind is PongActionKind.CANCEL_PENDING_ENTRY and requested_quantity != 0:
            raise ValueError("Entry cancellation cannot request position exposure.")
        material = json.dumps(
            {
                "profile": PONG_PROFILE_VERSION,
                "signal_source": PONG_SIGNAL_SOURCE,
                "kind": kind.value,
                "switch": source_switch_id,
                "sequence": source_sequence,
                "expected": expected.value,
                "reason": reason,
                "requested_total_quantity": requested_quantity,
                "score_authorized_quantity": score_quantity,
                "position_side_support": support,
                "quantity_policy_hash": self.quantity_policy.configuration_hash,
                "discernment_policy_hash": self.discernment_policy.policy_hash,
                "discernment_model_hash": self.discernment_policy.model_hash,
                "snapshot_hash": snapshot_hash,
                "bias_result_hash": bias_result_hash,
                "reconciliation_hash": reconciliation_hash,
                "blockers": list(blockers),
                "admission_id": admission_id,
                "target_action_id": target_action_id,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return PongAction(
            "pong-action-" + hashlib.sha256(material).hexdigest()[:32],
            kind,
            source_switch_id,
            source_sequence,
            expected,
            reason,
            requested_quantity,
            score_quantity,
            support,
            self.quantity_policy.configuration_hash,
            self.discernment_policy.policy_hash,
            self.discernment_policy.model_hash,
            snapshot_hash,
            bias_result_hash,
            reconciliation_hash,
            blockers,
            admission_id,
            target_action_id,
        )

    def _event_action(
        self,
        kind: PongActionKind,
        event: DiscernmentSwitch,
        expected: PongDirection,
        reason: str,
        requested_quantity: int,
        *,
        score_quantity: int = 0,
        support: float | None = None,
        blockers: tuple[str, ...] = (),
    ) -> PongAction:
        return self._action(
            kind,
            source_switch_id=event.event_id,
            source_sequence=event.sequence,
            expected=expected,
            reason=reason,
            requested_quantity=requested_quantity,
            score_quantity=score_quantity,
            support=support,
            blockers=blockers,
            snapshot_hash=event.snapshot.result_hash,
        )

    def observe_startup_seed(
        self,
        direction: DiscernmentDirection,
        snapshot: DiscernmentSnapshot,
    ) -> PongTransition:
        """Establish the already-latched preference without inventing a switch."""
        reason = self._snapshot_reason(snapshot)
        if reason is not None:
            return self._safety_block(reason)
        seeded = _pong_direction(direction)
        if self.last_switch_sequence or self.actual_quantity:
            return self._transition("STARTUP_SEED_TOO_LATE", False)
        if self.shadow_direction is None:
            self.shadow_direction = seeded
            self.last_snapshot = snapshot
            return self._transition("DISCERNMENT_SEED_OBSERVED_NO_SWITCH", False)
        if self.shadow_direction is seeded and self.last_snapshot is not None:
            return self._transition("STARTUP_SEED_REPLAY_IGNORED", False)
        return self._safety_block("STARTUP_SEED_DIRECTION_CONFLICT")

    def on_snapshot_update(self, snapshot: DiscernmentSnapshot) -> PongTransition:
        """Update score/status only; never manufacture a switch or beat."""
        reason = self._snapshot_reason(snapshot)
        if reason is not None:
            self.last_snapshot = snapshot if type(snapshot) is DiscernmentSnapshot else None
            if self.actual_quantity or self.state in {
                PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT,
            }:
                return self.on_safety_exit(reason=reason)
            return self._transition(reason, False)
        if (
            self.last_snapshot is not None
            and snapshot.continuity_domain != self.last_snapshot.continuity_domain
        ):
            return self._safety_block("DISCERNMENT_CONTINUITY_DOMAIN_CHANGED")
        if self.last_snapshot is not None and (
            _utc(snapshot.available_at) < _utc(self.last_snapshot.available_at)
            or snapshot.evidence_revision < self.last_snapshot.evidence_revision
        ):
            return self._transition("OUT_OF_ORDER_DISCERNMENT_SNAPSHOT_IGNORED", False)
        if (
            self.last_snapshot is not None
            and snapshot.evidence_revision == self.last_snapshot.evidence_revision
            and snapshot.result_hash != self.last_snapshot.result_hash
        ):
            return self._safety_block("DISCERNMENT_REVISION_IDENTITY_CONFLICT")
        self.last_snapshot = snapshot
        if (
            self.state in {PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT}
            and self.pending_action is not None
            and self.entry_admitted
            and not self.entry_cancel_requested
            and 0 < self.entry_filled_quantity < self.entry_target_quantity
        ):
            expected = self.pending_action.expected_direction
            score = snapshot.effective_long_score
            assert score is not None
            authorization = score_authorized_quantity(
                PongQuantityDirection(expected.value),
                score,
                evidence_valid=snapshot.valid,
                otherwise_eligible=True,
            )
            if authorization.requested_total_quantity < self.entry_target_quantity:
                self.entry_cancel_requested = True
                cancellation = self._action(
                    PongActionKind.CANCEL_PENDING_ENTRY,
                    source_switch_id=self.pending_action.source_switch_id,
                    source_sequence=self.pending_action.source_switch_sequence,
                    expected=PongDirection.FLAT,
                    reason="ENTRY_REMAINDER_CONFIDENCE_LOSS",
                    requested_quantity=0,
                    blockers=("ENTRY_REMAINDER_CONFIDENCE_LOSS",),
                    snapshot_hash=snapshot.result_hash,
                )
                return self._transition(
                    "ENTRY_REMAINDER_CANCELLATION_REQUESTED",
                    False,
                    (cancellation,),
                )
        return self._transition("DISCERNMENT_SNAPSHOT_UPDATED_NO_SWITCH", False)

    def on_bias_update(self, snapshot: DailyBiasResult) -> PongTransition:
        if type(snapshot) is not DailyBiasResult:
            raise ValueError("Pong requires a validated DailyBiasResult.")
        self.daily_bias = snapshot
        self._restored_daily_bias_result_hash = None
        return self._transition("DAILY_BIAS_UPDATED_NO_POSITION_ACTION", False)

    def on_reporting_boundary(self) -> PongTransition:
        return self._transition("REPORTING_BOUNDARY_NO_STATE_RESET", False)

    def on_switch(
        self,
        event: DiscernmentSwitch,
        bias: DailyBiasResult,
        *,
        entry_gates_pass: bool = True,
    ) -> PongTransition:
        if type(event) is not DiscernmentSwitch:
            return self._safety_block("DISCERNMENT_SWITCH_TYPE_INVALID")
        if type(bias) is not DailyBiasResult:
            raise ValueError("Pong requires a validated DailyBiasResult.")
        if type(entry_gates_pass) is not bool:
            return self._safety_block("ENTRY_GATE_CONTRACT_INVALID")
        snapshot_reason = self._snapshot_reason(event.snapshot)
        if snapshot_reason is not None:
            return self._safety_block(snapshot_reason)
        previous = _pong_direction(event.previous_direction)
        direction = _pong_direction(event.direction)
        if (
            not event.event_id
            or event.sequence <= 0
            or previous is direction
            or event.continuity_domain != event.snapshot.continuity_domain
            or event.available_at != event.snapshot.available_at
            or _utc(event.occurred_at) > _utc(event.available_at)
        ):
            return self._safety_block("DISCERNMENT_SWITCH_EVENT_INVALID")
        intrinsic_error = event.verification_error(self.discernment_policy)
        if intrinsic_error is not None:
            return self._safety_block(intrinsic_error)
        if event.sequence == self.last_switch_sequence:
            if event.event_id == self.last_switch_id:
                return self._transition("DUPLICATE_SWITCH_IGNORED", False)
            return self._safety_block("SWITCH_SEQUENCE_IDENTITY_CONFLICT")
        if event.sequence < self.last_switch_sequence:
            return self._transition("OUT_OF_ORDER_SWITCH_IGNORED", False)
        if not event.continuity_valid:
            return self._safety_block("DISCERNMENT_SWITCH_CONTINUITY_GAP")
        if (
            event.sequence != self.last_switch_sequence + 1
            or event.prior_switch_id != self.last_switch_id
        ):
            return self._safety_block("DISCERNMENT_SWITCH_CONTINUITY_GAP")
        if self.shadow_direction is None:
            return self._safety_block("DISCERNMENT_SWITCH_SEED_MISSING")
        stream_error = event.verification_error(
            self.discernment_policy,
            expected_previous_direction=DiscernmentDirection(
                self.shadow_direction.value,
            ),
            expected_prior_switch_id=self.last_switch_id,
            expected_sequence=self.last_switch_sequence + 1,
        )
        if stream_error is not None:
            return self._safety_block(stream_error)
        if previous is not self.shadow_direction:
            return self._safety_block("DISCERNMENT_SHADOW_DIRECTION_MISMATCH")
        if (
            self.last_snapshot is not None
            and event.continuity_domain != self.last_snapshot.continuity_domain
        ):
            return self._safety_block("DISCERNMENT_CONTINUITY_DOMAIN_CHANGED")

        self.shadow_direction = direction
        self.last_switch_id = event.event_id
        self.last_switch_sequence = event.sequence
        self.last_snapshot = event.snapshot
        self.daily_bias = bias
        self._restored_daily_bias_result_hash = None

        if self.state is PongState.SAFETY_BLOCKED:
            self.last_consumed_switch_id = event.event_id
            return self._transition("SAFETY_LOCKOUT_RETAINED", True)
        if self.state in {PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT}:
            self.last_consumed_switch_id = event.event_id
            if self.actual_quantity:
                return self.on_safety_exit(
                    reason="PARTIAL_ENTRY_INVALIDATED_BY_OPPOSITE_SWITCH", event=event,
                )
            assert self.pending_action is not None
            cancellation = self._event_action(
                PongActionKind.CANCEL_PENDING_ENTRY, event, PongDirection.FLAT,
                "ENTRY_SIGNAL_SUPERSEDED_BEFORE_FILL", 0,
            )
            self._clear_entry_candidate()
            self.state = PongState.FLAT_WAIT_ALIGNMENT
            return self._transition(
                "ENTRY_PENDING_SWITCH_CANCELLED_NO_QUEUED_ENTRY", True,
                (cancellation,),
            )
        if self.state is PongState.EXIT_PENDING:
            self.last_consumed_switch_id = event.event_id
            return self._transition("EXIT_PENDING_SWITCH_OBSERVED_NO_QUEUED_ENTRY", True)
        if self.state in {PongState.LONG, PongState.SHORT}:
            if self.actual_quantity <= 0:
                return self._safety_block("POSITION_QUANTITY_STATE_INVALID")
            action = self._event_action(
                PongActionKind.REQUEST_EXIT, event, self.actual_position,
                "NEXT_OPPOSITE_DISCERNMENT_SWITCH", self.actual_quantity,
            )
            self.exit_target_quantity = self.actual_quantity
            self.exit_filled_quantity = 0
            self.state = PongState.EXIT_PENDING
            self.last_consumed_switch_id = event.event_id
            if self._protective_actions:
                self._deferred_exit_action = action
                self._pending_protective_cancel_ids = set(
                    self._protective_actions,
                )
                cancellations = tuple(
                    self._action(
                        PongActionKind.CANCEL_PROTECTIVE_STOP,
                        source_switch_id=event.event_id,
                        source_sequence=event.sequence,
                        expected=protective.expected_direction,
                        reason="CANCEL_PROTECTION_BEFORE_POSITION_EXIT",
                        requested_quantity=0,
                        snapshot_hash=event.snapshot.result_hash,
                        target_action_id=protective_id,
                    )
                    for protective_id, protective in sorted(
                        self._protective_actions.items(),
                    )
                )
                self.pending_action = cancellations[0]
                return self._transition(
                    "PROTECTIVE_CANCEL_REQUESTED_BEFORE_EXIT",
                    True,
                    cancellations,
                )
            self.pending_action = action
            self._reducing_actions[action.action_id] = action
            self._reducing_remaining[action.action_id] = action.requested_total_quantity
            return self._transition("POSITION_EXIT_REQUESTED", True, (action,))
        if self.state is PongState.FLAT_WAIT_BEAT:
            anchor = self.confirmed_flat_switch_sequence
            if anchor is None or event.sequence <= anchor:
                return self._safety_block("WAITING_BEAT_CONTINUITY_INVALID")
            self.waiting_leg_completed = True
            self.state = PongState.FLAT_WAIT_ALIGNMENT

        self.last_consumed_switch_id = event.event_id
        return self._evaluate_flat_entry(event, bias, entry_gates_pass=entry_gates_pass)

    def _evaluate_flat_entry(
        self,
        event: DiscernmentSwitch,
        bias: DailyBiasResult,
        *,
        entry_gates_pass: bool,
    ) -> PongTransition:
        direction = _pong_direction(event.direction)
        bias_reason = _pong_bias_eligibility(
            bias, at=event.available_at, direction=direction,
        )
        if bias_reason != "DAILY_BIAS_ALIGNED":
            return self._transition(bias_reason, True)
        if not entry_gates_pass:
            return self._transition("ENTRY_GATES_BLOCKED", True)
        authorization = score_authorized_quantity(
            PongQuantityDirection(direction.value), event.effective_long_score,
            evidence_valid=event.snapshot.valid, otherwise_eligible=True,
        )
        kind = (
            PongActionKind.REQUEST_ENTRY_LONG
            if direction is PongDirection.LONG
            else PongActionKind.REQUEST_ENTRY_SHORT
        )
        action = self._event_action(
            kind, event, direction, "ALIGNED_DISCERNMENT_SWITCH_AFTER_REQUIRED_BEAT",
            authorization.requested_total_quantity,
            score_quantity=authorization.requested_total_quantity,
            support=authorization.position_side_support,
        )
        self.pending_action = action
        self.entry_admitted = False
        self.entry_cancel_requested = False
        self.entry_target_quantity = action.requested_total_quantity
        self.entry_filled_quantity = 0
        self.score_authorized_quantity = action.score_authorized_quantity
        self.risk_admitted_quantity = 0
        self.quantity_blockers = ()
        self.waiting_leg_completed = False
        self.state = (
            PongState.ENTRY_PENDING_LONG
            if direction is PongDirection.LONG
            else PongState.ENTRY_PENDING_SHORT
        )
        return self._transition("ENTRY_REQUESTED_FOR_REVALIDATION", True, (action,))

    def on_entry_admission(
        self,
        *,
        action_id: str,
        bias: DailyBiasResult,
        at: str,
        current_snapshot: DiscernmentSnapshot,
        current_switch_cursor: DiscernmentSwitch,
        source_fresh: bool,
        entry_gates_pass: bool,
        reconciliation: PongReconciliationProof,
        available_loss_allowance_dollars: object,
        already_reserved_risk_dollars: object = 0,
    ) -> PongTransition:
        if type(bias) is not DailyBiasResult:
            raise ValueError("Pong requires a validated DailyBiasResult.")
        if type(reconciliation) is not PongReconciliationProof:
            return self._safety_block("PONG_RECONCILIATION_CONTRACT_INVALID")
        if (
            self.pending_action is None
            or self.pending_action.action_id != action_id
            or self.state not in {PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT}
            or self.actual_quantity
        ):
            return self._safety_block("UNEXPECTED_ENTRY_ADMISSION")
        if type(source_fresh) is not bool or type(entry_gates_pass) is not bool:
            return self._deny_pending_entry("ENTRY_GATE_CONTRACT_INVALID")
        if type(current_switch_cursor) is not DiscernmentSwitch:
            return self._deny_pending_entry("DISCERNMENT_SWITCH_CURSOR_INVALID")
        cursor_error = current_switch_cursor.verification_error(
            self.discernment_policy,
        )
        if cursor_error is not None:
            return self._deny_pending_entry(cursor_error)
        expected = self.pending_action.expected_direction
        if (
            current_switch_cursor.event_id != self.pending_action.source_switch_id
            or current_switch_cursor.event_id != self.last_switch_id
            or current_switch_cursor.sequence != self.pending_action.source_switch_sequence
            or current_switch_cursor.sequence != self.last_switch_sequence
            or _pong_direction(current_switch_cursor.direction) is not expected
        ):
            return self._deny_pending_entry(
                "DISCERNMENT_SWITCH_CURSOR_CHANGED_AT_ADMISSION",
            )
        try:
            snapshot_reason = self._snapshot_reason(current_snapshot, at=at)
        except (TypeError, ValueError):
            snapshot_reason = "DISCERNMENT_ADMISSION_TIME_INVALID"
        if snapshot_reason is not None:
            return self._deny_pending_entry(snapshot_reason)
        if (
            current_switch_cursor.continuity_domain
            != current_snapshot.continuity_domain
        ):
            return self._deny_pending_entry(
                "DISCERNMENT_SWITCH_CURSOR_CHANGED_AT_ADMISSION",
            )
        if self.last_snapshot is None or current_snapshot.continuity_domain != self.last_snapshot.continuity_domain:
            return self._deny_pending_entry("DISCERNMENT_CONTINUITY_DOMAIN_CHANGED")
        if (
            _utc(current_snapshot.available_at) < _utc(self.last_snapshot.available_at)
            or current_snapshot.evidence_revision < self.last_snapshot.evidence_revision
        ):
            return self._deny_pending_entry("DISCERNMENT_SNAPSHOT_STALE_AT_ADMISSION")
        if not source_fresh:
            return self._deny_pending_entry("DISCERNMENT_SOURCE_NOT_FRESH")
        bias_reason = _pong_bias_eligibility(bias, at=at, direction=expected)
        if bias_reason != "DAILY_BIAS_ALIGNED":
            return self._deny_pending_entry(bias_reason)
        if not entry_gates_pass:
            return self._deny_pending_entry("ENTRY_GATES_BLOCKED_AT_ADMISSION")
        score = current_snapshot.effective_long_score
        assert score is not None
        if (
            expected is PongDirection.LONG
            and score <= self.discernment_policy.short_on_threshold
        ) or (
            expected is PongDirection.SHORT
            and score >= self.discernment_policy.long_on_threshold
        ):
            return self._deny_pending_entry("OPPOSITE_DISCERNMENT_SWITCH_NOT_PROCESSED")
        authorization = score_authorized_quantity(
            PongQuantityDirection(expected.value), score,
            evidence_valid=current_snapshot.valid, otherwise_eligible=True,
        )
        original_action = self.pending_action
        try:
            quantity_admission = assess_quantity_admission(
                authorization,
                opportunity_action_id=original_action.action_id,
                source_switch_id=original_action.source_switch_id,
                snapshot_hash=current_snapshot.result_hash,
                bias_result_hash=bias.result_hash,
                admission_at=at,
                reconciliation=reconciliation,
                available_loss_allowance_dollars=available_loss_allowance_dollars,
                already_reserved_risk_dollars=already_reserved_risk_dollars,
                policy=self.quantity_policy,
            )
        except (TypeError, ValueError):
            return self._deny_pending_entry("PONG_RISK_INPUT_INVALID")
        self.score_authorized_quantity = authorization.requested_total_quantity
        self.risk_admitted_quantity = quantity_admission.admitted_quantity
        self.quantity_blockers = quantity_admission.blockers
        if quantity_admission.admitted_quantity == 0:
            reason = quantity_admission.blockers[0] if quantity_admission.blockers else "QUANTITY_ADMISSION_DENIED"
            return self._deny_pending_entry(reason)

        original_quantity = original_action.requested_total_quantity
        reason = (
            "ENTRY_ADMISSION_DOWNGRADED_NO_EXECUTION"
            if quantity_admission.admitted_quantity < original_quantity
            else "ENTRY_ADMISSION_CONFIRMED_NO_EXECUTION"
        )
        self.pending_action = self._action(
            original_action.kind,
            source_switch_id=original_action.source_switch_id,
            source_sequence=self.last_switch_sequence,
            expected=expected,
            reason=reason,
            requested_quantity=quantity_admission.admitted_quantity,
            score_quantity=authorization.requested_total_quantity,
            support=authorization.position_side_support,
            blockers=quantity_admission.blockers,
            snapshot_hash=current_snapshot.result_hash,
            bias_result_hash=bias.result_hash,
            reconciliation_hash=reconciliation.result_hash,
            admission_id=quantity_admission.admission_id,
        )
        self.entry_target_quantity = quantity_admission.admitted_quantity
        self.last_snapshot = current_snapshot
        self.daily_bias = bias
        self._restored_daily_bias_result_hash = None
        self.entry_admitted = True
        return self._transition(reason, False, (self.pending_action,))

    def _deny_pending_entry(self, reason: str) -> PongTransition:
        assert self.pending_action is not None
        action = self._action(
            PongActionKind.CANCEL_PENDING_ENTRY,
            source_switch_id=self.pending_action.source_switch_id,
            source_sequence=self.last_switch_sequence,
            expected=PongDirection.FLAT,
            reason=reason,
            requested_quantity=0,
            blockers=(reason,),
            snapshot_hash=None if self.last_snapshot is None else self.last_snapshot.result_hash,
        )
        self._clear_entry_candidate()
        self.state = PongState.FLAT_WAIT_ALIGNMENT
        return self._transition(reason, False, (action,))

    def _clear_entry_candidate(self) -> None:
        self.pending_action = None
        self.entry_admitted = False
        self.entry_cancel_requested = False
        self.entry_target_quantity = 0
        self.entry_filled_quantity = 0

    def on_entry_fill(
        self,
        *,
        action_id: str,
        direction: PongDirection,
        quantity: int,
        fill_id: str,
    ) -> PongTransition:
        expected_state = (
            PongState.ENTRY_PENDING_LONG
            if direction is PongDirection.LONG
            else PongState.ENTRY_PENDING_SHORT
        )
        if (
            fill_id in self._entry_fill_ids
            or fill_id in self._exit_fill_ids
            or fill_id in self._safety_fill_ids
        ):
            return self._transition("DUPLICATE_ENTRY_FILL_IGNORED", False)
        if type(quantity) is not int or quantity <= 0 or not fill_id:
            return self._safety_block("ENTRY_FILL_CONTRACT_INVALID")
        if (
            self.pending_action is None
            or self.pending_action.action_id != action_id
            or self.state is not expected_state
            or not self.entry_admitted
            or self.entry_filled_quantity + quantity > self.entry_target_quantity
        ):
            if direction in {PongDirection.LONG, PongDirection.SHORT}:
                # A stale/cancel-racing native fill is still real exposure.  Its
                # immutable execution identity must also remain consumed so a
                # repeated callback cannot manufacture additional exposure.
                self._entry_fill_ids.add(fill_id)
                self.actual_position = direction
                self.actual_quantity += quantity
            return self.on_safety_exit(reason="UNEXPECTED_OR_STALE_ENTRY_FILL")
        self._entry_fill_ids.add(fill_id)
        self.entry_filled_quantity += quantity
        self.actual_position = direction
        self.actual_quantity += quantity
        self.protected_quantity = min(self.protected_quantity, self.actual_quantity)
        protection = self._action(
            PongActionKind.REQUEST_PROTECTIVE_STOP,
            source_switch_id=self.pending_action.source_switch_id,
            source_sequence=self.last_switch_sequence,
            expected=direction,
            reason="PROTECT_ACTUAL_FILLED_QUANTITY",
            requested_quantity=self.actual_quantity,
            score_quantity=self.pending_action.score_authorized_quantity,
            support=self.pending_action.position_side_support,
            snapshot_hash=None if self.last_snapshot is None else self.last_snapshot.result_hash,
        )
        self._protective_actions[protection.action_id] = protection
        self._protective_remaining[protection.action_id] = self.actual_quantity
        if self.entry_filled_quantity == self.entry_target_quantity:
            self.state = PongState.LONG if direction is PongDirection.LONG else PongState.SHORT
            self.pending_action = None
            self.entry_admitted = False
            self.entry_cancel_requested = False
            reason = "ENTRY_FILL_PROTECTION_REQUIRED"
        else:
            reason = "ENTRY_PARTIAL_FILL_PROTECTION_REQUIRED"
        return self._transition(reason, False, (protection,))

    def _retire_reducing_action(
        self,
        action_id: str,
        actions: dict[str, PongAction],
        remaining: dict[str, int],
    ) -> None:
        action = actions.pop(action_id, None)
        remaining.pop(action_id, None)
        if action is not None:
            self._retired_reducing_actions[action_id] = action

    def on_protection_update(
        self, *, action_id: str, quantity: int, signed: bool,
    ) -> PongTransition:
        if (
            type(action_id) is not str
            or not action_id
            or type(quantity) is not int
            or type(signed) is not bool
            or not signed
        ):
            return self._safety_block("PROTECTIVE_QUANTITY_UNVERIFIED")
        action = self._protective_actions.get(action_id)
        if (
            action is None
            or action.kind is not PongActionKind.REQUEST_PROTECTIVE_STOP
            or action.expected_direction is not self.actual_position
            or action.requested_total_quantity != quantity
            or quantity != self.actual_quantity
        ):
            return self._safety_block("PROTECTIVE_QUANTITY_MISMATCH")
        for prior_action_id in tuple(self._protective_actions):
            if prior_action_id != action_id:
                self._retire_reducing_action(
                    prior_action_id,
                    self._protective_actions,
                    self._protective_remaining,
                )
        self._protective_remaining[action_id] = quantity
        self.protected_quantity = quantity
        if self.state is PongState.EXIT_PENDING and self._reducing_actions:
            self._pending_protective_cancel_ids.add(action_id)
            cancellation = self._action(
                PongActionKind.CANCEL_PROTECTIVE_STOP,
                source_switch_id=action.source_switch_id,
                source_sequence=action.source_switch_sequence,
                expected=action.expected_direction,
                reason="CANCEL_REPROTECTION_BEFORE_REMAINING_EXIT",
                requested_quantity=0,
                snapshot_hash=action.snapshot_hash,
                target_action_id=action_id,
            )
            self.pending_action = cancellation
            return self._transition(
                "REPROTECTION_CANCEL_REQUESTED_BEFORE_REMAINING_EXIT",
                False,
                (cancellation,),
            )
        return self._transition("ACTUAL_QUANTITY_PROTECTED", False)

    def on_protective_stop_cancelled(self, *, action_id: str) -> PongTransition:
        """Confirm one protective cancellation before releasing a deferred exit."""
        if (
            type(action_id) is not str
            or action_id not in self._pending_protective_cancel_ids
            or action_id not in self._protective_actions
        ):
            return self._safety_block("UNEXPECTED_PROTECTIVE_CANCEL_CONFIRMATION")
        self._pending_protective_cancel_ids.remove(action_id)
        self._retire_reducing_action(
            action_id,
            self._protective_actions,
            self._protective_remaining,
        )
        if self._pending_protective_cancel_ids:
            return self._transition("PROTECTIVE_CANCEL_PARTIAL_CONFIRMATION", False)
        self.protected_quantity = 0
        deferred = self._deferred_exit_action
        self._deferred_exit_action = None
        if deferred is None:
            if self.actual_quantity == 0:
                self.pending_action = (
                    next(iter(self._reducing_actions.values()))
                    if self._reducing_actions else None
                )
                return self._transition(
                    "RESIDUAL_PROTECTIVE_CANCELLED_FLAT_LOCKOUT_RETAINED",
                    False,
                )
            covered_quantity = sum(
                self._reducing_remaining[reducing_id]
                for reducing_id, reducing_action in self._reducing_actions.items()
                if reducing_action.expected_direction is self.actual_position
            )
            if covered_quantity >= self.actual_quantity:
                self.pending_action = next(iter(self._reducing_actions.values()))
                return self._transition(
                    "PROTECTIVE_CANCELLED_EXISTING_EXIT_COVERAGE_RETAINED",
                    False,
                )
            return self._safety_block("DEFERRED_EXIT_IDENTITY_MISSING")
        if self.actual_quantity <= 0 or self.actual_position is PongDirection.FLAT:
            self.pending_action = None
            return self._safety_block("DEFERRED_EXIT_NO_LONGER_MATCHES_EXPOSURE")
        covered_quantity = sum(
            self._reducing_remaining[reducing_id]
            for reducing_id, reducing_action in self._reducing_actions.items()
            if reducing_action.expected_direction is self.actual_position
        )
        requested_quantity = (
            self.actual_quantity
            if deferred.kind is PongActionKind.REQUEST_EXIT
            else max(0, self.actual_quantity - covered_quantity)
        )
        if requested_quantity == 0:
            self.pending_action = (
                next(iter(self._reducing_actions.values()))
                if self._reducing_actions else None
            )
            return self._transition(
                "PROTECTIVE_CANCELLED_EXISTING_EXIT_COVERAGE_RETAINED",
                False,
            )
        released = self._action(
            deferred.kind,
            source_switch_id=deferred.source_switch_id,
            source_sequence=deferred.source_switch_sequence,
            expected=self.actual_position,
            reason=deferred.reason_code,
            requested_quantity=requested_quantity,
            score_quantity=deferred.score_authorized_quantity,
            support=deferred.position_side_support,
            blockers=deferred.blockers,
            snapshot_hash=deferred.snapshot_hash,
            bias_result_hash=deferred.bias_result_hash,
            reconciliation_hash=deferred.reconciliation_hash,
            admission_id=deferred.admission_id,
        )
        self._reducing_actions[released.action_id] = released
        self._reducing_remaining[released.action_id] = requested_quantity
        self.pending_action = released
        self.exit_target_quantity = max(
            self.exit_target_quantity,
            self.exit_filled_quantity + self.actual_quantity,
        )
        reason = (
            "SAFETY_EXIT_RELEASED_AFTER_PROTECTIVE_CANCEL"
            if released.kind is PongActionKind.REQUEST_SAFETY_EXIT
            else "POSITION_EXIT_RELEASED_AFTER_PROTECTIVE_CANCEL"
        )
        return self._transition(reason, False, (released,))

    def on_entry_rejected(self, *, action_id: str) -> PongTransition:
        if (
            self.pending_action is None
            or self.pending_action.action_id != action_id
            or self.state not in {PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT}
        ):
            return self._safety_block("UNEXPECTED_ENTRY_TERMINAL_EVENT")
        if self.actual_quantity:
            if self.protected_quantity != self.actual_quantity:
                return self.on_safety_exit(reason="PARTIAL_ENTRY_REMAINDER_REJECTED_UNPROTECTED")
            direction = self.actual_position
            self.pending_action = None
            self.entry_admitted = False
            self.entry_cancel_requested = False
            self.entry_target_quantity = self.actual_quantity
            self.state = PongState.LONG if direction is PongDirection.LONG else PongState.SHORT
            return self._transition("ENTRY_REMAINDER_REJECTED_POSITION_RETAINED", False)
        self._clear_entry_candidate()
        self.state = PongState.FLAT_WAIT_ALIGNMENT
        return self._transition("ENTRY_REJECTED_FLAT", False)

    def on_exit_fill(self, *, action_id: str, quantity: int, fill_id: str) -> PongTransition:
        return self._record_reducing_fill(
            action_id=action_id, quantity=quantity, fill_id=fill_id, safety=False,
        )

    def on_safety_exit_fill(self, *, action_id: str, quantity: int, fill_id: str) -> PongTransition:
        return self._record_reducing_fill(
            action_id=action_id, quantity=quantity, fill_id=fill_id, safety=True,
        )

    def on_protective_stop_fill(
        self, *, action_id: str, quantity: int, fill_id: str,
    ) -> PongTransition:
        return self._record_reducing_fill(
            action_id=action_id,
            quantity=quantity,
            fill_id=fill_id,
            protective=True,
        )

    def _record_reducing_fill(
        self,
        *,
        action_id: str,
        quantity: int,
        fill_id: str,
        safety: bool = False,
        protective: bool = False,
    ) -> PongTransition:
        if safety and protective:
            return self._safety_block("REDUCING_FILL_KIND_AMBIGUOUS")
        expected_kind = (
            PongActionKind.REQUEST_PROTECTIVE_STOP
            if protective
            else PongActionKind.REQUEST_SAFETY_EXIT
            if safety
            else PongActionKind.REQUEST_EXIT
        )
        fill_ids = self._safety_fill_ids if safety or protective else self._exit_fill_ids
        if (
            fill_id in self._entry_fill_ids
            or fill_id in self._exit_fill_ids
            or fill_id in self._safety_fill_ids
        ):
            return self._transition("DUPLICATE_REDUCING_FILL_IGNORED", False)
        active_actions = self._protective_actions if protective else self._reducing_actions
        active_remaining = (
            self._protective_remaining if protective else self._reducing_remaining
        )
        action = active_actions.get(action_id)
        retired = action is None
        if retired:
            action = self._retired_reducing_actions.get(action_id)
        remaining = 0 if retired else active_remaining.get(action_id)
        state_expected = self.state in {PongState.EXIT_PENDING, PongState.SAFETY_BLOCKED}
        if (
            not fill_id or type(quantity) is not int or quantity <= 0
            or action is None
            or remaining is None
            or action.kind is not expected_kind
            or action.expected_direction not in {PongDirection.LONG, PongDirection.SHORT}
        ):
            return self._safety_block("UNEXPECTED_REDUCING_FILL")
        fill_ids.add(fill_id)
        prior_position = self.actual_position
        prior_quantity = self.actual_quantity
        prior_signed = (
            prior_quantity
            if prior_position is PongDirection.LONG
            else -prior_quantity
            if prior_position is PongDirection.SHORT
            else 0
        )
        fill_delta = (
            -quantity
            if action.expected_direction is PongDirection.LONG
            else quantity
        )
        resulting_signed = prior_signed + fill_delta
        if not retired:
            if quantity >= remaining:
                self._retire_reducing_action(
                    action_id, active_actions, active_remaining,
                )
                self._pending_protective_cancel_ids.discard(action_id)
            else:
                active_remaining[action_id] = remaining - quantity
        if self.pending_action is not None and self.pending_action.action_id == action_id:
            self.pending_action = (
                next(iter(self._reducing_actions.values()))
                if self._reducing_actions else None
            )
        if resulting_signed > 0:
            self.actual_position = PongDirection.LONG
            self.actual_quantity = resulting_signed
        elif resulting_signed < 0:
            self.actual_position = PongDirection.SHORT
            self.actual_quantity = -resulting_signed
        else:
            self.actual_position = PongDirection.FLAT
            self.actual_quantity = 0
        protective_coverage = sum(
            self._protective_remaining[protective_id]
            for protective_id, protective_action in self._protective_actions.items()
            if protective_action.expected_direction is self.actual_position
        )
        self.protected_quantity = min(self.actual_quantity, protective_coverage)
        self.exit_filled_quantity += quantity
        unexpected = (
            protective
            or retired
            or (not protective and not state_expected)
            or prior_position is PongDirection.FLAT
            or action.expected_direction is not prior_position
            or quantity > remaining
            or quantity > prior_quantity
        )
        if unexpected:
            if protective:
                self._deferred_exit_action = None
            if self.actual_quantity == 0:
                self._pending_protective_cancel_ids.clear()
            return self.on_safety_exit(
                reason=(
                    "PROTECTIVE_STOP_FILLED"
                    if protective
                    else "UNEXPECTED_REDUCING_FILL"
                ),
            )
        if self.actual_quantity:
            protection = self._action(
                PongActionKind.REQUEST_PROTECTIVE_STOP,
                source_switch_id=action.source_switch_id,
                source_sequence=self.last_switch_sequence,
                expected=self.actual_position,
                reason="REPROTECT_REMAINING_ACTUAL_QUANTITY",
                requested_quantity=self.actual_quantity,
                snapshot_hash=None if self.last_snapshot is None else self.last_snapshot.result_hash,
            )
            self._protective_actions[protection.action_id] = protection
            self._protective_remaining[protection.action_id] = self.actual_quantity
            reason = (
                "SAFETY_EXIT_PARTIAL_FILL_PROTECTION_REQUIRED"
                if safety else "EXIT_PARTIAL_FILL_PROTECTION_REQUIRED"
            )
            return self._transition(reason, False, (protection,))
        self.protected_quantity = 0
        self.pending_action = (
            next(iter(self._reducing_actions.values()))
            if self._reducing_actions else None
        )
        reason = (
            "SAFETY_EXIT_FILL_AWAITING_SIGNED_FLAT_RECONCILIATION"
            if safety else "EXIT_FILL_AWAITING_SIGNED_FLAT_RECONCILIATION"
        )
        return self._transition(reason, False)

    def on_signed_flat_reconciliation(
        self,
        *,
        reconciliation: PongReconciliationProof,
        at: str,
    ) -> PongTransition:
        if type(reconciliation) is not PongReconciliationProof:
            return self._safety_block("PONG_RECONCILIATION_CONTRACT_INVALID")
        reconciliation_reason = reconciliation.validation_reason(
            at=at, policy=self.quantity_policy,
        )
        flat = (
            reconciliation_reason is None
            and reconciliation.position_quantity == 0
            and reconciliation.working_owned_orders == 0
            and not reconciliation.unresolved_execution
            and self.actual_quantity == 0
            and not self._reducing_actions
            and not self._protective_actions
            and not self._pending_protective_cancel_ids
            and self._deferred_exit_action is None
        )
        if self.state is PongState.SAFETY_BLOCKED:
            if flat and (
                self.pending_action is None
                or self.pending_action.kind is not PongActionKind.REQUEST_SAFETY_EXIT
                or self.exit_filled_quantity >= self.exit_target_quantity
            ):
                self.actual_position = PongDirection.FLAT
                self.pending_action = None
                self.entry_admitted = False
                self.entry_cancel_requested = False
                return self._transition("SAFETY_FLAT_CONFIRMED_LOCKOUT_RETAINED", False)
            return self._transition("SAFETY_FLAT_NOT_CONFIRMED", False)
        if reconciliation_reason is not None:
            return self._safety_block(reconciliation_reason)
        if (
            self.state is not PongState.EXIT_PENDING
            or not flat
            or self.exit_filled_quantity != self.exit_target_quantity
        ):
            return self._safety_block("EXIT_RECONCILIATION_INCOMPLETE")
        self.actual_position = PongDirection.FLAT
        self.pending_action = None
        self.entry_admitted = False
        self.entry_cancel_requested = False
        self.exit_target_quantity = 0
        self.exit_filled_quantity = 0
        self._reducing_actions.clear()
        self._reducing_remaining.clear()
        self.confirmed_flat_switch_sequence = self.last_switch_sequence
        self.waiting_leg_completed = False
        self.state = PongState.FLAT_WAIT_BEAT
        return self._transition("EXIT_COMPLETE_WAITING_ONE_FULL_LEG", False)

    def on_safety_exit(
        self, *, reason: str, event: DiscernmentSwitch | None = None,
    ) -> PongTransition:
        actions: list[PongAction] = []
        source_switch_id = event.event_id if event is not None else self.last_switch_id or "discernment-safety-without-switch"
        source_sequence = event.sequence if event is not None else self.last_switch_sequence
        if self.state in {PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT}:
            actions.append(self._action(
                PongActionKind.CANCEL_PENDING_ENTRY,
                source_switch_id=source_switch_id,
                source_sequence=source_sequence,
                expected=PongDirection.FLAT,
                reason=reason,
                requested_quantity=0,
            ))
        adverse_reducing_ids = tuple(
            action_id
            for action_id, action in self._reducing_actions.items()
            if action.expected_direction is not self.actual_position
        )
        if adverse_reducing_ids:
            self.recovery_blockers = tuple(dict.fromkeys((
                *self.recovery_blockers,
                *(f"PONG_ADVERSE_REDUCING_ACTION_UNRESOLVED:{action_id}"
                  for action_id in sorted(adverse_reducing_ids)),
            )))
        if self.actual_quantity:
            covered_quantity = sum(
                self._reducing_remaining[action_id]
                for action_id, action in self._reducing_actions.items()
                if action.expected_direction is self.actual_position
            )
            if covered_quantity > self.actual_quantity:
                self.recovery_blockers = tuple(dict.fromkeys((
                    *self.recovery_blockers,
                    "REDUCING_ORDER_COVERAGE_EXCEEDS_POSITION",
                )))
            uncovered_quantity = max(0, self.actual_quantity - covered_quantity)
            if self._protective_actions:
                if uncovered_quantity:
                    self._deferred_exit_action = self._action(
                        PongActionKind.REQUEST_SAFETY_EXIT,
                        source_switch_id=source_switch_id,
                        source_sequence=source_sequence,
                        expected=self.actual_position,
                        reason=reason,
                        requested_quantity=uncovered_quantity,
                        snapshot_hash=(
                            None if self.last_snapshot is None
                            else self.last_snapshot.result_hash
                        ),
                    )
                new_cancel_ids = tuple(
                    action_id
                    for action_id in sorted(self._protective_actions)
                    if action_id not in self._pending_protective_cancel_ids
                )
                self._pending_protective_cancel_ids.update(new_cancel_ids)
                for action_id in new_cancel_ids:
                    protective = self._protective_actions[action_id]
                    actions.append(self._action(
                        PongActionKind.CANCEL_PROTECTIVE_STOP,
                        source_switch_id=source_switch_id,
                        source_sequence=source_sequence,
                        expected=protective.expected_direction,
                        reason="CANCEL_PROTECTION_BEFORE_SAFETY_EXIT",
                        requested_quantity=0,
                        snapshot_hash=(
                            None if self.last_snapshot is None
                            else self.last_snapshot.result_hash
                        ),
                        target_action_id=action_id,
                    ))
                if actions:
                    self.pending_action = actions[-1]
            elif uncovered_quantity:
                action = self._action(
                    PongActionKind.REQUEST_SAFETY_EXIT,
                    source_switch_id=source_switch_id,
                    source_sequence=source_sequence,
                    expected=self.actual_position,
                    reason=reason,
                    requested_quantity=uncovered_quantity,
                    snapshot_hash=(
                        None if self.last_snapshot is None
                        else self.last_snapshot.result_hash
                    ),
                )
                self._reducing_actions[action.action_id] = action
                self._reducing_remaining[action.action_id] = uncovered_quantity
                self.pending_action = action
                actions.append(action)
            elif self._reducing_actions:
                self.pending_action = next(iter(self._reducing_actions.values()))
            self.exit_target_quantity = max(
                self.exit_target_quantity,
                self.exit_filled_quantity + self.actual_quantity,
            )
        else:
            if self._protective_actions:
                new_cancel_ids = tuple(
                    action_id
                    for action_id in sorted(self._protective_actions)
                    if action_id not in self._pending_protective_cancel_ids
                )
                self._pending_protective_cancel_ids.update(new_cancel_ids)
                for action_id in new_cancel_ids:
                    protective = self._protective_actions[action_id]
                    actions.append(self._action(
                        PongActionKind.CANCEL_PROTECTIVE_STOP,
                        source_switch_id=source_switch_id,
                        source_sequence=source_sequence,
                        expected=protective.expected_direction,
                        reason="CANCEL_RESIDUAL_PROTECTION_WHILE_FLAT",
                        requested_quantity=0,
                        target_action_id=action_id,
                    ))
            if not self._reducing_actions:
                self.pending_action = actions[-1] if actions else None
        self.entry_admitted = False
        if self.safety_reason is None:
            self.safety_reason = reason
        self.state = PongState.SAFETY_BLOCKED
        return self._transition("SAFETY_LOCKOUT_LATCHED", False, tuple(actions))

    def _safety_block(self, reason: str) -> PongTransition:
        if self.state is PongState.SAFETY_BLOCKED:
            if self.safety_reason is None:
                self.safety_reason = reason
            return self._transition("SAFETY_LOCKOUT_RETAINED", False)
        if self.actual_quantity or self.state in {
            PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT,
        }:
            return self.on_safety_exit(reason=reason)
        self.safety_reason = reason
        self.state = PongState.SAFETY_BLOCKED
        return self._transition(reason, False)

    def checkpoint(self) -> PongCheckpoint:
        """Return a hash-bound logical checkpoint without writing it anywhere."""
        snapshot = self.last_snapshot
        if snapshot is None:
            snapshot_hash, snapshot_domain, snapshot_revision = (
                self._restored_snapshot_proof
            )
        else:
            snapshot_hash = snapshot.result_hash
            snapshot_domain = snapshot.continuity_domain
            snapshot_revision = snapshot.evidence_revision
        checkpoint = PongCheckpoint(
            schema=PONG_CHECKPOINT_SCHEMA,
            profile=PONG_PROFILE_VERSION,
            mode=self.quantity_policy.mode,
            account_name=self.quantity_policy.account_name,
            account_class=self.quantity_policy.account_class,
            instrument=self.quantity_policy.instrument,
            canonical_contract=self.quantity_policy.canonical_contract,
            live_authority=self.quantity_policy.live_authority,
            signal_source=PONG_SIGNAL_SOURCE,
            quantity_policy_hash=self.quantity_policy.configuration_hash,
            discernment_policy_hash=self.discernment_policy.policy_hash,
            discernment_model_hash=self.discernment_policy.model_hash,
            state=self.state,
            shadow_direction=self.shadow_direction,
            actual_position=self.actual_position,
            actual_quantity=self.actual_quantity,
            protected_quantity=self.protected_quantity,
            last_switch_id=self.last_switch_id,
            last_switch_sequence=self.last_switch_sequence,
            last_consumed_switch_id=self.last_consumed_switch_id,
            last_snapshot_hash=snapshot_hash,
            last_snapshot_continuity_domain=snapshot_domain,
            last_snapshot_revision=snapshot_revision,
            pending_action=self.pending_action,
            reducing_actions=tuple(
                (self._reducing_actions[action_id], self._reducing_remaining[action_id])
                for action_id in sorted(self._reducing_actions)
            ),
            protective_actions=tuple(
                (
                    self._protective_actions[action_id],
                    self._protective_remaining[action_id],
                )
                for action_id in sorted(self._protective_actions)
            ),
            retired_reducing_actions=tuple(
                self._retired_reducing_actions[action_id]
                for action_id in sorted(self._retired_reducing_actions)
            ),
            deferred_exit_action=self._deferred_exit_action,
            pending_protective_cancel_ids=tuple(sorted(
                self._pending_protective_cancel_ids,
            )),
            confirmed_flat_switch_sequence=self.confirmed_flat_switch_sequence,
            waiting_leg_completed=self.waiting_leg_completed,
            entry_admitted=self.entry_admitted,
            entry_cancel_requested=self.entry_cancel_requested,
            entry_target_quantity=self.entry_target_quantity,
            entry_filled_quantity=self.entry_filled_quantity,
            exit_target_quantity=self.exit_target_quantity,
            exit_filled_quantity=self.exit_filled_quantity,
            daily_bias_result_hash=(
                self._restored_daily_bias_result_hash
                or self.daily_bias.result_hash
            ),
            safety_reason=self.safety_reason,
            recovery_blockers=self.recovery_blockers,
            score_authorized_quantity=self.score_authorized_quantity,
            risk_admitted_quantity=self.risk_admitted_quantity,
            quantity_blockers=self.quantity_blockers,
            entry_fill_ids=tuple(sorted(self._entry_fill_ids)),
            exit_fill_ids=tuple(sorted(self._exit_fill_ids)),
            safety_fill_ids=tuple(sorted(self._safety_fill_ids)),
            checkpoint_hash="",
        )
        return replace(checkpoint, checkpoint_hash=_checkpoint_hash(checkpoint))

    @classmethod
    def restore_checkpoint(
        cls,
        checkpoint: PongCheckpoint | str | bytes,
        *,
        current_snapshot: DiscernmentSnapshot | None,
        current_bias: DailyBiasResult,
        reconciliation: PongReconciliationProof,
        at: str,
        quantity_policy: PongQuantityPolicy = PONG_QUANTITY_POLICY,
        discernment_policy: DiscernmentPolicy = PONG_RESEARCH_POLICY,
    ) -> PongStateMachine:
        """Restore only verified flat continuity; all ambiguity remains locked out."""
        restored = cls(quantity_policy, discernment_policy)

        def latch_restore_block(reason: str) -> PongStateMachine:
            restored.state = PongState.SAFETY_BLOCKED
            if restored.safety_reason is None:
                restored.safety_reason = reason
            restored.recovery_blockers = tuple(dict.fromkeys(
                (*restored.recovery_blockers, reason),
            ))
            return restored

        if type(checkpoint) in {str, bytes}:
            try:
                checkpoint = deserialize_pong_checkpoint(checkpoint)
            except (TypeError, ValueError):
                return latch_restore_block("PONG_CHECKPOINT_INTEGRITY_FAILED")
        try:
            fill_id_groups = (
                checkpoint.entry_fill_ids,
                checkpoint.exit_fill_ids,
                checkpoint.safety_fill_ids,
            )
            all_fill_ids = tuple(
                fill_id for group in fill_id_groups for fill_id in group
            )
            snapshot_proof_values = (
                checkpoint.last_snapshot_hash,
                checkpoint.last_snapshot_continuity_domain,
                checkpoint.last_snapshot_revision,
            )
            integrity_valid = (
                type(checkpoint) is PongCheckpoint
                and checkpoint.schema == PONG_CHECKPOINT_SCHEMA
                and checkpoint.profile == PONG_PROFILE_VERSION
                and checkpoint.mode == quantity_policy.mode == PONG_MODE
                and checkpoint.account_name == quantity_policy.account_name
                and checkpoint.account_class == quantity_policy.account_class
                and checkpoint.instrument == quantity_policy.instrument
                and checkpoint.canonical_contract == quantity_policy.canonical_contract
                and checkpoint.live_authority
                == quantity_policy.live_authority
                == PONG_LIVE_AUTHORITY
                and checkpoint.signal_source == PONG_SIGNAL_SOURCE
                and checkpoint.quantity_policy_hash
                == quantity_policy.configuration_hash
                and checkpoint.discernment_policy_hash
                == discernment_policy.policy_hash
                and checkpoint.discernment_model_hash
                == discernment_policy.model_hash
                and checkpoint.checkpoint_hash == _checkpoint_hash(checkpoint)
                and type(checkpoint.state) is PongState
                and type(checkpoint.waiting_leg_completed) is bool
                and type(checkpoint.entry_admitted) is bool
                and type(checkpoint.entry_cancel_requested) is bool
                and (
                    checkpoint.shadow_direction is None
                    or type(checkpoint.shadow_direction) is PongDirection
                )
                and type(checkpoint.actual_position) is PongDirection
                and all(
                    type(value) is int and value >= 0
                    for value in (
                        checkpoint.actual_quantity,
                        checkpoint.protected_quantity,
                        checkpoint.last_switch_sequence,
                        checkpoint.entry_target_quantity,
                        checkpoint.entry_filled_quantity,
                        checkpoint.exit_target_quantity,
                        checkpoint.exit_filled_quantity,
                        checkpoint.score_authorized_quantity,
                        checkpoint.risk_admitted_quantity,
                    )
                )
                and (
                    checkpoint.confirmed_flat_switch_sequence is None
                    or (
                        type(checkpoint.confirmed_flat_switch_sequence) is int
                        and checkpoint.confirmed_flat_switch_sequence >= 0
                    )
                )
                and (
                    all(value is None for value in snapshot_proof_values)
                    or (
                        isinstance(checkpoint.last_snapshot_hash, str)
                        and len(checkpoint.last_snapshot_hash) == 64
                        and isinstance(
                            checkpoint.last_snapshot_continuity_domain, str,
                        )
                        and bool(checkpoint.last_snapshot_continuity_domain)
                        and type(checkpoint.last_snapshot_revision) is int
                        and checkpoint.last_snapshot_revision >= 0
                    )
                )
                and checkpoint.protected_quantity <= checkpoint.actual_quantity
                and (
                    checkpoint.actual_quantity > 0
                    or checkpoint.actual_position is PongDirection.FLAT
                )
                and (
                    checkpoint.actual_quantity == 0
                    or checkpoint.actual_position is not PongDirection.FLAT
                )
                and (
                    checkpoint.pending_action is None
                    or (
                        type(checkpoint.pending_action) is PongAction
                        and checkpoint.pending_action.quantity_policy_hash
                        == quantity_policy.configuration_hash
                        and checkpoint.pending_action.discernment_policy_hash
                        == discernment_policy.policy_hash
                        and checkpoint.pending_action.discernment_model_hash
                        == discernment_policy.model_hash
                    )
                )
                and all(
                    type(action) is PongAction
                    and type(remaining) is int
                    and remaining > 0
                    and remaining <= action.requested_total_quantity
                    and action.kind in {
                        PongActionKind.REQUEST_EXIT,
                        PongActionKind.REQUEST_SAFETY_EXIT,
                    }
                    and action.expected_direction in {
                        PongDirection.LONG, PongDirection.SHORT,
                    }
                    and action.quantity_policy_hash
                    == quantity_policy.configuration_hash
                    and action.discernment_policy_hash
                    == discernment_policy.policy_hash
                    and action.discernment_model_hash
                    == discernment_policy.model_hash
                    for action, remaining in checkpoint.reducing_actions
                )
                and len({action.action_id for action, _ in checkpoint.reducing_actions})
                == len(checkpoint.reducing_actions)
                and sum(
                    remaining
                    for action, remaining in checkpoint.reducing_actions
                    if action.expected_direction is checkpoint.actual_position
                ) <= checkpoint.actual_quantity
                and all(
                    type(action) is PongAction
                    and action.kind is PongActionKind.REQUEST_PROTECTIVE_STOP
                    and action.expected_direction in {
                        PongDirection.LONG, PongDirection.SHORT,
                    }
                    and type(remaining) is int
                    and 0 < remaining <= action.requested_total_quantity
                    and action.quantity_policy_hash
                    == quantity_policy.configuration_hash
                    and action.discernment_policy_hash
                    == discernment_policy.policy_hash
                    and action.discernment_model_hash
                    == discernment_policy.model_hash
                    for action, remaining in checkpoint.protective_actions
                )
                and all(
                    type(action) is PongAction
                    and action.kind in {
                        PongActionKind.REQUEST_PROTECTIVE_STOP,
                        PongActionKind.REQUEST_EXIT,
                        PongActionKind.REQUEST_SAFETY_EXIT,
                    }
                    and action.quantity_policy_hash
                    == quantity_policy.configuration_hash
                    and action.discernment_policy_hash
                    == discernment_policy.policy_hash
                    and action.discernment_model_hash
                    == discernment_policy.model_hash
                    for action in checkpoint.retired_reducing_actions
                )
                and len({
                    action.action_id for action, _ in checkpoint.protective_actions
                }) == len(checkpoint.protective_actions)
                and len({
                    action.action_id for action in checkpoint.retired_reducing_actions
                }) == len(checkpoint.retired_reducing_actions)
                and len({
                    *(action.action_id for action, _ in checkpoint.reducing_actions),
                    *(action.action_id for action, _ in checkpoint.protective_actions),
                    *(action.action_id for action in checkpoint.retired_reducing_actions),
                }) == (
                    len(checkpoint.reducing_actions)
                    + len(checkpoint.protective_actions)
                    + len(checkpoint.retired_reducing_actions)
                )
                and (
                    checkpoint.deferred_exit_action is None
                    or (
                        type(checkpoint.deferred_exit_action) is PongAction
                        and checkpoint.deferred_exit_action.kind in {
                            PongActionKind.REQUEST_EXIT,
                            PongActionKind.REQUEST_SAFETY_EXIT,
                        }
                        and checkpoint.deferred_exit_action.quantity_policy_hash
                        == quantity_policy.configuration_hash
                        and checkpoint.deferred_exit_action.discernment_policy_hash
                        == discernment_policy.policy_hash
                        and checkpoint.deferred_exit_action.discernment_model_hash
                        == discernment_policy.model_hash
                    )
                )
                and type(checkpoint.pending_protective_cancel_ids) is tuple
                and set(checkpoint.pending_protective_cancel_ids).issubset({
                    action.action_id for action, _ in checkpoint.protective_actions
                })
                and len(checkpoint.pending_protective_cancel_ids)
                == len(set(checkpoint.pending_protective_cancel_ids))
                and type(checkpoint.recovery_blockers) is tuple
                and len(set(checkpoint.recovery_blockers))
                == len(checkpoint.recovery_blockers)
                and all(
                    isinstance(reason, str) and bool(reason)
                    for reason in checkpoint.recovery_blockers
                )
                and type(checkpoint.quantity_blockers) is tuple
                and all(
                    isinstance(reason, str) and bool(reason)
                    for reason in checkpoint.quantity_blockers
                )
                and all(type(group) is tuple for group in fill_id_groups)
                and all(
                    isinstance(fill_id, str) and bool(fill_id)
                    for fill_id in all_fill_ids
                )
                and len(all_fill_ids) == len(set(all_fill_ids))
                and isinstance(checkpoint.daily_bias_result_hash, str)
                and len(checkpoint.daily_bias_result_hash) == 64
                and (
                    checkpoint.state is not PongState.SAFETY_BLOCKED
                    or isinstance(checkpoint.safety_reason, str)
                    and bool(checkpoint.safety_reason)
                )
                and (
                    not checkpoint.recovery_blockers
                    or checkpoint.state is PongState.SAFETY_BLOCKED
                )
            )
        except (AttributeError, TypeError, ValueError):
            integrity_valid = False
        if not integrity_valid:
            return latch_restore_block("PONG_CHECKPOINT_INTEGRITY_FAILED")

        restored.state = checkpoint.state
        restored.shadow_direction = checkpoint.shadow_direction
        restored.actual_position = checkpoint.actual_position
        restored.actual_quantity = checkpoint.actual_quantity
        restored.protected_quantity = checkpoint.protected_quantity
        restored.last_switch_id = checkpoint.last_switch_id
        restored.last_switch_sequence = checkpoint.last_switch_sequence
        restored.last_consumed_switch_id = checkpoint.last_consumed_switch_id
        restored._restored_snapshot_proof = (
            checkpoint.last_snapshot_hash,
            checkpoint.last_snapshot_continuity_domain,
            checkpoint.last_snapshot_revision,
        )
        restored.pending_action = checkpoint.pending_action
        restored._reducing_actions = {
            action.action_id: action for action, _ in checkpoint.reducing_actions
        }
        restored._reducing_remaining = {
            action.action_id: remaining
            for action, remaining in checkpoint.reducing_actions
        }
        restored._protective_actions = {
            action.action_id: action for action, _ in checkpoint.protective_actions
        }
        restored._protective_remaining = {
            action.action_id: remaining
            for action, remaining in checkpoint.protective_actions
        }
        restored._retired_reducing_actions = {
            action.action_id: action
            for action in checkpoint.retired_reducing_actions
        }
        restored._deferred_exit_action = checkpoint.deferred_exit_action
        restored._pending_protective_cancel_ids = set(
            checkpoint.pending_protective_cancel_ids,
        )
        restored.confirmed_flat_switch_sequence = checkpoint.confirmed_flat_switch_sequence
        restored.waiting_leg_completed = checkpoint.waiting_leg_completed
        restored.entry_admitted = checkpoint.entry_admitted
        restored.entry_cancel_requested = checkpoint.entry_cancel_requested
        restored.entry_target_quantity = checkpoint.entry_target_quantity
        restored.entry_filled_quantity = checkpoint.entry_filled_quantity
        restored.exit_target_quantity = checkpoint.exit_target_quantity
        restored.exit_filled_quantity = checkpoint.exit_filled_quantity
        restored.safety_reason = checkpoint.safety_reason
        restored.recovery_blockers = checkpoint.recovery_blockers
        restored._restored_daily_bias_result_hash = (
            checkpoint.daily_bias_result_hash
        )
        restored.score_authorized_quantity = checkpoint.score_authorized_quantity
        restored.risk_admitted_quantity = checkpoint.risk_admitted_quantity
        restored.quantity_blockers = checkpoint.quantity_blockers
        restored._entry_fill_ids = set(checkpoint.entry_fill_ids)
        restored._exit_fill_ids = set(checkpoint.exit_fill_ids)
        restored._safety_fill_ids = set(checkpoint.safety_fill_ids)

        if (
            type(current_bias) is not DailyBiasResult
            or current_bias.result_hash != checkpoint.daily_bias_result_hash
        ):
            return latch_restore_block(
                "PONG_RESTART_DAILY_BIAS_PROOF_MISMATCH",
            )
        restored.daily_bias = current_bias
        restored._restored_daily_bias_result_hash = None

        if checkpoint.last_snapshot_hash is None:
            snapshot_matches = current_snapshot is None
        else:
            snapshot_matches = (
                type(current_snapshot) is DiscernmentSnapshot
                and restored._snapshot_reason(current_snapshot, at=at) is None
                and current_snapshot.result_hash == checkpoint.last_snapshot_hash
                and current_snapshot.continuity_domain
                == checkpoint.last_snapshot_continuity_domain
                and current_snapshot.evidence_revision
                == checkpoint.last_snapshot_revision
            )
        if not snapshot_matches:
            return latch_restore_block(
                "PONG_RESTART_DISCERNMENT_PROOF_MISMATCH",
            )
        restored.last_snapshot = current_snapshot

        reconciliation_reason = (
            "PONG_RECONCILIATION_CONTRACT_INVALID"
            if type(reconciliation) is not PongReconciliationProof
            else reconciliation.validation_reason(at=at, policy=quantity_policy)
        )
        if (
            reconciliation_reason is not None
            or reconciliation.position_quantity != checkpoint.actual_quantity
            or reconciliation.working_owned_orders != 0
            or reconciliation.unresolved_execution
        ):
            return latch_restore_block("PONG_RESTART_RECONCILIATION_REQUIRED")

        restart_blocked = False
        if checkpoint.pending_action is not None:
            latch_restore_block("PONG_RESTART_PENDING_ACTION_UNRESOLVED")
            restart_blocked = True
        if (
            checkpoint.protective_actions
            or checkpoint.pending_protective_cancel_ids
            or checkpoint.deferred_exit_action is not None
        ):
            latch_restore_block("PONG_RESTART_PROTECTIVE_ACTION_UNRESOLVED")
            restart_blocked = True
        if checkpoint.actual_quantity or checkpoint.actual_position is not PongDirection.FLAT:
            latch_restore_block("PONG_RESTART_NONFLAT_RECOVERY_REQUIRED")
            restart_blocked = True
        if checkpoint.state not in {
            PongState.FLAT_WAIT_ALIGNMENT,
            PongState.FLAT_WAIT_BEAT,
            PongState.SAFETY_BLOCKED,
        }:
            latch_restore_block("PONG_RESTART_NONRESUMABLE_STATE")
            restart_blocked = True
        if restart_blocked:
            return restored
        if checkpoint.state is PongState.SAFETY_BLOCKED:
            restored.safety_reason = (
                checkpoint.safety_reason or "PONG_RESTART_SAFETY_LOCKOUT_RETAINED"
            )
        return restored

    def _transition(
        self,
        reason: str,
        consumed: bool,
        actions: tuple[PongAction, ...] = (),
    ) -> PongTransition:
        return PongTransition(self.state, actions, reason, consumed)

    def status(self) -> dict[str, object]:
        snapshot = self.last_snapshot
        checkpoint = self.checkpoint()
        blockers: list[str] = list(self.quantity_blockers)
        blockers.extend(self.recovery_blockers)
        blockers.extend(("PONG_EXECUTION_DISABLED", "PONG_NOT_REGISTERED"))
        if not DISCERNMENT_PRODUCTION_CONFIGURED:
            blockers.append("DISCERNMENT_PRODUCTION_NOT_CONFIGURED")
        if snapshot is None:
            blockers.append("DISCERNMENT_UNAVAILABLE")
        else:
            if not snapshot.valid:
                blockers.extend(snapshot.reason_codes)
            if snapshot.calibration_status == EXPERIMENTAL_DIRECTIONAL_SUPPORT:
                blockers.append("DISCERNMENT_NOT_CALIBRATED")
            if not self.discernment_policy.reachability.strong_long_reachable:
                blockers.append(STRONG_THRESHOLD_UNREACHABLE)
        if self.safety_reason:
            blockers.append(self.safety_reason)
        if (
            self.quantity_policy.maximum_position_risk_dollars
            < self.quantity_policy.nominal_risk_per_contract * 2
        ):
            blockers.append("TWO_MNQ_EXCEEDS_POSITION_RISK_LIMIT")
        if self.quantity_policy.native_maximum_quantity < 2:
            blockers.append("PONG_NATIVE_QUANTITY_CAPABILITY_UNPROVEN")
        return {
            "schema": "beelzebub-pong-discernment-status-v1",
            "profile": PONG_PROFILE_VERSION,
            "mode": self.quantity_policy.mode,
            "account_name": self.quantity_policy.account_name,
            "account_class": self.quantity_policy.account_class,
            "live_authority": self.quantity_policy.live_authority,
            "execution_enabled": PONG_EXECUTION_ENABLED,
            "profile_registration": PONG_PROFILE_REGISTRATION,
            "signal_source": PONG_SIGNAL_SOURCE,
            "instrument": PONG_INSTRUMENT,
            "canonical_contract": PONG_CONTRACT,
            "state": self.state.value,
            "latched_logical_direction": None if self.shadow_direction is None else self.shadow_direction.value,
            "actual_position": self.actual_position.value,
            "actual_quantity": self.actual_quantity,
            "protected_quantity": self.protected_quantity,
            "effective_long_discernment": None if snapshot is None else snapshot.effective_long_score,
            "effective_short_discernment": (
                None if snapshot is None or snapshot.effective_long_score is None
                else 1 - snapshot.effective_long_score
            ),
            "discernment_validity": "UNAVAILABLE" if snapshot is None else snapshot.validity.value,
            "discernment_calibration_status": None if snapshot is None else snapshot.calibration_status,
            "volume_regime": "UNAVAILABLE" if snapshot is None else snapshot.volume_regime.value,
            "last_snapshot_hash": None if snapshot is None else snapshot.result_hash,
            "checkpoint_snapshot_hash": checkpoint.last_snapshot_hash,
            "last_switch_id": self.last_switch_id,
            "last_switch_sequence": self.last_switch_sequence,
            "last_consumed_switch_id": self.last_consumed_switch_id,
            "logical_checkpoint_hash": checkpoint.checkpoint_hash,
            "pending_action": None if self.pending_action is None else self.pending_action.kind.value,
            "pending_action_id": (
                None if self.pending_action is None else self.pending_action.action_id
            ),
            "working_reducing_actions": [
                {
                    "action_id": action_id,
                    "kind": self._reducing_actions[action_id].kind.value,
                    "remaining_quantity": self._reducing_remaining[action_id],
                }
                for action_id in sorted(self._reducing_actions)
            ],
            "working_protective_actions": [
                {
                    "action_id": action_id,
                    "remaining_quantity": self._protective_remaining[action_id],
                }
                for action_id in sorted(self._protective_actions)
            ],
            "retired_reducing_action_ids": sorted(
                self._retired_reducing_actions,
            ),
            "pending_protective_cancel_ids": sorted(
                self._pending_protective_cancel_ids,
            ),
            "score_authorized_quantity": self.score_authorized_quantity,
            "risk_admitted_quantity": self.risk_admitted_quantity,
            "entry_target_quantity": self.entry_target_quantity,
            "entry_filled_quantity": self.entry_filled_quantity,
            "entry_cancel_requested": self.entry_cancel_requested,
            "waiting_state": self.state is PongState.FLAT_WAIT_BEAT,
            "confirmed_flat_switch_sequence": self.confirmed_flat_switch_sequence,
            "waiting_leg_completed": self.waiting_leg_completed,
            "daily_bias_source_id": self.daily_bias.source_id,
            "daily_bias_configured": self.daily_bias.configured,
            "daily_bias_direction": self.daily_bias.direction.value,
            "daily_bias_confidence": self.daily_bias.confidence.value,
            "daily_bias_result_hash": self.daily_bias.result_hash,
            "checkpoint_daily_bias_result_hash": checkpoint.daily_bias_result_hash,
            "recovery_blockers": list(self.recovery_blockers),
            "default_entry_blocker": "DISCERNMENT_PRODUCTION_NOT_CONFIGURED",
            "blockers": list(dict.fromkeys(blockers)),
            "safety_reason": self.safety_reason,
            "live_capital": self.quantity_policy.live_capital,
        }
