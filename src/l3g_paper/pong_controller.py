"""Durable operator overlay for the fixed 2026-09-11 Pong experiment.

This module is deliberately independent of Pong strategy semantics.  It owns
only a fixed entry fence/flat window, a fixed termination deadline, and a
short controller-health lease.  Risk-reducing commands remain available in
every phase.  Native execution and ledger integration are supplied by the
owning runtime through the small runner callbacks at the bottom of the file.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import hashlib
import json
import os
from pathlib import Path
import time
import uuid
from typing import Callable
from zoneinfo import ZoneInfo


PONG_CONTROLLER_SCHEMA = "beelzebub-pong-bounded-controller-v1"
PONG_CONTROLLER_ACCOUNT = "Sim101"
PONG_CONTROLLER_ACCOUNT_CLASS = "LOCAL_SIMULATION"
PONG_CONTROLLER_INSTRUMENT = "MNQ SEP26"
PONG_CONTROLLER_CONTRACT = "MNQU6"
PONG_CONTROLLER_LIVE_AUTHORITY = "DISARMED"
PONG_CONTROLLER_HEARTBEAT_SECONDS = 5
_DENVER = ZoneInfo("America/Denver")
_NEW_YORK = ZoneInfo("America/New_York")


def _wire(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Pong controller timestamps must be timezone-aware.")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Pong controller timestamps must include an offset.")
    return parsed.astimezone(timezone.utc)


def _fixed_local(hour: int, minute: int) -> datetime:
    return datetime(2026, 9, 11, hour, minute, tzinfo=_DENVER)


PONG_BLACKOUT_START_LOCAL = _fixed_local(7, 30)
PONG_BLACKOUT_END_LOCAL = _fixed_local(7, 35)
PONG_EXPERIMENT_END_LOCAL = _fixed_local(8, 35)
PONG_BLACKOUT_START_UTC = _wire(PONG_BLACKOUT_START_LOCAL)
PONG_BLACKOUT_END_UTC = _wire(PONG_BLACKOUT_END_LOCAL)
PONG_EXPERIMENT_END_UTC = _wire(PONG_EXPERIMENT_END_LOCAL)
PONG_EXPERIMENT_END_NEW_YORK = PONG_EXPERIMENT_END_LOCAL.astimezone(
    _NEW_YORK,
).isoformat()


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


class PongControllerIntegrityError(RuntimeError):
    """The durable controller state cannot be trusted."""


class PongControllerLeaseError(RuntimeError):
    """Another process still owns the unexpired controller lease."""


class PongControllerPhase(StrEnum):
    PREPARED = "PREPARED"
    RUNNING_PRE_BLACKOUT = "RUNNING_PRE_BLACKOUT"
    BLACKOUT_FENCING = "BLACKOUT_FENCING"
    BLACKOUT_FLATTENING = "BLACKOUT_FLATTENING"
    BLACKOUT_RECONCILING = "BLACKOUT_RECONCILING"
    BLACKOUT_FLAT = "BLACKOUT_FLAT"
    RESUME_BLOCKED = "RESUME_BLOCKED"
    RUNNING_POST_BLACKOUT = "RUNNING_POST_BLACKOUT"
    TERMINATION_FENCING = "TERMINATION_FENCING"
    TERMINATION_FLATTENING = "TERMINATION_FLATTENING"
    TERMINATION_RECONCILING = "TERMINATION_RECONCILING"
    TERMINATION_SEALING = "TERMINATION_SEALING"
    TERMINATION_STOPPING = "TERMINATION_STOPPING"
    TERMINATED = "TERMINATED"
    FAILED_SAFE = "FAILED_SAFE"


class PongControllerCommandKind(StrEnum):
    FENCE_NEW_ENTRIES = "FENCE_NEW_ENTRIES"
    CANCEL_OWNED_PENDING_ENTRIES = "CANCEL_OWNED_PENDING_ENTRIES"
    CANCEL_REMAINING_OWNED_ORDERS = "CANCEL_REMAINING_OWNED_ORDERS"
    CONTROLLED_FLATTEN = "CONTROLLED_FLATTEN"
    REQUEST_SIGNED_FLAT_RECONCILIATION = "REQUEST_SIGNED_FLAT_RECONCILIATION"
    RELEASE_ENTRY_FENCE = "RELEASE_ENTRY_FENCE"
    SEAL_EVIDENCE = "SEAL_EVIDENCE"
    STOP_OPERATION = "STOP_OPERATION"


class PongControllerCommandStatus(StrEnum):
    REQUESTED = "REQUESTED"
    DISPATCHED = "DISPATCHED"
    ACKNOWLEDGED = "ACKNOWLEDGED"


@dataclass(frozen=True)
class PongControllerCommand:
    command_id: str
    sequence: int
    kind: PongControllerCommandKind
    reason: str
    requested_at: str
    status: PongControllerCommandStatus = PongControllerCommandStatus.REQUESTED

    def __post_init__(self) -> None:
        _utc(self.requested_at)
        if (
            not self.command_id.startswith("pong-controller-command-")
            or type(self.sequence) is not int
            or self.sequence <= 0
            or type(self.kind) is not PongControllerCommandKind
            or not self.reason
            or type(self.status) is not PongControllerCommandStatus
        ):
            raise ValueError("Pong controller command is invalid.")


@dataclass(frozen=True)
class PongControllerObservation:
    observed_at: str
    entry_fence_active: bool
    position_quantity: int
    working_owned_orders: int
    working_entry_orders: int
    unresolved_execution: bool
    reconciliation_current: bool
    protection_current: bool
    entry_gates_healthy: bool
    safety_lockout: bool
    runtime_active: bool
    evidence_sealed: bool
    signature_verified: bool = True
    account_name: str = PONG_CONTROLLER_ACCOUNT
    account_class: str = PONG_CONTROLLER_ACCOUNT_CLASS
    instrument: str = PONG_CONTROLLER_INSTRUMENT
    canonical_contract: str = PONG_CONTROLLER_CONTRACT
    live_authority: str = PONG_CONTROLLER_LIVE_AUTHORITY

    def __post_init__(self) -> None:
        _utc(self.observed_at)
        booleans = (
            self.entry_fence_active, self.unresolved_execution,
            self.reconciliation_current, self.protection_current,
            self.entry_gates_healthy, self.safety_lockout,
            self.runtime_active, self.evidence_sealed,
            self.signature_verified,
        )
        if any(type(value) is not bool for value in booleans):
            raise ValueError("Pong controller observation flags must be booleans.")
        if (
            type(self.position_quantity) is not int
            or abs(self.position_quantity) > 1
            or type(self.working_owned_orders) is not int
            or type(self.working_entry_orders) is not int
            or min(self.working_owned_orders, self.working_entry_orders) < 0
            or self.working_entry_orders > self.working_owned_orders
        ):
            raise ValueError("Pong controller native quantities are invalid.")
        if (
            self.account_name,
            self.account_class,
            self.instrument,
            self.canonical_contract,
            self.live_authority,
        ) != (
            PONG_CONTROLLER_ACCOUNT,
            PONG_CONTROLLER_ACCOUNT_CLASS,
            PONG_CONTROLLER_INSTRUMENT,
            PONG_CONTROLLER_CONTRACT,
            PONG_CONTROLLER_LIVE_AUTHORITY,
        ):
            raise ValueError("Pong controller observation scope is invalid.")

    @property
    def clean_flat(self) -> bool:
        return (
            self.position_quantity == 0
            and self.working_owned_orders == 0
            and self.working_entry_orders == 0
            and not self.unresolved_execution
            and self.reconciliation_current
        )

    @property
    def admission_health_reason(self) -> str | None:
        if not self.signature_verified:
            return "PONG_CONTROLLER_OBSERVATION_SIGNATURE_INVALID"
        if self.safety_lockout:
            return "PONG_SAFETY_LOCKOUT_RETAINED"
        if not self.entry_gates_healthy:
            return "PONG_ENTRY_GATES_UNHEALTHY"
        if not self.runtime_active:
            return "PONG_RUNTIME_NOT_ACTIVE"
        return None

    @property
    def observation_hash(self) -> str:
        return _digest(asdict(self))


@dataclass(frozen=True)
class PongEntryFenceDecision:
    allowed: bool
    reason: str
    controller_id: str | None
    controller_phase: str | None
    state_hash: str | None
    risk_reduction_allowed: bool = True


@dataclass(frozen=True)
class PongControllerState:
    schema: str
    experiment_id: str
    controller_id: str
    process_instance_id: str
    pid: int
    state_path: str
    created_at: str
    updated_at: str
    heartbeat_valid_until: str
    blackout_start_local: str
    blackout_end_local: str
    experiment_end_local: str
    blackout_start_utc: str
    blackout_end_utc: str
    experiment_end_utc: str
    experiment_end_new_york: str
    phase: PongControllerPhase
    command_sequence: int
    pending_command: PongControllerCommand | None
    acknowledged_command_ids: tuple[str, ...]
    blackout_switch_high_water: int
    last_blackout_switch_id: str | None
    last_observation_hash: str | None
    last_error: str | None
    transitions: tuple[str, ...]
    state_hash: str


_STATE_KEYS = frozenset(PongControllerState.__dataclass_fields__)
_COMMAND_KEYS = frozenset(PongControllerCommand.__dataclass_fields__)


def _state_payload(state: PongControllerState) -> dict[str, object]:
    payload = asdict(state)
    payload["phase"] = state.phase.value
    if state.pending_command is not None:
        payload["pending_command"]["kind"] = state.pending_command.kind.value  # type: ignore[index]
        payload["pending_command"]["status"] = state.pending_command.status.value  # type: ignore[index]
    payload.pop("state_hash")
    return payload


def _state_hash(state: PongControllerState) -> str:
    return _digest(_state_payload(state))


def _decode_state(raw: bytes, expected_path: Path) -> PongControllerState:
    try:
        payload = json.loads(raw)
        if type(payload) is not dict or set(payload) != _STATE_KEYS:
            raise ValueError("state fields")
        pending = payload["pending_command"]
        if pending is not None:
            if type(pending) is not dict or set(pending) != _COMMAND_KEYS:
                raise ValueError("command fields")
            pending = PongControllerCommand(
                **{
                    **pending,
                    "kind": PongControllerCommandKind(pending["kind"]),
                    "status": PongControllerCommandStatus(pending["status"]),
                },
            )
        state = PongControllerState(
            **{
                **payload,
                "phase": PongControllerPhase(payload["phase"]),
                "pending_command": pending,
                "acknowledged_command_ids": tuple(payload["acknowledged_command_ids"]),
                "transitions": tuple(payload["transitions"]),
            },
        )
        if raw != _canonical({**_state_payload(state), "state_hash": state.state_hash}):
            raise ValueError("non-canonical state")
        if state.state_hash != _state_hash(state):
            raise ValueError("state hash")
        if Path(state.state_path).resolve() != expected_path.resolve():
            raise ValueError("state path")
        if (
            state.schema != PONG_CONTROLLER_SCHEMA
            or state.blackout_start_local != PONG_BLACKOUT_START_LOCAL.isoformat()
            or state.blackout_end_local != PONG_BLACKOUT_END_LOCAL.isoformat()
            or state.experiment_end_local != PONG_EXPERIMENT_END_LOCAL.isoformat()
            or state.blackout_start_utc != PONG_BLACKOUT_START_UTC
            or state.blackout_end_utc != PONG_BLACKOUT_END_UTC
            or state.experiment_end_utc != PONG_EXPERIMENT_END_UTC
            or state.experiment_end_new_york != PONG_EXPERIMENT_END_NEW_YORK
            or not state.experiment_id
            or not state.controller_id.startswith("pong-controller-")
            or not state.process_instance_id.startswith("pong-controller-process-")
            or type(state.pid) is not int
            or state.pid <= 0
            or type(state.command_sequence) is not int
            or state.command_sequence < 0
            or type(state.blackout_switch_high_water) is not int
            or state.blackout_switch_high_water < -1
            or len(state.acknowledged_command_ids)
            != len(set(state.acknowledged_command_ids))
        ):
            raise ValueError("state identity")
        for value in (
            state.created_at, state.updated_at, state.heartbeat_valid_until,
            state.blackout_start_utc, state.blackout_end_utc,
            state.experiment_end_utc,
        ):
            _utc(value)
        return state
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise PongControllerIntegrityError(
            "PONG_CONTROLLER_STATE_INTEGRITY_FAILED",
        ) from error


def _atomic_write(path: Path, state: PongControllerState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    final = replace(state, state_hash=_state_hash(state))
    raw = _canonical({**_state_payload(final), "state_hash": final.state_hash})
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


class PongBoundedController:
    """Restart-safe state machine for the fixed NY-open scheduling overlay."""

    def __init__(self, path: Path, state: PongControllerState) -> None:
        self.path = path.resolve()
        self.state = state

    @classmethod
    def open(
        cls,
        path: str | Path,
        *,
        experiment_id: str,
        at: str,
        pid: int | None = None,
        process_instance_id: str | None = None,
    ) -> "PongBoundedController":
        target = Path(path).resolve()
        now = _utc(at)
        use_pid = os.getpid() if pid is None else pid
        if type(use_pid) is not int or use_pid <= 0 or not experiment_id:
            raise ValueError("Pong controller identity is invalid.")
        process_id = process_instance_id or "pong-controller-process-" + uuid.uuid4().hex
        if target.exists():
            state = _decode_state(target.read_bytes(), target)
            if state.experiment_id != experiment_id:
                raise PongControllerIntegrityError(
                    "PONG_CONTROLLER_EXPERIMENT_ID_MISMATCH",
                )
            same_owner = state.process_instance_id == process_id and state.pid == use_pid
            if not same_owner and now <= _utc(state.heartbeat_valid_until):
                raise PongControllerLeaseError("PONG_CONTROLLER_LEASE_STILL_ACTIVE")
            state = replace(
                state,
                process_instance_id=process_id,
                pid=use_pid,
                updated_at=_wire(now),
                heartbeat_valid_until=_wire(
                    now + timedelta(seconds=PONG_CONTROLLER_HEARTBEAT_SECONDS),
                ),
                transitions=tuple((*state.transitions, "CONTROLLER_PROCESS_RESTORED"))[-32:],
                state_hash="",
            )
        else:
            created = _wire(now)
            state = PongControllerState(
                schema=PONG_CONTROLLER_SCHEMA,
                experiment_id=experiment_id,
                controller_id="pong-controller-" + uuid.uuid4().hex,
                process_instance_id=process_id,
                pid=use_pid,
                state_path=str(target),
                created_at=created,
                updated_at=created,
                heartbeat_valid_until=_wire(
                    now + timedelta(seconds=PONG_CONTROLLER_HEARTBEAT_SECONDS),
                ),
                blackout_start_local=PONG_BLACKOUT_START_LOCAL.isoformat(),
                blackout_end_local=PONG_BLACKOUT_END_LOCAL.isoformat(),
                experiment_end_local=PONG_EXPERIMENT_END_LOCAL.isoformat(),
                blackout_start_utc=PONG_BLACKOUT_START_UTC,
                blackout_end_utc=PONG_BLACKOUT_END_UTC,
                experiment_end_utc=PONG_EXPERIMENT_END_UTC,
                experiment_end_new_york=PONG_EXPERIMENT_END_NEW_YORK,
                phase=PongControllerPhase.PREPARED,
                command_sequence=0,
                pending_command=None,
                acknowledged_command_ids=(),
                blackout_switch_high_water=-1,
                last_blackout_switch_id=None,
                last_observation_hash=None,
                last_error=None,
                transitions=("CONTROLLER_PREPARED",),
                state_hash="",
            )
        controller = cls(target, state)
        controller._save()
        return controller

    @classmethod
    def load(cls, path: str | Path) -> "PongBoundedController":
        target = Path(path).resolve()
        return cls(target, _decode_state(target.read_bytes(), target))

    def _save(self) -> None:
        self.state = replace(self.state, state_hash=_state_hash(self.state))
        _atomic_write(self.path, self.state)

    def _heartbeat(self, now: datetime) -> None:
        self.state = replace(
            self.state,
            updated_at=_wire(now),
            heartbeat_valid_until=_wire(
                now + timedelta(seconds=PONG_CONTROLLER_HEARTBEAT_SECONDS),
            ),
            state_hash="",
        )

    def _phase(self, phase: PongControllerPhase, reason: str) -> None:
        if self.state.phase is phase:
            return
        self.state = replace(
            self.state,
            phase=phase,
            transitions=tuple((*self.state.transitions, f"{phase.value}:{reason}"))[-32:],
            state_hash="",
        )

    def _request(
        self,
        kind: PongControllerCommandKind,
        reason: str,
        now: datetime,
    ) -> PongControllerCommand:
        sequence = self.state.command_sequence + 1
        identity = {
            "controller_id": self.state.controller_id,
            "sequence": sequence,
            "kind": kind.value,
            "reason": reason,
        }
        command = PongControllerCommand(
            command_id="pong-controller-command-" + _digest(identity)[:32],
            sequence=sequence,
            kind=kind,
            reason=reason,
            requested_at=_wire(now),
        )
        self.state = replace(
            self.state, command_sequence=sequence, pending_command=command,
            state_hash="",
        )
        return command

    @staticmethod
    def _command_settled(
        command: PongControllerCommand,
        observation: PongControllerObservation,
    ) -> bool:
        return {
            PongControllerCommandKind.FENCE_NEW_ENTRIES: observation.entry_fence_active,
            PongControllerCommandKind.CANCEL_OWNED_PENDING_ENTRIES: observation.working_entry_orders == 0,
            PongControllerCommandKind.CANCEL_REMAINING_OWNED_ORDERS: observation.working_owned_orders == 0,
            PongControllerCommandKind.CONTROLLED_FLATTEN: observation.position_quantity == 0,
            PongControllerCommandKind.REQUEST_SIGNED_FLAT_RECONCILIATION: observation.clean_flat,
            PongControllerCommandKind.RELEASE_ENTRY_FENCE: not observation.entry_fence_active,
            PongControllerCommandKind.SEAL_EVIDENCE: observation.evidence_sealed,
            PongControllerCommandKind.STOP_OPERATION: not observation.runtime_active,
        }[command.kind]

    def mark_dispatched(self, command_id: str, *, at: str) -> None:
        now = _utc(at)
        pending = self.state.pending_command
        if pending is None or pending.command_id != command_id:
            raise ValueError("PONG_CONTROLLER_COMMAND_NOT_PENDING")
        if pending.status is not PongControllerCommandStatus.REQUESTED:
            return
        self._heartbeat(now)
        self.state = replace(
            self.state,
            pending_command=replace(
                pending, status=PongControllerCommandStatus.DISPATCHED,
            ),
            state_hash="",
        )
        self._save()

    def acknowledge_command(
        self,
        command_id: str,
        *,
        at: str,
        accepted: bool,
        reason: str = "",
    ) -> None:
        now = _utc(at)
        pending = self.state.pending_command
        if pending is None or pending.command_id != command_id:
            if command_id in self.state.acknowledged_command_ids:
                return
            raise ValueError("PONG_CONTROLLER_COMMAND_NOT_PENDING")
        self._heartbeat(now)
        if not accepted:
            failure = reason or "PONG_CONTROLLER_COMMAND_REJECTED"
            self.state = replace(
                self.state,
                pending_command=None,
                last_error=failure,
                acknowledged_command_ids=tuple((
                    *self.state.acknowledged_command_ids, command_id,
                ))[-64:],
                state_hash="",
            )
            self._phase(PongControllerPhase.FAILED_SAFE, failure)
        else:
            self.state = replace(
                self.state,
                pending_command=replace(
                    pending, status=PongControllerCommandStatus.ACKNOWLEDGED,
                ),
                acknowledged_command_ids=tuple((
                    *self.state.acknowledged_command_ids, command_id,
                ))[-64:],
                state_hash="",
            )
        self._save()

    def _ensure_flat(
        self,
        observation: PongControllerObservation,
        now: datetime,
        *,
        prefix: str,
        fencing_phase: PongControllerPhase,
        flattening_phase: PongControllerPhase,
        reconciling_phase: PongControllerPhase,
    ) -> PongControllerCommand | None:
        if not observation.entry_fence_active:
            self._phase(fencing_phase, prefix + "_ENTRY_FENCE_REQUIRED")
            return self._request(
                PongControllerCommandKind.FENCE_NEW_ENTRIES,
                prefix + "_FENCE_NEW_ENTRIES",
                now,
            )
        if observation.working_entry_orders:
            self._phase(flattening_phase, prefix + "_CANCEL_PENDING_ENTRIES")
            return self._request(
                PongControllerCommandKind.CANCEL_OWNED_PENDING_ENTRIES,
                prefix + "_CANCEL_OWNED_PENDING_ENTRIES",
                now,
            )
        if observation.position_quantity:
            self._phase(flattening_phase, prefix + "_CONTROLLED_FLATTEN")
            if not observation.protection_current:
                self.state = replace(
                    self.state,
                    last_error="PONG_POSITION_PROTECTION_NOT_CURRENT_DURING_FLATTEN",
                    state_hash="",
                )
            return self._request(
                PongControllerCommandKind.CONTROLLED_FLATTEN,
                prefix + "_CONTROLLED_FLATTEN",
                now,
            )
        if observation.working_owned_orders:
            self._phase(flattening_phase, prefix + "_CANCEL_RESIDUAL_OWNED_ORDERS")
            return self._request(
                PongControllerCommandKind.CANCEL_REMAINING_OWNED_ORDERS,
                prefix + "_CANCEL_REMAINING_OWNED_ORDERS_AFTER_FLAT",
                now,
            )
        if observation.unresolved_execution or not observation.reconciliation_current:
            self._phase(reconciling_phase, prefix + "_SIGNED_RECONCILIATION")
            return self._request(
                PongControllerCommandKind.REQUEST_SIGNED_FLAT_RECONCILIATION,
                prefix + "_REQUEST_SIGNED_FLAT_RECONCILIATION",
                now,
            )
        return None

    def tick(
        self,
        observation: PongControllerObservation,
        *,
        at: str | None = None,
    ) -> PongControllerCommand | None:
        now = _utc(at or observation.observed_at)
        if _utc(observation.observed_at) > now:
            raise ValueError("Pong controller observation is future-dated.")
        self._heartbeat(now)
        self.state = replace(
            self.state, last_observation_hash=observation.observation_hash,
            state_hash="",
        )
        pending = self.state.pending_command
        if pending is not None:
            if self._command_settled(pending, observation):
                self.state = replace(self.state, pending_command=None, state_hash="")
            else:
                self._save()
                return (
                    pending
                    if pending.status is PongControllerCommandStatus.REQUESTED
                    else None
                )

        blackout_start = _utc(PONG_BLACKOUT_START_UTC)
        blackout_end = _utc(PONG_BLACKOUT_END_UTC)
        deadline = _utc(PONG_EXPERIMENT_END_UTC)
        command: PongControllerCommand | None = None

        if now >= deadline:
            command = self._ensure_flat(
                observation, now,
                prefix="PONG_EXPERIMENT_END",
                fencing_phase=PongControllerPhase.TERMINATION_FENCING,
                flattening_phase=PongControllerPhase.TERMINATION_FLATTENING,
                reconciling_phase=PongControllerPhase.TERMINATION_RECONCILING,
            )
            if command is None and not observation.evidence_sealed:
                self._phase(
                    PongControllerPhase.TERMINATION_SEALING,
                    "PONG_EXPERIMENT_EVIDENCE_SEAL_REQUIRED",
                )
                command = self._request(
                    PongControllerCommandKind.SEAL_EVIDENCE,
                    "PONG_EXPERIMENT_END_SEAL_EVIDENCE",
                    now,
                )
            elif command is None and observation.runtime_active:
                self._phase(
                    PongControllerPhase.TERMINATION_STOPPING,
                    "PONG_EXPERIMENT_OPERATION_STOP_REQUIRED",
                )
                command = self._request(
                    PongControllerCommandKind.STOP_OPERATION,
                    "PONG_EXPERIMENT_END_STOP_OPERATION",
                    now,
                )
            elif command is None:
                self._phase(
                    PongControllerPhase.TERMINATED,
                    "PONG_EXPERIMENT_TERMINATED_FLAT_RECONCILED_SEALED",
                )
        elif blackout_start <= now < blackout_end:
            command = self._ensure_flat(
                observation, now,
                prefix="PONG_NY_OPEN_BLACKOUT",
                fencing_phase=PongControllerPhase.BLACKOUT_FENCING,
                flattening_phase=PongControllerPhase.BLACKOUT_FLATTENING,
                reconciling_phase=PongControllerPhase.BLACKOUT_RECONCILING,
            )
            if command is None:
                self._phase(
                    PongControllerPhase.BLACKOUT_FLAT,
                    "PONG_NY_OPEN_BLACKOUT_FLAT_RECONCILED",
                )
        elif now >= blackout_end:
            # Complete a missed/in-progress blackout cleanup before the first
            # release.  Once post-blackout operation has begun, an owned
            # position is legitimate strategy exposure: controller health may
            # fence new entries, but only the strategy/risk engine may reduce
            # that exposure before the fixed experiment deadline.
            resume_requires_flat = self.state.phase in {
                PongControllerPhase.PREPARED,
                PongControllerPhase.RUNNING_PRE_BLACKOUT,
                PongControllerPhase.BLACKOUT_FENCING,
                PongControllerPhase.BLACKOUT_FLATTENING,
                PongControllerPhase.BLACKOUT_RECONCILING,
                PongControllerPhase.BLACKOUT_FLAT,
                PongControllerPhase.FAILED_SAFE,
            }
            command = (
                self._ensure_flat(
                    observation, now,
                    prefix="PONG_NY_OPEN_RESUME",
                    fencing_phase=PongControllerPhase.BLACKOUT_FENCING,
                    flattening_phase=PongControllerPhase.BLACKOUT_FLATTENING,
                    reconciling_phase=PongControllerPhase.BLACKOUT_RECONCILING,
                )
                if resume_requires_flat
                else None
            )
            health_reason = observation.admission_health_reason
            if command is None and health_reason is not None:
                self._phase(PongControllerPhase.RESUME_BLOCKED, health_reason)
                if not observation.entry_fence_active:
                    command = self._request(
                        PongControllerCommandKind.FENCE_NEW_ENTRIES,
                        health_reason,
                        now,
                    )
            elif command is None and observation.entry_fence_active:
                self._phase(
                    PongControllerPhase.RESUME_BLOCKED,
                    "PONG_NY_OPEN_RESUME_FENCE_RELEASE_PENDING",
                )
                command = self._request(
                    PongControllerCommandKind.RELEASE_ENTRY_FENCE,
                    "PONG_NY_OPEN_RESUME_HEALTHY",
                    now,
                )
            elif command is None:
                self._phase(
                    PongControllerPhase.RUNNING_POST_BLACKOUT,
                    "PONG_NY_OPEN_RESUMED_FOR_FRESH_SWITCHES",
                )
        else:
            health_reason = observation.admission_health_reason
            if health_reason is not None:
                self._phase(PongControllerPhase.FAILED_SAFE, health_reason)
                if not observation.entry_fence_active:
                    command = self._request(
                        PongControllerCommandKind.FENCE_NEW_ENTRIES,
                        health_reason,
                        now,
                    )
            elif observation.entry_fence_active:
                command = self._request(
                    PongControllerCommandKind.RELEASE_ENTRY_FENCE,
                    "PONG_PRE_BLACKOUT_HEALTHY",
                    now,
                )
            else:
                self._phase(
                    PongControllerPhase.RUNNING_PRE_BLACKOUT,
                    "PONG_PRE_BLACKOUT_HEALTHY",
                )
        self._save()
        return command

    def assess_entry(
        self,
        *,
        source_switch_id: str,
        source_switch_sequence: int,
        source_event_at: str,
        at: str,
    ) -> PongEntryFenceDecision:
        now = _utc(at)
        event_at = _utc(source_event_at)
        if (
            not source_switch_id
            or type(source_switch_sequence) is not int
            or source_switch_sequence < 0
            or event_at > now
        ):
            raise ValueError("Pong controller entry provenance is invalid.")
        blackout_start = _utc(PONG_BLACKOUT_START_UTC)
        blackout_end = _utc(PONG_BLACKOUT_END_UTC)
        deadline = _utc(PONG_EXPERIMENT_END_UTC)
        if blackout_start <= now < blackout_end:
            high_water = max(
                self.state.blackout_switch_high_water, source_switch_sequence,
            )
            self.state = replace(
                self.state,
                blackout_switch_high_water=high_water,
                last_blackout_switch_id=source_switch_id,
                state_hash="",
            )
            self._save()
            return self._entry_decision(False, "PONG_NY_OPEN_BLACKOUT_ENTRY_FENCED")
        if now >= deadline:
            return self._entry_decision(False, "PONG_EXPERIMENT_DEADLINE_REACHED")
        if now > _utc(self.state.heartbeat_valid_until):
            return self._entry_decision(False, "PONG_CONTROLLER_HEARTBEAT_EXPIRED")
        allowed_phases = {
            PongControllerPhase.RUNNING_PRE_BLACKOUT,
            PongControllerPhase.RUNNING_POST_BLACKOUT,
        }
        if self.state.phase not in allowed_phases:
            return self._entry_decision(
                False, "PONG_CONTROLLER_ENTRY_FENCE_ACTIVE",
            )
        if now >= blackout_end and (
            event_at <= blackout_end
            or source_switch_sequence <= self.state.blackout_switch_high_water
        ):
            return self._entry_decision(
                False, "PONG_BLACKOUT_SWITCH_CANNOT_QUEUE_FOR_RESUME",
            )
        return self._entry_decision(True, "PONG_CONTROLLER_ENTRY_ELIGIBLE")

    def _entry_decision(self, allowed: bool, reason: str) -> PongEntryFenceDecision:
        return PongEntryFenceDecision(
            allowed=allowed,
            reason=reason,
            controller_id=self.state.controller_id,
            controller_phase=self.state.phase.value,
            state_hash=self.state.state_hash,
        )


def read_controller_entry_fence(
    path: str | Path,
    *,
    source_switch_id: str,
    source_switch_sequence: int,
    source_event_at: str,
    at: str,
) -> PongEntryFenceDecision:
    """Fail-closed admission read for the runtime-side controller watchdog."""
    try:
        controller = PongBoundedController.load(path)
        return controller.assess_entry(
            source_switch_id=source_switch_id,
            source_switch_sequence=source_switch_sequence,
            source_event_at=source_event_at,
            at=at,
        )
    except (OSError, PongControllerIntegrityError, ValueError):
        return PongEntryFenceDecision(
            False,
            "PONG_CONTROLLER_STATE_UNAVAILABLE_OR_INVALID",
            None,
            None,
            None,
        )


def run_controller(
    controller: PongBoundedController,
    *,
    observe: Callable[[], PongControllerObservation],
    dispatch: Callable[[PongControllerCommand], None],
    poll_seconds: float = 0.5,
) -> None:
    """Run under the ordinary service host; no chat timer owns this loop."""
    if not 0.05 <= poll_seconds <= 1.0:
        raise ValueError("Pong controller polling must remain in [0.05, 1.0] seconds.")
    while controller.state.phase is not PongControllerPhase.TERMINATED:
        observation = observe()
        command = controller.tick(observation)
        if command is not None:
            controller.mark_dispatched(command.command_id, at=observation.observed_at)
            try:
                dispatch(command)
            except Exception as error:
                controller.acknowledge_command(
                    command.command_id,
                    at=observation.observed_at,
                    accepted=False,
                    reason="PONG_CONTROLLER_DISPATCH_FAILED:" + type(error).__name__,
                )
        time.sleep(poll_seconds)
