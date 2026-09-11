"""Pure, execution-disabled Pong state machine for design review.

This module deliberately has no execution transport, account binding, order
sender, persistence side effect, or profile registration.  It converts pinned
Perpetual logical switch events plus an injected daily-bias snapshot into
abstract actions that a future, separately reviewed adapter could evaluate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
import hashlib
import json


PONG_EXECUTION_ENABLED = False
PONG_PROFILE_REGISTRATION = "NOT_REGISTERED"
PONG_PROFILE_VERSION = "BEELZEBUB_PONG_DESIGN_V0"
PONG_SOURCE_PROFILE_VERSION = "BEELZEBUB_FIVE_MINUTE_PERPETUAL_V2"
PONG_SOURCE_POLICY_HASH = (
    "daf3cc6daacdb32d5629fd6cbd97ef2246172426a00392f59f1246c567658fe7"
)
PONG_INSTRUMENT = "MNQ SEP26"


class PongDirection(StrEnum):
    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"


class DailyBiasDirection(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"
    UNKNOWN = "UNKNOWN"


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
    REQUEST_EXIT = "REQUEST_EXIT"
    REQUEST_SAFETY_EXIT = "REQUEST_SAFETY_EXIT"
    CANCEL_PENDING_ENTRY = "CANCEL_PENDING_ENTRY"


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Pong timestamps must include a UTC offset.")
    return parsed


@dataclass(frozen=True)
class DailyBiasSnapshot:
    """Injected, causal bias evidence; no production provider exists yet."""

    direction: DailyBiasDirection = DailyBiasDirection.UNKNOWN
    configured: bool = False
    source_id: str | None = None
    instrument: str = PONG_INSTRUMENT
    as_of: str | None = None
    available_at: str | None = None
    expires_at: str | None = None

    def eligibility(self, *, at: str, direction: PongDirection) -> str:
        if not self.configured:
            return "DAILY_BIAS_UNCONFIGURED"
        if self.instrument != PONG_INSTRUMENT:
            return "DAILY_BIAS_INSTRUMENT_MISMATCH"
        if self.direction not in {
            DailyBiasDirection.LONG, DailyBiasDirection.SHORT,
        }:
            return "DAILY_BIAS_NOT_DIRECTIONAL"
        if not all((self.source_id, self.as_of, self.available_at, self.expires_at)):
            return "DAILY_BIAS_PROVENANCE_INCOMPLETE"
        decision_time = _utc(at)
        as_of = _utc(str(self.as_of))
        available = _utc(str(self.available_at))
        expires = _utc(str(self.expires_at))
        if as_of > available or expires <= available:
            return "DAILY_BIAS_PROVENANCE_INVALID"
        if available > decision_time:
            return "DAILY_BIAS_NOT_YET_AVAILABLE"
        if decision_time > expires:
            return "DAILY_BIAS_STALE"
        if self.direction.value != direction.value:
            return "PERPETUAL_SWITCH_NOT_ALIGNED_WITH_DAILY_BIAS"
        return "DAILY_BIAS_ALIGNED"


@dataclass(frozen=True)
class PerpetualSwitchEvent:
    event_id: str
    sequence: int
    previous_direction: PongDirection
    direction: PongDirection
    occurred_at: str
    prior_switch_id: str | None
    source_profile_version: str = PONG_SOURCE_PROFILE_VERSION
    source_policy_hash: str = PONG_SOURCE_POLICY_HASH
    continuity_valid: bool = True


@dataclass(frozen=True)
class PongAction:
    action_id: str
    kind: PongActionKind
    source_switch_id: str
    expected_direction: PongDirection
    reason_code: str


@dataclass(frozen=True)
class PongTransition:
    state: PongState
    actions: tuple[PongAction, ...]
    reason_code: str
    switch_consumed: bool


class PongStateMachine:
    """Deterministic design skeleton with abstract, non-executable actions."""

    def __init__(self) -> None:
        self.state = PongState.FLAT_WAIT_ALIGNMENT
        self.shadow_direction: PongDirection | None = None
        self.actual_position = PongDirection.FLAT
        self.last_switch_id: str | None = None
        self.last_switch_sequence = 0
        self.last_consumed_switch_id: str | None = None
        self.pending_action: PongAction | None = None
        self.confirmed_flat_switch_sequence: int | None = None
        self.waiting_leg_completed = False
        self.entry_admitted = False
        self.exit_fill_seen = False
        self.safety_exit_fill_seen = False
        self.daily_bias = DailyBiasSnapshot()
        self.safety_reason: str | None = None

    def _action(
        self,
        kind: PongActionKind,
        event: PerpetualSwitchEvent,
        expected: PongDirection,
        reason: str,
    ) -> PongAction:
        material = json.dumps(
            {
                "profile": PONG_PROFILE_VERSION,
                "kind": kind.value,
                "switch": event.event_id,
                "sequence": event.sequence,
                "expected": expected.value,
                "reason": reason,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return PongAction(
            "pong-action-" + hashlib.sha256(material).hexdigest()[:32],
            kind,
            event.event_id,
            expected,
            reason,
        )

    def observe_startup_seed(self, direction: PongDirection) -> PongTransition:
        """Establish shadow direction without fabricating a switch or beat."""
        if direction is PongDirection.FLAT:
            raise ValueError("Pong startup seed must be LONG or SHORT.")
        if self.last_switch_sequence or self.actual_position is not PongDirection.FLAT:
            return self._transition("STARTUP_SEED_TOO_LATE", False)
        if self.shadow_direction is None:
            self.shadow_direction = direction
            return self._transition("STARTUP_SEED_OBSERVED_NO_SWITCH", False)
        if self.shadow_direction is direction:
            return self._transition("STARTUP_SEED_REPLAY_IGNORED", False)
        return self._safety_block("STARTUP_SEED_DIRECTION_CONFLICT")

    def on_bias_update(self, snapshot: DailyBiasSnapshot) -> PongTransition:
        """Record provenance only; a bias change is never an exit signal."""
        self.daily_bias = snapshot
        return self._transition("DAILY_BIAS_UPDATED_NO_POSITION_ACTION", False)

    def on_reporting_boundary(self) -> PongTransition:
        """Reporting labels do not reset position, beat, or safety state."""
        return self._transition("REPORTING_BOUNDARY_NO_STATE_RESET", False)

    def on_switch(
        self,
        event: PerpetualSwitchEvent,
        bias: DailyBiasSnapshot,
        *,
        entry_gates_pass: bool = True,
    ) -> PongTransition:
        if (
            event.source_profile_version != PONG_SOURCE_PROFILE_VERSION
            or event.source_policy_hash != PONG_SOURCE_POLICY_HASH
        ):
            return self._safety_block("PERPETUAL_SOURCE_IDENTITY_MISMATCH")
        if (
            not event.event_id
            or event.sequence <= 0
            or event.previous_direction is PongDirection.FLAT
            or event.direction is PongDirection.FLAT
        ):
            return self._safety_block("PERPETUAL_SWITCH_EVENT_INVALID")
        _utc(event.occurred_at)
        if event.sequence == self.last_switch_sequence:
            if event.event_id == self.last_switch_id:
                return self._transition("DUPLICATE_SWITCH_IGNORED", False)
            return self._safety_block("SWITCH_SEQUENCE_IDENTITY_CONFLICT")
        if event.sequence < self.last_switch_sequence:
            return self._transition("OUT_OF_ORDER_SWITCH_IGNORED", False)
        if event.previous_direction is event.direction:
            return self._transition("SAME_DIRECTION_OBSERVATION_NOT_A_SWITCH", False)
        if not event.continuity_valid:
            return self._safety_block("PERPETUAL_SWITCH_CONTINUITY_GAP")
        expected_sequence = self.last_switch_sequence + 1
        expected_prior = self.last_switch_id
        if event.sequence != expected_sequence or event.prior_switch_id != expected_prior:
            return self._safety_block("PERPETUAL_SWITCH_CONTINUITY_GAP")
        if self.shadow_direction is None:
            return self._safety_block("PERPETUAL_SWITCH_SEED_MISSING")
        if event.previous_direction is not self.shadow_direction:
            return self._safety_block("PERPETUAL_SHADOW_DIRECTION_MISMATCH")

        self.shadow_direction = event.direction
        self.last_switch_id = event.event_id
        self.last_switch_sequence = event.sequence
        self.daily_bias = bias

        if self.state is PongState.SAFETY_BLOCKED:
            self.last_consumed_switch_id = event.event_id
            return self._transition("SAFETY_LOCKOUT_RETAINED", True)
        if self.state in {
            PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT,
        }:
            assert self.pending_action is not None
            cancellation = self._action(
                PongActionKind.CANCEL_PENDING_ENTRY,
                event,
                PongDirection.FLAT,
                "ENTRY_SIGNAL_SUPERSEDED_BEFORE_FILL",
            )
            self.pending_action = None
            self.entry_admitted = False
            self.state = PongState.FLAT_WAIT_ALIGNMENT
            self.last_consumed_switch_id = event.event_id
            return self._transition(
                "ENTRY_PENDING_SWITCH_CANCELLED_NO_QUEUED_ENTRY", True,
                (cancellation,),
            )
        if self.state is PongState.EXIT_PENDING:
            self.last_consumed_switch_id = event.event_id
            return self._transition(
                "EXIT_PENDING_SWITCH_OBSERVED_NO_QUEUED_ENTRY", True,
            )
        if self.state in {PongState.LONG, PongState.SHORT}:
            action = self._action(
                PongActionKind.REQUEST_EXIT,
                event,
                PongDirection.FLAT,
                "NEXT_OPPOSITE_PERPETUAL_SWITCH",
            )
            self.pending_action = action
            self.exit_fill_seen = False
            self.state = PongState.EXIT_PENDING
            self.last_consumed_switch_id = event.event_id
            return self._transition("POSITION_EXIT_REQUESTED", True, (action,))
        if self.state is PongState.FLAT_WAIT_BEAT:
            anchor = self.confirmed_flat_switch_sequence
            if anchor is None or event.sequence <= anchor:
                return self._safety_block("WAITING_BEAT_CONTINUITY_INVALID")
            self.waiting_leg_completed = True
            self.state = PongState.FLAT_WAIT_ALIGNMENT

        self.last_consumed_switch_id = event.event_id
        return self._evaluate_flat_entry(
            event, bias, entry_gates_pass=entry_gates_pass,
        )

    def _evaluate_flat_entry(
        self,
        event: PerpetualSwitchEvent,
        bias: DailyBiasSnapshot,
        *,
        entry_gates_pass: bool,
    ) -> PongTransition:
        bias_reason = bias.eligibility(at=event.occurred_at, direction=event.direction)
        if bias_reason != "DAILY_BIAS_ALIGNED":
            return self._transition(bias_reason, True)
        if not entry_gates_pass:
            return self._transition("ENTRY_GATES_BLOCKED", True)
        kind = (
            PongActionKind.REQUEST_ENTRY_LONG
            if event.direction is PongDirection.LONG
            else PongActionKind.REQUEST_ENTRY_SHORT
        )
        action = self._action(
            kind, event, event.direction, "ALIGNED_SWITCH_AFTER_REQUIRED_BEAT",
        )
        self.pending_action = action
        self.entry_admitted = False
        self.waiting_leg_completed = False
        self.state = (
            PongState.ENTRY_PENDING_LONG
            if event.direction is PongDirection.LONG
            else PongState.ENTRY_PENDING_SHORT
        )
        return self._transition("ENTRY_REQUESTED_FOR_REVALIDATION", True, (action,))

    def on_entry_admission(
        self,
        *,
        action_id: str,
        bias: DailyBiasSnapshot,
        at: str,
        source_fresh: bool,
        entry_gates_pass: bool,
    ) -> PongTransition:
        """Revalidate a candidate at the last possible pre-execution boundary.

        A future adapter must receive this successful transition before it may
        submit anything.  The design module itself still has no order sender.
        """
        if (
            self.pending_action is None
            or self.pending_action.action_id != action_id
            or self.state not in {
                PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT,
            }
        ):
            return self._safety_block("UNEXPECTED_ENTRY_ADMISSION")
        expected = self.pending_action.expected_direction
        if not source_fresh:
            return self._deny_pending_entry("PERPETUAL_SOURCE_NOT_FRESH")
        bias_reason = bias.eligibility(at=at, direction=expected)
        if bias_reason != "DAILY_BIAS_ALIGNED":
            return self._deny_pending_entry(bias_reason)
        if not entry_gates_pass:
            return self._deny_pending_entry("ENTRY_GATES_BLOCKED_AT_ADMISSION")
        self.daily_bias = bias
        self.entry_admitted = True
        return self._transition("ENTRY_ADMISSION_CONFIRMED_NO_EXECUTION", False)

    def _deny_pending_entry(self, reason: str) -> PongTransition:
        assert self.pending_action is not None
        action = PongAction(
            action_id=self.pending_action.action_id + "-cancel",
            kind=PongActionKind.CANCEL_PENDING_ENTRY,
            source_switch_id=self.pending_action.source_switch_id,
            expected_direction=PongDirection.FLAT,
            reason_code=reason,
        )
        self.pending_action = None
        self.entry_admitted = False
        self.state = PongState.FLAT_WAIT_ALIGNMENT
        return self._transition(reason, False, (action,))

    def on_entry_fill(
        self, *, action_id: str, direction: PongDirection, quantity: int,
    ) -> PongTransition:
        expected_state = (
            PongState.ENTRY_PENDING_LONG
            if direction is PongDirection.LONG
            else PongState.ENTRY_PENDING_SHORT
        )
        if (
            self.pending_action is None
            or self.pending_action.action_id != action_id
            or self.state is not expected_state
            or quantity != 1
            or not self.entry_admitted
        ):
            if direction in {PongDirection.LONG, PongDirection.SHORT} and quantity > 0:
                self.actual_position = direction
            return self.on_safety_exit(reason="UNEXPECTED_OR_STALE_ENTRY_FILL")
        self.actual_position = direction
        self.state = PongState.LONG if direction is PongDirection.LONG else PongState.SHORT
        self.pending_action = None
        self.entry_admitted = False
        return self._transition("ENTRY_FILL_CONFIRMED", False)

    def on_entry_rejected(self, *, action_id: str) -> PongTransition:
        if (
            self.pending_action is None
            or self.pending_action.action_id != action_id
            or self.state not in {
                PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT,
            }
        ):
            return self._safety_block("UNEXPECTED_ENTRY_TERMINAL_EVENT")
        self.pending_action = None
        self.entry_admitted = False
        self.state = PongState.FLAT_WAIT_ALIGNMENT
        return self._transition("ENTRY_REJECTED_FLAT", False)

    def on_exit_fill(self, *, action_id: str, quantity: int) -> PongTransition:
        if (
            self.state is not PongState.EXIT_PENDING
            or self.pending_action is None
            or self.pending_action.action_id != action_id
            or quantity != 1
        ):
            return self._safety_block("UNEXPECTED_EXIT_FILL")
        self.exit_fill_seen = True
        return self._transition("EXIT_FILL_AWAITING_SIGNED_FLAT_RECONCILIATION", False)

    def on_safety_exit_fill(self, *, action_id: str, quantity: int) -> PongTransition:
        if (
            self.state is not PongState.SAFETY_BLOCKED
            or self.pending_action is None
            or self.pending_action.kind is not PongActionKind.REQUEST_SAFETY_EXIT
            or self.pending_action.action_id != action_id
            or quantity != 1
        ):
            return self._safety_block("UNEXPECTED_SAFETY_EXIT_FILL")
        self.safety_exit_fill_seen = True
        return self._transition(
            "SAFETY_EXIT_FILL_AWAITING_SIGNED_FLAT_RECONCILIATION", False,
        )

    def on_signed_flat_reconciliation(
        self,
        *,
        position_quantity: int,
        working_owned_orders: int,
        signed: bool,
    ) -> PongTransition:
        if self.state is PongState.SAFETY_BLOCKED:
            fill_requirement_met = (
                self.actual_position is PongDirection.FLAT
                or self.safety_exit_fill_seen
            )
            if (
                fill_requirement_met
                and signed
                and position_quantity == 0
                and working_owned_orders == 0
            ):
                self.actual_position = PongDirection.FLAT
                self.pending_action = None
                self.entry_admitted = False
                self.safety_exit_fill_seen = False
                return self._transition("SAFETY_FLAT_CONFIRMED_LOCKOUT_RETAINED", False)
            return self._transition("SAFETY_FLAT_NOT_CONFIRMED", False)
        if (
            self.state is not PongState.EXIT_PENDING
            or not self.exit_fill_seen
            or not signed
            or position_quantity != 0
            or working_owned_orders != 0
        ):
            return self._safety_block("EXIT_RECONCILIATION_INCOMPLETE")
        self.actual_position = PongDirection.FLAT
        self.pending_action = None
        self.entry_admitted = False
        self.exit_fill_seen = False
        self.confirmed_flat_switch_sequence = self.last_switch_sequence
        self.waiting_leg_completed = False
        self.state = PongState.FLAT_WAIT_BEAT
        return self._transition("EXIT_COMPLETE_WAITING_ONE_FULL_LEG", False)

    def on_safety_exit(
        self, *, reason: str, event: PerpetualSwitchEvent | None = None,
    ) -> PongTransition:
        actions: tuple[PongAction, ...] = ()
        if event is None:
            event = PerpetualSwitchEvent(
                event_id="safety-" + hashlib.sha256(reason.encode()).hexdigest()[:24],
                sequence=max(1, self.last_switch_sequence),
                previous_direction=self.actual_position,
                direction=(
                    PongDirection.SHORT
                    if self.actual_position is PongDirection.LONG
                    else PongDirection.LONG
                ),
                occurred_at="1970-01-01T00:00:00Z",
                prior_switch_id=self.last_switch_id,
            )
        if self.actual_position is not PongDirection.FLAT:
            action = self._action(
                PongActionKind.REQUEST_SAFETY_EXIT,
                event,
                PongDirection.FLAT,
                reason,
            )
            self.pending_action = action
            self.entry_admitted = False
            self.safety_exit_fill_seen = False
            actions = (action,)
        elif self.state in {
            PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT,
        }:
            action = self._action(
                PongActionKind.CANCEL_PENDING_ENTRY,
                event,
                PongDirection.FLAT,
                reason,
            )
            self.pending_action = None
            self.entry_admitted = False
            actions = (action,)
        self.safety_reason = reason
        self.state = PongState.SAFETY_BLOCKED
        return self._transition("SAFETY_LOCKOUT_LATCHED", False, actions)

    def _safety_block(self, reason: str) -> PongTransition:
        if self.state is PongState.SAFETY_BLOCKED:
            if self.safety_reason is None:
                self.safety_reason = reason
            return self._transition("SAFETY_LOCKOUT_RETAINED", False)
        if (
            self.actual_position is not PongDirection.FLAT
            or self.state in {
                PongState.ENTRY_PENDING_LONG, PongState.ENTRY_PENDING_SHORT,
            }
        ):
            return self.on_safety_exit(reason=reason)
        self.safety_reason = reason
        self.state = PongState.SAFETY_BLOCKED
        return self._transition(reason, False)

    def _transition(
        self,
        reason: str,
        consumed: bool,
        actions: tuple[PongAction, ...] = (),
    ) -> PongTransition:
        return PongTransition(self.state, actions, reason, consumed)

    def status(self) -> dict[str, object]:
        return {
            "schema": "beelzebub-pong-design-status-v0",
            "profile": PONG_PROFILE_VERSION,
            "execution_enabled": PONG_EXECUTION_ENABLED,
            "profile_registration": PONG_PROFILE_REGISTRATION,
            "source_profile_version": PONG_SOURCE_PROFILE_VERSION,
            "source_policy_hash": PONG_SOURCE_POLICY_HASH,
            "state": self.state.value,
            "shadow_direction": (
                None if self.shadow_direction is None else self.shadow_direction.value
            ),
            "actual_position": self.actual_position.value,
            "last_switch_id": self.last_switch_id,
            "last_switch_sequence": self.last_switch_sequence,
            "last_consumed_switch_id": self.last_consumed_switch_id,
            "pending_action": (
                None if self.pending_action is None else self.pending_action.kind.value
            ),
            "entry_admitted": self.entry_admitted,
            "confirmed_flat_switch_sequence": self.confirmed_flat_switch_sequence,
            "waiting_leg_completed": self.waiting_leg_completed,
            "daily_bias_source_id": self.daily_bias.source_id,
            "daily_bias_configured": self.daily_bias.configured,
            "daily_bias_direction": self.daily_bias.direction.value,
            "daily_bias_as_of": self.daily_bias.as_of,
            "daily_bias_available_at": self.daily_bias.available_at,
            "daily_bias_expires_at": self.daily_bias.expires_at,
            "default_entry_blocker": "DAILY_BIAS_UNCONFIGURED",
            "safety_reason": self.safety_reason,
            "live_capital": "DENIED",
        }
