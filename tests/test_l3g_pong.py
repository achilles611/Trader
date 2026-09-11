from __future__ import annotations

import unittest

from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.contracts import (
    FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
    FIVE_MINUTE_PERPETUAL_POLICY,
)
from src.l3g_paper.daily_bias import (
    BiasInputProvenance,
    BiasUpdateReason,
    CoreComponentEvidence,
    CoreComponentKind,
    DailyBiasDirection,
    DailyBiasEngine,
    DailyBiasResult,
    HourlyBiasFrame,
    OptionsDayClass,
    OptionsOverlayEvidence,
    OptionsOverlayOutcome,
    default_unconfigured_bias,
)
from src.l3g_paper.pong import (
    PerpetualSwitchEvent,
    PongActionKind,
    PongDirection,
    PongState,
    PongStateMachine,
    PONG_EXECUTION_ENABLED,
    PONG_PROFILE_REGISTRATION,
    PONG_SOURCE_POLICY_HASH,
    PONG_SOURCE_PROFILE_VERSION,
)


def _provenance(name: str, *, stale: bool = False) -> BiasInputProvenance:
    definition = {"test_component": name, "version": 0}
    return BiasInputProvenance(
        source="DETERMINISTIC_TEST_FIXTURE",
        calculation_id=f"test-{name}",
        calculation_version="0",
        calculation_hash=canonical_hash(definition),
        window_start="2026-09-11T00:00:00Z",
        window_end="2026-09-11T01:00:00Z",
        latest_event_at="2026-09-11T00:59:59Z",
        observed_at="2026-09-11T01:00:00Z",
        available_at="2026-09-11T01:00:00Z",
        valid_until=(
            "2026-09-11T01:00:30Z" if stale else "2026-09-11T02:05:00Z"
        ),
        input_ids=(f"test-{name}-input",),
        input_hashes=(canonical_hash({"test_input": name}),),
    )


def bias(
    direction: DailyBiasDirection,
    *,
    stale: bool = False,
) -> DailyBiasResult:
    if direction is DailyBiasDirection.UNAVAILABLE and not stale:
        return default_unconfigured_bias()
    component_directions = (
        (direction, direction, direction)
        if direction in {DailyBiasDirection.LONG, DailyBiasDirection.SHORT}
        else (
            DailyBiasDirection.LONG,
            DailyBiasDirection.SHORT,
            DailyBiasDirection.NEUTRAL,
        )
    )
    components = tuple(
        CoreComponentEvidence(
            component,
            component_direction,
            True,
            True,
            False,
            ("DETERMINISTIC_TEST_COMPONENT",),
            _provenance(component.value.lower(), stale=stale),
        )
        for component, component_direction in zip(CoreComponentKind, component_directions)
    )
    options = OptionsOverlayEvidence(
        OptionsDayClass.ORDINARY,
        OptionsOverlayOutcome.NOT_APPLICABLE,
        None,
        False,
        ("ORDINARY_TEST_DAY",),
        _provenance("options", stale=stale),
    )
    return DailyBiasEngine().update(
        HourlyBiasFrame(
            "MNQ SEP26",
            "2026-09-11T00:00:00Z",
            "2026-09-11T01:00:00Z",
            "2026-09-11T01:00:00Z",
            components,
            options,
            True,
            None,
        ),
        BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
    ).result


def switch(
    sequence: int,
    previous: PongDirection,
    direction: PongDirection,
    *,
    prior: str | None,
    event_id: str | None = None,
    continuity_valid: bool = True,
) -> PerpetualSwitchEvent:
    return PerpetualSwitchEvent(
        event_id or f"switch-{sequence}",
        sequence,
        previous,
        direction,
        f"2026-09-11T01:{sequence:02d}:00Z",
        prior,
        continuity_valid=continuity_valid,
    )


class PongStateMachineTests(unittest.TestCase):
    def _enter(
        self,
        machine: PongStateMachine,
        event: PerpetualSwitchEvent,
        direction: PongDirection,
    ) -> None:
        result = machine.on_switch(
            event,
            bias(DailyBiasDirection(direction.value)),
        )
        self.assertEqual(len(result.actions), 1)
        action = result.actions[0]
        admission = machine.on_entry_admission(
            action_id=action.action_id,
            bias=bias(DailyBiasDirection(direction.value)),
            at=event.occurred_at,
            source_fresh=True,
            entry_gates_pass=True,
        )
        self.assertEqual(admission.reason_code, "ENTRY_ADMISSION_CONFIRMED_NO_EXECUTION")
        machine.on_entry_fill(
            action_id=action.action_id, direction=direction, quantity=1,
        )

    def test_framework_is_pinned_and_execution_disabled(self) -> None:
        self.assertFalse(PONG_EXECUTION_ENABLED)
        self.assertEqual(PONG_PROFILE_REGISTRATION, "NOT_REGISTERED")
        self.assertEqual(
            PONG_SOURCE_PROFILE_VERSION,
            FIVE_MINUTE_PERPETUAL_ENTRY_PROFILE_VERSION,
        )
        self.assertEqual(
            PONG_SOURCE_POLICY_HASH,
            FIVE_MINUTE_PERPETUAL_POLICY.configuration_hash,
        )
        self.assertEqual(
            PongStateMachine().status()["default_entry_blocker"],
            "DAILY_BIAS_UNCONFIGURED",
        )

    def test_long_example_waits_one_complete_leg_then_reenters(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        self.assertEqual(machine.state, PongState.LONG)

        exit_event = switch(
            2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id,
        )
        exit_result = machine.on_switch(
            exit_event, bias(DailyBiasDirection.LONG),
        )
        self.assertEqual(exit_result.actions[0].kind, PongActionKind.REQUEST_EXIT)
        machine.on_exit_fill(
            action_id=exit_result.actions[0].action_id, quantity=1,
        )
        machine.on_signed_flat_reconciliation(
            position_quantity=0, working_owned_orders=0, signed=True,
        )
        self.assertEqual(machine.state, PongState.FLAT_WAIT_BEAT)

        reentry = switch(
            3, PongDirection.SHORT, PongDirection.LONG, prior=exit_event.event_id,
        )
        result = machine.on_switch(reentry, bias(DailyBiasDirection.LONG))
        self.assertEqual(result.actions[0].kind, PongActionKind.REQUEST_ENTRY_LONG)
        self.assertEqual(machine.state, PongState.ENTRY_PENDING_LONG)

    def test_short_example_is_exact_mirror(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.LONG)
        first = switch(1, PongDirection.LONG, PongDirection.SHORT, prior=None)
        self._enter(machine, first, PongDirection.SHORT)
        exit_event = switch(
            2, PongDirection.SHORT, PongDirection.LONG, prior=first.event_id,
        )
        exited = machine.on_switch(exit_event, bias(DailyBiasDirection.SHORT))
        machine.on_exit_fill(action_id=exited.actions[0].action_id, quantity=1)
        machine.on_signed_flat_reconciliation(
            position_quantity=0, working_owned_orders=0, signed=True,
        )
        reentry = switch(
            3, PongDirection.LONG, PongDirection.SHORT, prior=exit_event.event_id,
        )
        result = machine.on_switch(reentry, bias(DailyBiasDirection.SHORT))
        self.assertEqual(result.actions[0].kind, PongActionKind.REQUEST_ENTRY_SHORT)

    def test_bias_mismatch_can_skip_additional_beats(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        second = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        exited = machine.on_switch(second, bias(DailyBiasDirection.LONG))
        machine.on_exit_fill(action_id=exited.actions[0].action_id, quantity=1)
        machine.on_signed_flat_reconciliation(
            position_quantity=0, working_owned_orders=0, signed=True,
        )

        third = switch(3, PongDirection.SHORT, PongDirection.LONG, prior=second.event_id)
        mismatch = machine.on_switch(third, bias(DailyBiasDirection.SHORT))
        self.assertEqual(mismatch.actions, ())
        self.assertEqual(
            mismatch.reason_code,
            "PERPETUAL_SWITCH_NOT_ALIGNED_WITH_DAILY_BIAS",
        )
        fourth = switch(4, PongDirection.LONG, PongDirection.SHORT, prior=third.event_id)
        still_mismatch = machine.on_switch(fourth, bias(DailyBiasDirection.LONG))
        self.assertEqual(still_mismatch.actions, ())
        fifth = switch(5, PongDirection.SHORT, PongDirection.LONG, prior=fourth.event_id)
        aligned = machine.on_switch(fifth, bias(DailyBiasDirection.LONG))
        self.assertEqual(aligned.actions[0].kind, PongActionKind.REQUEST_ENTRY_LONG)

    def test_bias_change_while_waiting_does_not_manufacture_a_beat(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        second = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        exited = machine.on_switch(second, bias(DailyBiasDirection.LONG))
        machine.on_exit_fill(action_id=exited.actions[0].action_id, quantity=1)
        machine.on_signed_flat_reconciliation(
            position_quantity=0, working_owned_orders=0, signed=True,
        )
        self.assertEqual(machine.state, PongState.FLAT_WAIT_BEAT)

        update = machine.on_bias_update(bias(DailyBiasDirection.SHORT))
        self.assertEqual(update.actions, ())
        self.assertEqual(update.reason_code, "DAILY_BIAS_UPDATED_NO_POSITION_ACTION")
        self.assertEqual(machine.state, PongState.FLAT_WAIT_BEAT)
        self.assertEqual(machine.last_switch_sequence, 2)

        third = switch(3, PongDirection.SHORT, PongDirection.LONG, prior=second.event_id)
        opposed = machine.on_switch(third, bias(DailyBiasDirection.SHORT))
        self.assertEqual(opposed.actions, ())
        self.assertEqual(opposed.reason_code, "PERPETUAL_SWITCH_NOT_ALIGNED_WITH_DAILY_BIAS")
        fourth = switch(4, PongDirection.LONG, PongDirection.SHORT, prior=third.event_id)
        aligned = machine.on_switch(fourth, bias(DailyBiasDirection.SHORT))
        self.assertEqual(aligned.actions[0].kind, PongActionKind.REQUEST_ENTRY_SHORT)

    def test_unconfigured_unknown_and_stale_bias_deny_entries(self) -> None:
        fixtures = (
            (default_unconfigured_bias(), "DAILY_BIAS_UNCONFIGURED"),
            (bias(DailyBiasDirection.NEUTRAL), "DAILY_BIAS_NOT_DIRECTIONAL"),
            (bias(DailyBiasDirection.LONG, stale=True), "DAILY_BIAS_STALE"),
        )
        for snapshot, reason in fixtures:
            with self.subTest(reason=reason):
                machine = PongStateMachine()
                machine.observe_startup_seed(PongDirection.SHORT)
                result = machine.on_switch(
                    switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None),
                    snapshot,
                )
                self.assertEqual(result.actions, ())
                self.assertEqual(result.reason_code, reason)
                self.assertEqual(machine.state, PongState.FLAT_WAIT_ALIGNMENT)

    def test_seed_and_same_direction_replays_never_create_beats(self) -> None:
        machine = PongStateMachine()
        first = machine.observe_startup_seed(PongDirection.LONG)
        replay = machine.observe_startup_seed(PongDirection.LONG)
        same = machine.on_switch(
            switch(1, PongDirection.LONG, PongDirection.LONG, prior=None),
            bias(DailyBiasDirection.LONG),
        )
        self.assertEqual(first.actions, ())
        self.assertEqual(replay.reason_code, "STARTUP_SEED_REPLAY_IGNORED")
        self.assertEqual(same.reason_code, "SAME_DIRECTION_OBSERVATION_NOT_A_SWITCH")
        self.assertEqual(machine.last_switch_sequence, 0)

    def test_entry_pending_switch_cancels_and_stale_fill_blocks(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        pending = machine.on_switch(first, bias(DailyBiasDirection.LONG))
        entry_action = pending.actions[0]
        second = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        superseded = machine.on_switch(second, bias(DailyBiasDirection.LONG))
        self.assertEqual(
            superseded.actions[0].kind, PongActionKind.CANCEL_PENDING_ENTRY,
        )
        stale = machine.on_entry_fill(
            action_id=entry_action.action_id,
            direction=PongDirection.LONG,
            quantity=1,
        )
        self.assertEqual(stale.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(stale.reason_code, "SAFETY_LOCKOUT_LATCHED")
        self.assertEqual(stale.actions[0].kind, PongActionKind.REQUEST_SAFETY_EXIT)
        self.assertEqual(machine.actual_position, PongDirection.LONG)

    def test_entry_admission_rechecks_freshness_bias_and_risk(self) -> None:
        fixtures = (
            (False, bias(DailyBiasDirection.LONG), True, "PERPETUAL_SOURCE_NOT_FRESH"),
            (True, bias(DailyBiasDirection.SHORT), True,
             "PERPETUAL_SWITCH_NOT_ALIGNED_WITH_DAILY_BIAS"),
            (True, bias(DailyBiasDirection.LONG), False,
             "ENTRY_GATES_BLOCKED_AT_ADMISSION"),
        )
        for fresh, snapshot, gates, reason in fixtures:
            with self.subTest(reason=reason):
                machine = PongStateMachine()
                machine.observe_startup_seed(PongDirection.SHORT)
                event = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
                candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
                denied = machine.on_entry_admission(
                    action_id=candidate.actions[0].action_id,
                    bias=snapshot,
                    at=event.occurred_at,
                    source_fresh=fresh,
                    entry_gates_pass=gates,
                )
                self.assertEqual(denied.reason_code, reason)
                self.assertEqual(denied.actions[0].kind, PongActionKind.CANCEL_PENDING_ENTRY)
                self.assertEqual(machine.state, PongState.FLAT_WAIT_ALIGNMENT)

    def test_switch_during_exit_is_not_queued_for_entry(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        second = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        exiting = machine.on_switch(second, bias(DailyBiasDirection.LONG))
        third = switch(3, PongDirection.SHORT, PongDirection.LONG, prior=second.event_id)
        during = machine.on_switch(third, bias(DailyBiasDirection.LONG))
        self.assertEqual(during.actions, ())
        self.assertEqual(
            during.reason_code, "EXIT_PENDING_SWITCH_OBSERVED_NO_QUEUED_ENTRY",
        )
        machine.on_exit_fill(action_id=exiting.actions[0].action_id, quantity=1)
        machine.on_signed_flat_reconciliation(
            position_quantity=0, working_owned_orders=0, signed=True,
        )
        self.assertEqual(machine.confirmed_flat_switch_sequence, 3)
        fourth = switch(4, PongDirection.LONG, PongDirection.SHORT, prior=third.event_id)
        result = machine.on_switch(fourth, bias(DailyBiasDirection.SHORT))
        self.assertEqual(result.actions[0].kind, PongActionKind.REQUEST_ENTRY_SHORT)

    def test_duplicate_out_of_order_and_gap_events_never_create_actions(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        result = machine.on_switch(first, default_unconfigured_bias())
        duplicate = machine.on_switch(first, bias(DailyBiasDirection.LONG))
        old = machine.on_switch(
            switch(0, PongDirection.LONG, PongDirection.SHORT, prior=None),
            bias(DailyBiasDirection.SHORT),
        )
        self.assertEqual(result.actions, ())
        self.assertEqual(duplicate.reason_code, "DUPLICATE_SWITCH_IGNORED")
        self.assertEqual(old.reason_code, "PERPETUAL_SWITCH_EVENT_INVALID")
        self.assertEqual(machine.last_switch_sequence, 1)

        gap = machine.on_switch(
            switch(3, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id),
            bias(DailyBiasDirection.SHORT),
        )
        self.assertEqual(gap.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(gap.actions, ())

    def test_continuity_gap_while_exposed_requests_safety_exit(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        gap = machine.on_switch(
            switch(3, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id),
            bias(DailyBiasDirection.SHORT),
        )
        self.assertEqual(gap.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(gap.actions[0].kind, PongActionKind.REQUEST_SAFETY_EXIT)
        self.assertEqual(machine.safety_reason, "PERPETUAL_SWITCH_CONTINUITY_GAP")

    def test_bias_change_does_not_exit_and_reporting_boundary_preserves_state(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        update = machine.on_bias_update(bias(DailyBiasDirection.SHORT))
        boundary = machine.on_reporting_boundary()
        self.assertEqual(update.actions, ())
        self.assertEqual(boundary.actions, ())
        self.assertEqual(machine.state, PongState.LONG)
        self.assertEqual(machine.actual_position, PongDirection.LONG)
        self.assertEqual(machine.last_switch_id, first.event_id)

    def test_bias_change_while_short_does_not_force_exit(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.LONG)
        first = switch(1, PongDirection.LONG, PongDirection.SHORT, prior=None)
        self._enter(machine, first, PongDirection.SHORT)
        update = machine.on_bias_update(bias(DailyBiasDirection.LONG))
        self.assertEqual(update.actions, ())
        self.assertEqual(machine.state, PongState.SHORT)
        self.assertEqual(machine.actual_position, PongDirection.SHORT)
        self.assertEqual(machine.last_switch_id, first.event_id)

    def test_safety_exit_retains_lockout_after_flat_and_later_switches(self) -> None:
        machine = PongStateMachine()
        machine.observe_startup_seed(PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        safety = machine.on_safety_exit(reason="QUOTE_STALE")
        self.assertEqual(safety.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(safety.actions[0].kind, PongActionKind.REQUEST_SAFETY_EXIT)
        before_fill = machine.on_signed_flat_reconciliation(
            position_quantity=0, working_owned_orders=0, signed=True,
        )
        self.assertEqual(before_fill.reason_code, "SAFETY_FLAT_NOT_CONFIRMED")
        machine.on_safety_exit_fill(
            action_id=safety.actions[0].action_id, quantity=1,
        )
        machine.on_signed_flat_reconciliation(
            position_quantity=0, working_owned_orders=0, signed=True,
        )
        self.assertEqual(machine.state, PongState.SAFETY_BLOCKED)
        second = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        later = machine.on_switch(second, bias(DailyBiasDirection.SHORT))
        self.assertEqual(later.actions, ())
        self.assertEqual(later.reason_code, "SAFETY_LOCKOUT_RETAINED")


if __name__ == "__main__":
    unittest.main()
