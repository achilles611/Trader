from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
import json
import math
from pathlib import Path
import unittest

from src.lane_iii.contracts import canonical_hash
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
from src.l3g_paper.discernment import (
    DISCERNMENT_SCHEMA,
    EXPERIMENTAL_DIRECTIONAL_SUPPORT,
    PONG_RESEARCH_EVIDENCE_MAPPING,
    PONG_RESEARCH_POLICY,
    STRONG_THRESHOLD_UNREACHABLE,
    ComponentEffect,
    DiscernmentDirection,
    DiscernmentEngine,
    DiscernmentEvidence,
    DiscernmentPolicy,
    DiscernmentSnapshot,
    DiscernmentSwitch,
    DiscernmentValidity,
    VolumeRegime,
)
from src.l3g_paper.pong import (
    PONG_EXECUTION_ENABLED,
    PONG_PROFILE_REGISTRATION,
    PONG_SIGNAL_SOURCE,
    PongActionKind,
    PongDirection,
    PongState,
    PongStateMachine,
    deserialize_pong_checkpoint,
    serialize_pong_checkpoint,
)
from src.l3g_paper.pong_quantity import (
    PONG_QUANTITY_POLICY,
    PongReconciliationProof,
)


def _provenance(name: str, *, stale: bool = False) -> BiasInputProvenance:
    definition = {"test_component": name, "version": 1}
    return BiasInputProvenance(
        source="DETERMINISTIC_TEST_FIXTURE",
        calculation_id=f"test-{name}",
        calculation_version="1",
        calculation_hash=canonical_hash(definition),
        window_start="2026-09-11T00:00:00Z",
        window_end="2026-09-11T01:00:00Z",
        latest_event_at="2026-09-11T00:59:59Z",
        observed_at="2026-09-11T01:00:00Z",
        available_at="2026-09-11T01:00:00Z",
        valid_until="2026-09-11T01:00:30Z" if stale else "2026-09-11T02:05:00Z",
        input_ids=(f"test-{name}-input",),
        input_hashes=(canonical_hash({"test_input": name}),),
    )


def bias(direction: DailyBiasDirection, *, stale: bool = False) -> DailyBiasResult:
    if direction is DailyBiasDirection.UNAVAILABLE and not stale:
        return default_unconfigured_bias()
    component_directions = (
        (direction, direction, direction)
        if direction in {DailyBiasDirection.LONG, DailyBiasDirection.SHORT}
        else (DailyBiasDirection.LONG, DailyBiasDirection.SHORT, DailyBiasDirection.NEUTRAL)
    )
    components = tuple(
        CoreComponentEvidence(
            component, component_direction, True, True, False,
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
            "MNQ SEP26", "2026-09-11T00:00:00Z", "2026-09-11T01:00:00Z",
            "2026-09-11T01:00:00Z", components, options, True, None,
        ),
        BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
    ).result


def snapshot(
    score: float | None,
    *,
    minute: int,
    valid: bool = True,
    domain: str = "test-continuity-domain",
    reliability: float | None = None,
    policy: DiscernmentPolicy = PONG_RESEARCH_POLICY,
    include_required_components: bool = True,
) -> DiscernmentSnapshot:
    if reliability is None:
        reliability = policy.reliability
    if valid and include_required_components:
        if score is None:
            raise ValueError("A valid deterministic snapshot requires a score.")
        if not math.isclose(reliability, policy.reliability, rel_tol=0, abs_tol=1e-15):
            raise ValueError("The deterministic snapshot reliability must match its policy.")
        raw_target = 0.5 + (score - 0.5) / reliability
        if not 0 < raw_target < 1:
            raise ValueError("The requested deterministic score is unattainable.")
        prior_log_odds = math.log(
            policy.prior_long_score / (1 - policy.prior_long_score)
        )
        remaining_effect = math.log(raw_target / (1 - raw_target)) - prior_log_odds
        group_effects: dict[str, float] = {}
        for group, bound in policy.dependency_group_bounds:
            contribution = max(-bound, min(bound, remaining_effect))
            group_effects[group] = contribution
            remaining_effect -= contribution
        if not math.isclose(remaining_effect, 0.0, rel_tol=0, abs_tol=1e-12):
            raise ValueError("The requested deterministic score exceeds dependency bounds.")

        mapped_groups = {
            (item.component_id, item.component_version): item.dependency_group
            for item in PONG_RESEARCH_EVIDENCE_MAPPING.effects
        }
        representatives = {
            item.dependency_group: (item.component_id, item.component_version)
            for item in PONG_RESEARCH_EVIDENCE_MAPPING.effects
        }
        specifications = [
            [
                component_id,
                component_version,
                mapped_groups.get(
                    (component_id, component_version),
                    policy.dependency_group_bounds[0][0],
                ),
                0.0,
                True,
            ]
            for component_id, component_version in sorted(policy.required_components)
        ]
        for group, contribution in group_effects.items():
            matching = next(
                (item for item in specifications if item[2] == group),
                None,
            )
            if matching is None:
                component_id, component_version = representatives.get(
                    group, (f"DETERMINISTIC_TEST_{group}", "1"),
                )
                specifications.append(
                    [component_id, component_version, group, contribution, False]
                )
            else:
                matching[3] = contribution

        timestamp = f"2026-09-11T01:{minute:02d}:00Z"
        valid_until = (
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            + timedelta(seconds=30)
        ).isoformat().replace("+00:00", "Z")
        engine = DiscernmentEngine(policy)
        update = None
        for index, (component_id, component_version, group, effect, required) in enumerate(
            sorted(specifications), start=1,
        ):
            source_id = f"fixture-{minute}-{component_id}-{component_version}"
            update = engine.on_evidence(
                DiscernmentEvidence(
                    event_id=f"fixture-event-{minute}-{component_id}-{component_version}",
                    component_id=component_id,
                    component_version=component_version,
                    dependency_group=group,
                    log_odds_effect=effect,
                    required=required,
                    source_event_at=timestamp,
                    received_at=timestamp,
                    available_at=timestamp,
                    valid_until=valid_until,
                    evidence_revision=minute * 10 + index,
                    continuity_domain=domain,
                    continuity_valid=True,
                    source_ids=(source_id,),
                    source_hashes=(canonical_hash({"fixture_source": source_id}),),
                ),
                volume_regime=VolumeRegime.NORMAL,
            )
        if update is None or not update.snapshot.valid:
            raise AssertionError("The deterministic fixture did not produce a valid snapshot.")
        if update.snapshot.verification_error(policy, at=timestamp) is not None:
            raise AssertionError("The deterministic fixture produced an unverifiable snapshot.")
        if not math.isclose(
            update.snapshot.effective_long_score or 0.0,
            score,
            rel_tol=0,
            abs_tol=1e-15,
        ):
            raise AssertionError("The deterministic fixture did not reproduce the requested score.")
        return update.snapshot

    raw_score = (
        None if score is None else 0.5 + (score - 0.5) / reliability
    )
    timestamp = f"2026-09-11T01:{minute:02d}:00Z"
    valid_until = (
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        + timedelta(seconds=30)
    ).isoformat().replace("+00:00", "Z")
    policy_hash = policy.policy_hash
    model_hash = policy.model_hash
    validity = DiscernmentValidity.VALID if valid else DiscernmentValidity.UNAVAILABLE
    component_effects: tuple[ComponentEffect, ...] = ()
    if valid and include_required_components:
        dependency_group = (
            policy.dependency_group_bounds[0][0]
            if policy.dependency_group_bounds
            else "DETERMINISTIC_TEST_DEPENDENCY"
        )
        effects: list[ComponentEffect] = []
        for component_id, component_version in sorted(policy.required_components):
            source_id = f"fixture-{minute}-{component_id}-{component_version}"
            evidence = DiscernmentEvidence(
                event_id=f"fixture-event-{minute}-{component_id}-{component_version}",
                component_id=component_id,
                component_version=component_version,
                dependency_group=dependency_group,
                log_odds_effect=0.0,
                required=True,
                source_event_at=timestamp,
                received_at=timestamp,
                available_at=timestamp,
                valid_until=valid_until,
                evidence_revision=minute + 1,
                continuity_domain=domain,
                continuity_valid=True,
                source_ids=(source_id,),
                source_hashes=(canonical_hash({"fixture_source": source_id}),),
            )
            effects.append(ComponentEffect(
                component_id=evidence.component_id,
                component_version=evidence.component_version,
                dependency_group=evidence.dependency_group,
                log_odds_effect=evidence.log_odds_effect,
                event_id=evidence.event_id,
                source_event_at=evidence.source_event_at,
                received_at=evidence.received_at,
                available_at=evidence.available_at,
                valid_until=evidence.valid_until,
                evidence_revision=evidence.evidence_revision,
                continuity_domain=evidence.continuity_domain,
                continuity_valid=evidence.continuity_valid,
                required=evidence.required,
                source_ids=evidence.source_ids,
                source_hashes=evidence.source_hashes,
                evidence_hash=evidence.evidence_hash,
            ))
        component_effects = tuple(effects)
    source_ids = tuple(sorted({
        source_id
        for component in component_effects
        for source_id in component.source_ids
    }))
    reasons = ("DETERMINISTIC_TEST_FIXTURE",) if valid else ("TEST_UNAVAILABLE",)
    result_payload = {
        "schema": DISCERNMENT_SCHEMA,
        "instrument": "MNQ SEP26",
        "contract": "MNQU6",
        "as_of": timestamp,
        "received_at": timestamp,
        "available_at": timestamp,
        "evidence_revision": minute + 1,
        "continuity_domain": domain,
        "continuity_valid": valid,
        "raw_long_score": raw_score if valid else None,
        "effective_long_score": score if valid else None,
        "long_discernment_pct": None if score is None else score * 100,
        "short_discernment_pct": None if score is None else (1 - score) * 100,
        "reliability": reliability,
        "calibration_status": EXPERIMENTAL_DIRECTIONAL_SUPPORT,
        "component_effects": [item.payload() for item in component_effects],
        "dependency_group_effects": [],
        "volume_regime": VolumeRegime.NORMAL.value,
        "source_ids": list(source_ids),
        "policy_hash": policy_hash,
        "model_hash": model_hash,
        "validity": validity.value,
        "reason_codes": list(reasons),
        "execution_enabled": False,
    }
    return DiscernmentSnapshot(
        instrument="MNQ SEP26",
        contract="MNQU6",
        as_of=timestamp,
        received_at=timestamp,
        available_at=timestamp,
        evidence_revision=minute + 1,
        continuity_domain=domain,
        continuity_valid=valid,
        raw_long_score=raw_score if valid else None,
        effective_long_score=score if valid else None,
        long_discernment_pct=None if score is None else score * 100,
        short_discernment_pct=None if score is None else (1 - score) * 100,
        reliability=reliability,
        calibration_status=EXPERIMENTAL_DIRECTIONAL_SUPPORT,
        component_effects=component_effects,
        dependency_group_effects=(),
        volume_regime=VolumeRegime.NORMAL,
        source_ids=source_ids,
        policy_hash=policy_hash,
        model_hash=model_hash,
        validity=validity,
        reason_codes=reasons,
        result_hash=canonical_hash(result_payload),
    )


def switch(
    sequence: int,
    previous: PongDirection,
    direction: PongDirection,
    *,
    prior: str | None,
    score: float | None = None,
    domain: str = "test-continuity-domain",
    policy: DiscernmentPolicy = PONG_RESEARCH_POLICY,
) -> DiscernmentSwitch:
    chosen_score = score
    if chosen_score is None:
        chosen_score = 0.70 if direction is PongDirection.LONG else 0.30
    snap = snapshot(
        chosen_score, minute=max(1, sequence), domain=domain, policy=policy,
    )
    return DiscernmentSwitch.create(
        policy=policy,
        sequence=sequence,
        prior_switch_id=prior,
        previous_direction=DiscernmentDirection(previous.value),
        direction=DiscernmentDirection(direction.value),
        snapshot=snap,
    )


def seed(machine: PongStateMachine, direction: PongDirection) -> None:
    score = 0.70 if direction is PongDirection.LONG else 0.30
    result = machine.observe_startup_seed(
        DiscernmentDirection(direction.value),
        snapshot(score, minute=0, policy=machine.discernment_policy),
    )
    if result.reason_code != "DISCERNMENT_SEED_OBSERVED_NO_SWITCH":
        raise AssertionError(result)


def reconciliation(
    at: str,
    *,
    position_quantity: int = 0,
    working_owned_orders: int = 0,
    unresolved_execution: bool = False,
) -> PongReconciliationProof:
    return PongReconciliationProof.create(
        source_hash=canonical_hash({"signed_reconciliation_fixture": 1}),
        observed_at=at,
        available_at=at,
        position_quantity=position_quantity,
        working_owned_orders=working_owned_orders,
        unresolved_execution=unresolved_execution,
    )


def strong_discernment_policy() -> DiscernmentPolicy:
    return replace(
        PONG_RESEARCH_POLICY,
        reliability=1.0,
        model_version="pong-discernment-v1-deterministic-strong-test-fixture",
        dependency_group_bounds=(
            ("FLOW_AND_INTERACTION_SHARED", 4.0),
            ("WEAK_STRUCTURE", 1.0),
        ),
        evidence_mapping_hash=canonical_hash({
            "fixture": "pong-discernment-v1-deterministic-strong-test-fixture",
            "dependency_group_bounds": [
                ["FLOW_AND_INTERACTION_SHARED", 4.0],
                ["WEAK_STRUCTURE", 1.0],
            ],
        }),
    )


class PongStateMachineTests(unittest.TestCase):
    def test_pong_is_absent_from_operational_registration_and_native_paths(self) -> None:
        repository_root = Path(__file__).resolve().parents[1]
        operational_paths = (
            repository_root / "src/l3g_paper/runtime.py",
            repository_root / "src/l3g_paper/profile_switch.py",
            repository_root / "src/l3g_paper/ninjatrader_transport.py",
            repository_root / "src/copytrade/control_center.py",
            repository_root / "ninjatrader/NinjaScript/AddOns/BeelzebubPaperExecutionAddOn.cs",
            repository_root / "ninjatrader/NinjaScript/AddOns/BeelzebubLiveExecutionAddOn.cs",
            repository_root / "ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs",
            repository_root / "ninjatrader/NinjaScript/Indicators/BeelzebubReadOnlyMarketObserver.cs",
        )
        for path in operational_paths:
            with self.subTest(path=path):
                text = path.read_text(encoding="utf-8")
                self.assertNotIn("BEELZEBUB_PONG", text)
                self.assertNotIn("l3g_paper.pong", text)

    def _enter(
        self,
        machine: PongStateMachine,
        event: DiscernmentSwitch,
        direction: PongDirection,
        *,
        policy=PONG_QUANTITY_POLICY,
    ) -> None:
        self.assertEqual(machine.quantity_policy, policy)
        result = machine.on_switch(event, bias(DailyBiasDirection(direction.value)))
        self.assertEqual(len(result.actions), 1)
        action = result.actions[0]
        admission = machine.on_entry_admission(
            action_id=action.action_id,
            bias=bias(DailyBiasDirection(direction.value)),
            at=event.available_at,
            current_snapshot=event.snapshot,
            current_switch_cursor=event,
            source_fresh=True,
            entry_gates_pass=True,
            reconciliation=reconciliation(event.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        self.assertIn(
            admission.reason_code,
            {"ENTRY_ADMISSION_CONFIRMED_NO_EXECUTION", "ENTRY_ADMISSION_DOWNGRADED_NO_EXECUTION"},
        )
        admitted_action = admission.actions[0]
        fill = machine.on_entry_fill(
            action_id=admitted_action.action_id,
            direction=direction,
            quantity=machine.entry_target_quantity,
            fill_id=f"fill-{admitted_action.action_id}",
        )
        self.assertEqual(fill.actions[0].kind, PongActionKind.REQUEST_PROTECTIVE_STOP)
        machine.on_protection_update(
            action_id=fill.actions[0].action_id,
            quantity=machine.actual_quantity,
            signed=True,
        )

    def _begin_exit(
        self,
        machine: PongStateMachine,
        event: DiscernmentSwitch,
        bias_direction: DailyBiasDirection,
    ):
        transition = machine.on_switch(event, bias(bias_direction))
        if transition.actions and all(
            item.kind is PongActionKind.CANCEL_PROTECTIVE_STOP
            for item in transition.actions
        ):
            for cancellation in transition.actions:
                self.assertIsNotNone(cancellation.target_action_id)
                transition = machine.on_protective_stop_cancelled(
                    action_id=cancellation.target_action_id or "",
                )
        self.assertEqual(transition.actions[0].kind, PongActionKind.REQUEST_EXIT)
        return transition

    def test_framework_uses_discernment_and_remains_disabled(self) -> None:
        status = PongStateMachine().status()
        self.assertFalse(PONG_EXECUTION_ENABLED)
        self.assertEqual(PONG_PROFILE_REGISTRATION, "NOT_REGISTERED")
        self.assertEqual(PONG_SIGNAL_SOURCE, "CAUSAL_DISCERNMENT_THRESHOLD_TRANSITIONS")
        self.assertNotIn("PERPETUAL", PONG_SIGNAL_SOURCE)
        self.assertEqual(status["default_entry_blocker"], "DISCERNMENT_PRODUCTION_NOT_CONFIGURED")
        self.assertIn("PONG_EXECUTION_DISABLED", status["blockers"])
        self.assertEqual(status["mode"], "PAPER_SIM101")
        self.assertEqual(status["account_name"], "Sim101")
        self.assertEqual(status["account_class"], "LOCAL_SIMULATION")
        self.assertEqual(status["instrument"], "MNQ SEP26")
        self.assertEqual(status["canonical_contract"], "MNQU6")
        self.assertEqual(status["live_authority"], "DISARMED")
        self.assertEqual(status["live_capital"], "DENIED")
        checkpoint = PongStateMachine().checkpoint()
        self.assertEqual(checkpoint.mode, "PAPER_SIM101")
        self.assertEqual(checkpoint.live_authority, "DISARMED")

    def test_long_cycle_exits_waits_one_leg_and_reenters(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        self.assertEqual((machine.state, machine.actual_quantity), (PongState.LONG, 1))
        opposite = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        exit_result = self._begin_exit(
            machine, opposite, DailyBiasDirection.LONG,
        )
        machine.on_exit_fill(
            action_id=exit_result.actions[0].action_id, quantity=1, fill_id="exit-long",
        )
        machine.on_signed_flat_reconciliation(
            reconciliation=reconciliation(opposite.available_at), at=opposite.available_at,
        )
        self.assertEqual(machine.state, PongState.FLAT_WAIT_BEAT)
        reentry = switch(3, PongDirection.SHORT, PongDirection.LONG, prior=opposite.event_id)
        result = machine.on_switch(reentry, bias(DailyBiasDirection.LONG))
        self.assertEqual(result.actions[0].kind, PongActionKind.REQUEST_ENTRY_LONG)

    def test_short_cycle_is_exact_mirror(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.LONG)
        first = switch(1, PongDirection.LONG, PongDirection.SHORT, prior=None)
        self._enter(machine, first, PongDirection.SHORT)
        opposite = switch(2, PongDirection.SHORT, PongDirection.LONG, prior=first.event_id)
        exited = self._begin_exit(
            machine, opposite, DailyBiasDirection.SHORT,
        )
        machine.on_exit_fill(
            action_id=exited.actions[0].action_id, quantity=1, fill_id="exit-short",
        )
        machine.on_signed_flat_reconciliation(
            reconciliation=reconciliation(opposite.available_at), at=opposite.available_at,
        )
        reentry = switch(3, PongDirection.LONG, PongDirection.SHORT, prior=opposite.event_id)
        result = machine.on_switch(reentry, bias(DailyBiasDirection.SHORT))
        self.assertEqual(result.actions[0].kind, PongActionKind.REQUEST_ENTRY_SHORT)

    def test_bias_mismatch_can_skip_any_number_of_beats(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        second = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        exited = self._begin_exit(
            machine, second, DailyBiasDirection.LONG,
        )
        machine.on_exit_fill(action_id=exited.actions[0].action_id, quantity=1, fill_id="x")
        machine.on_signed_flat_reconciliation(
            reconciliation=reconciliation(second.available_at), at=second.available_at,
        )
        third = switch(3, PongDirection.SHORT, PongDirection.LONG, prior=second.event_id)
        self.assertEqual(
            machine.on_switch(third, bias(DailyBiasDirection.SHORT)).reason_code,
            "DISCERNMENT_SWITCH_NOT_ALIGNED_WITH_DAILY_BIAS",
        )
        fourth = switch(4, PongDirection.LONG, PongDirection.SHORT, prior=third.event_id)
        self.assertEqual(machine.on_switch(fourth, bias(DailyBiasDirection.LONG)).actions, ())
        fifth = switch(5, PongDirection.SHORT, PongDirection.LONG, prior=fourth.event_id)
        self.assertEqual(
            machine.on_switch(fifth, bias(DailyBiasDirection.LONG)).actions[0].kind,
            PongActionKind.REQUEST_ENTRY_LONG,
        )

    def test_bias_score_and_reporting_updates_never_manufacture_beat(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.LONG)
        before = machine.last_switch_sequence
        self.assertEqual(machine.on_bias_update(bias(DailyBiasDirection.SHORT)).actions, ())
        self.assertEqual(machine.on_snapshot_update(snapshot(0.72, minute=2)).actions, ())
        old = machine.on_snapshot_update(snapshot(0.73, minute=1))
        self.assertEqual(old.reason_code, "OUT_OF_ORDER_DISCERNMENT_SNAPSHOT_IGNORED")
        self.assertAlmostEqual(machine.last_snapshot.effective_long_score, 0.72)
        self.assertEqual(machine.on_reporting_boundary().actions, ())
        self.assertEqual(machine.last_switch_sequence, before)

    def test_unconfigured_neutral_and_stale_bias_deny_entry(self) -> None:
        fixtures = (
            (default_unconfigured_bias(), "DAILY_BIAS_UNCONFIGURED"),
            (bias(DailyBiasDirection.NEUTRAL), "DAILY_BIAS_NOT_DIRECTIONAL"),
            (bias(DailyBiasDirection.LONG, stale=True), "DAILY_BIAS_STALE"),
        )
        for bias_value, reason in fixtures:
            with self.subTest(reason=reason):
                machine = PongStateMachine()
                seed(machine, PongDirection.SHORT)
                result = machine.on_switch(
                    switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None),
                    bias_value,
                )
                self.assertEqual((result.actions, result.reason_code), ((), reason))

    def test_pending_opposite_switch_cancels_and_never_queues_replacement(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        pending = machine.on_switch(first, bias(DailyBiasDirection.LONG))
        second = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        cancelled = machine.on_switch(second, bias(DailyBiasDirection.SHORT))
        self.assertEqual(cancelled.actions[0].kind, PongActionKind.CANCEL_PENDING_ENTRY)
        self.assertEqual(machine.state, PongState.FLAT_WAIT_ALIGNMENT)
        stale = machine.on_entry_fill(
            action_id=pending.actions[0].action_id,
            direction=PongDirection.LONG, quantity=1, fill_id="late",
        )
        self.assertEqual(stale.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(stale.actions[-1].kind, PongActionKind.REQUEST_SAFETY_EXIT)

    def test_strong_candidate_is_rechecked_and_default_caps_downgrade_to_one(self) -> None:
        discernment_policy = strong_discernment_policy()
        machine = PongStateMachine(discernment_policy=discernment_policy)
        seed(machine, PongDirection.SHORT)
        event = switch(
            1, PongDirection.SHORT, PongDirection.LONG, prior=None,
            score=0.90, policy=discernment_policy,
        )
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
        self.assertEqual(candidate.actions[0].requested_total_quantity, 2)
        admitted = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=event.available_at,
            current_snapshot=event.snapshot, current_switch_cursor=event,
            source_fresh=True, entry_gates_pass=True,
            reconciliation=reconciliation(event.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        self.assertEqual(admitted.reason_code, "ENTRY_ADMISSION_DOWNGRADED_NO_EXECUTION")
        self.assertEqual(machine.entry_target_quantity, 1)
        self.assertIn("TWO_MNQ_EXCEEDS_POSITION_RISK_LIMIT", machine.quantity_blockers)
        self.assertIn("PONG_NATIVE_QUANTITY_CAPABILITY_UNPROVEN", machine.quantity_blockers)
        self.assertNotEqual(candidate.actions[0].action_id, admitted.actions[0].action_id)
        self.assertIsNotNone(admitted.actions[0].admission_id)
        self.assertEqual(
            admitted.actions[0].quantity_policy_hash,
            PONG_QUANTITY_POLICY.configuration_hash,
        )
        self.assertEqual(admitted.actions[0].snapshot_hash, event.snapshot.result_hash)
        self.assertEqual(
            admitted.actions[0].bias_result_hash,
            bias(DailyBiasDirection.LONG).result_hash,
        )
        self.assertIsNotNone(admitted.actions[0].reconciliation_hash)

    def test_fake_two_capability_exercises_partial_fill_and_protection(self) -> None:
        fake_policy = replace(
            PONG_QUANTITY_POLICY,
            maximum_position_risk_dollars=Decimal("100"), native_maximum_quantity=2,
            native_capability_status="DETERMINISTIC_TEST_FIXTURE_ONLY",
        )
        discernment_policy = strong_discernment_policy()
        machine = PongStateMachine(fake_policy, discernment_policy)
        seed(machine, PongDirection.SHORT)
        event = switch(
            1, PongDirection.SHORT, PongDirection.LONG, prior=None,
            score=0.90, policy=discernment_policy,
        )
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
        admitted = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=event.available_at,
            current_snapshot=event.snapshot, current_switch_cursor=event,
            source_fresh=True, entry_gates_pass=True,
            reconciliation=reconciliation(event.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        action_id = admitted.actions[0].action_id
        first = machine.on_entry_fill(
            action_id=action_id,
            direction=PongDirection.LONG, quantity=1, fill_id="partial-1",
        )
        self.assertEqual(first.reason_code, "ENTRY_PARTIAL_FILL_PROTECTION_REQUIRED")
        self.assertEqual(first.actions[0].requested_total_quantity, 1)
        machine.on_protection_update(
            action_id=first.actions[0].action_id, quantity=1, signed=True,
        )
        second = machine.on_entry_fill(
            action_id=action_id,
            direction=PongDirection.LONG, quantity=1, fill_id="partial-2",
        )
        self.assertEqual(second.actions[0].requested_total_quantity, 2)
        machine.on_protection_update(
            action_id=second.actions[0].action_id, quantity=2, signed=True,
        )
        self.assertEqual((machine.state, machine.actual_quantity, machine.protected_quantity), (PongState.LONG, 2, 2))

    def test_score_can_downgrade_same_opportunity_but_not_resurrect_it(self) -> None:
        discernment_policy = strong_discernment_policy()
        machine = PongStateMachine(discernment_policy=discernment_policy)
        seed(machine, PongDirection.SHORT)
        event = switch(
            1, PongDirection.SHORT, PongDirection.LONG, prior=None,
            score=0.90, policy=discernment_policy,
        )
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
        lowered = snapshot(0.84999, minute=2, policy=discernment_policy)
        admitted = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=lowered.available_at,
            current_snapshot=lowered, current_switch_cursor=event,
            source_fresh=True, entry_gates_pass=True,
            reconciliation=reconciliation(lowered.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        self.assertEqual(admitted.reason_code, "ENTRY_ADMISSION_DOWNGRADED_NO_EXECUTION")
        self.assertEqual(machine.entry_target_quantity, 1)
        opposite = snapshot(0.30, minute=3, policy=discernment_policy)
        denied = PongStateMachine(discernment_policy=discernment_policy)
        seed(denied, PongDirection.SHORT)
        pending = denied.on_switch(event, bias(DailyBiasDirection.LONG))
        result = denied.on_entry_admission(
            action_id=pending.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=opposite.available_at,
            current_snapshot=opposite, current_switch_cursor=event,
            source_fresh=True, entry_gates_pass=True,
            reconciliation=reconciliation(opposite.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        self.assertEqual(result.reason_code, "OPPOSITE_DISCERNMENT_SWITCH_NOT_PROCESSED")
        self.assertEqual(denied.state, PongState.FLAT_WAIT_ALIGNMENT)

    def test_future_snapshot_is_not_admitted(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        event = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
        future = snapshot(0.72, minute=2)
        result = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=event.available_at,
            current_snapshot=future, current_switch_cursor=event,
            source_fresh=True, entry_gates_pass=True,
            reconciliation=reconciliation(event.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        self.assertEqual(result.reason_code, "DISCERNMENT_NOT_YET_AVAILABLE")
        self.assertEqual(machine.state, PongState.FLAT_WAIT_ALIGNMENT)

    def test_nonboolean_gate_contract_cannot_be_truthy_admission(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        event = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))

        result = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=event.available_at,
            current_snapshot=event.snapshot,
            current_switch_cursor=event,
            source_fresh="true",  # type: ignore[arg-type]
            entry_gates_pass=True,
            reconciliation=reconciliation(event.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )

        self.assertEqual(result.reason_code, "ENTRY_GATE_CONTRACT_INVALID")
        self.assertEqual(machine.state, PongState.FLAT_WAIT_ALIGNMENT)

    def test_expired_snapshot_is_not_revived_by_caller_freshness_flag(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        event = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
        evaluated_at = "2026-09-11T01:01:31Z"

        result = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=evaluated_at,
            current_snapshot=event.snapshot, current_switch_cursor=event,
            source_fresh=True,
            entry_gates_pass=True,
            reconciliation=reconciliation(evaluated_at),
            available_loss_allowance_dollars=Decimal("200"),
        )

        self.assertEqual(
            result.reason_code, "DISCERNMENT_EVIDENCE_EXPIRED_AT_ADMISSION",
        )
        self.assertEqual(machine.state, PongState.FLAT_WAIT_ALIGNMENT)

    def test_required_component_identity_is_rechecked_at_admission(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        event = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
        incomplete = snapshot(
            0.72, minute=2, include_required_components=False,
        )

        result = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=incomplete.available_at,
            current_snapshot=incomplete, current_switch_cursor=event,
            source_fresh=True,
            entry_gates_pass=True,
            reconciliation=reconciliation(incomplete.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )

        self.assertEqual(result.reason_code, "DISCERNMENT_REQUIRED_EVIDENCE_MISSING")
        self.assertEqual(machine.state, PongState.FLAT_WAIT_ALIGNMENT)
        with self.assertRaisesRegex(ValueError, "DISCERNMENT_REQUIRED_EVIDENCE_MISSING"):
            DiscernmentSwitch.create(
                policy=PONG_RESEARCH_POLICY,
                sequence=2,
                prior_switch_id=event.event_id,
                previous_direction=DiscernmentDirection.LONG,
                direction=DiscernmentDirection.SHORT,
                snapshot=snapshot(
                    0.30, minute=2, include_required_components=False,
                ),
            )

    def test_hash_consistent_snapshot_score_and_group_forgeries_are_rejected(self) -> None:
        authentic = snapshot(0.70, minute=1)

        forged_groups = tuple(
            (group, effect / 2)
            for group, effect in authentic.dependency_group_effects
        )
        group_payload = authentic.payload(include_result_hash=False)
        group_payload["dependency_group_effects"] = [
            list(item) for item in forged_groups
        ]
        group_forgery = replace(
            authentic,
            dependency_group_effects=forged_groups,
            result_hash=canonical_hash(group_payload),
        )

        forged_effective = 0.72
        forged_raw = 0.5 + (forged_effective - 0.5) / authentic.reliability
        score_payload = authentic.payload(include_result_hash=False)
        score_payload.update({
            "raw_long_score": forged_raw,
            "effective_long_score": forged_effective,
            "long_discernment_pct": forged_effective * 100,
            "short_discernment_pct": (1 - forged_effective) * 100,
        })
        score_forgery = replace(
            authentic,
            raw_long_score=forged_raw,
            effective_long_score=forged_effective,
            long_discernment_pct=forged_effective * 100,
            short_discernment_pct=(1 - forged_effective) * 100,
            result_hash=canonical_hash(score_payload),
        )

        cases = (
            (
                group_forgery,
                "DISCERNMENT_DEPENDENCY_EFFECT_DERIVATION_MISMATCH",
            ),
            (score_forgery, "DISCERNMENT_SCORE_DERIVATION_MISMATCH"),
        )
        for forged, reason in cases:
            with self.subTest(reason=reason):
                self.assertEqual(
                    forged.verification_error(
                        PONG_RESEARCH_POLICY, at=forged.available_at,
                    ),
                    reason,
                )
                with self.assertRaisesRegex(ValueError, reason):
                    DiscernmentSwitch.create(
                        policy=PONG_RESEARCH_POLICY,
                        sequence=1,
                        prior_switch_id=None,
                        previous_direction=DiscernmentDirection.SHORT,
                        direction=DiscernmentDirection.LONG,
                        snapshot=forged,
                    )

    def test_admission_uses_injected_hysteresis_thresholds(self) -> None:
        policy = replace(
            PONG_RESEARCH_POLICY,
            long_on_threshold=0.60,
            short_on_threshold=0.40,
            model_version="pong-discernment-threshold-test-fixture",
        )
        machine = PongStateMachine(discernment_policy=policy)
        seed(machine, PongDirection.SHORT)
        event = switch(
            1, PongDirection.SHORT, PongDirection.LONG, prior=None,
            score=0.61, policy=policy,
        )
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
        opposite = snapshot(0.39, minute=2, policy=policy)

        result = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=opposite.available_at,
            current_snapshot=opposite, current_switch_cursor=event,
            source_fresh=True,
            entry_gates_pass=True,
            reconciliation=reconciliation(opposite.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )

        self.assertEqual(result.reason_code, "OPPOSITE_DISCERNMENT_SWITCH_NOT_PROCESSED")
        self.assertEqual(machine.state, PongState.FLAT_WAIT_ALIGNMENT)

    def test_admission_rejects_an_intervening_switch_cursor_round_trip(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        candidate = machine.on_switch(first, bias(DailyBiasDirection.LONG))
        second = switch(
            2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id,
        )
        third = switch(
            3, PongDirection.SHORT, PongDirection.LONG, prior=second.event_id,
        )

        result = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG),
            at=third.available_at,
            current_snapshot=third.snapshot,
            current_switch_cursor=third,
            source_fresh=True,
            entry_gates_pass=True,
            reconciliation=reconciliation(third.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )

        self.assertEqual(
            result.reason_code,
            "DISCERNMENT_SWITCH_CURSOR_CHANGED_AT_ADMISSION",
        )
        self.assertEqual(machine.state, PongState.FLAT_WAIT_ALIGNMENT)

    def test_partial_position_opposite_switch_cancels_and_safety_exits(self) -> None:
        fake_policy = replace(
            PONG_QUANTITY_POLICY,
            maximum_position_risk_dollars=Decimal("100"), native_maximum_quantity=2,
            native_capability_status="DETERMINISTIC_TEST_FIXTURE_ONLY",
        )
        discernment_policy = strong_discernment_policy()
        machine = PongStateMachine(fake_policy, discernment_policy)
        seed(machine, PongDirection.SHORT)
        first = switch(
            1, PongDirection.SHORT, PongDirection.LONG, prior=None,
            score=0.90, policy=discernment_policy,
        )
        pending = machine.on_switch(first, bias(DailyBiasDirection.LONG))
        admitted = machine.on_entry_admission(
            action_id=pending.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=first.available_at,
            current_snapshot=first.snapshot, current_switch_cursor=first,
            source_fresh=True, entry_gates_pass=True,
            reconciliation=reconciliation(first.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        machine.on_entry_fill(
            action_id=admitted.actions[0].action_id,
            direction=PongDirection.LONG, quantity=1, fill_id="partial",
        )
        machine.on_protection_update(
            action_id=machine.status()["working_protective_actions"][0]["action_id"],
            quantity=1,
            signed=True,
        )
        second = switch(
            2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id,
            policy=discernment_policy,
        )
        result = machine.on_switch(second, bias(DailyBiasDirection.SHORT))
        self.assertEqual(
            [item.kind for item in result.actions],
            [
                PongActionKind.CANCEL_PENDING_ENTRY,
                PongActionKind.CANCEL_PROTECTIVE_STOP,
            ],
        )
        released = machine.on_protective_stop_cancelled(
            action_id=result.actions[-1].target_action_id or "",
        )
        self.assertEqual(
            released.actions[0].kind,
            PongActionKind.REQUEST_SAFETY_EXIT,
        )
        self.assertEqual(machine.state, PongState.SAFETY_BLOCKED)

    def test_partial_entry_confidence_loss_cancels_remainder_and_accounts_race(self) -> None:
        fake_policy = replace(
            PONG_QUANTITY_POLICY,
            maximum_position_risk_dollars=Decimal("100"),
            native_maximum_quantity=2,
            native_capability_status="DETERMINISTIC_TEST_FIXTURE_ONLY",
        )
        discernment_policy = strong_discernment_policy()
        machine = PongStateMachine(fake_policy, discernment_policy)
        seed(machine, PongDirection.SHORT)
        event = switch(
            1,
            PongDirection.SHORT,
            PongDirection.LONG,
            prior=None,
            score=0.90,
            policy=discernment_policy,
        )
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
        admitted = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG),
            at=event.available_at,
            current_snapshot=event.snapshot,
            current_switch_cursor=event,
            source_fresh=True,
            entry_gates_pass=True,
            reconciliation=reconciliation(event.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        entry = admitted.actions[0]
        first_fill = machine.on_entry_fill(
            action_id=entry.action_id,
            direction=PongDirection.LONG,
            quantity=1,
            fill_id="confidence-partial",
        )
        machine.on_protection_update(
            action_id=first_fill.actions[0].action_id,
            quantity=1,
            signed=True,
        )

        cancelled = machine.on_snapshot_update(
            snapshot(0.84, minute=2, policy=discernment_policy),
        )
        self.assertEqual(
            cancelled.reason_code,
            "ENTRY_REMAINDER_CANCELLATION_REQUESTED",
        )
        self.assertEqual(
            cancelled.actions[0].kind,
            PongActionKind.CANCEL_PENDING_ENTRY,
        )
        self.assertTrue(machine.entry_cancel_requested)

        raced = machine.on_entry_fill(
            action_id=entry.action_id,
            direction=PongDirection.LONG,
            quantity=1,
            fill_id="confidence-racing-fill",
        )
        self.assertEqual(machine.actual_quantity, 2)
        self.assertEqual(
            raced.actions[0].requested_total_quantity,
            2,
        )
        machine.on_protection_update(
            action_id=raced.actions[0].action_id,
            quantity=2,
            signed=True,
        )
        self.assertEqual(
            (machine.state, machine.actual_quantity, machine.protected_quantity),
            (PongState.LONG, 2, 2),
        )

    def test_exit_partial_fills_reprotect_and_cannot_overexit(self) -> None:
        fake_policy = replace(
            PONG_QUANTITY_POLICY,
            maximum_position_risk_dollars=Decimal("100"), native_maximum_quantity=2,
            native_capability_status="DETERMINISTIC_TEST_FIXTURE_ONLY",
        )
        discernment_policy = strong_discernment_policy()
        machine = PongStateMachine(fake_policy, discernment_policy)
        seed(machine, PongDirection.SHORT)
        first = switch(
            1, PongDirection.SHORT, PongDirection.LONG, prior=None,
            score=0.90, policy=discernment_policy,
        )
        self._enter(machine, first, PongDirection.LONG, policy=fake_policy)
        second = switch(
            2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id,
            policy=discernment_policy,
        )
        exiting = self._begin_exit(
            machine, second, DailyBiasDirection.LONG,
        )
        action = exiting.actions[0]
        self.assertEqual(action.requested_total_quantity, 2)
        partial = machine.on_exit_fill(action_id=action.action_id, quantity=1, fill_id="exit-1")
        self.assertEqual(partial.actions[0].requested_total_quantity, 1)
        reprotection = machine.on_protection_update(
            action_id=partial.actions[0].action_id, quantity=1, signed=True,
        )
        self.assertEqual(
            reprotection.actions[0].kind,
            PongActionKind.CANCEL_PROTECTIVE_STOP,
        )
        machine.on_protective_stop_cancelled(
            action_id=reprotection.actions[0].target_action_id or "",
        )
        self.assertEqual(
            machine.on_exit_fill(action_id=action.action_id, quantity=1, fill_id="exit-1").reason_code,
            "DUPLICATE_REDUCING_FILL_IGNORED",
        )
        machine.on_exit_fill(action_id=action.action_id, quantity=1, fill_id="exit-2")
        machine.on_signed_flat_reconciliation(
            reconciliation=reconciliation(second.available_at), at=second.available_at,
        )
        self.assertEqual(machine.state, PongState.FLAT_WAIT_BEAT)
        over = machine.on_exit_fill(action_id=action.action_id, quantity=1, fill_id="exit-over")
        self.assertEqual(over.state, PongState.SAFETY_BLOCKED)

    def test_reducing_overfill_records_opposite_exposure_exactly_once(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        opposite = switch(
            2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id,
        )
        exiting = self._begin_exit(
            machine, opposite, DailyBiasDirection.LONG,
        )
        action = exiting.actions[0]

        overfill = machine.on_exit_fill(
            action_id=action.action_id,
            quantity=2,
            fill_id="native-reducing-overfill",
        )

        self.assertEqual(overfill.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(
            (machine.actual_position, machine.actual_quantity),
            (PongDirection.SHORT, 1),
        )
        self.assertEqual(
            overfill.actions[-1].kind,
            PongActionKind.REQUEST_SAFETY_EXIT,
        )
        self.assertEqual(
            overfill.actions[-1].expected_direction,
            PongDirection.SHORT,
        )
        duplicate = machine.on_exit_fill(
            action_id=action.action_id,
            quantity=2,
            fill_id="native-reducing-overfill",
        )
        self.assertEqual(duplicate.reason_code, "DUPLICATE_REDUCING_FILL_IGNORED")
        self.assertEqual(
            (machine.actual_position, machine.actual_quantity),
            (PongDirection.SHORT, 1),
        )

    def test_cancelled_protective_fill_racing_exit_is_visible_and_contained(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        protective_id = machine.status()["working_protective_actions"][0]["action_id"]
        opposite = switch(
            2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id,
        )
        cancellation = machine.on_switch(
            opposite, bias(DailyBiasDirection.LONG),
        )
        self.assertEqual(
            cancellation.actions[0].kind,
            PongActionKind.CANCEL_PROTECTIVE_STOP,
        )
        released = machine.on_protective_stop_cancelled(
            action_id=protective_id,
        )
        exit_action = released.actions[0]
        machine.on_exit_fill(
            action_id=exit_action.action_id,
            quantity=1,
            fill_id="normal-exit-first",
        )

        raced = machine.on_protective_stop_fill(
            action_id=protective_id,
            quantity=1,
            fill_id="cancel-racing-protective-fill",
        )

        self.assertEqual(raced.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(
            (machine.actual_position, machine.actual_quantity),
            (PongDirection.SHORT, 1),
        )
        self.assertEqual(
            raced.actions[-1].expected_direction,
            PongDirection.SHORT,
        )
        self.assertEqual(
            machine.on_protective_stop_fill(
                action_id=protective_id,
                quantity=1,
                fill_id="cancel-racing-protective-fill",
            ).reason_code,
            "DUPLICATE_REDUCING_FILL_IGNORED",
        )

    def test_safety_escalation_keeps_existing_exit_coverage_without_overlap(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        opposite = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        exiting = self._begin_exit(
            machine, opposite, DailyBiasDirection.LONG,
        )
        original_exit = exiting.actions[0]

        escalation = machine.on_safety_exit(reason="QUOTE_STALE")
        self.assertEqual(escalation.actions, ())
        self.assertEqual(sum(machine._reducing_remaining.values()), 1)
        filled = machine.on_exit_fill(
            action_id=original_exit.action_id, quantity=1, fill_id="old-exit-arrived",
        )
        self.assertEqual(filled.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(machine.actual_quantity, 0)
        self.assertEqual(machine._reducing_remaining, {})

    def test_late_partial_entry_fill_adds_only_uncovered_safety_quantity(self) -> None:
        fake_policy = replace(
            PONG_QUANTITY_POLICY,
            maximum_position_risk_dollars=Decimal("100"), native_maximum_quantity=2,
            native_capability_status="DETERMINISTIC_TEST_FIXTURE_ONLY",
        )
        discernment_policy = strong_discernment_policy()
        machine = PongStateMachine(fake_policy, discernment_policy)
        seed(machine, PongDirection.SHORT)
        first = switch(
            1, PongDirection.SHORT, PongDirection.LONG, prior=None,
            score=0.90, policy=discernment_policy,
        )
        candidate = machine.on_switch(first, bias(DailyBiasDirection.LONG))
        admitted = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG), at=first.available_at,
            current_snapshot=first.snapshot, current_switch_cursor=first,
            source_fresh=True, entry_gates_pass=True,
            reconciliation=reconciliation(first.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        entry_action = admitted.actions[0]
        machine.on_entry_fill(
            action_id=entry_action.action_id,
            direction=PongDirection.LONG, quantity=1, fill_id="entry-first",
        )
        machine.on_protection_update(
            action_id=machine.status()["working_protective_actions"][0]["action_id"],
            quantity=1,
            signed=True,
        )
        opposite = switch(
            2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id,
            policy=discernment_policy,
        )
        cancelled = machine.on_switch(opposite, bias(DailyBiasDirection.SHORT))
        protective_cancel = cancelled.actions[-1]
        self.assertEqual(
            protective_cancel.kind,
            PongActionKind.CANCEL_PROTECTIVE_STOP,
        )
        late = machine.on_entry_fill(
            action_id=entry_action.action_id,
            direction=PongDirection.LONG, quantity=1, fill_id="entry-raced-cancel",
        )
        self.assertEqual(late.actions, ())
        released = machine.on_protective_stop_cancelled(
            action_id=protective_cancel.target_action_id or "",
        )
        safety_exit = released.actions[-1]
        self.assertEqual(safety_exit.requested_total_quantity, 2)
        self.assertEqual(sum(machine._reducing_remaining.values()), 2)
        duplicate = machine.on_entry_fill(
            action_id=entry_action.action_id,
            direction=PongDirection.LONG, quantity=1,
            fill_id="entry-raced-cancel",
        )
        self.assertEqual(duplicate.reason_code, "DUPLICATE_ENTRY_FILL_IGNORED")
        self.assertEqual(machine.actual_quantity, 2)
        self.assertEqual(sum(machine._reducing_remaining.values()), 2)
        machine.on_safety_exit_fill(
            action_id=safety_exit.action_id, quantity=1, fill_id="safety-old",
        )
        machine.on_safety_exit_fill(
            action_id=safety_exit.action_id, quantity=1, fill_id="safety-new",
        )
        self.assertEqual(machine.actual_quantity, 0)
        self.assertEqual(machine._reducing_remaining, {})

    def test_duplicate_out_of_order_gap_and_domain_change_never_enter(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        machine.on_switch(first, default_unconfigured_bias())
        self.assertEqual(machine.on_switch(first, bias(DailyBiasDirection.LONG)).reason_code, "DUPLICATE_SWITCH_IGNORED")
        gap = machine.on_switch(
            switch(3, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id),
            bias(DailyBiasDirection.SHORT),
        )
        self.assertEqual(gap.state, PongState.SAFETY_BLOCKED)
        other = PongStateMachine()
        seed(other, PongDirection.SHORT)
        changed = switch(
            1, PongDirection.SHORT, PongDirection.LONG, prior=None, domain="new-domain",
        )
        self.assertEqual(other.on_switch(changed, bias(DailyBiasDirection.LONG)).actions, ())
        self.assertEqual(other.state, PongState.SAFETY_BLOCKED)

    def test_switch_identity_and_threshold_contract_are_pinned(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        valid = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        tampered = replace(
            valid,
            event_id="pong-discernment-switch-00000000000000000000000000000000",
        )
        result = machine.on_switch(tampered, bias(DailyBiasDirection.LONG))
        self.assertEqual(result.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(machine.safety_reason, "DISCERNMENT_SWITCH_IDENTITY_MISMATCH")
        with self.assertRaisesRegex(ValueError, "opposite threshold transition"):
            DiscernmentSwitch.create(
                policy=PONG_RESEARCH_POLICY,
                sequence=1,
                prior_switch_id=None,
                previous_direction=DiscernmentDirection.SHORT,
                direction=DiscernmentDirection.LONG,
                snapshot=snapshot(0.40, minute=1),
            )

    def test_unavailable_snapshot_while_positioned_requests_safety_exit(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        invalid = machine.on_snapshot_update(snapshot(None, minute=2, valid=False))
        self.assertEqual(invalid.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(
            invalid.actions[0].kind,
            PongActionKind.CANCEL_PROTECTIVE_STOP,
        )

    def test_protection_mismatch_latches_safety_and_uses_actual_quantity(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        result = machine.on_switch(first, bias(DailyBiasDirection.LONG))
        action = result.actions[0]
        admitted = machine.on_entry_admission(
            action_id=action.action_id, bias=bias(DailyBiasDirection.LONG),
            at=first.available_at, current_snapshot=first.snapshot,
            current_switch_cursor=first,
            source_fresh=True, entry_gates_pass=True,
            reconciliation=reconciliation(first.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        protection = machine.on_entry_fill(
            action_id=admitted.actions[0].action_id, direction=PongDirection.LONG,
            quantity=1, fill_id="entry",
        )
        mismatch = machine.on_protection_update(
            action_id=protection.actions[0].action_id, quantity=0, signed=True,
        )
        self.assertEqual(mismatch.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(
            mismatch.actions[0].kind,
            PongActionKind.CANCEL_PROTECTIVE_STOP,
        )
        released = machine.on_protective_stop_cancelled(
            action_id=mismatch.actions[0].target_action_id or "",
        )
        self.assertEqual(released.actions[0].requested_total_quantity, 1)

    def test_unexpected_overfill_is_preserved_and_reduced_at_actual_quantity(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        event = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        candidate = machine.on_switch(event, bias(DailyBiasDirection.LONG))
        result = machine.on_entry_fill(
            action_id=candidate.actions[0].action_id,
            direction=PongDirection.LONG, quantity=3, fill_id="native-overfill",
        )
        self.assertEqual(machine.actual_quantity, 3)
        self.assertEqual(result.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(result.actions[-1].requested_total_quantity, 3)

    def test_safety_exit_retains_lockout_after_flat_and_later_switches(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        safety = machine.on_safety_exit(reason="QUOTE_STALE")
        self.assertEqual(
            safety.actions[0].kind,
            PongActionKind.CANCEL_PROTECTIVE_STOP,
        )
        self.assertEqual(
            machine.on_signed_flat_reconciliation(
                reconciliation=reconciliation(first.available_at), at=first.available_at,
            ).reason_code,
            "SAFETY_FLAT_NOT_CONFIRMED",
        )
        released = machine.on_protective_stop_cancelled(
            action_id=safety.actions[0].target_action_id or "",
        )
        self.assertEqual(
            released.actions[0].kind,
            PongActionKind.REQUEST_SAFETY_EXIT,
        )
        machine.on_safety_exit_fill(
            action_id=released.actions[0].action_id,
            quantity=1,
            fill_id="safe-exit",
        )
        machine.on_signed_flat_reconciliation(
            reconciliation=reconciliation(first.available_at), at=first.available_at,
        )
        second = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        later = machine.on_switch(second, bias(DailyBiasDirection.SHORT))
        self.assertEqual((later.actions, later.reason_code), ((), "SAFETY_LOCKOUT_RETAINED"))

    def test_flat_checkpoint_restores_consumed_switch_continuity(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        event = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        current_bias = bias(DailyBiasDirection.SHORT)
        result = machine.on_switch(event, current_bias)
        self.assertEqual(result.reason_code, "DISCERNMENT_SWITCH_NOT_ALIGNED_WITH_DAILY_BIAS")
        checkpoint = machine.checkpoint()

        restored = PongStateMachine.restore_checkpoint(
            checkpoint,
            current_snapshot=event.snapshot,
            current_bias=current_bias,
            reconciliation=reconciliation(event.available_at),
            at=event.available_at,
        )
        self.assertEqual(restored.state, PongState.FLAT_WAIT_ALIGNMENT)
        self.assertEqual(restored.last_consumed_switch_id, event.event_id)
        self.assertEqual(restored.last_switch_sequence, 1)
        self.assertEqual(
            restored.on_switch(event, current_bias).reason_code,
            "DUPLICATE_SWITCH_IGNORED",
        )

    def test_repeated_restore_preserves_consumed_switch_continuity(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        event = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        current_bias = bias(DailyBiasDirection.SHORT)
        machine.on_switch(event, current_bias)

        first_restore = PongStateMachine.restore_checkpoint(
            serialize_pong_checkpoint(machine.checkpoint()),
            current_snapshot=event.snapshot,
            current_bias=current_bias,
            reconciliation=reconciliation(event.available_at),
            at=event.available_at,
        )
        second_restore = PongStateMachine.restore_checkpoint(
            serialize_pong_checkpoint(first_restore.checkpoint()),
            current_snapshot=event.snapshot,
            current_bias=current_bias,
            reconciliation=reconciliation(event.available_at),
            at=event.available_at,
        )

        self.assertEqual(second_restore.state, PongState.FLAT_WAIT_ALIGNMENT)
        self.assertEqual(second_restore.last_consumed_switch_id, event.event_id)
        self.assertEqual(second_restore.last_switch_sequence, event.sequence)
        self.assertEqual(
            second_restore.on_switch(event, current_bias).reason_code,
            "DUPLICATE_SWITCH_IGNORED",
        )

    def test_prior_lifecycle_fill_id_survives_new_candidate_and_repeated_restore(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        candidate = machine.on_switch(first, bias(DailyBiasDirection.LONG))
        admitted = machine.on_entry_admission(
            action_id=candidate.actions[0].action_id,
            bias=bias(DailyBiasDirection.LONG),
            at=first.available_at,
            current_snapshot=first.snapshot,
            current_switch_cursor=first,
            source_fresh=True,
            entry_gates_pass=True,
            reconciliation=reconciliation(first.available_at),
            available_loss_allowance_dollars=Decimal("200"),
        )
        entry_action = admitted.actions[0]
        old_fill_id = "prior-lifecycle-entry-fill"
        entered = machine.on_entry_fill(
            action_id=entry_action.action_id,
            direction=PongDirection.LONG,
            quantity=1,
            fill_id=old_fill_id,
        )
        machine.on_protection_update(
            action_id=entered.actions[0].action_id,
            quantity=1,
            signed=True,
        )
        second = switch(
            2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id,
        )
        exiting = self._begin_exit(
            machine, second, DailyBiasDirection.LONG,
        )
        machine.on_exit_fill(
            action_id=exiting.actions[0].action_id,
            quantity=1,
            fill_id="prior-lifecycle-exit-fill",
        )
        machine.on_signed_flat_reconciliation(
            reconciliation=reconciliation(second.available_at),
            at=second.available_at,
        )
        third = switch(
            3, PongDirection.SHORT, PongDirection.LONG, prior=second.event_id,
        )
        current_bias = bias(DailyBiasDirection.LONG)
        machine.on_switch(third, current_bias)

        first_restore = PongStateMachine.restore_checkpoint(
            serialize_pong_checkpoint(machine.checkpoint()),
            current_snapshot=third.snapshot,
            current_bias=current_bias,
            reconciliation=reconciliation(third.available_at),
            at=third.available_at,
        )
        second_restore = PongStateMachine.restore_checkpoint(
            serialize_pong_checkpoint(first_restore.checkpoint()),
            current_snapshot=third.snapshot,
            current_bias=current_bias,
            reconciliation=reconciliation(third.available_at),
            at=third.available_at,
        )

        duplicate = second_restore.on_entry_fill(
            action_id=entry_action.action_id,
            direction=PongDirection.LONG,
            quantity=1,
            fill_id=old_fill_id,
        )
        self.assertEqual(duplicate.reason_code, "DUPLICATE_ENTRY_FILL_IGNORED")
        self.assertEqual(
            (second_restore.actual_position, second_restore.actual_quantity),
            (PongDirection.FLAT, 0),
        )

    def test_truncated_checkpoint_never_defaults_to_clean_state(self) -> None:
        encoded = serialize_pong_checkpoint(PongStateMachine().checkpoint())
        truncated = encoded[:-17]

        restored = PongStateMachine.restore_checkpoint(
            truncated,
            current_snapshot=None,
            current_bias=default_unconfigured_bias(),
            reconciliation=reconciliation("2026-09-11T01:00:00Z"),
            at="2026-09-11T01:00:00Z",
        )

        self.assertEqual(restored.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(restored.safety_reason, "PONG_CHECKPOINT_INTEGRITY_FAILED")
        self.assertEqual(
            restored.recovery_blockers, ("PONG_CHECKPOINT_INTEGRITY_FAILED",),
        )
        repeated = PongStateMachine.restore_checkpoint(
            serialize_pong_checkpoint(restored.checkpoint()),
            current_snapshot=None,
            current_bias=default_unconfigured_bias(),
            reconciliation=reconciliation("2026-09-11T01:00:00Z"),
            at="2026-09-11T01:00:00Z",
        )
        self.assertEqual(repeated.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(repeated.safety_reason, "PONG_CHECKPOINT_INTEGRITY_FAILED")

    def test_checkpoint_codec_is_canonical_strict_and_hash_bound(self) -> None:
        checkpoint = PongStateMachine().checkpoint()
        encoded = serialize_pong_checkpoint(checkpoint)
        self.assertEqual(deserialize_pong_checkpoint(encoded), checkpoint)
        self.assertEqual(serialize_pong_checkpoint(deserialize_pong_checkpoint(encoded)), encoded)

        unknown = json.loads(encoded)
        unknown["unexpected"] = True
        with self.assertRaisesRegex(ValueError, "missing or unknown"):
            deserialize_pong_checkpoint(json.dumps(unknown, separators=(",", ":")))

        missing = json.loads(encoded)
        missing.pop("state")
        with self.assertRaisesRegex(ValueError, "missing or unknown"):
            deserialize_pong_checkpoint(json.dumps(missing, separators=(",", ":")))

        tampered = json.loads(encoded)
        tampered["actual_quantity"] = 1
        with self.assertRaisesRegex(ValueError, "PONG_CHECKPOINT_INTEGRITY_FAILED"):
            deserialize_pong_checkpoint(json.dumps(
                tampered, sort_keys=True, separators=(",", ":"),
            ))

        with self.assertRaisesRegex(ValueError, "NOT_CANONICAL"):
            deserialize_pong_checkpoint(json.dumps(json.loads(encoded), indent=2))

    def test_checkpoint_integrity_failure_latches_new_lockout(self) -> None:
        machine = PongStateMachine()
        checkpoint = machine.checkpoint()
        tampered = replace(checkpoint, last_switch_sequence=99)
        restored = PongStateMachine.restore_checkpoint(
            tampered,
            current_snapshot=None,
            current_bias=default_unconfigured_bias(),
            reconciliation=reconciliation("2026-09-11T01:00:00Z"),
            at="2026-09-11T01:00:00Z",
        )
        self.assertEqual(restored.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(restored.safety_reason, "PONG_CHECKPOINT_INTEGRITY_FAILED")

    def test_restart_retains_existing_lockout(self) -> None:
        machine = PongStateMachine()
        machine.on_safety_exit(reason="QUOTE_STALE")
        checkpoint = machine.checkpoint()
        restored = PongStateMachine.restore_checkpoint(
            checkpoint,
            current_snapshot=None,
            current_bias=default_unconfigured_bias(),
            reconciliation=reconciliation("2026-09-11T01:00:00Z"),
            at="2026-09-11T01:00:00Z",
        )
        self.assertEqual(restored.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(restored.safety_reason, "QUOTE_STALE")

    def test_pending_checkpoint_never_resumes_entry_after_restart(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        event = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        current_bias = bias(DailyBiasDirection.LONG)
        machine.on_switch(event, current_bias)
        checkpoint = machine.checkpoint()
        restored = PongStateMachine.restore_checkpoint(
            checkpoint,
            current_snapshot=event.snapshot,
            current_bias=current_bias,
            reconciliation=reconciliation(event.available_at),
            at=event.available_at,
        )
        self.assertEqual(restored.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(restored.safety_reason, "PONG_RESTART_PENDING_ACTION_UNRESOLVED")
        self.assertIsNotNone(restored.pending_action)
        assert restored.pending_action is not None
        self.assertEqual(restored.pending_action.action_id, checkpoint.pending_action.action_id)
        self.assertIn(
            "PONG_RESTART_PENDING_ACTION_UNRESOLVED",
            restored.recovery_blockers,
        )
        self.assertEqual(
            restored.on_switch(event, current_bias).actions,
            (),
        )

        repeated = PongStateMachine.restore_checkpoint(
            serialize_pong_checkpoint(restored.checkpoint()),
            current_snapshot=event.snapshot,
            current_bias=current_bias,
            reconciliation=reconciliation(event.available_at),
            at=event.available_at,
        )
        self.assertEqual(repeated.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(repeated.pending_action.action_id, restored.pending_action.action_id)

        late = repeated.on_entry_fill(
            action_id=restored.pending_action.action_id,
            direction=PongDirection.LONG,
            quantity=1,
            fill_id="restart-late-entry",
        )
        self.assertEqual(late.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(late.actions[-1].kind, PongActionKind.REQUEST_SAFETY_EXIT)
        self.assertEqual(repeated.actual_quantity, 1)
        duplicate = repeated.on_entry_fill(
            action_id=restored.pending_action.action_id,
            direction=PongDirection.LONG,
            quantity=1,
            fill_id="restart-late-entry",
        )
        self.assertEqual(duplicate.reason_code, "DUPLICATE_ENTRY_FILL_IGNORED")
        self.assertEqual(repeated.actual_quantity, 1)

    def test_nonflat_restore_quarantines_reducing_identity_across_restarts(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.SHORT)
        first = switch(1, PongDirection.SHORT, PongDirection.LONG, prior=None)
        self._enter(machine, first, PongDirection.LONG)
        opposite = switch(2, PongDirection.LONG, PongDirection.SHORT, prior=first.event_id)
        exiting = self._begin_exit(
            machine, opposite, DailyBiasDirection.LONG,
        )
        exit_action = exiting.actions[0]

        restored = PongStateMachine.restore_checkpoint(
            serialize_pong_checkpoint(machine.checkpoint()),
            current_snapshot=opposite.snapshot,
            current_bias=bias(DailyBiasDirection.LONG),
            reconciliation=reconciliation(
                opposite.available_at, position_quantity=1,
            ),
            at=opposite.available_at,
        )
        self.assertEqual(restored.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(restored._reducing_remaining, {exit_action.action_id: 1})
        self.assertIn("PONG_RESTART_NONFLAT_RECOVERY_REQUIRED", restored.recovery_blockers)

        repeated = PongStateMachine.restore_checkpoint(
            serialize_pong_checkpoint(restored.checkpoint()),
            current_snapshot=opposite.snapshot,
            current_bias=bias(DailyBiasDirection.LONG),
            reconciliation=reconciliation(
                opposite.available_at, position_quantity=1,
            ),
            at=opposite.available_at,
        )
        self.assertEqual(repeated._reducing_remaining, {exit_action.action_id: 1})
        fill = repeated.on_exit_fill(
            action_id=exit_action.action_id,
            quantity=1,
            fill_id="restart-exit-fill",
        )
        self.assertEqual(fill.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(repeated.actual_quantity, 0)
        self.assertEqual(
            repeated.on_exit_fill(
                action_id=exit_action.action_id,
                quantity=1,
                fill_id="restart-exit-fill",
            ).reason_code,
            "DUPLICATE_REDUCING_FILL_IGNORED",
        )

    def test_restore_failure_preserves_existing_lockout_reason_and_proof(self) -> None:
        seeded = PongStateMachine()
        seed(seeded, PongDirection.LONG)
        seeded.on_safety_exit(reason="QUOTE_STALE")
        checkpoint = seeded.checkpoint()
        cases = (
            (
                "PONG_RESTART_DAILY_BIAS_PROOF_MISMATCH",
                bias(DailyBiasDirection.LONG), checkpoint.last_snapshot_hash,
                reconciliation("2026-09-11T01:00:00Z"),
                "2026-09-11T01:00:00Z",
            ),
            (
                "PONG_RESTART_DISCERNMENT_PROOF_MISMATCH",
                default_unconfigured_bias(), None,
                reconciliation("2026-09-11T01:00:00Z"),
                "2026-09-11T01:00:00Z",
            ),
            (
                "PONG_RESTART_RECONCILIATION_REQUIRED",
                default_unconfigured_bias(), checkpoint.last_snapshot_hash,
                reconciliation(
                    "2026-09-11T01:00:00Z", working_owned_orders=1,
                ),
                "2026-09-11T01:00:00Z",
            ),
        )
        for reason, current_bias, snapshot_marker, proof, at in cases:
            with self.subTest(reason=reason):
                current_snapshot = (
                    seeded.last_snapshot if snapshot_marker is not None else None
                )
                restored = PongStateMachine.restore_checkpoint(
                    serialize_pong_checkpoint(checkpoint),
                    current_snapshot=current_snapshot,
                    current_bias=current_bias,
                    reconciliation=proof,
                    at=at,
                )
                self.assertEqual(restored.state, PongState.SAFETY_BLOCKED)
                self.assertEqual(restored.safety_reason, "QUOTE_STALE")
                self.assertIn(reason, restored.recovery_blockers)
                preserved = restored.checkpoint()
                self.assertEqual(
                    preserved.last_snapshot_hash, checkpoint.last_snapshot_hash,
                )
                self.assertEqual(
                    preserved.daily_bias_result_hash,
                    checkpoint.daily_bias_result_hash,
                )
                repeated = PongStateMachine.restore_checkpoint(
                    serialize_pong_checkpoint(preserved),
                    current_snapshot=current_snapshot,
                    current_bias=current_bias,
                    reconciliation=proof,
                    at=at,
                )
                self.assertEqual(repeated.state, PongState.SAFETY_BLOCKED)
                self.assertEqual(repeated.safety_reason, "QUOTE_STALE")
                self.assertIn(reason, repeated.recovery_blockers)

    def test_restart_requires_signed_matching_reconciliation(self) -> None:
        machine = PongStateMachine()
        checkpoint = machine.checkpoint()
        restored = PongStateMachine.restore_checkpoint(
            checkpoint,
            current_snapshot=None,
            current_bias=default_unconfigured_bias(),
            reconciliation=reconciliation(
                "2026-09-11T01:00:00Z", working_owned_orders=1,
            ),
            at="2026-09-11T01:00:00Z",
        )
        self.assertEqual(restored.state, PongState.SAFETY_BLOCKED)
        self.assertEqual(restored.safety_reason, "PONG_RESTART_RECONCILIATION_REQUIRED")

    def test_restart_rejects_stale_or_wrong_identity_reconciliation(self) -> None:
        checkpoint = PongStateMachine().checkpoint()
        for proof, at in (
            (
                reconciliation("2026-09-11T01:00:00Z"),
                "2026-09-11T01:00:06Z",
            ),
            (
                PongReconciliationProof.create(
                    source_hash="a" * 64,
                    observed_at="2026-09-11T01:00:00Z",
                    available_at="2026-09-11T01:00:00Z",
                    position_quantity=0,
                    working_owned_orders=0,
                    unresolved_execution=False,
                    account_name="NotSim101",
                ),
                "2026-09-11T01:00:00Z",
            ),
        ):
            with self.subTest(proof=proof.source_id):
                restored = PongStateMachine.restore_checkpoint(
                    checkpoint,
                    current_snapshot=None,
                    current_bias=default_unconfigured_bias(),
                    reconciliation=proof,
                    at=at,
                )
                self.assertEqual(restored.state, PongState.SAFETY_BLOCKED)
                self.assertEqual(
                    restored.safety_reason,
                    "PONG_RESTART_RECONCILIATION_REQUIRED",
                )

    def test_status_projection_is_explicit_and_never_armed(self) -> None:
        machine = PongStateMachine()
        seed(machine, PongDirection.LONG)
        machine.on_snapshot_update(snapshot(0.73, minute=1))
        status = machine.status()
        expected = {
            "effective_long_discernment", "effective_short_discernment",
            "latched_logical_direction", "discernment_validity",
            "discernment_calibration_status", "daily_bias_direction",
            "volume_regime", "score_authorized_quantity",
            "risk_admitted_quantity", "actual_quantity", "waiting_state",
            "last_switch_id", "blockers",
        }
        self.assertTrue(expected.issubset(status))
        self.assertAlmostEqual(status["effective_short_discernment"], 0.27)
        self.assertIn(STRONG_THRESHOLD_UNREACHABLE, status["blockers"])
        self.assertFalse(status["execution_enabled"])


if __name__ == "__main__":
    unittest.main()
