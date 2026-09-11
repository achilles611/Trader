from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3g_paper.daily_bias import DailyBiasDirection, default_unconfigured_bias
from src.l3g_paper.pong import PongDirection, PongState, PongStateMachine
from src.l3g_paper.pong_quantity import PONG_QUANTITY_POLICY, PongReconciliationProof
from src.l3g_paper.pong_runtime import (
    PongLoadedRuntimeIdentity,
    PongNativeDirectiveKind,
    PongNativeEventProof,
    PongRuntimeBridge,
    PongRuntimeBridgeIntegrityError,
)
from src.lane_iii.contracts import canonical_hash
from tests.test_l3g_pong import (
    bias,
    seed,
    snapshot,
    strong_discernment_policy,
    switch,
)


SOURCE = "c" * 64
BUILD = "d" * 64
AT = "2026-09-11T01:01:00Z"


def runtime_identity(
    *, source: str = SOURCE, build: str = BUILD,
) -> PongLoadedRuntimeIdentity:
    return PongLoadedRuntimeIdentity(
        execution_session_id="l3g-es-current-loaded-addon",
        addon_protocol_version="lane-iii-paper-execution-v3",
        addon_source_fingerprint=source,
        addon_build_fingerprint=build,
        verified_at=AT,
        attestation_verified=True,
    )


def native_proof(receipt: str, *, source: str = SOURCE) -> PongNativeEventProof:
    return PongNativeEventProof(
        receipt_id=receipt,
        observed_at=AT,
        source_fingerprint=source,
        build_fingerprint=BUILD,
        execution_session_id="l3g-es-current-loaded-addon",
        signature_verified=True,
    )


def quantity_policy():
    return replace(
        PONG_QUANTITY_POLICY,
        reconciliation_source_fingerprint=SOURCE,
        reconciliation_build_fingerprint=BUILD,
        native_capability_status="CURRENT_AUTHENTICATED_ADDON_ONE_MNQ",
    )


def reconciliation(
    at: str = AT, *, position: int = 0, orders: int = 0,
) -> PongReconciliationProof:
    return PongReconciliationProof.create(
        source_fingerprint=SOURCE,
        build_fingerprint=BUILD,
        source_hash=canonical_hash({"runtime_reconciliation": at}),
        observed_at=at,
        available_at=at,
        position_quantity=position,
        working_owned_orders=orders,
        unresolved_execution=False,
    )


def admitted_long(*, strong: bool = False):
    discernment_policy = strong_discernment_policy() if strong else None
    machine = (
        PongStateMachine(quantity_policy(), discernment_policy)
        if discernment_policy is not None
        else PongStateMachine(quantity_policy())
    )
    seed(machine, PongDirection.SHORT)
    event = switch(
        1,
        PongDirection.SHORT,
        PongDirection.LONG,
        prior=None,
        score=0.90 if strong else 0.70,
        policy=machine.discernment_policy,
    )
    current_bias = bias(DailyBiasDirection.LONG)
    candidate = machine.on_switch(event, current_bias)
    admission = machine.on_entry_admission(
        action_id=candidate.actions[0].action_id,
        bias=current_bias,
        at=event.available_at,
        current_snapshot=event.snapshot,
        current_switch_cursor=event,
        source_fresh=True,
        entry_gates_pass=True,
        reconciliation=reconciliation(event.available_at),
        available_loss_allowance_dollars=Decimal("200"),
    )
    return machine, event, current_bias, candidate, admission


class PongRuntimeBridgeTests(unittest.TestCase):
    def test_desired_two_is_visible_but_exactly_one_native_entry_is_emitted(self) -> None:
        with TemporaryDirectory() as temp:
            machine, _, _, _, admission = admitted_long(strong=True)
            self.assertEqual(admission.actions[0].score_authorized_quantity, 2)
            self.assertEqual(admission.actions[0].requested_total_quantity, 1)
            bridge = PongRuntimeBridge.create(
                machine,
                Path(temp) / "bridge.json",
                runtime_identity=runtime_identity(),
            )
            update = bridge.apply_transition(admission)
            self.assertEqual(len(update.directives), 1)
            directive = update.directives[0]
            self.assertEqual(directive.kind, PongNativeDirectiveKind.ENTER_LONG)
            self.assertEqual(directive.quantity, 1)
            self.assertEqual(directive.score_desired_quantity, 2)
            self.assertEqual(directive.admitted_total_quantity, 1)
            self.assertEqual(bridge.status()["native_execution_cap"], 1)

    def test_entry_ack_fill_and_native_protection_checkpoint_exactly_once(self) -> None:
        with TemporaryDirectory() as temp:
            machine, _, _, _, admission = admitted_long()
            bridge = PongRuntimeBridge.create(
                machine, Path(temp) / "bridge.json",
                runtime_identity=runtime_identity(),
            )
            entry = bridge.apply_transition(admission).directives[0]
            bridge.mark_dispatched(entry.directive_id)
            acknowledged = bridge.on_command_ack(
                entry.directive_id, native_proof("receipt-entry-ack"),
            )
            self.assertEqual(acknowledged.transition.reason_code, "PONG_NATIVE_COMMAND_ACKNOWLEDGED")
            duplicate = bridge.on_command_ack(
                entry.directive_id, native_proof("receipt-entry-ack"),
            )
            self.assertTrue(duplicate.duplicate_receipt)

            filled = bridge.on_entry_fill(
                entry.directive_id,
                native_proof("receipt-entry-fill"),
                quantity=1,
                fill_id="native-entry-fill-1",
            )
            self.assertEqual(len(filled.directives), 1)
            protection = filled.directives[0]
            self.assertEqual(
                protection.kind, PongNativeDirectiveKind.VERIFY_NATIVE_PROTECTION,
            )
            protected = bridge.on_protection_working(
                protection.directive_id,
                native_proof("receipt-protection-working"),
                quantity=1,
            )
            self.assertEqual(protected.transition.reason_code, "ACTUAL_QUANTITY_PROTECTED")
            self.assertEqual(machine.protected_quantity, 1)
            self.assertTrue(bridge.path.exists())

    def test_exit_cancel_fill_and_signed_flat_reconciliation_reach_wait_beat(self) -> None:
        with TemporaryDirectory() as temp:
            machine, first, _, _, admission = admitted_long()
            bridge = PongRuntimeBridge.create(
                machine, Path(temp) / "bridge.json",
                runtime_identity=runtime_identity(),
            )
            entry = bridge.apply_transition(admission).directives[0]
            filled = bridge.on_entry_fill(
                entry.directive_id,
                native_proof("entry-fill"),
                quantity=1,
                fill_id="entry-fill-id",
            )
            protection = filled.directives[0]
            bridge.on_protection_working(
                protection.directive_id,
                native_proof("protection-working"),
                quantity=1,
            )

            opposite = switch(
                2, PongDirection.LONG, PongDirection.SHORT,
                prior=first.event_id,
            )
            exit_start = machine.on_switch(
                opposite, bias(DailyBiasDirection.LONG),
            )
            cancel = bridge.apply_transition(exit_start).directives[0]
            self.assertEqual(cancel.kind, PongNativeDirectiveKind.CANCEL_PROTECTIVE_STOP)
            released = bridge.on_protective_cancelled(
                cancel.directive_id, native_proof("protective-cancelled"),
            )
            self.assertEqual(released.directives[0].kind, PongNativeDirectiveKind.EXIT)
            exit_directive = released.directives[0]
            exited = bridge.on_reducing_fill(
                exit_directive.directive_id,
                native_proof("exit-fill"),
                quantity=1,
                fill_id="exit-fill-id",
            )
            self.assertIn("AWAITING_SIGNED_FLAT_RECONCILIATION", exited.transition.reason_code)
            final = bridge.on_signed_flat_reconciliation(
                reconciliation(opposite.available_at),
                native_proof("flat-reconciliation"),
                at=opposite.available_at,
            )
            self.assertEqual(final.transition.reason_code, "EXIT_COMPLETE_WAITING_ONE_FULL_LEG")
            self.assertEqual(machine.state, PongState.FLAT_WAIT_BEAT)
            self.assertIsNotNone(bridge.status()["last_reconciliation_hash"])

    def test_loaded_runtime_identity_is_dynamic_bound_and_mismatch_is_rejected(self) -> None:
        with TemporaryDirectory() as temp:
            machine = PongStateMachine(quantity_policy())
            bridge = PongRuntimeBridge.create(
                machine, Path(temp) / "bridge.json",
                runtime_identity=runtime_identity(),
            )
            restored = PongRuntimeBridge.restore(
                bridge.path,
                current_snapshot=None,
                current_bias=default_unconfigured_bias(),
                reconciliation=reconciliation(),
                current_runtime_identity=runtime_identity(),
                quantity_policy=quantity_policy(),
                at=AT,
            )
            self.assertEqual(
                restored.status()["runtime_identity_hash"],
                runtime_identity().identity_hash,
            )
            with self.assertRaises(PongRuntimeBridgeIntegrityError):
                PongRuntimeBridge.restore(
                    bridge.path,
                    current_snapshot=None,
                    current_bias=default_unconfigured_bias(),
                    reconciliation=reconciliation(),
                    current_runtime_identity=runtime_identity(source="e" * 64),
                    quantity_policy=quantity_policy(),
                    at=AT,
                )

    def test_dispatched_unresolved_directive_is_not_resubmitted_after_restart(self) -> None:
        with TemporaryDirectory() as temp:
            machine, event, current_bias, _, admission = admitted_long()
            bridge = PongRuntimeBridge.create(
                machine, Path(temp) / "bridge.json",
                runtime_identity=runtime_identity(),
            )
            entry = bridge.apply_transition(admission).directives[0]
            bridge.mark_dispatched(entry.directive_id)
            restored = PongRuntimeBridge.restore(
                bridge.path,
                current_snapshot=event.snapshot,
                current_bias=current_bias,
                reconciliation=reconciliation(event.available_at),
                current_runtime_identity=runtime_identity(),
                quantity_policy=quantity_policy(),
                at=event.available_at,
            )
            self.assertEqual(restored.undispatched_directives(), ())
            self.assertEqual(
                restored.status()["safety_blocker"],
                "PONG_NATIVE_RESTART_RECONCILIATION_REQUIRED",
            )
            self.assertEqual(restored.machine.state, PongState.SAFETY_BLOCKED)

    def test_invalid_event_attestation_latches_safety_and_tamper_fails_restore(self) -> None:
        with TemporaryDirectory() as temp:
            machine, _, _, _, admission = admitted_long()
            bridge = PongRuntimeBridge.create(
                machine, Path(temp) / "bridge.json",
                runtime_identity=runtime_identity(),
            )
            entry = bridge.apply_transition(admission).directives[0]
            with self.assertRaisesRegex(ValueError, "PONG_NATIVE_EVENT_IDENTITY_MISMATCH"):
                bridge.on_entry_fill(
                    entry.directive_id,
                    native_proof("bad-event", source="e" * 64),
                    quantity=1,
                    fill_id="bad-fill",
                )
            self.assertEqual(machine.state, PongState.SAFETY_BLOCKED)

            payload = json.loads(bridge.path.read_text(encoding="ascii"))
            payload["directive_sequence"] += 1
            bridge.path.write_text(
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                encoding="ascii",
            )
            with self.assertRaises(PongRuntimeBridgeIntegrityError):
                PongRuntimeBridge.restore(
                    bridge.path,
                    current_snapshot=None,
                    current_bias=default_unconfigured_bias(),
                    reconciliation=reconciliation(),
                    current_runtime_identity=runtime_identity(),
                    quantity_policy=quantity_policy(),
                    at=AT,
                )


if __name__ == "__main__":
    unittest.main()
