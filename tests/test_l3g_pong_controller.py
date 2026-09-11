from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3g_paper.pong_controller import (
    PONG_BLACKOUT_END_UTC,
    PONG_BLACKOUT_START_UTC,
    PONG_EXPERIMENT_END_NEW_YORK,
    PONG_EXPERIMENT_END_UTC,
    PongBoundedController,
    PongControllerCommandKind,
    PongControllerIntegrityError,
    PongControllerLeaseError,
    PongControllerObservation,
    PongControllerPhase,
    read_controller_entry_fence,
)


def observation(
    at: str,
    *,
    fence: bool = False,
    position: int = 0,
    owned: int = 0,
    entries: int = 0,
    unresolved: bool = False,
    reconciled: bool = True,
    protected: bool = False,
    healthy: bool = True,
    lockout: bool = False,
    active: bool = True,
    sealed: bool = False,
) -> PongControllerObservation:
    return PongControllerObservation(
        observed_at=at,
        entry_fence_active=fence,
        position_quantity=position,
        working_owned_orders=owned,
        working_entry_orders=entries,
        unresolved_execution=unresolved,
        reconciliation_current=reconciled,
        protection_current=protected,
        entry_gates_healthy=healthy,
        safety_lockout=lockout,
        runtime_active=active,
        evidence_sealed=sealed,
    )


class PongBoundedControllerTests(unittest.TestCase):
    def make(self, root: str, at: str = "2026-09-11T13:20:00Z"):
        return PongBoundedController.open(
            Path(root) / "pong-controller.json",
            experiment_id="pong-ny-open-20260911",
            at=at,
            pid=4101,
            process_instance_id="pong-controller-process-test-one",
        )

    def settle(
        self,
        controller: PongBoundedController,
        command,
        at: str,
    ) -> None:
        controller.mark_dispatched(command.command_id, at=at)
        controller.acknowledge_command(
            command.command_id, at=at, accepted=True,
        )

    def test_fixed_identity_pid_deadlines_and_restart_are_durable(self) -> None:
        with TemporaryDirectory() as temp:
            controller = self.make(temp)
            original_id = controller.state.controller_id
            self.assertEqual(controller.state.pid, 4101)
            self.assertEqual(controller.state.blackout_start_utc, PONG_BLACKOUT_START_UTC)
            self.assertEqual(controller.state.blackout_end_utc, PONG_BLACKOUT_END_UTC)
            self.assertEqual(controller.state.experiment_end_utc, PONG_EXPERIMENT_END_UTC)
            self.assertEqual(PONG_EXPERIMENT_END_NEW_YORK, "2026-09-11T10:35:00-04:00")
            with self.assertRaises(PongControllerLeaseError):
                PongBoundedController.open(
                    controller.path,
                    experiment_id=controller.state.experiment_id,
                    at="2026-09-11T13:20:01Z",
                    pid=4102,
                    process_instance_id="pong-controller-process-test-two",
                )
            restored = PongBoundedController.open(
                controller.path,
                experiment_id=controller.state.experiment_id,
                at="2026-09-11T13:20:06Z",
                pid=4102,
                process_instance_id="pong-controller-process-test-two",
            )
            self.assertEqual(restored.state.controller_id, original_id)
            self.assertEqual(restored.state.pid, 4102)
            self.assertIn("CONTROLLER_PROCESS_RESTORED", restored.state.transitions)

    def test_blackout_fences_cancels_flattens_reconciles_and_requires_fresh_switch(self) -> None:
        with TemporaryDirectory() as temp:
            controller = self.make(temp)
            self.assertIsNone(controller.tick(observation("2026-09-11T13:29:59Z")))
            self.assertEqual(controller.state.phase, PongControllerPhase.RUNNING_PRE_BLACKOUT)

            command = controller.tick(observation(
                "2026-09-11T13:30:00Z", position=1, owned=2, entries=1,
                reconciled=False, protected=True,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.FENCE_NEW_ENTRIES)
            self.settle(controller, command, "2026-09-11T13:30:00Z")
            command = controller.tick(observation(
                "2026-09-11T13:30:01Z", fence=True, position=1,
                owned=2, entries=1, reconciled=False, protected=True,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.CANCEL_OWNED_PENDING_ENTRIES)
            self.settle(controller, command, "2026-09-11T13:30:01Z")
            command = controller.tick(observation(
                "2026-09-11T13:30:02Z", fence=True, position=1,
                owned=1, entries=0, reconciled=False, protected=True,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.CONTROLLED_FLATTEN)
            self.settle(controller, command, "2026-09-11T13:30:02Z")
            command = controller.tick(observation(
                "2026-09-11T13:30:03Z", fence=True, position=0,
                owned=1, reconciled=False,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.CANCEL_REMAINING_OWNED_ORDERS)
            self.settle(controller, command, "2026-09-11T13:30:03Z")
            command = controller.tick(observation(
                "2026-09-11T13:30:04Z", fence=True, reconciled=False,
            ))
            self.assertEqual(
                command.kind,
                PongControllerCommandKind.REQUEST_SIGNED_FLAT_RECONCILIATION,
            )
            self.settle(controller, command, "2026-09-11T13:30:04Z")
            self.assertIsNone(controller.tick(observation(
                "2026-09-11T13:30:05Z", fence=True,
            )))
            self.assertEqual(controller.state.phase, PongControllerPhase.BLACKOUT_FLAT)

            denied = controller.assess_entry(
                source_switch_id="switch-blackout-7",
                source_switch_sequence=7,
                source_event_at="2026-09-11T13:31:00Z",
                at="2026-09-11T13:31:00Z",
            )
            self.assertFalse(denied.allowed)
            self.assertEqual(controller.state.blackout_switch_high_water, 7)

            command = controller.tick(observation(
                "2026-09-11T13:35:01Z", fence=True,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.RELEASE_ENTRY_FENCE)
            self.settle(controller, command, "2026-09-11T13:35:01Z")
            self.assertIsNone(controller.tick(observation(
                "2026-09-11T13:35:02Z", fence=False,
            )))
            self.assertEqual(controller.state.phase, PongControllerPhase.RUNNING_POST_BLACKOUT)
            stale = controller.assess_entry(
                source_switch_id="switch-blackout-7",
                source_switch_sequence=7,
                source_event_at="2026-09-11T13:34:59Z",
                at="2026-09-11T13:35:02Z",
            )
            self.assertFalse(stale.allowed)
            self.assertEqual(stale.reason, "PONG_BLACKOUT_SWITCH_CANNOT_QUEUE_FOR_RESUME")
            fresh = controller.assess_entry(
                source_switch_id="switch-post-8",
                source_switch_sequence=8,
                source_event_at="2026-09-11T13:35:01Z",
                at="2026-09-11T13:35:02Z",
            )
            self.assertTrue(fresh.allowed)

    def test_controller_loss_and_safety_lockout_fail_closed_but_reduction_remains(self) -> None:
        with TemporaryDirectory() as temp:
            controller = self.make(temp)
            controller.tick(observation("2026-09-11T13:20:00Z"))
            expired = read_controller_entry_fence(
                controller.path,
                source_switch_id="switch-1",
                source_switch_sequence=1,
                source_event_at="2026-09-11T13:20:00Z",
                at="2026-09-11T13:20:06Z",
            )
            self.assertFalse(expired.allowed)
            self.assertEqual(expired.reason, "PONG_CONTROLLER_HEARTBEAT_EXPIRED")
            self.assertTrue(expired.risk_reduction_allowed)

            restarted = PongBoundedController.open(
                controller.path,
                experiment_id=controller.state.experiment_id,
                at="2026-09-11T13:35:00Z",
                pid=4102,
                process_instance_id="pong-controller-process-test-two",
            )
            self.assertIsNone(restarted.tick(observation(
                "2026-09-11T13:35:00Z", fence=True, lockout=True,
            )))
            self.assertEqual(restarted.state.phase, PongControllerPhase.RESUME_BLOCKED)
            denied = restarted.assess_entry(
                source_switch_id="switch-2",
                source_switch_sequence=2,
                source_event_at="2026-09-11T13:35:00Z",
                at="2026-09-11T13:35:00Z",
            )
            self.assertFalse(denied.allowed)
            self.assertTrue(denied.risk_reduction_allowed)

    def test_post_blackout_exposure_is_not_flattened_and_health_loss_only_fences_entries(self) -> None:
        with TemporaryDirectory() as temp:
            controller = self.make(temp)
            command = controller.tick(observation(
                "2026-09-11T13:35:01Z", fence=True,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.RELEASE_ENTRY_FENCE)
            self.settle(controller, command, "2026-09-11T13:35:01Z")
            self.assertIsNone(controller.tick(observation(
                "2026-09-11T13:35:02Z",
            )))
            self.assertEqual(controller.state.phase, PongControllerPhase.RUNNING_POST_BLACKOUT)

            self.assertIsNone(controller.tick(observation(
                "2026-09-11T13:36:00Z", position=1, owned=1,
                protected=True,
            )))
            self.assertEqual(controller.state.phase, PongControllerPhase.RUNNING_POST_BLACKOUT)

            command = controller.tick(observation(
                "2026-09-11T13:36:01Z", position=1, owned=1,
                protected=True, healthy=False,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.FENCE_NEW_ENTRIES)
            self.settle(controller, command, "2026-09-11T13:36:01Z")
            self.assertIsNone(controller.tick(observation(
                "2026-09-11T13:36:02Z", fence=True, position=1,
                owned=1, protected=True, healthy=False,
            )))
            self.assertEqual(controller.state.phase, PongControllerPhase.RESUME_BLOCKED)

            denied = controller.assess_entry(
                source_switch_id="switch-post-9",
                source_switch_sequence=9,
                source_event_at="2026-09-11T13:36:02Z",
                at="2026-09-11T13:36:02Z",
            )
            self.assertFalse(denied.allowed)
            self.assertTrue(denied.risk_reduction_allowed)

    def test_deadline_flattens_reconciles_seals_and_stops(self) -> None:
        with TemporaryDirectory() as temp:
            controller = self.make(temp)
            at = "2026-09-11T14:35:00Z"
            command = controller.tick(observation(
                at, position=-1, owned=1, reconciled=False, protected=True,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.FENCE_NEW_ENTRIES)
            self.settle(controller, command, at)
            command = controller.tick(observation(
                at, fence=True, position=-1, owned=1,
                reconciled=False, protected=True,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.CONTROLLED_FLATTEN)
            self.settle(controller, command, at)
            command = controller.tick(observation(
                at, fence=True, owned=1, reconciled=False,
            ))
            self.assertEqual(command.kind, PongControllerCommandKind.CANCEL_REMAINING_OWNED_ORDERS)
            self.settle(controller, command, at)
            command = controller.tick(observation(at, fence=True, reconciled=False))
            self.assertEqual(
                command.kind,
                PongControllerCommandKind.REQUEST_SIGNED_FLAT_RECONCILIATION,
            )
            self.settle(controller, command, at)
            command = controller.tick(observation(at, fence=True))
            self.assertEqual(command.kind, PongControllerCommandKind.SEAL_EVIDENCE)
            self.settle(controller, command, at)
            command = controller.tick(observation(at, fence=True, sealed=True))
            self.assertEqual(command.kind, PongControllerCommandKind.STOP_OPERATION)
            self.settle(controller, command, at)
            self.assertIsNone(controller.tick(observation(
                at, fence=True, sealed=True, active=False,
            )))
            self.assertEqual(controller.state.phase, PongControllerPhase.TERMINATED)

    def test_corrupt_or_wrong_scope_state_never_grants_entry(self) -> None:
        with TemporaryDirectory() as temp:
            controller = self.make(temp)
            payload = json.loads(controller.path.read_text(encoding="ascii"))
            payload["pid"] = 99
            controller.path.write_text(
                json.dumps(payload, sort_keys=True, separators=(",", ":")),
                encoding="ascii",
            )
            with self.assertRaises(PongControllerIntegrityError):
                PongBoundedController.load(controller.path)
            decision = read_controller_entry_fence(
                controller.path,
                source_switch_id="switch-1",
                source_switch_sequence=1,
                source_event_at="2026-09-11T13:20:00Z",
                at="2026-09-11T13:20:01Z",
            )
            self.assertFalse(decision.allowed)
            self.assertEqual(
                decision.reason,
                "PONG_CONTROLLER_STATE_UNAVAILABLE_OR_INVALID",
            )

    def test_observation_contract_rejects_wrong_account_and_quantity(self) -> None:
        good = observation("2026-09-11T13:20:00Z")
        with self.assertRaises(ValueError):
            replace(good, account_name="LiveAccount")
        with self.assertRaises(ValueError):
            replace(good, position_quantity=2)


if __name__ == "__main__":
    unittest.main()
