from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import math
import unittest

from src.l3g_paper.pong_quantity import (
    PONG_QUANTITY_POLICY,
    PongExposureTracker,
    PongQuantityDirection,
    PongReconciliationProof,
    assess_quantity_admission,
    score_authorized_quantity,
)


class PongQuantityTests(unittest.TestCase):
    def authorization(
        self, direction: PongQuantityDirection, score: float,
    ):
        return score_authorized_quantity(
            direction, score, evidence_valid=True, otherwise_eligible=True,
        )

    def admission(self, authorization, **changes):
        proof_changes = changes.pop("proof_changes", {})
        at = "2026-09-11T01:00:00Z"
        proof_values = {
            "source_hash": "a" * 64,
            "observed_at": at,
            "available_at": at,
            "position_quantity": 0,
            "working_owned_orders": 0,
            "unresolved_execution": False,
        }
        proof_values.update(proof_changes)
        values = {
            "opportunity_action_id": "pong-action-test-opportunity",
            "source_switch_id": "pong-discernment-switch-test",
            "snapshot_hash": "b" * 64,
            "bias_result_hash": "c" * 64,
            "admission_at": at,
            "reconciliation": PongReconciliationProof.create(**proof_values),
            "available_loss_allowance_dollars": Decimal("200"),
        }
        values.update(changes)
        return assess_quantity_admission(authorization, **values)

    def test_full_precision_inclusive_strong_threshold_and_mirror(self) -> None:
        self.assertEqual(
            self.authorization(PongQuantityDirection.LONG, 0.84999).requested_total_quantity,
            1,
        )
        self.assertEqual(
            self.authorization(PongQuantityDirection.LONG, 0.85).requested_total_quantity,
            2,
        )
        self.assertEqual(
            self.authorization(PongQuantityDirection.SHORT, 0.15001).requested_total_quantity,
            1,
        )
        self.assertEqual(
            self.authorization(PongQuantityDirection.SHORT, 0.15).requested_total_quantity,
            2,
        )

    def test_invalid_or_ineligible_is_zero_and_nonfinite_is_refused(self) -> None:
        invalid = score_authorized_quantity(
            PongQuantityDirection.LONG, 0.99,
            evidence_valid=False, otherwise_eligible=True,
        )
        ineligible = score_authorized_quantity(
            PongQuantityDirection.LONG, 0.99,
            evidence_valid=True, otherwise_eligible=False,
        )
        self.assertEqual((invalid.requested_total_quantity, ineligible.requested_total_quantity), (0, 0))
        for value in (math.nan, math.inf, -0.01, 1.01):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.authorization(PongQuantityDirection.LONG, value)

    def test_historical_default_risk_and_native_caps_reduce_two_to_one(self) -> None:
        self.assertEqual(
            PONG_QUANTITY_POLICY.native_capability_status,
            "HISTORICAL_ADDON_SEALED_TO_ONE_NOT_CURRENT_AUTHORITY",
        )
        result = self.admission(self.authorization(PongQuantityDirection.LONG, 0.90))
        self.assertEqual(result.score_authorized_quantity, 2)
        self.assertEqual(result.risk_capacity_quantity, 1)
        self.assertEqual(result.native_capacity_quantity, 1)
        self.assertEqual(result.admitted_quantity, 1)
        self.assertEqual(result.reserved_risk_dollars, Decimal("50.00"))
        self.assertIn("TWO_MNQ_EXCEEDS_POSITION_RISK_LIMIT", result.blockers)
        self.assertIn("PONG_NATIVE_QUANTITY_CAPABILITY_UNPROVEN", result.blockers)
        self.assertTrue(result.admission_id.startswith("pong-admission-"))
        self.assertEqual(result.opportunity_action_id, "pong-action-test-opportunity")
        self.assertEqual(result.authorization_hash, self.authorization(
            PongQuantityDirection.LONG, 0.90,
        ).authorization_hash)

    def test_admission_identity_changes_with_opportunity_and_proofs(self) -> None:
        authorization = self.authorization(PongQuantityDirection.LONG, 0.70)
        first = self.admission(authorization)
        second = self.admission(
            authorization, opportunity_action_id="pong-action-second-opportunity",
        )
        third = self.admission(
            authorization,
            proof_changes={"source_hash": "d" * 64},
        )
        fourth = self.admission(
            authorization,
            already_reserved_risk_dollars=Decimal("1"),
        )
        self.assertNotEqual(first.admission_id, second.admission_id)
        self.assertNotEqual(first.admission_id, third.admission_id)
        self.assertNotEqual(first.admission_id, fourth.admission_id)

    def test_reconciliation_freshness_integrity_and_signature_fail_closed(self) -> None:
        authorization = self.authorization(PongQuantityDirection.LONG, 0.70)
        stale = self.admission(
            authorization,
            admission_at="2026-09-11T01:00:06Z",
        )
        self.assertEqual(stale.admitted_quantity, 0)
        self.assertIn("PONG_RECONCILIATION_STALE", stale.blockers)

        proof = PongReconciliationProof.create(
            source_hash="a" * 64,
            observed_at="2026-09-11T01:00:00Z",
            available_at="2026-09-11T01:00:00Z",
            position_quantity=0, working_owned_orders=0,
            unresolved_execution=False,
        )
        tampered = replace(proof, position_quantity=1)
        result = self.admission(authorization, reconciliation=tampered)
        self.assertEqual(result.admitted_quantity, 0)
        self.assertIn("PONG_RECONCILIATION_INTEGRITY_FAILED", result.blockers)

        wrong_source = PongReconciliationProof.create(
            source_id="unapproved-observer", source_hash="a" * 64,
            observed_at="2026-09-11T01:00:00Z",
            available_at="2026-09-11T01:00:00Z",
            position_quantity=0, working_owned_orders=0,
            unresolved_execution=False,
        )
        rejected = self.admission(authorization, reconciliation=wrong_source)
        self.assertEqual(rejected.admitted_quantity, 0)
        self.assertIn("PONG_RECONCILIATION_CONTRACT_INVALID", rejected.blockers)

    def test_fake_reviewed_caps_demonstrate_two_without_widening_defaults(self) -> None:
        fake_policy = replace(
            PONG_QUANTITY_POLICY,
            maximum_position_risk_dollars=Decimal("100"),
            native_maximum_quantity=2,
            native_capability_status="DETERMINISTIC_TEST_FIXTURE_ONLY",
        )
        result = assess_quantity_admission(
            self.authorization(PongQuantityDirection.SHORT, 0.10),
            opportunity_action_id="pong-action-test-opportunity",
            source_switch_id="pong-discernment-switch-test",
            snapshot_hash="b" * 64, bias_result_hash="c" * 64,
            admission_at="2026-09-11T01:00:00Z",
            reconciliation=PongReconciliationProof.create(
                source_hash="a" * 64,
                observed_at="2026-09-11T01:00:00Z",
                available_at="2026-09-11T01:00:00Z",
                position_quantity=0, working_owned_orders=0,
                unresolved_execution=False,
            ),
            available_loss_allowance_dollars=Decimal("200"), policy=fake_policy,
        )
        self.assertEqual(result.admitted_quantity, 2)
        self.assertEqual(result.reserved_risk_dollars, Decimal("100.00"))
        self.assertEqual(result.blockers, ())
        self.assertEqual(PONG_QUANTITY_POLICY.native_maximum_quantity, 1)
        self.assertEqual(PONG_QUANTITY_POLICY.maximum_position_risk_dollars, Decimal("50.00"))

    def test_identity_orders_unresolved_and_reservations_fail_closed(self) -> None:
        auth = self.authorization(PongQuantityDirection.LONG, 0.70)
        for changes, reason in (
            ({"proof_changes": {"mode": "LIVE"}}, "PONG_EXECUTION_IDENTITY_MISMATCH"),
            ({"proof_changes": {"instrument": "NQ SEP26"}}, "PONG_EXECUTION_IDENTITY_MISMATCH"),
            ({"proof_changes": {"live_authority": "ARMED"}}, "PONG_EXECUTION_IDENTITY_MISMATCH"),
            ({"proof_changes": {"position_quantity": 1}}, "PYRAMIDING_OR_REVERSAL_DENIED"),
            ({"proof_changes": {"working_owned_orders": 1}}, "WORKING_OWNED_ORDERS_PRESENT"),
            ({"proof_changes": {"unresolved_execution": True}}, "UNRESOLVED_EXECUTION_TRUTH"),
        ):
            with self.subTest(reason=reason):
                result = self.admission(auth, **changes)
                self.assertEqual(result.admitted_quantity, 0)
                self.assertIn(reason, result.blockers)
        reserved = self.admission(
            auth, already_reserved_risk_dollars=Decimal("50"),
        )
        self.assertEqual(reserved.admitted_quantity, 0)
        self.assertIn("PONG_RISK_BUDGET_INSUFFICIENT", reserved.blockers)
        self.assertEqual(PONG_QUANTITY_POLICY.mode, "PAPER_SIM101")
        self.assertEqual(PONG_QUANTITY_POLICY.live_authority, "DISARMED")
        with self.assertRaises(ValueError):
            replace(PONG_QUANTITY_POLICY, mode="LIVE")
        with self.assertRaises(ValueError):
            replace(PONG_QUANTITY_POLICY, live_authority="ARMED")

    def test_partial_fill_protection_cancel_race_and_exact_once_exit(self) -> None:
        tracker = PongExposureTracker(2)
        self.assertEqual(tracker.record_entry_fill("entry-1", 1), 1)
        self.assertFalse(tracker.protection_complete)
        tracker.confirm_protection(1)
        self.assertTrue(tracker.protection_complete)
        self.assertEqual(tracker.request_cancel_remainder(), 1)
        # A fill racing cancellation remains real, bounded, and must be protected.
        self.assertEqual(tracker.record_entry_fill("entry-2", 1), 2)
        tracker.confirm_protection(2)
        self.assertEqual(tracker.record_entry_fill("entry-2", 1), 2)
        self.assertEqual(tracker.required_exit_quantity, 2)
        self.assertEqual(tracker.record_exit_fill("entry-1", 1), 2)
        self.assertEqual(tracker.record_exit_fill("exit-1", 1), 1)
        tracker.confirm_protection(1)
        self.assertEqual(tracker.record_exit_fill("exit-1", 1), 1)
        self.assertEqual(tracker.record_exit_fill("exit-2", 1), 0)
        tracker.confirm_protection(0)
        with self.assertRaises(ValueError):
            tracker.record_exit_fill("exit-over", 1)

    def test_rejected_remainder_never_becomes_add_on_authority(self) -> None:
        tracker = PongExposureTracker(2)
        tracker.record_entry_fill("partial", 1)
        tracker.confirm_protection(1)
        tracker.request_cancel_remainder()
        tracker.confirm_entry_remainder_cancelled(1)
        self.assertEqual(tracker.remaining_entry_quantity, 0)
        with self.assertRaises(ValueError):
            tracker.record_entry_fill("late-new-order", 1)


if __name__ == "__main__":
    unittest.main()
