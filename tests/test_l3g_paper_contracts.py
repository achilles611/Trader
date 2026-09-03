from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import unittest

from src.l3g_paper.contracts import (
    ACCOUNT_BINDING, AUTHORITY, POLICY, RISK_PROFILE, ExecutionAccountBinding,
    ExecutionAction, PaperDirection, PaperExecutionCommand, deterministic_id,
    refuse_execution_target,
)
from src.l3g_paper.sessions import PaperSessionKind


class PaperContractTests(unittest.TestCase):
    def test_artifacts_are_exact_hash_bound_and_never_scientific(self) -> None:
        self.assertEqual(POLICY.configuration_hash, "070587789049231d268cb742404eb6dcc21d91014c9529aa109419a563729a91")
        self.assertEqual(RISK_PROFILE.configuration_hash, "eef09f7b185cc197aced3e7b91dd03f1571be52be3635747d2d209821ebcaa34")
        self.assertEqual(POLICY.policy_id, "l3g-ny-high-confluence-commissioning-policy-v1")
        self.assertEqual((POLICY.entry_profile, POLICY.entry_profile_version), ("NY_HIGH_CONFLUENCE_COMMISSIONING", "NY_HIGH_CONFLUENCE_COMMISSIONING_V1"))
        self.assertIs(POLICY.entry_session_kind, PaperSessionKind.NEW_YORK_RTH)
        self.assertEqual(POLICY.entry_support_threshold, Decimal("0.675"))
        self.assertEqual(POLICY.entry_dominance_margin, Decimal("0.10"))
        self.assertEqual(POLICY.entry_family_count, 3)
        self.assertEqual(POLICY.reentry_cooldown_seconds, 3600)
        self.assertEqual(RISK_PROFILE.maximum_position_age_seconds, 3600)
        self.assertEqual(RISK_PROFILE.maximum_session_entries, 1)
        self.assertEqual(RISK_PROFILE.reentry_cooldown_seconds, 3600)
        self.assertFalse(POLICY.scientific_eligibility)
        self.assertFalse(AUTHORITY.authority_payload()["scientific_eligibility"])
        self.assertEqual(AUTHORITY.authority_payload()["live_capital"], "DENIED")
        with self.assertRaisesRegex(ValueError, "high-confluence commissioning policy tuning"):
            replace(POLICY, entry_support_threshold=Decimal("0.65"))
        with self.assertRaisesRegex(ValueError, "high-confluence commissioning risk limits"):
            replace(RISK_PROFILE, maximum_session_entries=2)

    def test_no_other_account_binding_or_execution_target_is_constructible(self) -> None:
        for account in ("", "Sim102", "Lucid25kflex01"):
            with self.assertRaises(ValueError):
                ExecutionAccountBinding(account_name=account)
        for target in ("", "LIVE", "REAL", "LUCID", "Lucid25kflex01", "PROVIDER_EVALUATION", "unknown"):
            with self.assertRaises(ValueError):
                refuse_execution_target(target)
        refuse_execution_target("PAPER_SIM101")

    def test_deterministic_namespace_and_closed_command(self) -> None:
        self.assertEqual(deterministic_id("l3g-pd-", {"a": 1}), deterministic_id("l3g-pd-", {"a": 1}))
        with self.assertRaises(ValueError):
            deterministic_id("l3d-d-", {"a": 1})
        values = dict(
            command_id="l3g-pc-" + "a" * 32, command_sequence=1, session_id="s", intent_id="i",
            decision_id="d", action=ExecutionAction.ENTER_LONG, account_name="Sim101",
            account_class="LOCAL_SIMULATION", instrument="MNQ SEP26", quantity=1,
            expected_position=PaperDirection.LONG, created_at="2026-08-24T14:00:00Z",
            expires_at="2026-08-24T14:00:05Z", policy_hash=POLICY.configuration_hash,
            risk_profile_hash=RISK_PROFILE.configuration_hash, account_binding_hash=ACCOUNT_BINDING.binding_hash,
            reason_code="test", risk_grant_id="g",
        )
        command = PaperExecutionCommand(**values)
        self.assertNotIn("provider_sequence", command.payload())
        with self.assertRaises(ValueError):
            PaperExecutionCommand(**{**values, "quantity": 2})
        with self.assertRaises(ValueError):
            PaperExecutionCommand(**{**values, "account_name": "Sim102"})
        with self.assertRaises(ValueError):
            PaperExecutionCommand(**{**values, "expected_position": PaperDirection.SHORT})
        for action, quantity, expected in (
            (ExecutionAction.ENTER_SHORT, 1, PaperDirection.SHORT),
            (ExecutionAction.EXIT, 1, PaperDirection.FLAT),
            (ExecutionAction.EMERGENCY_FLATTEN, 1, PaperDirection.FLAT),
            (ExecutionAction.CANCEL_OWNED_ORDERS, 0, PaperDirection.FLAT),
            (ExecutionAction.RECONCILE, 0, PaperDirection.FLAT),
        ):
            with self.subTest(action=action):
                PaperExecutionCommand(**{**values, "action": action, "quantity": quantity, "expected_position": expected})


if __name__ == "__main__":
    unittest.main()
