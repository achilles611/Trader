from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from src.l3g_paper.contracts import POLICY, PaperDecisionKind, PaperDirection
from src.l3g_paper.risk import PaperRiskAuthority, PaperRiskSnapshot

from .l3g_helpers import warmed_bullish_policy


AT = "2026-08-24T14:00:00Z"


def healthy_snapshot() -> PaperRiskSnapshot:
    return PaperRiskSnapshot(
        AT, position_snapshot_complete=True, order_snapshot_complete=True, reconciliation_current=True,
        local_bridge_healthy=True, market_price_connected=True, execution_bridge_healthy=True,
        evidence_warmed=True, local_sequence_gap=False, depth_reset_recovery=False,
        quote_observed_at=AT, classified_trade_observed_at=AT, depth_mutation_observed_at=AT,
    )


class PaperRiskTests(unittest.TestCase):
    def test_exact_preflight_and_entry_grant(self) -> None:
        authority = PaperRiskAuthority(); snapshot = healthy_snapshot()
        self.assertEqual(authority.preflight(snapshot, at=AT), (True, ()))
        decision = warmed_bullish_policy()[2]
        decision = replace(decision, created_at=AT, expires_at="2026-08-24T14:00:05Z")
        intent = authority.make_intent(decision, reference_bid=Decimal("100"), reference_ask=Decimal("100.25"), reference_last=Decimal("100.25"))
        self.assertTrue(authority.evaluate(intent, snapshot, at=AT).granted)

    def test_account_quantity_position_pending_and_losses_fail_closed(self) -> None:
        authority = PaperRiskAuthority(); decision = warmed_bullish_policy()[2]
        decision = replace(decision, created_at=AT, expires_at="2026-08-24T14:00:05Z")
        intent = authority.make_intent(decision, reference_bid=Decimal("100"), reference_ask=Decimal("100.25"), reference_last=Decimal("100.25"))
        attacks = (
            replace(healthy_snapshot(), account_name="Sim102"),
            replace(healthy_snapshot(), current_position=PaperDirection.LONG, current_position_quantity=1),
            replace(healthy_snapshot(), working_entry_orders=1),
            replace(healthy_snapshot(), daily_realized_pnl=Decimal("-200")),
            replace(healthy_snapshot(), session_entry_count=12),
            replace(healthy_snapshot(), consecutive_losses=4),
            replace(healthy_snapshot(), foreign_activity=True),
        )
        for snapshot in attacks:
            with self.subTest(snapshot=snapshot):
                self.assertFalse(authority.evaluate(intent, snapshot, at=AT).granted)

    def test_freshness_session_stop_slippage_and_max_age(self) -> None:
        authority = PaperRiskAuthority()
        self.assertTrue(authority.hard_flat_due("2026-08-24T19:58:00Z"))
        aged = replace(healthy_snapshot(), position_opened_at="2026-08-24T13:50:00Z")
        self.assertTrue(authority.maximum_age_due(aged, AT))
        self.assertEqual(authority.protective_stop_price(PaperDirection.LONG, Decimal("20000")), Decimal("19975.00"))
        decision = replace(warmed_bullish_policy()[2], created_at=AT, expires_at="2026-08-24T14:00:05Z")
        intent = authority.make_intent(decision, reference_bid=Decimal("100"), reference_ask=Decimal("100.25"), reference_last=Decimal("100"))
        self.assertFalse(authority.enforce_fill(PaperDirection.LONG, intent, Decimal("102.50"))[0])

    def test_all_identity_continuity_and_freshness_attacks_deny_entry(self) -> None:
        decision = replace(warmed_bullish_policy()[2], created_at=AT, expires_at="2026-08-24T14:00:05Z")
        attacks = {
            "blank account": replace(healthy_snapshot(), account_name=""),
            "lucid account": replace(healthy_snapshot(), account_name="Lucid25kflex01", account_class="PROVIDER_EVALUATION"),
            "multiple accounts": replace(healthy_snapshot(), account_match_count=2),
            "missing Sim101": replace(healthy_snapshot(), account_match_count=0),
            "wrong instrument": replace(healthy_snapshot(), instrument="NQ SEP26"),
            "wrong expiry": replace(healthy_snapshot(), instrument="MNQ DEC26"),
            "wrong canonical": replace(healthy_snapshot(), canonical_contract="MNQ"),
            "wrong tick": replace(healthy_snapshot(), tick_size=Decimal("0.50")),
            "local sequence gap": replace(healthy_snapshot(), local_sequence_gap=True),
            "depth reset": replace(healthy_snapshot(), depth_reset_recovery=True),
            "market disconnect": replace(healthy_snapshot(), market_price_connected=False),
            "local disconnect": replace(healthy_snapshot(), local_bridge_healthy=False),
            "execution disconnect": replace(healthy_snapshot(), execution_bridge_healthy=False),
            "quote stale": replace(healthy_snapshot(), quote_observed_at="2026-08-24T13:59:57Z"),
            "trade stale": replace(healthy_snapshot(), classified_trade_observed_at="2026-08-24T13:59:54Z"),
            "depth stale": replace(healthy_snapshot(), depth_mutation_observed_at="2026-08-24T13:59:54Z"),
            "foreign non-MNQ activity": replace(healthy_snapshot(), foreign_activity=True),
            "ambiguous truth": replace(healthy_snapshot(), unresolved_execution=True),
        }
        for name, snapshot in attacks.items():
            with self.subTest(name=name):
                authority = PaperRiskAuthority()
                intent = authority.make_intent(decision, reference_bid=Decimal("100"), reference_ask=Decimal("100.25"), reference_last=Decimal("100"))
                self.assertFalse(authority.evaluate(intent, snapshot, at=AT).granted)

    def test_entry_window_and_grant_expiry_are_exact(self) -> None:
        authority = PaperRiskAuthority()
        self.assertFalse(authority.preflight(healthy_snapshot(), at="2026-08-24T13:34:59Z")[0])
        decision = replace(warmed_bullish_policy()[2], created_at=AT, expires_at="2026-08-24T14:00:05Z")
        intent = authority.make_intent(decision, reference_bid=Decimal("100"), reference_ask=Decimal("100.25"), reference_last=Decimal("100"))
        grant = authority.evaluate(intent, healthy_snapshot(), at=AT)
        self.assertTrue(grant.valid_at("2026-08-24T14:00:05Z"))
        self.assertFalse(grant.valid_at("2026-08-24T14:00:05.000001Z"))

    def test_commissioning_warmup_and_strategy_evidence_are_distinct_authorities(self) -> None:
        authority = PaperRiskAuthority()
        commissioning_ready = replace(
            healthy_snapshot(), evidence_warmed=False, commissioning_session_warmed=True,
        )
        allowed, reasons = authority.preflight(commissioning_ready, at=AT, commissioning=True)
        self.assertTrue(allowed, reasons)
        strategy_allowed, strategy_reasons = authority.preflight(commissioning_ready, at=AT)
        self.assertFalse(strategy_allowed)
        self.assertIn("PAPER_EVIDENCE_NOT_WARMED", strategy_reasons)

        commissioning_cold = replace(
            healthy_snapshot(), evidence_warmed=True, commissioning_session_warmed=False,
        )
        allowed, reasons = authority.preflight(commissioning_cold, at=AT, commissioning=True)
        self.assertFalse(allowed)
        self.assertIn("COMMISSIONING_SESSION_NOT_WARMED", reasons)
        self.assertNotIn("PAPER_EVIDENCE_NOT_WARMED", reasons)

    def test_commissioning_freshness_thresholds_are_exact_and_independent_of_latch(self) -> None:
        authority = PaperRiskAuthority()
        boundary = replace(
            healthy_snapshot(), commissioning_session_warmed=True,
            quote_observed_at="2026-08-24T13:59:58Z",
            classified_trade_observed_at="2026-08-24T13:59:55Z",
            depth_mutation_observed_at="2026-08-24T13:59:55Z",
        )
        self.assertTrue(authority.preflight(boundary, at=AT, commissioning=True)[0])
        stale = replace(boundary, quote_observed_at="2026-08-24T13:59:57.999999Z")
        allowed, reasons = authority.preflight(stale, at=AT, commissioning=True)
        self.assertFalse(allowed)
        self.assertIn("QUOTE_STALE", reasons)


if __name__ == "__main__":
    unittest.main()
