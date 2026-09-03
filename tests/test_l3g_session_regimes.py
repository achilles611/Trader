from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3g_paper.contracts import (
    ACCOUNT_BINDING, POLICY, RISK_PROFILE, EvidenceFamily, HypothesisKind,
    PaperDecision, PaperDecisionKind, PaperDirection, PaperEvidence,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.policy import ExperimentalPaperPolicy
from src.l3g_paper.risk import PaperRiskAuthority, PaperRiskSnapshot
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.sessions import (
    ASIA_PROFILE, NEW_YORK_RTH_PROFILE, NY_AFTER_PROFILE, PaperCalendarState,
    PaperSessionCalendar, PaperSessionKind, PaperSessionResolver,
)
from src.l3f_provider.tradovate_observation import StreamHealth

from tests.l3g_helpers import ObservationFactory, warmed_bullish_policy


def resolved(value: str):
    result = PaperSessionResolver().resolve(value, generation=7)
    assert result.context.session_kind is not PaperSessionKind.OFF_SESSION
    return result


def healthy(context, at: str, **changes: object) -> PaperRiskSnapshot:
    values: dict[str, object] = {
        "observed_at": at,
        "position_snapshot_complete": True,
        "order_snapshot_complete": True,
        "reconciliation_current": True,
        "local_bridge_healthy": True,
        "market_price_connected": True,
        "execution_bridge_healthy": True,
        "evidence_warmed": True,
        "local_sequence_gap": False,
        "depth_reset_recovery": False,
        "quote_observed_at": at,
        "classified_trade_observed_at": at,
        "depth_mutation_observed_at": at,
        "session_kind": context.session_kind,
        "session_id": context.session_id,
        "trade_date": context.trade_date,
        "session_profile_hash": context.session_profile_hash,
        "session_generation": context.session_generation,
    }
    values.update(changes)
    return PaperRiskSnapshot(**values)  # type: ignore[arg-type]


class SessionClassificationTests(unittest.TestCase):
    def test_asia_and_new_york_boundaries_use_america_new_york(self) -> None:
        cases = (
            ("2026-08-24T22:00:00Z", PaperSessionKind.ASIA, "2026-08-25", False),  # Sunday 18:00 warmup
            ("2026-08-25T03:59:00Z", PaperSessionKind.ASIA, "2026-08-25", True),  # Monday 23:59
            ("2026-08-25T04:00:00Z", PaperSessionKind.ASIA, "2026-08-25", True),  # Tuesday 00:00
            ("2026-08-25T06:00:00Z", PaperSessionKind.OFF_SESSION, "2026-08-25", False),
            ("2026-08-25T13:30:00Z", PaperSessionKind.NEW_YORK_RTH, "2026-08-25", False),
            ("2026-08-25T13:35:00Z", PaperSessionKind.NEW_YORK_RTH, "2026-08-25", True),
            ("2026-08-25T19:30:00Z", PaperSessionKind.NEW_YORK_RTH, "2026-08-25", False),
            ("2026-08-25T20:00:00Z", PaperSessionKind.NY_AFTER, "2026-08-25", False),
            ("2026-08-25T20:05:00Z", PaperSessionKind.NY_AFTER, "2026-08-25", True),
            ("2026-08-25T21:30:00Z", PaperSessionKind.NY_AFTER, "2026-08-25", False),
            ("2026-08-28T22:00:00Z", PaperSessionKind.OFF_SESSION, "2026-08-28", False),  # Friday 18:00
            ("2026-08-29T16:00:00Z", PaperSessionKind.OFF_SESSION, "2026-08-29", False),
        )
        for value, kind, trade_date, entry_authorized in cases:
            with self.subTest(value=value):
                result = PaperSessionResolver().resolve(value, generation=3)
                self.assertEqual(result.context.session_kind, kind)
                self.assertEqual(result.context.trade_date, trade_date)
                self.assertEqual(result.entry_authorized, entry_authorized)
        hard_flat = resolved("2026-08-25T19:58:00Z").context
        self.assertTrue(hard_flat.hard_flat_due_at(datetime(2026, 8, 25, 19, 58, tzinfo=timezone.utc)))
        ny_after_hard_flat = resolved("2026-08-25T21:58:00Z").context
        self.assertTrue(ny_after_hard_flat.hard_flat_due_at(datetime(2026, 8, 25, 21, 58, tzinfo=timezone.utc)))
        self.assertEqual(ny_after_hard_flat.session_family.value, "NEW_YORK")

    def test_new_york_to_after_to_asia_are_distinct_local_evidence_domains(self) -> None:
        rth = resolved("2026-08-25T19:59:00Z").context
        after = resolved("2026-08-25T20:05:00Z").context
        asia = resolved("2026-08-25T22:05:00Z").context
        self.assertEqual((rth.session_family.value, after.session_family.value, asia.session_family.value), ("NEW_YORK", "NEW_YORK", "ASIA"))
        self.assertEqual(len({rth.session_id, after.session_id, asia.session_id}), 3)
        self.assertEqual(len({rth.session_profile_hash, after.session_profile_hash, asia.session_profile_hash}), 3)

    def test_holiday_required_fails_closed_and_dst_offsets_are_not_fixed(self) -> None:
        calendar = PaperSessionCalendar({"2026-08-25": PaperCalendarState.HOLIDAY_OVERRIDE_REQUIRED})
        result = PaperSessionResolver(calendar).resolve("2026-08-25T13:35:00Z")
        self.assertEqual(result.context.session_kind, PaperSessionKind.NEW_YORK_RTH)
        self.assertFalse(result.entry_authorized)
        self.assertEqual(result.reason_code, "HOLIDAY_SESSION_UNVERIFIED")
        winter = PaperSessionResolver().resolve("2026-01-05T23:05:00Z")  # 18:05 EST
        summer = PaperSessionResolver().resolve("2026-07-06T22:05:00Z")  # 18:05 EDT
        self.assertTrue(winter.entry_authorized)
        self.assertTrue(summer.entry_authorized)
        self.assertEqual(winter.context.timezone, "America/New_York")
        self.assertEqual(summer.context.timezone, "America/New_York")

    def test_known_holiday_candidate_requires_verified_override_by_default(self) -> None:
        # Thanksgiving is a candidate fence, not a claim of a particular CME
        # schedule; entry must remain denied until an operator verifies it.
        result = PaperSessionResolver().resolve("2026-11-26T14:35:00Z")
        self.assertEqual(result.context.session_kind, PaperSessionKind.NEW_YORK_RTH)
        self.assertEqual(result.context.calendar_state, PaperCalendarState.HOLIDAY_OVERRIDE_REQUIRED)
        self.assertFalse(result.entry_authorized)
        self.assertEqual(result.reason_code, "HOLIDAY_SESSION_UNVERIFIED")
        verified = PaperSessionResolver(PaperSessionCalendar({
            "2026-11-26": PaperCalendarState.HOLIDAY_OVERRIDE_VERIFIED,
        })).resolve("2026-11-26T14:35:00Z")
        self.assertTrue(verified.entry_authorized)


class SessionEvidenceTests(unittest.TestCase):
    def test_runtime_transition_increments_generation_and_closes_prior_domain(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            try:
                runtime = LaneIIIPaperRuntime(ledger)
                asia = ObservationFactory(start=datetime(2026, 8, 24, 22, 5, tzinfo=timezone.utc))
                runtime.ingest(asia.quote(100))
                first = runtime.status()
                ny = ObservationFactory(start=datetime(2026, 8, 25, 13, 35, tzinfo=timezone.utc))
                runtime.ingest(ny.quote(101))
                second = runtime.status()
                self.assertEqual(first["current_session"], "ASIA")
                self.assertEqual(second["current_session"], "NEW_YORK_RTH")
                self.assertGreater(int(second["session_generation"]), int(first["session_generation"]))
                self.assertTrue(any(item["kind"] == "SESSION_CLOSED" for item in ledger.recent(20, domain="SESSION")))
            finally:
                ledger.close()

    def test_transition_resets_evidence_and_generation_changes_hypothesis_identity(self) -> None:
        asia = resolved("2026-08-24T22:05:00Z").context
        ny = resolved("2026-08-25T13:35:00Z").context
        policy = ExperimentalPaperPolicy(); policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory()
        policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}), session_context=asia)
        quote = factory.quote(100); policy.ingest(quote, session_context=asia)
        for price in (100, 99, 100):
            quote = factory.quote(price); policy.ingest(factory.trade(quote, price), session_context=asia)
        self.assertEqual(policy.session_context.session_id, asia.session_id)
        policy.ingest(factory.quote(101), session_context=ny)
        self.assertEqual(policy.session_context.session_id, ny.session_id)
        self.assertEqual(policy.active_evidence("2026-08-25T13:35:01Z"), ())
        self.assertNotEqual((asia.session_id, asia.session_generation), (ny.session_id, ny.session_generation))

    def test_mixed_session_evidence_sources_are_refused(self) -> None:
        asia = resolved("2026-08-24T22:05:00Z").context
        with self.assertRaisesRegex(ValueError, "CROSS_SESSION_SOURCE_SET"):
            PaperEvidence(
                "l3g-pe-" + "a" * 32, HypothesisKind.BULLISH_REVERSAL,
                EvidenceFamily.ORDER_FLOW, "MIXED", Decimal("0.5"), True,
                "2026-08-24T22:05:00Z", "2026-08-24T22:06:00Z", ("a", "b"), (1, 2), ("x", "y"),
                session_kind=asia.session_kind, session_id=asia.session_id, trade_date=asia.trade_date,
                session_profile_hash=asia.session_profile_hash, session_generation=asia.session_generation,
                source_session_ids=(asia.session_id, "MNQU6:NEW_YORK_RTH:2026-08-25"),
            )


class SessionRiskAndLedgerTests(unittest.TestCase):
    def test_single_entry_cap_and_ny_rth_only_profile_reject_ny_after(self) -> None:
        ny = resolved("2026-08-25T13:35:00Z").context
        after = resolved("2026-08-25T20:05:00Z").context
        policy_decision = replace(warmed_bullish_policy()[2], created_at="2026-08-25T13:35:00Z", expires_at="2026-08-25T13:35:05Z")
        decision = replace(
            policy_decision, session_kind=ny.session_kind, session_id=ny.session_id, trade_date=ny.trade_date,
            session_profile_hash=ny.session_profile_hash, session_generation=ny.session_generation,
        )
        authority = PaperRiskAuthority()
        intent = authority.make_intent(decision, reference_bid=Decimal("100"), reference_ask=Decimal("100.25"), reference_last=Decimal("100"))
        ny_snapshot = healthy(ny, "2026-08-25T13:35:00Z", daily_realized_pnl=Decimal("-125"), trade_date_entry_count=0)
        self.assertTrue(authority.evaluate(intent, ny_snapshot, at="2026-08-25T13:35:00Z").granted)
        self.assertFalse(authority.evaluate(intent, replace(ny_snapshot, daily_realized_pnl=Decimal("-200")), at="2026-08-25T13:35:00Z").granted)
        self.assertFalse(authority.evaluate(intent, replace(ny_snapshot, trade_date_entry_count=RISK_PROFILE.maximum_session_entries), at="2026-08-25T13:35:00Z").granted)
        self.assertFalse(authority.evaluate(intent, replace(ny_snapshot, foreign_activity=True), at="2026-08-25T13:35:00Z").granted)
        self.assertFalse(authority.evaluate(intent, replace(ny_snapshot, unresolved_execution=True), at="2026-08-25T13:35:00Z").granted)
        self.assertEqual(after.trade_date, ny.trade_date)
        self.assertEqual(after.session_family, ny.session_family)
        after_decision = replace(
            decision, created_at="2026-08-25T20:05:00Z", expires_at="2026-08-25T20:05:05Z",
            session_kind=after.session_kind, session_id=after.session_id,
            session_profile_hash=after.session_profile_hash,
        )
        after_intent = authority.make_intent(after_decision, reference_bid=Decimal("100"), reference_ask=Decimal("100.25"), reference_last=Decimal("100"))
        after_grant = authority.evaluate(after_intent, healthy(after, "2026-08-25T20:05:00Z"), at="2026-08-25T20:05:00Z")
        self.assertFalse(after_grant.granted)
        self.assertIn("PROFILE_SESSION_MISMATCH", after_grant.reason_codes)

    def test_ledger_filters_preserve_session_dimension(self) -> None:
        asia = resolved("2026-08-24T22:05:00Z").context
        ny = resolved("2026-08-25T13:35:00Z").context
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            ledger.append("INCIDENT_SESSION_TEST", {**asia.payload(), "label": "asia"}, identity="session-test-asia")
            ledger.append("INCIDENT_SESSION_TEST", {**ny.payload(), "label": "ny"}, identity="session-test-ny")
            self.assertEqual(len(ledger.recent(session_kind=PaperSessionKind.ASIA_GLOBEX)), 1)
            self.assertEqual(ledger.recent(session_id=ny.session_id)[0]["payload"]["label"], "ny")
            ledger.close()


class NinjaSessionFenceSourceTests(unittest.TestCase):
    def test_compiled_addon_has_separate_session_and_time_fences(self) -> None:
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs").read_text(encoding="utf-8")
        for marker in (
            "America/New_York", "ASIA", "NEW_YORK_RTH", "NY_AFTER", "NEW_YORK", "ValidatePaperSessionFence",
            "SESSION_PROFILE_HASH_MISMATCH", "ENTRY_CUTOFF_PASSED", "SESSION_OFF_SESSION",
            "trade_date", "session_generation",
        ):
            self.assertIn(marker, source)


if __name__ == "__main__":
    unittest.main()
