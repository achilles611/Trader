from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from zoneinfo import ZoneInfo

from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.contracts import POLICY, RISK_PROFILE
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.ninjatrader_transport import (
    EXPECTED_ADDON_SOURCE_FINGERPRINT,
    expected_addon_source_fingerprint,
)
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.sessions import (
    ASIA_PROFILE,
    LONDON_PROFILE,
    NEW_YORK_RTH_PROFILE,
    NY_AFTER_PROFILE,
    PaperCalendarState,
    PaperSessionCalendar,
    PaperSessionKind,
    PaperSessionResolver,
    context_from_identity,
    session_catalog,
)
from src.l3g_paper.slim_status import derive_slim_paper_status
from src.ops_scheduler.service import scheduler_templates
from src.ops_scheduler.tasks import validate_session
from src.ops_scheduler.triggers import resolve_occurrences

from tests.l3g_helpers import ObservationFactory


UTC = timezone.utc
LONDON = ZoneInfo("Europe/London")
NEW_YORK = ZoneInfo("America/New_York")


def resolve(value: str | datetime):
    return PaperSessionResolver().resolve(value, generation=7)


def warm_london(runtime: LaneIIIPaperRuntime) -> ObservationFactory:
    factory = ObservationFactory(
        session="authentic-london-fixture",
        start=datetime(2026, 7, 6, 7, 0, tzinfo=UTC),
    )
    runtime.on_observation_transport_state(StreamHealth.HEALTHY)
    runtime.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
    for price in (100, 99, 100):
        quote = factory.quote(price)
        runtime.ingest(quote)
        runtime.ingest(factory.trade(quote, price))
    for operation, volume in (("ADD", 10), ("UPDATE", 5), ("UPDATE", 10), ("UPDATE", 5), ("UPDATE", 11)):
        runtime.ingest(factory.depth(operation, volume))
    return factory


class LondonClassificationTests(unittest.TestCase):
    def test_exact_gmt_and_bst_half_open_boundaries(self) -> None:
        cases = (
            ("2026-01-15T07:59:59.999999Z", PaperSessionKind.OFF_SESSION, False),
            ("2026-01-15T08:00:00Z", PaperSessionKind.LONDON, True),
            ("2026-01-15T11:29:59.999999Z", PaperSessionKind.LONDON, True),
            ("2026-01-15T11:30:00Z", PaperSessionKind.OFF_SESSION, False),
            ("2026-07-15T06:59:59.999999Z", PaperSessionKind.OFF_SESSION, False),
            ("2026-07-15T07:00:00Z", PaperSessionKind.LONDON, True),
            ("2026-07-15T10:29:59.999999Z", PaperSessionKind.LONDON, True),
            ("2026-07-15T10:30:00Z", PaperSessionKind.OFF_SESSION, False),
        )
        for timestamp, expected_kind, expected_entry in cases:
            with self.subTest(timestamp=timestamp):
                result = resolve(timestamp)
                self.assertEqual(result.context.session_kind, expected_kind)
                self.assertEqual(result.entry_authorized, expected_entry)
        winter_context = resolve("2026-01-15T08:00:00Z").context
        summer_context = resolve("2026-07-15T07:00:00Z").context
        self.assertEqual(winter_context.boundary_at("hard_flat_deadline"), datetime(2026, 1, 15, 11, 30, tzinfo=UTC))
        self.assertEqual(summer_context.boundary_at("hard_flat_deadline"), datetime(2026, 7, 15, 10, 30, tzinfo=UTC))
        self.assertFalse(winter_context.hard_flat_due_at(datetime(2026, 1, 15, 11, 29, 59, 999999, tzinfo=UTC)))
        self.assertTrue(winter_context.hard_flat_due_at(datetime(2026, 1, 15, 11, 30, tzinfo=UTC)))

    def test_both_uk_us_dst_mismatch_periods_and_new_york_conversion(self) -> None:
        # The US is on daylight time while the UK is still/on-again GMT in
        # both mismatch periods. London remains authoritative in each case.
        for local_new_york in (
            datetime(2026, 3, 16, 4, 0, tzinfo=NEW_YORK),
            datetime(2026, 10, 26, 4, 0, tzinfo=NEW_YORK),
        ):
            with self.subTest(local_new_york=local_new_york.isoformat()):
                self.assertEqual(
                    (local_new_york.astimezone(LONDON).hour, local_new_york.astimezone(LONDON).minute),
                    (8, 0),
                )
                result = resolve(local_new_york)
                self.assertEqual(result.context.session_kind, PaperSessionKind.LONDON)
                self.assertEqual(result.context.timezone, "Europe/London")
                self.assertEqual(result.context.boundary_at("entry_start"), local_new_york.astimezone(UTC))

    def test_precedence_and_adjacent_sessions_remain_deterministic(self) -> None:
        summer = (
            ("2026-07-06T05:59:59Z", PaperSessionKind.ASIA),       # 01:59:59 New York
            ("2026-07-06T06:00:00Z", PaperSessionKind.OFF_SESSION),
            ("2026-07-06T06:59:59Z", PaperSessionKind.OFF_SESSION),
            ("2026-07-06T07:00:00Z", PaperSessionKind.LONDON),
            ("2026-07-06T10:30:00Z", PaperSessionKind.OFF_SESSION),
            ("2026-07-06T13:30:00Z", PaperSessionKind.NEW_YORK_RTH),
            ("2026-07-06T20:00:00Z", PaperSessionKind.NY_AFTER),
        )
        for timestamp, expected in summer:
            with self.subTest(timestamp=timestamp):
                self.assertEqual(resolve(timestamp).context.session_kind, expected)
        self.assertEqual(
            [item["session_kind"] for item in session_catalog()],
            ["ASIA", "LONDON", "NEW_YORK_RTH", "NY_AFTER"],
        )

    def test_weekend_and_existing_holiday_fence_fail_closed(self) -> None:
        weekend = resolve("2026-07-11T07:30:00Z")
        self.assertEqual(weekend.context.session_kind, PaperSessionKind.OFF_SESSION)
        self.assertFalse(weekend.entry_authorized)

        holiday = resolve("2026-07-03T07:30:00Z")
        self.assertEqual(holiday.context.session_kind, PaperSessionKind.LONDON)
        self.assertEqual(holiday.context.calendar_state, PaperCalendarState.HOLIDAY_OVERRIDE_REQUIRED)
        self.assertEqual(holiday.reason_code, "HOLIDAY_SESSION_UNVERIFIED")
        verified = PaperSessionResolver(PaperSessionCalendar({
            "2026-07-03": PaperCalendarState.HOLIDAY_OVERRIDE_VERIFIED,
        })).resolve("2026-07-03T07:30:00Z")
        self.assertTrue(verified.entry_authorized)

    def test_profile_contract_and_existing_profile_hashes_are_stable(self) -> None:
        self.assertEqual(LONDON_PROFILE.timezone, "Europe/London")
        self.assertEqual(LONDON_PROFILE.session_kind, PaperSessionKind.LONDON)
        self.assertEqual(LONDON_PROFILE.payload()["session_profile_hash"], "db211b6665e873fc3bf0b93db76210b25d154893ca1d5ca15ef0d7d6bea233cc")
        self.assertEqual(ASIA_PROFILE.profile_hash, "55225b35ccdb289d179bb23afd7f3fdb2c5ab193d53aba21603f17ff9f6d43aa")
        self.assertEqual(NEW_YORK_RTH_PROFILE.profile_hash, "8b8560a08ff41963a7a78d09bc977fbc1faf10f4a11ce58d05f47cacd89e0814")
        self.assertEqual(NY_AFTER_PROFILE.profile_hash, "e0cea9aa679c24ad4491ad929bcd72832cd3dcb49e2e5a7a64226c8abb5a1db2")
        old_asia = context_from_identity(
            PaperSessionKind.ASIA,
            "MNQU6:ASIA:2026-07-06",
            "2026-07-06",
            ASIA_PROFILE.profile_hash,
            3,
        )
        self.assertEqual(old_asia.session_family.value, "ASIA")


class LondonIsolationAndSerializationTests(unittest.TestCase):
    def test_startup_restart_and_transition_do_not_recover_london_warmup(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            first = LaneIIIPaperRuntime(ledger)
            warm_london(first)
            first_status = first.status()
            self.assertEqual(first_status["current_session"], "LONDON")
            self.assertTrue(first_status["commissioning_session_warmed"])
            first.stop()
            ledger.close()

            restarted_ledger = PaperLedger(path)
            restarted = LaneIIIPaperRuntime(restarted_ledger)
            london = ObservationFactory(
                session="restart-london-fixture",
                start=datetime(2026, 7, 6, 7, 15, tzinfo=UTC),
            )
            restarted.ingest(london.quote(100))
            restarted_status = restarted.status()
            self.assertEqual(restarted_status["current_session"], "LONDON")
            self.assertFalse(restarted_status["commissioning_session_warmed"])

            new_york = ObservationFactory(
                session="new-york-after-london-fixture",
                start=datetime(2026, 7, 6, 13, 35, tzinfo=UTC),
            )
            restarted.ingest(new_york.quote(101))
            transitioned = restarted.status()
            self.assertEqual(transitioned["current_session"], "NEW_YORK_RTH")
            self.assertFalse(transitioned["commissioning_session_warmed"])
            self.assertGreater(transitioned["session_generation"], restarted_status["session_generation"])
            restarted.stop()
            restarted_ledger.close()

    def test_london_warmup_evidence_and_ledger_partition_are_independent(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            warm_london(runtime)
            status = runtime.status()
            self.assertEqual(status["current_session_family"], "EUROPE")
            self.assertEqual(status["session_timezone"], "Europe/London")
            self.assertTrue(status["commissioning_session_warmed"])
            self.assertEqual(status["last_paper_decision"]["session_kind"], "LONDON")
            self.assertGreater(len(runtime.policy.active_evidence("2026-07-06T07:00:02Z")), 0)
            london_rows = ledger.recent(1000, session_kind="LONDON", session_family="EUROPE")
            self.assertGreater(len(london_rows), 0)
            self.assertTrue(all(row["session_kind"] == "LONDON" and row["session_family"] == "EUROPE" for row in london_rows))
            self.assertEqual(ledger.recent(1000, session_kind="ASIA"), [])

            off_session = ObservationFactory(
                session="post-london-fixture",
                start=datetime(2026, 7, 6, 10, 30, tzinfo=UTC),
            )
            runtime.ingest(off_session.quote(101))
            self.assertEqual(runtime.status()["current_session"], "OFF_SESSION")
            self.assertFalse(runtime.status()["commissioning_session_warmed"])
            runtime.stop()
            ledger.close()

    def test_runtime_api_and_slim_projection_serialize_london_first_class(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            factory = ObservationFactory(start=datetime(2026, 1, 15, 8, 5, tzinfo=UTC))
            runtime.ingest(factory.quote(100))
            status = runtime.status()
            self.assertEqual(status["entry_profile"], "NY_HIGH_CONFLUENCE_COMMISSIONING")
            self.assertEqual(status["entry_profile_version"], "NY_HIGH_CONFLUENCE_COMMISSIONING_V1")
            self.assertEqual(status["entry_session_kind"], "NEW_YORK_RTH")
            self.assertEqual(status["effective_confidence_threshold"], "0.675")
            self.assertEqual(status["current_session"], "LONDON")
            self.assertEqual(status["current_session_family"], "EUROPE")
            self.assertEqual(status["london_session_pnl"], "0")
            london_definition = next(item for item in status["session_definitions"] if item["session_kind"] == "LONDON")
            self.assertEqual(london_definition["timezone"], "Europe/London")
            self.assertEqual((london_definition["entry_start"], london_definition["entry_cutoff"]), ("08:00", "11:30"))
            slim = derive_slim_paper_status(status, {}, {}, {"result": "BLOCKED", "blocking_reasons": ["NO_CURRENT_EVENT_SESSION"]})
            self.assertEqual(slim["session"]["session_kind"], "LONDON")
            self.assertEqual(slim["session"]["session_family"], "EUROPE")
            self.assertEqual(slim["session"]["timezone"], "Europe/London")
            runtime.stop()
            ledger.close()

    def test_ny_commissioning_policy_risk_and_addon_fingerprint_remain_sealed(self) -> None:
        self.assertEqual(POLICY.policy_id, "l3g-ny-high-confluence-commissioning-policy-v1")
        self.assertEqual(POLICY.entry_support_threshold, Decimal("0.675"))
        self.assertEqual(POLICY.entry_dominance_margin, Decimal("0.10"))
        self.assertEqual(RISK_PROFILE.maximum_position_age_seconds, 3600)
        self.assertEqual(RISK_PROFILE.maximum_session_entries, 1)
        self.assertEqual(RISK_PROFILE.maximum_absolute_position, 1)
        self.assertFalse(RISK_PROFILE.approved_for_live)
        self.assertEqual(expected_addon_source_fingerprint(), EXPECTED_ADDON_SOURCE_FINGERPRINT)
        source = (Path(__file__).parents[1] / "ninjatrader" / "NinjaScript" / "AddOns" / "BeelzebubPaperExecutionAddOn.cs").read_text(encoding="utf-8")
        for marker in (
            'LondonTimezone = "Europe/London"',
            'expectedKind = "LONDON"',
            'expectedFamily = "EUROPE"',
            "LondonProfileHash",
            "GMT Standard Time",
        ):
            self.assertIn(marker, source)


class LondonSchedulerTests(unittest.TestCase):
    def test_london_is_valid_in_tasks_templates_and_dst_aware_occurrences(self) -> None:
        self.assertEqual(validate_session({"session": "london"}), {"session": "LONDON"})
        london_templates = [item for item in scheduler_templates() if item["template_id"].startswith("london-")]
        self.assertEqual(len(london_templates), 4)

        summer = resolve_occurrences(
            "SESSION_RELATIVE",
            {"session": "LONDON", "event": "OPEN", "offset_minutes": 0},
            after=datetime(2026, 7, 5, tzinfo=UTC),
            count=1,
        )[0]
        winter = resolve_occurrences(
            "SESSION_RELATIVE",
            {"session": "LONDON", "event": "OPEN", "offset_minutes": 0},
            after=datetime(2026, 1, 4, tzinfo=UTC),
            count=1,
        )[0]
        self.assertEqual(summer.due_at, datetime(2026, 7, 6, 7, 0, tzinfo=UTC))
        self.assertEqual(winter.due_at, datetime(2026, 1, 5, 8, 0, tzinfo=UTC))
        self.assertEqual(summer.timezone, "Europe/London")


if __name__ == "__main__":
    unittest.main()
