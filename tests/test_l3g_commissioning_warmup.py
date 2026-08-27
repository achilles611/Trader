from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservationError
from src.l3f_provider.tradovate_observation import ProviderErrorCode, StreamHealth
from src.l3g_paper.contracts import PaperRuntimeState
from src.l3g_paper.ledger import (
    CommissioningTailCategory,
    PaperLedger,
    commissioning_tail_classification,
)
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from src.l3g_paper.sessions import UNSPECIFIED_OFF_SESSION_CONTEXT

from .l3g_helpers import ObservationFactory


START = datetime(2026, 8, 26, 14, 0, tzinfo=timezone.utc)


class CommissioningWarmupTests(unittest.TestCase):
    @staticmethod
    def warm(runtime: LaneIIIPaperRuntime) -> ObservationFactory:
        factory = ObservationFactory(start=START)
        runtime.on_observation_transport_state(StreamHealth.HEALTHY)
        runtime.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
        for price in (100, 99, 100):
            quote = factory.quote(price)
            runtime.ingest(quote)
            runtime.ingest(factory.trade(quote, price))
        for operation, volume in (("ADD", 10), ("UPDATE", 5), ("UPDATE", 10), ("UPDATE", 5), ("UPDATE", 11)):
            runtime.ingest(factory.depth(operation, volume))
        return factory

    def test_authentic_required_families_latch_once_and_natural_expiry_does_not_clear_it(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            factory = self.warm(runtime)
            status = runtime.status()
            self.assertTrue(status["commissioning_session_warmed"])
            progress = status["commissioning_warmup"]["required_families"]
            self.assertEqual(set(progress), {"STRUCTURAL_CONTEXT", "ORDER_FLOW", "RESTING_LIQUIDITY"})
            self.assertTrue(all(value["seen"] for value in progress.values()))
            warmed_records = ledger.recent_kinds(("COMMISSIONING_SESSION_WARMED",))
            self.assertEqual(len(warmed_records), 1)
            self.assertEqual(warmed_records[0]["payload"]["authority_effect"], "NONE")
            self.assertEqual(
                warmed_records[0]["payload"]["record_semantics"],
                "COMMISSIONING_READINESS_STATE_ATTESTATION",
            )
            self.assertEqual(warmed_records[0]["payload"]["record_semantics_version"], 1)
            self.assertEqual(warmed_records[0]["payload"]["commissioning_warmup_state"], "WARMED")
            self.assertEqual(
                commissioning_tail_classification(
                    "INCIDENT", warmed_records[0]["kind"], warmed_records[0]["payload"],
                ).category,
                CommissioningTailCategory.AUTHORITY_OBSERVATION,
            )

            after_expiry = (factory.start + timedelta(minutes=3)).isoformat().replace("+00:00", "Z")
            self.assertEqual(runtime.policy.active_evidence(after_expiry), ())
            runtime._observe_commissioning_warmup(after_expiry)
            self.assertTrue(runtime.status()["commissioning_session_warmed"])
            self.assertEqual(len(ledger.recent_kinds(("COMMISSIONING_SESSION_WARMED",))), 1)
            runtime.stop(); ledger.close()

    def test_canonical_continuity_failures_reset_the_latch(self) -> None:
        def sequence_gap(runtime: LaneIIIPaperRuntime, factory: ObservationFactory) -> None:
            factory.sequence += 1
            runtime.ingest(factory.quote(100))

        def rejection(runtime: LaneIIIPaperRuntime, _: ObservationFactory) -> None:
            runtime.on_observation_rejection(NinjaTraderObservationError(ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD, "test"))

        def malformed(runtime: LaneIIIPaperRuntime, factory: ObservationFactory) -> None:
            runtime.ingest(factory.make("QUOTE", {
                "contract_id": "MNQ SEP26", "bid": "101", "ask": "100",
                "bid_size": 1, "ask_size": 1,
            }))

        def local_disconnect(runtime: LaneIIIPaperRuntime, _: ObservationFactory) -> None:
            runtime.on_observation_transport_state(StreamHealth.DISCONNECTED)

        def market_disconnect(runtime: LaneIIIPaperRuntime, factory: ObservationFactory) -> None:
            runtime.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Disconnected"}))

        def market_reconnect(runtime: LaneIIIPaperRuntime, factory: ObservationFactory) -> None:
            runtime.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))

        def observation_session_change(runtime: LaneIIIPaperRuntime, factory: ObservationFactory) -> None:
            factory.session = "replacement-market-session"
            runtime.ingest(factory.quote(100))

        def timestamp_backward(runtime: LaneIIIPaperRuntime, factory: ObservationFactory) -> None:
            event = factory.quote(100)
            runtime.ingest(replace(
                event,
                ninja_receipt_time="2026-08-26T13:59:59Z",
                provider_timestamp="2026-08-26T13:59:59Z",
            ))

        def depth_reset(runtime: LaneIIIPaperRuntime, factory: ObservationFactory) -> None:
            event = factory.depth("UPDATE", 10)
            runtime.ingest(replace(event, payload={**event.payload, "is_reset": True}))

        def contract_mismatch(runtime: LaneIIIPaperRuntime, factory: ObservationFactory) -> None:
            event = factory.quote(100)
            runtime.ingest(replace(event, payload={**event.payload, "contract_id": "NQ SEP26"}))

        cases = {
            "sequence gap": sequence_gap,
            "observation rejection": rejection,
            "malformed observation": malformed,
            "local bridge disconnect": local_disconnect,
            "market-data disconnect": market_disconnect,
            "market-data reconnect": market_reconnect,
            "observation-session change": observation_session_change,
            "timestamp moved backward": timestamp_backward,
            "depth reset": depth_reset,
            "contract mismatch": contract_mismatch,
        }
        for name, action in cases.items():
            with self.subTest(name=name), TemporaryDirectory() as directory:
                ledger = PaperLedger(Path(directory) / "paper.sqlite3")
                runtime = LaneIIIPaperRuntime(ledger)
                factory = self.warm(runtime)
                self.assertTrue(runtime.status()["commissioning_session_warmed"])
                action(runtime, factory)
                self.assertFalse(runtime.status()["commissioning_session_warmed"])
                reset_records = ledger.recent_kinds(("COMMISSIONING_SESSION_WARMUP_RESET",), limit=20)
                self.assertEqual(len(reset_records), 1)
                self.assertEqual(reset_records[0]["payload"]["authority_effect"], "NONE")
                self.assertEqual(
                    reset_records[0]["payload"]["record_semantics"],
                    "COMMISSIONING_READINESS_STATE_ATTESTATION",
                )
                self.assertEqual(reset_records[0]["payload"]["record_semantics_version"], 1)
                self.assertEqual(reset_records[0]["payload"]["commissioning_warmup_state"], "NOT_WARMED")
                self.assertEqual(
                    commissioning_tail_classification(
                        "INCIDENT", reset_records[0]["kind"], reset_records[0]["payload"],
                    ).category,
                    CommissioningTailCategory.AUTHORITY_OBSERVATION,
                )
                runtime.stop(); ledger.close()

    def test_session_generation_off_session_reconnect_and_restart_start_cold(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            runtime = LaneIIIPaperRuntime(ledger)
            self.warm(runtime)
            context = runtime._session_context
            runtime._set_session_context(replace(context, session_generation=context.session_generation + 1), reason="TEST_GENERATION")
            self.assertFalse(runtime.status()["commissioning_session_warmed"])

            runtime._snapshot = replace(runtime._snapshot, commissioning_session_warmed=True)
            runtime._commissioning_warmup_warmed_at = "2026-08-26T14:00:01Z"
            runtime._set_session_context(UNSPECIFIED_OFF_SESSION_CONTEXT, reason="TEST_OFF_SESSION")
            self.assertFalse(runtime.status()["commissioning_session_warmed"])

            runtime._snapshot = replace(runtime._snapshot, commissioning_session_warmed=True)
            runtime._commissioning_warmup_warmed_at = "2026-08-26T14:00:01Z"
            runtime.on_observation_transport_state(StreamHealth.DISCONNECTED)
            runtime._snapshot = replace(runtime._snapshot, commissioning_session_warmed=True)
            runtime._commissioning_warmup_warmed_at = "2026-08-26T14:00:01Z"
            runtime.on_observation_transport_state(StreamHealth.HEALTHY)
            self.assertFalse(runtime.status()["commissioning_session_warmed"])

            runtime._snapshot = replace(runtime._snapshot, commissioning_session_warmed=True)
            runtime._commissioning_warmup_warmed_at = "2026-08-26T14:00:01Z"
            runtime.on_execution_bridge_state("DISCONNECTED")
            self.assertFalse(runtime.status()["commissioning_session_warmed"])
            runtime._snapshot = replace(runtime._snapshot, commissioning_session_warmed=True)
            runtime._commissioning_warmup_warmed_at = "2026-08-26T14:00:01Z"
            runtime.on_execution_bridge_state("AUTHENTICATED")
            self.assertFalse(runtime.status()["commissioning_session_warmed"])
            runtime.stop(); ledger.close()

            restarted_ledger = PaperLedger(path)
            restarted = LaneIIIPaperRuntime(restarted_ledger)
            self.assertFalse(restarted.status()["commissioning_session_warmed"])
            restarted.stop(); restarted_ledger.close()

    def test_rehearsal_is_authority_free_and_reports_each_shared_gate(self) -> None:
        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3")
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._state = PaperRuntimeState.READY_DISARMED
            before = ledger.health_status()["highest_sequence"]
            risk_before = runtime.risk.status()["arm_attempts"]
            at = "2026-08-26T14:00:00Z"
            with patch("src.l3g_paper.runtime._now", return_value=at):
                result = runtime.commissioning_rehearsal(lambda commissioning_id, snapshot: {
                    "ledger_trust_state": "TEST_VERIFIED_ANCHOR",
                    "verified_anchor": 10,
                    "arm_snapshot_tip": 12,
                    "unverified_tail_rows": 2,
                })
            self.assertEqual(result["result"], "BLOCKED")
            self.assertIn("commissioning_warmup", result)
            self.assertIn("strategy_evidence", result)
            self.assertIn("market_freshness", result)
            self.assertIn("broker", result)
            self.assertIn("addon", result)
            self.assertIn("ownership", result)
            ledger.flush_deferred()
            self.assertEqual(runtime.state, PaperRuntimeState.READY_DISARMED)
            self.assertEqual(runtime.status()["entry_owner"], "NONE")
            self.assertEqual(runtime.risk.status()["arm_attempts"], risk_before)
            self.assertEqual(ledger.health_status()["highest_sequence"], before)
            runtime.stop(); ledger.close()


if __name__ == "__main__":
    unittest.main()
