from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.contracts import (
    FIVE_MINUTE_POLICY,
    POLICY,
    PaperDecisionKind,
    PaperDirection,
    resolve_paper_policy_profile,
)
from src.l3g_paper.ledger import PaperLedger
from src.l3g_paper.five_minute_analysis import export_session_analysis
from src.l3g_paper.policy import ExperimentalPaperPolicy
from src.l3g_paper.runtime import LaneIIIPaperRuntime
from tests.l3g_helpers import ObservationFactory


class FiveMinuteProfileTests(unittest.TestCase):
    @staticmethod
    def ready_policy() -> tuple[ExperimentalPaperPolicy, ObservationFactory]:
        policy = ExperimentalPaperPolicy(FIVE_MINUTE_POLICY)
        policy.on_transport_state(StreamHealth.HEALTHY)
        factory = ObservationFactory(start=datetime(2026, 8, 24, 14, 0, tzinfo=timezone.utc))
        policy.ingest_runtime(
            factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"})
        )
        policy.ingest_runtime(factory.quote(100))
        policy._depth_recovering = False
        return policy, factory

    @staticmethod
    def scores(policy: ExperimentalPaperPolicy, bullish: str, bearish: str) -> None:
        families = {"positive_family_count": 1, "blocking_contradiction": False}

        def score(_at: str, hypothesis: object) -> tuple[Decimal, dict[str, object]]:
            value = bullish if getattr(hypothesis, "value", None) == "BULLISH_REVERSAL" else bearish
            return Decimal(value), dict(families)

        policy.score = score  # type: ignore[method-assign]

    def advance_boundary(self, factory: ObservationFactory) -> None:
        factory.start += timedelta(minutes=5)

    def test_profile_identity_is_separate_and_selection_is_closed(self) -> None:
        self.assertEqual(resolve_paper_policy_profile(None), POLICY)
        self.assertEqual(resolve_paper_policy_profile("BEELZEBUB_SCALPER_V2"), POLICY)
        self.assertEqual(
            resolve_paper_policy_profile("BEELZEBUB_FIVE_MINUTE_BIAS_V1"),
            FIVE_MINUTE_POLICY,
        )
        self.assertNotEqual(FIVE_MINUTE_POLICY.configuration_hash, POLICY.configuration_hash)
        self.assertEqual(
            FIVE_MINUTE_POLICY.configuration_hash,
            "9d94d10feb963a1815b9928893868ddc793d5ec42b6835148d65ed0c9956bdac",
        )
        self.assertEqual(FIVE_MINUTE_POLICY.decision_interval_seconds, 300)
        with self.assertRaisesRegex(ValueError, "Unknown BEELZEBUB_L3G_PAPER_PROFILE"):
            resolve_paper_policy_profile("UNSEALED_PROFILE")

    def test_waits_for_next_boundary_then_enters_once(self) -> None:
        policy, factory = self.ready_policy()
        self.scores(policy, "0.60", "0.50")
        self.advance_boundary(factory)
        decision = policy.ingest_runtime(factory.quote(100))
        self.assertIsNotNone(decision)
        assert decision is not None
        self.assertEqual(decision.decision, PaperDecisionKind.LONG)
        self.assertEqual(decision.reason_code, "FIVE_MINUTE_ENTER_LONG")
        self.assertEqual(decision.family_summary["action"], "ENTER")
        self.assertEqual(decision.family_summary["candle_close_utc"], "2026-08-24T14:05:00Z")
        self.assertGreaterEqual(decision.family_summary["decision_latency_ms"], 0)
        self.assertIsNone(policy.ingest_runtime(factory.quote(100)))

    def test_holds_same_bias_and_stages_opposite_bias_reversal(self) -> None:
        policy, factory = self.ready_policy()
        self.scores(policy, "0.61", "0.49")
        self.advance_boundary(factory)
        held = policy.ingest_runtime(factory.quote(100), current_position=PaperDirection.LONG)
        assert held is not None
        self.assertEqual(held.decision, PaperDecisionKind.NO_TRADE)
        self.assertEqual(held.reason_code, "FIVE_MINUTE_HOLD_LONG")
        self.assertEqual(held.family_summary["action"], "HOLD")

        self.scores(policy, "0.44", "0.62")
        self.advance_boundary(factory)
        reversal = policy.ingest_runtime(factory.quote(99), current_position=PaperDirection.LONG)
        assert reversal is not None
        self.assertEqual(reversal.decision, PaperDecisionKind.EXIT)
        self.assertEqual(reversal.reason_code, "FIVE_MINUTE_REVERSE_TO_SHORT")
        self.assertEqual(reversal.family_summary["target_position"], "SHORT")
        self.assertEqual(reversal.family_summary["decision_protocol"], "EXIT_RECONCILE_THEN_ENTER")

    def test_tie_is_flat_block_or_position_hold(self) -> None:
        policy, factory = self.ready_policy()
        self.scores(policy, "0.50", "0.50")
        self.advance_boundary(factory)
        flat = policy.ingest_runtime(factory.quote(100), current_position=PaperDirection.FLAT)
        assert flat is not None
        self.assertEqual(flat.reason_code, "FIVE_MINUTE_BIAS_TIE_FLAT")
        self.assertEqual(flat.family_summary["action"], "BLOCKED")

        self.advance_boundary(factory)
        positioned = policy.ingest_runtime(factory.quote(100), current_position=PaperDirection.SHORT)
        assert positioned is not None
        self.assertEqual(positioned.reason_code, "FIVE_MINUTE_BIAS_TIE_HOLD")
        self.assertEqual(positioned.family_summary["target_position"], "SHORT")

    def test_profile_cannot_append_to_a_v2_ledger_epoch(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "paper.sqlite3"
            ledger = PaperLedger(path)
            ledger.append("SESSION_AUTHORITY", {"test": "v2"})
            ledger.close()
            with self.assertRaisesRegex(RuntimeError, "PAPER_PROFILE_LEDGER_EPOCH_MISMATCH"):
                PaperLedger(path, policy=FIVE_MINUTE_POLICY)

    def test_reversal_entry_is_a_new_decision_after_flat_reconciliation(self) -> None:
        policy, factory = self.ready_policy()
        self.scores(policy, "0.40", "0.65")
        self.advance_boundary(factory)
        reversal = policy.ingest_runtime(factory.quote(99), current_position=PaperDirection.LONG)
        assert reversal is not None

        with TemporaryDirectory() as directory:
            ledger = PaperLedger(Path(directory) / "paper.sqlite3", policy=FIVE_MINUTE_POLICY)
            runtime = LaneIIIPaperRuntime(ledger)
            runtime._session_context = policy.session_context
            captured: list[object] = []
            runtime._request_entry = lambda decision: captured.append(decision) or True  # type: ignore[method-assign]
            runtime._last_reconciliation = {"timestamp": reversal.created_at}
            runtime._request_five_minute_reversal_entry(reversal)
            self.assertEqual(len(captured), 1)
            entry = captured[0]
            self.assertEqual(entry.decision, PaperDecisionKind.SHORT)
            self.assertEqual(entry.reason_code, "FIVE_MINUTE_REVERSE_ENTRY_SHORT")
            self.assertEqual(entry.source_observation_ids, (reversal.paper_decision_id,))
            ledger.close()

    def test_closed_session_exports_json_csv_and_reason_log(self) -> None:
        policy, factory = self.ready_policy()
        self.scores(policy, "0.60", "0.48")
        self.advance_boundary(factory)
        first = policy.ingest_runtime(factory.quote(100), current_position=PaperDirection.FLAT)
        assert first is not None
        policy.ingest_runtime(factory.quote(101), current_position=PaperDirection.LONG)
        self.advance_boundary(factory)
        second = policy.ingest_runtime(factory.quote(101), current_position=PaperDirection.LONG)
        assert second is not None

        with TemporaryDirectory() as directory:
            root = Path(directory)
            ledger_path = root / "paper.sqlite3"
            ledger = PaperLedger(ledger_path, policy=FIVE_MINUTE_POLICY)
            for decision in (first, second):
                ledger.append(
                    "DECISION", decision.payload(), identity=decision.paper_decision_id,
                    occurred_at=decision.created_at,
                )
            ledger.append(
                "EXECUTION_REALIZED_PNL",
                {
                    "session_id": first.session_id,
                    "entry_decision_id": first.paper_decision_id,
                    "realized_pnl": "0.50",
                },
            )
            ledger.append(
                "SESSION_OPERATIONAL_PAPER_STOPPED",
                {**policy.session_context.payload(), "final_position": "FLAT"},
            )
            runtime = LaneIIIPaperRuntime(ledger)
            runtime.bind_runtime_identity({
                "git_sha": "test", "ledger": str(ledger_path),
                "audit": str(root / "analysis-root"), "control_center": "test",
                "python": "test", "pid": 1,
            })
            runtime._export_five_minute_analysis_locked(policy.session_context)
            self.assertEqual(runtime._last_five_minute_analysis["status"], "EXPORTED")
            ledger.close()

            result = export_session_analysis(
                ledger_path, root / "analysis-root" / "five-minute-session-analysis",
            )
            self.assertEqual(result["summary"]["decision_count"], 2)
            self.assertEqual(result["summary"]["worked_count"], 1)
            self.assertEqual(result["summary"]["realized_trade_pnl"], "0.50")
            for artifact in result["artifacts"].values():
                self.assertTrue(Path(artifact["path"]).is_file())
            markdown_path = Path(result["artifacts"]["markdown"]["path"])
            self.assertIn("FIVE_MINUTE_ENTER_LONG", markdown_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
