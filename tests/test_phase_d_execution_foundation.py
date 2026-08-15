from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.copytrade.execution import DeterministicExecutionSimulator, ExecutionEngine, SimulatorPlan
from src.copytrade.execution_contracts import ExecutionSafetyContext, ExecutionState
from src.copytrade.models import CopySignal, as_utc, stable_id
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x1111111111111111111111111111111111111111"
TIME = as_utc("2026-01-01T00:00:00+00:00")


def signal(name: str, *, action: str = "open", direction: str = "long", quantity: float = 1.0) -> CopySignal:
    return CopySignal(
        signal_id=stable_id("phase_d_test_signal", name), target_wallet=WALLET, campaign_id="campaign",
        source_event_id=stable_id("phase_d_test_source", name), symbol="BTC", action=action, direction=direction,
        target_price=100.0, target_quantity=quantity, target_notional=quantity * 100.0, allocation_fraction=0.1,
        requested_capital=quantity * 100.0, created_at=TIME, source_event_timestamp=TIME,
        target_position_before=quantity if action in {"reduce", "close"} else 0.0,
    )


class PhaseDExecutionFoundationTests(unittest.TestCase):
    def engine(self, directory: Path, *plans: SimulatorPlan) -> tuple[CopyTradeDatabase, ExecutionEngine, DeterministicExecutionSimulator]:
        database = CopyTradeDatabase(directory / "copy.sqlite3")
        database.initialize()
        adapter = DeterministicExecutionSimulator(plans)
        return database, ExecutionEngine(database, adapter), adapter

    def test_duplicate_signals_and_concurrent_workers_create_one_intent_and_one_submit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("acknowledged"))
            item = signal("duplicate")
            with ThreadPoolExecutor(max_workers=2) as workers:
                results = list(workers.map(lambda _: engine.process_signal(item), range(2)))
            self.assertEqual({result.intent_id for result in results}, {stable_id("phase_d_execution_intent_v1", 1, item.signal_id)})
            self.assertEqual(len(database.list_execution_intents()), 1)
            self.assertEqual(adapter.submit_calls, 1)
            self.assertEqual(len(database.list_execution_state_events(results[0].intent_id)), 5)

    def test_illegal_transition_and_intent_rewrite_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, _ = self.engine(Path(temp))
            intent = engine.accept_signal(signal("immutable"))
            with self.assertRaisesRegex(ValueError, "Illegal Phase-D execution transition"):
                database.transition_execution_intent(intent.intent_id, ExecutionState.FILLED, reason="bad", source="test")
            altered = signal("immutable", quantity=2)
            altered = CopySignal(**{**altered.__dict__, "signal_id": intent.signal_id})
            with self.assertRaisesRegex(ValueError, "immutable, non-equivalent"):
                engine.accept_signal(altered)

    def test_unsupported_persisted_contract_version_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, _ = self.engine(Path(temp))
            intent = engine.accept_signal(signal("version"))
            with database._connect() as connection:  # type: ignore[attr-defined]
                connection.execute("UPDATE phase_d_execution_intents SET contract_version=2 WHERE intent_id=?", (intent.intent_id,))
            with self.assertRaisesRegex(ValueError, "Unsupported Phase-D execution contract version"):
                database.get_execution_intent(intent.intent_id)

    def test_timeout_after_acceptance_reconciles_without_resubmission(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("accepted_timeout"))
            intent = engine.process_signal(signal("timeout"))
            self.assertEqual(intent.state, ExecutionState.SUBMISSION_UNKNOWN)
            self.assertEqual(adapter.submit_calls, 1)
            restarted = ExecutionEngine(database, adapter)
            resolved = restarted.resume_intent(intent.intent_id)
            self.assertEqual(resolved.state, ExecutionState.ACKNOWLEDGED)
            self.assertEqual(adapter.submit_calls, 1)
            self.assertEqual(database.execution_read_model()["unknown_submissions"], 0)

    def test_timeout_after_rejection_reconciles_to_venue_rejection(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, engine, adapter = self.engine(Path(temp), SimulatorPlan("rejected_timeout", reason="insufficient_margin"))
            intent = engine.process_signal(signal("rejected-timeout"))
            self.assertEqual(intent.state, ExecutionState.SUBMISSION_UNKNOWN)
            resolved = engine.resume_intent(intent.intent_id)
            self.assertEqual(resolved.state, ExecutionState.REJECTED_BY_VENUE)
            self.assertEqual(adapter.submit_calls, 1)

    def test_crash_after_submit_converges_through_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("immediate_fill"))

            def crash(stage: str) -> None:
                if stage == "after_external_submit":
                    raise RuntimeError("simulated process loss after external side effect")

            intent = engine.process_signal(signal("crash"), fault_hook=crash)
            self.assertEqual(intent.state, ExecutionState.SUBMISSION_UNKNOWN)
            restored = ExecutionEngine(database, adapter).resume_intent(intent.intent_id)
            self.assertEqual(restored.state, ExecutionState.FILLED)
            self.assertEqual(adapter.submit_calls, 1)
            self.assertEqual(database.phase_d_local_positions(), {"BTC": 1.0})

    def test_partial_fill_duplicate_and_cancel_fill_race_preserve_actual_fill_quantity(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("partial", fill_quantities=(0.25,)))
            intent = engine.process_signal(signal("partial"))
            self.assertEqual(intent.state, ExecutionState.PARTIALLY_FILLED)
            cancelled = engine.request_cancel(intent.intent_id)
            self.assertEqual(cancelled.state, ExecutionState.CANCELLED)
            submission = database.get_execution_submission(intent.intent_id)
            self.assertIsNotNone(submission)
            adapter.emit_fill(submission.client_order_id, 0.30, venue_fill_id="late-fill")  # type: ignore[union-attr]
            adapter.emit_fill(submission.client_order_id, 0.30, venue_fill_id="late-fill")  # duplicate venue event
            final = engine.reconcile_intent(intent.intent_id)
            self.assertEqual(final.state, ExecutionState.CANCELLED)
            fills = database.list_execution_fills(intent.intent_id)
            self.assertEqual(len(fills), 2)
            self.assertAlmostEqual(sum(float(item["quantity"]) for item in fills), 0.55)
            self.assertAlmostEqual(database.phase_d_local_positions()["BTC"], 0.55)

    def test_mismatch_blocks_new_entries_but_not_a_safe_close(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp))
            opened = engine.process_signal(signal("open"))
            self.assertEqual(opened.state, ExecutionState.FILLED)
            adapter.set_reported_positions({"BTC": 0.5})
            reconciliation = engine.reconcile_positions()
            self.assertEqual(reconciliation["state"], "MISMATCH")
            blocked = engine.process_signal(signal("blocked-entry"))
            self.assertEqual(blocked.state, ExecutionState.BLOCKED)
            self.assertEqual(database.latest_execution_risk_decision(blocked.intent_id)["reason"], "entry_blocked_reconciliation_required")
            closed = engine.process_signal(signal("exit", action="close"))
            self.assertEqual(closed.state, ExecutionState.FILLED)

    def test_reduce_only_validation_bounds_a_close_to_verified_venue_position(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, engine, _ = self.engine(Path(temp))
            reduced = engine.process_signal(
                signal("oversized-exit", action="reduce", quantity=1.0),
                context=ExecutionSafetyContext(verified_positions={"BTC": 0.4}),
            )
            self.assertEqual(reduced.state, ExecutionState.BLOCKED)
            self.assertEqual(engine.store.latest_execution_risk_decision(reduced.intent_id)["reason"], "reduce_only_size_exceeds_position")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
