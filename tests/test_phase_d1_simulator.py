from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from src.copytrade.execution import (
    DeterministicExecutionSimulator,
    ExecutionEngine,
    SimulatedClock,
    SimulatorPlan,
    SimulatorScenario,
    SimulatorStep,
)
from src.copytrade.execution_contracts import ExecutionState
from src.copytrade.models import CopySignal, as_utc, stable_id
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x1111111111111111111111111111111111111111"
TIME = as_utc("2026-01-01T00:00:00+00:00")


def signal(name: str) -> CopySignal:
    return CopySignal(
        stable_id("d1-signal", name), WALLET, "campaign", stable_id("d1-source", name), "BTC", "open", "long",
        100, 1, 100, .1, 100, TIME, TIME,
    )


class DeterministicSimulatorTests(unittest.TestCase):
    def setup_engine(self, root: Path, simulator: DeterministicExecutionSimulator):
        database = CopyTradeDatabase(root / "copy.sqlite3")
        database.initialize()
        return database, ExecutionEngine(database, simulator)

    def test_timeout_before_acceptance_remains_unknown_without_resubmit(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            adapter = DeterministicExecutionSimulator([SimulatorPlan("timeout_before_accept")])
            database, engine = self.setup_engine(Path(temp), adapter)
            intent = engine.process_signal(signal("pre-accept"))
            self.assertEqual(intent.state, ExecutionState.SUBMISSION_UNKNOWN)
            self.assertEqual(adapter.submit_calls, 1)
            self.assertEqual(engine.resume_intent(intent.intent_id).state, ExecutionState.SUBMISSION_UNKNOWN)
            self.assertEqual(adapter.submit_calls, 1)
            self.assertEqual(database.execution_read_model()["unknown_submissions"], 1)

    def test_fill_before_ack_and_delayed_visibility_converge(self) -> None:
        scenario = SimulatorScenario(
            "fill-before-ack", submit_mode="acknowledged",
            submit_steps=(SimulatorStep("fill", .25, artifact_id="fill-a"),),
            reconciliation_steps=(SimulatorStep("fill", .75, artifact_id="fill-b"),),
        )
        with tempfile.TemporaryDirectory() as temp:
            database, engine = self.setup_engine(Path(temp), DeterministicExecutionSimulator(scenarios=(scenario,)))
            intent = engine.process_signal(signal("fill-before-ack"))
            self.assertEqual(intent.state, ExecutionState.PARTIALLY_FILLED)
            final = engine.reconcile_intent(intent.intent_id)
            self.assertEqual(final.state, ExecutionState.FILLED)
            self.assertEqual([item["venue_fill_id"] for item in database.list_execution_fills(intent.intent_id)], ["fill-a", "fill-b"])

    def test_scripted_duplicate_out_of_order_partial_fills_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            clock = SimulatedClock(TIME)
            adapter = DeterministicExecutionSimulator([SimulatorPlan("acknowledged")], clock=clock)
            database, engine = self.setup_engine(Path(temp), adapter)
            intent = engine.process_signal(signal("out-of-order"))
            submission = database.get_execution_submission(intent.intent_id)
            self.assertIsNotNone(submission)
            adapter.replay_steps(submission.client_order_id, (  # type: ignore[union-attr]
                SimulatorStep("fill", .45, artifact_id="third", milliseconds=30),
                SimulatorStep("fill", .25, artifact_id="first", milliseconds=10),
                SimulatorStep("fill", .30, artifact_id="second", milliseconds=10),
                SimulatorStep("duplicate_fill"),
            ))
            final = engine.reconcile_intent(intent.intent_id)
            self.assertEqual(final.state, ExecutionState.FILLED)
            fills = database.list_execution_fills(intent.intent_id)
            self.assertEqual({item["venue_fill_id"] for item in fills}, {"first", "second", "third"})
            self.assertAlmostEqual(sum(float(item["quantity"]) for item in fills), 1.0)

    def test_external_order_position_and_stale_reads_are_venue_side_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            adapter = DeterministicExecutionSimulator()
            _, engine = self.setup_engine(Path(temp), adapter)
            intent = engine.process_signal(signal("external"))
            adapter.inject_external_order("ETH", -2)
            adapter.inject_external_position("ETH", -2)
            self.assertEqual(len(adapter.list_open_orders()), 1)
            adapter.set_stale_positions([])
            self.assertEqual(adapter.get_positions(), [])
            adapter.set_stale_positions(None)
            reconciliation = engine.reconcile_positions()
            self.assertEqual(reconciliation["state"], "MISMATCH")
            self.assertEqual(intent.state, ExecutionState.FILLED)

    def test_identical_script_replay_has_identical_economic_outcome(self) -> None:
        scenario = SimulatorScenario(
            "replay", submit_steps=(SimulatorStep("fill", .25, artifact_id="one"),),
            reconciliation_steps=(SimulatorStep("fill", .75, artifact_id="two"),),
        )
        outcomes = []
        for _ in range(2):
            with tempfile.TemporaryDirectory() as temp:
                database, engine = self.setup_engine(
                    Path(temp), DeterministicExecutionSimulator(scenarios=(scenario,), clock=SimulatedClock(TIME)),
                )
                intent = engine.process_signal(signal("replay"))
                final = engine.reconcile_intent(intent.intent_id)
                outcomes.append((final.intent_id, final.state.value, database.phase_d_local_positions(), [
                    (item["venue_fill_id"], item["quantity"], item["price"]) for item in database.list_execution_fills(intent.intent_id)
                ]))
        self.assertEqual(outcomes[0], outcomes[1])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
