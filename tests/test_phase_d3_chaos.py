from __future__ import annotations

import random
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

from src.copytrade.config import ArtifactConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig
from src.copytrade.execution import (
    DeterministicExecutionSimulator,
    DeterministicFaultInjector,
    ExecutionEngine,
    ExecutionSafetyContext,
    ExecutionState,
    InjectedExecutionFault,
    SimulatorPlan,
)
from src.copytrade.models import CopySignal, as_utc, stable_id
from src.copytrade.paper import PaperExecutionEngine
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x1111111111111111111111111111111111111111"
TIME = as_utc("2026-01-01T00:00:00+00:00")


def signal(name: str, *, action: str = "open", direction: str = "long", quantity: float = 1.0) -> CopySignal:
    return CopySignal(
        signal_id=stable_id("phase_d3_signal", name), target_wallet=WALLET, campaign_id="campaign",
        source_event_id=stable_id("phase_d3_source", name), symbol="BTC", action=action, direction=direction,
        target_price=100.0, target_quantity=quantity, target_notional=quantity * 100.0, allocation_fraction=0.1,
        requested_capital=quantity * 100.0, created_at=TIME, source_event_timestamp=TIME,
        target_position_before=quantity if action in {"reduce", "close"} else 0.0,
    )


class PhaseDChaosRecoveryTests(unittest.TestCase):
    def engine(self, directory: Path, *plans: SimulatorPlan) -> tuple[CopyTradeDatabase, ExecutionEngine, DeterministicExecutionSimulator]:
        database = CopyTradeDatabase(directory / "copy.sqlite3")
        database.initialize()
        adapter = DeterministicExecutionSimulator(plans)
        return database, ExecutionEngine(database, adapter, safety_context=ExecutionSafetyContext()), adapter

    def test_crash_checkpoints_preserve_resume_and_exactly_once_fills(self) -> None:
        cases = (
            ("after_intent_persistence", ExecutionState.CREATED, True),
            ("after_external_submit", ExecutionState.SUBMISSION_UNKNOWN, False),
            ("before_local_fill_persistence", ExecutionState.SUBMITTING, True),
            ("after_local_fill_persistence", ExecutionState.SUBMITTING, True),
        )
        for checkpoint, expected, raises in cases:
            with self.subTest(checkpoint=checkpoint), tempfile.TemporaryDirectory() as temp:
                database, engine, adapter = self.engine(Path(temp), SimulatorPlan("immediate_fill"))
                fault = DeterministicFaultInjector([checkpoint])
                item = signal(f"crash-{checkpoint}")
                if raises:
                    with self.assertRaises(InjectedExecutionFault):
                        engine.process_signal(item, fault_hook=fault)
                    intent = database.get_execution_intent_for_signal(item.signal_id)
                else:
                    intent = engine.process_signal(item, fault_hook=fault)
                self.assertIsNotNone(intent)
                self.assertEqual(intent.state, expected)  # type: ignore[union-attr]
                restored = ExecutionEngine(database, adapter, safety_context=ExecutionSafetyContext()).resume_intent(intent.intent_id)  # type: ignore[union-attr]
                self.assertEqual(restored.state, ExecutionState.FILLED)
                self.assertEqual(adapter.submit_calls, 1)
                self.assertEqual(len(database.list_execution_fills(restored.intent_id)), 1)
                self.assertEqual(database.phase_d_local_positions(), {"BTC": 1.0})
                self.assertIn(checkpoint, fault.observed)

    def test_temporary_unavailability_stale_reads_and_interrupted_reconciliation_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("immediate_fill"))
            engine.process_signal(signal("availability-open"))
            adapter.set_temporary_unavailable()
            unavailable = engine.reconcile_positions()
            self.assertEqual(unavailable["state"], "INCOMPLETE")
            self.assertEqual(database.execution_read_model()["execution_health"]["state"], "RECONCILIATION_INCOMPLETE")
            blocked = engine.process_signal(signal("availability-entry"))
            self.assertEqual(blocked.state, ExecutionState.BLOCKED)
            adapter.set_temporary_unavailable(False)
            adapter.set_stale_positions([])
            stale = engine.reconcile_positions()
            self.assertEqual(stale["state"], "INCOMPLETE")
            adapter.set_stale_positions(None)
            interrupted = engine.reconcile_positions(fault_hook=DeterministicFaultInjector(["after_position_observation"]))
            self.assertEqual(interrupted["state"], "INCOMPLETE")
            self.assertEqual(database.latest_execution_reconciliation()["state"], "INCOMPLETE")
            self.assertTrue(database.execution_has_unresolved_entry_risk())

    def test_external_manual_activity_is_unknown_and_direction_mismatch_rejects_unsafe_reduce(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("immediate_fill"))
            adapter.inject_external_position("BTC", 0.5)
            manual = engine.reconcile_positions()
            self.assertEqual(manual["state"], "MISMATCH")
            self.assertEqual(database.execution_read_model()["position_mismatches"][0]["state"], "UNKNOWN_POSITION")
            self.assertEqual(engine.process_signal(signal("manual-entry")).state, ExecutionState.BLOCKED)

        with tempfile.TemporaryDirectory() as temp:
            _, engine, adapter = self.engine(Path(temp), SimulatorPlan("immediate_fill"))
            engine.process_signal(signal("direction-open"))
            adapter.set_reported_positions({"BTC": -1.0})
            self.assertEqual(engine.reconcile_positions()["state"], "MISMATCH")
            unsafe = engine.process_signal(
                signal("direction-close", action="close"),
                context=ExecutionSafetyContext(
                    verified_positions={"BTC": -1.0}, verified_positions_current=True,
                    verified_positions_authoritative=True,
                ),
            )
            self.assertEqual(unsafe.state, ExecutionState.BLOCKED)
            self.assertEqual(engine.store.latest_execution_risk_decision(unsafe.intent_id)["reason"], "reduce_only_direction_mismatch")

    def test_verified_flat_requires_fresh_positions_no_open_orders_and_no_unknown_submission(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, engine, adapter = self.engine(Path(temp))
            self.assertEqual(engine.verify_flat()["state"], "VERIFIED_FLAT")
            adapter.set_stale_positions([])
            stale = engine.verify_flat()
            self.assertEqual(stale["state"], "INCOMPLETE")
            self.assertIn("stale", stale["reason"])
            adapter.set_stale_positions(None)
            adapter.inject_external_order("BTC", 1.0)
            rejected = engine.verify_flat()
            self.assertEqual(rejected["state"], "INCOMPLETE")
            self.assertIn("open_order_present", rejected["reason"])

        with tempfile.TemporaryDirectory() as temp:
            _, engine, _ = self.engine(Path(temp), SimulatorPlan("timeout_before_accept"))
            unknown = engine.process_signal(signal("flat-unknown"))
            self.assertEqual(unknown.state, ExecutionState.SUBMISSION_UNKNOWN)
            rejected = engine.verify_flat()
            self.assertEqual(rejected["state"], "INCOMPLETE")
            self.assertIn("unresolved_submission_present", rejected["reason"])

    def test_open_order_authority_remains_latched_after_positions_only_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp))
            adapter.inject_external_order("BTC", 1.0)
            failed_flat = engine.verify_flat()
            self.assertEqual(failed_flat["state"], "INCOMPLETE")
            self.assertIn("open_order_present", failed_flat["reason"])
            self.assertTrue(database.execution_open_order_reconciliation_unhealthy())

            # This is fresh, authoritative position evidence, but it says
            # nothing about an outstanding venue order and must not clear it.
            self.assertEqual(engine.reconcile_positions()["state"], "VERIFIED_FLAT")
            self.assertTrue(database.execution_open_order_reconciliation_unhealthy())
            health = database.execution_read_model()["execution_health"]
            self.assertEqual(health["state"], "OPEN_ORDER_RECONCILIATION_INCOMPLETE")
            self.assertTrue(health["safety"]["unhealthy"])
            self.assertEqual(engine.process_signal(signal("open-order-latched")).state, ExecutionState.BLOCKED)

            # Only a later open-order authority observation may clear the
            # latch.  Position reconciliation is intentionally irrelevant.
            adapter.clear_external_orders()
            self.assertEqual(engine.reconcile_open_orders()["state"], "MATCHED")
            self.assertFalse(database.execution_open_order_reconciliation_unhealthy())
            self.assertTrue(database.execution_safety_health()["healthy"])

    def test_cancellation_and_reconciliation_crashes_remain_ambiguous_until_repaired(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("partial", fill_quantities=(0.25,)))
            partial = engine.process_signal(signal("cancel-crash"))
            cancelled = engine.request_cancel(
                partial.intent_id, fault_hook=DeterministicFaultInjector(["after_cancel_acceptance"]),
            )
            self.assertEqual(cancelled.state, ExecutionState.RECONCILIATION_REQUIRED)
            repaired = ExecutionEngine(database, adapter, safety_context=ExecutionSafetyContext()).resume_intent(partial.intent_id)
            self.assertEqual(repaired.state, ExecutionState.CANCELLED)
            self.assertAlmostEqual(sum(float(row["quantity"]) for row in database.list_execution_fills(partial.intent_id)), 0.25)

        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("accepted_timeout"))
            unknown = engine.process_signal(signal("reconcile-crash"))
            with self.assertRaises(InjectedExecutionFault):
                engine.reconcile_intent(unknown.intent_id, fault_hook=DeterministicFaultInjector(["before_reconciliation"]))
            self.assertEqual(database.latest_execution_reconciliation()["state"], "RECONCILING")
            repaired = ExecutionEngine(database, adapter, safety_context=ExecutionSafetyContext()).resume_intent(unknown.intent_id)
            self.assertEqual(repaired.state, ExecutionState.ACKNOWLEDGED)

    def test_global_venue_fill_identity_cannot_be_applied_to_two_submissions(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("acknowledged"), SimulatorPlan("acknowledged"))
            first = engine.process_signal(signal("shared-fill-one"))
            first_submission = database.get_execution_submission(first.intent_id)
            self.assertIsNotNone(first_submission)
            adapter.emit_fill(first_submission.client_order_id, 1.0, venue_fill_id="globally-unique-fill")  # type: ignore[union-attr]
            self.assertEqual(engine.reconcile_intent(first.intent_id).state, ExecutionState.FILLED)
            second = engine.process_signal(signal("shared-fill-two"))
            second_submission = database.get_execution_submission(second.intent_id)
            self.assertIsNotNone(second_submission)
            adapter.emit_fill(second_submission.client_order_id, 1.0, venue_fill_id="globally-unique-fill")  # type: ignore[union-attr]
            self.assertEqual(engine.reconcile_intent(second.intent_id).state, ExecutionState.RECONCILIATION_REQUIRED)
            self.assertEqual(len(database.list_execution_fills()), 1)

    def test_concurrent_unknown_resume_and_generated_out_of_order_fills_converge(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.engine(Path(temp), SimulatorPlan("accepted_timeout"))
            unknown = engine.process_signal(signal("concurrent-resume"))
            self.assertEqual(unknown.state, ExecutionState.SUBMISSION_UNKNOWN)
            with ThreadPoolExecutor(max_workers=2) as workers:
                results = list(workers.map(
                    lambda _: ExecutionEngine(database, adapter, safety_context=ExecutionSafetyContext()).resume_intent(unknown.intent_id), range(2),
                ))
            self.assertEqual({result.state for result in results}, {ExecutionState.ACKNOWLEDGED})
            self.assertEqual(adapter.submit_calls, 1)
            self.assertEqual(len(database.list_execution_intents()), 1)

        generator = random.Random(7331)
        for case in range(5):
            with self.subTest(generated_case=case), tempfile.TemporaryDirectory() as temp:
                database, engine, adapter = self.engine(Path(temp), SimulatorPlan("acknowledged"))
                intent = engine.process_signal(signal(f"generated-{case}"))
                submission = database.get_execution_submission(intent.intent_id)
                self.assertIsNotNone(submission)
                cuts = sorted((generator.random(), generator.random()))
                pieces = [cuts[0], cuts[1] - cuts[0], 1.0 - cuts[1]]
                order = list(range(3))
                generator.shuffle(order)
                for received, index in enumerate(order):
                    adapter.emit_fill(
                        submission.client_order_id, pieces[index], venue_fill_id=f"generated-{case}-{index}",
                        timestamp=TIME + timedelta(milliseconds=30 - received),
                    )
                adapter.emit_fill(
                    submission.client_order_id, pieces[order[-1]], venue_fill_id=f"generated-{case}-{order[-1]}",
                    timestamp=TIME,
                )
                final = engine.reconcile_intent(intent.intent_id)
                replayed = engine.reconcile_intent(intent.intent_id)
                self.assertEqual((final.state, replayed.state), (ExecutionState.FILLED, ExecutionState.FILLED))
                fills = database.list_execution_fills(intent.intent_id)
                self.assertEqual(len(fills), 3)
                self.assertAlmostEqual(sum(float(row["quantity"]) for row in fills), 1.0)
                self.assertEqual(database.phase_d_local_positions(), {"BTC": 1.0})

    def test_paper_projection_and_economics_roll_back_as_one_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = CopyTradeDatabase(root / "copy.sqlite3")
            database.initialize()
            config = CopyTradeConfig(
                artifacts=ArtifactConfig(database_path=root / "copy.sqlite3", obsidian_root=root / "obsidian"),
                paper_execution=PaperExecutionConfig(min_order_notional=1),
                risk=RiskConfig(max_signal_age_seconds=10**12, kill_switch_path=root / "kill.txt"),
            )
            item = signal("paper-atomic")
            with self.assertRaises(InjectedExecutionFault):
                PaperExecutionEngine(config, database).process_signal(
                    item, fault_hook=DeterministicFaultInjector(["after_phase_d_projection"]),
                )
            self.assertIsNone(database.get_execution_attempt(item.signal_id))
            self.assertIsNone(database.get_execution_intent_for_signal(item.signal_id))
            self.assertEqual(database.list_virtual_positions(), [])
            replay = PaperExecutionEngine(config, database).process_signal(item)
            self.assertEqual(replay.status, "filled")
            self.assertEqual(len(database.list_execution_intents()), 1)
            self.assertEqual(len(database.list_execution_fills()), 1)
            self.assertEqual(len(database.list_virtual_positions()), 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
