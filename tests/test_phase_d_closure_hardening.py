from __future__ import annotations

import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from src.copytrade.config import ArtifactConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig
from src.copytrade.execution import DeterministicExecutionSimulator, ExecutionEngine, SimulatorPlan
from src.copytrade.execution_contracts import ExecutionSafetyContext, ExecutionState, VenueFill, VenueOrderStatus
from src.copytrade.models import CopySignal, as_utc, stable_id
from src.copytrade.paper import PaperExecutionEngine
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x1111111111111111111111111111111111111111"
TIME = as_utc("2026-01-01T00:00:00+00:00")
SIMULATION_SCOPE = {"execution_domain": "SIMULATOR", "execution_account_id": "SIMULATOR:default"}
PAPER_SCOPE = {"execution_domain": "PAPER_COMPAT", "execution_account_id": "PAPER_COMPAT:legacy_paper"}


def signal(
    name: str, *, action: str = "open", direction: str = "long", quantity: float = 1.0,
    target_position_before: float | None = None,
) -> CopySignal:
    return CopySignal(
        signal_id=stable_id("phase_d_closure_signal", name), target_wallet=WALLET, campaign_id="closure",
        source_event_id=stable_id("phase_d_closure_source", name), symbol="BTC", action=action, direction=direction,
        target_price=100.0, target_quantity=quantity, target_notional=quantity * 100.0, allocation_fraction=0.1,
        requested_capital=quantity * 100.0, created_at=TIME, source_event_timestamp=TIME,
        target_position_before=target_position_before if target_position_before is not None else (
            quantity if action in {"reduce", "close"} else 0.0
        ),
    )


def paper_config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "copy.sqlite3", obsidian_root=root / "obsidian"),
        paper_execution=PaperExecutionConfig(min_order_notional=1),
        risk=RiskConfig(max_signal_age_seconds=10**12, kill_switch_path=root / "kill.txt"),
    )


class PhaseDClosureHardeningTests(unittest.TestCase):
    def setup_engine(
        self, root: Path, *plans: SimulatorPlan,
    ) -> tuple[CopyTradeDatabase, ExecutionEngine, DeterministicExecutionSimulator]:
        database = CopyTradeDatabase(root / "copy.sqlite3")
        database.initialize()
        adapter = DeterministicExecutionSimulator(plans)
        return database, ExecutionEngine(database, adapter, safety_context=ExecutionSafetyContext()), adapter

    def test_degraded_exit_requires_current_authoritative_position_but_preserves_bounds_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, engine, _ = self.setup_engine(Path(temp))
            missing = engine.process_signal(
                signal("degraded-missing", action="close"),
                context=ExecutionSafetyContext(reconciliation_healthy=False),
            )
            self.assertEqual(missing.state, ExecutionState.BLOCKED)
            self.assertEqual(
                engine.store.latest_execution_risk_decision(missing.intent_id)["reason"],
                "reduce_only_verified_position_required",
            )
            wrong_direction = engine.process_signal(
                signal("degraded-direction", action="close"),
                context=ExecutionSafetyContext(
                    reconciliation_healthy=False, verified_positions={"BTC": -1.0},
                    verified_positions_current=True, verified_positions_authoritative=True,
                ),
            )
            self.assertEqual(
                engine.store.latest_execution_risk_decision(wrong_direction.intent_id)["reason"],
                "reduce_only_direction_mismatch",
            )
            oversized = engine.process_signal(
                signal("degraded-size", action="close", quantity=2.0),
                context=ExecutionSafetyContext(
                    reconciliation_healthy=False, verified_positions={"BTC": 1.0},
                    verified_positions_current=True, verified_positions_authoritative=True,
                ),
            )
            self.assertEqual(
                engine.store.latest_execution_risk_decision(oversized.intent_id)["reason"],
                "reduce_only_size_exceeds_position",
            )

    def test_account_reconciliation_health_is_scoped_latched_and_not_cleared_by_order_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.setup_engine(Path(temp), SimulatorPlan("immediate_fill"))
            filled = engine.process_signal(signal("health-open"))
            self.assertEqual(filled.state, ExecutionState.FILLED)
            adapter.set_temporary_unavailable()
            self.assertEqual(engine.reconcile_positions()["state"], "INCOMPLETE")
            self.assertTrue(database.execution_account_reconciliation_unhealthy(**SIMULATION_SCOPE))
            adapter.set_temporary_unavailable(False)
            self.assertEqual(engine.reconcile_intent(filled.intent_id).state, ExecutionState.FILLED)
            self.assertTrue(database.execution_account_reconciliation_unhealthy(**SIMULATION_SCOPE))
            self.assertEqual(database.execution_read_model()["execution_health"]["state"], "RECONCILIATION_INCOMPLETE")
            blocked = engine.process_signal(signal("health-new-entry"))
            self.assertEqual(blocked.state, ExecutionState.BLOCKED)
            self.assertEqual(engine.reconcile_positions()["state"], "MATCHED")
            self.assertFalse(database.execution_account_reconciliation_unhealthy(**SIMULATION_SCOPE))

    def test_paper_compatibility_and_simulator_exposure_are_domain_isolated(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = CopyTradeDatabase(root / "copy.sqlite3")
            database.initialize()
            paper = PaperExecutionEngine(paper_config(root), database)
            self.assertEqual(paper.process_signal(signal("paper-domain")).status, "filled")
            paper_positions_before = database.phase_d_local_positions(**PAPER_SCOPE)
            self.assertIn("BTC", paper_positions_before)
            adapter = DeterministicExecutionSimulator([SimulatorPlan("immediate_fill")])
            simulator = ExecutionEngine(database, adapter, safety_context=ExecutionSafetyContext())
            simulated = simulator.process_signal(signal("sim-domain"))
            self.assertEqual(simulated.state, ExecutionState.FILLED)
            self.assertEqual(database.phase_d_local_positions(**SIMULATION_SCOPE), {"BTC": 1.0})
            self.assertEqual(database.phase_d_local_positions(**PAPER_SCOPE), paper_positions_before)
            paper_intent = database.get_execution_intent_for_signal(signal("paper-domain").signal_id)
            self.assertEqual((paper_intent.execution_domain, paper_intent.execution_account_id), ("PAPER_COMPAT", "PAPER_COMPAT:legacy_paper"))  # type: ignore[union-attr]
            read_model = database.execution_read_model()
            self.assertEqual(len(read_model["recent_intents"]), 1)
            self.assertEqual(read_model["paper_compatibility_audit"]["intent_count"], 1)
            self.assertEqual(read_model["paper_compatibility_audit"]["fill_count"], 1)

    def test_actual_fill_side_is_accounted_and_side_conflict_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.setup_engine(Path(temp), SimulatorPlan("acknowledged"))
            intent = engine.process_signal(signal("side-conflict"))
            submission = database.get_execution_submission(intent.intent_id)
            self.assertIsNotNone(submission)
            adapter.emit_fill(submission.client_order_id, 1.0, venue_fill_id="wrong-side", side="SELL")  # type: ignore[union-attr]
            reconciled = engine.reconcile_intent(intent.intent_id)
            self.assertEqual(reconciled.state, ExecutionState.RECONCILIATION_REQUIRED)
            self.assertEqual(database.phase_d_local_positions(**SIMULATION_SCOPE), {"BTC": -1.0})
            database.initialize()
            self.assertEqual(database.phase_d_local_positions(**SIMULATION_SCOPE), {"BTC": -1.0})
            self.assertIn(
                "FILL_SIDE_CONFLICT",
                {issue["category"] for issue in database.list_execution_integrity_issues(intent_id=intent.intent_id)},
            )
            self.assertTrue(database.execution_safety_health(**SIMULATION_SCOPE)["integrity_unhealthy"])
            self.assertEqual(database.execution_read_model()["execution_health"]["state"], "INTEGRITY_FAILURE")
            self.assertTrue(database.execution_has_unresolved_entry_risk(**SIMULATION_SCOPE))

    def test_conflicting_duplicate_fill_payload_is_retained_once_and_escalated(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, _ = self.setup_engine(Path(temp), SimulatorPlan("acknowledged"))
            intent = engine.process_signal(signal("duplicate-payload"))
            submission = database.get_execution_submission(intent.intent_id)
            self.assertIsNotNone(submission)
            first = VenueFill("duplicate-payload", submission.client_order_id, 0.4, 100.0, 0.0, TIME)  # type: ignore[union-attr]
            conflict = VenueFill("duplicate-payload", submission.client_order_id, 0.5, 101.0, 0.0, TIME)  # type: ignore[union-attr]
            self.assertTrue(database.record_execution_venue_fill(intent.intent_id, submission.submission_id, first))  # type: ignore[union-attr]
            self.assertFalse(database.record_execution_venue_fill(intent.intent_id, submission.submission_id, conflict))  # type: ignore[union-attr]
            self.assertEqual(len(database.list_execution_fills(intent.intent_id)), 1)
            self.assertIn(
                "CONFLICTING_VENUE_FILL_ID",
                {issue["category"] for issue in database.list_execution_integrity_issues(intent_id=intent.intent_id)},
            )
            self.assertTrue(database.execution_safety_health(**SIMULATION_SCOPE)["integrity_unhealthy"])
            self.assertEqual(database.execution_read_model()["execution_health"]["state"], "INTEGRITY_FAILURE")
            self.assertEqual(engine.reconcile_intent(intent.intent_id).state, ExecutionState.RECONCILIATION_REQUIRED)

    def test_integrity_failure_degrades_combined_safety_and_preserves_only_verified_exits(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.setup_engine(
                Path(temp), SimulatorPlan("acknowledged"), SimulatorPlan("immediate_fill"),
            )
            ready_before_failure = engine.accept_signal(signal("overfill-ready-before-failure"))
            self.assertEqual(
                engine.validate_intent(ready_before_failure.intent_id, context=ExecutionSafetyContext()).state,
                ExecutionState.READY,
            )
            intent = engine.process_signal(signal("overfill"))
            submission = database.get_execution_submission(intent.intent_id)
            self.assertIsNotNone(submission)
            adapter.emit_fill(submission.client_order_id, 0.6, venue_fill_id="overfill-a")  # type: ignore[union-attr]
            adapter.emit_fill(submission.client_order_id, 0.6, venue_fill_id="overfill-b")  # type: ignore[union-attr]
            self.assertEqual(engine.reconcile_intent(intent.intent_id).state, ExecutionState.RECONCILIATION_REQUIRED)
            self.assertAlmostEqual(database.phase_d_local_positions(**SIMULATION_SCOPE)["BTC"], 1.2)
            self.assertIn(
                "OVERFILL_DETECTED",
                {issue["category"] for issue in database.list_execution_integrity_issues(intent_id=intent.intent_id)},
            )
            safety = database.execution_safety_health(**SIMULATION_SCOPE)
            self.assertTrue(safety["integrity_unhealthy"])
            self.assertTrue(safety["unhealthy"])
            health = database.execution_read_model()["execution_health"]
            self.assertEqual(health["state"], "INTEGRITY_FAILURE")
            self.assertNotEqual(health["state"], "CONTINUOUS")
            self.assertEqual(
                engine.submit_ready_intent(ready_before_failure.intent_id, context=ExecutionSafetyContext()).state,
                ExecutionState.BLOCKED,
            )
            self.assertEqual(engine.process_signal(signal("overfill-open")).state, ExecutionState.BLOCKED)
            self.assertEqual(engine.process_signal(signal("overfill-add", action="add")).state, ExecutionState.BLOCKED)

            missing_evidence = engine.process_signal(signal("overfill-close-missing", action="close"))
            self.assertEqual(missing_evidence.state, ExecutionState.BLOCKED)
            self.assertEqual(
                database.latest_execution_risk_decision(missing_evidence.intent_id)["reason"],
                "reduce_only_verified_position_required",
            )
            bounded_exit = engine.process_signal(
                signal("overfill-close-verified", action="close", quantity=1.0),
                context=ExecutionSafetyContext(
                    verified_positions={"BTC": 1.2}, verified_positions_current=True,
                    verified_positions_authoritative=True,
                ),
            )
            self.assertEqual(bounded_exit.state, ExecutionState.FILLED)

    def test_submission_evidence_is_monotonic_and_conflicting_order_id_escalates(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, adapter = self.setup_engine(Path(temp), SimulatorPlan("immediate_fill"))
            intent = engine.process_signal(signal("monotonic"))
            submission = database.get_execution_submission(intent.intent_id)
            self.assertIsNotNone(submission)
            stale = database.update_execution_submission(
                intent.intent_id, state=VenueOrderStatus.ACKNOWLEDGED.value, venue_order_id=submission.venue_order_id,
                filled_quantity=0.1, raw_evidence={"stale": True}, updated_at=TIME - timedelta(seconds=1),
            )
            self.assertEqual(stale.state, VenueOrderStatus.FILLED.value)
            self.assertEqual(stale.filled_quantity, 1.0)
            conflict = database.update_execution_submission(
                intent.intent_id, state=VenueOrderStatus.FILLED.value, venue_order_id="conflicting-order-id",
                filled_quantity=1.0, raw_evidence={"conflicting": True}, updated_at=TIME + timedelta(seconds=1),
            )
            self.assertEqual(conflict.venue_order_id, submission.venue_order_id)
            self.assertIn(
                "CONFLICTING_VENUE_ORDER_ID",
                {issue["category"] for issue in database.list_execution_integrity_issues(intent_id=intent.intent_id)},
            )
            self.assertTrue(database.execution_safety_health(**SIMULATION_SCOPE)["integrity_unhealthy"])
            self.assertEqual(database.execution_read_model()["execution_health"]["state"], "INTEGRITY_FAILURE")
            self.assertEqual(engine.reconcile_intent(intent.intent_id).state, ExecutionState.RECONCILIATION_REQUIRED)

    def test_cancelled_submission_does_not_regress_to_stale_ack(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, engine, _ = self.setup_engine(Path(temp), SimulatorPlan("acknowledged"))
            intent = engine.process_signal(signal("cancel-monotonic"))
            cancelled = engine.request_cancel(intent.intent_id)
            self.assertEqual(cancelled.state, ExecutionState.CANCELLED)
            submission = database.get_execution_submission(intent.intent_id)
            stale = database.update_execution_submission(
                intent.intent_id, state=VenueOrderStatus.ACKNOWLEDGED.value, venue_order_id=submission.venue_order_id,  # type: ignore[union-attr]
                filled_quantity=0.0, raw_evidence={"stale": True}, updated_at=TIME - timedelta(seconds=1),
            )
            self.assertEqual(stale.state, VenueOrderStatus.CANCELLED.value)

    def test_restart_requires_explicit_admission_context_but_unknown_reconciles_without_one(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, configured, adapter = self.setup_engine(Path(temp), SimulatorPlan("immediate_fill"), SimulatorPlan("accepted_timeout"))
            created = configured.accept_signal(signal("restart-created"))
            self.assertEqual(ExecutionEngine(database, adapter).resume_intent(created.intent_id).state, ExecutionState.CREATED)
            ready = configured.validate_intent(created.intent_id, context=ExecutionSafetyContext())
            self.assertEqual(ready.state, ExecutionState.READY)
            self.assertEqual(ExecutionEngine(database, adapter).resume_intent(ready.intent_id).state, ExecutionState.READY)
            admitted = ExecutionEngine(database, adapter, safety_context=ExecutionSafetyContext()).resume_intent(ready.intent_id)
            self.assertEqual(admitted.state, ExecutionState.FILLED)
            unknown = configured.process_signal(signal("restart-unknown"))
            self.assertEqual(unknown.state, ExecutionState.SUBMISSION_UNKNOWN)
            repaired = ExecutionEngine(database, adapter).resume_intent(unknown.intent_id)
            self.assertEqual(repaired.state, ExecutionState.ACKNOWLEDGED)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
