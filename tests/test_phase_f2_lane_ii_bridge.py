from __future__ import annotations

import hashlib
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from src.copytrade.execution import (
    DeterministicExecutionSimulator,
    ExecutionEngine,
    SimulatorPlan,
)
from src.copytrade.execution_contracts import (
    ExecutionSafetyContext,
    ExecutionState,
    ExposureEffect,
    ReconciliationState,
)
from src.copytrade.storage import CopyTradeDatabase
from src.lane_ii.boundary import OperationalInput, OperationalInputSource, TradeDirection
from src.lane_ii.phase_d_bridge import (
    LANE_II_SIMULATOR_DOMAIN,
    ExecutionSizingEvidence,
    LaneIIAdmissionRefused,
    LaneIIPhaseDBridge,
    LaneIISizingRefused,
    VerifiedPositionTruth,
)
from src.lane_ii.trader_v0 import TraderV0, TraderV0DecisionInput, create_f1_trade_intent


TIME = "2026-08-18T00:00:00Z"
ACCOUNT = "LANE_II_SIMULATOR:f2-test-account"


class PhaseF2LaneIIBridgeTests(unittest.TestCase):
    def operational_input(self, source: OperationalInputSource, token: str) -> OperationalInput:
        return OperationalInput(
            input_id=f"{source.value.lower()}-{token}", source=source, observed_at=TIME,
            payload_hash=token * 64, source_system=f"{source.value.lower()}-feed",
        )

    def entry_inputs(self) -> tuple[OperationalInput, ...]:
        return (
            self.operational_input(OperationalInputSource.LIVE_PUBLIC_WALLET_ACTIVITY, "a"),
            self.operational_input(OperationalInputSource.LIVE_PUBLIC_MARKET_DATA, "b"),
            self.operational_input(OperationalInputSource.OPERATIONAL_INDICATOR, "c"),
            self.operational_input(OperationalInputSource.CONFIGURATION_OR_RISK_POLICY, "d"),
        )

    def decision_input(self, **changes: object) -> TraderV0DecisionInput:
        payload: dict[str, object] = {
            "operational_inputs": self.entry_inputs(), "now": TIME, "symbol": "BTC",
            "direction": TradeDirection.LONG, "source_action_at": TIME, "market_observed_at": TIME,
            "indicator_ids": ("wallet-flow", "microstructure"), "effective_confidence": 0.60,
            "expected_gross_edge": 0.020, "estimated_fees": 0.001, "estimated_spread": 0.001,
            "estimated_slippage": 0.001, "estimated_market_impact": 0.001,
            "estimated_latency_cost": 0.001, "alpha_survival": 1.0,
            "requested_notional_ceiling": 120.0, "market_regime": "normal",
        }
        payload.update(changes)
        return TraderV0DecisionInput(**payload)  # type: ignore[arg-type]

    def request(self, **changes: object):
        request = create_f1_trade_intent(TraderV0().decide(self.decision_input()))
        return replace(request, **changes) if changes else request

    def sizing(self, **changes: object) -> ExecutionSizingEvidence:
        payload: dict[str, object] = {
            "symbol": "BTC", "mark_price": 100.0, "price_observed_at": TIME,
            "metadata_observed_at": TIME, "quantity_decimals": 2,
            "minimum_quantity": 0.01, "source": "authoritative-simulator-market",
        }
        payload.update(changes)
        return ExecutionSizingEvidence(**payload)  # type: ignore[arg-type]

    def setup(
        self, root: Path, *plans: SimulatorPlan, phase_d_notional_limit: float = 50.0,
    ) -> tuple[CopyTradeDatabase, LaneIIPhaseDBridge, ExecutionEngine, DeterministicExecutionSimulator]:
        database = CopyTradeDatabase(root / "copytrade.sqlite3")
        database.initialize()
        adapter = DeterministicExecutionSimulator(plans)
        bridge = LaneIIPhaseDBridge(
            database, execution_account_id=ACCOUNT, phase_d_notional_limit=phase_d_notional_limit,
            clock=lambda: TIME,
        )
        engine = ExecutionEngine(
            database, adapter, execution_domain=LANE_II_SIMULATOR_DOMAIN, execution_account_id=ACCOUNT,
        )
        return database, bridge, engine, adapter

    @staticmethod
    def healthy(**changes: object) -> ExecutionSafetyContext:
        payload: dict[str, object] = {
            "source_recovery_continuous": True, "market_evidence_current": True,
            "reconciliation_healthy": True,
        }
        payload.update(changes)
        return ExecutionSafetyContext(**payload)  # type: ignore[arg-type]

    def test_exact_f1_entry_reaches_fill_ledger_and_reconciliation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, bridge, engine, adapter = self.setup(Path(temp))
            intent = bridge.admit_entry(self.request(), sizing=self.sizing())
            self.assertEqual(intent.execution_domain, LANE_II_SIMULATOR_DOMAIN)
            self.assertEqual(intent.execution_account_id, ACCOUNT)
            self.assertEqual(intent.exposure_effect, ExposureEffect.INCREASE)
            self.assertEqual(intent.requested_capital, 50.0)
            self.assertEqual(intent.requested_quantity, 0.5)
            self.assertEqual(intent.provenance["lane_ii"]["source"], "LANE_II")
            self.assertFalse(intent.provenance["lane_ii"]["lane_ii_execution_authority"])
            result = engine.resume_intent(intent.intent_id, context=self.healthy())
            self.assertEqual(result.state, ExecutionState.FILLED)
            self.assertEqual(adapter.submit_calls, 1)
            self.assertEqual(len(database.list_execution_fills(intent.intent_id)), 1)
            reconciled = engine.reconcile_positions()
            self.assertEqual(reconciled["state"], ReconciliationState.MATCHED.value)
            self.assertEqual(reconciled["local_positions"], {"BTC": 0.5})

    def test_unknown_changed_expired_corrupt_and_foreign_intents_refuse(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, bridge, _, _ = self.setup(Path(temp))
            cases = {
                "version": self.request(strategy_version="2"),
                "strategy": self.request(strategy_identity="trader-strategy-" + "f" * 32),
                "expired": self.request(
                    created_at="2026-08-17T23:59:00Z", expires_at="2026-08-17T23:59:30Z",
                ),
                "authority": self.request(authority_decision_hash="f" * 64),
                "provenance": self.request(input_provenance_hashes=("f" * 64,)),
                "policy": self.request(risk_policy_ref="foreign-policy"),
            }
            for name, request in cases.items():
                with self.subTest(name=name), self.assertRaises(LaneIIAdmissionRefused):
                    bridge.admit_entry(request, sizing=self.sizing())
            with self.assertRaises(LaneIIAdmissionRefused):
                bridge.admit_entry({"intent": self.request()}, sizing=self.sizing())

    def test_quantity_evidence_is_fresh_conservative_and_cannot_exceed_ceiling(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, bridge, _, _ = self.setup(Path(temp), phase_d_notional_limit=50.0)
            intent = bridge.admit_entry(self.request(), sizing=self.sizing(mark_price=33.0, quantity_decimals=2))
            self.assertEqual(intent.requested_quantity, 1.51)
            self.assertLessEqual(intent.requested_quantity * 33.0, 50.0)
            with self.assertRaisesRegex(LaneIISizingRefused, "market_price_stale"):
                bridge.admit_entry(
                    self.request(direction=TradeDirection.SHORT),
                    sizing=self.sizing(price_observed_at="2026-08-17T23:59:00Z"),
                )
            with self.assertRaisesRegex(LaneIISizingRefused, "instrument_metadata_stale"):
                bridge.admit_entry(
                    self.request(direction=TradeDirection.SHORT),
                    sizing=self.sizing(metadata_observed_at="2026-08-17T23:59:00Z"),
                )
            with self.assertRaises(LaneIISizingRefused):
                bridge.admit_entry(
                    self.request(direction=TradeDirection.SHORT),
                    sizing=self.sizing(minimum_quantity=2.0),
                )

    def test_phase_d_can_refuse_and_lane_request_cannot_force_sizing(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, bridge, engine, adapter = self.setup(Path(temp), phase_d_notional_limit=10.0)
            intent = bridge.admit_entry(self.request(), sizing=self.sizing())
            self.assertEqual(intent.requested_quantity, 0.1)
            result = engine.resume_intent(
                intent.intent_id,
                context=self.healthy(entry_inhibited=True, entry_inhibit_reason="phase_d_policy_refusal"),
            )
            self.assertEqual(result.state, ExecutionState.BLOCKED)
            self.assertEqual(adapter.submit_calls, 0)

    def test_duplicate_delivery_and_restart_paths_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database, bridge, engine, adapter = self.setup(root, SimulatorPlan("accepted_timeout", fill_quantities=(0.5,)))
            first = bridge.admit_entry(self.request(), sizing=self.sizing())
            duplicate = bridge.admit_entry(self.request(), sizing=self.sizing())
            self.assertEqual(first.intent_id, duplicate.intent_id)
            # Restart after admission and before submission.
            restarted = ExecutionEngine(
                CopyTradeDatabase(root / "copytrade.sqlite3"), adapter,
                execution_domain=LANE_II_SIMULATOR_DOMAIN, execution_account_id=ACCOUNT,
            )
            unknown = restarted.resume_intent(first.intent_id, context=self.healthy())
            self.assertEqual(unknown.state, ExecutionState.SUBMISSION_UNKNOWN)
            # Restart after the ambiguous external submission: reconcile, never resubmit.
            restarted_again = ExecutionEngine(
                CopyTradeDatabase(root / "copytrade.sqlite3"), adapter,
                execution_domain=LANE_II_SIMULATOR_DOMAIN, execution_account_id=ACCOUNT,
            )
            filled = restarted_again.resume_intent(first.intent_id, context=self.healthy())
            self.assertEqual(filled.state, ExecutionState.FILLED)
            self.assertEqual(adapter.submit_calls, 1)
            self.assertEqual(len(database.list_execution_intents()), 1)
            self.assertEqual(len(database.list_execution_fills(first.intent_id)), 1)

    def test_partial_fill_duplicate_fill_and_replay_converge(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, bridge, engine, adapter = self.setup(Path(temp), SimulatorPlan("partial", fill_quantities=(0.2,)))
            intent = bridge.admit_entry(self.request(), sizing=self.sizing())
            partial = engine.resume_intent(intent.intent_id, context=self.healthy())
            self.assertEqual(partial.state, ExecutionState.PARTIALLY_FILLED)
            submission = database.get_execution_submission(intent.intent_id)
            assert submission is not None
            first_fill = adapter.list_fills(submission.client_order_id)[0]
            adapter.emit_fill(
                submission.client_order_id, 0.0 + first_fill.quantity,
                venue_fill_id=first_fill.venue_fill_id,
            )
            adapter.emit_fill(submission.client_order_id, 0.3, venue_fill_id="remaining-fill")
            filled = engine.reconcile_intent(intent.intent_id)
            self.assertEqual(filled.state, ExecutionState.FILLED)
            self.assertEqual(len(database.list_execution_fills(intent.intent_id)), 2)
            replayed = engine.reconcile_intent(intent.intent_id)
            self.assertEqual(replayed.state, ExecutionState.FILLED)
            self.assertEqual(len(database.list_execution_fills(intent.intent_id)), 2)

    def test_verified_exit_uses_phase_d_truth_is_reduce_only_and_flattens(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database, bridge, engine, adapter = self.setup(Path(temp))
            entry = bridge.admit_entry(self.request(), sizing=self.sizing())
            self.assertEqual(engine.resume_intent(entry.intent_id, context=self.healthy()).state, ExecutionState.FILLED)
            position_input = self.decision_input(
                operational_inputs=(self.operational_input(
                    OperationalInputSource.CURRENT_ACCOUNT_OR_EXECUTION_STATE, "e",
                ),),
                position_open=True,
                hard_risk_exit=True,
            )
            exit_decision = TraderV0().decide(position_input)
            truth = VerifiedPositionTruth(
                symbol="BTC", signed_quantity=0.5, observed_at=TIME,
                provenance_hash=hashlib.sha256(b"phase-d-position-truth").hexdigest(), authoritative=True,
            )
            exit_intent = bridge.admit_verified_flatten(exit_decision, position=truth, sizing=self.sizing())
            self.assertEqual(exit_intent.direction, "long")
            self.assertEqual(exit_intent.exposure_effect, ExposureEffect.FLATTEN)
            result = engine.resume_intent(
                exit_intent.intent_id,
                context=self.healthy(
                    verified_positions={"BTC": 0.5}, verified_positions_current=True,
                    verified_positions_authoritative=True,
                ),
            )
            self.assertEqual(result.state, ExecutionState.FILLED)
            submission = database.get_execution_submission(exit_intent.intent_id)
            assert submission is not None
            request = adapter._requests[submission.client_order_id]
            self.assertTrue(request.reduce_only)
            self.assertEqual(request.side, "SELL")
            verified = engine.verify_flat()
            self.assertEqual(verified["state"], ReconciliationState.VERIFIED_FLAT.value)

    def test_exit_cannot_invent_reverse_or_unverified_exposure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, bridge, _, _ = self.setup(Path(temp))
            decision = TraderV0().decide(self.decision_input(
                operational_inputs=(self.operational_input(
                    OperationalInputSource.CURRENT_ACCOUNT_OR_EXECUTION_STATE, "e",
                ),),
                position_open=True, hard_risk_exit=True,
            ))
            base = {
                "symbol": "BTC", "observed_at": TIME,
                "provenance_hash": hashlib.sha256(b"truth").hexdigest(),
            }
            with self.assertRaises(LaneIIAdmissionRefused):
                bridge.admit_verified_flatten(
                    decision, position=VerifiedPositionTruth(**base, signed_quantity=0.5, authoritative=False),
                    sizing=self.sizing(),
                )
            with self.assertRaises(LaneIISizingRefused):
                bridge.admit_verified_flatten(
                    decision, position=VerifiedPositionTruth(**base, signed_quantity=0.0, authoritative=True),
                    sizing=self.sizing(),
                )

    def test_manual_position_and_foreign_order_are_detected(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, bridge, engine, adapter = self.setup(Path(temp))
            intent = bridge.admit_entry(self.request(), sizing=self.sizing())
            engine.resume_intent(intent.intent_id, context=self.healthy())
            adapter.inject_external_position("BTC", 0.75)
            positions = engine.reconcile_positions()
            self.assertEqual(positions["state"], ReconciliationState.MISMATCH.value)
            adapter.inject_external_order("ETH", 1.0)
            orders = engine.reconcile_open_orders()
            self.assertEqual(orders["state"], ReconciliationState.INCOMPLETE.value)
            self.assertGreaterEqual(len(orders["active_orders"]), 1)

    def test_lane_scope_cannot_use_phase_c_signal_admission(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _, _, engine, _ = self.setup(Path(temp))
            with self.assertRaisesRegex(ValueError, "Phase-C signals cannot be admitted"):
                engine.accept_signal(object())  # type: ignore[arg-type]


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
