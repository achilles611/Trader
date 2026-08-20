from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from src.lane_iii.contracts import EvidenceFamily, OperatorCommand
from src.lane_iii.market_data import DataQuality, MNQContract
from src.lane_iii.simulated_execution import (
    COMMISSIONED_SIMULATION_CONFIG,
    DeterministicExecutionReplay,
    DeterministicMNQSimulator,
    SimulatedMarketState,
    SimulatedOrderKind,
    SimulatedOrderState,
    SimulatedPositionSide,
    SimulationAdmissionStatus,
    SimulationConfig,
    SimulationHealth,
    SimulationLatency,
    SimulationRecoveryRefused,
    SimulationRefused,
)
from src.lane_iii.simulation_persistence import SimulationStateStore
from src.lane_iii.trader_v0 import SignalDecision, SignalDecisionType, SignalReason
from src.lane_iii.trader_v0 import TraderV0
import tests.test_lane_iii_phase_d as l3d_fixtures


BASE = datetime(2026, 1, 2, tzinfo=timezone.utc)


def at(milliseconds: int) -> str:
    return (BASE + timedelta(milliseconds=milliseconds)).isoformat().replace("+00:00", "Z")


def signal(kind: SignalDecisionType, milliseconds: int, *, hypothesis_id: str = "l3c-h-phase-e-fixture") -> SignalDecision:
    return SignalDecision.create(
        decision=kind,
        hypothesis_id=None if kind is SignalDecisionType.NO_TRADE else hypothesis_id,
        related_hypothesis_id=None,
        created_at=at(milliseconds),
        expires_at=at(milliseconds + 5_000),
        relative_support_snapshot=None,
        family_summary=(),
        reason_code={
            SignalDecisionType.LONG: SignalReason.ENTRY_BULLISH_REVERSAL,
            SignalDecisionType.SHORT: SignalReason.ENTRY_BEARISH_CONTINUATION,
            SignalDecisionType.EXIT: SignalReason.CONFIDENCE_DECAY,
            SignalDecisionType.NO_TRADE: SignalReason.NO_ELIGIBLE_HYPOTHESIS,
        }[kind],
        l3c_snapshot_hash="a" * 64,
        data_quality_hash="b" * 64,
        source_state_hash="c" * 64,
    )


def market(
    event_id: str,
    milliseconds: int,
    *,
    bid: str = "20000.00",
    ask: str = "20000.25",
    bid_quantity: int = 1,
    ask_quantity: int = 1,
    quality: DataQuality = DataQuality.HEALTHY,
    contract: MNQContract = COMMISSIONED_SIMULATION_CONFIG.contract,
) -> SimulatedMarketState:
    if quality is not DataQuality.HEALTHY:
        return SimulatedMarketState(event_id, at(milliseconds), contract, None, None, None, None, quality)
    return SimulatedMarketState(
        event_id, at(milliseconds), contract, Decimal(bid), Decimal(ask), bid_quantity, ask_quantity, quality,
    )


class LaneIIIPhaseETests(unittest.TestCase):
    def simulator(self, config: SimulationConfig = COMMISSIONED_SIMULATION_CONFIG) -> DeterministicMNQSimulator:
        result = DeterministicMNQSimulator(config, run_id="phase-e-test")
        result.on_market(market("m0", 0, contract=config.contract))
        result.apply_operator_command(OperatorCommand.ARM, requested_at=at(0))
        result.apply_operator_command(OperatorCommand.RESUME_NEW_ENTRIES, requested_at=at(0))
        return result

    def fill_long(self, simulator: DeterministicMNQSimulator, *, event_id: str = "m1", milliseconds: int = 100) -> SignalDecision:
        decision = signal(SignalDecisionType.LONG, 0)
        self.assertIs(simulator.admit_signal(decision).status, SimulationAdmissionStatus.ADMITTED)
        simulator.on_market(market(event_id, milliseconds, contract=simulator.config.contract))
        self.assertIs(simulator.position.side, SimulatedPositionSide.LONG)
        return decision

    def test_clean_entry_then_thesis_exit_is_not_same_as_flat_confirmation(self) -> None:
        simulator = self.simulator()
        entry = self.fill_long(simulator)
        self.assertEqual(simulator.position.quantity, 1)
        exit_signal = signal(SignalDecisionType.EXIT, 200, hypothesis_id=entry.hypothesis_id or "l3c-h-phase-e-fixture")
        simulator.admit_signal(exit_signal)
        self.assertIs(simulator.position.side, SimulatedPositionSide.LONG)
        simulator.on_market(market("m2", 300, bid="20001.00", ask="20001.25"))
        self.assertIs(simulator.position.side, SimulatedPositionSide.FLAT)
        self.assertGreater(simulator.position.realized_pnl, Decimal("0"))
        event_types = [event.event_type.value for event in simulator.ledger]
        self.assertIn("STRATEGY_EXIT_REQUESTED", event_types)
        self.assertIn("ORDER_FILLED", event_types)

    def test_latency_fills_at_post_latency_book_not_signal_book(self) -> None:
        simulator = self.simulator()
        simulator.admit_signal(signal(SignalDecisionType.LONG, 0))
        simulator.on_market(market("m1", 100, bid="20001.00", ask="20001.25"))
        self.assertEqual(simulator.position.average_entry_price, Decimal("20001.50"))

    def test_partial_entry_retains_partial_position_and_exit_cancels_remaining(self) -> None:
        config = SimulationConfig(COMMISSIONED_SIMULATION_CONFIG.contract, configured_quantity=2, maximum_position_quantity=2)
        simulator = self.simulator(config)
        simulator.admit_signal(signal(SignalDecisionType.LONG, 0))
        simulator.on_market(market("m1", 100, ask_quantity=1, contract=config.contract))
        self.assertEqual(simulator.position.quantity, 1)
        entry = next(order for order in simulator.orders.values() if order.kind is SimulatedOrderKind.MARKET_ENTRY)
        self.assertIs(entry.state, SimulatedOrderState.PARTIALLY_FILLED)
        simulator.admit_signal(signal(SignalDecisionType.EXIT, 200))
        self.assertIs(simulator.position.side, SimulatedPositionSide.LONG)
        simulator.on_market(market("m2", 300, bid="19999.75", ask="20000.00", bid_quantity=1, ask_quantity=5, contract=config.contract))
        self.assertIs(simulator.position.side, SimulatedPositionSide.FLAT)
        self.assertEqual(simulator.orders[entry.order_id].filled_quantity, 1)
        self.assertIs(simulator.orders[entry.order_id].state, SimulatedOrderState.CANCELLED)

    def test_partial_exit_retains_protection_for_exact_remaining_exposure(self) -> None:
        config = SimulationConfig(COMMISSIONED_SIMULATION_CONFIG.contract, configured_quantity=2, maximum_position_quantity=2)
        simulator = self.simulator(config)
        simulator.admit_signal(signal(SignalDecisionType.LONG, 0))
        simulator.on_market(market("m1", 100, ask_quantity=2, contract=config.contract))
        simulator.admit_signal(signal(SignalDecisionType.EXIT, 200))
        simulator.on_market(market("m2", 300, bid_quantity=1, ask_quantity=2, contract=config.contract))
        self.assertEqual(simulator.position.quantity, 1)
        exit_order = next(value for value in simulator.orders.values() if value.kind is SimulatedOrderKind.MARKET_EXIT)
        self.assertIs(exit_order.state, SimulatedOrderState.PARTIALLY_FILLED)
        stop = next(value for value in simulator.orders.values() if value.kind is SimulatedOrderKind.PROTECTIVE_STOP)
        self.assertEqual(stop.remaining_quantity, 1)

    def test_cancel_request_is_distinct_from_cancel_confirmation(self) -> None:
        simulator = self.simulator()
        simulator.admit_signal(signal(SignalDecisionType.LONG, 0))
        order = next(value for value in simulator.orders.values() if value.kind is SimulatedOrderKind.MARKET_ENTRY)
        simulator.request_cancel(order.order_id, requested_at=at(10))
        self.assertIs(simulator.orders[order.order_id].state, SimulatedOrderState.CANCEL_REQUESTED)
        simulator.on_market(market("m1", 20))
        self.assertIs(simulator.orders[order.order_id].state, SimulatedOrderState.CANCEL_REQUESTED)
        simulator.on_market(market("m2", 35))
        self.assertIs(simulator.orders[order.order_id].state, SimulatedOrderState.CANCELLED)

    def test_protective_stop_gap_through_uses_obtainable_book_and_outranks_thesis(self) -> None:
        simulator = self.simulator()
        self.fill_long(simulator)
        stop = next(value for value in simulator.orders.values() if value.kind is SimulatedOrderKind.PROTECTIVE_STOP)
        self.assertEqual(stop.stop_price, Decimal("19995.50"))
        simulator.on_market(market("m2", 200, bid="19994.00", ask="19994.25"))
        self.assertIs(simulator.position.side, SimulatedPositionSide.FLAT)
        stop_fill = next(fill for fill in simulator.fills if fill.order_id == stop.order_id)
        self.assertEqual(stop_fill.price, Decimal("19993.75"))
        self.assertLess(stop_fill.price, stop.stop_price)

    def test_quality_gap_blocks_entry_and_does_not_fabricate_fill(self) -> None:
        simulator = self.simulator()
        simulator.on_market(market("gap", 10, quality=DataQuality.GAPPED))
        self.assertIs(simulator.health, SimulationHealth.UNRESOLVED)
        rejected = simulator.admit_signal(signal(SignalDecisionType.LONG, 20))
        self.assertIs(rejected.status, SimulationAdmissionStatus.REJECTED)
        self.assertEqual(rejected.reason, "MARKET_QUALITY_DEGRADED")
        self.assertIs(simulator.position.side, SimulatedPositionSide.FLAT)
        simulator.on_market(market("recovered", 30))
        self.assertIs(simulator.health, SimulationHealth.HEALTHY)

    def test_duplicate_signal_and_duplicate_market_event_are_idempotent(self) -> None:
        simulator = self.simulator()
        decision = signal(SignalDecisionType.LONG, 0)
        simulator.admit_signal(decision)
        duplicate = simulator.admit_signal(decision)
        self.assertIs(duplicate.status, SimulationAdmissionStatus.DUPLICATE)
        simulator.on_market(market("m1", 100))
        report = simulator.report()
        simulator.on_market(market("m1", 100))
        self.assertEqual(simulator.report(), report)
        self.assertEqual(len([value for value in simulator.orders.values() if value.kind is SimulatedOrderKind.MARKET_ENTRY]), 1)

    def test_expired_and_disarmed_signals_are_visible_rejections(self) -> None:
        simulator = DeterministicMNQSimulator(run_id="rejection")
        simulator.on_market(market("m0", 0))
        disarmed = simulator.admit_signal(signal(SignalDecisionType.LONG, 0))
        self.assertEqual(disarmed.reason, "SIMULATOR_DISARMED")
        simulator.apply_operator_command(OperatorCommand.ARM, requested_at=at(10))
        simulator.apply_operator_command(OperatorCommand.RESUME_NEW_ENTRIES, requested_at=at(10))
        expired = signal(SignalDecisionType.LONG, 20)
        late = simulator.admit_signal(expired, admitted_at=at(5_021))
        self.assertEqual(late.reason, "EXPIRED_SIGNAL")

    def test_malformed_and_wrong_artifact_decisions_are_visible_rejections(self) -> None:
        simulator = self.simulator()
        malformed = simulator.admit_signal(object(), admitted_at=at(1))
        self.assertIs(malformed.status, SimulationAdmissionStatus.REJECTED)
        self.assertEqual(malformed.reason, "MALFORMED_DECISION")
        forged = object.__new__(SignalDecision)
        valid = signal(SignalDecisionType.LONG, 2)
        for field, value in valid.__dict__.items():
            object.__setattr__(forged, field, value)
        object.__setattr__(forged, "strategy_artifact_hash", "f" * 64)
        rejected = simulator.admit_signal(forged)
        self.assertIs(rejected.status, SimulationAdmissionStatus.REJECTED)
        self.assertEqual(rejected.reason, "ARTIFACT_MISMATCH")

    def test_flatten_request_is_not_flat_until_market_can_fill_it(self) -> None:
        simulator = self.simulator()
        self.fill_long(simulator)
        simulator.apply_operator_command(OperatorCommand.FLATTEN, requested_at=at(200))
        self.assertIs(simulator.position.side, SimulatedPositionSide.LONG)
        simulator.on_market(market("m2", 300, bid="19999.00", ask="19999.25"))
        self.assertIs(simulator.position.side, SimulatedPositionSide.FLAT)
        self.assertTrue(simulator.operator_state.flatten_latched)
        with self.assertRaises(SimulationRefused):
            simulator.apply_operator_command(OperatorCommand.RESUME_NEW_ENTRIES, requested_at=at(301))

    def test_loss_limit_blocks_new_entries_but_exit_remains_admitted(self) -> None:
        config = SimulationConfig(COMMISSIONED_SIMULATION_CONFIG.contract, maximum_session_loss=Decimal("1.00"), protective_stop_ticks=2)
        simulator = self.simulator(config)
        self.fill_long(simulator)
        simulator.on_market(market("m2", 200, bid="19999.00", ask="19999.25", contract=config.contract))
        self.assertTrue(simulator.loss_ceiling_breached)
        blocked = simulator.admit_signal(signal(SignalDecisionType.LONG, 300, hypothesis_id="l3c-h-new"))
        self.assertEqual(blocked.reason, "LOSS_CEILING")
        exit_admission = simulator.admit_signal(signal(SignalDecisionType.EXIT, 300))
        self.assertIs(exit_admission.status, SimulationAdmissionStatus.ADMITTED)

    def test_restart_restores_working_and_partially_filled_state_exactly(self) -> None:
        config = SimulationConfig(COMMISSIONED_SIMULATION_CONFIG.contract, configured_quantity=2, maximum_position_quantity=2)
        simulator = self.simulator(config)
        simulator.admit_signal(signal(SignalDecisionType.LONG, 0))
        simulator.on_market(market("m1", 100, ask_quantity=1, contract=config.contract))
        before = simulator.report()
        with tempfile.TemporaryDirectory() as directory:
            store = SimulationStateStore(Path(directory) / "l3e.sqlite")
            store.initialize()
            store.checkpoint(simulator)
            recovered = store.recover(config, "phase-e-test")
        self.assertEqual(recovered.report(), before)
        self.assertIs(recovered.position.side, SimulatedPositionSide.LONG)
        self.assertEqual(recovered.position.quantity, 1)
        self.assertTrue(any(order.state is SimulatedOrderState.PARTIALLY_FILLED for order in recovered.orders.values()))

    def test_corrupted_persistence_fails_closed_without_flat_assumption(self) -> None:
        simulator = self.simulator()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "l3e.sqlite"
            store = SimulationStateStore(path)
            store.initialize()
            store.checkpoint(simulator)
            connection = sqlite3.connect(path)
            try:
                connection.execute("UPDATE l3e_simulation_snapshots SET state_hash='0' * 64")
                connection.commit()
            finally:
                connection.close()
            with self.assertRaises(SimulationRecoveryRefused):
                store.recover(COMMISSIONED_SIMULATION_CONFIG, "phase-e-test")

    def test_deterministic_replay_reproduces_ledger_and_state_hashes(self) -> None:
        events = (
            market("m0", 0),
            (OperatorCommand.ARM, at(0)),
            (OperatorCommand.RESUME_NEW_ENTRIES, at(0)),
            signal(SignalDecisionType.LONG, 0),
            market("m1", 100, bid="20001.00", ask="20001.25"),
            signal(SignalDecisionType.EXIT, 200),
            market("m2", 300, bid="20002.00", ask="20002.25"),
        )
        first = DeterministicExecutionReplay(DeterministicMNQSimulator(run_id="one")).replay(events)
        second = DeterministicExecutionReplay(DeterministicMNQSimulator(run_id="one")).replay(events)
        self.assertEqual(first, second)
        self.assertIs(first.final_position.side, SimulatedPositionSide.FLAT)

    def test_frozen_l3c_to_l3d_to_l3e_path_uses_real_trader_v0_decisions(self) -> None:
        """Primary bridge proof: no handcrafted directional command enters L3-E."""
        fixture = l3d_fixtures.LaneIIIPhaseDTests()
        trader = TraderV0()
        entry = trader.evaluate(fixture.snapshot(fixture.record()), fixture.quality())
        self.assertIs(entry.decision, SignalDecisionType.LONG)
        start = datetime.fromisoformat(entry.created_at.replace("Z", "+00:00"))
        simulator = DeterministicMNQSimulator(run_id="frozen-l3d-bridge")
        simulator.on_market(SimulatedMarketState("l3b-market-0", entry.created_at, simulator.config.contract, Decimal("20000"), Decimal("20000.25"), 1, 1, DataQuality.HEALTHY))
        simulator.apply_operator_command(OperatorCommand.ARM, requested_at=entry.created_at)
        simulator.apply_operator_command(OperatorCommand.RESUME_NEW_ENTRIES, requested_at=entry.created_at)
        simulator.admit_signal(entry)
        fill_time = (start + timedelta(milliseconds=100)).isoformat().replace("+00:00", "Z")
        simulator.on_market(SimulatedMarketState("l3b-market-1", fill_time, simulator.config.contract, Decimal("20000"), Decimal("20000.25"), 1, 1, DataQuality.HEALTHY))
        later = start + timedelta(seconds=1)
        # The real L3-D fixture's two retained families cause a thesis EXIT.
        decayed = fixture.record(
            at=later, created_at=start - timedelta(seconds=10), score="0.57",
            families=(EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceFamily.ORDER_FLOW), hypothesis_id=entry.hypothesis_id,
        )
        exit_signal = trader.evaluate(fixture.snapshot(decayed, at=later), fixture.quality(later))
        self.assertIs(exit_signal.decision, SignalDecisionType.EXIT)
        simulator.admit_signal(exit_signal)
        exit_time = (later + timedelta(milliseconds=100)).isoformat().replace("+00:00", "Z")
        simulator.on_market(SimulatedMarketState("l3b-market-2", exit_time, simulator.config.contract, Decimal("20001"), Decimal("20001.25"), 1, 1, DataQuality.HEALTHY))
        self.assertIs(simulator.position.side, SimulatedPositionSide.FLAT)

    def test_wrong_contract_is_refused_and_no_provider_or_broker_dependency_exists(self) -> None:
        simulator = self.simulator()
        wrong = MNQContract("MNQZ6", "2026-12")
        with self.assertRaises(SimulationRefused):
            simulator.on_market(market("wrong", 10, contract=wrong))
        source = (Path(__file__).parents[1] / "src" / "lane_iii" / "simulated_execution.py").read_text(encoding="utf-8")
        imports = []
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
            elif isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
        denied = ("requests", "websockets", "boto3", "hyperliquid", "rithmic", "tradovate", "src.copytrade", "src.phase_e")
        self.assertFalse([name for name in imports if name.lower().startswith(denied)])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
