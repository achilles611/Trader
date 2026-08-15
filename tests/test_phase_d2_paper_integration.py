from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from src.copytrade.config import ArtifactConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig
from src.copytrade.execution_contracts import ExecutionState
from src.copytrade.models import CopySignal, as_utc, stable_id
from src.copytrade.paper import PaperExecutionEngine
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x1111111111111111111111111111111111111111"
TIME = as_utc("2026-01-01T00:00:00+00:00")


def config(root: Path) -> CopyTradeConfig:
    return CopyTradeConfig(
        artifacts=ArtifactConfig(database_path=root / "copy.sqlite3", obsidian_root=root / "obsidian"),
        paper_execution=PaperExecutionConfig(fee_rate=.001, slippage_bps=5, min_order_notional=1),
        risk=RiskConfig(max_signal_age_seconds=10**12, kill_switch_path=root / "kill.txt"),
    )


def signal(name: str, *, action: str = "open", quantity: float = 1, before: float = 0) -> CopySignal:
    return CopySignal(
        stable_id("d2-signal", name), WALLET, "campaign", stable_id("d2-source", name), "BTC", action, "long",
        100, quantity, quantity * 100, .1 if action in {"open", "add"} else 0, 100 if action in {"open", "add"} else 0,
        TIME, TIME, target_position_before=before,
    )


class PhaseDPaperIntegrationTests(unittest.TestCase):
    def test_open_fee_slippage_and_close_remain_economically_identical(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            baseline = PaperExecutionEngine(config(root))
            database = CopyTradeDatabase(root / "copy.sqlite3")
            database.initialize()
            integrated = PaperExecutionEngine(config(root), database)
            open_signal = signal("open")
            self.assertEqual(baseline.process_signal(open_signal).status, integrated.process_signal(open_signal).status)
            close_signal = signal("close", action="close", quantity=1, before=1)
            self.assertEqual(baseline.process_signal(close_signal).status, integrated.process_signal(close_signal).status)
            base_sleeve = next(iter(baseline.portfolio.sleeves.values()))
            d_sleeve = next(iter(integrated.portfolio.sleeves.values()))
            self.assertEqual(base_sleeve.__dict__, d_sleeve.__dict__)
            self.assertEqual(baseline.portfolio.cash, integrated.portfolio.cash)
            intents = database.list_execution_intents()
            self.assertEqual([intent.state for intent in intents], [ExecutionState.FILLED, ExecutionState.FILLED])
            self.assertEqual(len(database.list_execution_fills()), 2)

    def test_blocked_paper_entry_has_durable_phase_d_reason_without_economic_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = CopyTradeDatabase(root / "copy.sqlite3")
            database.initialize()
            engine = PaperExecutionEngine(config(root), database)
            blocked = engine.process_signal(signal("blocked"), forced_reason="entry_control_paused")
            intent = database.get_execution_intent_for_signal(blocked.signal_id)
            self.assertEqual(blocked.status, "skipped")
            self.assertIsNotNone(intent)
            self.assertEqual(intent.state, ExecutionState.BLOCKED)  # type: ignore[union-attr]
            self.assertEqual(database.latest_execution_risk_decision(intent.intent_id)["reason"], "entry_control_paused")  # type: ignore[union-attr]
            self.assertEqual(database.list_virtual_positions(open_only=True), [])

    def test_restart_replays_legacy_attempt_into_phase_d_without_second_paper_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = CopyTradeDatabase(root / "copy.sqlite3")
            database.initialize()
            first = PaperExecutionEngine(config(root), database)
            item = signal("restart")
            first.process_signal(item)
            # Simulate loss of the additive D projection only. Historical paper
            # rows remain authoritative and replay must not create another sleeve.
            with database._connect() as connection:  # type: ignore[attr-defined]
                connection.execute("DELETE FROM phase_d_execution_fills")
                connection.execute("DELETE FROM phase_d_execution_submissions")
                connection.execute("DELETE FROM phase_d_execution_risk_decisions")
                connection.execute("DELETE FROM phase_d_execution_state_events")
                connection.execute("DELETE FROM phase_d_execution_intents")
            restarted = PaperExecutionEngine(config(root), database)
            replay = restarted.process_signal(item)
            self.assertEqual(replay.status, "filled")
            self.assertEqual(len(database.list_virtual_positions()), 1)
            self.assertEqual(len(database.list_execution_intents()), 1)
            self.assertEqual(database.list_execution_intents()[0].state, ExecutionState.FILLED)

    def test_existing_exit_is_projected_while_new_entry_is_blocked(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = CopyTradeDatabase(root / "copy.sqlite3")
            database.initialize()
            engine = PaperExecutionEngine(config(root), database)
            engine.process_signal(signal("open"))
            close = engine.process_signal(signal("close", action="close", quantity=1, before=1), forced_reason=None)
            self.assertEqual(close.status, "filled")
            self.assertEqual(database.get_execution_intent_for_signal(close.signal_id).state, ExecutionState.FILLED)  # type: ignore[union-attr]


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
