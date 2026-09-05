from __future__ import annotations

import asyncio
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

from src.copytrade.backtest import CopyTradeBacktester
from src.copytrade.config import ArtifactConfig, CopyTradeConfig, PaperExecutionConfig, RiskConfig, SizingConfig
from src.copytrade.hyperliquid import HyperliquidWatcher
from src.copytrade.models import CopySignal, PositionEventType, RawFill, Target, as_utc, stable_id
from src.copytrade.paper import PaperExecutionEngine, TargetSizeClassifier
from src.copytrade.reconstruction import PositionReconstructor, SourcePositionContinuityError, aggregate_partial_fills
from src.copytrade.reporting import ObsidianExporter
from src.copytrade.service import CopyTradeService
from src.copytrade.storage import CopyTradeDatabase


WALLET = "0x1111111111111111111111111111111111111111"


def fill(
    tid: int, time_ms: int, side: str, quantity: float, start_position: float | str, *, order: int | None = None,
    price: float = 100.0, equity: float = 1000.0,
) -> RawFill:
    return RawFill.from_hyperliquid(
        {
            "coin": "BTC", "px": str(price), "sz": str(quantity), "side": side,
            "dir": "Open Long" if side == "B" else "Close Long", "time": time_ms,
            "startPosition": str(start_position), "hash": f"0x{tid:064x}", "oid": order or tid,
            "tid": tid, "fee": "0.01", "accountValue": str(equity),
        },
        WALLET,
    )


def signal(
    event_id: str, *, action: str = "open", direction: str = "long", capital: float = 20.0,
    event_time: object | None = None, quantity: float = 1.0, before: float = 0.0,
) -> CopySignal:
    timestamp = as_utc(event_time or 1_700_000_000_000)
    return CopySignal(
        signal_id=stable_id("testsignal", event_id, action), target_wallet=WALLET, campaign_id="campaign",
        source_event_id=event_id, symbol="BTC", action=action, direction=direction, target_price=100.0,
        target_quantity=quantity, target_notional=quantity * 100, allocation_fraction=0.1,
        requested_capital=capital, created_at=timestamp, source_event_timestamp=timestamp,
        target_position_before=before,
    )


class CopyTradeTests(unittest.TestCase):
    def config(self, directory: Path, **changes: object) -> CopyTradeConfig:
        config = CopyTradeConfig(
            artifacts=ArtifactConfig(database_path=directory / "copy.sqlite3", obsidian_root=directory / "obsidian"),
            paper_execution=PaperExecutionConfig(fee_rate=0.0, slippage_bps=5.0, min_order_notional=1.0, random_seed=11),
            sizing=SizingConfig(min_history=2),
            risk=RiskConfig(max_signal_age_seconds=60_000_000_000, kill_switch_path=directory / "kill.txt"),
        )
        return replace(config, **changes)

    def test_deduplication_and_restart_reconstruction(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            database = CopyTradeDatabase(Path(temp) / "copy.sqlite3")
            database.initialize()
            item = fill(1, 1_700_000_000_000, "B", 1, 0)
            self.assertTrue(database.insert_raw_fill(item))
            self.assertFalse(database.insert_raw_fill(item))
            restarted = CopyTradeDatabase(Path(temp) / "copy.sqlite3")
            result = PositionReconstructor().reconstruct(restarted.list_raw_fills(WALLET))
            self.assertEqual(len(result.events), 1)
            self.assertEqual(result.events[0].event_type, PositionEventType.OPEN)

    def test_partial_fills_aggregate_to_one_position_event_and_campaign(self) -> None:
        fills = [
            fill(1, 1_700_000_000_000, "B", 0.4, 0, order=99),
            fill(2, 1_700_000_000_001, "B", 0.6, 0.4, order=99),
        ]
        self.assertEqual(len(aggregate_partial_fills(fills)), 1)
        result = PositionReconstructor().reconstruct(fills)
        self.assertEqual(len(result.events), 1)
        self.assertEqual(result.events[0].event_type, PositionEventType.OPEN)
        self.assertEqual(len(result.events[0].raw_fill_ids), 2)
        self.assertEqual(len(result.campaigns), 1)

    def test_same_timestamp_partials_use_source_position_boundary_not_event_id(self) -> None:
        # Event IDs sort in the opposite direction to the causal source
        # positions.  A close-short batch must still begin at -2, not -1.
        fills = [
            fill(9, 1_700_000_000_000, "B", 1, -1, order=99),
            fill(1, 1_700_000_000_000, "B", 1, -2, order=99),
        ]
        aggregate = aggregate_partial_fills(fills)[0]
        self.assertEqual(aggregate.position_before, -2)
        result = PositionReconstructor().reconstruct(fills)
        self.assertEqual(result.events[0].event_type, PositionEventType.CLOSE)

    def test_same_timestamp_source_chain_overrides_event_id_order(self) -> None:
        timestamp = 1_700_000_000_000
        # The input/unique IDs are deliberately not causal order.  The raw
        # source positions prove the only A/B/A progression 0 -> 1 -> 2 -> 3.
        fills = [
            fill(9, timestamp, "B", 1, 2, order=99),
            fill(1, timestamp, "B", 1, 0, order=99),
            fill(5, timestamp, "B", 1, 1, order=99),
        ]
        aggregate = aggregate_partial_fills(fills)[0]
        self.assertEqual([item.target_trade_id for item in aggregate.fills], ["1", "5", "9"])
        self.assertEqual((aggregate.position_before, aggregate.signed_quantity), (0, 3))

    def test_decimal_position_alias_is_normalized_ephemerally(self) -> None:
        timestamp = 1_700_000_000_000
        # The tail is an alternative binary64 spelling of the source-proven
        # 0.1 lattice endpoint.  Input raw evidence must remain untouched.
        fills = [
            fill(9, timestamp, "B", 0.1, "0.10000000000000001", order=99),
            fill(1, timestamp, "B", 0.1, "0.0", order=99),
        ]
        aggregate = aggregate_partial_fills(fills)[0]
        self.assertEqual([item.target_trade_id for item in aggregate.fills], ["1", "9"])
        self.assertEqual(aggregate.fills[1].raw_payload["startPosition"], "0.10000000000000001")
        self.assertEqual(aggregate.fills[1].raw_payload["_copytrade_canonical_start_position"], "0.1")
        self.assertEqual(len(PositionReconstructor().reconstruct(fills).events), 1)
        self.assertEqual(fills[0].raw_payload["startPosition"], "0.10000000000000001")

    def test_decimal_position_alias_cross_timestamp_uses_prior_source_lattice(self) -> None:
        timestamp = 1_700_000_000_000
        fills = [
            fill(1, timestamp, "B", 0.1, "0.0", order=1),
            fill(2, timestamp + 1, "B", 0.1, "0.10000000000000001", order=2),
        ]
        result = PositionReconstructor().reconstruct(fills)
        self.assertEqual([event.event_type for event in result.events], [PositionEventType.OPEN, PositionEventType.ADD])
        self.assertEqual(fills[1].raw_payload["startPosition"], "0.10000000000000001")

    def test_decimal_alias_continuity_uses_exact_source_boundaries_not_float_drift(self) -> None:
        # These values are the same binary64 source spelling / Decimal 0.1
        # lattice pattern found in saved FARTCOIN evidence.  The original
        # field remains high precision, so its 5e-11 stated-scale tolerance
        # must not be widened to conceal the one-ULP float state drift.
        timestamp = 1_700_000_000_000
        fills = [
            fill(1, timestamp, "B", 0.1, "3715129.7999999999", order=1),
            fill(2, timestamp + 1, "B", 0.1, "3715129.9", order=2),
        ]
        aggregate = aggregate_partial_fills(fills)[0]
        self.assertEqual(aggregate.position_tolerance, 5e-11)
        result = PositionReconstructor().reconstruct(fills)
        self.assertEqual([event.event_type for event in result.events], [PositionEventType.ADD, PositionEventType.ADD])

    def test_decimal_alias_continuity_does_not_accept_material_source_gap(self) -> None:
        timestamp = 1_700_000_000_000
        fills = [
            fill(1, timestamp, "B", 0.1, "3715129.7999999999", order=1),
            # The exact source lattice says the next boundary is 3715129.9,
            # so this 0.1 gap remains a hard discontinuity.
            fill(2, timestamp + 1, "B", 0.1, "3715130.0", order=2),
        ]
        with self.assertRaises(SourcePositionContinuityError) as raised:
            PositionReconstructor().reconstruct(fills)
        self.assertEqual(raised.exception.reason, "UNRESOLVED_SOURCE_POSITION_CONTINUITY")

    def test_decimal_position_alias_does_not_relax_non_alias_gap(self) -> None:
        timestamp = 1_700_000_000_000
        # This is close in ordinary decimal terms, but it is not the same
        # binary64 value as 0.1 and therefore cannot be canonicalized.
        fills = [
            fill(1, timestamp, "B", 0.1, "0.0", order=99),
            fill(2, timestamp, "B", 0.1, "0.1000000001", order=99),
        ]
        with self.assertRaises(SourcePositionContinuityError) as raised:
            aggregate_partial_fills(fills)
        self.assertEqual(raised.exception.reason, "UNRESOLVED_SOURCE_POSITION_GAP")

    def test_same_timestamp_source_ambiguities_fail_closed(self) -> None:
        timestamp = 1_700_000_000_000
        cases = {
            "UNRESOLVED_SOURCE_POSITION_DUPLICATE_BOUNDARY": [
                fill(1, timestamp, "B", 1, 0, order=99), fill(2, timestamp, "B", 1, 0, order=99),
            ],
            "UNRESOLVED_SOURCE_POSITION_AMBIGUOUS_CHAIN": [
                fill(1, timestamp, "B", 1, 0, order=99), fill(2, timestamp, "A", 1, 1, order=99),
            ],
            "UNRESOLVED_SOURCE_POSITION_GAP": [
                fill(1, timestamp, "B", 1, 0, order=99), fill(2, timestamp, "B", 1, 2, order=99),
            ],
        }
        for expected, fills in cases.items():
            with self.subTest(expected=expected), self.assertRaises(SourcePositionContinuityError) as raised:
                aggregate_partial_fills(fills)
            self.assertEqual(raised.exception.reason, expected)

    def test_same_timestamp_mixed_side_unique_chain_preserves_order_interleaving(self) -> None:
        timestamp = 1_700_000_000_000
        # startPosition and signed source size prove the only chain:
        # sell 2 -> 1, buy 1 -> 1.5, sell 1.5 -> 0.5.  The first and last
        # fills share an order ID, but the intervening order must remain in
        # causal position rather than being grouped past it.
        fills = [
            fill(30, timestamp, "A", 0.5, "1.5", order=10),
            fill(10, timestamp, "A", 1, "2.0", order=10),
            fill(20, timestamp, "B", 0.5, "1.0", order=20),
        ]
        aggregates = aggregate_partial_fills(fills)
        self.assertEqual([[item.target_trade_id for item in aggregate.fills] for aggregate in aggregates], [["10"], ["20"], ["30"]])
        self.assertEqual([event.event_type for event in PositionReconstructor().reconstruct(fills).events], [
            PositionEventType.REDUCE, PositionEventType.ADD, PositionEventType.REDUCE,
        ])

    def test_same_timestamp_mixed_side_ambiguity_repetition_gap_and_external_boundary(self) -> None:
        timestamp = 1_700_000_000_000
        cases = {
            "UNRESOLVED_SOURCE_POSITION_AMBIGUOUS_CHAIN": [
                fill(1, timestamp, "B", 1, 0), fill(2, timestamp, "A", 1, 1),
            ],
            "UNRESOLVED_SOURCE_POSITION_DUPLICATE_BOUNDARY": [
                fill(3, timestamp, "B", 1, 0), fill(4, timestamp, "A", 1, 0),
            ],
            "UNRESOLVED_SOURCE_POSITION_GAP": [
                fill(5, timestamp, "B", 1, 0), fill(6, timestamp, "A", 1, 2),
            ],
        }
        for expected, source in cases.items():
            with self.subTest(expected=expected), self.assertRaises(SourcePositionContinuityError) as raised:
                aggregate_partial_fills(source)
            self.assertEqual(raised.exception.reason, expected)

        # The same 1 -> 0 -> 1 cycle is ambiguous in isolation, but the prior
        # source boundary proves that the sell must be first.
        externally_anchored = [
            fill(7, timestamp - 1, "B", 1, 0),
            fill(8, timestamp, "B", 1, 0),
            fill(9, timestamp, "A", 1, 1),
        ]
        ordered = aggregate_partial_fills(externally_anchored)
        self.assertEqual([[item.target_trade_id for item in aggregate.fills] for aggregate in ordered], [["7"], ["9"], ["8"]])

    def test_cross_timestamp_source_position_gap_fails_closed(self) -> None:
        fills = [
            fill(1, 1_700_000_000_000, "B", 1, 0, order=1),
            # Reusing the order ID must not obscure a source-position gap
            # between its timestamp-separated pieces.
            fill(2, 1_700_000_000_100, "B", 1, 2, order=1),
        ]
        with self.assertRaises(SourcePositionContinuityError) as raised:
            PositionReconstructor().reconstruct(fills)
        self.assertEqual(raised.exception.reason, "UNRESOLVED_SOURCE_POSITION_CONTINUITY")

    def test_open_add_reduce_close_and_flip_reconstruction(self) -> None:
        sequence = [
            fill(1, 1_700_000_000_000, "B", 1, 0), fill(2, 1_700_000_000_100, "B", 1, 1),
            fill(3, 1_700_000_000_200, "A", 1, 2), fill(4, 1_700_000_000_300, "A", 1, 1),
            fill(5, 1_700_000_000_400, "B", 1, 0), fill(6, 1_700_000_000_500, "A", 2, 1),
        ]
        result = PositionReconstructor().reconstruct(sequence)
        self.assertEqual(
            [event.event_type for event in result.events],
            [PositionEventType.OPEN, PositionEventType.ADD, PositionEventType.REDUCE, PositionEventType.CLOSE, PositionEventType.OPEN, PositionEventType.CLOSE, PositionEventType.OPEN],
        )
        self.assertEqual(result.events[-1].after_quantity, -1)
        self.assertEqual(len([campaign for campaign in result.campaigns if campaign.closed_at]), 2)

    def test_size_classifier_uses_prior_observations_only(self) -> None:
        classifier = TargetSizeClassifier(SizingConfig(min_history=2))
        self.assertEqual(classifier.classify(WALLET, 100, 1000).bucket, "fallback")
        self.assertEqual(classifier.classify(WALLET, 100, 1000).bucket, "fallback")
        self.assertEqual(classifier.classify(WALLET, 30, 1000).allocation_fraction, 0.05)
        self.assertEqual(classifier.classify(WALLET, 100, 1000).allocation_fraction, 0.10)
        decision = classifier.classify(WALLET, 200, 1000)
        self.assertEqual(decision.bucket, "large")
        self.assertEqual(decision.allocation_fraction, 0.20)
        self.assertGreater(decision.size_ratio or 0, 1.5)

    def test_available_capital_and_portfolio_cap_are_applied(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = self.config(Path(temp), risk=replace(self.config(Path(temp)).risk, max_total_committed_fraction=0.10))
            engine = PaperExecutionEngine(config)
            attempt = engine.process_signal(signal("one", capital=50))
            self.assertEqual(attempt.status, "filled")
            sleeve = next(iter(engine.portfolio.sleeves.values()))
            self.assertAlmostEqual(sleeve.allocated_capital, 20.0)
            self.assertAlmostEqual(engine.portfolio.cash, 180.0)

    def test_target_partial_and_full_close(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            engine = PaperExecutionEngine(self.config(Path(temp)))
            self.assertEqual(engine.process_signal(signal("open", capital=100)).status, "filled")
            original_quantity = next(iter(engine.portfolio.sleeves.values())).quantity
            self.assertEqual(engine.process_signal(signal("reduce", action="reduce", quantity=25, before=100)).status, "filled")
            sleeve = next(iter(engine.portfolio.sleeves.values()))
            self.assertAlmostEqual(sleeve.quantity / original_quantity, 0.75, places=5)
            self.assertEqual(engine.process_signal(signal("close", action="close", quantity=75, before=75)).status, "filled")
            self.assertFalse(sleeve.is_open)
            self.assertAlmostEqual(engine.portfolio.committed_capital, 0.0)

    def test_fee_and_concurrent_target_capital_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fee_config = self.config(root, paper_execution=PaperExecutionConfig(fee_rate=0.01, slippage_bps=0, min_order_notional=1), risk=replace(self.config(root).risk, max_total_committed_fraction=1, max_capital_per_target_fraction=1, max_capital_per_symbol_fraction=1))
            engine = PaperExecutionEngine(fee_config)
            self.assertEqual(engine.process_signal(signal("fee-entry", capital=20)).status, "filled")
            self.assertAlmostEqual(engine.portfolio.cash, 179.8)
            self.assertEqual(engine.process_signal(signal("fee-exit", action="close", quantity=1, before=1)).status, "filled")
            self.assertAlmostEqual(engine.portfolio.cash, 199.6)

            no_fee = replace(fee_config, paper_execution=replace(fee_config.paper_execution, fee_rate=0.0))
            concurrent = PaperExecutionEngine(no_fee)
            first = signal("first", capital=100)
            second = replace(signal("second", capital=100), target_wallet="0x2222222222222222222222222222222222222222", symbol="ETH")
            third = replace(signal("third", capital=1), target_wallet="0x3333333333333333333333333333333333333333", symbol="SOL")
            self.assertEqual(concurrent.process_signal(first).status, "filled")
            self.assertEqual(concurrent.process_signal(second).status, "filled")
            self.assertEqual(concurrent.process_signal(third).reason, "insufficient_capital")
            self.assertAlmostEqual(concurrent.portfolio.cash, 0.0)

    def test_live_mode_is_rejected_even_with_explicit_opt_in(self) -> None:
        with self.assertRaises(ValueError):
            CopyTradeConfig(mode="live", live_enabled=False).validate()
        with self.assertRaises(ValueError):
            CopyTradeConfig(mode="live", live_enabled=True).validate()

    def test_stale_latency_and_slippage(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = self.config(Path(temp), risk=replace(self.config(Path(temp)).risk, max_signal_age_seconds=1), paper_execution=replace(self.config(Path(temp)).paper_execution, detection_latency_ms=250))
            engine = PaperExecutionEngine(config)
            old = signal("old", event_time=1_700_000_000_000)
            self.assertEqual(engine.process_signal(old, received_at=old.source_event_timestamp + timedelta(seconds=2)).reason, "stale_signal")
            current = signal("current", event_time=1_700_000_100_000)
            attempt = engine.process_signal(current)
            self.assertEqual(attempt.detection_latency_ms, 250.0)
            sleeve = next(iter(engine.portfolio.sleeves.values()))
            self.assertGreater(sleeve.entry_price, current.target_price)

    def test_event_replay_is_deterministic_and_walk_forward_has_prior_only_seed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = self.config(Path(temp), sizing=SizingConfig(min_history=1), backtest=replace(CopyTradeConfig().backtest, default_training_days=1, default_forward_days=1))
            fills = [
                fill(1, 1_700_000_000_000, "B", 1, 0), fill(2, 1_700_000_001_000, "A", 1, 1),
                fill(3, 1_700_172_800_000, "B", 2, 0), fill(4, 1_700_172_801_000, "A", 2, 2),
                fill(5, 1_700_345_600_000, "B", 3, 0), fill(6, 1_700_345_601_000, "A", 3, 3),
            ]
            first = CopyTradeBacktester(config).run(fills)
            second = CopyTradeBacktester(config).run(fills)
            self.assertEqual(first.summary, second.summary)
            windows = CopyTradeBacktester(config).walk_forward(fills, training_days=1, forward_days=1)
            self.assertTrue(windows)
            self.assertEqual(windows[0]["training_campaigns"], 1)

    def test_backtest_run_configuration_with_paths_is_persistable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = self.config(Path(temp))
            database = CopyTradeDatabase(config.artifacts.database_path)
            database.initialize()
            run = CopyTradeBacktester(config, database).run([fill(1, 1_700_000_000_000, "B", 1, 0)])
            self.assertEqual(run.summary["events_replayed"], 1)

    def test_watcher_snapshot_storage_deduplicates_and_obsidian_report_generates(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config = self.config(Path(temp))
            service = CopyTradeService(config)
            service.database.upsert_target(Target(wallet=WALLET))
            item = fill(1, 1_700_000_000_000, "B", 1, 0)
            asyncio.run(service.ingest_watched_fills(WALLET, [item], True))
            asyncio.run(service.ingest_watched_fills(WALLET, [item], False))
            self.assertEqual(len(service.database.list_raw_fills(WALLET)), 1)
            self.assertEqual(len(service.database.dashboard_snapshot()["execution_attempts"]), 1)
            result = service.reconstruct(WALLET)
            self.assertEqual(len(result["events"]), 1)
            note = ObsidianExporter(config, service.database).export_target(WALLET)
            self.assertTrue(note.exists())
            self.assertTrue((config.artifacts.obsidian_root / "charts" / f"{WALLET}_equity.svg").exists())


if __name__ == "__main__":
    unittest.main()
