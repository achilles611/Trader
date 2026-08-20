"""Adversarial and deterministic tests for Lane III Phase B observations."""

from __future__ import annotations

import ast
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from src.lane_iii.contracts import canonical_hash
from src.lane_iii.market_data import (
    AggressorProvenance,
    AggressorSide,
    BackpressureRefused,
    BarAccumulator,
    BookDeltaEvent,
    BookLevel,
    BookSide,
    BookSnapshotEvent,
    BoundedMarketDataBuffer,
    DataQuality,
    DepthOperation,
    EventHeader,
    EventTimestamps,
    LiquidityBehavior,
    L3B_L3A_CONSTITUTION,
    MarketDataPipeline,
    MarketDataRefused,
    MarketDataSource,
    MarketStream,
    MNQContract,
    OrderingOutcome,
    OrderBookReconstructor,
    QuoteEvent,
    RawProviderEvent,
    SessionContextAccumulator,
    TradeEvent,
)
from src.lane_iii.market_data_capture import AppendOnlyMarketCapture, DeterministicReplay, event_from_payload
from src.lane_iii.contracts import L3A_MANIFEST


ROOT = Path(__file__).resolve().parents[1]
SOURCE = MarketDataSource("fixture-provider", "mnq-depth")
INSTRUMENT = MNQContract("MNQU6", "2026-09")
BASE = "2026-08-19T14:30:00Z"


class LaneIIIPhaseBTests(unittest.TestCase):
    def header(
        self,
        event_id: str,
        stream: MarketStream,
        sequence: int | None,
        *,
        receipt: str = BASE,
        exchange: str | None = BASE,
        provider: str | None = BASE,
    ) -> EventHeader:
        return EventHeader(
            event_id=event_id,
            source=SOURCE,
            instrument=INSTRUMENT,
            timestamps=EventTimestamps(receipt, exchange, provider),
            stream=stream,
            raw_event_id="raw-" + event_id,
            raw_payload_hash=canonical_hash({"event_id": event_id}),
            provider_sequence=sequence,
            provider_event_id="provider-" + event_id,
        )

    def trade(
        self,
        event_id: str,
        sequence: int | None,
        *,
        price: Decimal = Decimal("22000.00"),
        size: int = 3,
        side: AggressorSide = AggressorSide.BUY,
        provenance: AggressorProvenance = AggressorProvenance.PROVIDER,
        receipt: str = BASE,
        exchange: str | None = BASE,
    ) -> TradeEvent:
        return TradeEvent(self.header(event_id, MarketStream.TRADE, sequence, receipt=receipt, exchange=exchange), price, size, side, provenance)

    def snapshot(self, event_id: str = "snapshot-1", sequence: int | None = 1, *, receipt: str = BASE) -> BookSnapshotEvent:
        return BookSnapshotEvent(
            self.header(event_id, MarketStream.DEPTH, sequence, receipt=receipt),
            (BookLevel(Decimal("22000.00"), 10), BookLevel(Decimal("21999.75"), 8)),
            (BookLevel(Decimal("22000.25"), 12), BookLevel(Decimal("22000.50"), 7)),
        )

    def delta(
        self,
        event_id: str,
        sequence: int | None,
        side: BookSide,
        operation: DepthOperation,
        price: Decimal,
        quantity: int | None = None,
    ) -> BookDeltaEvent:
        return BookDeltaEvent(self.header(event_id, MarketStream.DEPTH, sequence), side, operation, price, quantity)

    # Event validity / numeric integrity

    def test_concrete_mnq_identity_preserves_expiry_and_rejects_non_mnq(self) -> None:
        self.assertEqual(INSTRUMENT.strategy_instrument.value, "MNQ")
        self.assertEqual(INSTRUMENT.contract_expiry, "2026-09")
        with self.assertRaises(MarketDataRefused):
            MNQContract("NQU6", "2026-09")
        with self.assertRaises(ValueError):
            MNQContract("MNQU6", "2026-12")

    def test_timestamp_clocks_remain_separate_and_unavailable_is_not_manufactured(self) -> None:
        times = EventTimestamps("2026-08-19T08:30:00-06:00", None, "2026-08-19T14:30:01Z")
        self.assertIsNone(times.exchange_time)
        self.assertEqual(times.authoritative_event_time, "2026-08-19T14:30:01Z")
        self.assertEqual(times.payload()["local_receipt_time"], "2026-08-19T14:30:00Z")
        with self.assertRaises(ValueError):
            EventTimestamps("2026-08-19T14:30:00")

    def test_malformed_trade_quote_and_depth_are_refused(self) -> None:
        with self.assertRaises(ValueError):
            TradeEvent(self.header("bad-trade", MarketStream.TRADE, 1), Decimal("22000"), -1)
        with self.assertRaises(ValueError):
            QuoteEvent(self.header("bad-quote", MarketStream.QUOTE, 1), Decimal("22000.25"), Decimal("22000.00"), 1, 1)
        with self.assertRaises(ValueError):
            BookDeltaEvent(self.header("bad-depth", MarketStream.DEPTH, 1), BookSide.BID, DepthOperation.UPSERT, Decimal("22000.10"), 1)

    def test_nonfinite_and_invalid_increment_values_are_refused(self) -> None:
        with self.assertRaises(ValueError):
            TradeEvent(self.header("nan", MarketStream.TRADE, 1), Decimal("NaN"), 1)
        with self.assertRaises(ValueError):
            TradeEvent(self.header("inf", MarketStream.TRADE, 1), Decimal("Infinity"), 1)
        with self.assertRaises(ValueError):
            QuoteEvent(self.header("zero", MarketStream.QUOTE, 1), Decimal("22000"), Decimal("22000.25"), 0, 1)

    def test_unknown_aggressor_stays_unknown_and_derived_side_has_provenance(self) -> None:
        unknown = TradeEvent(self.header("unknown", MarketStream.TRADE, 1), Decimal("22000"), 1)
        self.assertIs(unknown.aggressor_side, AggressorSide.UNKNOWN)
        self.assertIs(unknown.aggressor_provenance, AggressorProvenance.UNAVAILABLE)
        derived = TradeEvent(
            self.header("derived", MarketStream.TRADE, 2), Decimal("22000.25"), 2,
            AggressorSide.BUY, AggressorProvenance.QUOTE_DERIVED, "quote-2",
        )
        self.assertEqual(derived.payload()["derivation_quote_event_id"], "quote-2")
        with self.assertRaises(ValueError):
            TradeEvent(self.header("false-fact", MarketStream.TRADE, 3), Decimal("22000"), 1, AggressorSide.BUY)

    # Ordering and book recovery

    def test_snapshot_and_incremental_depth_reconstruct_deterministically(self) -> None:
        book = OrderBookReconstructor(SOURCE, INSTRUMENT)
        first = book.apply(self.snapshot())
        second = book.apply(self.delta("upsert", 2, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 15))
        self.assertEqual(first.state.quality, DataQuality.HEALTHY)
        self.assertEqual(second.outcome.value, "DELTA_APPLIED")
        self.assertEqual(second.changes[0].behavior, LiquidityBehavior.ADD)
        self.assertEqual(second.state.bids[0].quantity, 15)

    def test_duplicate_late_and_equal_timestamp_sequence_order_are_visible(self) -> None:
        book = OrderBookReconstructor(SOURCE, INSTRUMENT)
        book.apply(self.snapshot("snapshot", 10))
        self.assertEqual(book.apply(self.delta("duplicate", 10, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 11)).outcome.value, "DUPLICATE")
        self.assertEqual(book.apply(self.delta("late", 9, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 11)).outcome.value, "LATE")
        applied = book.apply(self.delta("same-time-next-sequence", 11, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 11))
        self.assertEqual(applied.outcome.value, "DELTA_APPLIED")
        self.assertEqual(applied.state.latest_sequence, 11)

    def test_missing_sequence_and_reconnect_require_a_recovery_snapshot(self) -> None:
        book = OrderBookReconstructor(SOURCE, INSTRUMENT)
        book.apply(self.snapshot("snapshot", 1))
        gap = book.apply(self.delta("gap", 3, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 9))
        self.assertEqual(gap.outcome.value, "GAP")
        self.assertEqual(gap.state.quality, DataQuality.GAPPED)
        self.assertEqual(book.apply(self.delta("not-authoritative", 4, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 8)).outcome.value, "RECOVERY_REQUIRED")
        recovered = book.apply(self.snapshot("recovery", 5))
        self.assertEqual(recovered.state.quality, DataQuality.HEALTHY)
        book.notify_reconnect()
        self.assertEqual(book.apply(self.delta("after-disconnect", 1, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 8)).outcome.value, "RECOVERY_REQUIRED")
        self.assertEqual(book.apply(self.snapshot("reset-sequence-snapshot", 1)).state.quality, DataQuality.HEALTHY)

    def test_unsequenced_depth_is_explicitly_incomplete_not_silently_healthy(self) -> None:
        book = OrderBookReconstructor(SOURCE, INSTRUMENT)
        snapshot = book.apply(self.snapshot("unsequenced", None))
        self.assertEqual(snapshot.state.quality, DataQuality.INCOMPLETE)
        self.assertEqual(book.apply(self.delta("unsequenced-delta", None, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 11)).outcome.value, "RECOVERY_REQUIRED")

    def test_book_change_measurements_remain_mechanical_and_execution_requires_trade_match(self) -> None:
        book = OrderBookReconstructor(SOURCE, INSTRUMENT)
        book.apply(self.snapshot())
        reduce = book.apply(self.delta("reduce", 2, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 5)).changes[0]
        self.assertEqual(reduce.behavior, LiquidityBehavior.REDUCE)
        execute = book.classify_reduction_with_trades(reduce, (self.trade("hit-bid", 1, price=Decimal("22000"), side=AggressorSide.SELL),))
        self.assertEqual(execute.behavior, LiquidityBehavior.EXECUTE)
        pull = book.apply(self.delta("remove", 3, BookSide.BID, DepthOperation.REMOVE, Decimal("22000"))).changes[0]
        self.assertEqual(pull.behavior, LiquidityBehavior.REDUCE)
        self.assertEqual(book.classify_reduction_with_trades(pull, ()).behavior, LiquidityBehavior.PULL)
        replenish = book.apply(self.delta("readd", 4, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 8)).changes[0]
        self.assertEqual(replenish.behavior, LiquidityBehavior.REPLENISH)

    def test_book_staleness_and_persistence_are_explicit(self) -> None:
        book = OrderBookReconstructor(SOURCE, INSTRUMENT)
        book.apply(self.snapshot(receipt="2026-08-19T14:30:00Z"))
        state = book.mark_stale("2026-08-19T14:31:01Z", timedelta(minutes=1))
        self.assertEqual(state.quality, DataQuality.STALE)
        book = OrderBookReconstructor(SOURCE, INSTRUMENT)
        book.apply(self.snapshot())
        persistence = book.persistence_measurements("2026-08-19T14:31:00Z", timedelta(seconds=30))
        self.assertTrue(persistence)
        self.assertTrue(all(value.behavior is LiquidityBehavior.PERSIST for value in persistence))

    # Flow, context, bars, staleness

    def test_trade_flow_is_deterministic_and_gap_or_unknown_marks_cvd_incomplete(self) -> None:
        pipeline = MarketDataPipeline(SOURCE, INSTRUMENT)
        first = pipeline.apply(self.trade("buy", 1, size=5)).trade_flow
        second = pipeline.apply(self.trade("sell", 2, size=2, side=AggressorSide.SELL)).trade_flow
        self.assertEqual(first.cumulative_delta, 5)
        self.assertEqual(second.cumulative_delta, 3)
        gapped = pipeline.apply(self.trade("gap", 4, size=1)).trade_flow
        self.assertFalse(gapped.complete)
        self.assertIsNone(gapped.cumulative_delta)
        pipeline = MarketDataPipeline(SOURCE, INSTRUMENT)
        unknown = TradeEvent(self.header("unknown-flow", MarketStream.TRADE, 1), Decimal("22000"), 2)
        self.assertFalse(pipeline.apply(unknown).trade_flow.complete)  # type: ignore[union-attr]
        pipeline = MarketDataPipeline(SOURCE, INSTRUMENT)
        self.assertFalse(pipeline.apply(self.trade("unsequenced-flow", None)).trade_flow.complete)  # type: ignore[union-attr]

    def test_cvd_resets_at_the_explicit_session_boundary(self) -> None:
        pipeline = MarketDataPipeline(SOURCE, INSTRUMENT)
        first = pipeline.apply(self.trade("session-one", 1, size=5, exchange="2026-08-19T23:00:00Z", receipt="2026-08-19T23:00:00Z")).trade_flow
        second = pipeline.apply(self.trade("session-two", 2, size=2, exchange="2026-08-20T23:00:00Z", receipt="2026-08-20T23:00:00Z")).trade_flow
        self.assertEqual(first.cumulative_delta, 5)
        self.assertEqual(second.cumulative_delta, 2)

    def test_ohlc_bars_have_disclosed_time_basis_and_late_trade_cannot_rewrite_history(self) -> None:
        bars = BarAccumulator(INSTRUMENT, timedelta(minutes=1))
        bars.ingest(self.trade("t1", 1, price=Decimal("22000"), exchange="2026-08-19T14:30:01Z"))
        bars.ingest(self.trade("t2", 2, price=Decimal("22001"), exchange="2026-08-19T14:30:30Z"))
        complete = bars.ingest(self.trade("t3", 3, price=Decimal("22000.25"), exchange="2026-08-19T14:31:01Z"))
        self.assertEqual(complete[0].high, Decimal("22001"))
        self.assertEqual(complete[0].time_basis, "exchange_time")
        self.assertEqual(bars.ingest(self.trade("late", 4, exchange="2026-08-19T14:30:10Z")), ())
        self.assertFalse(bars.current().complete)  # type: ignore[union-attr]

    def test_session_context_uses_dst_aware_cme_boundaries_and_resets(self) -> None:
        context = SessionContextAccumulator(INSTRUMENT)
        overnight = context.ingest(self.trade("overnight", 1, price=Decimal("22000"), exchange="2026-03-09T01:00:00Z", receipt="2026-03-09T01:00:00Z"))
        cash = context.ingest(self.trade("cash", 2, price=Decimal("22001"), exchange="2026-03-09T14:31:00Z", receipt="2026-03-09T14:31:00Z"))
        self.assertEqual(overnight.session_id, cash.session_id)
        self.assertEqual(overnight.overnight_high, Decimal("22000"))
        self.assertEqual(cash.cash_open_price, Decimal("22001"))
        next_session = context.ingest(self.trade("next", 3, exchange="2026-03-10T00:01:00Z", receipt="2026-03-10T00:01:00Z"))
        self.assertNotEqual(next_session.session_id, cash.session_id)
        self.assertEqual(next_session.prior_session_high, Decimal("22001"))

    def test_family_staleness_has_distinct_lifetimes_without_strategy_thresholds(self) -> None:
        pipeline = MarketDataPipeline(SOURCE, INSTRUMENT)
        quote = QuoteEvent(self.header("quote", MarketStream.QUOTE, 1, receipt="2026-08-19T14:30:00Z"), Decimal("22000"), Decimal("22000.25"), 4, 5)
        pipeline.apply(quote)
        health = pipeline.staleness(
            "2026-08-19T14:31:00Z", trade_maximum_age=timedelta(minutes=10), quote_maximum_age=timedelta(seconds=30), book_maximum_age=timedelta(minutes=10),
        )
        self.assertEqual(health["quote"], DataQuality.STALE)
        self.assertEqual(health["trade"], DataQuality.INCOMPLETE)

    # Capture, replay, backpressure, and provider boundary

    def test_raw_and_normalized_capture_are_append_only_integrity_checked_and_replayable(self) -> None:
        raw = RawProviderEvent("raw-fixture", SOURCE, BASE, {"packet": "trade", "size": 3}, "provider-fixture")
        events = (self.snapshot(), self.trade("trade", 1, size=3), self.delta("delta", 2, BookSide.BID, DepthOperation.UPSERT, Decimal("22000"), 9))
        with tempfile.TemporaryDirectory() as directory:
            capture = AppendOnlyMarketCapture(directory)
            capture.record_raw(raw)
            for event in events:
                capture.record_normalized(event)
            self.assertEqual(capture.stats().raw_events, 1)
            self.assertEqual(tuple(capture.raw_events()), (raw,))
            loaded = tuple(capture.normalized_events())
            first = DeterministicReplay(MarketDataPipeline(SOURCE, INSTRUMENT)).replay(loaded)
            restarted = AppendOnlyMarketCapture(directory)
            second = DeterministicReplay(MarketDataPipeline(SOURCE, INSTRUMENT)).replay_capture(restarted)
            self.assertEqual(first.final_book_hash, second.final_book_hash)
            self.assertEqual(first.results, second.results)

    def test_capture_integrity_tampering_fails_visibly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            capture = AppendOnlyMarketCapture(directory)
            capture.record_normalized(self.snapshot())
            text = capture.normalized_path.read_text(encoding="utf-8").replace("22000.25", "22000.50", 1)
            capture.normalized_path.write_text(text, encoding="utf-8")
            with self.assertRaises(MarketDataRefused):
                tuple(capture.normalized_events())

    def test_replay_rehydrates_through_strict_canonical_constructors(self) -> None:
        payload = self.trade("round-trip", 1).payload()
        self.assertEqual(event_from_payload(payload), self.trade("round-trip", 1))
        payload["size"] = -1
        with self.assertRaises(ValueError):
            event_from_payload(payload)

    def test_bounded_buffer_rejects_bursts_visibly_without_dropping(self) -> None:
        buffer = BoundedMarketDataBuffer(2)
        buffer.publish(self.trade("one", 1))
        buffer.publish(self.trade("two", 2))
        with self.assertRaises(BackpressureRefused):
            buffer.publish(self.trade("three", 3))
        self.assertEqual(buffer.rejected, 1)
        self.assertEqual(buffer.quality, DataQuality.INVALID)
        self.assertEqual(len(buffer.drain()), 2)

    def test_plausible_fixture_burst_processes_without_silent_loss(self) -> None:
        pipeline = MarketDataPipeline(SOURCE, INSTRUMENT)
        total = 1_000
        for sequence in range(1, total + 1):
            pipeline.apply(self.trade(f"burst-{sequence}", sequence, size=1))
        flow = pipeline.trade_flow.measurements()
        metrics = pipeline.metrics()
        self.assertEqual(flow.trade_count, total)
        self.assertEqual(flow.total_volume, total)
        self.assertTrue(flow.complete)
        self.assertEqual(metrics.events_processed, total)
        self.assertEqual(metrics.events_rejected, 0)
        self.assertEqual(metrics.sequence_gaps, 0)

    # Constitutional and authority isolation

    def test_l3b_source_has_no_transport_execution_phase_e_or_lane_ii_imports(self) -> None:
        forbidden: list[str] = []
        for source in (ROOT / "src" / "lane_iii").glob("market_data*.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names: list[str] = []
                if isinstance(node, ast.ImportFrom) and node.module:
                    names.append(node.module)
                elif isinstance(node, ast.Import):
                    names.extend(alias.name for alias in node.names)
                forbidden.extend(name for name in names if name.startswith(("src.phase_e", "src.lane_ii", "src.copytrade", "requests", "websockets")))
        self.assertEqual(forbidden, [])

    def test_l3b_is_bound_to_the_unchanged_l3a_constitution(self) -> None:
        self.assertIs(L3B_L3A_CONSTITUTION, L3A_MANIFEST)

    def test_l3b_pipeline_exposes_observation_not_hypothesis_confidence_or_execution_methods(self) -> None:
        forbidden = {"hypothesis", "confidence", "signal", "intent", "order", "broker", "account", "risk", "execute"}
        public = {name.lower() for name in dir(MarketDataPipeline) if not name.startswith("_")}
        self.assertFalse(public & forbidden)
        self.assertFalse(hasattr(BoundedMarketDataBuffer, "submit_order"))

    def test_options_extension_requires_explicit_vintage_but_no_provider_exists(self) -> None:
        from src.lane_iii.market_data import DerivativesContextEvent, OptionRight

        extension = DerivativesContextEvent(
            self.header("option", MarketStream.DERIVATIVES_CONTEXT, 1), "MNQ", "2026-09-18", Decimal("22000"),
            OptionRight.CALL, 123, 7, "2026-08-18T21:00:00Z",
        )
        self.assertEqual(extension.payload()["data_vintage_time"], "2026-08-18T21:00:00Z")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
