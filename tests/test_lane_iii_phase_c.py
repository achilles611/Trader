"""Adversarial commissioning tests for Lane III Phase C (L3-C)."""

from __future__ import annotations

import ast
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import unittest

from src.lane_iii.contracts import EvidenceFamily, EvidenceRelation, HypothesisDirection, HypothesisKind
from src.lane_iii.hypothesis_engine import (
    BEARISH_CONTINUATION,
    BULLISH_CONTINUATION,
    BULLISH_REVERSAL,
    ConfidenceAssessment,
    DeterministicHypothesisReplay,
    EvidenceDerivation,
    EvidenceImpact,
    EvidenceKind,
    EvidenceObject,
    EvidenceUsability,
    HypothesisEngine,
    HypothesisEngineConfig,
    HypothesisState,
    L3C_L3A_AUTHORITY_MANIFEST_HASH,
    L3C_L3B_CONSTITUTION,
    SourceProvenance,
)
from src.lane_iii.market_data import (
    AggressorProvenance,
    AggressorSide,
    BookApplication,
    BookDeltaEvent,
    BookLevel,
    BookSide,
    BookSnapshotEvent,
    DataQuality,
    DepthOperation,
    EventHeader,
    EventTimestamps,
    LiquidityBehavior,
    MNQContract,
    MarketDataPipeline,
    MarketDataSource,
    MarketStream,
    PipelineResult,
    TradeEvent,
)


ROOT = Path(__file__).resolve().parents[1]


class LaneIIIPhaseCTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = MarketDataSource("fixture", "mnq")
        self.instrument = MNQContract("MNQU6", "2026-09")
        self.base = datetime(2026, 8, 19, 14, 30, tzinfo=timezone.utc)

    def config(self, **overrides: object) -> HypothesisEngineConfig:
        values: dict[str, object] = {
            "dominance_threshold": Decimal("0.90"),
            "flow_lifetime": timedelta(seconds=10),
            "structural_lifetime": timedelta(seconds=15),
            "liquidity_lifetime": timedelta(seconds=10),
            "timing_lifetime": timedelta(seconds=10),
            "hypothesis_idle_lifetime": timedelta(seconds=15),
            "hypothesis_maximum_lifetime": timedelta(seconds=60),
            "trade_maximum_age": timedelta(seconds=10),
            "book_maximum_age": timedelta(seconds=10),
            "maximum_evidence_objects": 32,
            "maximum_history": 32,
        }
        values.update(overrides)
        return HypothesisEngineConfig(**values)  # type: ignore[arg-type]

    def header(self, event_id: str, stream: MarketStream, sequence: int | None, second: int) -> EventHeader:
        at = (self.base + timedelta(seconds=second)).isoformat().replace("+00:00", "Z")
        return EventHeader(
            event_id, self.source, self.instrument, EventTimestamps(at, at), stream,
            f"raw-{event_id}", "a" * 64, sequence,
        )

    def trade(self, event_id: str, sequence: int | None, second: int, price: str, side: AggressorSide, *, size: int = 2) -> TradeEvent:
        return TradeEvent(
            self.header(event_id, MarketStream.TRADE, sequence, second), Decimal(price), size,
            side, AggressorProvenance.PROVIDER,
        )

    def snapshot(self, event_id: str = "book-1", sequence: int | None = 1, second: int = 0) -> BookSnapshotEvent:
        return BookSnapshotEvent(
            self.header(event_id, MarketStream.DEPTH, sequence, second),
            (BookLevel(Decimal("100.00"), 10),), (BookLevel(Decimal("100.25"), 10),),
        )

    def delta(self, event_id: str, sequence: int | None, second: int, side: BookSide, operation: DepthOperation, price: str, quantity: int | None = None) -> BookDeltaEvent:
        return BookDeltaEvent(self.header(event_id, MarketStream.DEPTH, sequence, second), side, operation, Decimal(price), quantity)

    def apply(self, pipeline: MarketDataPipeline, engine: HypothesisEngine, event: object):
        result = pipeline.apply(event)  # type: ignore[arg-type]
        return engine.observe(event, result, pipeline)  # type: ignore[arg-type]

    @staticmethod
    def hypothesis(snapshot, identity):
        return next(item for item in snapshot.hypotheses if item.identity == identity)

    def bullish_reversal_scenario(self, engine: HypothesisEngine, pipeline: MarketDataPipeline):
        """Scenario A: selling fails to progress, bid replenishes, range reclaims."""
        events = (
            self.snapshot(),
            self.trade("a-t1", 1, 1, "100.00", AggressorSide.SELL),
            self.trade("a-t2", 2, 2, "99.75", AggressorSide.SELL),
            self.trade("a-t3", 3, 3, "100.00", AggressorSide.SELL),
            self.delta("a-d2", 2, 4, BookSide.BID, DepthOperation.REMOVE, "100.00"),
            self.delta("a-d3", 3, 5, BookSide.BID, DepthOperation.UPSERT, "100.00", 10),
            self.delta("a-d4", 4, 6, BookSide.BID, DepthOperation.REMOVE, "100.00"),
            self.delta("a-d5", 5, 7, BookSide.BID, DepthOperation.UPSERT, "100.00", 10),
        )
        for event in events:
            state = self.apply(pipeline, engine, event)
        return state

    def test_evidence_is_deterministic_and_provenance_is_traceable(self) -> None:
        events = (
            self.trade("t1", 1, 1, "100.00", AggressorSide.SELL),
            self.trade("t2", 2, 2, "99.75", AggressorSide.SELL),
            self.trade("t3", 3, 3, "100.00", AggressorSide.SELL),
        )
        one_pipeline, one_engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config())
        two_pipeline, two_engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config())
        one = DeterministicHypothesisReplay(one_pipeline, one_engine).replay(events)
        two = DeterministicHypothesisReplay(two_pipeline, two_engine).replay(events)
        self.assertEqual(one, two)
        self.assertEqual(one.final_snapshot_hash, two.final_snapshot_hash)
        evidence = next(item.evidence for item in one.snapshots[-1].evidence if item.evidence.kind is EvidenceKind.AGGRESSIVE_SELL_IMBALANCE)
        self.assertEqual(evidence.source.source_event_ids, ("t1", "t2", "t3"))
        self.assertEqual(len(evidence.source.source_payload_hashes), 3)
        self.assertEqual(evidence.source.window_start, "2026-08-19T14:30:01Z")
        self.assertEqual(evidence.source.window_end, "2026-08-19T14:30:03Z")

    def test_duplicate_source_event_does_not_duplicate_evidence(self) -> None:
        pipeline, engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config(timing_lifetime=timedelta(seconds=30)))
        for event in (
            self.trade("t1", 1, 1, "100.00", AggressorSide.SELL),
            self.trade("t2", 2, 2, "99.75", AggressorSide.SELL),
            self.trade("t3", 3, 3, "99.50", AggressorSide.SELL),
        ):
            state = self.apply(pipeline, engine, event)
        before = state.metrics.evidence_objects_created
        duplicate = self.trade("duplicate-packet", 3, 4, "99.50", AggressorSide.SELL)
        state = self.apply(pipeline, engine, duplicate)
        self.assertEqual(state.metrics.evidence_objects_created, before)
        self.assertEqual(state.metrics.observations_processed, 4)

    def test_stale_invalidates_existing_source_dependent_evidence(self) -> None:
        pipeline, engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config(timing_lifetime=timedelta(seconds=30)))
        state = self.apply(pipeline, engine, self.trade("t1", 1, 1, "100.00", AggressorSide.BUY))
        self.assertTrue(any(item.usability is EvidenceUsability.AUTHORITATIVE for item in state.evidence))
        state = engine.advance("2026-08-19T14:30:12Z", pipeline)
        self.assertTrue(any(item.usability is EvidenceUsability.UNUSABLE for item in state.evidence))
        self.assertEqual(state.metrics.active_hypotheses, 0)

    def test_gapped_dom_cannot_create_authoritative_liquidity_evidence(self) -> None:
        pipeline, engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config())
        self.apply(pipeline, engine, self.snapshot())
        state = self.apply(pipeline, engine, self.delta("gap", 3, 1, BookSide.BID, DepthOperation.UPSERT, "100.00", 12))
        self.assertEqual(pipeline.book._state().quality, DataQuality.GAPPED)
        self.assertFalse(any(item.evidence.family is EvidenceFamily.RESTING_LIQUIDITY and item.usability is EvidenceUsability.AUTHORITATIVE for item in state.evidence))
        self.assertEqual(state.rejected_observations[-1].source_quality, DataQuality.GAPPED)

    def test_correlated_evidence_uses_one_family_contribution(self) -> None:
        engine = HypothesisEngine(self.source, self.instrument, self.config())
        at = "2026-08-19T14:30:00Z"
        source = SourceProvenance(("s1",), ("a" * 64,), at, at, DataQuality.HEALTHY, "l3b:synthetic")
        def evidence(identity: str, strength: str) -> EvidenceObject:
            payload = {"identity": identity, "strength": strength}
            return EvidenceObject(
                "l3c-e-" + "b" * 32 if identity == "one" else "l3c-e-" + "c" * 32,
                EvidenceFamily.ORDER_FLOW, EvidenceKind.AGGRESSIVE_SELL_IMBALANCE,
                EvidenceDerivation.HIGHER_ORDER_DERIVED, at, "2026-08-19T14:30:10Z", source,
                (EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.SUPPORTS, Decimal(strength)),), "flow:same-window", Decimal(strength),
            )
        assessment = engine._assess(BEARISH_CONTINUATION, (evidence("one", "0.40"), evidence("two", "0.80")), datetime.fromisoformat(at.replace("Z", "+00:00")))
        self.assertEqual(len(assessment.family_contributions), 1)
        self.assertEqual(assessment.family_contributions[0].strongest_support, Decimal("0.80"))
        self.assertEqual(assessment.relative_support, Decimal("0.58"))

    def test_independent_families_contribute_separately(self) -> None:
        engine = HypothesisEngine(self.source, self.instrument, self.config())
        at = "2026-08-19T14:30:00Z"
        source = SourceProvenance(("s1",), ("a" * 64,), at, at, DataQuality.HEALTHY, "l3b:synthetic")
        flow = EvidenceObject("l3c-e-" + "d" * 32, EvidenceFamily.ORDER_FLOW, EvidenceKind.AGGRESSIVE_SELL_IMBALANCE, EvidenceDerivation.HIGHER_ORDER_DERIVED, at, "2026-08-19T14:30:10Z", source, (EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.SUPPORTS, Decimal("0.50")),), "flow", Decimal("0.50"))
        structural = EvidenceObject("l3c-e-" + "e" * 32, EvidenceFamily.STRUCTURAL_CONTEXT, EvidenceKind.BELOW_VWAP, EvidenceDerivation.HIGHER_ORDER_DERIVED, at, "2026-08-19T14:30:10Z", source, (EvidenceImpact(BEARISH_CONTINUATION, EvidenceRelation.SUPPORTS, Decimal("0.50")),), "structure", Decimal("0.50"))
        assessment = engine._assess(BEARISH_CONTINUATION, (flow, structural), datetime.fromisoformat(at.replace("Z", "+00:00")))
        self.assertEqual(len(assessment.family_contributions), 2)
        self.assertEqual(assessment.relative_support, Decimal("0.60"))

    def test_competing_hypotheses_coexist_and_no_winner_is_forced(self) -> None:
        pipeline, engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config())
        state = self.bullish_reversal_scenario(engine, pipeline)
        bullish_reversal = self.hypothesis(state, BULLISH_REVERSAL)
        bearish_continuation = self.hypothesis(state, BEARISH_CONTINUATION)
        self.assertGreater(bullish_reversal.confidence.relative_support, Decimal("0.50"))
        self.assertEqual(bearish_continuation.state, HypothesisState.CONFLICTED)
        self.assertIsNone(state.dominant_hypothesis_id)

    def test_contradiction_reduces_relative_support_and_can_conflict(self) -> None:
        pipeline, engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config())
        self.apply(pipeline, engine, self.trade("t1", 1, 1, "100.00", AggressorSide.SELL))
        self.apply(pipeline, engine, self.trade("t2", 2, 2, "99.75", AggressorSide.SELL))
        before = self.apply(pipeline, engine, self.trade("t3", 3, 3, "99.50", AggressorSide.SELL))
        before_score = self.hypothesis(before, BEARISH_CONTINUATION).confidence.relative_support
        after = self.apply(pipeline, engine, self.trade("t4", 4, 4, "100.00", AggressorSide.SELL))
        record = self.hypothesis(after, BEARISH_CONTINUATION)
        self.assertLess(record.confidence.relative_support, before_score)
        self.assertEqual(record.state, HypothesisState.CONFLICTED)
        self.assertTrue(after.confidence_updates)

    def test_classified_bid_pull_invalidates_bullish_continuation(self) -> None:
        pipeline, engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config())
        self.apply(pipeline, engine, self.snapshot())
        self.apply(pipeline, engine, self.trade("t1", 1, 1, "100.00", AggressorSide.BUY))
        state = self.apply(pipeline, engine, self.trade("t2", 2, 2, "100.25", AggressorSide.BUY))
        self.assertIn(BULLISH_CONTINUATION, {item.identity for item in state.hypotheses})
        event = self.delta("remove", 2, 3, BookSide.BID, DepthOperation.REMOVE, "100.00")
        actual = pipeline.apply(event)
        assert actual.book_application is not None
        classified = pipeline.book.classify_reduction_with_trades(actual.book_application.changes[0], ())
        self.assertIs(classified.behavior, LiquidityBehavior.PULL)
        result = PipelineResult(event.header.event_id, None, BookApplication(actual.book_application.outcome, actual.book_application.state, (classified,)), None, None, ())
        state = engine.observe(event, result, pipeline)
        record = self.hypothesis(state, BULLISH_CONTINUATION)
        self.assertEqual(record.state, HypothesisState.INVALIDATED)
        self.assertTrue(record.invalidated_by_evidence_ids)

    def test_expiration_and_explicit_advance_are_replay_stable(self) -> None:
        events = (
            self.trade("t1", 1, 1, "100.00", AggressorSide.SELL),
            self.trade("t2", 2, 2, "99.75", AggressorSide.SELL),
            self.trade("t3", 3, 3, "99.50", AggressorSide.SELL),
        )
        snapshots = []
        for _ in range(2):
            pipeline, engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config())
            DeterministicHypothesisReplay(pipeline, engine).replay(events)
            snapshots.append(engine.advance("2026-08-19T14:30:20Z", pipeline))
        self.assertEqual(snapshots[0].snapshot_hash, snapshots[1].snapshot_hash)
        self.assertTrue(any(item.usability is EvidenceUsability.EXPIRED for item in snapshots[0].evidence))
        self.assertTrue(all(item.state is HypothesisState.EXPIRED for item in snapshots[0].hypotheses))

    def test_recovering_incomplete_and_invalid_data_cannot_sustain_evidence(self) -> None:
        incomplete_pipeline, incomplete_engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config())
        state = self.apply(incomplete_pipeline, incomplete_engine, self.snapshot("unsequenced-snapshot", None))
        self.assertEqual(state.rejected_observations[-1].source_quality, DataQuality.INCOMPLETE)
        pipeline, engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, self.config())
        for event in (
            self.snapshot(),
            self.delta("remove-1", 2, 1, BookSide.BID, DepthOperation.REMOVE, "100.00"),
            self.delta("restore-1", 3, 2, BookSide.BID, DepthOperation.UPSERT, "100.00", 10),
            self.delta("remove-2", 4, 3, BookSide.BID, DepthOperation.REMOVE, "100.00"),
            self.delta("restore-2", 5, 4, BookSide.BID, DepthOperation.UPSERT, "100.00", 10),
        ):
            state = self.apply(pipeline, engine, event)
        bid_replenishment = next(item for item in state.evidence if item.evidence.kind is EvidenceKind.BID_REPLENISHMENT)
        self.assertIs(bid_replenishment.usability, EvidenceUsability.AUTHORITATIVE)
        pipeline.notify_reconnect()
        state = engine.advance("2026-08-19T14:30:05Z", pipeline)
        bid_replenishment = next(item for item in state.evidence if item.evidence.kind is EvidenceKind.BID_REPLENISHMENT)
        self.assertIs(bid_replenishment.usability, EvidenceUsability.UNUSABLE)
        self.assertEqual(bid_replenishment.deactivation_reason.value, "SOURCE_RECOVERING")
        pipeline.book.mark_invalid()
        state = engine.advance("2026-08-19T14:30:06Z", pipeline)
        self.assertEqual(pipeline.book._state().quality, DataQuality.INVALID)
        self.assertFalse(any(item.evidence.family is EvidenceFamily.RESTING_LIQUIDITY and item.usability is EvidenceUsability.AUTHORITATIVE for item in state.evidence))

    def test_high_rate_replay_is_bounded_and_has_no_silent_loss(self) -> None:
        config = self.config(maximum_evidence_objects=16, maximum_history=16, flow_window_events=4, structural_window_events=4, minimum_flow_events=3)
        events = tuple(
            self.trade(f"burst-{sequence}", sequence, sequence, "100.00" if sequence % 2 else "99.75", AggressorSide.SELL if sequence % 2 else AggressorSide.BUY)
            for sequence in range(1, 1001)
        )
        pipeline, engine = MarketDataPipeline(self.source, self.instrument), HypothesisEngine(self.source, self.instrument, config)
        report = DeterministicHypothesisReplay(pipeline, engine).replay(events)
        self.assertEqual(report.events_processed, 1000)
        self.assertEqual(report.snapshots[-1].metrics.observations_processed, 1000)
        self.assertLessEqual(report.snapshots[-1].metrics.retained_evidence_objects, 16)
        self.assertLessEqual(report.snapshots[-1].metrics.retained_hypothesis_records, 16)

    def test_l3c_source_has_no_disallowed_authority_dependencies_or_public_methods(self) -> None:
        source_path = ROOT / "src" / "lane_iii" / "hypothesis_engine.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
            elif isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
        self.assertFalse([name for name in imported if name.startswith(("src.phase_e", "src.lane_ii", "src.copytrade", "requests", "websockets"))])
        public = {name.lower() for name in dir(HypothesisEngine) if not name.startswith("_")}
        self.assertFalse(public & {"submit", "order", "broker", "account", "position", "size", "risk", "execute", "intent"})
        self.assertNotIn("phase_e", source_path.read_text(encoding="utf-8").lower())

    def test_confidence_is_not_a_probability_contract(self) -> None:
        assessment = ConfidenceAssessment("2026-08-19T14:30:00Z", Decimal("0.50"), ())
        self.assertEqual(assessment.relative_support, Decimal("0.50"))
        self.assertFalse(hasattr(assessment, "probability"))
        self.assertFalse(hasattr(assessment, "win_rate"))

    def test_configuration_hash_binds_the_frozen_constitution(self) -> None:
        config = self.config()
        self.assertEqual(L3C_L3B_CONSTITUTION.manifest_hash, L3C_L3A_AUTHORITY_MANIFEST_HASH)
        self.assertIn("l3a_authority_manifest_hash", config.payload())
        self.assertNotEqual(config.configuration_hash, self.config(flow_lifetime=timedelta(seconds=9)).configuration_hash)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
