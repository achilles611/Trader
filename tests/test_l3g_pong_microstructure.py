from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.pong_microstructure import (
    AggressorProvenance,
    AggressorSide,
    BookSide,
    CausalEventRef,
    ClassifiedFlowAdapter,
    ClassifiedTrade,
    DepthEventKind,
    DepthObservation,
    FlowBehavior,
    LiquidityChangeKind,
    LiquidityClusterAdapter,
    PriceResponse,
    ReductionAttribution,
    VolumeRegime,
    VolumeRegimeAdapter,
    VolumeWindow,
    VolumeWindowKind,
)


BASE = datetime(2026, 9, 10, 14, 0, tzinfo=timezone.utc)


def stamp(seconds: int, *, base: datetime = BASE) -> str:
    return (base + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def event(sequence: int, *, available_offset: int | None = None) -> CausalEventRef:
    available = sequence if available_offset is None else available_offset
    payload = {"sequence": sequence, "available": available}
    return CausalEventRef(
        f"event-{sequence}", canonical_hash(payload), stamp(sequence), stamp(sequence),
        stamp(available), sequence, "LOCAL_CALLBACK_ORDER", "test-continuity",
    )


def trade(sequence: int, side: AggressorSide, price: str, volume: str = "1") -> ClassifiedTrade:
    return ClassifiedTrade(
        event(sequence), "MNQ SEP26", Decimal(price), Decimal(volume), side,
        AggressorProvenance.UNAVAILABLE
        if side is AggressorSide.UNKNOWN
        else AggressorProvenance.AUTHENTICATED_PROVIDER,
    )


def depth(
    sequence: int,
    side: BookSide,
    quantity: str,
    *,
    matched: str = "0",
    interaction: str = "0",
    cancellation: bool = False,
    kind: DepthEventKind = DepthEventKind.UPDATE,
    response: PriceResponse = PriceResponse.UNAVAILABLE,
) -> DepthObservation:
    price = Decimal("100")
    return DepthObservation(
        event(sequence), "MNQ SEP26", side, price, Decimal(quantity), Decimal("200"),
        price, kind, Decimal(matched), Decimal(interaction), cancellation, response,
    )


def volume_window(
    name: str,
    start: datetime,
    volume: str,
    *,
    kind: VolumeWindowKind = VolumeWindowKind.COMPLETED,
    continuity: bool = True,
    fresh: bool = True,
    available_at: datetime | None = None,
) -> VolumeWindow:
    end = start + timedelta(minutes=5)
    available = available_at or end
    payload = {"name": name, "start": start.isoformat(), "volume": volume}
    return VolumeWindow(
        name, canonical_hash(payload), "UTC-14:00-5M", start.isoformat(), end.isoformat(),
        available.isoformat(), Decimal(volume), kind, continuity, fresh,
    )


class ClassifiedFlowAdapterTests(unittest.TestCase):
    def test_buy_and_sell_progress_are_mirrored(self) -> None:
        buy = ClassifiedFlowAdapter().evaluate(
            (trade(1, AggressorSide.BUY, "100"), trade(2, AggressorSide.BUY, "100"),
             trade(3, AggressorSide.BUY, "100.25"), trade(4, AggressorSide.BUY, "100.25")),
            window_start=stamp(0), as_of=stamp(4), feed_continuity_valid=True,
        )
        sell = ClassifiedFlowAdapter().evaluate(
            (trade(1, AggressorSide.SELL, "100"), trade(2, AggressorSide.SELL, "100"),
             trade(3, AggressorSide.SELL, "99.75"), trade(4, AggressorSide.SELL, "99.75")),
            window_start=stamp(0), as_of=stamp(4), feed_continuity_valid=True,
        )
        self.assertEqual(buy.behavior, FlowBehavior.BUY_PROGRESS)
        self.assertEqual(sell.behavior, FlowBehavior.SELL_PROGRESS)
        self.assertEqual(buy.imbalance, -sell.imbalance)
        self.assertEqual(buy.classified_coverage, Decimal("1"))

    def test_buy_and_sell_absorption_are_mirrored(self) -> None:
        buy = ClassifiedFlowAdapter().evaluate(
            tuple(trade(index, AggressorSide.BUY, "100") for index in range(1, 5)),
            window_start=stamp(0), as_of=stamp(4), feed_continuity_valid=True,
        )
        sell = ClassifiedFlowAdapter().evaluate(
            tuple(trade(index, AggressorSide.SELL, "100") for index in range(1, 5)),
            window_start=stamp(0), as_of=stamp(4), feed_continuity_valid=True,
        )
        self.assertEqual(buy.behavior, FlowBehavior.BUY_ABSORBED)
        self.assertEqual(sell.behavior, FlowBehavior.SELL_ABSORBED)

    def test_unknown_volume_reduces_coverage_and_sparse_flow_is_unavailable(self) -> None:
        value = ClassifiedFlowAdapter().evaluate(
            (
                trade(1, AggressorSide.BUY, "100", "1"),
                trade(2, AggressorSide.BUY, "100.25", "1"),
                trade(3, AggressorSide.UNKNOWN, "100.25", "8"),
            ),
            window_start=stamp(0), as_of=stamp(3), feed_continuity_valid=True,
        )
        self.assertFalse(value.valid)
        self.assertEqual(value.unknown_volume, Decimal("8"))
        self.assertEqual(value.classified_coverage, Decimal("0.2"))
        self.assertEqual(value.behavior, FlowBehavior.UNAVAILABLE)
        self.assertIn("CLASSIFIED_FLOW_LOW_CLASSIFIED_COVERAGE", value.reason_codes)
        self.assertIn("CLASSIFIED_FLOW_SPARSE_COUNT", value.reason_codes)

    def test_order_is_deterministic_and_future_available_data_is_refused(self) -> None:
        values = tuple(trade(index, AggressorSide.BUY, str(100 + index / 4)) for index in range(1, 5))
        left = ClassifiedFlowAdapter().evaluate(
            values, window_start=stamp(0), as_of=stamp(4), feed_continuity_valid=True,
        )
        right = ClassifiedFlowAdapter().evaluate(
            reversed(values), window_start=stamp(0), as_of=stamp(4), feed_continuity_valid=True,
        )
        self.assertEqual(left.result_hash, right.result_hash)
        future = ClassifiedTrade(
            event(5, available_offset=10), "MNQ SEP26", Decimal("101"), Decimal("1"),
            AggressorSide.BUY, AggressorProvenance.AUTHENTICATED_PROVIDER,
        )
        with self.assertRaisesRegex(ValueError, "Future-available"):
            ClassifiedFlowAdapter().evaluate(
                (*values, future), window_start=stamp(0), as_of=stamp(5),
                feed_continuity_valid=True,
            )


class LiquidityClusterAdapterTests(unittest.TestCase):
    def test_bid_and_ask_clusters_retain_interactions_depletion_and_replenishment(self) -> None:
        for side in (BookSide.BID, BookSide.ASK):
            result = LiquidityClusterAdapter().evaluate(
                (
                    depth(1, side, "100"),
                    depth(2, side, "70", matched="30", interaction="30", response=PriceResponse.REJECTION),
                    depth(3, side, "90", interaction="5", response=PriceResponse.ACCEPTANCE),
                ),
                as_of=stamp(3), feed_continuity_valid=True,
            )
            self.assertTrue(result.valid)
            self.assertEqual(result.relative_size, Decimal("0.45"))
            self.assertEqual(result.persistence_seconds, Decimal("2"))
            self.assertEqual(result.interaction_count, 2)
            self.assertEqual(result.interaction_volume, Decimal("35"))
            self.assertEqual(result.depleted_quantity, Decimal("30"))
            self.assertEqual(result.replenished_quantity, Decimal("20"))
            self.assertEqual(result.acceptance_count, 1)
            self.assertEqual(result.rejection_count, 1)
            self.assertEqual(result.transitions[0].kind, LiquidityChangeKind.REDUCE)
            self.assertEqual(
                result.transitions[0].reduction_attribution,
                ReductionAttribution.TRADE_MATCHED_CONSUMPTION,
            )

    def test_cancellation_requires_support_and_range_loss_remains_ambiguous(self) -> None:
        supported = LiquidityClusterAdapter().evaluate(
            (depth(1, BookSide.BID, "100"), depth(2, BookSide.BID, "70", cancellation=True)),
            as_of=stamp(2), feed_continuity_valid=True,
        )
        ambiguous = LiquidityClusterAdapter().evaluate(
            (depth(1, BookSide.ASK, "100"), depth(2, BookSide.ASK, "0", kind=DepthEventKind.OUT_OF_RANGE)),
            as_of=stamp(2), feed_continuity_valid=True,
        )
        self.assertEqual(
            supported.transitions[0].reduction_attribution,
            ReductionAttribution.SUPPORTED_CANCELLATION,
        )
        self.assertEqual(
            ambiguous.transitions[0].reduction_attribution,
            ReductionAttribution.AMBIGUOUS,
        )
        self.assertFalse(ambiguous.valid)
        self.assertIn("LIQUIDITY_CLUSTER_RANGE_OR_RESET_AMBIGUOUS", ambiguous.reason_codes)

    def test_silent_time_does_not_increase_persistence(self) -> None:
        observations = (depth(1, BookSide.BID, "100"), depth(2, BookSide.BID, "100"))
        recent = LiquidityClusterAdapter().evaluate(
            observations, as_of=stamp(2), feed_continuity_valid=True,
        )
        stale = LiquidityClusterAdapter().evaluate(
            observations, as_of=stamp(30), feed_continuity_valid=True,
        )
        self.assertEqual(recent.persistence_seconds, stale.persistence_seconds)
        self.assertFalse(stale.valid)
        self.assertIn("LIQUIDITY_CLUSTER_STALE", stale.reason_codes)


class VolumeRegimeAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.current_start = BASE
        self.prior = tuple(
            volume_window(f"prior-{days}", BASE - timedelta(days=days), "100")
            for days in (3, 2, 1)
        )

    def test_prior_only_low_normal_and_high(self) -> None:
        adapter = VolumeRegimeAdapter()
        for amount, expected in (("50", VolumeRegime.LOW), ("100", VolumeRegime.NORMAL), ("150", VolumeRegime.HIGH)):
            current = volume_window(
                f"current-{amount}", self.current_start, amount, kind=VolumeWindowKind.AS_OF,
            )
            result = adapter.evaluate(current, self.prior, as_of=current.available_at)
            self.assertTrue(result.valid)
            self.assertEqual(result.regime, expected)
            self.assertEqual(result.prior_median_volume, Decimal("100"))

    def test_broken_feed_is_not_low_volume(self) -> None:
        current = volume_window(
            "current-broken", self.current_start, "1", kind=VolumeWindowKind.AS_OF,
            continuity=False,
        )
        result = VolumeRegimeAdapter().evaluate(current, self.prior, as_of=current.available_at)
        self.assertEqual(result.regime, VolumeRegime.UNAVAILABLE)
        self.assertIn("VOLUME_REGIME_BROKEN_FEED", result.reason_codes)

    def test_insufficient_history_and_future_reference_are_explicit(self) -> None:
        current = volume_window(
            "current", self.current_start, "50", kind=VolumeWindowKind.AS_OF,
        )
        insufficient = VolumeRegimeAdapter().evaluate(
            current, self.prior[:2], as_of=current.available_at,
        )
        self.assertEqual(insufficient.regime, VolumeRegime.UNAVAILABLE)
        self.assertIn("VOLUME_REGIME_INSUFFICIENT_PRIOR_HISTORY", insufficient.reason_codes)
        future_known = volume_window(
            "prior-late", BASE - timedelta(days=4), "100",
            available_at=BASE + timedelta(seconds=1),
        )
        with self.assertRaisesRegex(ValueError, "not available before"):
            VolumeRegimeAdapter().evaluate(
                current, (*self.prior, future_known), as_of=current.available_at,
            )


if __name__ == "__main__":
    unittest.main()
