from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
import unittest

from src.l3f_provider.ninjatrader_observation import expected_observer_source_fingerprint
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION
from src.l3g_paper.fhb import (
    FHBAction, FHBConfig, FocusedHorizonBand,
    FHB_ACTUAL_OFFERED_SERVICE_POLICY_ID,
    FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_ID,
    _mnq_price_ticks,
)
from src.l3g_paper.pong_microstructure import BookSide, CausalEventRef
from src.l3g_paper.pong_production import PongProductionHost
from src.l3g_paper.pong_scheduler import PongSchedulingConfig, PongSchedulingMode
from src.copytrade.config import CopyTradeConfig
from src.copytrade.control_center import create_control_center_app
from tests.test_l3g_pong_inputs import verified
from tests.test_l3g_pong_production import pending_collection, runtime_snapshot


class ControllableClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def wire(at: datetime) -> str:
    return at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def event(sequence: int, at: datetime) -> CausalEventRef:
    return CausalEventRef(
        f"fhb-{sequence}",
        f"{sequence:064x}",
        wire(at),
        wire(at),
        wire(at),
        sequence,
        "LOCAL_CALLBACK_ORDER_ONLY",
        "TEST-FHB",
    )


def levels(start: Decimal, count: int, step: Decimal) -> list[dict[str, object]]:
    return [
        {"price": str(start + step * index), "size": index + 1}
        for index in range(count)
    ]


def positions(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {"position": index, "price": row["price"], "size": row["size"]}
        for index, row in enumerate(rows)
    ]


def with_positions(value: dict[str, object]) -> dict[str, object]:
    value["position_bids"] = positions(value["bids"])  # type: ignore[arg-type]
    value["position_asks"] = positions(value["asks"])  # type: ignore[arg-type]
    return value


def payload(
    *,
    sequence: int,
    side: str = "Bid",
    mutation_price: str = "100.00",
    mutation_volume: int = 1,
    operation: str = "Update",
    bids: list[dict[str, object]] | None = None,
    asks: list[dict[str, object]] | None = None,
    generation: int = 1,
    reset: bool = False,
) -> dict[str, object]:
    return {
        "contract_id": "MNQ SEP26",
        "operation": operation,
        "side": side,
        "mutation_price": mutation_price,
        "mutation_volume": mutation_volume,
        "mutation_position": 0,
        "is_reset": reset,
        "snapshot_ordering": "PRICE_CANONICAL_BEST_TO_WORST_V1",
        "bids": bids or levels(Decimal("100.00"), 10, Decimal("-0.25")),
        "asks": asks or levels(Decimal("100.25"), 10, Decimal("0.25")),
        "source_generation": generation,
        "source_event_sequence": sequence,
        "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
    }


class FocusedHorizonBandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fhb = FocusedHorizonBand()
        self.start = datetime(2026, 9, 16, 18, 0, tzinfo=timezone.utc)

    def test_exact_five_second_cutoff_and_same_timestamp_order_are_inclusive(self) -> None:
        first = self.fhb.process_depth(payload(sequence=1), event(1, self.start))
        self.assertIs(first.action, FHBAction.REWARM_REQUIRED)
        second_at = self.start + timedelta(seconds=5)
        second = self.fhb.process_depth(
            payload(sequence=2, mutation_volume=2), event(2, second_at),
        )
        self.assertIs(second.action, FHBAction.EVALUATE)
        self.assertEqual(
            [item.event.sequence for item in second.seed_observations],
            [1, 2],
        )
        self.assertEqual(self.fhb.status()["last_rewarm_seconds"], 5.0)
        self.assertEqual(self.fhb.status()["completed_rewarms"], 1)
        third = self.fhb.process_depth(
            payload(sequence=3, mutation_volume=3), event(3, second_at),
        )
        self.assertIs(third.action, FHBAction.EVALUATE)
        self.assertEqual([item.event.sequence for item in third.seed_observations], [3])

    def test_option3_binds_actual_offered_service_without_a_rate_floor(self) -> None:
        config = FHBConfig.option_3()
        payload = config.payload()
        self.assertEqual(
            payload["qualification_policy_id"],
            FHB_ACTUAL_OFFERED_SERVICE_POLICY_ID,
        )
        self.assertEqual(
            config.actual_offered_service_policy_payload()["minimum_observed_event_rate"],
            "NONE",
        )
        self.assertEqual(FHBConfig.from_payload(payload), config)

    def test_periodic_profile_binds_sim_actual_offered_service_without_rate_floor(self) -> None:
        config = FHBConfig.periodic_protected()
        policy = config.actual_offered_service_policy_payload()
        self.assertEqual(
            config.qualification_policy_id,
            FHB_PERIODIC_ACTUAL_OFFERED_SERVICE_POLICY_ID,
        )
        self.assertEqual(policy["destination"], "Sim101/LOCAL_SIMULATION")
        self.assertEqual(policy["minimum_observed_event_rate"], "NONE")
        self.assertEqual(FHBConfig.from_payload(config.payload()), config)

    def test_signed_policy_is_exact_top10_five_seconds_and_32_ticks(self) -> None:
        config = FHBConfig()
        self.assertEqual(config.core_levels_per_side, 10)
        self.assertEqual(config.shadow_history_seconds, 5)
        self.assertEqual(config.movement_reserve_ticks, 32)
        self.assertEqual(
            FHBConfig.from_payload(config.payload()).policy_fingerprint,
            config.policy_fingerprint,
        )
        self.assertEqual(len(config.policy_fingerprint), 64)
        with self.assertRaisesRegex(ValueError, "FHB_CORE_WIDTH_UNSUPPORTED"):
            FHBConfig(core_levels_per_side=20)
        with self.assertRaisesRegex(ValueError, "FHB_MOVEMENT_RESERVE_UNSUPPORTED"):
            FHBConfig(movement_reserve_ticks=64)

    def test_exact_price_tick_conversion_cache_is_bounded_and_semantic(self) -> None:
        _mnq_price_ticks.cache_clear()
        self.assertEqual(_mnq_price_ticks("30111.50"), 120446)
        self.assertEqual(_mnq_price_ticks("30111.50"), 120446)
        info = _mnq_price_ticks.cache_info()
        self.assertEqual(info.maxsize, 8192)
        self.assertEqual(info.misses, 1)
        self.assertEqual(info.hits, 1)
        with self.assertRaisesRegex(ValueError, "FHB_PRICE_NOT_ON_TICK_GRID"):
            _mnq_price_ticks("30111.51")
        with self.assertRaisesRegex(ValueError, "FHB_PRICE_INVALID"):
            _mnq_price_ticks("0")

    def test_signed_bid_and_ask_prefix_membership_not_nearest_price(self) -> None:
        gapped_asks = levels(Decimal("100.25"), 1, Decimal("0.25")) + levels(
            Decimal("120.00"), 9, Decimal("1.00"),
        )
        first_payload = payload(
            sequence=1,
            side="Ask",
            mutation_price="120.00",
            asks=gapped_asks,
        )
        self.fhb.process_depth(first_payload, event(1, self.start))
        result = self.fhb.process_depth(
            payload(
                sequence=2,
                side="Ask",
                mutation_price="120.00",
                asks=gapped_asks,
            ),
            event(2, self.start + timedelta(seconds=5)),
        )
        self.assertIs(result.action, FHBAction.EVALUATE)
        self.assertTrue(result.post_core_member)
        status = self.fhb.status()
        self.assertGreater(status["movement_reserve_observations"], 0)
        self.assertEqual(status["movement_guard"]["reserve_ticks"], 32)
        self.assertEqual(
            status["movement_guard"]["mode"],
            "DIAGNOSTIC_ONLY_ALL_AUTHENTIC_INPUT_RETAINED",
        )

    def test_out_of_core_mutation_is_shadowed_and_intentionally_skipped(self) -> None:
        self.fhb.process_depth(
            payload(sequence=1, mutation_price="97.00"), event(1, self.start),
        )
        result = self.fhb.process_depth(
            payload(sequence=2, mutation_price="97.00"),
            event(2, self.start + timedelta(seconds=5)),
        )
        self.assertIs(result.action, FHBAction.SKIP_OUT_OF_CORE)
        self.assertFalse(result.pre_core_member)
        self.assertFalse(result.post_core_member)
        self.assertEqual(self.fhb.status()["out_of_core_skips"], 1)

    def test_removal_uses_pre_core_and_promoted_price_requires_complete_shadow(self) -> None:
        self.fhb.process_depth(
            payload(sequence=1, mutation_price="97.50"), event(1, self.start),
        )
        promoted_book = levels(Decimal("99.75"), 10, Decimal("-0.25"))
        result = self.fhb.process_depth(
            payload(
                sequence=2,
                operation="Remove",
                mutation_price="100.00",
                mutation_volume=0,
                bids=promoted_book,
            ),
            event(2, self.start + timedelta(seconds=5)),
        )
        self.assertIs(result.action, FHBAction.EVALUATE)
        self.assertTrue(result.pre_core_member)
        self.assertFalse(result.post_core_member)
        self.assertIn(390, result.promoted_price_ticks)  # 97.50 / 0.25

        cold = FocusedHorizonBand()
        cold.process_depth(payload(sequence=1), event(1, self.start))
        blocked = cold.process_depth(
            payload(
                sequence=2,
                operation="Remove",
                mutation_price="100.00",
                mutation_volume=0,
                bids=promoted_book,
            ),
            event(2, self.start + timedelta(seconds=5)),
        )
        self.assertIs(blocked.action, FHBAction.REWARM_REQUIRED)
        self.assertIn("BID:390", cold.status()["rewarm_prices"])

    def test_reset_generation_change_and_capacity_loss_are_visible(self) -> None:
        self.fhb.process_depth(payload(sequence=1), event(1, self.start))
        reset = self.fhb.process_depth(
            payload(sequence=2, reset=True), event(2, self.start + timedelta(seconds=6)),
        )
        self.assertIs(reset.action, FHBAction.REWARM_REQUIRED)
        self.assertTrue(reset.clear_feature_windows)
        generation = self.fhb.process_depth(
            payload(sequence=3, generation=2), event(3, self.start + timedelta(seconds=7)),
        )
        self.assertTrue(generation.clear_feature_windows)
        self.assertEqual(self.fhb.status()["source_generation"], 2)

        bounded = FocusedHorizonBand(FHBConfig(maximum_shadow_prices_per_side=20))
        bounded.process_depth(payload(sequence=1), event(1, self.start))
        for index in range(11):
            bounded.process_depth(
                payload(sequence=2 + index, mutation_price=str(Decimal("90") - index)),
                event(2 + index, self.start + timedelta(milliseconds=index + 1)),
            )
        self.assertGreater(bounded.status()["capacity_losses"], 0)

    def test_unavailable_and_crossed_books_invalidate_without_fallback(self) -> None:
        unavailable_payload = payload(sequence=1)
        unavailable_payload.update({
            "status": "UNAVAILABLE",
            "reason": "CROSSED_NATIVE_POSITIONAL_SNAPSHOT",
            "side": None,
            "mutation_side": "Bid",
            "bids": [],
            "asks": [],
        })
        unavailable = self.fhb.process_depth(
            unavailable_payload,
            event(1, self.start),
        )
        self.assertIs(unavailable.action, FHBAction.REWARM_REQUIRED)
        self.assertTrue(unavailable.clear_feature_windows)
        status = self.fhb.status()
        self.assertEqual(status["maintenance_ticks"], 1)
        self.assertEqual(status["last_source_sequence"], 1)
        self.assertEqual(
            status["unavailable_reason"],
            "FHB_SOURCE_UNAVAILABLE:CROSSED_NATIVE_POSITIONAL_SNAPSHOT",
        )
        self.assertFalse(status["entry_ready"])

        crossed = payload(
            sequence=2,
            bids=levels(Decimal("100.50"), 10, Decimal("-0.25")),
            asks=levels(Decimal("100.25"), 10, Decimal("0.25")),
        )
        rejected = self.fhb.process_depth(
            crossed,
            event(2, self.start + timedelta(seconds=1)),
        )
        self.assertIs(rejected.action, FHBAction.REWARM_REQUIRED)
        self.assertEqual(
            self.fhb.status()["unavailable_reason"],
            "FHB_SIGNED_BOOK_CROSSED",
        )

        partial = payload(
            sequence=3,
            bids=levels(Decimal("100.00"), 9, Decimal("-0.25")),
        )
        rejected = self.fhb.process_depth(
            partial,
            event(3, self.start + timedelta(seconds=2)),
        )
        self.assertIs(rejected.action, FHBAction.REWARM_REQUIRED)
        self.assertTrue(rejected.clear_feature_windows)
        self.assertEqual(
            self.fhb.status()["unavailable_reason"],
            "FHB_POSITION_CURSOR_REQUIRED_FOR_INCOMPLETE_CORE",
        )

    def test_dense_prefix_seams_are_rejected_without_sparse_position_contract(self) -> None:
        self.fhb.process_depth(payload(sequence=1), event(1, self.start))
        warmed_at = self.start + timedelta(seconds=5)
        self.assertIs(
            self.fhb.process_depth(payload(sequence=2), event(2, warmed_at)).action,
            FHBAction.EVALUATE,
        )
        resets_before = self.fhb.status()["reset_count"]

        crossed_remove = payload(
            sequence=3,
            operation="Remove",
            mutation_price="0",
            mutation_volume=0,
            bids=[],
            asks=[],
        )
        crossed_remove["status"] = "UNAVAILABLE"
        crossed_remove["reason"] = "CROSSED_NATIVE_POSITIONAL_SNAPSHOT"
        crossed_remove["bids"] = []
        crossed_remove["asks"] = []
        first = self.fhb.process_depth(
            crossed_remove,
            event(3, warmed_at + timedelta(milliseconds=1)),
        )
        self.assertIs(first.action, FHBAction.REWARM_REQUIRED)
        self.assertTrue(first.clear_feature_windows)
        self.assertFalse(self.fhb.status()["coherence_pending"])
        self.assertEqual(len(self.fhb.status()["signed_core_ticks"]["BID"]), 0)
        self.assertFalse(self.fhb.entry_ready)

        crossed_add = payload(sequence=4, operation="Add", bids=[], asks=[])
        crossed_add["status"] = "UNAVAILABLE"
        crossed_add["reason"] = "CROSSED_NATIVE_POSITIONAL_SNAPSHOT"
        crossed_add["bids"] = []
        crossed_add["asks"] = []
        second = self.fhb.process_depth(
            crossed_add,
            event(4, warmed_at + timedelta(milliseconds=2)),
        )
        self.assertIs(second.action, FHBAction.REWARM_REQUIRED)
        self.assertEqual(len(self.fhb.status()["signed_core_ticks"]["BID"]), 0)

        partial_asks = levels(Decimal("100.50"), 9, Decimal("0.25"))
        partial_remove = payload(
            sequence=5,
            side="Ask",
            operation="Remove",
            mutation_price="0",
            mutation_volume=0,
            asks=partial_asks,
        )
        third = self.fhb.process_depth(
            partial_remove,
            event(5, warmed_at + timedelta(milliseconds=3)),
        )
        self.assertIs(third.action, FHBAction.REWARM_REQUIRED)
        self.assertEqual(len(self.fhb.status()["signed_core_ticks"]["ASK"]), 0)

        reconciled = self.fhb.process_depth(
            payload(sequence=6, side="Ask", operation="Add", mutation_price="100.25"),
            event(6, warmed_at + timedelta(milliseconds=4)),
        )
        status = self.fhb.status()
        self.assertIs(reconciled.action, FHBAction.REWARM_REQUIRED)
        self.assertFalse(status["entry_ready"])
        self.assertFalse(status["coherence_pending"])
        self.assertEqual(status["crossed_unavailable_records"], 0)
        self.assertEqual(status["partial_signed_records"], 0)
        self.assertEqual(status["coherence_reconciliations"], 0)
        self.assertEqual(status["coherence_reconciliation_failures"], 0)
        self.assertGreater(status["reset_count"], resets_before)

    def test_positional_seam_ambiguity_invalidates_fail_closed(self) -> None:
        self.fhb.process_depth(payload(sequence=1), event(1, self.start))
        ambiguous = payload(
            sequence=2,
            operation="Add",
            mutation_price="90.00",
            bids=[],
            asks=[],
        )
        ambiguous["mutation_position"] = 12
        ambiguous["status"] = "UNAVAILABLE"
        ambiguous["reason"] = "CROSSED_NATIVE_POSITIONAL_SNAPSHOT"
        ambiguous["bids"] = []
        ambiguous["asks"] = []
        refused = self.fhb.process_depth(
            ambiguous,
            event(2, self.start + timedelta(milliseconds=1)),
        )
        self.assertIs(refused.action, FHBAction.REWARM_REQUIRED)
        status = self.fhb.status()
        self.assertFalse(status["coherence_pending"])
        self.assertEqual(
            status["unavailable_reason"],
            "FHB_SOURCE_UNAVAILABLE:CROSSED_NATIVE_POSITIONAL_SNAPSHOT",
        )
        self.assertEqual(status["signed_core_ticks"], {"BID": [], "ASK": []})

    def test_signed_working_prefix_recovers_without_treating_cross_as_available(self) -> None:
        self.fhb.process_depth(with_positions(payload(sequence=1)), event(1, self.start))
        warmed_at = self.start + timedelta(seconds=5)
        self.fhb.process_depth(with_positions(payload(sequence=2)), event(2, warmed_at))
        crossed = payload(
            sequence=3,
            operation="Add",
            mutation_price="100.25",
            mutation_volume=2,
            bids=[
                {"price": "100.25", "size": 2},
                *levels(Decimal("100.00"), 9, Decimal("-0.25")),
            ],
        )
        crossed.update({
            "status": "UNAVAILABLE",
            "reason": "CROSSED_NATIVE_POSITIONAL_SNAPSHOT",
            "mutation_resolved_price": "100.25",
        })
        prior_bids = positions(levels(Decimal("100.00"), 10, Decimal("-0.25")))
        crossed["position_bids"] = [
            {"position": 0, "price": "100.25", "size": 2},
            *[
                {**row, "position": int(row["position"]) + 1}
                for row in prior_bids
            ],
        ]
        crossed["position_asks"] = positions(crossed["asks"])  # type: ignore[arg-type]
        held = self.fhb.process_depth(
            crossed,
            event(3, warmed_at + timedelta(milliseconds=1)),
        )
        self.assertIs(held.action, FHBAction.REWARM_REQUIRED)
        self.assertFalse(self.fhb.entry_ready)
        self.assertTrue(self.fhb.status()["coherence_pending"])
        self.assertEqual(
            self.fhb.status()["signed_core_ticks"]["BID"][0], 401,
        )

        recovered = with_positions(payload(
            sequence=4,
            operation="Remove",
            mutation_price="0",
            mutation_volume=0,
        ))
        recovered["mutation_resolved_price"] = "100.25"
        decision = self.fhb.process_depth(
            recovered,
            event(4, warmed_at + timedelta(milliseconds=2)),
        )
        status = self.fhb.status()
        self.assertIs(decision.action, FHBAction.EVALUATE)
        self.assertTrue(decision.clear_feature_windows)
        self.assertFalse(status["coherence_pending"])
        self.assertEqual(status["coherence_reconciliations"], 1)
        self.assertEqual(status["reset_count"], 0)

        ordinary_bid = payload(sequence=5, mutation_volume=4)
        ordinary_bid["bids"][0]["size"] = 4  # type: ignore[index]
        self.fhb.process_depth(
            ordinary_bid,
            event(5, warmed_at + timedelta(milliseconds=3)),
        )
        ordinary_ask = payload(
            sequence=6,
            side="Ask",
            mutation_price="100.25",
            mutation_volume=5,
        )
        ordinary_ask["bids"][0]["size"] = 4  # type: ignore[index]
        ordinary_ask["asks"][0]["size"] = 5  # type: ignore[index]
        self.fhb.process_depth(
            ordinary_ask,
            event(6, warmed_at + timedelta(milliseconds=4)),
        )

        prior_bid_rows = levels(Decimal("100.00"), 10, Decimal("-0.25"))
        prior_bid_rows[0]["size"] = 4
        second_cross = payload(
            sequence=7,
            operation="Add",
            mutation_price="100.25",
            mutation_volume=2,
            bids=[
                {"price": "100.25", "size": 2},
                *prior_bid_rows[:9],
            ],
        )
        second_cross.update({
            "status": "UNAVAILABLE",
            "reason": "CROSSED_NATIVE_POSITIONAL_SNAPSHOT",
            "mutation_resolved_price": "100.25",
        })
        prior = positions(prior_bid_rows)
        second_cross["position_bids"] = [
            {"position": 0, "price": "100.25", "size": 2},
            *[
                {**row, "position": int(row["position"]) + 1}
                for row in prior
            ],
        ]
        second_cross["position_asks"] = positions(second_cross["asks"])  # type: ignore[arg-type]
        second_cross["position_asks"][0]["size"] = 5  # type: ignore[index]
        second_cross["asks"][0]["size"] = 5  # type: ignore[index]
        held_again = self.fhb.process_depth(
            second_cross,
            event(7, warmed_at + timedelta(milliseconds=5)),
        )
        second_status = self.fhb.status()
        self.assertIs(held_again.action, FHBAction.REWARM_REQUIRED)
        self.assertTrue(second_status["coherence_pending"])
        self.assertEqual(second_status["crossed_unavailable_records"], 2)
        self.assertEqual(second_status["coherence_reconciliation_failures"], 0)

    def test_sparse_authentic_rebuild_shape_keeps_hidden_positions_exact(self) -> None:
        bid_rows = levels(Decimal("100.00"), 10, Decimal("-0.25"))
        ask_rows = levels(Decimal("100.25"), 11, Decimal("0.25"))

        def signed(
            sequence: int,
            current_asks: list[dict[str, object]],
            *,
            operation: str,
            position: int,
            price: str,
            volume: int,
        ) -> dict[str, object]:
            ask_prefix: list[dict[str, object]] = []
            for row in current_asks[:10]:
                if ask_prefix and Decimal(str(ask_prefix[-1]["price"])) >= Decimal(str(row["price"])):
                    break
                ask_prefix.append(dict(row))
            value = payload(
                sequence=sequence,
                side="Ask",
                operation=operation,
                mutation_price=price,
                mutation_volume=volume,
                bids=bid_rows,
                asks=ask_prefix,
            )
            value["mutation_position"] = position
            value["mutation_resolved_price"] = price
            value["position_bids"] = positions(bid_rows)
            value["position_asks"] = positions(current_asks)
            return value

        initial = with_positions(payload(sequence=1, bids=bid_rows, asks=ask_rows[:10]))
        initial["position_asks"] = positions(ask_rows)
        self.fhb.process_depth(initial, event(1, self.start))
        warm = with_positions(payload(sequence=2, bids=bid_rows, asks=ask_rows[:10]))
        warm["position_asks"] = positions(ask_rows)
        self.fhb.process_depth(warm, event(2, self.start + timedelta(seconds=5)))
        resets_before = self.fhb.status()["reset_count"]

        after_remove = [dict(row) for row in ask_rows[1:]]
        coherent_remove = signed(
            3, after_remove, operation="Remove", position=0,
            price="100.25", volume=0,
        )
        self.fhb.process_depth(
            coherent_remove,
            event(3, self.start + timedelta(seconds=5, milliseconds=1)),
        )

        after_duplicate_add = [
            *after_remove[:2],
            {"price": "100.75", "size": 7},
            *after_remove[2:],
        ]
        partial_two = signed(
            4, after_duplicate_add, operation="Add", position=2,
            price="100.75", volume=7,
        )
        held_two = self.fhb.process_depth(
            partial_two,
            event(4, self.start + timedelta(seconds=5, milliseconds=2)),
        )
        self.assertIs(held_two.action, FHBAction.REWARM_REQUIRED)
        self.assertEqual(len(self.fhb.status()["signed_core_ticks"]["ASK"]), 2)

        after_front_remove = [dict(row) for row in after_duplicate_add[1:]]
        partial_one = signed(
            5, after_front_remove, operation="Remove", position=0,
            price="100.50", volume=0,
        )
        held_one = self.fhb.process_depth(
            partial_one,
            event(5, self.start + timedelta(seconds=5, milliseconds=3)),
        )
        self.assertIs(held_one.action, FHBAction.REWARM_REQUIRED)
        status = self.fhb.status()
        self.assertTrue(status["coherence_pending"])
        self.assertEqual(len(status["signed_core_ticks"]["ASK"]), 1)
        self.assertEqual(status["partial_signed_records"], 2)
        self.assertEqual(status["coherence_reconciliation_failures"], 0)
        self.assertEqual(status["reset_count"], resets_before)

    def test_zero_price_positional_remove_uses_coherent_prior_core_identity(self) -> None:
        self.fhb.process_depth(
            payload(sequence=1, mutation_price="97.50"),
            event(1, self.start),
        )
        result = self.fhb.process_depth(
            payload(
                sequence=2,
                operation="Remove",
                mutation_price="0",
                mutation_volume=0,
                bids=levels(Decimal("99.75"), 10, Decimal("-0.25")),
            ),
            event(2, self.start + timedelta(seconds=5)),
        )
        self.assertIs(result.action, FHBAction.EVALUATE)
        self.assertTrue(result.pre_core_member)
        self.assertFalse(result.post_core_member)
        self.assertEqual(result.price_ticks, 400)
        self.assertEqual(
            [item.price for item in result.seed_observations],
            [Decimal("100.00")],
        )

    def test_zero_price_remove_without_coherent_prior_position_fails_closed(self) -> None:
        result = self.fhb.process_depth(
            payload(
                sequence=1,
                operation="Remove",
                mutation_price="0",
                mutation_volume=0,
            ),
            event(1, self.start),
        )
        self.assertIs(result.action, FHBAction.REWARM_REQUIRED)
        self.assertTrue(result.clear_feature_windows)
        self.assertEqual(
            self.fhb.status()["unavailable_reason"],
            "FHB_MUTATION_IDENTITY_UNRESOLVED",
        )

    def test_zero_price_deep_remove_skips_only_when_signed_core_is_unchanged(self) -> None:
        self.fhb.process_depth(payload(sequence=1), event(1, self.start))
        deep_remove = payload(
            sequence=2,
            operation="Remove",
            mutation_price="0",
            mutation_volume=0,
        )
        deep_remove["mutation_position"] = 23
        result = self.fhb.process_depth(
            deep_remove,
            event(2, self.start + timedelta(seconds=5)),
        )
        self.assertIs(result.action, FHBAction.SKIP_OUT_OF_CORE)
        self.assertIsNone(result.price_ticks)
        self.assertFalse(result.clear_feature_windows)
        self.assertTrue(self.fhb.status()["entry_ready"])

        changed = payload(
            sequence=3,
            operation="Remove",
            mutation_price="0",
            mutation_volume=0,
            bids=levels(Decimal("99.75"), 10, Decimal("-0.25")),
        )
        changed["mutation_position"] = 25
        blocked = self.fhb.process_depth(
            changed,
            event(3, self.start + timedelta(seconds=6)),
        )
        self.assertIs(blocked.action, FHBAction.REWARM_REQUIRED)
        self.assertTrue(blocked.clear_feature_windows)
        self.assertEqual(
            self.fhb.status()["unavailable_reason"],
            "FHB_MUTATION_IDENTITY_UNRESOLVED",
        )


class FHBProductionIntegrationTests(unittest.TestCase):
    @staticmethod
    def _host(root: str, config: FHBConfig, clock: ControllableClock) -> PongProductionHost:
        return PongProductionHost(
            root,
            selected_profile=PONG_ENTRY_PROFILE_VERSION,
            expected_market_source_fingerprint=expected_observer_source_fingerprint(),
            runtime_snapshot=runtime_snapshot,
            collector_status=pending_collection,
            controller_dispatch=lambda _kind, _reason: True,
            native_dispatch=lambda directive, _evidence: {
                "accepted": True,
                "command_id": None,
                "directive_id": directive.directive_id,
            },
            fhb_config=config,
            monotonic_clock=clock,
        )

    def test_live_host_selects_fhb_and_skips_only_out_of_core_depth_evaluation(self) -> None:
        with TemporaryDirectory(prefix="fhb-production-") as root:
            host = PongProductionHost(
                Path(root),
                selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection,
                controller_dispatch=lambda _kind, _reason: True,
                native_dispatch=lambda directive, _evidence: {
                    "accepted": True,
                    "command_id": None,
                    "directive_id": directive.directive_id,
                },
                fhb_config=FHBConfig(),
            )
            with patch.object(host, "_evaluate_microstructure") as evaluate:
                host._consume_observation(verified(
                    "DEPTH", payload(sequence=1), local_sequence=1,
                    observed=wire(self_time := datetime(2026, 9, 16, 18, 0, tzinfo=timezone.utc)),
                ))
                host._consume_observation(verified(
                    "DEPTH", payload(sequence=2, mutation_price="97.00"), local_sequence=2,
                    observed=wire(self_time + timedelta(seconds=5)),
                ))
                self.assertEqual(evaluate.call_count, 0)
                host._consume_observation(verified(
                    "DEPTH", payload(sequence=3), local_sequence=3,
                    observed=wire(self_time + timedelta(seconds=6)),
                ))
                self.assertEqual(evaluate.call_count, 1)
            status = host.status()
            self.assertEqual(
                status["fhb"]["configuration"]["profile_id"],
                "TOP10_CORE_WITH_5S_SHADOW_V1",
            )
            self.assertEqual(status["fhb"]["out_of_core_skips"], 1)
            self.assertEqual(status["compute_workers"], 0)
            activation = status["fhb_activation"]
            self.assertEqual(
                activation["requested_profile"],
                "TOP10_CORE_WITH_5S_SHADOW_V1",
            )
            self.assertTrue(activation["admitted"])
            self.assertTrue(activation["instantiated"])
            self.assertTrue(activation["ready"])
            self.assertEqual(activation["maintenance_ticks"], 3)
            self.assertEqual(activation["last_maintained_source_sequence"], 3)
            self.assertEqual(activation["last_output_source_sequence"], 3)
            self.assertFalse(activation["consumed"])
            self.assertEqual(activation["consumer_count"], 0)
            self.assertEqual(activation["fallback"]["mode"], "NONE")
            self.assertFalse(
                activation["fallback"]["full_depth_fallback_allowed"],
            )
            self.assertEqual(
                activation["effective_policy_fingerprint"],
                status["fhb"]["effective_policy_fingerprint"],
            )

    def test_real_strategy_consumers_read_fhb_outputs_and_report_last_sequence(self) -> None:
        with TemporaryDirectory(prefix="fhb-consumer-") as root:
            clock = ControllableClock()
            host = self._host(root, FHBConfig(), clock)
            start = datetime(2026, 9, 16, 18, 0, tzinfo=timezone.utc)
            depth_records = (
                (1, "Bid", "100.00", 0.0),
                (2, "Ask", "100.25", 0.1),
                (3, "Bid", "100.00", 5.0),
                (4, "Ask", "100.25", 5.1),
            )
            for sequence, side, price, seconds in depth_records:
                host._consume_observation(verified(
                    "DEPTH",
                    payload(
                        sequence=sequence,
                        side=side,
                        mutation_price=price,
                    ),
                    local_sequence=sequence,
                    observed=wire(start + timedelta(seconds=seconds)),
                ))
            host._consume_observation(verified(
                "TRADE",
                {
                    "contract_id": "MNQ SEP26",
                    "price": 100,
                    "size": 1,
                    "aggressor_side": "BUY",
                    "aggressor_source": "NATIVE_BID_ASK",
                    "source_event_sequence": 1,
                    "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
                },
                local_sequence=5,
                observed=wire(start + timedelta(seconds=5.2)),
            ))
            activation = host.status()["fhb_activation"]
            self.assertTrue(activation["maintained"])
            self.assertTrue(activation["consumed"])
            self.assertEqual(activation["consumer_count"], 1)
            self.assertEqual(activation["last_consumer_source_sequence"], 4)
            self.assertEqual(activation["output_batches"], 2)
            self.assertGreaterEqual(activation["output_observations"], 4)

    def test_requested_profile_must_match_an_admitted_effective_policy(self) -> None:
        with TemporaryDirectory(prefix="fhb-request-binding-") as root:
            with self.assertRaisesRegex(ValueError, "FHB_REQUEST_NOT_ADMITTED"):
                PongProductionHost(
                    Path(root),
                    selected_profile=PONG_ENTRY_PROFILE_VERSION,
                    expected_market_source_fingerprint=(
                        expected_observer_source_fingerprint()
                    ),
                    runtime_snapshot=runtime_snapshot,
                    collector_status=pending_collection,
                    controller_dispatch=lambda _kind, _reason: True,
                    native_dispatch=lambda _directive, _evidence: {
                        "accepted": True,
                    },
                    fhb_requested_profile="TOP10_CORE_WITH_5S_SHADOW_V1",
                )

    def test_activation_readiness_inherits_elapsed_depth_heartbeat_timeout(self) -> None:
        with TemporaryDirectory(prefix="fhb-heartbeat-") as root:
            clock = ControllableClock()
            host = self._host(root, FHBConfig(), clock)
            start = datetime(2026, 9, 16, 18, 0, tzinfo=timezone.utc)
            host._consume_observation(verified(
                "DEPTH", payload(sequence=1), local_sequence=1,
                observed=wire(start),
            ))
            host._consume_observation(verified(
                "DEPTH", payload(sequence=2), local_sequence=2,
                observed=wire(start + timedelta(seconds=5)),
            ))
            self.assertTrue(host.status()["fhb_activation"]["ready"])

            clock.advance(6.0)
            status = host.status()
            self.assertFalse(status["fhb_activation"]["ready"])
            self.assertEqual(
                status["fhb_activation"]["unavailable_reason"],
                "FHB_SOURCE_HEARTBEAT_TIMEOUT",
            )
            self.assertEqual(
                status["fhb_freshness"]["current_heartbeat_blocker"],
                "FHB_SOURCE_HEARTBEAT_TIMEOUT",
            )
            self.assertIn(
                "FHB_SOURCE_HEARTBEAT_TIMEOUT",
                status["blocking_reasons"],
            )

    def test_selected_route_requires_coherent_dec26_topology(self) -> None:
        selected = {
            "USERPROFILE": os.environ["USERPROFILE"],
            "BEELZEBUB_L3G_PAPER_PROFILE": PONG_ENTRY_PROFILE_VERSION,
            "BEELZEBUB_PONG_FHB_PROFILE": "TOP10_CORE_WITH_5S_SHADOW_V1",
            "BEELZEBUB_L3G_PAPER_CONTRACT_TOPOLOGY": "DEC26",
            "BEELZEBUB_PONG_CONTRACT_TOPOLOGY": "DEC26",
            "BEELZEBUB_PONG_SCHEDULING_MODE": "PERIODIC_FLAT_PROTECTED",
            "BEELZEBUB_PONG_PERIODIC_INTERVAL_MS": "20",
        }
        with patch.dict(os.environ, selected, clear=True):
            app = create_control_center_app(CopyTradeConfig())
        self.assertIsNotNone(app)

        mixed = dict(selected)
        mixed["BEELZEBUB_L3G_PAPER_CONTRACT_TOPOLOGY"] = "SEP26"
        with patch.dict(os.environ, mixed, clear=True):
            with self.assertRaisesRegex(
                RuntimeError, "PONG_PAPER_CONTRACT_TOPOLOGY_MISMATCH",
            ):
                create_control_center_app(CopyTradeConfig())

    def test_fhb_refuses_unsuccessful_helper_owner(self) -> None:
        with TemporaryDirectory(prefix="fhb-production-") as root:
            with self.assertRaisesRegex(ValueError, "FHB_SERIAL_ORDERED_OWNER_REQUIRED"):
                PongProductionHost(
                    Path(root),
                    selected_profile=PONG_ENTRY_PROFILE_VERSION,
                    expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                    runtime_snapshot=runtime_snapshot,
                    collector_status=pending_collection,
                    controller_dispatch=lambda _kind, _reason: True,
                    native_dispatch=lambda _directive, _evidence: {"accepted": True},
                    compute_workers=1,
                    fhb_config=FHBConfig(),
                )

    def test_periodic_route_owns_entry_scoring_and_rejects_double_throttle(self) -> None:
        scheduling = PongSchedulingConfig(
            PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
            20,
        )
        with TemporaryDirectory(prefix="fhb-periodic-") as root:
            host = PongProductionHost(
                Path(root),
                selected_profile=PONG_ENTRY_PROFILE_VERSION,
                expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                runtime_snapshot=runtime_snapshot,
                collector_status=pending_collection,
                controller_dispatch=lambda _kind, _reason: True,
                native_dispatch=lambda _directive, _evidence: {"accepted": True},
                fhb_config=FHBConfig(),
                scheduling=scheduling,
            )
            composition = host.status()["evaluation_schedule"]
            self.assertEqual(composition["entry_scoring_owner"], "PONG_PERIODIC_DEADLINE")
            self.assertEqual(composition["fhb_entry_cadence_interval_ms"], 0)
            self.assertFalse(composition["duplicate_entry_throttle"])

            with patch.object(host, "_evaluate_microstructure") as evaluate:
                start = datetime(2026, 9, 20, 22, 0, tzinfo=timezone.utc)
                host._consume_observation(verified(
                    "DEPTH", payload(sequence=1), local_sequence=1,
                    observed=wire(start),
                ))
                host._consume_observation(verified(
                    "DEPTH", payload(sequence=2, mutation_price="97.00"),
                    local_sequence=2,
                    observed=wire(start + timedelta(seconds=5)),
                ))
            self.assertEqual(evaluate.call_count, 2)
            self.assertEqual(
                host.status()["evaluation_schedule"]["active_path"],
                "EVENT_DRIVEN_PROTECTION",
            )
            activation = host.status()["fhb_activation"]
            self.assertEqual(
                activation["entry_evaluation_trigger"],
                "EVENT_DRIVEN_PROTECTION",
            )
            self.assertFalse(activation["event_trigger_reduction_claimed"])

        with TemporaryDirectory(prefix="fhb-double-throttle-") as root:
            with self.assertRaisesRegex(
                ValueError, "FHB_PERIODIC_DUPLICATE_ENTRY_CADENCE",
            ):
                PongProductionHost(
                    Path(root),
                    selected_profile=PONG_ENTRY_PROFILE_VERSION,
                    expected_market_source_fingerprint=expected_observer_source_fingerprint(),
                    runtime_snapshot=runtime_snapshot,
                    collector_status=pending_collection,
                    controller_dispatch=lambda _kind, _reason: True,
                    native_dispatch=lambda _directive, _evidence: {"accepted": True},
                    fhb_config=FHBConfig.option_3(),
                    scheduling=scheduling,
                )

    def test_fhb_freshness_gates_measure_queue_and_source_age_without_dropping_state(self) -> None:
        with TemporaryDirectory(prefix="fhb-freshness-") as root:
            clock = ControllableClock()
            host = self._host(root, FHBConfig(), clock)
            start = datetime(2026, 9, 20, 22, 0, tzinfo=timezone.utc)
            clock.advance(2.0)
            with patch(
                "src.l3g_paper.pong_production._now",
                return_value=wire(start),
            ):
                host._consume_observation(
                    verified(
                        "DEPTH", payload(sequence=1), local_sequence=1,
                        observed=wire(start),
                    ),
                    admitted_monotonic=0.0,
                )
            freshness = host.status()["fhb_freshness"]
            self.assertEqual(freshness["current_blocker"], "FHB_HOST_QUEUE_AGE_EXCEEDED")
            self.assertEqual(freshness["breach_count"], 1)
            self.assertEqual(host.status()["fhb"]["source_depth_records"], 1)

            with patch(
                "src.l3g_paper.pong_production._now",
                return_value=wire(start + timedelta(seconds=2)),
            ):
                host._consume_observation(
                    verified(
                        "DEPTH", payload(sequence=2), local_sequence=2,
                        observed=wire(start),
                    ),
                    admitted_monotonic=clock(),
                )
            freshness = host.status()["fhb_freshness"]
            self.assertEqual(
                freshness["current_blocker"],
                "FHB_EVENT_PROCESSING_AGE_EXCEEDED",
            )
            self.assertEqual(host.status()["fhb"]["source_depth_records"], 2)

            clock.advance(6.0)
            host._consume_observation(
                verified(
                    "QUOTE",
                    {
                        "contract_id": "MNQ SEP26",
                        "bid": 99.75,
                        "ask": 100,
                        "bid_size": 1,
                        "ask_size": 1,
                        "source_event_sequence": 1,
                        "publication_policy": (
                            "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1"
                        ),
                    },
                    local_sequence=3,
                    observed=wire(start + timedelta(seconds=8)),
                ),
                admitted_monotonic=clock(),
            )
            freshness = host.status()["fhb_freshness"]
            self.assertEqual(
                freshness["current_blocker"],
                "FHB_EVENT_PROCESSING_AGE_EXCEEDED",
            )
            self.assertEqual(freshness["heartbeat_stream"], "DEPTH")
            self.assertTrue(freshness["persistent_breach"])
            self.assertGreaterEqual(freshness["current_breach_seconds"], 6.0)

            with patch(
                "src.l3g_paper.pong_production._now",
                return_value=wire(start + timedelta(seconds=8)),
            ):
                host._consume_observation(
                    verified(
                        "DEPTH", payload(sequence=3), local_sequence=4,
                        observed=wire(start + timedelta(seconds=8)),
                    ),
                    admitted_monotonic=clock(),
                )
            freshness = host.status()["fhb_freshness"]
            self.assertIsNone(freshness["current_blocker"])
            self.assertEqual(freshness["recovery_count"], 1)
            self.assertGreaterEqual(freshness["last_breach_seconds"], 6.0)

    def test_option_3_uses_monotonic_start_spacing_without_catch_up(self) -> None:
        with TemporaryDirectory(prefix="fhb-cadence-") as root:
            clock = ControllableClock()
            host = self._host(root, FHBConfig.option_3(), clock)
            start = datetime(2026, 9, 17, 20, 0, tzinfo=timezone.utc)
            starts: list[float] = []
            with patch.object(
                host,
                "_evaluate_microstructure",
                side_effect=lambda _at: starts.append(clock()),
            ):
                for sequence, event_seconds, clock_seconds in (
                    (1, 0.0, 0.0),
                    (2, 5.0, 0.0),
                    (3, 5.1, 0.1),
                    (4, 5.2, 0.149),
                    (5, 5.3, 0.001),
                ):
                    clock.advance(clock_seconds)
                    host._consume_observation(verified(
                        "DEPTH",
                        payload(sequence=sequence),
                        local_sequence=sequence,
                        observed=wire(start + timedelta(seconds=event_seconds)),
                    ))
                clock.advance(5.0)
                self.assertEqual(starts, [0.0, 0.25])
                host._consume_observation(verified(
                    "DEPTH",
                    payload(sequence=6),
                    local_sequence=6,
                    observed=wire(start + timedelta(seconds=10.3)),
                ))
            self.assertEqual(starts, [0.0, 0.25, 5.25])
            pipeline = host.status()["microstructure_pipeline"]
            self.assertEqual(pipeline["entry_evaluation_starts"], 3)
            self.assertEqual(pipeline["entry_evaluation_deferred"], 2)
            self.assertGreaterEqual(
                pipeline["minimum_entry_evaluation_spacing_ms"],
                250.0,
            )
            self.assertEqual(pipeline["applied_state_revision"], 6)
            self.assertEqual(
                pipeline["last_entry_evaluation"]["source_generation"],
                1,
            )

    def test_option_3_preserves_mandatory_fhb_state_and_protection_callbacks(self) -> None:
        with TemporaryDirectory(prefix="fhb-state-parity-") as legacy_root, TemporaryDirectory(
            prefix="fhb-state-parity-cadence-",
        ) as cadence_root:
            legacy_clock = ControllableClock()
            cadence_clock = ControllableClock()
            legacy = self._host(legacy_root, FHBConfig(), legacy_clock)
            cadence = self._host(cadence_root, FHBConfig.option_3(), cadence_clock)
            start = datetime(2026, 9, 17, 20, 0, tzinfo=timezone.utc)
            records = tuple(
                verified(
                    "DEPTH",
                    payload(
                        sequence=sequence,
                        mutation_price="97.00" if sequence == 3 else "100.00",
                    ),
                    local_sequence=sequence,
                    observed=wire(start + timedelta(seconds=offset)),
                )
                for sequence, offset in ((1, 0.0), (2, 5.0), (3, 5.1), (4, 5.2))
            )
            with patch.object(legacy, "_evaluate_microstructure"), patch.object(
                cadence, "_evaluate_microstructure",
            ):
                for record in records:
                    legacy._consume_observation(record)
                    cadence._consume_observation(record)
                    cadence_clock.advance(0.05)
            legacy_status = legacy._fhb.status()
            cadence_status = cadence._fhb.status()
            for value in (legacy_status, cadence_status):
                value.pop("configuration")
                value.pop("effective_policy")
                value.pop("effective_policy_fingerprint")
                value.pop("shadow_maintenance_nanoseconds")
            self.assertEqual(legacy_status, cadence_status)
            self.assertEqual(legacy._stream_sequences, cadence._stream_sequences)
            self.assertEqual(legacy._stream_counts, cadence._stream_counts)
            self.assertEqual(legacy._market_state_revision, cadence._market_state_revision)

            bridge = Mock()
            bridge.on_protection_working.return_value = None
            cadence._bridge = bridge
            with patch.object(
                cadence,
                "_directive_for_message_locked",
                return_value="directive-1",
            ), patch.object(
                cadence,
                "_event_proof_locked",
                return_value=Mock(),
            ):
                cadence.on_execution_message({
                    "message_type": "ORDER_EVENT",
                    "order_role": "PROTECTIVE",
                    "order_state": "WORKING",
                    "quantity": 1,
                })
            bridge.on_protection_working.assert_called_once()


if __name__ == "__main__":
    unittest.main()
