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
        self.assertGreater(self.fhb.status()["movement_reserve_observations"], 0)

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
