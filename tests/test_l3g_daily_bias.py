from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import unittest

from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.daily_bias import (
    BiasConfidenceTier,
    BiasInputProvenance,
    BiasUpdateReason,
    ClassifiedSide,
    ClassifiedTradeInput,
    CompletedHourlyOrderFlow,
    CoreComponentEvidence,
    CoreComponentKind,
    DailyBiasResult,
    DailyBiasDirection,
    DailyBiasEngine,
    HourlyBiasFrame,
    HourlyOrderFlowEvaluator,
    OptionsDayClass,
    OptionsOverlayEvidence,
    OptionsOverlayOutcome,
    PONG_DAILY_BIAS_PRODUCTION_CONFIGURED,
    default_unconfigured_bias,
    unconfigured_component,
)


START = "2026-09-11T00:00:00Z"
END = "2026-09-11T01:00:00Z"
DECISION = "2026-09-11T01:00:02Z"
VALID = "2026-09-11T02:05:00Z"


def provenance(
    name: str,
    *,
    start: str = START,
    end: str = END,
    event_at: str = "2026-09-11T00:59:59Z",
    observed_at: str = "2026-09-11T01:00:00Z",
    available_at: str = DECISION,
    valid_until: str = VALID,
) -> BiasInputProvenance:
    calculation = {"name": name, "version": "test-v0"}
    source = {"name": name, "event_at": event_at}
    return BiasInputProvenance(
        source="DETERMINISTIC_TEST_FIXTURE",
        calculation_id=f"test-{name}",
        calculation_version="test-v0",
        calculation_hash=canonical_hash(calculation),
        window_start=start,
        window_end=end,
        latest_event_at=event_at,
        observed_at=observed_at,
        available_at=available_at,
        valid_until=valid_until,
        input_ids=(f"{name}-input",),
        input_hashes=(canonical_hash(source),),
    )


def component(
    kind: CoreComponentKind,
    direction: DailyBiasDirection,
    *,
    configured: bool = True,
    valid: bool = True,
    veto: bool = False,
    provenance_value: BiasInputProvenance | None = None,
) -> CoreComponentEvidence:
    return CoreComponentEvidence(
        kind,
        direction,
        configured,
        valid,
        veto,
        (f"TEST_{kind.value}_{direction.value}",),
        provenance_value or provenance(kind.value.lower()),
    )


def options(
    outcome: OptionsOverlayOutcome = OptionsOverlayOutcome.NOT_APPLICABLE,
    *,
    direction: DailyBiasDirection | None = None,
    valid_until: str = VALID,
) -> OptionsOverlayEvidence:
    ordinary = outcome is OptionsOverlayOutcome.NOT_APPLICABLE
    unavailable = outcome is OptionsOverlayOutcome.UNAVAILABLE
    return OptionsOverlayEvidence(
        day_class=(
            OptionsDayClass.ORDINARY
            if ordinary
            else OptionsDayClass.UNKNOWN
            if unavailable
            else OptionsDayClass.MATERIAL_EXPIRY
        ),
        outcome=outcome,
        direction=direction,
        required=not ordinary,
        reason_codes=(f"TEST_OPTIONS_{outcome.value}",),
        provenance=provenance("options", valid_until=valid_until),
    )


def frame(
    directions: tuple[DailyBiasDirection, DailyBiasDirection, DailyBiasDirection],
    *,
    overlay: OptionsOverlayEvidence | None = None,
    components: tuple[CoreComponentEvidence, ...] | None = None,
    start: str = START,
    end: str = END,
    decision: str = DECISION,
    continuity_valid: bool = True,
    prior_result_id: str | None = None,
) -> HourlyBiasFrame:
    return HourlyBiasFrame(
        "MNQ SEP26",
        start,
        end,
        decision,
        components or tuple(
            component(kind, direction)
            for kind, direction in zip(CoreComponentKind, directions)
        ),
        overlay or options(),
        continuity_valid,
        prior_result_id,
    )


def result(
    directions: tuple[DailyBiasDirection, DailyBiasDirection, DailyBiasDirection],
    *,
    overlay: OptionsOverlayEvidence | None = None,
    components: tuple[CoreComponentEvidence, ...] | None = None,
) -> DailyBiasResult:
    return DailyBiasEngine().update(
        frame(directions, overlay=overlay, components=components),
        BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
    ).result


class DailyBiasEngineTests(unittest.TestCase):
    def test_three_and_two_of_three_long(self) -> None:
        three = result((DailyBiasDirection.LONG,) * 3)
        two = result((
            DailyBiasDirection.LONG,
            DailyBiasDirection.LONG,
            DailyBiasDirection.SHORT,
        ))
        self.assertEqual(three.direction, DailyBiasDirection.LONG)
        self.assertEqual(three.confidence, BiasConfidenceTier.CORE_3_OF_3)
        self.assertEqual(two.direction, DailyBiasDirection.LONG)
        self.assertEqual(two.confidence, BiasConfidenceTier.CORE_2_OF_3)

    def test_three_and_two_of_three_short(self) -> None:
        three = result((DailyBiasDirection.SHORT,) * 3)
        two = result((
            DailyBiasDirection.SHORT,
            DailyBiasDirection.LONG,
            DailyBiasDirection.SHORT,
        ))
        self.assertEqual(three.direction, DailyBiasDirection.SHORT)
        self.assertEqual(three.confidence, BiasConfidenceTier.CORE_3_OF_3)
        self.assertEqual(two.direction, DailyBiasDirection.SHORT)
        self.assertEqual(two.confidence, BiasConfidenceTier.CORE_2_OF_3)

    def test_split_vote_is_neutral(self) -> None:
        value = result((
            DailyBiasDirection.LONG,
            DailyBiasDirection.SHORT,
            DailyBiasDirection.NEUTRAL,
        ))
        self.assertEqual(value.direction, DailyBiasDirection.NEUTRAL)
        self.assertIn("CORE_NO_TWO_OF_THREE_AGREEMENT", value.reason_codes)

    def test_stale_or_missing_required_core_is_unavailable(self) -> None:
        stale_components = tuple(
            component(
                kind,
                DailyBiasDirection.LONG,
                provenance_value=provenance(
                    kind.value.lower(),
                    available_at="2026-09-11T01:00:01Z",
                    valid_until="2026-09-11T01:00:01.500000Z",
                ) if kind is CoreComponentKind.HOURLY_CRT else None,
            )
            for kind in CoreComponentKind
        )
        stale = result((DailyBiasDirection.LONG,) * 3, components=stale_components)
        missing_components = tuple(
            unconfigured_component(
                kind,
                boundary_start=START,
                boundary_end=END,
                available_at=DECISION,
            ) if kind is CoreComponentKind.HOURLY_TREND else component(
                kind, DailyBiasDirection.LONG,
            )
            for kind in CoreComponentKind
        )
        missing = result((DailyBiasDirection.LONG,) * 3, components=missing_components)
        self.assertEqual(stale.direction, DailyBiasDirection.UNAVAILABLE)
        self.assertIn("CORE_COMPONENT_STALE_OR_UNAVAILABLE", stale.reason_codes)
        self.assertEqual(missing.direction, DailyBiasDirection.UNAVAILABLE)
        self.assertFalse(missing.configured)
        self.assertEqual(
            missing.eligibility(at=DECISION, direction="LONG"),
            "DAILY_BIAS_UNCONFIGURED",
        )

    def test_core_veto_forces_neutral(self) -> None:
        components = tuple(
            component(kind, DailyBiasDirection.LONG, veto=kind is CoreComponentKind.HOURLY_CRT)
            for kind in CoreComponentKind
        )
        value = result((DailyBiasDirection.LONG,) * 3, components=components)
        self.assertEqual(value.direction, DailyBiasDirection.NEUTRAL)
        self.assertIn("CORE_STRONG_CONTRARY_VETO", value.reason_codes)

    def test_bias_updates_only_on_completed_boundary_or_explicit_exception(self) -> None:
        engine = DailyBiasEngine()
        first = engine.update(
            frame((DailyBiasDirection.LONG,) * 3),
            BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
        )
        intrahour = frame(
            (DailyBiasDirection.SHORT,) * 3,
            prior_result_id=first.result.result_id,
        )
        denied = engine.update(intrahour, BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY)
        self.assertFalse(denied.changed)
        self.assertEqual(denied.reason_code, "INTRA_HOUR_BIAS_UPDATE_DENIED")
        self.assertIs(denied.result, first.result)

    def test_no_lookahead_in_event_window_or_availability(self) -> None:
        future_window = tuple(
            component(
                kind,
                DailyBiasDirection.LONG,
                provenance_value=provenance(
                    kind.value.lower(),
                    end="2026-09-11T01:01:00Z",
                    event_at="2026-09-11T01:00:30Z",
                    observed_at="2026-09-11T01:00:31Z",
                    available_at="2026-09-11T01:00:32Z",
                ),
            ) if kind is CoreComponentKind.HOURLY_TREND else component(
                kind, DailyBiasDirection.LONG,
            )
            for kind in CoreComponentKind
        )
        with self.assertRaisesRegex(ValueError, "after the completed boundary"):
            frame((DailyBiasDirection.LONG,) * 3, components=future_window)

        late = tuple(
            component(
                kind,
                DailyBiasDirection.LONG,
                provenance_value=provenance(
                    kind.value.lower(), available_at="2026-09-11T01:00:03Z",
                ),
            ) if kind is CoreComponentKind.HOURLY_CRT else component(
                kind, DailyBiasDirection.LONG,
            )
            for kind in CoreComponentKind
        )
        with self.assertRaisesRegex(ValueError, "not causally available"):
            frame((DailyBiasDirection.LONG,) * 3, components=late)

    def test_options_not_applicable_leaves_core_vote_unchanged(self) -> None:
        value = result((
            DailyBiasDirection.LONG,
            DailyBiasDirection.LONG,
            DailyBiasDirection.SHORT,
        ))
        self.assertEqual(value.direction, DailyBiasDirection.LONG)
        self.assertEqual(value.confidence, BiasConfidenceTier.CORE_2_OF_3)
        self.assertIn("OPTIONS_NOT_APPLICABLE", value.reason_codes)

    def test_options_confirm_raises_confidence_but_cannot_create_direction(self) -> None:
        confirmed = result(
            (DailyBiasDirection.LONG, DailyBiasDirection.LONG, DailyBiasDirection.SHORT),
            overlay=options(
                OptionsOverlayOutcome.CONFIRM,
                direction=DailyBiasDirection.LONG,
            ),
        )
        split = result(
            (DailyBiasDirection.LONG, DailyBiasDirection.SHORT, DailyBiasDirection.NEUTRAL),
            overlay=options(
                OptionsOverlayOutcome.CONFIRM,
                direction=DailyBiasDirection.LONG,
            ),
        )
        self.assertEqual(confirmed.direction, DailyBiasDirection.LONG)
        self.assertEqual(
            confirmed.confidence,
            BiasConfidenceTier.CORE_2_OF_3_OPTIONS_CONFIRMED,
        )
        self.assertEqual(split.direction, DailyBiasDirection.NEUTRAL)

    def test_options_caution_denies_two_of_three_and_reduces_three_of_three(self) -> None:
        caution = options(OptionsOverlayOutcome.CAUTION)
        marginal = result(
            (DailyBiasDirection.LONG, DailyBiasDirection.LONG, DailyBiasDirection.SHORT),
            overlay=caution,
        )
        unanimous = result((DailyBiasDirection.LONG,) * 3, overlay=caution)
        self.assertEqual(marginal.direction, DailyBiasDirection.NEUTRAL)
        self.assertEqual(
            unanimous.confidence,
            BiasConfidenceTier.CORE_3_OF_3_OPTIONS_CAUTION,
        )

    def test_options_veto_denies_new_direction(self) -> None:
        value = result(
            (DailyBiasDirection.SHORT,) * 3,
            overlay=options(OptionsOverlayOutcome.VETO),
        )
        self.assertEqual(value.direction, DailyBiasDirection.NEUTRAL)
        self.assertIn("OPTIONS_VETO", value.reason_codes)

    def test_required_options_unavailable_fails_conservatively(self) -> None:
        value = result(
            (DailyBiasDirection.LONG,) * 3,
            overlay=options(OptionsOverlayOutcome.UNAVAILABLE),
        )
        self.assertEqual(value.direction, DailyBiasDirection.UNAVAILABLE)
        self.assertIn("REQUIRED_OPTIONS_CONTEXT_UNAVAILABLE", value.reason_codes)

    def test_options_overlay_may_change_inside_hour_only_with_identical_core(self) -> None:
        engine = DailyBiasEngine()
        first_frame = frame((DailyBiasDirection.LONG,) * 3)
        first = engine.update(first_frame, BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY)
        changed_frame = HourlyBiasFrame(
            first_frame.instrument,
            first_frame.boundary_start,
            first_frame.boundary_end,
            "2026-09-11T01:00:03Z",
            first_frame.components,
            options(OptionsOverlayOutcome.VETO),
            True,
            first.result.result_id,
        )
        changed = engine.update(changed_frame, BiasUpdateReason.OPTIONS_OVERLAY_CHANGE)
        self.assertTrue(changed.changed)
        self.assertEqual(changed.result.direction, DailyBiasDirection.NEUTRAL)

        mutated_core = frame(
            (DailyBiasDirection.SHORT,) * 3,
            overlay=options(OptionsOverlayOutcome.CONFIRM, direction=DailyBiasDirection.SHORT),
            decision="2026-09-11T01:00:04Z",
            prior_result_id=changed.result.result_id,
        )
        denied = engine.update(mutated_core, BiasUpdateReason.OPTIONS_OVERLAY_CHANGE)
        self.assertFalse(denied.changed)
        self.assertEqual(denied.reason_code, "OPTIONS_UPDATE_CHANGED_CORE_EVIDENCE")

    def test_continuity_gap_produces_unavailable_not_invented_bias(self) -> None:
        engine = DailyBiasEngine()
        first = engine.update(
            frame((DailyBiasDirection.LONG,) * 3),
            BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
        ).result
        gap_components = tuple(
            component(
                kind,
                DailyBiasDirection.SHORT,
                provenance_value=provenance(
                    kind.value.lower(),
                    start="2026-09-11T02:00:00Z",
                    end="2026-09-11T03:00:00Z",
                    event_at="2026-09-11T02:59:59Z",
                    observed_at="2026-09-11T03:00:00Z",
                    available_at="2026-09-11T03:00:02Z",
                    valid_until="2026-09-11T04:05:00Z",
                ),
            )
            for kind in CoreComponentKind
        )
        gap_options = OptionsOverlayEvidence(
            OptionsDayClass.ORDINARY,
            OptionsOverlayOutcome.NOT_APPLICABLE,
            None,
            False,
            ("TEST_OPTIONS_NOT_APPLICABLE",),
            provenance(
                "options-gap",
                start="2026-09-11T02:00:00Z",
                end="2026-09-11T03:00:00Z",
                event_at="2026-09-11T02:59:59Z",
                observed_at="2026-09-11T03:00:00Z",
                available_at="2026-09-11T03:00:02Z",
                valid_until="2026-09-11T04:05:00Z",
            ),
        )
        gap = engine.update(
            frame(
                (DailyBiasDirection.SHORT,) * 3,
                components=gap_components,
                overlay=gap_options,
                start="2026-09-11T02:00:00Z",
                end="2026-09-11T03:00:00Z",
                decision="2026-09-11T03:00:02Z",
                prior_result_id=first.result_id,
            ),
            BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
        )
        self.assertTrue(gap.changed)
        self.assertEqual(gap.result.direction, DailyBiasDirection.UNAVAILABLE)
        self.assertIn("HOURLY_BIAS_CONTINUITY_GAP", gap.result.reason_codes)

    def test_default_is_provenance_complete_and_unconfigured(self) -> None:
        value = default_unconfigured_bias()
        self.assertFalse(value.configured)
        self.assertEqual(value.direction, DailyBiasDirection.UNAVAILABLE)
        self.assertEqual(len(value.components), 3)
        self.assertTrue(all(item.provenance.input_hashes for item in value.components))
        self.assertEqual(value.eligibility(at=DECISION, direction="LONG"), "DAILY_BIAS_UNCONFIGURED")

    def test_result_identity_and_payload_are_deterministic_and_replayable(self) -> None:
        first = result((DailyBiasDirection.LONG,) * 3)
        second = result((DailyBiasDirection.LONG,) * 3)
        self.assertEqual(first.result_id, second.result_id)
        self.assertEqual(first.result_hash, second.result_hash)
        self.assertEqual(first.payload(), second.payload())
        self.assertEqual(first.payload()["vote_counts"]["LONG"], 3)
        self.assertEqual(len(first.payload()["components"]), 3)


class HourlyOrderFlowTests(unittest.TestCase):
    @staticmethod
    def trades(*, buy: int, sell: int) -> tuple[ClassifiedTradeInput, ...]:
        base = datetime(2026, 9, 11, tzinfo=timezone.utc)
        values: list[ClassifiedTradeInput] = []
        for index, side in enumerate(
            (ClassifiedSide.BUY,) * buy + (ClassifiedSide.SELL,) * sell,
            start=1,
        ):
            event = base + timedelta(minutes=index * 3)
            event_text = event.isoformat().replace("+00:00", "Z")
            observed = (event + timedelta(milliseconds=5)).isoformat().replace("+00:00", "Z")
            available = (event + timedelta(milliseconds=10)).isoformat().replace("+00:00", "Z")
            values.append(ClassifiedTradeInput(
                event_id=f"classified-{index}",
                payload_hash=canonical_hash({"index": index, "side": side.value}),
                source="NINJATRADER_AUTHENTICATED_CLASSIFICATION",
                instrument="MNQ SEP26",
                side=side,
                size=Decimal("2") if side is ClassifiedSide.BUY else Decimal("1"),
                event_at=event_text,
                observed_at=observed,
                available_at=available,
                authenticated=True,
            ))
        return tuple(values)

    def evaluate(self, trades: tuple[ClassifiedTradeInput, ...], *, continuity: bool = True) -> CoreComponentEvidence:
        return HourlyOrderFlowEvaluator().evaluate(CompletedHourlyOrderFlow(
            START,
            END,
            "2026-09-11T01:00:10Z",
            START,
            END,
            continuity,
            trades,
        ))

    def test_completed_hour_order_flow_is_directional_or_neutral_by_signed_volume(self) -> None:
        long = self.evaluate(self.trades(buy=6, sell=2))
        short_inputs = tuple(
            ClassifiedTradeInput(
                event_id=item.event_id,
                payload_hash=item.payload_hash,
                source=item.source,
                instrument=item.instrument,
                side=(ClassifiedSide.SELL if item.side is ClassifiedSide.BUY else ClassifiedSide.BUY),
                size=item.size,
                event_at=item.event_at,
                observed_at=item.observed_at,
                available_at=item.available_at,
                authenticated=True,
            )
            for item in self.trades(buy=6, sell=2)
        )
        short = self.evaluate(short_inputs)
        neutral_inputs = tuple(
            ClassifiedTradeInput(
                event_id=item.event_id,
                payload_hash=item.payload_hash,
                source=item.source,
                instrument=item.instrument,
                side=item.side,
                size=Decimal("1"),
                event_at=item.event_at,
                observed_at=item.observed_at,
                available_at=item.available_at,
                authenticated=True,
            )
            for item in self.trades(buy=4, sell=4)
        )
        neutral = self.evaluate(neutral_inputs)
        self.assertEqual(long.direction, DailyBiasDirection.LONG)
        self.assertEqual(short.direction, DailyBiasDirection.SHORT)
        self.assertEqual(neutral.direction, DailyBiasDirection.NEUTRAL)

    def test_minimum_evidence_and_continuity_fail_closed(self) -> None:
        insufficient = self.evaluate(self.trades(buy=5, sell=2))
        discontinuous = self.evaluate(self.trades(buy=6, sell=2), continuity=False)
        self.assertEqual(insufficient.direction, DailyBiasDirection.UNAVAILABLE)
        self.assertFalse(insufficient.valid)
        self.assertEqual(discontinuous.direction, DailyBiasDirection.UNAVAILABLE)
        self.assertFalse(discontinuous.valid)

    def test_future_or_unauthenticated_flow_is_rejected(self) -> None:
        values = list(self.trades(buy=6, sell=2))
        item = values[0]
        values[0] = ClassifiedTradeInput(
            item.event_id,
            item.payload_hash,
            item.source,
            item.instrument,
            item.side,
            item.size,
            item.event_at,
            item.observed_at,
            item.available_at,
            False,
        )
        with self.assertRaisesRegex(ValueError, "authenticated"):
            self.evaluate(tuple(values))


class DailyBiasAuthorityBoundaryTests(unittest.TestCase):
    def test_daily_bias_and_pong_are_not_registered_or_order_capable(self) -> None:
        root = Path(__file__).resolve().parents[1]
        daily_source = (root / "src" / "l3g_paper" / "daily_bias.py").read_text(encoding="utf-8")
        pong_source = (root / "src" / "l3g_paper" / "pong.py").read_text(encoding="utf-8")
        registration_sources = "\n".join(
            (root / path).read_text(encoding="utf-8")
            for path in (
                "src/l3g_paper/contracts.py",
                "src/copytrade/control_center.py",
                "src/l3g_paper/runtime.py",
            )
        )
        for forbidden in (
            "PaperExecutionTransport",
            "LaneIIIPaperRuntime",
            "NinjaTraderSim101PaperAdapter",
            "submit_order",
            "operational_paper_start",
        ):
            self.assertNotIn(forbidden, daily_source)
            self.assertNotIn(forbidden, pong_source)
        # The elevated one-hour authority now registers a separate experimental
        # operational wrapper. The pure voter/machine still own no transport.
        from src.l3g_paper.contracts import resolve_paper_profile
        trial = resolve_paper_profile("BEELZEBUB_PONG_DISCERNMENT_V1")
        self.assertTrue(trial.policy.trial_required)
        self.assertEqual(trial.policy.duration_seconds, 3600)
        self.assertEqual(trial.risk.maximum_entry_quantity, 1)
        self.assertFalse(trial.risk.approved_for_live)
        self.assertFalse(PONG_DAILY_BIAS_PRODUCTION_CONFIGURED)


if __name__ == "__main__":
    unittest.main()
