from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import unittest

from src.l3g_paper.discernment import (
    DISCERNMENT_EXECUTION_ENABLED,
    EXPERIMENTAL_DIRECTIONAL_SUPPORT,
    STRONG_THRESHOLD_UNREACHABLE,
    DiscernmentDirection,
    DiscernmentEngine,
    DiscernmentEvidence,
    DiscernmentPolicy,
    DiscernmentValidity,
    VolumeRegime,
)


BASE = datetime(2026, 9, 10, 14, 0, tzinfo=timezone.utc)


def stamp(seconds: int) -> str:
    return (BASE + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def evidence(
    revision: int,
    effect: float,
    *,
    component: str = "flow",
    version: str = "1",
    group: str = "order-flow",
    required: bool = True,
    event_id: str | None = None,
    domain: str = "feed-a",
    continuous: bool = True,
    at: int | None = None,
    source_id: str | None = None,
) -> DiscernmentEvidence:
    second = revision if at is None else at
    source = source_id or f"source-{component}-{second}"
    return DiscernmentEvidence(
        event_id or f"event-{domain}-{revision}-{component}",
        component,
        version,
        group,
        effect,
        required,
        stamp(second),
        stamp(second + 1),
        stamp(second + 2),
        stamp(second + 30),
        revision,
        domain,
        continuous,
        (source,),
        (digest(source),),
    )


def policy(
    *,
    required=(('flow', '1'),),
    bounds=(('order-flow', 4.0),),
) -> DiscernmentPolicy:
    return DiscernmentPolicy.research_fixture(
        required_components=required,
        dependency_group_bounds=bounds,
    )


class DiscernmentCoreTests(unittest.TestCase):
    def test_missing_required_is_unavailable_not_neutral(self) -> None:
        engine = DiscernmentEngine(
            policy(
                required=(("flow", "1"), ("liquidity", "1")),
                bounds=(("order-flow", 4.0), ("book", 4.0)),
            )
        )
        first = engine.on_evidence(evidence(1, 2.0))
        self.assertEqual(first.snapshot.validity, DiscernmentValidity.UNAVAILABLE)
        self.assertIsNone(first.snapshot.raw_long_score)
        self.assertIsNone(first.snapshot.effective_long_score)
        self.assertIsNone(first.snapshot.long_discernment_pct)
        self.assertIn("MISSING_REQUIRED_EVIDENCE:liquidity@1", first.snapshot.reason_codes)

        second = engine.on_evidence(
            evidence(2, 1.0, component="liquidity", group="book", at=3)
        )
        self.assertTrue(second.snapshot.valid)
        self.assertGreater(second.snapshot.raw_long_score, second.snapshot.effective_long_score)
        self.assertEqual(second.snapshot.calibration_status, EXPERIMENTAL_DIRECTIONAL_SUPPORT)
        self.assertFalse(second.snapshot.execution_enabled)
        self.assertFalse(DISCERNMENT_EXECUTION_ENABLED)

    def test_replaces_component_and_bounds_correlated_dependency_group(self) -> None:
        engine = DiscernmentEngine(
            policy(
                required=(("flow", "1"), ("cvd", "1")),
                bounds=(("shared-flow", 1.0),),
            )
        )
        engine.on_evidence(evidence(1, 0.8, group="shared-flow"))
        combined = engine.on_evidence(
            evidence(2, 0.8, component="cvd", group="shared-flow", at=3)
        )
        self.assertEqual(combined.snapshot.dependency_group_effects, (("shared-flow", 1.0),))
        self.assertEqual(len(combined.snapshot.component_effects), 2)

        replaced = engine.on_evidence(
            evidence(3, -0.8, component="flow", group="shared-flow", at=6)
        )
        self.assertEqual(len(replaced.snapshot.component_effects), 2)
        self.assertAlmostEqual(replaced.snapshot.dependency_group_effects[0][1], 0.0)

    def test_identical_rolling_window_does_not_create_decision_opportunity(self) -> None:
        engine = DiscernmentEngine(policy())
        initial = evidence(1, 2.0, source_id="same-window")
        seeded = engine.on_evidence(initial)
        self.assertTrue(seeded.admitted)
        replay_envelope = replace(
            initial,
            event_id="same-window-reprocessed",
            received_at=stamp(5),
            available_at=stamp(6),
            valid_until=stamp(31),
            evidence_revision=2,
        )
        repeated = engine.on_evidence(replay_envelope)
        self.assertFalse(repeated.admitted)
        self.assertIsNone(repeated.switch)
        self.assertEqual(repeated.reason_code, "IDENTICAL_EVIDENCE_NO_DECISION")
        self.assertEqual(repeated.snapshot.component_effects[0].event_id, initial.event_id)

    def test_hysteresis_seed_and_exactly_once_distinct_switches(self) -> None:
        engine = DiscernmentEngine(policy())
        seed = engine.on_evidence(evidence(1, 2.0))
        self.assertEqual(engine.latched_direction, DiscernmentDirection.LONG)
        self.assertIsNone(seed.switch)

        middle = engine.on_evidence(evidence(2, 0.2, at=3))
        self.assertIsNone(middle.switch)
        self.assertEqual(engine.latched_direction, DiscernmentDirection.LONG)

        short = engine.on_evidence(evidence(3, -2.0, at=6))
        self.assertIsNotNone(short.switch)
        assert short.switch is not None
        self.assertEqual(short.switch.sequence, 1)
        self.assertEqual(short.switch.previous_direction, DiscernmentDirection.LONG)
        self.assertEqual(short.switch.direction, DiscernmentDirection.SHORT)
        self.assertEqual(short.switch.event_id, short.switch.switch_id)
        self.assertIsNone(short.switch.prior_switch_id)

        same_side = engine.on_evidence(evidence(4, -4.0, at=9))
        self.assertIsNone(same_side.switch)
        long = engine.on_evidence(evidence(5, 2.0, at=12))
        self.assertIsNotNone(long.switch)
        assert long.switch is not None
        self.assertEqual(long.switch.sequence, 2)
        self.assertEqual(long.switch.prior_switch_id, short.switch.event_id)
        self.assertEqual(long.switch.effective_long_score, long.snapshot.effective_long_score)

        duplicate = engine.on_evidence(evidence(5, 2.0, at=12))
        self.assertEqual(duplicate.reason_code, "DUPLICATE_EVIDENCE_IGNORED")
        self.assertIsNone(duplicate.switch)

    def test_time_expiry_bias_and_fill_callbacks_never_switch(self) -> None:
        engine = DiscernmentEngine(policy())
        engine.on_evidence(evidence(1, 2.0))
        for callback, second in (
            (engine.on_time, 5),
            (engine.on_bias_update, 6),
            (engine.on_fill, 7),
            (engine.on_expiry, 40),
        ):
            update = callback(stamp(second))
            self.assertIsNone(update.switch)
        self.assertEqual(engine.last_snapshot.validity, DiscernmentValidity.UNAVAILABLE)
        self.assertEqual(engine.latched_direction, DiscernmentDirection.LONG)

    def test_continuity_gap_cannot_create_switch_and_new_domain_reseeds(self) -> None:
        engine = DiscernmentEngine(policy())
        engine.on_evidence(evidence(1, 2.0))
        gap = engine.on_evidence(evidence(3, -2.0, at=4))
        self.assertFalse(gap.snapshot.continuity_valid)
        self.assertFalse(gap.snapshot.valid)
        self.assertIsNone(gap.switch)

        stuck = engine.on_evidence(evidence(4, -2.0, at=7))
        self.assertEqual(stuck.reason_code, "CONTINUITY_DOMAIN_REQUIRES_RESEED")
        self.assertIsNone(stuck.switch)

        reseed = engine.on_evidence(evidence(1, -2.0, domain="feed-b", at=10))
        self.assertTrue(reseed.snapshot.valid)
        self.assertIsNone(reseed.switch)
        self.assertEqual(engine.latched_direction, DiscernmentDirection.SHORT)

    def test_deterministic_hash_ignores_input_mapping_order(self) -> None:
        configured = policy(
            required=(("a", "1"), ("b", "1")),
            bounds=(("ga", 2.0), ("gb", 2.0)),
        )
        first = DiscernmentEngine(configured)
        second = DiscernmentEngine(configured)
        a_first = evidence(1, 0.4, component="a", group="ga", at=1, event_id="a")
        b_second = evidence(2, -0.1, component="b", group="gb", at=1, event_id="b")
        b_first = replace(b_second, evidence_revision=1)
        a_second = replace(a_first, evidence_revision=2)
        first.on_evidence(a_first)
        result_one = first.on_evidence(b_second).snapshot
        second.on_evidence(b_first)
        result_two = second.on_evidence(a_second).snapshot
        self.assertEqual(result_one.result_hash, result_two.result_hash)
        self.assertEqual(result_one.payload(), result_two.payload())

    def test_default_reachability_reports_exact_85_percent_unreachable(self) -> None:
        reachability = policy().reachability
        self.assertLess(reachability.maximum_effective_long_score, 0.85)
        self.assertGreater(reachability.minimum_effective_long_score, 0.15)
        self.assertFalse(reachability.strong_long_reachable)
        self.assertFalse(reachability.strong_short_reachable)
        self.assertEqual(reachability.reason_code, STRONG_THRESHOLD_UNREACHABLE)

    def test_nonfinite_future_and_invalid_contract_inputs_fail(self) -> None:
        with self.assertRaises(ValueError):
            evidence(1, float("nan"))
        with self.assertRaises(ValueError):
            replace(evidence(1, 1.0), source_event_at=stamp(10))
        with self.assertRaises(ValueError):
            DiscernmentEngine(policy(), contract="NQ SEP26")


if __name__ == "__main__":
    unittest.main()
