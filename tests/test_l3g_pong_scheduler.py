from __future__ import annotations

import unittest

from src.l3g_paper.pong_scheduler import (
    PongPeriodicDeadline,
    PongSchedulingConfig,
    PongSchedulingMode,
    scheduling_config_from_environment,
)


class PongPeriodicDeadlineTests(unittest.TestCase):
    def test_environment_selection_is_explicit_and_profile_safe(self) -> None:
        self.assertEqual(
            scheduling_config_from_environment(
                "PERIODIC_FLAT_PROTECTED", "20",
            ).mode,
            PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
        )
        with self.assertRaisesRegex(ValueError, "PONG_SCHEDULING_ENVIRONMENT_INCOMPLETE"):
            scheduling_config_from_environment("PERIODIC_FLAT_PROTECTED", None)
        with self.assertRaisesRegex(ValueError, "PONG_EVENT_DRIVEN_INTERVAL_MUST_BE_ZERO"):
            scheduling_config_from_environment("EVENT_DRIVEN", "20")

    def test_absolute_grid_skips_obsolete_work_and_reconciles_outcomes(self) -> None:
        scheduler = PongPeriodicDeadline(
            PongSchedulingConfig(
                mode=PongSchedulingMode.PERIODIC_FLAT_PROTECTED,
                interval_ms=20,
            ),
            clock_ns=lambda: 0,
        )
        first = scheduler.take_due(20_000_000)
        assert first is not None
        self.assertEqual(first.scheduled_deadline_ns, 20_000_000)
        scheduler.complete(first, applied=True)

        current = scheduler.take_due(100_000_000)
        assert current is not None
        self.assertEqual(current.scheduled_deadline_ns, 40_000_000)
        self.assertEqual(current.missed_obsolete, 3)
        scheduler.supersede_for_protection(current)

        metrics = scheduler.metrics()
        self.assertEqual(metrics["scheduled_opportunities"], 5)
        self.assertEqual(metrics["skipped_obsolete_deadlines"], 3)
        self.assertEqual(metrics["protection_mode_supersessions"], 1)
        self.assertTrue(metrics["deadline_reconciliation"])


if __name__ == "__main__":
    unittest.main()
