"""Regression for Windows' 15.625 ms GetTickCount64 at a 10 ms cadence.

These tests use deterministic clock values; they do not sleep, replay a corpus,
start a production host, or make a measured performance claim.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import patch

from src.l3f_provider.ninjatrader_observation import expected_observer_source_fingerprint
from src.l3g_paper.contracts import PONG_ENTRY_PROFILE_VERSION
from src.l3g_paper.pong_periodic import OfflinePeriodicPong
from src.l3g_paper.pong_production import PongProductionHost


START = datetime(2026, 9, 14, 17, 43, 54, 549971, tzinfo=timezone.utc)
ORIGIN_NS = 8_000_000_000_000


class PeriodicClockRegressionTests(unittest.TestCase):
    def make_host(self):
        directory = TemporaryDirectory(prefix="pong-periodic-clock-")
        self.addCleanup(directory.cleanup)
        return PongProductionHost(
            Path(directory.name), selected_profile=PONG_ENTRY_PROFILE_VERSION,
            expected_market_source_fingerprint=expected_observer_source_fingerprint(),
            runtime_snapshot=lambda: {}, collector_status=lambda: {},
            controller_dispatch=lambda *_: self.fail("controller dispatch forbidden"),
            native_dispatch=lambda *_: self.fail("native dispatch forbidden"),
        )

    def test_default_clock_has_resolution_below_the_minimum_tested_interval(self):
        adapter = OfflinePeriodicPong(self.make_host(), interval_ms=10)
        # This directly rejects the actual regression: on the measured Python
        # 3.12 Windows environment monotonic_ns was GetTickCount64 at 15.625 ms.
        self.assertIs(adapter.clock_ns, time.perf_counter_ns)
        info = time.get_clock_info("perf_counter")
        self.assertTrue(info.monotonic)
        self.assertFalse(info.adjustable)
        self.assertLessEqual(info.resolution, .010/10)

    def drive_clock(self, *, quantization_ns=1, phase_ns=0):
        current = [ORIGIN_NS]
        adapter = OfflinePeriodicPong(self.make_host(), interval_ms=10,
            origin_ns=ORIGIN_NS, phase_ns=phase_ns, clock_ns=lambda: current[0])
        results = []
        # Distinct UTC and monotonic epochs prevent an accidental wall-clock
        # epoch assumption. The real owner polls at each simulated millisecond.
        with patch("src.l3g_paper.pong_periodic.time.monotonic_ns",
                   side_effect=AssertionError("coarse Windows clock used")):
            for elapsed_ms in range(101):
                elapsed_ns = elapsed_ms*1_000_000
                current[0] = ORIGIN_NS+(elapsed_ns//quantization_ns)*quantization_ns
                result = adapter.tick(adapter.clock_ns(), START+timedelta(milliseconds=elapsed_ms))
                if result is not None:
                    results.append(result)
        return adapter, results

    def test_ten_ms_absolute_grid_does_not_invent_misses_from_clock_quantization(self):
        adapter, results = self.drive_clock()
        self.assertEqual(len(results), 11)  # t=0,10,...,100 ms, inclusive.
        self.assertEqual(adapter.metrics()["missed_ticks"], 0)
        self.assertEqual([r.lateness_ns for r in results], [0]*11)
        self.assertEqual(adapter.next_deadline_ns, ORIGIN_NS+110_000_000)

        coarse, coarse_results = self.drive_clock(quantization_ns=15_625_000)
        self.assertEqual(len(coarse_results), 7)
        self.assertEqual(coarse.metrics()["missed_ticks"], 3)
        # The difference is clock quantization alone: no actual calculation
        # elapsed or queue overload was supplied to this deterministic schedule.

    def test_half_interval_phase_keeps_cutoffs_on_the_shared_precise_origin(self):
        adapter, results = self.drive_clock(phase_ns=5_000_000)
        self.assertEqual(len(results), 10)
        self.assertEqual(adapter.metrics()["missed_ticks"], 0)
        for ordinal, result in enumerate(results):
            elapsed_ms = 5+10*ordinal
            self.assertEqual(result.scheduled_deadline_ns, ORIGIN_NS+elapsed_ms*1_000_000)
            self.assertEqual(result.started_ns, result.scheduled_deadline_ns)
            self.assertEqual(result.view.available_cutoff,
                (START+timedelta(milliseconds=elapsed_ms)).isoformat().replace("+00:00", "Z"))
        self.assertEqual(adapter.next_deadline_ns, ORIGIN_NS+105_000_000)


if __name__ == "__main__":
    unittest.main()
