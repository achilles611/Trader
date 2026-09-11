from __future__ import annotations

from dataclasses import asdict
import math
import unittest

from src.copytrade.confidence import ConfidenceEngine, ModelEvidence, effective_confidence
from src.shared.confidence_math import (
    attainable_effective_range,
    combine_log_odds,
    logit_probability,
    shrink_toward_neutral,
    sigmoid_clipped,
)


class SharedConfidenceMathTests(unittest.TestCase):
    def test_copytrade_wrapper_preserves_legacy_math_and_serialization(self) -> None:
        baseline = 0.8
        updates = {"first": 0.7, "second": -0.25}
        legacy_log_odds = math.log(max(baseline, 1e-9) / max(1 - baseline, 1e-9))
        for update in updates.values():
            legacy_log_odds += update
        expected_trade = 1 / (1 + math.exp(-max(-30.0, min(30.0, legacy_log_odds))))

        evidence = ModelEvidence(0.9, 0.8, 0.8, None, 0.8, 0.9, 0.9, 0.9, 0.8)
        engine = ConfidenceEngine()
        trade, explanation = engine.trade_confidence(baseline, evidence_updates=updates)
        snapshot = engine.snapshot(evidence, baseline, evidence_updates=updates)

        self.assertEqual(trade, expected_trade)
        self.assertEqual(
            explanation,
            ("first:+0.700 calibrated log-odds", "second:-0.250 calibrated log-odds"),
        )
        self.assertEqual(
            snapshot.payload(),
            asdict(snapshot),
        )
        self.assertEqual(
            set(snapshot.payload()),
            {"model_confidence", "trade_confidence", "effective_confidence", "explanation"},
        )

    def test_kernel_matches_legacy_clipping_and_shrinkage(self) -> None:
        self.assertEqual(combine_log_odds(0.8, (0.7, -0.25)), sigmoid_clipped(logit_probability(0.8) + 0.45))
        self.assertEqual(shrink_toward_neutral(0.9, 0), 0.5)
        self.assertEqual(effective_confidence(0.9, 0.2), shrink_toward_neutral(0.9, 0.2))
        self.assertLess(sigmoid_clipped(1e6), 1.0)
        self.assertGreater(sigmoid_clipped(-1e6), 0.0)

    def test_default_reliability_cannot_reach_exact_strong_threshold(self) -> None:
        low, high = attainable_effective_range(0.70)
        self.assertGreater(low, 0.15)
        self.assertLess(high, 0.85)
        self.assertLess(1 - low, 0.85)

    def test_nonfinite_and_range_inputs_fail(self) -> None:
        for value in (float("nan"), float("inf"), -0.01, 1.01):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    shrink_toward_neutral(value, 0.7)
        with self.assertRaises(ValueError):
            combine_log_odds(0.5, (float("nan"),))


if __name__ == "__main__":
    unittest.main()
