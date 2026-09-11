from __future__ import annotations

from decimal import Decimal
import unittest

from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.pong_replay import (
    CandidateReplayResult,
    ComparisonStatus,
    CoverageSourceKind,
    EvidenceBasis,
    OfflineReplayComparisonScaffold,
    ReplayCandidate,
    ReplayCoverageManifest,
    ReplayMetric,
    ReplaySourceRef,
)


AS_OF = "2026-09-11T01:00:00Z"
SIGNAL_HASH = canonical_hash({"signal": "fixed-discernment-policy-v1"})
LEGACY_SIGNAL_HASH = canonical_hash({"signal": "legacy-switch-policy"})
PERPETUAL_SIGNAL_HASH = canonical_hash({"signal": "perpetual-as-executed"})
ONE_HASH = canonical_hash({"quantity": 1})
TWO_HASH = canonical_hash({"quantity": "conditional-two"})
SOURCE = ReplaySourceRef("retained-source", canonical_hash({"source": 1}), AS_OF)


def coverage(
    source_kind: CoverageSourceKind = CoverageSourceKind.RETAINED_ACTUAL,
    **overrides: bool,
) -> ReplayCoverageManifest:
    values = {
        "full_market_event_stream": True,
        "causal_bias_history": True,
        "legacy_switch_stream": True,
        "executable_price_path": True,
        "outcome_path": True,
        "actual_execution_records": source_kind is CoverageSourceKind.RETAINED_ACTUAL,
        "closure_record": source_kind is CoverageSourceKind.RETAINED_ACTUAL,
        "low_volume_labels": True,
    }
    values.update(overrides)
    return ReplayCoverageManifest(
        "manifest-test", source_kind, AS_OF, (SOURCE,), **values,
    )


def metric(
    name: str,
    value: str | None,
    basis: EvidenceBasis,
) -> ReplayMetric:
    if value is None:
        return ReplayMetric(
            name, f"Explicit definition for {name}", EvidenceBasis.UNAVAILABLE,
            None, "SOURCE_OUTCOME_NOT_RETAINED",
        )
    return ReplayMetric(name, f"Explicit definition for {name}", basis, Decimal(value))


def result(
    candidate: ReplayCandidate,
    manifest: ReplayCoverageManifest,
    *,
    basis: EvidenceBasis,
    signal_hash: str,
    quantity_hash: str,
    pnl: str = "1",
    include_missing_mae: bool = False,
) -> CandidateReplayResult:
    metrics = [
        metric("SIGNAL_COUNT", "2", basis),
        metric("NET_PNL", pnl, basis),
    ]
    if include_missing_mae:
        metrics.append(metric("MAE", None, basis))
    return CandidateReplayResult(
        candidate, basis, manifest.coverage_hash, signal_hash, quantity_hash,
        tuple(metrics), (SOURCE.source_id,), (SOURCE.source_hash,),
        ("EXPLORATORY_NOT_GENERALIZABLE",),
    )


class OfflineReplayComparisonTests(unittest.TestCase):
    def test_missing_coverage_and_results_remain_unavailable_without_metrics(self) -> None:
        manifest = coverage(
            full_market_event_stream=False,
            causal_bias_history=False,
            legacy_switch_stream=False,
            executable_price_path=False,
            outcome_path=False,
            actual_execution_records=False,
            closure_record=False,
            low_volume_labels=False,
        )
        report = OfflineReplayComparisonScaffold().build(manifest)
        self.assertEqual(len(report.candidates), 4)
        self.assertTrue(all(item.basis is EvidenceBasis.UNAVAILABLE for item in report.candidates))
        self.assertTrue(all(item.result is None for item in report.candidates))
        self.assertTrue(all(item.status is ComparisonStatus.UNAVAILABLE for item in report.comparisons))
        fixed = next(item for item in report.candidates if item.candidate is ReplayCandidate.DISCERNMENT_FIXED_ONE)
        self.assertIn("FULL_MARKET_EVENT_STREAM_UNAVAILABLE", fixed.reason_codes)
        self.assertIn("OUTCOME_PATH_UNAVAILABLE", fixed.reason_codes)
        self.assertIn("LOW_VOLUME_SUBSET_UNAVAILABLE", fixed.reason_codes)

    def test_actual_modeled_and_conditional_comparisons_are_distinct(self) -> None:
        manifest = coverage()
        submitted = (
            result(
                ReplayCandidate.PERPETUAL_AS_EXECUTED, manifest, basis=EvidenceBasis.ACTUAL,
                signal_hash=PERPETUAL_SIGNAL_HASH, quantity_hash=ONE_HASH, pnl="3",
            ),
            result(
                ReplayCandidate.LEGACY_SWITCH_PONG, manifest, basis=EvidenceBasis.MODELED,
                signal_hash=LEGACY_SIGNAL_HASH, quantity_hash=ONE_HASH, pnl="4",
            ),
            result(
                ReplayCandidate.DISCERNMENT_FIXED_ONE, manifest, basis=EvidenceBasis.MODELED,
                signal_hash=SIGNAL_HASH, quantity_hash=ONE_HASH, pnl="5",
            ),
            result(
                ReplayCandidate.DISCERNMENT_CONDITIONAL_TWO, manifest, basis=EvidenceBasis.MODELED,
                signal_hash=SIGNAL_HASH, quantity_hash=TWO_HASH, pnl="7",
            ),
        )
        report = OfflineReplayComparisonScaffold().build(manifest, reversed(submitted))
        bases = {item.candidate: item.basis for item in report.candidates}
        self.assertEqual(bases[ReplayCandidate.PERPETUAL_AS_EXECUTED], EvidenceBasis.ACTUAL)
        self.assertEqual(bases[ReplayCandidate.DISCERNMENT_FIXED_ONE], EvidenceBasis.MODELED)
        self.assertTrue(all(item.status is ComparisonStatus.AVAILABLE for item in report.comparisons))
        quantity_comparison = report.comparisons[-1]
        pnl = next(item for item in quantity_comparison.metric_deltas if item.name == "NET_PNL")
        self.assertEqual(pnl.right_minus_left, Decimal("2"))
        self.assertEqual(report.report_hash, OfflineReplayComparisonScaffold().build(manifest, submitted).report_hash)

    def test_synthetic_fixtures_cannot_masquerade_as_actual_or_modeled(self) -> None:
        manifest = coverage(CoverageSourceKind.SYNTHETIC_FIXTURE)
        fixed = result(
            ReplayCandidate.DISCERNMENT_FIXED_ONE, manifest, basis=EvidenceBasis.SYNTHETIC,
            signal_hash=SIGNAL_HASH, quantity_hash=ONE_HASH,
        )
        report = OfflineReplayComparisonScaffold().build(manifest, (fixed,))
        projection = next(
            item for item in report.candidates
            if item.candidate is ReplayCandidate.DISCERNMENT_FIXED_ONE
        )
        self.assertEqual(projection.basis, EvidenceBasis.SYNTHETIC)
        perpetual = next(item for item in report.candidates if item.candidate is ReplayCandidate.PERPETUAL_AS_EXECUTED)
        self.assertEqual(perpetual.basis, EvidenceBasis.UNAVAILABLE)
        self.assertIn("ACTUAL_SOURCE_REQUIRED", perpetual.reason_codes)
        wrong = result(
            ReplayCandidate.DISCERNMENT_CONDITIONAL_TWO, manifest, basis=EvidenceBasis.MODELED,
            signal_hash=SIGNAL_HASH, quantity_hash=TWO_HASH,
        )
        with self.assertRaisesRegex(ValueError, "requires SYNTHETIC"):
            OfflineReplayComparisonScaffold().build(manifest, (wrong,))

    def test_result_is_refused_when_required_coverage_is_missing(self) -> None:
        manifest = coverage(full_market_event_stream=False)
        submitted = result(
            ReplayCandidate.DISCERNMENT_FIXED_ONE, manifest, basis=EvidenceBasis.MODELED,
            signal_hash=SIGNAL_HASH, quantity_hash=ONE_HASH,
        )
        with self.assertRaisesRegex(ValueError, "without required source coverage"):
            OfflineReplayComparisonScaffold().build(manifest, (submitted,))

    def test_fixed_and_conditional_require_identical_signal_policy(self) -> None:
        manifest = coverage()
        fixed = result(
            ReplayCandidate.DISCERNMENT_FIXED_ONE, manifest, basis=EvidenceBasis.MODELED,
            signal_hash=SIGNAL_HASH, quantity_hash=ONE_HASH,
        )
        conditional = result(
            ReplayCandidate.DISCERNMENT_CONDITIONAL_TWO, manifest, basis=EvidenceBasis.MODELED,
            signal_hash=LEGACY_SIGNAL_HASH, quantity_hash=TWO_HASH,
        )
        report = OfflineReplayComparisonScaffold().build(manifest, (fixed, conditional))
        comparison = report.comparisons[-1]
        self.assertEqual(comparison.status, ComparisonStatus.UNAVAILABLE)
        self.assertEqual(comparison.reason_codes, ("SIGNAL_POLICY_NOT_HELD_CONSTANT",))

    def test_missing_metric_is_partial_and_never_coerced_to_zero(self) -> None:
        manifest = coverage()
        fixed = result(
            ReplayCandidate.DISCERNMENT_FIXED_ONE, manifest, basis=EvidenceBasis.MODELED,
            signal_hash=SIGNAL_HASH, quantity_hash=ONE_HASH, include_missing_mae=True,
        )
        conditional = result(
            ReplayCandidate.DISCERNMENT_CONDITIONAL_TWO, manifest, basis=EvidenceBasis.MODELED,
            signal_hash=SIGNAL_HASH, quantity_hash=TWO_HASH,
        )
        report = OfflineReplayComparisonScaffold().build(manifest, (fixed, conditional))
        comparison = report.comparisons[-1]
        self.assertEqual(comparison.status, ComparisonStatus.PARTIAL)
        self.assertIn("METRIC_UNAVAILABLE:MAE", comparison.reason_codes)
        self.assertNotIn("MAE", {item.name for item in comparison.metric_deltas})


if __name__ == "__main__":
    unittest.main()
