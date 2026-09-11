from __future__ import annotations

from decimal import Decimal
import unittest

from src.lane_iii.contracts import canonical_hash
from src.l3g_paper.pong_replay import (
    CandidateReplayResult,
    ComparisonStatus,
    CoverageSourceKind,
    EvidenceBasis,
    MetricIdentity,
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
SOURCE_HASH = canonical_hash({"source": 1})


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
    source = ReplaySourceRef(
        "retained-source", SOURCE_HASH, AS_OF,
        tuple(sorted(name for name, available in values.items() if available)),
    )
    return ReplayCoverageManifest(
        "manifest-test", source_kind, AS_OF, (source,), **values,
    )


def metric(
    name: str,
    value: str | None,
    basis: EvidenceBasis,
    *,
    definition: str | None = None,
    unit: str = "COUNT",
    horizon: str = "FULL_REPLAY_INTERVAL",
    cost_basis: str = "RECORDED_OR_EXPLICIT_MODEL_COSTS",
    fill_basis: str = "CAUSALLY_AVAILABLE_EXECUTABLE_PRICE_MODEL",
) -> ReplayMetric:
    identity = MetricIdentity(
        name, definition or f"Explicit definition for {name}", unit, horizon,
        cost_basis, fill_basis,
    )
    if value is None:
        return ReplayMetric(
            identity, EvidenceBasis.UNAVAILABLE, None, "SOURCE_OUTCOME_NOT_RETAINED",
        )
    return ReplayMetric(identity, basis, Decimal(value))


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
    source = manifest.ordered_sources[0]
    metrics = [
        metric("SIGNAL_COUNT", "2", basis),
        metric("NET_PNL", pnl, basis),
    ]
    if include_missing_mae:
        metrics.append(metric("MAE", None, basis))
    return CandidateReplayResult(
        candidate, basis, manifest.coverage_hash, signal_hash, quantity_hash,
        tuple(metrics), (source.source_id,), (source.source_hash,),
        ("EXPLORATORY_NOT_GENERALIZABLE",),
    )


class OfflineReplayComparisonTests(unittest.TestCase):
    def test_available_coverage_requires_a_specifically_bound_source_digest(self) -> None:
        unsupported = ReplaySourceRef(
            "unsupported-source", SOURCE_HASH, AS_OF, (),
        )
        with self.assertRaisesRegex(ValueError, "lacks a bound source digest"):
            ReplayCoverageManifest(
                "manifest-unbound",
                CoverageSourceKind.RETAINED_ACTUAL,
                AS_OF,
                (unsupported,),
                True,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
            )

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
        artifacts = {item.coverage_kind: item for item in manifest.coverage_artifacts}
        self.assertEqual(
            artifacts["full_market_event_stream"].state,
            EvidenceBasis.UNAVAILABLE,
        )
        self.assertEqual(
            artifacts["full_market_event_stream"].unavailable_reason,
            "NOT_PROVEN_BY_COVERAGE_MANIFEST",
        )
        self.assertEqual(artifacts["full_market_event_stream"].source_ids, ())

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
        event_artifact = next(
            item for item in report.coverage.coverage_artifacts
            if item.coverage_kind == "full_market_event_stream"
        )
        self.assertEqual(event_artifact.state, EvidenceBasis.ACTUAL)
        self.assertEqual(event_artifact.source_ids, ("retained-source",))
        self.assertEqual(event_artifact.source_hashes, (SOURCE_HASH,))
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

    def test_same_metric_name_with_different_identity_is_not_compared(self) -> None:
        manifest = coverage()
        source = manifest.ordered_sources[0]
        fixed = CandidateReplayResult(
            ReplayCandidate.DISCERNMENT_FIXED_ONE,
            EvidenceBasis.MODELED,
            manifest.coverage_hash,
            SIGNAL_HASH,
            ONE_HASH,
            (metric("NET_PNL", "5", EvidenceBasis.MODELED, unit="USD"),),
            (source.source_id,),
            (source.source_hash,),
        )
        conditional = CandidateReplayResult(
            ReplayCandidate.DISCERNMENT_CONDITIONAL_TWO,
            EvidenceBasis.MODELED,
            manifest.coverage_hash,
            SIGNAL_HASH,
            TWO_HASH,
            (metric("NET_PNL", "7", EvidenceBasis.MODELED, unit="POINTS"),),
            (source.source_id,),
            (source.source_hash,),
        )
        comparison = OfflineReplayComparisonScaffold().build(
            manifest, (fixed, conditional),
        ).comparisons[-1]
        self.assertEqual(comparison.status, ComparisonStatus.UNAVAILABLE)
        self.assertIn("METRIC_IDENTITY_MISMATCH:NET_PNL", comparison.reason_codes)
        self.assertIn("NO_SHARED_EXACT_METRIC_IDENTITIES", comparison.reason_codes)
        self.assertEqual(comparison.metric_deltas, ())


if __name__ == "__main__":
    unittest.main()
