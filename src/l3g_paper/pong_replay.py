"""Provenance-strict offline comparison scaffold for Pong research.

This module deliberately performs no market simulation.  It accepts replay
results produced by a separate, explicit model and only reports comparisons
whose source coverage is proven.  Missing inputs or outcomes remain visible as
UNAVAILABLE and are never replaced with zeros or inferred trades.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
import re
from typing import Iterable

from src.lane_iii.contracts import canonical_hash, normalized_utc


PONG_REPLAY_SCHEMA = "beelzebub-pong-offline-replay-comparison-v1"


class EvidenceBasis(StrEnum):
    ACTUAL = "ACTUAL"
    MODELED = "MODELED"
    SYNTHETIC = "SYNTHETIC"
    UNAVAILABLE = "UNAVAILABLE"


class CoverageSourceKind(StrEnum):
    RETAINED_ACTUAL = "RETAINED_ACTUAL"
    SYNTHETIC_FIXTURE = "SYNTHETIC_FIXTURE"


class ReplayCandidate(StrEnum):
    PERPETUAL_AS_EXECUTED = "PERPETUAL_AS_EXECUTED"
    LEGACY_SWITCH_PONG = "LEGACY_SWITCH_PONG"
    DISCERNMENT_FIXED_ONE = "DISCERNMENT_FIXED_ONE"
    DISCERNMENT_CONDITIONAL_TWO = "DISCERNMENT_CONDITIONAL_TWO"


class ComparisonStatus(StrEnum):
    AVAILABLE = "AVAILABLE"
    PARTIAL = "PARTIAL"
    UNAVAILABLE = "UNAVAILABLE"


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} is required.")
    return value


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest.")
    return value


def _decimal(value: object, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal.") from exc
    if not result.is_finite():
        raise ValueError(f"{field} must be a finite decimal.")
    return result


def _dtext(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _time(value: str, field: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, field).replace("Z", "+00:00"))


@dataclass(frozen=True)
class ReplaySourceRef:
    source_id: str
    source_hash: str
    available_at: str

    def __post_init__(self) -> None:
        _text(self.source_id, "Replay source ID")
        _sha256(self.source_hash, "Replay source hash")
        _time(self.available_at, "Replay source available-at time")

    def payload(self) -> dict[str, str]:
        return {
            "source_id": self.source_id,
            "source_hash": self.source_hash,
            "available_at": normalized_utc(self.available_at, "Replay source available-at time"),
        }


@dataclass(frozen=True)
class ReplayCoverageManifest:
    manifest_id: str
    source_kind: CoverageSourceKind
    as_of: str
    sources: tuple[ReplaySourceRef, ...]
    full_market_event_stream: bool
    causal_bias_history: bool
    legacy_switch_stream: bool
    executable_price_path: bool
    outcome_path: bool
    actual_execution_records: bool
    closure_record: bool
    low_volume_labels: bool

    def __post_init__(self) -> None:
        _text(self.manifest_id, "Replay coverage manifest ID")
        if type(self.source_kind) is not CoverageSourceKind:
            raise ValueError("Replay coverage source kind is required.")
        decision = _time(self.as_of, "Replay coverage as-of time")
        if any(type(value) is not bool for value in (
            self.full_market_event_stream, self.causal_bias_history,
            self.legacy_switch_stream, self.executable_price_path, self.outcome_path,
            self.actual_execution_records, self.closure_record, self.low_volume_labels,
        )):
            raise ValueError("Replay coverage flags must be booleans.")
        if len({item.source_id for item in self.sources}) != len(self.sources):
            raise ValueError("Replay source IDs must be unique.")
        if any(_time(item.available_at, "Replay source available-at time") > decision for item in self.sources):
            raise ValueError("Replay source was not available at the manifest as-of time.")
        if self.source_kind is CoverageSourceKind.SYNTHETIC_FIXTURE and (
            self.actual_execution_records or self.closure_record
        ):
            raise ValueError("Synthetic fixtures cannot claim actual execution or closure evidence.")
        if any((
            self.full_market_event_stream, self.causal_bias_history,
            self.legacy_switch_stream, self.executable_price_path, self.outcome_path,
            self.actual_execution_records, self.closure_record, self.low_volume_labels,
        )) and not self.sources:
            raise ValueError("Claimed replay coverage requires immutable source references.")

    @property
    def ordered_sources(self) -> tuple[ReplaySourceRef, ...]:
        return tuple(sorted(self.sources, key=lambda item: (item.available_at, item.source_id)))

    def payload(self) -> dict[str, object]:
        return {
            "manifest_id": self.manifest_id, "source_kind": self.source_kind.value,
            "as_of": normalized_utc(self.as_of, "Replay coverage as-of time"),
            "sources": [item.payload() for item in self.ordered_sources],
            "coverage": {
                "full_market_event_stream": self.full_market_event_stream,
                "causal_bias_history": self.causal_bias_history,
                "legacy_switch_stream": self.legacy_switch_stream,
                "executable_price_path": self.executable_price_path,
                "outcome_path": self.outcome_path,
                "actual_execution_records": self.actual_execution_records,
                "closure_record": self.closure_record,
                "low_volume_labels": self.low_volume_labels,
            },
        }

    @property
    def coverage_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class ReplayMetric:
    """One explicitly defined metric; absent outcomes use UNAVAILABLE, never zero."""

    name: str
    definition: str
    basis: EvidenceBasis
    value: Decimal | None = None
    unavailable_reason: str | None = None

    def __post_init__(self) -> None:
        _text(self.name, "Replay metric name")
        _text(self.definition, "Replay metric definition")
        if type(self.basis) is not EvidenceBasis:
            raise ValueError("Replay metric basis is required.")
        if self.basis is EvidenceBasis.UNAVAILABLE:
            if self.value is not None or not self.unavailable_reason:
                raise ValueError("Unavailable replay metrics require a reason and no value.")
        else:
            if self.value is None or self.unavailable_reason is not None:
                raise ValueError("Available replay metrics require a value and no unavailable reason.")
            _decimal(self.value, "Replay metric value")

    def payload(self) -> dict[str, object]:
        return {
            "name": self.name, "definition": self.definition, "basis": self.basis.value,
            "value": None if self.value is None else _dtext(self.value),
            "unavailable_reason": self.unavailable_reason,
        }


@dataclass(frozen=True)
class CandidateReplayResult:
    candidate: ReplayCandidate
    basis: EvidenceBasis
    coverage_hash: str
    signal_policy_hash: str
    quantity_policy_hash: str
    metrics: tuple[ReplayMetric, ...]
    source_ids: tuple[str, ...]
    source_hashes: tuple[str, ...]
    limitations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if type(self.candidate) is not ReplayCandidate or type(self.basis) is not EvidenceBasis:
            raise ValueError("Replay candidate and evidence basis are required.")
        _sha256(self.coverage_hash, "Replay result coverage hash")
        _sha256(self.signal_policy_hash, "Replay signal policy hash")
        _sha256(self.quantity_policy_hash, "Replay quantity policy hash")
        if self.basis is EvidenceBasis.UNAVAILABLE:
            raise ValueError("Submitted replay results cannot use UNAVAILABLE basis.")
        if not self.source_ids or len(self.source_ids) != len(self.source_hashes):
            raise ValueError("Replay result source IDs and hashes must be non-empty and aligned.")
        if len(set(self.source_ids)) != len(self.source_ids):
            raise ValueError("Replay result source IDs must be unique.")
        for digest in self.source_hashes:
            _sha256(digest, "Replay result source hash")
        if len({item.name for item in self.metrics}) != len(self.metrics):
            raise ValueError("Replay metric names must be unique per candidate.")
        if any(item.basis not in {self.basis, EvidenceBasis.UNAVAILABLE} for item in self.metrics):
            raise ValueError("Replay metric basis must match its candidate result.")

    def payload(self) -> dict[str, object]:
        ordered_sources = sorted(zip(self.source_ids, self.source_hashes))
        return {
            "candidate": self.candidate.value, "basis": self.basis.value,
            "coverage_hash": self.coverage_hash,
            "signal_policy_hash": self.signal_policy_hash,
            "quantity_policy_hash": self.quantity_policy_hash,
            "metrics": [item.payload() for item in sorted(self.metrics, key=lambda value: value.name)],
            "sources": [
                {"source_id": source_id, "source_hash": source_hash}
                for source_id, source_hash in ordered_sources
            ],
            "limitations": sorted(self.limitations),
        }

    @property
    def result_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class ReplayCandidateProjection:
    candidate: ReplayCandidate
    basis: EvidenceBasis
    reason_codes: tuple[str, ...]
    result: CandidateReplayResult | None

    def payload(self) -> dict[str, object]:
        return {
            "candidate": self.candidate.value, "basis": self.basis.value,
            "reason_codes": list(self.reason_codes),
            "result": None if self.result is None else self.result.payload(),
        }


@dataclass(frozen=True)
class MetricDelta:
    name: str
    left_value: Decimal
    right_value: Decimal
    right_minus_left: Decimal

    def payload(self) -> dict[str, str]:
        return {
            "name": self.name, "left_value": _dtext(self.left_value),
            "right_value": _dtext(self.right_value),
            "right_minus_left": _dtext(self.right_minus_left),
        }


@dataclass(frozen=True)
class ReplayComparison:
    left: ReplayCandidate
    right: ReplayCandidate
    status: ComparisonStatus
    reason_codes: tuple[str, ...]
    metric_deltas: tuple[MetricDelta, ...]

    def payload(self) -> dict[str, object]:
        return {
            "left": self.left.value, "right": self.right.value,
            "status": self.status.value, "reason_codes": list(self.reason_codes),
            "metric_deltas": [item.payload() for item in self.metric_deltas],
        }


@dataclass(frozen=True)
class OfflineReplayReport:
    coverage: ReplayCoverageManifest
    candidates: tuple[ReplayCandidateProjection, ...]
    comparisons: tuple[ReplayComparison, ...]

    def payload(self) -> dict[str, object]:
        return {
            "schema": PONG_REPLAY_SCHEMA,
            "purpose": "EXECUTION_DISABLED_EXPLORATORY_COMPARISON",
            "coverage": self.coverage.payload(),
            "candidates": [item.payload() for item in self.candidates],
            "comparisons": [item.payload() for item in self.comparisons],
            "performance_claim": "NOT_ESTABLISHED",
            "execution_authority": "DENIED",
            "live_capital": "DENIED",
        }

    @property
    def report_hash(self) -> str:
        return canonical_hash(self.payload())


class OfflineReplayComparisonScaffold:
    """Validate supplied results and compare only causally covered candidates."""

    @staticmethod
    def _coverage_reasons(
        candidate: ReplayCandidate, coverage: ReplayCoverageManifest,
    ) -> tuple[str, ...]:
        missing: list[str] = []
        if candidate is ReplayCandidate.PERPETUAL_AS_EXECUTED:
            if coverage.source_kind is not CoverageSourceKind.RETAINED_ACTUAL:
                missing.append("ACTUAL_SOURCE_REQUIRED")
            if not coverage.actual_execution_records:
                missing.append("ACTUAL_EXECUTIONS_UNAVAILABLE")
            if not coverage.closure_record:
                missing.append("RUN_CLOSURE_UNAVAILABLE")
            return tuple(missing)
        if not coverage.causal_bias_history:
            missing.append("CAUSAL_BIAS_HISTORY_UNAVAILABLE")
        if not coverage.executable_price_path:
            missing.append("EXECUTABLE_PRICE_PATH_UNAVAILABLE")
        if not coverage.outcome_path:
            missing.append("OUTCOME_PATH_UNAVAILABLE")
        if candidate is ReplayCandidate.LEGACY_SWITCH_PONG:
            if not coverage.legacy_switch_stream:
                missing.append("LEGACY_SWITCH_STREAM_UNAVAILABLE")
        elif not coverage.full_market_event_stream:
            missing.append("FULL_MARKET_EVENT_STREAM_UNAVAILABLE")
        return tuple(missing)

    @staticmethod
    def _expected_basis(
        candidate: ReplayCandidate, coverage: ReplayCoverageManifest,
    ) -> EvidenceBasis:
        if candidate is ReplayCandidate.PERPETUAL_AS_EXECUTED:
            return EvidenceBasis.ACTUAL
        return (
            EvidenceBasis.SYNTHETIC
            if coverage.source_kind is CoverageSourceKind.SYNTHETIC_FIXTURE
            else EvidenceBasis.MODELED
        )

    @staticmethod
    def _comparison(
        left: ReplayCandidateProjection,
        right: ReplayCandidateProjection,
        *,
        require_same_signal_policy: bool,
    ) -> ReplayComparison:
        reasons: list[str] = []
        if left.result is None:
            reasons.append(f"{left.candidate.value}_UNAVAILABLE")
        if right.result is None:
            reasons.append(f"{right.candidate.value}_UNAVAILABLE")
        if reasons:
            return ReplayComparison(left.candidate, right.candidate, ComparisonStatus.UNAVAILABLE, tuple(reasons), ())
        assert left.result is not None and right.result is not None
        if require_same_signal_policy and left.result.signal_policy_hash != right.result.signal_policy_hash:
            return ReplayComparison(
                left.candidate, right.candidate, ComparisonStatus.UNAVAILABLE,
                ("SIGNAL_POLICY_NOT_HELD_CONSTANT",), (),
            )
        left_metrics = {
            item.name: item for item in left.result.metrics
            if item.basis is not EvidenceBasis.UNAVAILABLE and item.value is not None
        }
        right_metrics = {
            item.name: item for item in right.result.metrics
            if item.basis is not EvidenceBasis.UNAVAILABLE and item.value is not None
        }
        names = sorted(set(left_metrics) & set(right_metrics))
        deltas = tuple(
            MetricDelta(
                name, left_metrics[name].value, right_metrics[name].value,
                right_metrics[name].value - left_metrics[name].value,
            )
            for name in names
            if left_metrics[name].value is not None and right_metrics[name].value is not None
        )
        unavailable_names = sorted(
            {item.name for item in left.result.metrics + right.result.metrics}
            - set(names)
        )
        if not deltas:
            return ReplayComparison(
                left.candidate, right.candidate, ComparisonStatus.UNAVAILABLE,
                ("NO_SHARED_AVAILABLE_METRICS",), (),
            )
        return ReplayComparison(
            left.candidate, right.candidate,
            ComparisonStatus.PARTIAL if unavailable_names else ComparisonStatus.AVAILABLE,
            tuple(f"METRIC_UNAVAILABLE:{name}" for name in unavailable_names), deltas,
        )

    def build(
        self,
        coverage: ReplayCoverageManifest,
        submitted_results: Iterable[CandidateReplayResult] = (),
    ) -> OfflineReplayReport:
        submitted = tuple(submitted_results)
        if len({item.candidate for item in submitted}) != len(submitted):
            raise ValueError("Only one replay result may be submitted per candidate.")
        by_candidate = {item.candidate: item for item in submitted}
        projections: list[ReplayCandidateProjection] = []
        coverage_sources = {
            item.source_id: item.source_hash for item in coverage.ordered_sources
        }
        for candidate in ReplayCandidate:
            reasons = list(self._coverage_reasons(candidate, coverage))
            result = by_candidate.get(candidate)
            if result is not None:
                if reasons:
                    raise ValueError(
                        f"Replay result supplied without required source coverage: {candidate.value}."
                    )
                if result.coverage_hash != coverage.coverage_hash:
                    raise ValueError("Replay result is bound to a different coverage manifest.")
                expected = self._expected_basis(candidate, coverage)
                if result.basis is not expected:
                    raise ValueError(f"{candidate.value} requires {expected.value} evidence basis.")
                for source_id, source_hash in zip(result.source_ids, result.source_hashes):
                    if coverage_sources.get(source_id) != source_hash:
                        raise ValueError("Replay result source does not match the coverage manifest.")
            elif not reasons:
                reasons.append("REPLAY_RESULT_NOT_SUPPLIED")
            if candidate is not ReplayCandidate.PERPETUAL_AS_EXECUTED and not coverage.low_volume_labels:
                reasons.append("LOW_VOLUME_SUBSET_UNAVAILABLE")
            projections.append(ReplayCandidateProjection(
                candidate,
                EvidenceBasis.UNAVAILABLE if result is None else result.basis,
                tuple(reasons),
                result,
            ))
        projected = {item.candidate: item for item in projections}
        pairs = (
            (ReplayCandidate.PERPETUAL_AS_EXECUTED, ReplayCandidate.LEGACY_SWITCH_PONG, False),
            (ReplayCandidate.LEGACY_SWITCH_PONG, ReplayCandidate.DISCERNMENT_FIXED_ONE, False),
            (ReplayCandidate.DISCERNMENT_FIXED_ONE, ReplayCandidate.DISCERNMENT_CONDITIONAL_TWO, True),
        )
        comparisons = tuple(
            self._comparison(projected[left], projected[right], require_same_signal_policy=same)
            for left, right, same in pairs
        )
        return OfflineReplayReport(coverage, tuple(projections), comparisons)
