"""Explicit, serializable contracts for Phase E experiments.

These types deliberately model scientific intent instead of generic JSON
documents.  ``canonical_payload`` is the one representation used for hashing
and persistence, which makes identity stable across process restarts.
"""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Mapping


PHASE_E_SCHEMA_VERSION = "phase-e1"
CANONICALIZATION_VERSION = "phase-e1-type-tagged-sha256-v1"
SUPPORTED_SHORT_HORIZONS = frozenset({5, 15, 30, 60, 120, 300, 600})


class ExperimentStatus(StrEnum):
    """Persisted lifecycle states.  Terminal states are never reopened."""

    REGISTERED = "REGISTERED"
    RUNNING = "RUNNING"
    RECOVERABLE = "RECOVERABLE"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


class ExperimentConclusion(StrEnum):
    SURVIVED = "SURVIVED"
    REJECTED = "REJECTED"


class PromotionState(StrEnum):
    """E.1 records eligibility only; it cannot promote a trading signal."""

    NOT_ELIGIBLE = "NOT_ELIGIBLE"
    HISTORICAL_SURVIVOR = "HISTORICAL_SURVIVOR"
    REJECTED = "REJECTED"


class RejectionReason(StrEnum):
    INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"
    EFFECT_BELOW_THRESHOLD = "EFFECT_BELOW_THRESHOLD"
    STATISTICAL_THRESHOLD_NOT_MET = "STATISTICAL_THRESHOLD_NOT_MET"
    NULL_HYPOTHESIS_NOT_REJECTED = "NULL_HYPOTHESIS_NOT_REJECTED"
    INVALID_EVIDENCE = "INVALID_EVIDENCE"
    MALFORMED_STATISTIC = "MALFORMED_STATISTIC"
    PARTITION_INTEGRITY = "PARTITION_INTEGRITY"
    CODE_OR_CONFIGURATION_STALE = "CODE_OR_CONFIGURATION_STALE"
    EXECUTION_FAILURE = "EXECUTION_FAILURE"


def _utc(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"Invalid ISO-8601 UTC timestamp: {value!r}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid ISO-8601 UTC timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError("Timestamps must include an explicit UTC offset.")
    return parsed.astimezone(timezone.utc)


def normalized_utc(value: str) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _json_value(value: Any, *, path: str = "value") -> Any:
    """Return a JSON-safe copy and reject non-deterministic numeric values."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must not contain NaN or Infinity.")
        return value
    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} has a non-string mapping key.")
            output[key] = _json_value(child, path=f"{path}.{key}")
        return output
    if isinstance(value, (tuple, list)):
        return [_json_value(child, path=f"{path}[]") for child in value]
    raise ValueError(f"{path} contains unsupported value type {type(value).__name__}.")


def canonical_json(value: Any) -> str:
    """Return type-tagged canonical identity JSON.

    Floats use their exact IEEE-754 hexadecimal representation.  Every JSON
    type is tagged so a user mapping can never collide with an encoded scalar.
    This representation is for hashing only; ``storage_json`` preserves normal
    JSON values for persisted documents.
    """

    def encode(item: Any, *, path: str) -> Any:
        if item is None:
            return ["null"]
        if isinstance(item, bool):
            return ["bool", item]
        if isinstance(item, str):
            return ["string", unicodedata.normalize("NFC", item)]
        if isinstance(item, int):
            return ["integer", str(item)]
        if isinstance(item, float):
            if not math.isfinite(item):
                raise ValueError(f"{path} must not contain NaN or Infinity.")
            if item == 0.0:
                item = 0.0  # Canonicalize IEEE-754 negative zero.
            return ["float64", item.hex()]
        if isinstance(item, Mapping):
            pairs = []
            keys = list(item)
            if any(not isinstance(key, str) for key in keys):
                raise ValueError(f"{path} has a non-string mapping key.")
            normalized_keys = {key: unicodedata.normalize("NFC", key) for key in keys}
            if len(set(normalized_keys.values())) != len(keys):
                raise ValueError(f"{path} has mapping keys that collide after Unicode normalization.")
            for key in sorted(keys, key=normalized_keys.__getitem__):
                canonical_key = normalized_keys[key]
                pairs.append([canonical_key, encode(item[key], path=f"{path}.{canonical_key}")])
            return ["mapping", pairs]
        if isinstance(item, (tuple, list)):
            return ["sequence", [encode(child, path=f"{path}[]") for child in item]]
        raise ValueError(f"{path} contains unsupported value type {type(item).__name__}.")

    return json.dumps(encode(value, path="value"), ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def storage_json(value: Any) -> str:
    """Persist ordinary JSON while applying the same finite/type checks."""
    return json.dumps(_json_value(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def finite_number(value: Any, *, name: str, minimum: float | None = None, maximum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a finite number.")
    numeric = float(value)
    if not math.isfinite(numeric):
        raise ValueError(f"{name} must not be NaN or Infinity.")
    if minimum is not None and numeric < minimum:
        raise ValueError(f"{name} must be at least {minimum}.")
    if maximum is not None and numeric > maximum:
        raise ValueError(f"{name} must be at most {maximum}.")
    return numeric


@dataclass(frozen=True)
class OutcomeHorizon:
    seconds: int

    def __post_init__(self) -> None:
        if isinstance(self.seconds, bool) or not isinstance(self.seconds, int) or self.seconds not in SUPPORTED_SHORT_HORIZONS:
            allowed = ", ".join(str(item) for item in sorted(SUPPORTED_SHORT_HORIZONS))
            raise ValueError(f"Outcome horizon must be one of the short-horizon values: {allowed} seconds.")

    def payload(self) -> dict[str, int]:
        return {"seconds": self.seconds}


@dataclass(frozen=True)
class FeatureReference:
    feature_id: str
    version: int
    lookback_seconds: int = 0
    lookforward_seconds: int = 0

    def __post_init__(self) -> None:
        if (not isinstance(self.feature_id, str) or not self.feature_id.strip()
                or isinstance(self.version, bool) or not isinstance(self.version, int) or self.version <= 0):
            raise ValueError("Feature references require a nonempty ID and positive version.")
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0
               for value in (self.lookback_seconds, self.lookforward_seconds)):
            raise ValueError("Feature lookback/lookforward bounds must be nonnegative integer seconds.")
        if self.lookforward_seconds != 0:
            raise ValueError("Predictive feature references cannot require forward information.")

    def payload(self) -> dict[str, Any]:
        return {
            "feature_id": self.feature_id,
            "version": self.version,
            "lookback_seconds": self.lookback_seconds,
            "lookforward_seconds": self.lookforward_seconds,
        }


@dataclass(frozen=True)
class StatisticSpec:
    name: str
    parameters: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("A statistical test name is required.")
        _json_value(self.parameters, path="statistical_test.parameters")

    def payload(self) -> dict[str, Any]:
        return {"name": self.name, "parameters": _json_value(self.parameters, path="statistical_test.parameters")}


@dataclass(frozen=True)
class PartitionIdentity:
    """Time-aware partitions with horizon-aware purges between every split."""

    partition_id: str
    train_start: str
    train_end: str
    validation_start: str
    validation_end: str
    test_start: str
    test_end: str
    purge_seconds: int
    embargo_seconds: int
    random_seed: int
    horizon: OutcomeHorizon
    feature_lookback_seconds: int = 0
    sampling_algorithm: str = "NONE_V1"
    outcome_boundary_policy: str = "END_EXCLUSIVE_OUTCOME_CONTAINED"

    def __post_init__(self) -> None:
        if not isinstance(self.partition_id, str) or not self.partition_id.strip():
            raise ValueError("Partition identity requires an ID.")
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0
               for value in (self.purge_seconds, self.embargo_seconds, self.feature_lookback_seconds)):
            raise ValueError("Partition purge, embargo, and feature lookback values must be nonnegative integer seconds.")
        if isinstance(self.random_seed, bool) or not isinstance(self.random_seed, int):
            raise ValueError("Partition random seed must be an integer.")
        if not isinstance(self.sampling_algorithm, str) or not self.sampling_algorithm.strip():
            raise ValueError("Partition sampling algorithm/version is required.")
        if self.outcome_boundary_policy != "END_EXCLUSIVE_OUTCOME_CONTAINED":
            raise ValueError("Partitions must require each outcome window to end within its end-exclusive split.")
        train_start, train_end = _utc(self.train_start), _utc(self.train_end)
        validation_start, validation_end = _utc(self.validation_start), _utc(self.validation_end)
        test_start, test_end = _utc(self.test_start), _utc(self.test_end)
        if not train_start < train_end < validation_start < validation_end < test_start < test_end:
            raise ValueError("Train, validation, and test partitions must be strictly time ordered and non-overlapping.")
        required_gap = self.horizon.seconds + self.feature_lookback_seconds + self.purge_seconds + self.embargo_seconds
        if (validation_start - train_end).total_seconds() < required_gap:
            raise ValueError("Train and validation partitions violate the horizon-aware purge/embargo requirement.")
        if (test_start - validation_end).total_seconds() < required_gap:
            raise ValueError("Validation and test partitions violate the horizon-aware purge/embargo requirement.")

    def payload(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "train_start": normalized_utc(self.train_start),
            "train_end": normalized_utc(self.train_end),
            "validation_start": normalized_utc(self.validation_start),
            "validation_end": normalized_utc(self.validation_end),
            "test_start": normalized_utc(self.test_start),
            "test_end": normalized_utc(self.test_end),
            "purge_seconds": self.purge_seconds,
            "embargo_seconds": self.embargo_seconds,
            "random_seed": self.random_seed,
            "sampling_algorithm": self.sampling_algorithm,
            "feature_lookback_seconds": self.feature_lookback_seconds,
            "interval_semantics": "START_INCLUSIVE_END_EXCLUSIVE",
            "outcome_boundary_policy": self.outcome_boundary_policy,
            "outcome_horizon": self.horizon.payload(),
        }


@dataclass(frozen=True)
class HypothesisDefinition:
    """An immutable, falsifiable proposition declared before evaluation."""

    hypothesis_id: str
    version: int
    title: str
    proposition: str
    null_hypothesis: str
    alternative_hypothesis: str
    population_definition: str
    required_features: tuple[FeatureReference, ...]
    feature_transforms: Mapping[str, Any]
    entry_definition: str
    outcome_definition: str
    outcome_horizon: OutcomeHorizon
    comparator_definition: str
    inclusion_rules: tuple[str, ...]
    exclusion_rules: tuple[str, ...]
    minimum_sample_size: int
    statistical_test: StatisticSpec
    minimum_effect_size: float
    success_threshold: Mapping[str, Any]
    failure_threshold: Mapping[str, Any]
    multiple_testing_family: str
    partition: PartitionIdentity
    code_version: str
    config_version: str
    created_at: str
    predecessor_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.outcome_horizon, OutcomeHorizon) or not isinstance(self.partition, PartitionIdentity):
            raise ValueError("Hypotheses require typed outcome-horizon and partition contracts.")
        if not isinstance(self.statistical_test, StatisticSpec):
            raise ValueError("Hypotheses require a typed statistical-test contract.")
        if any(not isinstance(item, FeatureReference) for item in self.required_features):
            raise ValueError("Hypothesis feature references must use FeatureReference contracts.")
        required_text = (
            self.hypothesis_id, self.title, self.proposition, self.null_hypothesis,
            self.alternative_hypothesis, self.population_definition, self.entry_definition,
            self.outcome_definition, self.comparator_definition, self.multiple_testing_family,
            self.code_version, self.config_version,
        )
        if any(not isinstance(value, str) or not value.strip() for value in required_text):
            raise ValueError("Hypotheses require explicit identity, proposition, population, outcome, comparator, and provenance text.")
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version <= 0:
            raise ValueError("Hypothesis version must be a positive integer.")
        if self.predecessor_id is not None and (not isinstance(self.predecessor_id, str) or not self.predecessor_id.strip()):
            raise ValueError("Hypothesis predecessor ID must be nonempty text when provided.")
        if (isinstance(self.minimum_sample_size, bool) or not isinstance(self.minimum_sample_size, int)
                or self.minimum_sample_size <= 0):
            raise ValueError("Hypothesis minimum sample size must be positive.")
        finite_number(self.minimum_effect_size, name="minimum effect size", minimum=0.0)
        if self.partition.horizon.seconds != self.outcome_horizon.seconds:
            raise ValueError("Partition and outcome horizon must agree.")
        if not self.required_features and self.statistical_test.name != "DETERMINISTIC_NULL_EFFECT":
            raise ValueError("Non-null hypotheses require explicit feature references.")
        pairs = [(item.feature_id, item.version) for item in self.required_features]
        if len(pairs) != len(set(pairs)):
            raise ValueError("Hypothesis feature references must be unique.")
        if any(not isinstance(item, str) or not item.strip() for item in (*self.inclusion_rules, *self.exclusion_rules)):
            raise ValueError("Hypothesis inclusion and exclusion rules must be nonempty text.")
        expected_transforms = {f"{item.feature_id}@{item.version}" for item in self.required_features}
        if set(self.feature_transforms) != expected_transforms:
            raise ValueError("Feature transforms must declare exactly one transform for every required feature version.")
        required_lookback = max((item.lookback_seconds for item in self.required_features), default=0)
        if self.partition.feature_lookback_seconds != required_lookback:
            raise ValueError("Partition feature lookback must equal the maximum declared feature lookback.")
        _json_value(self.feature_transforms, path="feature_transforms")
        _json_value(self.success_threshold, path="success_threshold")
        _json_value(self.failure_threshold, path="failure_threshold")
        normalized_utc(self.created_at)

    def scientific_payload(self) -> dict[str, Any]:
        """Identity material, intentionally excluding only registration time."""
        return {
            "schema_version": PHASE_E_SCHEMA_VERSION,
            "canonicalization_version": CANONICALIZATION_VERSION,
            "hypothesis_id": self.hypothesis_id,
            "version": self.version,
            "title": self.title,
            "proposition": self.proposition,
            "null_hypothesis": self.null_hypothesis,
            "alternative_hypothesis": self.alternative_hypothesis,
            "population_definition": self.population_definition,
            "required_features": [item.payload() for item in sorted(self.required_features, key=lambda item: (item.feature_id, item.version))],
            "feature_transforms": _json_value(self.feature_transforms, path="feature_transforms"),
            "entry_definition": self.entry_definition,
            "outcome_definition": self.outcome_definition,
            "outcome_horizon": self.outcome_horizon.payload(),
            "comparator_definition": self.comparator_definition,
            "inclusion_rules": list(self.inclusion_rules),
            "exclusion_rules": list(self.exclusion_rules),
            "minimum_sample_size": self.minimum_sample_size,
            "statistical_test": self.statistical_test.payload(),
            "minimum_effect_size": finite_number(self.minimum_effect_size, name="minimum effect size", minimum=0.0),
            "success_threshold": _json_value(self.success_threshold, path="success_threshold"),
            "failure_threshold": _json_value(self.failure_threshold, path="failure_threshold"),
            "multiple_testing_family": self.multiple_testing_family,
            "partition": self.partition.payload(),
            "code_version": self.code_version,
            "config_version": self.config_version,
            "predecessor_id": self.predecessor_id,
        }

    def canonical_payload(self) -> dict[str, Any]:
        # Creation/registration time is immutable ledger metadata.  It is not
        # a scientific criterion and therefore cannot perturb identity.
        return self.scientific_payload()

    @property
    def definition_hash(self) -> str:
        return canonical_hash(self.canonical_payload())


@dataclass(frozen=True)
class ExperimentResult:
    sample_count: int
    effect_size: float
    p_value: float
    confidence_interval_low: float
    confidence_interval_high: float
    statistic: Mapping[str, Any]
    conclusion: ExperimentConclusion
    rejection_reason: RejectionReason | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.conclusion, ExperimentConclusion):
            raise ValueError("Experiment conclusion must use the declared conclusion vocabulary.")
        if self.rejection_reason is not None and not isinstance(self.rejection_reason, RejectionReason):
            raise ValueError("Experiment rejection reason must use the declared rejection vocabulary.")
        if isinstance(self.sample_count, bool) or not isinstance(self.sample_count, int) or self.sample_count < 0:
            raise ValueError("Experiment sample count must be a nonnegative integer.")
        finite_number(self.effect_size, name="effect size")
        finite_number(self.p_value, name="p value", minimum=0.0, maximum=1.0)
        low = finite_number(self.confidence_interval_low, name="confidence interval low")
        high = finite_number(self.confidence_interval_high, name="confidence interval high")
        if low > high:
            raise ValueError("Confidence interval lower bound cannot exceed upper bound.")
        _json_value(self.statistic, path="statistic")
        if self.conclusion == ExperimentConclusion.REJECTED and self.rejection_reason is None:
            raise ValueError("Rejected experiments require a durable rejection reason.")
        if self.conclusion == ExperimentConclusion.SURVIVED and self.rejection_reason is not None:
            raise ValueError("Surviving experiments cannot carry a rejection reason.")

    def payload(self) -> dict[str, Any]:
        return {
            "sample_count": self.sample_count,
            "effect_size": finite_number(self.effect_size, name="effect size"),
            "p_value": finite_number(self.p_value, name="p value", minimum=0.0, maximum=1.0),
            "confidence_interval": [
                finite_number(self.confidence_interval_low, name="confidence interval low"),
                finite_number(self.confidence_interval_high, name="confidence interval high"),
            ],
            "statistic": _json_value(self.statistic, path="statistic"),
            "conclusion": self.conclusion.value,
            "rejection_reason": self.rejection_reason.value if self.rejection_reason else None,
        }
