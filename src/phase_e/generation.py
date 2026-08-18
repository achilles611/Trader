"""Phase E.3 outcome-blind hypothesis generation.

This module deliberately has a much narrower read surface than E.2.  The
generator may read a completed materialization's membership, sampling design,
and predictor artifacts, but SQLite authorisation rejects every other source
table.  In particular, no outcome artifact or result table is available to the
candidate generator.

E.2 verification is invoked as a sealed integrity operation at the trust
boundary.  It returns fingerprints, not labels; the E.3 read boundary then
uses only predictor relations to derive a frozen hypothesis universe.
"""

from __future__ import annotations

import json
import math
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from .ledger import PhaseELedger
from .materialization import MaterializationStatus, PhaseEMaterializer
from .types import (
    FeatureReference,
    HypothesisDefinition,
    OutcomeHorizon,
    PartitionIdentity,
    StatisticSpec,
    canonical_hash,
    finite_number,
    normalized_utc,
    storage_json,
)


E3_GENERATOR_ALGORITHM = "OUTCOME_BLIND_SINGLE_FEATURE_V1"
E3_GENERATOR_CODE_VERSION = "phase-e3-generator-v1"
E3_GENERATOR_CONFIG_VERSION = "phase-e3-generator-config-v1"
PREDICATE_CANONICALIZATION_VERSION = "phase-e3-predicate-v1"
PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1 = "PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1"


class GenerationError(RuntimeError):
    """Base error for E.3 generation failures."""


class GenerationConflictError(GenerationError):
    """An immutable E.3 identity or lifecycle was contradicted."""


class GenerationIntegrityError(GenerationError):
    """Persisted E.3 evidence cannot be reconciled."""


class OutcomeAccessError(GenerationError):
    """The sealed E.3 predictor reader rejected a forbidden relation."""


class PredicateOperator(StrEnum):
    GT = "GT"
    GE = "GE"
    LT = "LT"
    LE = "LE"
    EQ = "EQ"
    BETWEEN = "BETWEEN"
    AND = "AND"


class ThresholdPolicy(StrEnum):
    FIXED_THRESHOLD_V1 = "FIXED_THRESHOLD_V1"
    SIGN_SPLIT_V1 = "SIGN_SPLIT_V1"
    TRAIN_QUANTILE_V1 = "TRAIN_QUANTILE_V1"


class GenerationStatus(StrEnum):
    REGISTERED = "REGISTERED"
    GENERATING = "GENERATING"
    CANDIDATES_FROZEN = "CANDIDATES_FROZEN"
    REGISTERING_HYPOTHESES = "REGISTERING_HYPOTHESES"
    COMPLETE = "COMPLETE"


_ATOMIC_OPERATORS = frozenset({
    PredicateOperator.GT, PredicateOperator.GE, PredicateOperator.LT,
    PredicateOperator.LE, PredicateOperator.EQ, PredicateOperator.BETWEEN,
})
_SINGLE_FEATURE_GENERATED_OPERATORS = frozenset({
    PredicateOperator.GT, PredicateOperator.GE, PredicateOperator.LT,
    PredicateOperator.LE, PredicateOperator.EQ,
})
_OPERATOR_ORDER = {item: index for index, item in enumerate((
    PredicateOperator.GT, PredicateOperator.GE, PredicateOperator.LT,
    PredicateOperator.LE, PredicateOperator.EQ, PredicateOperator.BETWEEN,
    PredicateOperator.AND,
))}
_PROHIBITED_IDENTITY_FEATURES = frozenset({"wallet", "wallet_id", "wallet_address", "symbol", "asset", "market_symbol"})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _feature_key(feature: FeatureReference) -> str:
    return f"{feature.feature_id}@{feature.version}"


def _finite_float(value: Any, *, name: str) -> float:
    return finite_number(value, name=name)


def _canonical_json(raw: Any, *, name: str) -> Any:
    if not isinstance(raw, str):
        raise GenerationIntegrityError(f"{name} must be canonical JSON text.")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise GenerationIntegrityError(f"{name} is malformed.") from exc
    if storage_json(value) != raw:
        raise GenerationIntegrityError(f"{name} is not canonical JSON.")
    return value


@dataclass(frozen=True)
class Predicate:
    """A typed, deliberately tiny predicate AST.

    ``AND`` exists so future explicitly-predeclared interactions can be
    represented without introducing an executable expression language.  This
    commissioning generator intentionally emits only one atomic predicate.
    """

    operator: PredicateOperator
    feature: FeatureReference | None = None
    threshold: float | None = None
    upper_threshold: float | None = None
    children: tuple["Predicate", ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.operator, PredicateOperator):
            raise ValueError("Predicate operator must use the closed E.3 vocabulary.")
        if self.operator is PredicateOperator.AND:
            if self.feature is not None or self.threshold is not None or self.upper_threshold is not None:
                raise ValueError("AND predicates contain only child predicates.")
            if len(self.children) < 2 or any(not isinstance(item, Predicate) for item in self.children):
                raise ValueError("AND predicates require at least two typed children.")
            hashes = [item.predicate_hash for item in self.children]
            if len(set(hashes)) != len(hashes):
                raise ValueError("AND predicates may not repeat a semantic child.")
            return
        if self.operator not in _ATOMIC_OPERATORS or not isinstance(self.feature, FeatureReference) or self.children:
            raise ValueError("Atomic predicates require exactly one typed feature and no children.")
        _finite_float(self.threshold, name="predicate threshold")
        if self.operator is PredicateOperator.BETWEEN:
            _finite_float(self.upper_threshold, name="predicate upper threshold")
            if float(self.threshold) > float(self.upper_threshold):
                raise ValueError("BETWEEN lower threshold must not exceed its upper threshold.")
        elif self.upper_threshold is not None:
            raise ValueError("Only BETWEEN predicates may declare an upper threshold.")

    def payload(self) -> dict[str, Any]:
        if self.operator is PredicateOperator.AND:
            return {
                "schema": PREDICATE_CANONICALIZATION_VERSION,
                "node": "AND",
                "children": [item.payload() for item in sorted(self.children, key=lambda item: item.predicate_hash)],
            }
        payload: dict[str, Any] = {
            "schema": PREDICATE_CANONICALIZATION_VERSION,
            "node": "COMPARISON",
            "operator": self.operator.value,
            "feature": self.feature.payload() if self.feature else None,
            "threshold": _finite_float(self.threshold, name="predicate threshold"),
        }
        if self.operator is PredicateOperator.BETWEEN:
            payload["upper_threshold"] = _finite_float(self.upper_threshold, name="predicate upper threshold")
        return payload

    @property
    def predicate_hash(self) -> str:
        return canonical_hash(self.payload())

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "Predicate":
        if not isinstance(payload, Mapping) or payload.get("schema") != PREDICATE_CANONICALIZATION_VERSION:
            raise ValueError("Predicate payload has an unsupported schema.")
        node = payload.get("node")
        if node == "AND":
            children = payload.get("children")
            if not isinstance(children, list):
                raise ValueError("AND predicate payload requires children.")
            return cls(PredicateOperator.AND, children=tuple(cls.from_payload(item) for item in children))
        if node != "COMPARISON" or not isinstance(payload.get("feature"), Mapping):
            raise ValueError("Predicate payload has an unsupported node.")
        feature_raw = payload["feature"]
        feature = FeatureReference(
            feature_id=feature_raw["feature_id"], version=feature_raw["version"],
            lookback_seconds=feature_raw.get("lookback_seconds", 0),
            lookforward_seconds=feature_raw.get("lookforward_seconds", 0),
        )
        return cls(
            PredicateOperator(payload["operator"]), feature=feature, threshold=payload.get("threshold"),
            upper_threshold=payload.get("upper_threshold"),
        )


@dataclass(frozen=True)
class StatisticalTestPlan:
    """An E.4 test contract; E.3 stores it but never executes it."""

    test_id: str
    direction: str
    effect_metric: str
    comparator_policy: str
    sampling_weights_required: bool
    resampling_seed: int | None
    resample_count: int | None
    significance_threshold: float
    minimum_effect_size: float
    minimum_sample_size: int

    def __post_init__(self) -> None:
        if not isinstance(self.test_id, str) or not self.test_id.strip():
            raise ValueError("Statistical test plan requires a test identifier/version.")
        if self.direction not in {"TWO_SIDED", "DIRECTIONAL"}:
            raise ValueError("Statistical test direction must be TWO_SIDED or DIRECTIONAL.")
        if not isinstance(self.effect_metric, str) or not self.effect_metric.strip():
            raise ValueError("Statistical test plan requires a declared effect metric.")
        if self.comparator_policy != PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1:
            raise ValueError("Only the frozen predicate-complement comparator is supported in E.3.")
        if not isinstance(self.sampling_weights_required, bool):
            raise ValueError("Sampling-weight requirement must be explicit.")
        if self.resampling_seed is not None and (isinstance(self.resampling_seed, bool) or not isinstance(self.resampling_seed, int)):
            raise ValueError("Resampling seed must be an integer when supplied.")
        if self.resample_count is not None and (isinstance(self.resample_count, bool) or not isinstance(self.resample_count, int) or self.resample_count <= 0):
            raise ValueError("Resample count must be positive when supplied.")
        if (self.resampling_seed is None) != (self.resample_count is None):
            raise ValueError("Resampling seed and count must be declared together.")
        finite_number(self.significance_threshold, name="significance threshold", minimum=0.0, maximum=1.0)
        if float(self.significance_threshold) == 0.0:
            raise ValueError("Significance threshold must be greater than zero.")
        finite_number(self.minimum_effect_size, name="minimum effect size", minimum=0.0)
        if isinstance(self.minimum_sample_size, bool) or not isinstance(self.minimum_sample_size, int) or self.minimum_sample_size <= 0:
            raise ValueError("Minimum sample size must be positive.")

    def payload(self) -> dict[str, Any]:
        return {
            "test_id": self.test_id,
            "direction": self.direction,
            "effect_metric": self.effect_metric,
            "comparator_policy": self.comparator_policy,
            "sampling_weights_required": self.sampling_weights_required,
            "resampling_seed": self.resampling_seed,
            "resample_count": self.resample_count,
            "significance_threshold": finite_number(self.significance_threshold, name="significance threshold", minimum=0.0, maximum=1.0),
            "minimum_effect_size": finite_number(self.minimum_effect_size, name="minimum effect size", minimum=0.0),
            "minimum_sample_size": self.minimum_sample_size,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "StatisticalTestPlan":
        return cls(**dict(payload))


@dataclass(frozen=True)
class HypothesisFamilySpec:
    """Immutable, predeclared E.3 search space."""

    family_id: str
    version: int
    title: str
    description: str
    allowed_features: tuple[FeatureReference, ...]
    allowed_operators: tuple[PredicateOperator, ...]
    threshold_policy: ThresholdPolicy
    fixed_thresholds: Mapping[str, tuple[float, ...]]
    quantiles: tuple[float, ...]
    permitted_interaction_order: int
    comparator_policy: str
    compatible_horizons: tuple[OutcomeHorizon, ...]
    statistical_test_plan: StatisticalTestPlan
    minimum_training_support: int
    minimum_feature_coverage: float
    maximum_candidates: int
    maximum_candidates_per_feature: int
    global_budget_behavior: str
    missing_feature_policy: str
    duplicate_semantics: str
    multiple_testing_family_rule: str
    generation_seed: int
    generator_algorithm: str = E3_GENERATOR_ALGORITHM
    generator_code_version: str = E3_GENERATOR_CODE_VERSION
    generator_config_version: str = E3_GENERATOR_CONFIG_VERSION

    def __post_init__(self) -> None:
        required_text = (
            self.family_id, self.title, self.description, self.comparator_policy,
            self.global_budget_behavior, self.missing_feature_policy, self.duplicate_semantics,
            self.multiple_testing_family_rule, self.generator_algorithm, self.generator_code_version,
            self.generator_config_version,
        )
        if any(not isinstance(value, str) or not value.strip() for value in required_text):
            raise ValueError("Hypothesis family requires explicit identity, policy, and generator provenance.")
        if isinstance(self.version, bool) or not isinstance(self.version, int) or self.version <= 0:
            raise ValueError("Hypothesis-family version must be positive.")
        if not self.allowed_features or any(not isinstance(item, FeatureReference) for item in self.allowed_features):
            raise ValueError("Hypothesis family requires at least one typed feature reference.")
        feature_pairs = [(item.feature_id, item.version) for item in self.allowed_features]
        if len(feature_pairs) != len(set(feature_pairs)):
            raise ValueError("Hypothesis-family feature references must be unique.")
        for feature in self.allowed_features:
            if feature.feature_id.lower() in _PROHIBITED_IDENTITY_FEATURES:
                raise ValueError("E.3 families may not search wallet or symbol identities.")
        if not self.allowed_operators or any(not isinstance(item, PredicateOperator) for item in self.allowed_operators):
            raise ValueError("Hypothesis family must use typed predicate operators.")
        if len(set(self.allowed_operators)) != len(self.allowed_operators):
            raise ValueError("Hypothesis-family predicate operators must be unique.")
        if not isinstance(self.threshold_policy, ThresholdPolicy):
            raise ValueError("Hypothesis family requires a versioned threshold policy.")
        if self.permitted_interaction_order != 1:
            raise ValueError("E.3 commissioning permits exactly single-feature hypotheses; AND is reserved for a future family version.")
        if any(item not in _SINGLE_FEATURE_GENERATED_OPERATORS for item in self.allowed_operators):
            raise ValueError("Initial E.3 single-feature generation supports GT, GE, LT, LE, and EQ only.")
        if self.threshold_policy is ThresholdPolicy.SIGN_SPLIT_V1 and not set(self.allowed_operators).issubset({PredicateOperator.GT, PredicateOperator.LT}):
            raise ValueError("SIGN_SPLIT_V1 supports only GT and LT predicates.")
        if self.threshold_policy is ThresholdPolicy.FIXED_THRESHOLD_V1:
            expected = {_feature_key(item) for item in self.allowed_features}
            if not self.fixed_thresholds or not set(self.fixed_thresholds).issubset(expected):
                raise ValueError("Fixed thresholds must be supplied only for declared feature references.")
            for key, values in self.fixed_thresholds.items():
                if not isinstance(values, tuple) or not values:
                    raise ValueError(f"Fixed threshold {key} must be a nonempty tuple.")
                for value in values:
                    finite_number(value, name=f"fixed threshold {key}")
        elif self.fixed_thresholds:
            raise ValueError("Only FIXED_THRESHOLD_V1 accepts fixed thresholds.")
        if self.threshold_policy is ThresholdPolicy.TRAIN_QUANTILE_V1:
            if not self.quantiles:
                raise ValueError("TRAIN_QUANTILE_V1 requires predeclared quantiles.")
            if any(not isinstance(value, (int, float)) or isinstance(value, bool) or not 0.0 < float(value) <= 1.0 for value in self.quantiles):
                raise ValueError("Quantiles must be finite values in (0, 1].")
        elif self.quantiles:
            raise ValueError("Only TRAIN_QUANTILE_V1 accepts quantiles.")
        if self.comparator_policy != PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1:
            raise ValueError("E.3 currently supports only the predeclared predicate-complement comparator.")
        if not self.compatible_horizons or any(not isinstance(item, OutcomeHorizon) for item in self.compatible_horizons):
            raise ValueError("Hypothesis family requires at least one typed compatible horizon.")
        if len({item.seconds for item in self.compatible_horizons}) != len(self.compatible_horizons):
            raise ValueError("Compatible horizons must be unique.")
        if not isinstance(self.statistical_test_plan, StatisticalTestPlan):
            raise ValueError("Hypothesis family requires a typed statistical-test plan.")
        if self.statistical_test_plan.comparator_policy != self.comparator_policy:
            raise ValueError("Family comparator and statistical-test comparator must agree.")
        for name, value in (("minimum training support", self.minimum_training_support),
                            ("maximum candidates", self.maximum_candidates),
                            ("maximum candidates per feature", self.maximum_candidates_per_feature)):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name.capitalize()} must be positive.")
        finite_number(self.minimum_feature_coverage, name="minimum feature coverage", minimum=0.0, maximum=1.0)
        if float(self.minimum_feature_coverage) == 0.0:
            raise ValueError("Minimum feature coverage must be greater than zero.")
        if isinstance(self.generation_seed, bool) or not isinstance(self.generation_seed, int):
            raise ValueError("Generation seed must be an integer.")
        if self.global_budget_behavior != "CANONICAL_ORDER_TRUNCATE_V1":
            raise ValueError("E.3 supports only deterministic canonical budget truncation.")
        if self.missing_feature_policy != "SUPPRESS_WITH_DURABLE_REASON_V1":
            raise ValueError("E.3 must retain missing-feature suppression evidence.")
        if self.duplicate_semantics != "CANONICAL_PREDICATE_AND_E1_DEFINITION_V1":
            raise ValueError("E.3 must use canonical predicate/E.1 semantic deduplication.")
        if self.multiple_testing_family_rule != "RUN_MATERIALIZATION_TEST_PLAN_V1":
            raise ValueError("E.3 must freeze correction families using the declared run/materialization/test-plan rule.")

    def payload(self) -> dict[str, Any]:
        return {
            "schema": "phase-e3-hypothesis-family-v1",
            "family_id": self.family_id,
            "version": self.version,
            "title": self.title,
            "description": self.description,
            "allowed_features": [item.payload() for item in self.allowed_features],
            "allowed_operators": [item.value for item in self.allowed_operators],
            "threshold_policy": self.threshold_policy.value,
            "fixed_thresholds": {key: list(values) for key, values in sorted(self.fixed_thresholds.items())},
            "quantiles": list(self.quantiles),
            "permitted_interaction_order": self.permitted_interaction_order,
            "comparator_policy": self.comparator_policy,
            "compatible_horizons": [item.payload() for item in self.compatible_horizons],
            "statistical_test_plan": self.statistical_test_plan.payload(),
            "minimum_training_support": self.minimum_training_support,
            "minimum_feature_coverage": finite_number(self.minimum_feature_coverage, name="minimum feature coverage", minimum=0.0, maximum=1.0),
            "maximum_candidates": self.maximum_candidates,
            "maximum_candidates_per_feature": self.maximum_candidates_per_feature,
            "global_budget_behavior": self.global_budget_behavior,
            "missing_feature_policy": self.missing_feature_policy,
            "duplicate_semantics": self.duplicate_semantics,
            "multiple_testing_family_rule": self.multiple_testing_family_rule,
            "generation_seed": self.generation_seed,
            "generator_algorithm": self.generator_algorithm,
            "generator_code_version": self.generator_code_version,
            "generator_config_version": self.generator_config_version,
        }

    @property
    def fingerprint(self) -> str:
        return canonical_hash(self.payload())

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "HypothesisFamilySpec":
        if not isinstance(payload, Mapping) or payload.get("schema") != "phase-e3-hypothesis-family-v1":
            raise ValueError("Unsupported hypothesis-family payload.")
        return cls(
            family_id=payload["family_id"], version=payload["version"], title=payload["title"], description=payload["description"],
            allowed_features=tuple(FeatureReference(**item) for item in payload["allowed_features"]),
            allowed_operators=tuple(PredicateOperator(item) for item in payload["allowed_operators"]),
            threshold_policy=ThresholdPolicy(payload["threshold_policy"]),
            fixed_thresholds={key: tuple(values) for key, values in payload["fixed_thresholds"].items()},
            quantiles=tuple(payload["quantiles"]), permitted_interaction_order=payload["permitted_interaction_order"],
            comparator_policy=payload["comparator_policy"],
            compatible_horizons=tuple(OutcomeHorizon(item["seconds"]) for item in payload["compatible_horizons"]),
            statistical_test_plan=StatisticalTestPlan.from_payload(payload["statistical_test_plan"]),
            minimum_training_support=payload["minimum_training_support"], minimum_feature_coverage=payload["minimum_feature_coverage"],
            maximum_candidates=payload["maximum_candidates"], maximum_candidates_per_feature=payload["maximum_candidates_per_feature"],
            global_budget_behavior=payload["global_budget_behavior"], missing_feature_policy=payload["missing_feature_policy"],
            duplicate_semantics=payload["duplicate_semantics"], multiple_testing_family_rule=payload["multiple_testing_family_rule"],
            generation_seed=payload["generation_seed"], generator_algorithm=payload["generator_algorithm"],
            generator_code_version=payload["generator_code_version"], generator_config_version=payload["generator_config_version"],
        )


def wallet_action_sign_family(*, minimum_training_support: int = 20, maximum_candidates: int = 2) -> HypothesisFamilySpec:
    """The small, two-sided control family suitable for causal wallet action."""
    return HypothesisFamilySpec(
        family_id="WALLET_ACTION_SIGN_V1", version=1,
        title="Wallet action sign control", description="Outcome-blind control questions over causal wallet action sign.",
        allowed_features=(FeatureReference("wallet_action", 1),),
        allowed_operators=(PredicateOperator.GT, PredicateOperator.LT), threshold_policy=ThresholdPolicy.SIGN_SPLIT_V1,
        fixed_thresholds={}, quantiles=(), permitted_interaction_order=1,
        comparator_policy=PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1, compatible_horizons=(OutcomeHorizon(5),),
        statistical_test_plan=StatisticalTestPlan(
            test_id="E4_WEIGHTED_NET_OUTCOME_DISTRIBUTION_DIFFERENCE_V1", direction="TWO_SIDED",
            effect_metric="DECLARED_PHASE_E2_NET_OUTCOME_V2", comparator_policy=PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1,
            sampling_weights_required=True, resampling_seed=17, resample_count=1_000,
            significance_threshold=0.05, minimum_effect_size=0.0, minimum_sample_size=minimum_training_support,
        ),
        minimum_training_support=minimum_training_support, minimum_feature_coverage=1.0,
        maximum_candidates=maximum_candidates, maximum_candidates_per_feature=maximum_candidates,
        global_budget_behavior="CANONICAL_ORDER_TRUNCATE_V1", missing_feature_policy="SUPPRESS_WITH_DURABLE_REASON_V1",
        duplicate_semantics="CANONICAL_PREDICATE_AND_E1_DEFINITION_V1",
        multiple_testing_family_rule="RUN_MATERIALIZATION_TEST_PLAN_V1", generation_seed=0,
    )


@dataclass(frozen=True)
class GenerationRunSpec:
    """Exact, timestamp-independent identity for one E.3 generator execution."""

    materialization_id: str
    materialization_specification_hash: str
    corpus_fingerprint: str
    source_fingerprint: str
    membership_fingerprint: str
    sampling_design_fingerprint: str
    feature_fingerprint: str
    complete_artifact_fingerprint: str
    family_id: str
    family_version: int
    family_fingerprint: str
    partition_name: str
    generation_seed: int
    candidate_budget: int
    generator_algorithm: str
    generator_code_version: str
    generator_config_version: str

    def __post_init__(self) -> None:
        if self.partition_name != "train":
            raise ValueError("E.3 generation is restricted to the train partition.")
        if any(not isinstance(value, str) or not value for value in (
            self.materialization_id, self.materialization_specification_hash, self.corpus_fingerprint,
            self.source_fingerprint, self.membership_fingerprint, self.sampling_design_fingerprint,
            self.feature_fingerprint, self.complete_artifact_fingerprint, self.family_id, self.family_fingerprint,
            self.generator_algorithm, self.generator_code_version, self.generator_config_version,
        )):
            raise ValueError("Generation run requires complete immutable provenance.")
        if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0
               for value in (self.family_version, self.candidate_budget)):
            raise ValueError("Generation family version and candidate budget must be positive.")
        if isinstance(self.generation_seed, bool) or not isinstance(self.generation_seed, int):
            raise ValueError("Generation seed must be an integer.")

    def payload(self) -> dict[str, Any]:
        return {
            "schema": "phase-e3-generation-run-v1", "materialization_id": self.materialization_id,
            "materialization_specification_hash": self.materialization_specification_hash,
            "corpus_fingerprint": self.corpus_fingerprint, "source_fingerprint": self.source_fingerprint,
            "membership_fingerprint": self.membership_fingerprint,
            "sampling_design_fingerprint": self.sampling_design_fingerprint,
            "feature_fingerprint": self.feature_fingerprint,
            "complete_artifact_fingerprint": self.complete_artifact_fingerprint,
            "family_id": self.family_id, "family_version": self.family_version, "family_fingerprint": self.family_fingerprint,
            "partition_name": self.partition_name, "generation_seed": self.generation_seed,
            "candidate_budget": self.candidate_budget, "generator_algorithm": self.generator_algorithm,
            "generator_code_version": self.generator_code_version, "generator_config_version": self.generator_config_version,
        }

    @property
    def run_id(self) -> str:
        return "e3-" + canonical_hash(self.payload())[:32]


@dataclass(frozen=True)
class HypothesisProposal:
    proposal_id: str
    generation_run_id: str
    family_id: str
    family_version: int
    source_materialization_id: str
    predicate: Predicate
    required_features: tuple[FeatureReference, ...]
    threshold_provenance: Mapping[str, Any]
    training_support_count: int
    training_missing_count: int
    training_population_count: int
    comparator_policy: str
    outcome_horizon: OutcomeHorizon
    proposed_hypothesis_id: str
    proposed_hypothesis_version: int
    proposed_definition_hash: str
    proposed_definition: Mapping[str, Any]
    multiple_testing_family_id: str
    generation_provenance: Mapping[str, Any]

    def payload(self) -> dict[str, Any]:
        return {
            "proposal_id": self.proposal_id, "generation_run_id": self.generation_run_id,
            "family_id": self.family_id, "family_version": self.family_version,
            "source_materialization_id": self.source_materialization_id, "predicate": self.predicate.payload(),
            "predicate_hash": self.predicate.predicate_hash,
            "required_features": [item.payload() for item in self.required_features],
            "threshold_provenance": dict(self.threshold_provenance),
            "training_support_count": self.training_support_count, "training_missing_count": self.training_missing_count,
            "training_population_count": self.training_population_count, "comparator_policy": self.comparator_policy,
            "outcome_horizon": self.outcome_horizon.payload(), "proposed_hypothesis_id": self.proposed_hypothesis_id,
            "proposed_hypothesis_version": self.proposed_hypothesis_version,
            "proposed_definition_hash": self.proposed_definition_hash,
            "multiple_testing_family_id": self.multiple_testing_family_id,
            "generation_provenance": dict(self.generation_provenance),
        }


class _OutcomeBlindReader:
    """One allow-listed SQLite reader for E.3 predictor evidence only."""

    _ALLOWED_RELATIONS = frozenset({
        "phase_e_materializations", "phase_e_materialization_membership",
        "phase_e_materialization_features", "phase_e_materialization_sampling_design",
    })

    def __init__(self, path: Path) -> None:
        self.path = path
        self.denied_relations: list[str] = []

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=20, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=20000")

        def authorizer(action: int, first: str | None, second: str | None, database: str | None, trigger: str | None) -> int:
            if action in {sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH}:
                self.denied_relations.append("ATTACH_OR_DETACH")
                return sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and isinstance(first, str) and first.lower() not in self._ALLOWED_RELATIONS:
                self.denied_relations.append(first.lower())
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorizer)
        try:
            yield connection
        except sqlite3.DatabaseError as exc:
            if self.denied_relations:
                raise OutcomeAccessError(
                    "E.3 predictor reader refused non-predictor relation access: " + ", ".join(sorted(set(self.denied_relations)))
                ) from exc
            raise
        finally:
            connection.close()


@dataclass(frozen=True)
class _E2Snapshot:
    run_spec: GenerationRunSpec
    materialization_spec: Mapping[str, Any]
    verification: Mapping[str, Any]


@dataclass(frozen=True)
class _PredictorFeature:
    feature: FeatureReference
    population_count: int
    missing_count: int
    values: tuple[tuple[str, float], ...]
    rows_fingerprint: str
    available: bool
    missing_reasons: Mapping[str, int]


@dataclass(frozen=True)
class _Candidate:
    feature_index: int
    feature: FeatureReference
    predicate: Predicate
    threshold_provenance: Mapping[str, Any]
    support_count: int
    missing_count: int
    population_count: int

    @property
    def order_key(self) -> tuple[int, float, int, str]:
        threshold = float(self.predicate.threshold or 0.0)
        return (self.feature_index, threshold, _OPERATOR_ORDER[self.predicate.operator], self.predicate.predicate_hash)


class PhaseEHypothesisGenerator:
    """Append-only E.3 generator with no prediction, signal, or trade authority."""

    TRADING_AUTHORITY = False

    def __init__(self, database_path: str | Path) -> None:
        self.path = Path(database_path)
        self._initialized = False

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=20, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=20000")
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except Exception:
                connection.rollback()
                raise
            else:
                connection.commit()

    @contextmanager
    def _predictor_connection(self, generation_run_id: str | None = None) -> Iterator[sqlite3.Connection]:
        reader = _OutcomeBlindReader(self.path)
        try:
            with reader.connection() as connection:
                yield connection
        except OutcomeAccessError as exc:
            self._last_denied_predictor_relations = tuple(sorted(set(reader.denied_relations)))
            if generation_run_id is not None:
                self._record_access_violation(generation_run_id, self._last_denied_predictor_relations, str(exc))
            raise

    def initialize(self) -> None:
        if self._initialized:
            return
        PhaseELedger(self.path).initialize()
        PhaseEMaterializer(self.path).initialize()
        with self._write() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS phase_e_hypothesis_families (
                    family_id TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version > 0),
                    specification_json TEXT NOT NULL,
                    specification_hash TEXT NOT NULL,
                    registered_at TEXT NOT NULL,
                    PRIMARY KEY(family_id, version)
                );
                CREATE TABLE IF NOT EXISTS phase_e_generation_runs (
                    generation_run_id TEXT PRIMARY KEY,
                    generation_specification_json TEXT NOT NULL,
                    generation_specification_hash TEXT NOT NULL,
                    materialization_id TEXT NOT NULL,
                    family_id TEXT NOT NULL,
                    family_version INTEGER NOT NULL,
                    family_fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('REGISTERED','GENERATING','CANDIDATES_FROZEN','REGISTERING_HYPOTHESES','COMPLETE')),
                    registered_at TEXT NOT NULL,
                    completed_at TEXT,
                    predictor_summary_fingerprint TEXT,
                    training_population_count INTEGER,
                    raw_candidate_count INTEGER,
                    unique_candidate_count INTEGER,
                    suppressed_counts_json TEXT,
                    registered_hypothesis_count INTEGER,
                    hypothesis_universe_fingerprint TEXT,
                    outcome_reads_attempted INTEGER NOT NULL DEFAULT 0 CHECK(outcome_reads_attempted = 0),
                    FOREIGN KEY(family_id, family_version) REFERENCES phase_e_hypothesis_families(family_id, version)
                );
                CREATE TABLE IF NOT EXISTS phase_e_generation_events (
                    event_id TEXT PRIMARY KEY,
                    generation_run_id TEXT NOT NULL REFERENCES phase_e_generation_runs(generation_run_id),
                    event_type TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    event_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e_hypothesis_proposals (
                    generation_run_id TEXT NOT NULL REFERENCES phase_e_generation_runs(generation_run_id),
                    proposal_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                    predicate_json TEXT NOT NULL,
                    predicate_hash TEXT NOT NULL,
                    required_features_json TEXT NOT NULL,
                    threshold_provenance_json TEXT NOT NULL,
                    training_support_count INTEGER NOT NULL CHECK(training_support_count >= 0),
                    training_missing_count INTEGER NOT NULL CHECK(training_missing_count >= 0),
                    training_population_count INTEGER NOT NULL CHECK(training_population_count >= 0),
                    comparator_policy TEXT NOT NULL,
                    outcome_horizon_seconds INTEGER NOT NULL,
                    proposed_hypothesis_id TEXT NOT NULL,
                    proposed_hypothesis_version INTEGER NOT NULL CHECK(proposed_hypothesis_version > 0),
                    proposed_definition_json TEXT NOT NULL,
                    proposed_definition_hash TEXT NOT NULL,
                    multiple_testing_family_id TEXT NOT NULL,
                    generation_provenance_json TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    PRIMARY KEY(generation_run_id, proposal_id),
                    UNIQUE(generation_run_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS phase_e_generation_suppressions (
                    generation_run_id TEXT NOT NULL REFERENCES phase_e_generation_runs(generation_run_id),
                    suppression_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                    reason TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    PRIMARY KEY(generation_run_id, suppression_id),
                    UNIQUE(generation_run_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS phase_e_generation_e1_mappings (
                    generation_run_id TEXT NOT NULL,
                    proposal_id TEXT NOT NULL,
                    experiment_id TEXT NOT NULL,
                    hypothesis_id TEXT NOT NULL,
                    hypothesis_version INTEGER NOT NULL,
                    definition_hash TEXT NOT NULL,
                    mapping_hash TEXT NOT NULL,
                    registered_at TEXT NOT NULL,
                    PRIMARY KEY(generation_run_id, proposal_id),
                    FOREIGN KEY(generation_run_id, proposal_id)
                        REFERENCES phase_e_hypothesis_proposals(generation_run_id, proposal_id)
                );
                CREATE TABLE IF NOT EXISTS phase_e_generation_manifests (
                    generation_run_id TEXT PRIMARY KEY REFERENCES phase_e_generation_runs(generation_run_id),
                    manifest_json TEXT NOT NULL,
                    manifest_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e_generation_access_violations (
                    violation_id TEXT PRIMARY KEY,
                    generation_run_id TEXT REFERENCES phase_e_generation_runs(generation_run_id),
                    relation_name TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    detail_json TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_phase_e_generation_runs_status ON phase_e_generation_runs(status, registered_at);
                CREATE INDEX IF NOT EXISTS idx_phase_e_generation_events ON phase_e_generation_events(generation_run_id, event_at);
                CREATE TRIGGER IF NOT EXISTS phase_e_hypothesis_families_no_update
                    BEFORE UPDATE ON phase_e_hypothesis_families BEGIN SELECT RAISE(ABORT, 'Phase E.3 hypothesis families are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_hypothesis_families_no_delete
                    BEFORE DELETE ON phase_e_hypothesis_families BEGIN SELECT RAISE(ABORT, 'Phase E.3 hypothesis families cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_runs_inputs_immutable
                    BEFORE UPDATE OF generation_run_id, generation_specification_json, generation_specification_hash, materialization_id,
                        family_id, family_version, family_fingerprint, registered_at ON phase_e_generation_runs
                    BEGIN SELECT RAISE(ABORT, 'Phase E.3 generation inputs are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_runs_no_delete
                    BEFORE DELETE ON phase_e_generation_runs BEGIN SELECT RAISE(ABORT, 'Phase E.3 generation runs cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_events_no_update
                    BEFORE UPDATE ON phase_e_generation_events BEGIN SELECT RAISE(ABORT, 'Phase E.3 lifecycle events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_events_no_delete
                    BEFORE DELETE ON phase_e_generation_events BEGIN SELECT RAISE(ABORT, 'Phase E.3 lifecycle events cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_hypothesis_proposals_only_generating
                    BEFORE INSERT ON phase_e_hypothesis_proposals
                    WHEN (SELECT status FROM phase_e_generation_runs WHERE generation_run_id=NEW.generation_run_id) <> 'GENERATING'
                    BEGIN SELECT RAISE(ABORT, 'E.3 proposals may be inserted only while generating'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_hypothesis_proposals_no_update
                    BEFORE UPDATE ON phase_e_hypothesis_proposals BEGIN SELECT RAISE(ABORT, 'E.3 proposals are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_hypothesis_proposals_no_delete
                    BEFORE DELETE ON phase_e_hypothesis_proposals BEGIN SELECT RAISE(ABORT, 'E.3 proposals cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_suppressions_only_generating
                    BEFORE INSERT ON phase_e_generation_suppressions
                    WHEN (SELECT status FROM phase_e_generation_runs WHERE generation_run_id=NEW.generation_run_id) <> 'GENERATING'
                    BEGIN SELECT RAISE(ABORT, 'E.3 suppressions may be inserted only while generating'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_suppressions_no_update
                    BEFORE UPDATE ON phase_e_generation_suppressions BEGIN SELECT RAISE(ABORT, 'E.3 suppressions are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_suppressions_no_delete
                    BEFORE DELETE ON phase_e_generation_suppressions BEGIN SELECT RAISE(ABORT, 'E.3 suppressions cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_e1_mappings_only_registering
                    BEFORE INSERT ON phase_e_generation_e1_mappings
                    WHEN (SELECT status FROM phase_e_generation_runs WHERE generation_run_id=NEW.generation_run_id) <> 'REGISTERING_HYPOTHESES'
                    BEGIN SELECT RAISE(ABORT, 'E.3 E.1 mappings may be inserted only while registering hypotheses'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_e1_mappings_no_update
                    BEFORE UPDATE ON phase_e_generation_e1_mappings BEGIN SELECT RAISE(ABORT, 'E.3 E.1 mappings are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_e1_mappings_no_delete
                    BEFORE DELETE ON phase_e_generation_e1_mappings BEGIN SELECT RAISE(ABORT, 'E.3 E.1 mappings cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_manifests_only_registering
                    BEFORE INSERT ON phase_e_generation_manifests
                    WHEN (SELECT status FROM phase_e_generation_runs WHERE generation_run_id=NEW.generation_run_id) <> 'REGISTERING_HYPOTHESES'
                    BEGIN SELECT RAISE(ABORT, 'E.3 manifests may be inserted only while registering hypotheses'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_manifests_no_update
                    BEFORE UPDATE ON phase_e_generation_manifests BEGIN SELECT RAISE(ABORT, 'E.3 manifests are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_manifests_no_delete
                    BEFORE DELETE ON phase_e_generation_manifests BEGIN SELECT RAISE(ABORT, 'E.3 manifests cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_access_violations_no_update
                    BEFORE UPDATE ON phase_e_generation_access_violations BEGIN SELECT RAISE(ABORT, 'E.3 access violations are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_generation_access_violations_no_delete
                    BEFORE DELETE ON phase_e_generation_access_violations BEGIN SELECT RAISE(ABORT, 'E.3 access violations cannot be deleted'); END;
                """
            )
        self._initialized = True

    # ----- family and planning -----------------------------------------------------
    def register_family(self, family: HypothesisFamilySpec, *, registered_at: str | None = None) -> dict[str, Any]:
        self.initialize()
        at = normalized_utc(registered_at or _now())
        payload, fingerprint = family.payload(), family.fingerprint
        with self._write() as connection:
            row = connection.execute(
                "SELECT * FROM phase_e_hypothesis_families WHERE family_id=? AND version=?", (family.family_id, family.version),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO phase_e_hypothesis_families VALUES (?, ?, ?, ?, ?)",
                    (family.family_id, family.version, storage_json(payload), fingerprint, at),
                )
            elif row["specification_hash"] != fingerprint or _canonical_json(row["specification_json"], name="hypothesis family") != payload:
                raise GenerationConflictError("Hypothesis-family version already exists with a different predeclared search space.")
        return self.get_family(family.family_id, family.version)

    def get_family(self, family_id: str, version: int) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM phase_e_hypothesis_families WHERE family_id=? AND version=?", (family_id, version)).fetchone()
            if row is None:
                raise GenerationConflictError(f"Unknown E.3 hypothesis family: {family_id}@{version}")
            return self._family_payload(row)

    def list_families(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute("SELECT * FROM phase_e_hypothesis_families ORDER BY family_id, version").fetchall()
            return [self._family_payload(row) for row in rows]

    def plan(self, materialization_id: str, family: HypothesisFamilySpec) -> dict[str, Any]:
        """Preview deterministic predictor-only candidates without persistence or E.1 registration."""
        self.initialize()
        snapshot = self._verified_snapshot(materialization_id, family)
        predictors, predictor_fp = self._read_predictors(snapshot, family)
        candidates, suppressions, raw_count = self._derive_candidates(snapshot, family, predictors)
        return {
            "materialization_id": materialization_id, "verified_materialization": True,
            "family_id": family.family_id, "family_version": family.version, "family_fingerprint": family.fingerprint,
            "partition": "train", "training_population_count": self._training_population(predictors),
            "available_features": [self._feature_inventory_payload(item) for item in predictors],
            "threshold_policy": family.threshold_policy.value, "candidate_budget": family.maximum_candidates,
            "raw_candidate_count": raw_count, "estimated_unique_proposals": len(candidates),
            "suppressed_counts": self._suppressed_counts(suppressions), "predictor_summary_fingerprint": predictor_fp,
            "outcome_access": "FORBIDDEN", "outcome_reads_attempted": 0, "trading_authority": False,
        }

    # ----- durable lifecycle -------------------------------------------------------
    def run(self, materialization_id: str, family: HypothesisFamilySpec, *, registered_at: str | None = None) -> dict[str, Any]:
        """Generate, freeze, and map one deterministic E.3 hypothesis universe."""
        self.initialize()
        self.register_family(family, registered_at=registered_at)
        snapshot = self._verified_snapshot(materialization_id, family)
        spec = snapshot.run_spec
        at = normalized_utc(registered_at or _now())
        self._register_run(spec, at)
        self._generate_if_needed(snapshot, family)
        self._register_e1_if_needed(spec.run_id)
        self._complete_if_ready(spec.run_id)
        return self.get(spec.run_id)

    def get(self, generation_run_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = self._require_run(connection, generation_run_id)
            self._validate_run(connection, row)
            return self._run_payload(connection, row)

    def list(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute("SELECT * FROM phase_e_generation_runs ORDER BY registered_at, generation_run_id").fetchall()
            for row in rows:
                self._validate_run(connection, row)
            return [self._run_payload(connection, row) for row in rows]

    def verify(self, generation_run_id: str) -> dict[str, Any]:
        """Reconcile a complete E.3 run and re-verify its E.2 trust boundary."""
        self.initialize()
        with self._connection() as connection:
            row = self._require_run(connection, generation_run_id)
            self._validate_run(connection, row)
            spec = self._run_spec_from_row(row)
            if row["status"] != GenerationStatus.COMPLETE.value:
                raise GenerationIntegrityError("Only COMPLETE E.3 generation runs are consumable or verifiable.")
            family = self._family_from_connection(connection, spec.family_id, spec.family_version)
        snapshot = self._verified_snapshot(spec.materialization_id, family)
        if snapshot.run_spec.payload() != spec.payload():
            raise GenerationIntegrityError("E.2 verification/source provenance no longer matches the frozen E.3 run.")
        with self._connection() as connection:
            row = self._require_run(connection, generation_run_id)
            self._validate_run(connection, row)
            self._verify_deterministic_generation(connection, row, snapshot, family)
            return {
                "generation_run_id": generation_run_id, "verified": True,
                "hypothesis_universe_fingerprint": row["hypothesis_universe_fingerprint"],
                "outcome_reads_attempted": int(row["outcome_reads_attempted"]),
                "outcome_reads_permitted": False, "trading_authority": False,
            }

    # ----- E.2 sealed verification + E.3 predictor source ------------------------
    def _verified_snapshot(self, materialization_id: str, family: HypothesisFamilySpec) -> _E2Snapshot:
        verification = PhaseEMaterializer(self.path).verify(materialization_id)
        if not verification.get("verified"):
            raise GenerationIntegrityError("E.3 refuses an unverified E.2 materialization.")
        snapshot = self._snapshot_from_predictor_reader(materialization_id, verification, family)
        # Repeat the E.2 verification after the predictor projection is read.
        # E.3 never receives outcome rows; this closes the check/use boundary.
        repeated = PhaseEMaterializer(self.path).verify(materialization_id)
        if dict(repeated) != dict(verification):
            raise GenerationIntegrityError("E.2 evidence changed during the E.3 trust-boundary check.")
        return snapshot

    def _snapshot_from_predictor_reader(self, materialization_id: str, verification: Mapping[str, Any], family: HypothesisFamilySpec) -> _E2Snapshot:
        with self._predictor_connection() as connection:
            row = connection.execute(
                """SELECT materialization_id, specification_json, specification_hash, source_universe_json, status,
                          membership_fingerprint, sampling_design_fingerprint, feature_artifact_fingerprint,
                          completed_artifact_fingerprint
                   FROM phase_e_materializations WHERE materialization_id=?""", (materialization_id,),
            ).fetchone()
        if row is None or row["status"] != MaterializationStatus.COMPLETE.value:
            raise GenerationIntegrityError("E.3 requires a COMPLETE E.2 materialization.")
        required = ("membership_fingerprint", "sampling_design_fingerprint", "feature_artifact_fingerprint", "completed_artifact_fingerprint")
        if any(row[name] != verification.get(name) for name in required):
            raise GenerationIntegrityError("E.2 verifier and E.3 predictor projection disagree on immutable fingerprints.")
        specification = _canonical_json(row["specification_json"], name="materialization specification")
        source = _canonical_json(row["source_universe_json"], name="materialization source universe")
        if not isinstance(specification, Mapping) or not isinstance(source, Mapping):
            raise GenerationIntegrityError("E.2 source projection is malformed.")
        partition = specification.get("partition")
        horizon = specification.get("outcome_horizon")
        if not isinstance(partition, Mapping) or not isinstance(horizon, Mapping):
            raise GenerationIntegrityError("E.2 materialization lacks typed partition/horizon evidence.")
        materialization_horizon = horizon.get("seconds")
        if materialization_horizon != partition.get("outcome_horizon", {}).get("seconds"):
            raise GenerationIntegrityError("E.2 materialization horizon/partition evidence disagrees.")
        if materialization_horizon not in {item.seconds for item in family.compatible_horizons}:
            raise GenerationConflictError("Hypothesis family is incompatible with the exact E.2 materialization horizon.")
        try:
            source_fingerprint = source["observation_fingerprint"]
            corpus_fingerprint = source["corpus_fingerprint"]
        except KeyError as exc:
            raise GenerationIntegrityError("E.2 materialization lacks bound Phase D source provenance.") from exc
        run_spec = GenerationRunSpec(
            materialization_id=materialization_id, materialization_specification_hash=row["specification_hash"],
            corpus_fingerprint=corpus_fingerprint, source_fingerprint=source_fingerprint,
            membership_fingerprint=row["membership_fingerprint"], sampling_design_fingerprint=row["sampling_design_fingerprint"],
            feature_fingerprint=row["feature_artifact_fingerprint"], complete_artifact_fingerprint=row["completed_artifact_fingerprint"],
            family_id=family.family_id, family_version=family.version, family_fingerprint=family.fingerprint,
            partition_name="train", generation_seed=family.generation_seed, candidate_budget=family.maximum_candidates,
            generator_algorithm=family.generator_algorithm, generator_code_version=family.generator_code_version,
            generator_config_version=family.generator_config_version,
        )
        return _E2Snapshot(run_spec=run_spec, materialization_spec=specification, verification=dict(verification))

    def _read_predictors(self, snapshot: _E2Snapshot, family: HypothesisFamilySpec) -> tuple[list[_PredictorFeature], str]:
        required_raw = snapshot.materialization_spec.get("required_features")
        if not isinstance(required_raw, list):
            raise GenerationIntegrityError("E.2 materialization lacks its frozen feature inventory.")
        required = {
            _feature_key(FeatureReference(**item)): FeatureReference(**item)
            for item in required_raw if isinstance(item, Mapping)
        }
        predictors: list[_PredictorFeature] = []
        with self._predictor_connection(snapshot.run_spec.run_id) as connection:
            population = int(connection.execute(
                "SELECT COUNT(*) FROM phase_e_materialization_membership WHERE materialization_id=? AND partition_name='train'",
                (snapshot.run_spec.materialization_id,),
            ).fetchone()[0])
            for feature in family.allowed_features:
                key = _feature_key(feature)
                if key not in required or required[key].payload() != feature.payload():
                    predictors.append(_PredictorFeature(feature, population, population, (), canonical_hash({"available": False, "feature": feature.payload()}), False, {"FEATURE_NOT_MATERIALIZED": population}))
                    continue
                rows = connection.execute(
                    """SELECT membership.ordinal, membership.observation_id, feature.value_json, feature.missing,
                              feature.missing_reason, feature.artifact_hash
                       FROM phase_e_materialization_membership AS membership
                       LEFT JOIN phase_e_materialization_features AS feature
                         ON feature.materialization_id=membership.materialization_id
                        AND feature.observation_id=membership.observation_id
                        AND feature.feature_id=? AND feature.feature_version=?
                       WHERE membership.materialization_id=? AND membership.partition_name='train'
                       ORDER BY membership.ordinal""",
                    (feature.feature_id, feature.version, snapshot.run_spec.materialization_id),
                ).fetchall()
                if len(rows) != population:
                    raise GenerationIntegrityError("Predictor query does not cover the frozen training membership.")
                values: list[tuple[str, float]] = []
                serialized_rows: list[dict[str, Any]] = []
                missing_reasons: dict[str, int] = {}
                missing_count = 0
                for ordinal, item in enumerate(rows):
                    if int(item["ordinal"]) != ordinal and population == len(rows):
                        # E.2 ordinals are global; this check is only about deterministic query order.
                        if ordinal and int(item["ordinal"]) <= int(rows[ordinal - 1]["ordinal"]):
                            raise GenerationIntegrityError("Training predictor ordering is not deterministic.")
                    if item["value_json"] is None or item["missing"] is None or item["artifact_hash"] is None:
                        raise GenerationIntegrityError("Required E.2 feature artifact is absent for a frozen training member.")
                    value_payload = _canonical_json(item["value_json"], name="predictor feature artifact")
                    if not isinstance(value_payload, Mapping) or set(value_payload) != {"value"}:
                        raise GenerationIntegrityError("Predictor feature artifact is malformed.")
                    missing = bool(item["missing"])
                    value = value_payload["value"]
                    reason = item["missing_reason"]
                    if missing:
                        if value is not None or not isinstance(reason, str) or not reason:
                            raise GenerationIntegrityError("Predictor missingness is not explicit.")
                        missing_count += 1
                        missing_reasons[reason] = missing_reasons.get(reason, 0) + 1
                    else:
                        numeric = _finite_float(value, name="predictor feature value")
                        if reason is not None:
                            raise GenerationIntegrityError("Nonmissing predictor feature has a missing reason.")
                        values.append((str(item["observation_id"]), numeric))
                    serialized_rows.append({
                        "ordinal": int(item["ordinal"]), "observation_id": item["observation_id"],
                        "value": value, "missing": missing, "missing_reason": reason, "artifact_hash": item["artifact_hash"],
                    })
                predictors.append(_PredictorFeature(
                    feature=feature, population_count=population, missing_count=missing_count, values=tuple(values),
                    rows_fingerprint=canonical_hash({"feature": feature.payload(), "rows": serialized_rows}), available=True,
                    missing_reasons={key: missing_reasons[key] for key in sorted(missing_reasons)},
                ))
        summary = {
            "schema": "phase-e3-predictor-summary-v1", "materialization_id": snapshot.run_spec.materialization_id,
            "membership_fingerprint": snapshot.run_spec.membership_fingerprint,
            "sampling_design_fingerprint": snapshot.run_spec.sampling_design_fingerprint,
            "feature_fingerprint": snapshot.run_spec.feature_fingerprint,
            "complete_artifact_fingerprint": snapshot.run_spec.complete_artifact_fingerprint,
            "partition": "train", "features": [self._feature_inventory_payload(item) for item in predictors],
        }
        return predictors, canonical_hash(summary)

    # ----- deterministic candidates ------------------------------------------------
    def _derive_candidates(self, snapshot: _E2Snapshot, family: HypothesisFamilySpec,
                           predictors: Sequence[_PredictorFeature]) -> tuple[list[_Candidate], list[tuple[str, Mapping[str, Any]]], int]:
        accepted: list[_Candidate] = []
        suppressions: list[tuple[str, Mapping[str, Any]]] = []
        raw_count = 0
        seen: dict[str, _Candidate] = {}
        for feature_index, predictor in enumerate(predictors):
            if not predictor.available:
                suppressions.append(("FEATURE_NOT_MATERIALIZED", {"feature": predictor.feature.payload()}))
                continue
            coverage = 0.0 if predictor.population_count == 0 else len(predictor.values) / predictor.population_count
            if predictor.population_count == 0:
                suppressions.append(("NO_TRAINING_MEMBERS", {"feature": predictor.feature.payload()}))
                continue
            if not predictor.values:
                reason = ("HISTORICAL_ACQUISITION_LATENCY_IS_NOT_A_CAUSAL_FEATURE"
                          if "HISTORICAL_ACQUISITION_LATENCY_IS_NOT_A_CAUSAL_FEATURE" in predictor.missing_reasons
                          else "FEATURE_ALL_MISSING")
                suppressions.append((reason, {"feature": predictor.feature.payload(), "missing_reasons": predictor.missing_reasons}))
                continue
            if coverage < float(family.minimum_feature_coverage):
                suppressions.append(("FEATURE_COVERAGE_BELOW_MINIMUM", {
                    "feature": predictor.feature.payload(), "coverage": coverage,
                    "minimum_coverage": family.minimum_feature_coverage, "missing_count": predictor.missing_count,
                }))
                continue
            thresholds = self._thresholds(family, predictor)
            for threshold, provenance in thresholds:
                for operator in family.allowed_operators:
                    raw_count += 1
                    predicate = Predicate(operator, feature=predictor.feature, threshold=threshold)
                    support = self._support_count(predicate, predictor.values)
                    candidate = _Candidate(feature_index, predictor.feature, predicate, provenance, support,
                                           predictor.missing_count, predictor.population_count)
                    prior = seen.get(predicate.predicate_hash)
                    if prior is not None:
                        suppressions.append(("SEMANTIC_DUPLICATE", {
                            "predicate": predicate.payload(), "duplicate_of_predicate_hash": prior.predicate.predicate_hash,
                            "threshold_provenance": provenance,
                        }))
                        continue
                    seen[predicate.predicate_hash] = candidate
                    valid_count = predictor.population_count - predictor.missing_count
                    comparator_count = valid_count - support
                    if support == 0:
                        suppressions.append(("ZERO_TRAINING_SUPPORT", {"predicate": predicate.payload(), "threshold_provenance": provenance}))
                    elif support < family.minimum_training_support:
                        suppressions.append(("INSUFFICIENT_TRAINING_SUPPORT", {
                            "predicate": predicate.payload(), "support_count": support,
                            "minimum_training_support": family.minimum_training_support,
                        }))
                    elif comparator_count == 0:
                        suppressions.append(("EMPTY_COMPARATOR", {"predicate": predicate.payload(), "valid_training_population": valid_count}))
                    elif comparator_count < family.minimum_training_support:
                        suppressions.append(("INSUFFICIENT_COMPARATOR_SUPPORT", {
                            "predicate": predicate.payload(), "comparator_count": comparator_count,
                            "minimum_training_support": family.minimum_training_support,
                        }))
                    else:
                        accepted.append(candidate)
        accepted.sort(key=lambda item: item.order_key)
        per_feature: dict[str, int] = {}
        budgeted: list[_Candidate] = []
        for candidate in accepted:
            key = _feature_key(candidate.feature)
            per_feature[key] = per_feature.get(key, 0) + 1
            if per_feature[key] > family.maximum_candidates_per_feature:
                suppressions.append(("PER_FEATURE_BUDGET", {"predicate": candidate.predicate.payload(), "limit": family.maximum_candidates_per_feature}))
            elif len(budgeted) >= family.maximum_candidates:
                suppressions.append(("FAMILY_CANDIDATE_BUDGET", {"predicate": candidate.predicate.payload(), "limit": family.maximum_candidates}))
            else:
                budgeted.append(candidate)
        return budgeted, suppressions, raw_count

    @staticmethod
    def _support_count(predicate: Predicate, values: Sequence[tuple[str, float]]) -> int:
        threshold = float(predicate.threshold or 0.0)
        operation: Callable[[float], bool] = {
            PredicateOperator.GT: lambda value: value > threshold,
            PredicateOperator.GE: lambda value: value >= threshold,
            PredicateOperator.LT: lambda value: value < threshold,
            PredicateOperator.LE: lambda value: value <= threshold,
            PredicateOperator.EQ: lambda value: value == threshold,
        }[predicate.operator]
        return sum(1 for _, value in values if operation(value))

    @staticmethod
    def _thresholds(family: HypothesisFamilySpec, predictor: _PredictorFeature) -> list[tuple[float, Mapping[str, Any]]]:
        if family.threshold_policy is ThresholdPolicy.FIXED_THRESHOLD_V1:
            return [
                (float(value), {"policy": family.threshold_policy.value, "fixed_threshold_index": index, "value": float(value)})
                for index, value in enumerate(family.fixed_thresholds.get(_feature_key(predictor.feature), ()))
            ]
        if family.threshold_policy is ThresholdPolicy.SIGN_SPLIT_V1:
            return [(0.0, {"policy": family.threshold_policy.value, "semantic_zero": True})]
        values = sorted(value for _, value in predictor.values)
        result: list[tuple[float, Mapping[str, Any]]] = []
        for index, quantile in enumerate(family.quantiles):
            rank = max(1, math.ceil(float(quantile) * len(values)))
            threshold = float(values[rank - 1])
            result.append((threshold, {
                "policy": family.threshold_policy.value, "quantile": float(quantile),
                "quantile_index": index, "nearest_rank": rank, "population_count": len(values), "selected_observed_value": threshold,
            }))
        return result

    # ----- registration and freeze -------------------------------------------------
    def _register_run(self, spec: GenerationRunSpec, registered_at: str) -> None:
        payload = spec.payload()
        with self._write() as connection:
            row = connection.execute("SELECT * FROM phase_e_generation_runs WHERE generation_run_id=?", (spec.run_id,)).fetchone()
            if row is None:
                connection.execute(
                    """INSERT INTO phase_e_generation_runs(
                           generation_run_id, generation_specification_json, generation_specification_hash, materialization_id,
                           family_id, family_version, family_fingerprint, status, registered_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (spec.run_id, storage_json(payload), canonical_hash(payload), spec.materialization_id, spec.family_id,
                     spec.family_version, spec.family_fingerprint, GenerationStatus.REGISTERED.value, registered_at),
                )
                self._append_event(connection, spec.run_id, "REGISTERED", None, GenerationStatus.REGISTERED,
                                   "predeclared_before_generation", registered_at,
                                   {"generation_specification_hash": canonical_hash(payload), "family_fingerprint": spec.family_fingerprint,
                                    "materialization_id": spec.materialization_id, "outcome_access": "FORBIDDEN"})
            else:
                self._validate_run(connection, row)
                if self._run_spec_from_row(row).payload() != payload:
                    raise GenerationConflictError("Deterministic E.3 run identity conflicts with different scientific inputs.")

    def _generate_if_needed(self, snapshot: _E2Snapshot, family: HypothesisFamilySpec) -> None:
        run_id = snapshot.run_spec.run_id
        with self._write() as connection:
            row = self._require_run(connection, run_id)
            self._validate_run(connection, row)
            status = GenerationStatus(row["status"])
            if status is GenerationStatus.REGISTERED:
                self._transition(connection, row, GenerationStatus.GENERATING, "generation_started", {
                    "partition": "train", "membership_fingerprint": snapshot.run_spec.membership_fingerprint,
                    "feature_fingerprint": snapshot.run_spec.feature_fingerprint, "outcome_access": "FORBIDDEN",
                })
            elif status is not GenerationStatus.GENERATING:
                return
        predictors, summary_fp = self._read_predictors(snapshot, family)
        candidates, suppressions, raw_count = self._derive_candidates(snapshot, family, predictors)
        # Re-establish E.2 verification immediately before freezing proposals.
        fresh = self._verified_snapshot(snapshot.run_spec.materialization_id, family)
        if fresh.run_spec.payload() != snapshot.run_spec.payload():
            raise GenerationIntegrityError("E.2 evidence changed before the E.3 candidate universe could freeze.")
        proposals = self._proposals(snapshot, family, candidates, summary_fp)
        universe_fp = self._universe_fingerprint(proposals)
        suppressed_counts = self._suppressed_counts(suppressions)
        with self._write() as connection:
            row = self._require_run(connection, run_id)
            self._validate_run(connection, row)
            if GenerationStatus(row["status"]) is not GenerationStatus.GENERATING:
                return
            for ordinal, proposal in enumerate(proposals):
                self._insert_proposal(connection, proposal, ordinal)
            for ordinal, (reason, detail) in enumerate(suppressions):
                self._insert_suppression(connection, run_id, ordinal, reason, detail)
            self._transition(connection, row, GenerationStatus.CANDIDATES_FROZEN, "candidate_universe_frozen", {
                "predictor_summary_fingerprint": summary_fp, "training_population_count": self._training_population(predictors),
                "raw_candidate_count": raw_count, "unique_candidate_count": len(proposals),
                "suppressed_counts": suppressed_counts, "hypothesis_universe_fingerprint": universe_fp,
                "outcome_reads_attempted": 0,
            }, projection={
                "predictor_summary_fingerprint": summary_fp, "training_population_count": self._training_population(predictors),
                "raw_candidate_count": raw_count, "unique_candidate_count": len(proposals),
                "suppressed_counts_json": storage_json(suppressed_counts), "hypothesis_universe_fingerprint": universe_fp,
            })

    def _proposals(self, snapshot: _E2Snapshot, family: HypothesisFamilySpec, candidates: Sequence[_Candidate],
                   predictor_summary_fingerprint: str) -> list[HypothesisProposal]:
        partition = self._partition_from_materialization(snapshot.materialization_spec)
        horizon = OutcomeHorizon(snapshot.materialization_spec["outcome_horizon"]["seconds"])
        multiple_testing_family_id = "e3-mtf-" + canonical_hash({
            "rule": family.multiple_testing_family_rule, "family_id": family.family_id, "family_version": family.version,
            "family_fingerprint": family.fingerprint, "generation_run_id": snapshot.run_spec.run_id,
            "materialization_id": snapshot.run_spec.materialization_id, "horizon": horizon.payload(),
            "comparator": family.comparator_policy, "test_plan": family.statistical_test_plan.payload(),
        })[:32]
        proposals: list[HypothesisProposal] = []
        for candidate in candidates:
            proposal_id = "e3p-" + canonical_hash({
                "generation_run_id": snapshot.run_spec.run_id, "predicate_hash": candidate.predicate.predicate_hash,
                "threshold_provenance": candidate.threshold_provenance, "feature": candidate.feature.payload(),
            })[:32]
            definition = self._e1_definition(snapshot, family, candidate, proposal_id, partition, horizon, multiple_testing_family_id)
            proposals.append(HypothesisProposal(
                proposal_id=proposal_id, generation_run_id=snapshot.run_spec.run_id, family_id=family.family_id,
                family_version=family.version, source_materialization_id=snapshot.run_spec.materialization_id,
                predicate=candidate.predicate, required_features=(candidate.feature,),
                threshold_provenance=candidate.threshold_provenance, training_support_count=candidate.support_count,
                training_missing_count=candidate.missing_count, training_population_count=candidate.population_count,
                comparator_policy=family.comparator_policy, outcome_horizon=horizon,
                proposed_hypothesis_id=definition.hypothesis_id, proposed_hypothesis_version=definition.version,
                proposed_definition_hash=definition.definition_hash, proposed_definition=definition.canonical_payload(),
                multiple_testing_family_id=multiple_testing_family_id,
                generation_provenance={
                    "generator_algorithm": family.generator_algorithm, "generator_code_version": family.generator_code_version,
                    "generator_config_version": family.generator_config_version, "generation_seed": family.generation_seed,
                    "predictor_summary_fingerprint": predictor_summary_fingerprint,
                    "materialization_membership_fingerprint": snapshot.run_spec.membership_fingerprint,
                    "sampling_design_fingerprint": snapshot.run_spec.sampling_design_fingerprint,
                    "feature_fingerprint": snapshot.run_spec.feature_fingerprint,
                    "complete_artifact_fingerprint": snapshot.run_spec.complete_artifact_fingerprint,
                },
            ))
        return proposals

    def _e1_definition(self, snapshot: _E2Snapshot, family: HypothesisFamilySpec, candidate: _Candidate,
                       proposal_id: str, partition: PartitionIdentity, horizon: OutcomeHorizon,
                       multiple_testing_family_id: str) -> HypothesisDefinition:
        predicate_json = storage_json(candidate.predicate.payload())
        feature = candidate.feature
        key = _feature_key(feature)
        direction_text = "differs from" if family.statistical_test_plan.direction == "TWO_SIDED" else "has the predeclared directional difference from"
        return HypothesisDefinition(
            hypothesis_id="e3h-" + proposal_id[4:], version=1,
            title=f"{family.title}: {candidate.predicate.operator.value}({_feature_key(feature)})",
            proposition=(f"Among exact E.2 training-materialization anchors satisfying {predicate_json}, the declared "
                         f"{horizon.seconds}-second outcome distribution {direction_text} its predeclared predicate complement."),
            null_hypothesis="The conditional outcome distribution does not differ from the predeclared predicate complement.",
            alternative_hypothesis=("The conditional outcome distribution differs from the predeclared predicate complement by at least "
                                    "the declared minimum effect size."),
            population_definition=(f"Frozen E.2 materialization {snapshot.run_spec.materialization_id}; train partition only; "
                                   "valid required predictor evidence only."),
            required_features=(feature,), feature_transforms={key: {"typed_predicate": candidate.predicate.payload()}},
            entry_definition=predicate_json,
            outcome_definition=(f"Exact E.2 {horizon.seconds}-second outcome artifact under its frozen resolution semantics; "
                                "E.4 must execute the declared metric without substitution."),
            outcome_horizon=horizon, comparator_definition=family.comparator_policy,
            inclusion_rules=("Frozen train membership only", "Required predictor evidence is finite and nonmissing", predicate_json),
            exclusion_rules=("Validation and test partitions excluded", "Missing required predictor evidence excluded"),
            minimum_sample_size=family.statistical_test_plan.minimum_sample_size,
            statistical_test=StatisticSpec(family.statistical_test_plan.test_id, family.statistical_test_plan.payload()),
            minimum_effect_size=family.statistical_test_plan.minimum_effect_size,
            success_threshold={"significance_threshold": family.statistical_test_plan.significance_threshold,
                               "minimum_effect_size": family.statistical_test_plan.minimum_effect_size},
            failure_threshold={"minimum_sample_size": family.statistical_test_plan.minimum_sample_size,
                               "comparator_policy": family.comparator_policy},
            multiple_testing_family=multiple_testing_family_id, partition=partition,
            code_version=family.generator_code_version, config_version=family.generator_config_version,
            created_at="2026-01-01T00:00:00Z", predecessor_id=None,
        )

    # ----- E.1 mapping and final manifest -----------------------------------------
    def _register_e1_if_needed(self, generation_run_id: str) -> None:
        with self._write() as connection:
            row = self._require_run(connection, generation_run_id)
            self._validate_run(connection, row)
            status = GenerationStatus(row["status"])
            if status is GenerationStatus.CANDIDATES_FROZEN:
                self._transition(connection, row, GenerationStatus.REGISTERING_HYPOTHESES, "e1_registration_started", {
                    "hypothesis_universe_fingerprint": row["hypothesis_universe_fingerprint"],
                })
            elif status is not GenerationStatus.REGISTERING_HYPOTHESES:
                return
        with self._connection() as connection:
            row = self._require_run(connection, generation_run_id)
            spec = self._run_spec_from_row(row)
            proposals = self._proposal_rows(connection, generation_run_id)
            mappings = {item["proposal_id"]: item for item in connection.execute(
                "SELECT * FROM phase_e_generation_e1_mappings WHERE generation_run_id=?", (generation_run_id,),
            ).fetchall()}
        ledger = PhaseELedger(self.path)
        for proposal in proposals:
            prior = mappings.get(proposal["proposal_id"])
            if prior is not None:
                self._validate_mapping(proposal, prior)
                continue
            definition = self._definition_from_payload(_canonical_json(proposal["proposed_definition_json"], name="proposed E.1 definition"))
            if definition.definition_hash != proposal["proposed_definition_hash"]:
                raise GenerationIntegrityError("Frozen E.3 proposal has a false E.1 definition hash.")
            registered = ledger.register(definition, corpus_fingerprint=spec.corpus_fingerprint)
            if (registered["hypothesis_id"] != proposal["proposed_hypothesis_id"]
                    or int(registered["hypothesis_version"]) != int(proposal["proposed_hypothesis_version"])
                    or registered["hypothesis_hash"] != proposal["proposed_definition_hash"]):
                raise GenerationIntegrityError("E.1 registration does not match the frozen E.3 proposal.")
            self._insert_mapping(generation_run_id, proposal, registered)

    def _complete_if_ready(self, generation_run_id: str) -> None:
        with self._write() as connection:
            row = self._require_run(connection, generation_run_id)
            self._validate_run(connection, row)
            if GenerationStatus(row["status"]) is not GenerationStatus.REGISTERING_HYPOTHESES:
                return
            proposals = self._proposal_rows(connection, generation_run_id)
            mappings = connection.execute("SELECT * FROM phase_e_generation_e1_mappings WHERE generation_run_id=?", (generation_run_id,)).fetchall()
            if len(proposals) != len(mappings):
                return
            by_proposal = {item["proposal_id"]: item for item in mappings}
            if set(by_proposal) != {item["proposal_id"] for item in proposals}:
                raise GenerationIntegrityError("E.1 mappings do not cover exactly the frozen E.3 universe.")
            for proposal in proposals:
                self._validate_mapping(proposal, by_proposal[proposal["proposal_id"]])
            manifest = self._manifest(connection, row, proposals, mappings)
            manifest_hash = canonical_hash(manifest)
            existing = connection.execute("SELECT * FROM phase_e_generation_manifests WHERE generation_run_id=?", (generation_run_id,)).fetchone()
            if existing is None:
                connection.execute("INSERT INTO phase_e_generation_manifests VALUES (?, ?, ?)",
                                   (generation_run_id, storage_json(manifest), manifest_hash))
            elif existing["manifest_hash"] != manifest_hash or _canonical_json(existing["manifest_json"], name="generation manifest") != manifest:
                raise GenerationIntegrityError("Generation manifest conflicts with frozen candidate evidence.")
            self._transition(connection, row, GenerationStatus.COMPLETE, "generation_complete", {
                "hypothesis_universe_fingerprint": row["hypothesis_universe_fingerprint"], "manifest_hash": manifest_hash,
                "registered_hypothesis_count": len(mappings), "outcome_reads_attempted": 0,
            }, projection={"registered_hypothesis_count": len(mappings)})

    # ----- persistence helpers -----------------------------------------------------
    def _insert_proposal(self, connection: sqlite3.Connection, proposal: HypothesisProposal, ordinal: int) -> None:
        body = proposal.payload()
        definition = proposal.proposed_definition
        artifact = canonical_hash({"ordinal": ordinal, "proposal": body, "definition": definition})
        connection.execute(
            """INSERT INTO phase_e_hypothesis_proposals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (proposal.generation_run_id, proposal.proposal_id, ordinal, storage_json(proposal.predicate.payload()),
             proposal.predicate.predicate_hash, storage_json([item.payload() for item in proposal.required_features]),
             storage_json(dict(proposal.threshold_provenance)), proposal.training_support_count, proposal.training_missing_count,
             proposal.training_population_count, proposal.comparator_policy, proposal.outcome_horizon.seconds,
             proposal.proposed_hypothesis_id, proposal.proposed_hypothesis_version, storage_json(definition),
             proposal.proposed_definition_hash, proposal.multiple_testing_family_id, storage_json(dict(proposal.generation_provenance)), artifact),
        )

    def _insert_suppression(self, connection: sqlite3.Connection, run_id: str, ordinal: int, reason: str, detail: Mapping[str, Any]) -> None:
        if not isinstance(reason, str) or not reason:
            raise GenerationIntegrityError("Suppression reason must be durable nonempty text.")
        suppression_id = "e3s-" + canonical_hash({"run": run_id, "ordinal": ordinal, "reason": reason, "detail": dict(detail)})[:32]
        artifact = canonical_hash({"generation_run_id": run_id, "suppression_id": suppression_id, "ordinal": ordinal,
                                  "reason": reason, "detail": dict(detail)})
        connection.execute("INSERT INTO phase_e_generation_suppressions VALUES (?, ?, ?, ?, ?, ?)",
                           (run_id, suppression_id, ordinal, reason, storage_json(dict(detail)), artifact))

    def _insert_mapping(self, run_id: str, proposal: sqlite3.Row, registered: Mapping[str, Any]) -> None:
        registered_at = normalized_utc(_now())
        mapping = {
            "generation_run_id": run_id, "proposal_id": proposal["proposal_id"], "experiment_id": registered["experiment_id"],
            "hypothesis_id": registered["hypothesis_id"], "hypothesis_version": int(registered["hypothesis_version"]),
            "definition_hash": registered["hypothesis_hash"],
        }
        mapping_hash = canonical_hash(mapping)
        with self._write() as connection:
            row = self._require_run(connection, run_id)
            self._validate_run(connection, row)
            existing = connection.execute(
                "SELECT * FROM phase_e_generation_e1_mappings WHERE generation_run_id=? AND proposal_id=?", (run_id, proposal["proposal_id"]),
            ).fetchone()
            if GenerationStatus(row["status"]) is not GenerationStatus.REGISTERING_HYPOTHESES:
                if existing is not None:
                    self._validate_mapping(proposal, existing)
                    return
                raise GenerationConflictError("Cannot add E.1 mappings outside frozen registration lifecycle stage.")
            if existing is None:
                connection.execute("INSERT INTO phase_e_generation_e1_mappings VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                   (run_id, proposal["proposal_id"], registered["experiment_id"], registered["hypothesis_id"],
                                    int(registered["hypothesis_version"]), registered["hypothesis_hash"], mapping_hash, registered_at))
            else:
                self._validate_mapping(proposal, existing)

    def _record_access_violation(self, run_id: str | None, relations: Sequence[str], message: str) -> None:
        """Leave durable evidence if an internal E.3 read path is ever violated."""
        detected_at = normalized_utc(_now())
        detail = {"message": message, "relations": list(relations), "outcome_access": "FORBIDDEN"}
        violation_id = "e3v-" + canonical_hash({"run": run_id, "at": detected_at, "detail": detail})[:32]
        artifact = canonical_hash({"violation_id": violation_id, "generation_run_id": run_id,
                                  "detected_at": detected_at, "detail": detail})
        with self._write() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO phase_e_generation_access_violations VALUES (?, ?, ?, ?, ?, ?)",
                (violation_id, run_id, ",".join(relations) or "UNKNOWN_FORBIDDEN_RELATION", detected_at,
                 storage_json(detail), artifact),
            )

    def _append_event(self, connection: sqlite3.Connection, run_id: str, event_type: str, from_status: GenerationStatus | None,
                      to_status: GenerationStatus, reason: str, event_at: str, payload: Mapping[str, Any]) -> None:
        payload_hash = canonical_hash(dict(payload))
        event_id = canonical_hash({"generation_run_id": run_id, "event_type": event_type,
                                   "from_status": from_status.value if from_status else None, "to_status": to_status.value,
                                   "reason": reason, "event_at": event_at, "payload_hash": payload_hash})
        connection.execute("INSERT INTO phase_e_generation_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (event_id, run_id, event_type, from_status.value if from_status else None, to_status.value,
                            reason, event_at, storage_json(dict(payload)), payload_hash))

    def _transition(self, connection: sqlite3.Connection, row: sqlite3.Row, to_status: GenerationStatus, reason: str,
                    payload: Mapping[str, Any], projection: Mapping[str, Any] | None = None) -> None:
        current = GenerationStatus(row["status"])
        at = normalized_utc(_now())
        assignments = {"status": to_status.value, **dict(projection or {})}
        if to_status is GenerationStatus.COMPLETE:
            assignments["completed_at"] = at
        columns = ", ".join(f"{key}=?" for key in assignments)
        connection.execute(f"UPDATE phase_e_generation_runs SET {columns} WHERE generation_run_id=? AND status=?",
                           (*assignments.values(), row["generation_run_id"], current.value))
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise GenerationConflictError("Concurrent E.3 lifecycle transition lost its compare-and-swap.")
        self._append_event(connection, row["generation_run_id"], to_status.value, current, to_status, reason, at, payload)

    # ----- reconciliation ----------------------------------------------------------
    def _validate_run(self, connection: sqlite3.Connection, row: sqlite3.Row) -> None:
        try:
            spec = self._run_spec_from_row(row)
            if (spec.run_id != row["generation_run_id"] or canonical_hash(spec.payload()) != row["generation_specification_hash"]
                    or spec.materialization_id != row["materialization_id"] or spec.family_id != row["family_id"]
                    or spec.family_version != int(row["family_version"]) or spec.family_fingerprint != row["family_fingerprint"]):
                raise GenerationIntegrityError("E.3 run projection conflicts with immutable generation inputs.")
            family = self._family_from_connection(connection, spec.family_id, spec.family_version)
            if family.fingerprint != spec.family_fingerprint:
                raise GenerationIntegrityError("Frozen E.3 family fingerprint cannot be reconciled.")
            registered_at = normalized_utc(row["registered_at"])
            if registered_at != row["registered_at"]:
                raise GenerationIntegrityError("E.3 registration timestamp is not canonical UTC.")
            status = GenerationStatus(row["status"])
            expected_states = [GenerationStatus.REGISTERED, GenerationStatus.GENERATING, GenerationStatus.CANDIDATES_FROZEN,
                               GenerationStatus.REGISTERING_HYPOTHESES, GenerationStatus.COMPLETE]
            events = connection.execute("SELECT * FROM phase_e_generation_events WHERE generation_run_id=?", (spec.run_id,)).fetchall()
            if len(events) != expected_states.index(status) + 1:
                raise GenerationIntegrityError("E.3 lifecycle projection has missing, extra, or impossible events.")
            by_target = {item["to_status"]: item for item in events}
            if len(by_target) != len(events):
                raise GenerationIntegrityError("E.3 lifecycle contains a duplicate semantic transition.")
            previous: GenerationStatus | None = None
            parsed: list[tuple[sqlite3.Row, Mapping[str, Any]]] = []
            prior_at = registered_at
            for expected in expected_states[:expected_states.index(status) + 1]:
                event = by_target.get(expected.value)
                if event is None:
                    raise GenerationIntegrityError("E.3 lifecycle misses an expected transition.")
                payload = _canonical_json(event["payload_json"], name="generation event")
                event_at = normalized_utc(event["event_at"])
                if event_at != event["event_at"] or _instant(event_at) < _instant(prior_at):
                    raise GenerationIntegrityError("E.3 lifecycle timing is noncanonical or reordered.")
                identity = {"generation_run_id": spec.run_id, "event_type": event["event_type"],
                            "from_status": previous.value if previous else None, "to_status": expected.value,
                            "reason": event["reason"], "event_at": event_at, "payload_hash": event["payload_hash"]}
                if canonical_hash(payload) != event["payload_hash"] or canonical_hash(identity) != event["event_id"]:
                    raise GenerationIntegrityError("E.3 lifecycle event hash is inconsistent.")
                if event["from_status"] != (previous.value if previous else None) or event["to_status"] != expected.value:
                    raise GenerationIntegrityError("E.3 lifecycle sequence is forged.")
                parsed.append((event, payload)); previous = expected; prior_at = event_at
            if (parsed[0][0]["event_type"] != "REGISTERED" or parsed[0][0]["reason"] != "predeclared_before_generation"
                    or parsed[0][0]["event_at"] != registered_at
                    or parsed[0][1] != {"generation_specification_hash": row["generation_specification_hash"],
                                         "family_fingerprint": spec.family_fingerprint, "materialization_id": spec.materialization_id,
                                         "outcome_access": "FORBIDDEN"}):
                raise GenerationIntegrityError("E.3 registration event does not bind frozen inputs/outcome prohibition.")
            if len(parsed) > 1 and (parsed[1][0]["event_type"] != "GENERATING" or parsed[1][0]["reason"] != "generation_started"
                                    or parsed[1][1] != {"partition": "train", "membership_fingerprint": spec.membership_fingerprint,
                                                        "feature_fingerprint": spec.feature_fingerprint, "outcome_access": "FORBIDDEN"}):
                raise GenerationIntegrityError("E.3 generation-start event is forged.")
            proposals = self._proposal_rows(connection, spec.run_id)
            suppressions = self._suppression_rows(connection, spec.run_id)
            if status in {GenerationStatus.REGISTERED, GenerationStatus.GENERATING} and (proposals or suppressions):
                raise GenerationIntegrityError("E.3 candidates exist before their universe freezes.")
            if expected_states.index(status) >= 2:
                universe = self._universe_fingerprint_from_rows(proposals)
                counts = self._suppressed_counts_from_rows(suppressions)
                frozen = parsed[2][1]
                expected_frozen = {
                    "predictor_summary_fingerprint": row["predictor_summary_fingerprint"],
                    "training_population_count": row["training_population_count"], "raw_candidate_count": row["raw_candidate_count"],
                    "unique_candidate_count": len(proposals), "suppressed_counts": counts,
                    "hypothesis_universe_fingerprint": universe, "outcome_reads_attempted": 0,
                }
                if (parsed[2][0]["event_type"] != "CANDIDATES_FROZEN" or parsed[2][0]["reason"] != "candidate_universe_frozen"
                        or frozen != expected_frozen or row["unique_candidate_count"] != len(proposals)
                        or row["suppressed_counts_json"] is None or _canonical_json(row["suppressed_counts_json"], name="suppressed counts") != counts
                        or row["hypothesis_universe_fingerprint"] != universe or int(row["outcome_reads_attempted"]) != 0):
                    raise GenerationIntegrityError("Frozen E.3 candidate universe cannot be reconciled.")
            if expected_states.index(status) >= 3:
                if (parsed[3][0]["event_type"] != "REGISTERING_HYPOTHESES" or parsed[3][0]["reason"] != "e1_registration_started"
                        or parsed[3][1] != {"hypothesis_universe_fingerprint": row["hypothesis_universe_fingerprint"]}):
                    raise GenerationIntegrityError("E.3 E.1-registration transition is forged.")
                mappings = connection.execute("SELECT * FROM phase_e_generation_e1_mappings WHERE generation_run_id=?", (spec.run_id,)).fetchall()
                if len({item["proposal_id"] for item in mappings}) != len(mappings):
                    raise GenerationIntegrityError("E.3 contains duplicate E.1 proposal mappings.")
                by_proposal = {item["proposal_id"]: item for item in proposals}
                for mapping in mappings:
                    if mapping["proposal_id"] not in by_proposal:
                        raise GenerationIntegrityError("E.3 E.1 mapping refers to an unknown proposal.")
                    self._validate_mapping(by_proposal[mapping["proposal_id"]], mapping)
                    hypothesis = connection.execute(
                        "SELECT definition_hash FROM phase_e_hypotheses WHERE hypothesis_id=? AND version=?",
                        (mapping["hypothesis_id"], mapping["hypothesis_version"]),
                    ).fetchone()
                    experiment = connection.execute(
                        "SELECT hypothesis_hash FROM phase_e_experiments WHERE experiment_id=?",
                        (mapping["experiment_id"],),
                    ).fetchone()
                    if (hypothesis is None or experiment is None or hypothesis["definition_hash"] != mapping["definition_hash"]
                            or experiment["hypothesis_hash"] != mapping["definition_hash"]):
                        raise GenerationIntegrityError("E.3 E.1 mapping does not resolve to the frozen ledger definition.")
            if status is GenerationStatus.COMPLETE:
                mappings = connection.execute("SELECT * FROM phase_e_generation_e1_mappings WHERE generation_run_id=?", (spec.run_id,)).fetchall()
                manifest_row = connection.execute("SELECT * FROM phase_e_generation_manifests WHERE generation_run_id=?", (spec.run_id,)).fetchone()
                manifest = self._manifest(connection, row, proposals, mappings)
                if (manifest_row is None or _canonical_json(manifest_row["manifest_json"], name="generation manifest") != manifest
                        or manifest_row["manifest_hash"] != canonical_hash(manifest)
                        or len(mappings) != len(proposals) or row["registered_hypothesis_count"] != len(mappings)
                        or row["completed_at"] is None or normalized_utc(row["completed_at"]) != row["completed_at"]
                        or row["completed_at"] != parsed[4][0]["event_at"]
                        or parsed[4][0]["event_type"] != "COMPLETE" or parsed[4][0]["reason"] != "generation_complete"
                        or parsed[4][1] != {"hypothesis_universe_fingerprint": row["hypothesis_universe_fingerprint"],
                                             "manifest_hash": manifest_row["manifest_hash"] if manifest_row else None,
                                             "registered_hypothesis_count": len(mappings), "outcome_reads_attempted": 0}):
                    raise GenerationIntegrityError("COMPLETE E.3 run lacks a reconciled immutable manifest.")
        except (ValueError, KeyError, TypeError, sqlite3.Error) as exc:
            if isinstance(exc, GenerationIntegrityError):
                raise
            raise GenerationIntegrityError(f"Malformed E.3 persisted state: {exc}") from exc

    def _proposal_rows(self, connection: sqlite3.Connection, run_id: str) -> list[sqlite3.Row]:
        rows = connection.execute("SELECT * FROM phase_e_hypothesis_proposals WHERE generation_run_id=? ORDER BY ordinal", (run_id,)).fetchall()
        for ordinal, row in enumerate(rows):
            if int(row["ordinal"]) != ordinal:
                raise GenerationIntegrityError("E.3 proposal ordinals are not contiguous deterministic order.")
            predicate_payload = _canonical_json(row["predicate_json"], name="proposal predicate")
            predicate = Predicate.from_payload(predicate_payload)
            if predicate.predicate_hash != row["predicate_hash"]:
                raise GenerationIntegrityError("E.3 proposal predicate hash is inconsistent.")
            features = _canonical_json(row["required_features_json"], name="proposal features")
            if features != [predicate.feature.payload()] or predicate.feature is None:
                raise GenerationIntegrityError("E.3 proposal required features do not match its typed predicate.")
            definition = _canonical_json(row["proposed_definition_json"], name="proposal E.1 definition")
            parsed_definition = self._definition_from_payload(definition)
            if parsed_definition.definition_hash != row["proposed_definition_hash"]:
                raise GenerationIntegrityError("E.3 proposal E.1 definition hash is inconsistent.")
            provenance = _canonical_json(row["generation_provenance_json"], name="proposal provenance")
            identity = {
                "proposal_id": row["proposal_id"], "generation_run_id": row["generation_run_id"],
                "family_id": self._run_spec_from_row(self._require_run(connection, run_id)).family_id,
                "family_version": self._run_spec_from_row(self._require_run(connection, run_id)).family_version,
                "source_materialization_id": self._run_spec_from_row(self._require_run(connection, run_id)).materialization_id,
                "predicate": predicate_payload, "predicate_hash": row["predicate_hash"], "required_features": features,
                "threshold_provenance": _canonical_json(row["threshold_provenance_json"], name="threshold provenance"),
                "training_support_count": row["training_support_count"], "training_missing_count": row["training_missing_count"],
                "training_population_count": row["training_population_count"], "comparator_policy": row["comparator_policy"],
                "outcome_horizon": {"seconds": row["outcome_horizon_seconds"]},
                "proposed_hypothesis_id": row["proposed_hypothesis_id"], "proposed_hypothesis_version": row["proposed_hypothesis_version"],
                "proposed_definition_hash": row["proposed_definition_hash"], "multiple_testing_family_id": row["multiple_testing_family_id"],
                "generation_provenance": provenance,
            }
            if canonical_hash({"ordinal": ordinal, "proposal": identity, "definition": definition}) != row["artifact_hash"]:
                raise GenerationIntegrityError("E.3 proposal artifact hash is inconsistent.")
        return rows

    def _verify_deterministic_generation(self, connection: sqlite3.Connection, row: sqlite3.Row,
                                         snapshot: _E2Snapshot, family: HypothesisFamilySpec) -> None:
        """Replay predictor-only generation so self-consistent SQL forgery fails."""
        predictors, summary_fingerprint = self._read_predictors(snapshot, family)
        candidates, expected_suppressions, raw_count = self._derive_candidates(snapshot, family, predictors)
        expected_proposals = self._proposals(snapshot, family, candidates, summary_fingerprint)
        actual_proposals = self._proposal_rows(connection, row["generation_run_id"])
        actual_suppressions = self._suppression_rows(connection, row["generation_run_id"])
        if (summary_fingerprint != row["predictor_summary_fingerprint"]
                or raw_count != row["raw_candidate_count"]
                or self._training_population(predictors) != row["training_population_count"]
                or self._universe_fingerprint(expected_proposals) != row["hypothesis_universe_fingerprint"]
                or len(expected_proposals) != len(actual_proposals)
                or self._suppressed_counts(expected_suppressions) != self._suppressed_counts_from_rows(actual_suppressions)):
            raise GenerationIntegrityError("E.3 persisted candidate universe conflicts with deterministic predictor-only replay.")
        for ordinal, (expected, actual) in enumerate(zip(expected_proposals, actual_proposals)):
            actual_payload = self._proposal_payload(actual)
            expected_payload = expected.payload()
            persisted_fields = {
                key: value for key, value in expected_payload.items()
                if key not in {"generation_run_id", "family_id", "family_version", "source_materialization_id"}
            }
            if {key: actual_payload.get(key) for key in persisted_fields} != persisted_fields:
                raise GenerationIntegrityError("E.3 persisted proposal conflicts with deterministic predictor-only replay.")
            definition = _canonical_json(actual["proposed_definition_json"], name="proposal E.1 definition")
            if definition != expected.proposed_definition:
                raise GenerationIntegrityError("E.3 persisted E.1 definition conflicts with deterministic predictor-only replay.")
        for ordinal, ((reason, detail), actual) in enumerate(zip(expected_suppressions, actual_suppressions)):
            if actual["reason"] != reason or _canonical_json(actual["detail_json"], name="suppression detail") != detail:
                raise GenerationIntegrityError("E.3 persisted suppression conflicts with deterministic predictor-only replay.")

    def _suppression_rows(self, connection: sqlite3.Connection, run_id: str) -> list[sqlite3.Row]:
        rows = connection.execute("SELECT * FROM phase_e_generation_suppressions WHERE generation_run_id=? ORDER BY ordinal", (run_id,)).fetchall()
        for ordinal, row in enumerate(rows):
            if int(row["ordinal"]) != ordinal:
                raise GenerationIntegrityError("E.3 suppression ordinals are not contiguous deterministic order.")
            detail = _canonical_json(row["detail_json"], name="suppression detail")
            expected = canonical_hash({"generation_run_id": run_id, "suppression_id": row["suppression_id"], "ordinal": ordinal,
                                       "reason": row["reason"], "detail": detail})
            if expected != row["artifact_hash"]:
                raise GenerationIntegrityError("E.3 suppression artifact hash is inconsistent.")
        return rows

    @staticmethod
    def _universe_fingerprint(proposals: Sequence[HypothesisProposal]) -> str:
        return canonical_hash({"schema": "phase-e3-hypothesis-universe-v1", "proposals": [
            {"ordinal": index, "proposal_id": item.proposal_id, "predicate_hash": item.predicate.predicate_hash,
             "definition_hash": item.proposed_definition_hash, "multiple_testing_family_id": item.multiple_testing_family_id}
            for index, item in enumerate(proposals)
        ]})

    @staticmethod
    def _universe_fingerprint_from_rows(rows: Sequence[sqlite3.Row]) -> str:
        return canonical_hash({"schema": "phase-e3-hypothesis-universe-v1", "proposals": [
            {"ordinal": int(item["ordinal"]), "proposal_id": item["proposal_id"], "predicate_hash": item["predicate_hash"],
             "definition_hash": item["proposed_definition_hash"], "multiple_testing_family_id": item["multiple_testing_family_id"]}
            for item in rows
        ]})

    @staticmethod
    def _suppressed_counts(suppressions: Sequence[tuple[str, Mapping[str, Any]]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for reason, _ in suppressions:
            counts[reason] = counts.get(reason, 0) + 1
        return {key: counts[key] for key in sorted(counts)}

    @staticmethod
    def _suppressed_counts_from_rows(rows: Sequence[sqlite3.Row]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in rows:
            counts[row["reason"]] = counts.get(row["reason"], 0) + 1
        return {key: counts[key] for key in sorted(counts)}

    def _manifest(self, connection: sqlite3.Connection, row: sqlite3.Row, proposals: Sequence[sqlite3.Row],
                  mappings: Sequence[sqlite3.Row]) -> dict[str, Any]:
        spec = self._run_spec_from_row(row)
        family = self._family_from_connection(connection, spec.family_id, spec.family_version)
        suppressions = self._suppression_rows(connection, spec.run_id)
        return {
            "schema": "phase-e3-generation-manifest-v1", "generation_run_id": spec.run_id,
            "source_materialization": {
                "materialization_id": spec.materialization_id, "materialization_specification_hash": spec.materialization_specification_hash,
                "corpus_fingerprint": spec.corpus_fingerprint, "source_fingerprint": spec.source_fingerprint,
                "membership_fingerprint": spec.membership_fingerprint, "sampling_design_fingerprint": spec.sampling_design_fingerprint,
                "feature_fingerprint": spec.feature_fingerprint, "complete_artifact_fingerprint": spec.complete_artifact_fingerprint,
            },
            "family_fingerprint": family.fingerprint, "predictor_summary_fingerprint": row["predictor_summary_fingerprint"],
            "training_population_count": row["training_population_count"],
            "feature_coverage": self._feature_coverage_from_proposal_rows(proposals),
            "raw_candidate_count": row["raw_candidate_count"], "deduplicated_candidate_count": len(proposals),
            "suppressed_counts": self._suppressed_counts_from_rows(suppressions),
            "registered_hypothesis_count": len(mappings), "duplicate_hypothesis_count": 0,
            "hypothesis_universe_fingerprint": self._universe_fingerprint_from_rows(proposals),
            "outcome_reads_attempted": 0, "outcome_reads_permitted": False, "completion_state": GenerationStatus.COMPLETE.value,
            "trading_authority": False,
        }

    @staticmethod
    def _feature_coverage_from_proposal_rows(rows: Sequence[sqlite3.Row]) -> list[dict[str, Any]]:
        seen: dict[str, dict[str, Any]] = {}
        for row in rows:
            feature = Predicate.from_payload(_canonical_json(row["predicate_json"], name="proposal predicate")).feature
            assert feature is not None
            key = _feature_key(feature)
            seen[key] = {"feature": feature.payload(), "training_population_count": int(row["training_population_count"]),
                         "training_missing_count": int(row["training_missing_count"])}
        return [seen[key] for key in sorted(seen)]

    def _run_payload(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        proposals = self._proposal_rows(connection, row["generation_run_id"])
        suppressions = self._suppression_rows(connection, row["generation_run_id"])
        mappings = connection.execute("SELECT * FROM phase_e_generation_e1_mappings WHERE generation_run_id=? ORDER BY proposal_id", (row["generation_run_id"],)).fetchall()
        manifest = connection.execute("SELECT * FROM phase_e_generation_manifests WHERE generation_run_id=?", (row["generation_run_id"],)).fetchone()
        return {
            "generation_run_id": row["generation_run_id"], "generation_specification": self._run_spec_from_row(row).payload(),
            "status": row["status"], "registered_at": row["registered_at"], "completed_at": row["completed_at"],
            "predictor_summary_fingerprint": row["predictor_summary_fingerprint"],
            "training_population_count": row["training_population_count"], "raw_candidate_count": row["raw_candidate_count"],
            "unique_candidate_count": row["unique_candidate_count"],
            "suppressed_counts": self._suppressed_counts_from_rows(suppressions),
            "registered_hypothesis_count": row["registered_hypothesis_count"],
            "hypothesis_universe_fingerprint": row["hypothesis_universe_fingerprint"],
            "proposals": [self._proposal_payload(item) for item in proposals],
            "e1_mappings": [dict(item) for item in mappings],
            "manifest": _canonical_json(manifest["manifest_json"], name="generation manifest") if manifest else None,
            "outcome_reads_attempted": int(row["outcome_reads_attempted"]), "outcome_reads_permitted": False,
            "trading_authority": False, "qualified_signal": False,
        }

    @staticmethod
    def _proposal_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "proposal_id": row["proposal_id"], "ordinal": row["ordinal"],
            "predicate": _canonical_json(row["predicate_json"], name="proposal predicate"), "predicate_hash": row["predicate_hash"],
            "required_features": _canonical_json(row["required_features_json"], name="proposal features"),
            "threshold_provenance": _canonical_json(row["threshold_provenance_json"], name="threshold provenance"),
            "training_support_count": row["training_support_count"], "training_missing_count": row["training_missing_count"],
            "training_population_count": row["training_population_count"], "comparator_policy": row["comparator_policy"],
            "outcome_horizon": {"seconds": row["outcome_horizon_seconds"]},
            "proposed_hypothesis_id": row["proposed_hypothesis_id"], "proposed_hypothesis_version": row["proposed_hypothesis_version"],
            "proposed_definition_hash": row["proposed_definition_hash"], "multiple_testing_family_id": row["multiple_testing_family_id"],
            "generation_provenance": _canonical_json(row["generation_provenance_json"], name="proposal provenance"),
        }

    # ----- typed reconstruction / small helpers -----------------------------------
    def _family_payload(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = _canonical_json(row["specification_json"], name="hypothesis family")
        family = HypothesisFamilySpec.from_payload(payload)
        if family.fingerprint != row["specification_hash"]:
            raise GenerationIntegrityError("Hypothesis-family fingerprint is inconsistent.")
        return {"family": family.payload(), "family_fingerprint": family.fingerprint, "registered_at": row["registered_at"], "trading_authority": False}

    def _family_from_connection(self, connection: sqlite3.Connection, family_id: str, version: int) -> HypothesisFamilySpec:
        row = connection.execute("SELECT * FROM phase_e_hypothesis_families WHERE family_id=? AND version=?", (family_id, version)).fetchone()
        if row is None:
            raise GenerationIntegrityError("Frozen E.3 run references an unknown hypothesis family.")
        payload = _canonical_json(row["specification_json"], name="hypothesis family")
        family = HypothesisFamilySpec.from_payload(payload)
        if family.fingerprint != row["specification_hash"]:
            raise GenerationIntegrityError("Hypothesis-family fingerprint is inconsistent.")
        return family

    @staticmethod
    def _run_spec_from_row(row: sqlite3.Row) -> GenerationRunSpec:
        payload = _canonical_json(row["generation_specification_json"], name="generation specification")
        if not isinstance(payload, Mapping) or payload.get("schema") != "phase-e3-generation-run-v1":
            raise GenerationIntegrityError("Generation specification has an unsupported schema.")
        body = dict(payload); body.pop("schema", None)
        return GenerationRunSpec(**body)

    @staticmethod
    def _partition_from_materialization(specification: Mapping[str, Any]) -> PartitionIdentity:
        raw = specification["partition"]
        return PartitionIdentity(
            partition_id=raw["partition_id"], train_start=raw["train_start"], train_end=raw["train_end"],
            validation_start=raw["validation_start"], validation_end=raw["validation_end"], test_start=raw["test_start"],
            test_end=raw["test_end"], purge_seconds=raw["purge_seconds"], embargo_seconds=raw["embargo_seconds"],
            random_seed=raw["random_seed"], horizon=OutcomeHorizon(raw["outcome_horizon"]["seconds"]),
            feature_lookback_seconds=raw.get("feature_lookback_seconds", 0), sampling_algorithm=raw["sampling_algorithm"],
            outcome_boundary_policy=raw["outcome_boundary_policy"],
        )

    @staticmethod
    def _definition_from_payload(payload: Mapping[str, Any]) -> HypothesisDefinition:
        partition = payload["partition"]
        return HypothesisDefinition(
            hypothesis_id=payload["hypothesis_id"], version=payload["version"], title=payload["title"],
            proposition=payload["proposition"], null_hypothesis=payload["null_hypothesis"], alternative_hypothesis=payload["alternative_hypothesis"],
            population_definition=payload["population_definition"],
            required_features=tuple(FeatureReference(**item) for item in payload["required_features"]),
            feature_transforms=payload["feature_transforms"], entry_definition=payload["entry_definition"],
            outcome_definition=payload["outcome_definition"], outcome_horizon=OutcomeHorizon(payload["outcome_horizon"]["seconds"]),
            comparator_definition=payload["comparator_definition"], inclusion_rules=tuple(payload["inclusion_rules"]),
            exclusion_rules=tuple(payload["exclusion_rules"]), minimum_sample_size=payload["minimum_sample_size"],
            statistical_test=StatisticSpec(payload["statistical_test"]["name"], payload["statistical_test"]["parameters"]),
            minimum_effect_size=payload["minimum_effect_size"], success_threshold=payload["success_threshold"],
            failure_threshold=payload["failure_threshold"], multiple_testing_family=payload["multiple_testing_family"],
            partition=PartitionIdentity(
                partition_id=partition["partition_id"], train_start=partition["train_start"], train_end=partition["train_end"],
                validation_start=partition["validation_start"], validation_end=partition["validation_end"], test_start=partition["test_start"],
                test_end=partition["test_end"], purge_seconds=partition["purge_seconds"], embargo_seconds=partition["embargo_seconds"],
                random_seed=partition["random_seed"], horizon=OutcomeHorizon(partition["outcome_horizon"]["seconds"]),
                feature_lookback_seconds=partition.get("feature_lookback_seconds", 0), sampling_algorithm=partition["sampling_algorithm"],
                outcome_boundary_policy=partition["outcome_boundary_policy"],
            ), code_version=payload["code_version"], config_version=payload["config_version"],
            created_at="2026-01-01T00:00:00Z", predecessor_id=payload.get("predecessor_id"),
        )

    @staticmethod
    def _validate_mapping(proposal: sqlite3.Row, mapping: sqlite3.Row) -> None:
        expected = {
            "generation_run_id": proposal["generation_run_id"], "proposal_id": proposal["proposal_id"],
            "experiment_id": mapping["experiment_id"], "hypothesis_id": proposal["proposed_hypothesis_id"],
            "hypothesis_version": int(proposal["proposed_hypothesis_version"]), "definition_hash": proposal["proposed_definition_hash"],
        }
        if (mapping["hypothesis_id"] != expected["hypothesis_id"] or int(mapping["hypothesis_version"]) != expected["hypothesis_version"]
                or mapping["definition_hash"] != expected["definition_hash"] or mapping["mapping_hash"] != canonical_hash(expected)
                or normalized_utc(mapping["registered_at"]) != mapping["registered_at"]):
            raise GenerationIntegrityError("E.3 proposal-to-E.1 mapping is inconsistent.")

    @staticmethod
    def _require_run(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM phase_e_generation_runs WHERE generation_run_id=?", (run_id,)).fetchone()
        if row is None:
            raise GenerationConflictError(f"Unknown E.3 generation run: {run_id}")
        return row

    @staticmethod
    def _training_population(predictors: Sequence[_PredictorFeature]) -> int:
        return predictors[0].population_count if predictors else 0

    @staticmethod
    def _feature_inventory_payload(item: _PredictorFeature) -> dict[str, Any]:
        return {
            "feature": item.feature.payload(), "available": item.available,
            "training_population_count": item.population_count, "training_nonmissing_count": len(item.values),
            "training_missing_count": item.missing_count, "coverage": (len(item.values) / item.population_count if item.population_count else 0.0),
            "missing_reasons": dict(item.missing_reasons), "predictor_rows_fingerprint": item.rows_fingerprint,
        }
