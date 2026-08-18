"""Phase E.4 preregistered, family-wide scientific evaluation.

E.4 consumes one COMPLETE E.3 universe and the exact COMPLETE E.2 evidence
snapshot that generated it.  It never generates a predicate, changes a train
threshold, queries the reserved test partition, or grants production authority.

The initial method is deliberately conservative:

* validation-only predicate-versus-complement evaluation;
* E.2 sampling-weighted mean net-outcome difference;
* whole-component deterministic bootstrap for uncertainty and a two-sided
  centered null test;
* components connect repeated wallets, shared causal evidence, and overlapping
  same-symbol outcome windows;
* Holm FWER correction over every member of the frozen E.3 universe, with
  unevaluable members retained using correction input p=1.

Protocol registration receives only authoritative contracts and artifact
fingerprints. Its sealed E.2 verifier may internally validate outcome artifact
hashes, but exposes no outcome value to the protocol builder. E.4 outcome values
are first extracted by :meth:`PhaseEEvaluator.evaluate`, after the immutable
protocol and exact member list have committed.
"""

from __future__ import annotations

import json
import math
import random
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from .generation import (
    PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1,
    GenerationStatus,
    HypothesisFamilySpec,
    PhaseEHypothesisGenerator,
    Predicate,
    PredicateOperator,
)
from .materialization import MaterializationStatus, PhaseEMaterializer
from .types import canonical_hash, finite_number, normalized_utc, storage_json


E4_PROTOCOL_SCHEMA = "phase-e4-evaluation-protocol-v1"
E4_RESULT_SCHEMA = "phase-e4-hypothesis-evaluation-v1"
E4_MANIFEST_SCHEMA = "phase-e4-evaluation-manifest-v1"
E4_EVALUATOR_CODE_VERSION = "phase-e4-cluster-bootstrap-v1"
E4_EVALUATOR_CONFIG_VERSION = "phase-e4-scientific-integrity-v1"
E4_STATISTICAL_METHOD = "E4_WEIGHTED_COMPONENT_BOOTSTRAP_MEAN_DIFFERENCE_V1"
E4_CORRECTION_METHOD = "HOLM_BONFERRONI_FWER_V1"
E4_CLUSTER_POLICY = "WALLET_SHARED_SOURCE_OVERLAPPING_WINDOW_COMPONENTS_V1"
E4_HOLDOUT_POLICY = "TEST_RESERVED_NO_QUERY_PATH_V1"


class EvaluationError(RuntimeError):
    """Base E.4 error."""


class EvaluationConflictError(EvaluationError):
    """An immutable protocol/run identity was contradicted."""


class EvaluationIntegrityError(EvaluationError):
    """Authoritative persisted evidence cannot be reconciled."""


class EvaluationRunStatus(StrEnum):
    EVALUATING = "EVALUATING"
    COMPLETE = "COMPLETE"
    PENDING = "PENDING"
    INVALID = "INVALID"


class HypothesisEvaluationStatus(StrEnum):
    EVALUATED = "EVALUATED"
    INSUFFICIENT_SUPPORT = "INSUFFICIENT_SUPPORT"
    PENDING_OUTCOME_MATURITY = "PENDING_OUTCOME_MATURITY"
    INCONCLUSIVE_MISSING_EVIDENCE = "INCONCLUSIVE_MISSING_EVIDENCE"
    INVALID_EVIDENCE = "INVALID_EVIDENCE"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _load(raw: Any, name: str) -> Any:
    if not isinstance(raw, str):
        raise EvaluationIntegrityError(f"{name} must be canonical JSON text.")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise EvaluationIntegrityError(f"{name} is malformed.") from exc
    if storage_json(value) != raw:
        raise EvaluationIntegrityError(f"{name} is not canonical JSON.")
    return value


def _positive_int(value: Any, name: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer of at least {minimum}.")
    return value


@dataclass(frozen=True)
class EvaluationSettings:
    """The small set of E.4-only rules fixed before outcome values are exposed.

    E.3 remains authoritative for alpha, the minimum effect floor, minimum arm
    support, statistical direction, resample count, and seed. E.4 adds a
    preregistered practical-relevance threshold that may only be stricter, plus
    dependence and numeric-integrity gates.
    """

    protocol_version: int = 1
    minimum_independent_components: int = 8
    minimum_components_per_arm: int = 2
    minimum_valid_resample_fraction: float = 0.90
    minimum_practical_effect: float = 0.001
    maximum_absolute_outcome: float = 10.0
    maximum_sampling_weight: float = 1_000_000_000.0

    def __post_init__(self) -> None:
        _positive_int(self.protocol_version, "Protocol version")
        _positive_int(self.minimum_independent_components, "Minimum independent components", minimum=2)
        _positive_int(self.minimum_components_per_arm, "Minimum components per arm", minimum=2)
        finite_number(self.minimum_valid_resample_fraction, name="minimum valid resample fraction", minimum=0.0, maximum=1.0)
        if not 0.5 <= float(self.minimum_valid_resample_fraction) <= 1.0:
            raise ValueError("Minimum valid resample fraction must be in [0.5, 1].")
        finite_number(self.minimum_practical_effect, name="minimum practical effect", minimum=0.0)
        finite_number(self.maximum_absolute_outcome, name="maximum absolute outcome", minimum=0.0)
        finite_number(self.maximum_sampling_weight, name="maximum sampling weight", minimum=0.0)
        if self.maximum_absolute_outcome == 0 or self.maximum_sampling_weight == 0:
            raise ValueError("Numeric integrity bounds must be greater than zero.")

    def payload(self) -> dict[str, Any]:
        return {
            "protocol_version": self.protocol_version,
            "minimum_independent_components": self.minimum_independent_components,
            "minimum_components_per_arm": self.minimum_components_per_arm,
            "minimum_valid_resample_fraction": finite_number(
                self.minimum_valid_resample_fraction, name="minimum valid resample fraction", minimum=0.0, maximum=1.0,
            ),
            "minimum_practical_effect": finite_number(
                self.minimum_practical_effect, name="minimum practical effect", minimum=0.0,
            ),
            "maximum_absolute_outcome": finite_number(
                self.maximum_absolute_outcome, name="maximum absolute outcome", minimum=0.0,
            ),
            "maximum_sampling_weight": finite_number(
                self.maximum_sampling_weight, name="maximum sampling weight", minimum=0.0,
            ),
        }


@dataclass(frozen=True)
class _Observation:
    observation_id: str
    ordinal: int
    anchor_at: str
    wallet: str | None
    symbol: str | None
    source_event_id: str
    stratum_id: str
    sampling_weight: float
    feature_values: Mapping[str, float | None]
    feature_sources: tuple[str, ...]
    outcome_state: str
    outcome_reason: str | None
    net_outcome: float | None
    outcome_source_id: str | None
    resolved_at: str | None


class _DisjointSet:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self.parent[max(left_root, right_root)] = min(left_root, right_root)


class PhaseEEvaluator:
    """Authoritative E.4 protocol registry and deterministic evaluator."""

    TRADING_AUTHORITY = False
    PREDICTION_AUTHORITY = False
    SIGNAL_AUTHORITY = False

    def __init__(self, database_path: str | Path, *, fault_hook: Callable[[str], None] | None = None) -> None:
        self.path = Path(database_path)
        self._initialized = False
        self._fault_hook = fault_hook

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
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()

    def initialize(self) -> None:
        if self._initialized:
            return
        PhaseEHypothesisGenerator(self.path).initialize()
        with self._write() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS phase_e_evaluation_protocols (
                    protocol_id TEXT PRIMARY KEY,
                    generation_run_id TEXT NOT NULL UNIQUE REFERENCES phase_e_generation_runs(generation_run_id),
                    family_id TEXT NOT NULL,
                    family_version INTEGER NOT NULL,
                    materialization_id TEXT NOT NULL REFERENCES phase_e_materializations(materialization_id),
                    protocol_version INTEGER NOT NULL CHECK(protocol_version > 0),
                    protocol_json TEXT NOT NULL,
                    protocol_hash TEXT NOT NULL,
                    family_size INTEGER NOT NULL CHECK(family_size > 0),
                    status TEXT NOT NULL CHECK(status IN ('REGISTERING','REGISTERED')),
                    registered_at TEXT NOT NULL,
                    FOREIGN KEY(family_id, family_version) REFERENCES phase_e_hypothesis_families(family_id, version)
                );
                CREATE TABLE IF NOT EXISTS phase_e_evaluation_protocol_members (
                    protocol_id TEXT NOT NULL REFERENCES phase_e_evaluation_protocols(protocol_id),
                    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                    proposal_id TEXT NOT NULL,
                    predicate_hash TEXT NOT NULL,
                    experiment_id TEXT NOT NULL,
                    hypothesis_id TEXT NOT NULL,
                    hypothesis_version INTEGER NOT NULL CHECK(hypothesis_version > 0),
                    definition_hash TEXT NOT NULL,
                    e3_mapping_hash TEXT NOT NULL,
                    member_hash TEXT NOT NULL,
                    PRIMARY KEY(protocol_id, proposal_id),
                    UNIQUE(protocol_id, ordinal),
                    UNIQUE(protocol_id, predicate_hash),
                    FOREIGN KEY(experiment_id) REFERENCES phase_e_experiments(experiment_id),
                    FOREIGN KEY(hypothesis_id, hypothesis_version) REFERENCES phase_e_hypotheses(hypothesis_id, version)
                );
                CREATE TABLE IF NOT EXISTS phase_e_evaluation_protocol_events (
                    event_id TEXT PRIMARY KEY,
                    protocol_id TEXT NOT NULL REFERENCES phase_e_evaluation_protocols(protocol_id),
                    event_type TEXT NOT NULL,
                    event_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e_evaluation_runs (
                    evaluation_run_id TEXT PRIMARY KEY,
                    protocol_id TEXT NOT NULL UNIQUE REFERENCES phase_e_evaluation_protocols(protocol_id),
                    protocol_hash TEXT NOT NULL,
                    materialization_id TEXT NOT NULL REFERENCES phase_e_materializations(materialization_id),
                    evidence_snapshot_hash TEXT NOT NULL,
                    correction_family_size INTEGER NOT NULL CHECK(correction_family_size > 0),
                    status TEXT NOT NULL CHECK(status IN ('EVALUATING','COMPLETE','PENDING','INVALID')),
                    started_at TEXT NOT NULL,
                    completed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS phase_e_hypothesis_evaluations (
                    evaluation_run_id TEXT NOT NULL REFERENCES phase_e_evaluation_runs(evaluation_run_id),
                    protocol_id TEXT NOT NULL,
                    proposal_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                    hypothesis_id TEXT NOT NULL,
                    evaluation_status TEXT NOT NULL,
                    raw_p_value REAL,
                    adjusted_p_value REAL NOT NULL CHECK(adjusted_p_value >= 0 AND adjusted_p_value <= 1),
                    correction_rank INTEGER NOT NULL CHECK(correction_rank > 0),
                    result_json TEXT NOT NULL,
                    result_hash TEXT NOT NULL,
                    PRIMARY KEY(evaluation_run_id, proposal_id),
                    UNIQUE(evaluation_run_id, ordinal),
                    FOREIGN KEY(protocol_id, proposal_id)
                        REFERENCES phase_e_evaluation_protocol_members(protocol_id, proposal_id)
                );
                CREATE TABLE IF NOT EXISTS phase_e_evaluation_manifests (
                    evaluation_run_id TEXT PRIMARY KEY REFERENCES phase_e_evaluation_runs(evaluation_run_id),
                    manifest_json TEXT NOT NULL,
                    manifest_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e_evaluation_events (
                    event_id TEXT PRIMARY KEY,
                    evaluation_run_id TEXT NOT NULL REFERENCES phase_e_evaluation_runs(evaluation_run_id),
                    event_type TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    event_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_phase_e_evaluation_protocols_family
                    ON phase_e_evaluation_protocols(family_id, family_version, registered_at);
                CREATE INDEX IF NOT EXISTS idx_phase_e_evaluation_results_status
                    ON phase_e_hypothesis_evaluations(evaluation_status, protocol_id, ordinal);
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_protocol_inputs_immutable
                    BEFORE UPDATE OF protocol_id, generation_run_id, family_id, family_version, materialization_id,
                        protocol_version, protocol_json, protocol_hash, family_size, registered_at
                    ON phase_e_evaluation_protocols
                    BEGIN SELECT RAISE(ABORT, 'E.4 protocol scientific inputs are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_protocol_status_once
                    BEFORE UPDATE OF status ON phase_e_evaluation_protocols
                    WHEN OLD.status <> 'REGISTERING' OR NEW.status <> 'REGISTERED'
                    BEGIN SELECT RAISE(ABORT, 'E.4 protocol may only seal once'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_protocols_no_delete
                    BEFORE DELETE ON phase_e_evaluation_protocols
                    BEGIN SELECT RAISE(ABORT, 'E.4 protocols cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_members_registering_insert
                    BEFORE INSERT ON phase_e_evaluation_protocol_members
                    WHEN (SELECT status FROM phase_e_evaluation_protocols WHERE protocol_id=NEW.protocol_id) <> 'REGISTERING'
                    BEGIN SELECT RAISE(ABORT, 'E.4 family members freeze only during protocol registration'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_members_no_update
                    BEFORE UPDATE ON phase_e_evaluation_protocol_members
                    BEGIN SELECT RAISE(ABORT, 'E.4 protocol members are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_members_no_delete
                    BEFORE DELETE ON phase_e_evaluation_protocol_members
                    BEGIN SELECT RAISE(ABORT, 'E.4 protocol members cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_protocol_events_no_update
                    BEFORE UPDATE ON phase_e_evaluation_protocol_events
                    BEGIN SELECT RAISE(ABORT, 'E.4 protocol events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_protocol_events_no_delete
                    BEFORE DELETE ON phase_e_evaluation_protocol_events
                    BEGIN SELECT RAISE(ABORT, 'E.4 protocol events cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_run_inputs_immutable
                    BEFORE UPDATE OF evaluation_run_id, protocol_id, protocol_hash, materialization_id,
                        evidence_snapshot_hash, correction_family_size, started_at
                    ON phase_e_evaluation_runs
                    BEGIN SELECT RAISE(ABORT, 'E.4 evaluation inputs are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_run_status_once
                    BEFORE UPDATE OF status ON phase_e_evaluation_runs
                    WHEN OLD.status <> 'EVALUATING' OR NEW.status NOT IN ('COMPLETE','PENDING','INVALID')
                    BEGIN SELECT RAISE(ABORT, 'E.4 run may finalize only once'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_runs_no_delete
                    BEFORE DELETE ON phase_e_evaluation_runs
                    BEGIN SELECT RAISE(ABORT, 'E.4 evaluation runs cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_hypothesis_evaluations_evaluating_insert
                    BEFORE INSERT ON phase_e_hypothesis_evaluations
                    WHEN (SELECT status FROM phase_e_evaluation_runs WHERE evaluation_run_id=NEW.evaluation_run_id) <> 'EVALUATING'
                    BEGIN SELECT RAISE(ABORT, 'E.4 results may be inserted only during atomic evaluation'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_hypothesis_evaluations_no_update
                    BEFORE UPDATE ON phase_e_hypothesis_evaluations
                    BEGIN SELECT RAISE(ABORT, 'E.4 hypothesis evaluations are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_hypothesis_evaluations_no_delete
                    BEFORE DELETE ON phase_e_hypothesis_evaluations
                    BEGIN SELECT RAISE(ABORT, 'E.4 hypothesis evaluations cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_manifests_evaluating_insert
                    BEFORE INSERT ON phase_e_evaluation_manifests
                    WHEN (SELECT status FROM phase_e_evaluation_runs WHERE evaluation_run_id=NEW.evaluation_run_id) <> 'EVALUATING'
                    BEGIN SELECT RAISE(ABORT, 'E.4 manifest must commit with its result family'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_manifests_no_update
                    BEFORE UPDATE ON phase_e_evaluation_manifests
                    BEGIN SELECT RAISE(ABORT, 'E.4 manifests are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_manifests_no_delete
                    BEFORE DELETE ON phase_e_evaluation_manifests
                    BEGIN SELECT RAISE(ABORT, 'E.4 manifests cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_events_no_update
                    BEFORE UPDATE ON phase_e_evaluation_events
                    BEGIN SELECT RAISE(ABORT, 'E.4 lifecycle events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_evaluation_events_no_delete
                    BEFORE DELETE ON phase_e_evaluation_events
                    BEGIN SELECT RAISE(ABORT, 'E.4 lifecycle events cannot be deleted'); END;
                """
            )
        self._initialized = True

    # ----- protocol registration -------------------------------------------------
    def eligible_families(self) -> list[dict[str, Any]]:
        """Return COMPLETE E.3 universes eligible for one E.4 protocol."""
        self.initialize()
        registered = {item["generation_run_id"] for item in self.list_protocols()}
        output = []
        for run in PhaseEHypothesisGenerator(self.path).list():
            if run["status"] == GenerationStatus.COMPLETE.value:
                output.append({
                    "generation_run_id": run["generation_run_id"],
                    "family_id": run["generation_specification"]["family_id"],
                    "family_version": run["generation_specification"]["family_version"],
                    "materialization_id": run["generation_specification"]["materialization_id"],
                    "family_size": run["registered_hypothesis_count"],
                    "hypothesis_universe_fingerprint": run["hypothesis_universe_fingerprint"],
                    "protocol_registered": run["generation_run_id"] in registered,
                    "evaluation_partition": "validation",
                    "test_holdout_access": "FORBIDDEN",
                    "trading_authority": False,
                })
        return output

    def preregister(
        self, generation_run_id: str, *, settings: EvaluationSettings | None = None,
        registered_at: str | None = None,
    ) -> dict[str, Any]:
        """Freeze one protocol before E.4 outcome values are exposed."""
        self.initialize()
        settings = settings or EvaluationSettings()
        if not isinstance(settings, EvaluationSettings):
            raise ValueError("E.4 settings must use the typed preregistration contract.")
        inputs = self._authoritative_inputs(generation_run_id)
        protocol = self._build_protocol(inputs, settings)
        protocol_hash = canonical_hash(protocol)
        protocol_id = "e4p-" + protocol_hash[:32]
        at = normalized_utc(registered_at or _now())
        proposals = inputs["run"]["proposals"]
        mappings = {item["proposal_id"]: item for item in inputs["run"]["e1_mappings"]}
        with self._write() as connection:
            existing_for_run = connection.execute(
                "SELECT * FROM phase_e_evaluation_protocols WHERE generation_run_id=?", (generation_run_id,),
            ).fetchone()
            if existing_for_run is not None:
                if existing_for_run["protocol_id"] != protocol_id or existing_for_run["protocol_hash"] != protocol_hash:
                    raise EvaluationConflictError(
                        "This E.3 generation run already has a sealed E.4 protocol; observed evidence cannot select another method.",
                    )
                self._validate_protocol(connection, existing_for_run)
                return self._protocol_payload(connection, existing_for_run)
            connection.execute(
                """INSERT INTO phase_e_evaluation_protocols(
                       protocol_id, generation_run_id, family_id, family_version, materialization_id,
                       protocol_version, protocol_json, protocol_hash, family_size, status, registered_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'REGISTERING', ?)""",
                (
                    protocol_id, generation_run_id, protocol["family"]["family_id"], protocol["family"]["family_version"],
                    protocol["evidence_snapshot"]["materialization_id"], settings.protocol_version,
                    storage_json(protocol), protocol_hash, len(proposals), at,
                ),
            )
            self._append_protocol_event(connection, protocol_id, "PREREGISTERED", at, {
                "protocol_hash": protocol_hash, "generation_run_id": generation_run_id,
                "hypothesis_universe_fingerprint": protocol["family"]["hypothesis_universe_fingerprint"],
                "outcome_values_exposed_to_protocol_builder": False,
                "evaluation_partition": "validation", "holdout_access": "FORBIDDEN",
            })
            for ordinal, proposal in enumerate(proposals):
                if int(proposal["ordinal"]) != ordinal or proposal["proposal_id"] not in mappings:
                    raise EvaluationIntegrityError("E.3 family/mapping order is incomplete during E.4 registration.")
                mapping = mappings[proposal["proposal_id"]]
                member = {
                    "protocol_id": protocol_id, "ordinal": ordinal, "proposal_id": proposal["proposal_id"],
                    "predicate_hash": proposal["predicate_hash"], "experiment_id": mapping["experiment_id"],
                    "hypothesis_id": mapping["hypothesis_id"], "hypothesis_version": int(mapping["hypothesis_version"]),
                    "definition_hash": mapping["definition_hash"], "e3_mapping_hash": mapping["mapping_hash"],
                }
                member_hash = canonical_hash(member)
                connection.execute(
                    """INSERT INTO phase_e_evaluation_protocol_members(
                           protocol_id, ordinal, proposal_id, predicate_hash, experiment_id, hypothesis_id,
                           hypothesis_version, definition_hash, e3_mapping_hash, member_hash
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (*member.values(), member_hash),
                )
            connection.execute(
                "UPDATE phase_e_evaluation_protocols SET status='REGISTERED' WHERE protocol_id=? AND status='REGISTERING'",
                (protocol_id,),
            )
            if connection.execute("SELECT changes()").fetchone()[0] != 1:
                raise EvaluationConflictError("E.4 protocol sealing lost its compare-and-swap.")
            self._append_protocol_event(connection, protocol_id, "FAMILY_FROZEN", at, {
                "family_size": len(proposals),
                "member_list_hash": self._member_list_hash(connection, protocol_id),
                "correction_denominator_policy": "EXACT_E3_UNIVERSE_UNEVALUABLE_AS_ONE_V1",
            })
            row = self._require_protocol(connection, protocol_id)
            self._validate_protocol(connection, row)
            return self._protocol_payload(connection, row)

    def _authoritative_inputs(self, generation_run_id: str) -> dict[str, Any]:
        generator = PhaseEHypothesisGenerator(self.path)
        generator.verify(generation_run_id)
        run = generator.get(generation_run_id)
        if run["status"] != GenerationStatus.COMPLETE.value or not run["proposals"]:
            raise EvaluationConflictError("E.4 requires a nonempty COMPLETE E.3 generation run.")
        spec = run["generation_specification"]
        family_entry = generator.get_family(spec["family_id"], int(spec["family_version"]))
        family = HypothesisFamilySpec.from_payload(family_entry["family"])
        materialization = PhaseEMaterializer(self.path).get(spec["materialization_id"])
        if materialization["status"] != MaterializationStatus.COMPLETE.value:
            raise EvaluationIntegrityError("E.4 requires the exact COMPLETE E.2 snapshot bound by E.3.")
        if materialization["specification"].get("materializer_code_version") == "phase-e2-materializer-v1":
            raise EvaluationIntegrityError("Initial E.4 refuses the legacy E.2 outcome schema.")
        plan = family.statistical_test_plan
        if (
            plan.test_id != "E4_WEIGHTED_NET_OUTCOME_DISTRIBUTION_DIFFERENCE_V1"
            or plan.direction != "TWO_SIDED"
            or plan.effect_metric != "DECLARED_PHASE_E2_NET_OUTCOME_V2"
            or plan.comparator_policy != PREDICATE_COMPLEMENT_WITHIN_PARTITION_V1
            or not plan.sampling_weights_required
            or plan.resampling_seed is None
            or plan.resample_count is None
        ):
            raise EvaluationIntegrityError("E.3 statistical plan is incompatible with the initial E.4 evaluator.")
        if len(run["e1_mappings"]) != len(run["proposals"]):
            raise EvaluationIntegrityError("E.3 does not map every frozen proposal to authoritative E.1 state.")
        multiple_families = {item["multiple_testing_family_id"] for item in run["proposals"]}
        if len(multiple_families) != 1:
            raise EvaluationIntegrityError("One E.3 run must bind exactly one multiple-testing family.")
        return {"run": run, "family": family, "materialization": materialization}

    def _build_protocol(self, inputs: Mapping[str, Any], settings: EvaluationSettings) -> dict[str, Any]:
        run, family, materialization = inputs["run"], inputs["family"], inputs["materialization"]
        spec = materialization["specification"]
        plan = family.statistical_test_plan
        partition = spec["partition"]
        if settings.minimum_practical_effect < plan.minimum_effect_size:
            raise EvaluationIntegrityError("E.4 practical relevance threshold cannot weaken the frozen E.3 effect floor.")
        return {
            "schema": E4_PROTOCOL_SCHEMA,
            "protocol_version": settings.protocol_version,
            "evaluator": {"code_version": E4_EVALUATOR_CODE_VERSION, "config_version": E4_EVALUATOR_CONFIG_VERSION},
            "family": {
                "family_id": family.family_id, "family_version": family.version,
                "family_fingerprint": family.fingerprint,
                "generation_run_id": run["generation_run_id"],
                "hypothesis_universe_fingerprint": run["hypothesis_universe_fingerprint"],
                "multiple_testing_family_id": run["proposals"][0]["multiple_testing_family_id"],
                "family_size": len(run["proposals"]),
                "family_membership_rule": "EXACT_CONTIGUOUS_E3_PROPOSAL_UNIVERSE_V1",
            },
            "evidence_snapshot": {
                "materialization_id": materialization["materialization_id"],
                "materialization_specification_hash": materialization["specification_hash"],
                "source_universe_hash": canonical_hash(materialization["source_universe"]),
                "corpus_fingerprint": materialization["source_universe"]["corpus_fingerprint"],
                "membership_fingerprint": materialization["membership_fingerprint"],
                "sampling_design_fingerprint": materialization["sampling_design_fingerprint"],
                "feature_artifact_fingerprint": materialization["feature_artifact_fingerprint"],
                "outcome_artifact_fingerprint": materialization["outcome_artifact_fingerprint"],
                "completed_artifact_fingerprint": materialization["completed_artifact_fingerprint"],
                "evidence_cutoff": materialization["source_universe"]["interval_end"],
                "eligible_source": spec["eligibility"]["source"],
            },
            "partitions": {
                "identity": partition,
                "evaluation_partition": "validation",
                "train_usage": "E3_THRESHOLD_DERIVATION_ONLY",
                "validation_usage": "ONE_SHOT_E4_FAMILY_EVALUATION",
                "test_usage": "RESERVED_NOT_QUERIED",
                "holdout_policy": E4_HOLDOUT_POLICY,
                "maximum_test_queries": 0,
                "outcome_containment": "ANCHOR_AND_RESOLUTION_BEFORE_END_EXCLUSIVE_V1",
                "purge_embargo_policy": "E1_HORIZON_LOOKBACK_PURGE_EMBARGO_GAP_V1",
            },
            "outcome": {
                "definition": "E2_HISTORICAL_TRADE_RETURN_V2.net_outcome",
                "horizon_seconds": spec["outcome_horizon"]["seconds"],
                "resolution": spec["outcome_resolution"],
                "unresolved_policy": "PENDING_NEVER_NEGATIVE_V1",
                "mature_missing_policy": "INCONCLUSIVE_RETAIN_IN_FAMILY_V1",
                "invalid_policy": "INVALID_RETAIN_IN_FAMILY_V1",
                "maximum_absolute_outcome": settings.maximum_absolute_outcome,
            },
            "sample_rules": {
                "experimental_observation": "ONE_FROZEN_E2_VALIDATION_WALLET_FILL_ANCHOR_V1",
                "independence_unit": "ONE_CONNECTED_WALLET_EVENT_TIME_COMPONENT_V1",
                "inclusion": [
                    "exact E.2 validation membership", "complete frozen predicate feature",
                    "resolved finite E.2 net outcome", "authoritative wallet/event lineage available",
                ],
                "exclusion": ["none silently; any unavailable required row fails closed for that family snapshot"],
                "minimum_observations_per_arm": plan.minimum_sample_size,
                "minimum_independent_components": settings.minimum_independent_components,
                "minimum_components_per_arm": settings.minimum_components_per_arm,
                "sampling_weights": "EXACT_E2_STRATUM_INVERSE_INCLUSION_WEIGHT_V1",
                "maximum_sampling_weight": settings.maximum_sampling_weight,
            },
            "effect": {
                "metric": "E2_SAMPLING_WEIGHTED_MEAN_NET_OUTCOME_DIFFERENCE_PREDICATE_MINUS_COMPLEMENT_V1",
                "e3_minimum_effect_floor": plan.minimum_effect_size,
                "minimum_practical_effect": settings.minimum_practical_effect,
                "practical_relevance_rule": "ABS_EFFECT_AT_LEAST_PREREGISTERED_E4_THRESHOLD_V1",
            },
            "uncertainty": {
                "metric": "PERCENTILE_COMPONENT_BOOTSTRAP_INTERVAL_V1",
                "confidence_level": 1.0 - plan.significance_threshold,
                "component_policy": E4_CLUSTER_POLICY,
                "resample_count": plan.resample_count,
                "base_seed": plan.resampling_seed,
                "seed_derivation": "SHA256_PROTOCOL_PROPOSAL_BASE_SEED_V1",
                "minimum_valid_resample_fraction": settings.minimum_valid_resample_fraction,
            },
            "test": {
                "null": "weighted predicate and complement mean net outcomes are equal",
                "alternative": "weighted predicate and complement mean net outcomes differ",
                "direction": "TWO_SIDED",
                "method": E4_STATISTICAL_METHOD,
                "raw_p_value_rule": "PLUS_ONE_CENTERED_COMPONENT_BOOTSTRAP_V1",
            },
            "multiple_testing": {
                "method": E4_CORRECTION_METHOD,
                "method_version": 1,
                "error_rate": "FWER",
                "family_alpha": plan.significance_threshold,
                "denominator": len(run["proposals"]),
                "unevaluable_correction_input": 1.0,
                "tie_break": "RAW_P_THEN_E3_ORDINAL_THEN_PROPOSAL_ID_V1",
            },
            "decisions": {
                "statistical_support": "HOLM_ADJUSTED_P_AT_MOST_FAMILY_ALPHA_V1",
                "practical_relevance": "SEPARATE_ABSOLUTE_EFFECT_GATE_V1",
                "scientific_support": "STATISTICAL_AND_PRACTICAL_BOTH_REQUIRED_V1",
                "downstream": "SCIENTIFIC_REVIEW_ONLY_NO_AUTOMATIC_PROMOTION_V1",
            },
            "limitations": [
                "Distinct unlinked wallets may still share unobserved coordination.",
                "Initial E.4 evaluates validation only and makes no causal or production-performance claim.",
            ],
            "authority": {"prediction": False, "signal": False, "execution": False, "trading": False},
        }

    # ----- execution -------------------------------------------------------------
    def evaluate(self, protocol_id: str, *, evaluated_at: str | None = None) -> dict[str, Any]:
        """Evaluate and finalize one family atomically.

        A killed process rolls back the entire run/results/manifest transaction;
        a retry recomputes the same deterministic artifacts. Concurrent workers
        serialize on SQLite and return the same immutable run.
        """
        self.initialize()
        with self._connection() as connection:
            protocol_row = self._require_protocol(connection, protocol_id)
            protocol = _load(protocol_row["protocol_json"], "E.4 protocol")
            generation_run_id = protocol["family"]["generation_run_id"]
        # Sealed deterministic E.2/E.3 replay precedes direct E.4 evidence extraction.
        self._authoritative_inputs(generation_run_id)
        at = normalized_utc(evaluated_at or _now())
        with self._write() as connection:
            protocol_row = self._require_protocol(connection, protocol_id)
            self._validate_protocol(connection, protocol_row)
            prior = connection.execute("SELECT * FROM phase_e_evaluation_runs WHERE protocol_id=?", (protocol_id,)).fetchone()
            if prior is not None:
                self._validate_run(connection, prior)
                return self._run_payload(connection, prior)
            protocol = _load(protocol_row["protocol_json"], "E.4 protocol")
            self._validate_upstream_locked(connection, protocol)
            observations, evidence_identity = self._load_validation_evidence(connection, protocol)
            members = connection.execute(
                "SELECT * FROM phase_e_evaluation_protocol_members WHERE protocol_id=? ORDER BY ordinal", (protocol_id,),
            ).fetchall()
            proposals = self._current_proposals(connection, protocol)
            results = self._compute_results(protocol_id, protocol, members, proposals, observations)
            evidence_snapshot_hash = canonical_hash(evidence_identity)
            run_id = "e4r-" + canonical_hash({
                "schema": "phase-e4-evaluation-run-identity-v1", "protocol_id": protocol_id,
                "protocol_hash": protocol_row["protocol_hash"], "evidence_snapshot_hash": evidence_snapshot_hash,
                "evaluator_code_version": E4_EVALUATOR_CODE_VERSION,
            })[:32]
            connection.execute(
                """INSERT INTO phase_e_evaluation_runs(
                       evaluation_run_id, protocol_id, protocol_hash, materialization_id, evidence_snapshot_hash,
                       correction_family_size, status, started_at, completed_at
                   ) VALUES (?, ?, ?, ?, ?, ?, 'EVALUATING', ?, NULL)""",
                (run_id, protocol_id, protocol_row["protocol_hash"], protocol["evidence_snapshot"]["materialization_id"],
                 evidence_snapshot_hash, len(members), at),
            )
            self._append_run_event(connection, run_id, "EVALUATING", None, EvaluationRunStatus.EVALUATING,
                                   "immutable_protocol_evaluation_started", at, {
                                       "protocol_hash": protocol_row["protocol_hash"],
                                       "evidence_snapshot_hash": evidence_snapshot_hash,
                                       "correction_family_size": len(members), "test_partition_queries": 0,
                                   })
            result_hashes: list[dict[str, Any]] = []
            for result in results:
                result_hash = canonical_hash(result)
                connection.execute(
                    """INSERT INTO phase_e_hypothesis_evaluations(
                           evaluation_run_id, protocol_id, proposal_id, ordinal, hypothesis_id, evaluation_status,
                           raw_p_value, adjusted_p_value, correction_rank, result_json, result_hash
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (run_id, protocol_id, result["proposal_id"], result["ordinal"], result["hypothesis_id"],
                     result["evaluation_status"], result["raw_p_value"], result["adjusted_p_value"],
                     result["correction_rank"], storage_json(result), result_hash),
                )
                result_hashes.append({"ordinal": result["ordinal"], "proposal_id": result["proposal_id"], "result_hash": result_hash})
            final_status = self._final_status(results)
            manifest = {
                "schema": E4_MANIFEST_SCHEMA, "evaluation_run_id": run_id, "protocol_id": protocol_id,
                "protocol_hash": protocol_row["protocol_hash"], "family": protocol["family"],
                "evidence_snapshot": {**protocol["evidence_snapshot"], "evaluation_snapshot_hash": evidence_snapshot_hash},
                "evaluation_partition": "validation", "test_partition_queries": 0,
                "holdout_policy": E4_HOLDOUT_POLICY,
                "correction": {
                    "method": E4_CORRECTION_METHOD, "family_size": len(members),
                    "member_list_hash": self._member_list_hash(connection, protocol_id),
                    "ordered_ranks": [
                        {"proposal_id": item["proposal_id"], "rank": item["correction_rank"],
                         "adjusted_p_value": item["adjusted_p_value"]}
                        for item in sorted(results, key=lambda value: value["correction_rank"])
                    ],
                },
                "result_hashes": result_hashes,
                "status_counts": self._status_counts(results), "completion_state": final_status.value,
                "authority": {"prediction": False, "signal": False, "execution": False, "trading": False},
            }
            manifest_hash = canonical_hash(manifest)
            connection.execute(
                "INSERT INTO phase_e_evaluation_manifests VALUES (?, ?, ?)",
                (run_id, storage_json(manifest), manifest_hash),
            )
            if self._fault_hook is not None:
                self._fault_hook("AFTER_RESULTS_INSERTED")
            connection.execute(
                "UPDATE phase_e_evaluation_runs SET status=?, completed_at=? WHERE evaluation_run_id=? AND status='EVALUATING'",
                (final_status.value, at, run_id),
            )
            if connection.execute("SELECT changes()").fetchone()[0] != 1:
                raise EvaluationConflictError("Concurrent E.4 finalization lost its compare-and-swap.")
            self._append_run_event(connection, run_id, final_status.value, EvaluationRunStatus.EVALUATING, final_status,
                                   "atomic_family_evaluation_finalized", at, {
                                       "manifest_hash": manifest_hash, "result_count": len(results),
                                       "status_counts": self._status_counts(results), "test_partition_queries": 0,
                                   })
            row = self._require_run(connection, run_id)
            self._validate_run(connection, row)
            return self._run_payload(connection, row)

    def _validate_upstream_locked(self, connection: sqlite3.Connection, protocol: Mapping[str, Any]) -> None:
        materializer = PhaseEMaterializer(self.path)
        materializer._initialized = True
        materialization_row = connection.execute(
            "SELECT * FROM phase_e_materializations WHERE materialization_id=?",
            (protocol["evidence_snapshot"]["materialization_id"],),
        ).fetchone()
        if materialization_row is None:
            raise EvaluationIntegrityError("E.4 protocol references a missing E.2 materialization.")
        materializer._validate_materialization(connection, materialization_row)
        generator = PhaseEHypothesisGenerator(self.path)
        generator._initialized = True
        generation_row = connection.execute(
            "SELECT * FROM phase_e_generation_runs WHERE generation_run_id=?",
            (protocol["family"]["generation_run_id"],),
        ).fetchone()
        if generation_row is None:
            raise EvaluationIntegrityError("E.4 protocol references a missing E.3 run.")
        generator._validate_run(connection, generation_row)
        feature_count, feature_fp = materializer._feature_fingerprint(connection, materialization_row["materialization_id"])
        outcome_count, outcome_fp = materializer._outcome_fingerprint(connection, materialization_row["materialization_id"])
        expected = protocol["evidence_snapshot"]
        if (
            materialization_row["status"] != MaterializationStatus.COMPLETE.value
            or generation_row["status"] != GenerationStatus.COMPLETE.value
            or generation_row["family_fingerprint"] != protocol["family"]["family_fingerprint"]
            or materialization_row["specification_hash"] != expected["materialization_specification_hash"]
            or canonical_hash(_load(materialization_row["source_universe_json"], "E.2 source universe")) != expected["source_universe_hash"]
            or materialization_row["membership_fingerprint"] != expected["membership_fingerprint"]
            or materialization_row["sampling_design_fingerprint"] != expected["sampling_design_fingerprint"]
            or feature_fp != expected["feature_artifact_fingerprint"]
            or outcome_fp != expected["outcome_artifact_fingerprint"]
            or materialization_row["completed_artifact_fingerprint"] != expected["completed_artifact_fingerprint"]
            or generation_row["hypothesis_universe_fingerprint"] != protocol["family"]["hypothesis_universe_fingerprint"]
            or int(generation_row["registered_hypothesis_count"]) != int(protocol["family"]["family_size"])
            or feature_count < 0 or outcome_count < 0
        ):
            raise EvaluationIntegrityError("E.2/E.3 evidence no longer matches the preregistered E.4 protocol.")
        protocol_row = connection.execute(
            "SELECT protocol_id FROM phase_e_evaluation_protocols WHERE generation_run_id=?",
            (protocol["family"]["generation_run_id"],),
        ).fetchone()
        if protocol_row is None:
            raise EvaluationIntegrityError("Authoritative E.4 protocol registration disappeared.")
        members = connection.execute(
            "SELECT * FROM phase_e_evaluation_protocol_members WHERE protocol_id=? ORDER BY ordinal",
            (protocol_row["protocol_id"],),
        ).fetchall()
        proposals = connection.execute(
            "SELECT * FROM phase_e_hypothesis_proposals WHERE generation_run_id=? ORDER BY ordinal",
            (protocol["family"]["generation_run_id"],),
        ).fetchall()
        mappings = {
            row["proposal_id"]: row for row in connection.execute(
                "SELECT * FROM phase_e_generation_e1_mappings WHERE generation_run_id=?",
                (protocol["family"]["generation_run_id"],),
            ).fetchall()
        }
        if len(members) != len(proposals):
            raise EvaluationIntegrityError("E.4 member list differs from the authoritative E.3 family.")
        for member, proposal in zip(members, proposals):
            mapping = mappings.get(proposal["proposal_id"])
            if (
                mapping is None or int(member["ordinal"]) != int(proposal["ordinal"])
                or member["proposal_id"] != proposal["proposal_id"]
                or member["predicate_hash"] != proposal["predicate_hash"]
                or member["experiment_id"] != mapping["experiment_id"]
                or member["hypothesis_id"] != mapping["hypothesis_id"]
                or int(member["hypothesis_version"]) != int(mapping["hypothesis_version"])
                or member["definition_hash"] != mapping["definition_hash"]
                or member["e3_mapping_hash"] != mapping["mapping_hash"]
            ):
                raise EvaluationIntegrityError("E.4 member lineage differs from authoritative E.3/E.1 state.")

    def _load_validation_evidence(
        self, connection: sqlite3.Connection, protocol: Mapping[str, Any],
    ) -> tuple[list[_Observation], dict[str, Any]]:
        """Read exactly validation rows. There is intentionally no partition argument."""
        materialization_id = protocol["evidence_snapshot"]["materialization_id"]
        # Literal validation predicate is a structural holdout guard. No E.4
        # method accepts or interpolates a partition name.
        rows = connection.execute(
            """SELECT member.ordinal, member.observation_id, member.normalized_at, member.stratum_id,
                      anchor.wallet, anchor.symbol, anchor.source_event_id,
                      outcome.resolved_at, outcome.source_observation_id AS outcome_source_observation_id,
                      outcome.payload_json AS outcome_payload_json, outcome.missing AS outcome_missing,
                      outcome.missing_reason AS outcome_missing_reason, outcome.artifact_hash AS outcome_artifact_hash
               FROM phase_e_materialization_membership AS member
               JOIN science_observations AS anchor ON anchor.observation_id=member.observation_id
               JOIN phase_e_materialization_outcomes AS outcome
                 ON outcome.materialization_id=member.materialization_id AND outcome.observation_id=member.observation_id
               WHERE member.materialization_id=? AND member.partition_name='validation'
               ORDER BY member.ordinal""",
            (materialization_id,),
        ).fetchall()
        features = connection.execute(
            """SELECT feature.* FROM phase_e_materialization_features AS feature
               JOIN phase_e_materialization_membership AS member
                 ON member.materialization_id=feature.materialization_id AND member.observation_id=feature.observation_id
               WHERE feature.materialization_id=? AND member.partition_name='validation'
               ORDER BY feature.observation_id, feature.feature_id, feature.feature_version""",
            (materialization_id,),
        ).fetchall()
        design_row = connection.execute(
            "SELECT design_json, artifact_hash FROM phase_e_materialization_sampling_design WHERE materialization_id=?",
            (materialization_id,),
        ).fetchone()
        if design_row is None:
            raise EvaluationIntegrityError("E.4 evidence lacks the frozen E.2 sampling design.")
        design = _load(design_row["design_json"], "E.2 sampling design")
        weights: dict[str, float] = {}
        for item in design["strata"]:
            weight = item.get("sampling_weight")
            if not isinstance(weight, Mapping) or not weight.get("numerator") or not weight.get("denominator"):
                raise EvaluationIntegrityError("E.2 validation stratum lacks a positive sampling weight.")
            numeric = float(weight["numerator"]) / float(weight["denominator"])
            maximum = float(protocol["sample_rules"]["maximum_sampling_weight"])
            if not math.isfinite(numeric) or numeric <= 0 or numeric > maximum:
                raise EvaluationIntegrityError("E.2 sampling weight violates the preregistered numeric bound.")
            weights[str(item["stratum_id"])] = numeric
        by_observation: dict[str, dict[str, Any]] = {}
        all_feature_sources: set[str] = set()
        for feature in features:
            observation_id = str(feature["observation_id"])
            value = _load(feature["value_json"], "E.2 feature value")
            sources = _load(feature["source_observation_ids_json"], "E.2 feature sources")
            numeric = value.get("value") if isinstance(value, Mapping) else None
            if numeric is not None:
                numeric = finite_number(numeric, name="E.4 predictor value")
            key = f"{feature['feature_id']}@{int(feature['feature_version'])}"
            entry = by_observation.setdefault(observation_id, {"values": {}, "sources": set(), "hashes": []})
            entry["values"][key] = numeric
            entry["sources"].update(str(item) for item in sources)
            entry["hashes"].append(feature["artifact_hash"])
            all_feature_sources.update(str(item) for item in sources)
        source_times = {
            str(item["observation_id"]): normalized_utc(str(item["normalized_at"]))
            for item in self._rows_by_ids(connection, sorted(all_feature_sources))
        }
        partition = protocol["partitions"]["identity"]
        validation_start, validation_end = _instant(partition["validation_start"]), _instant(partition["validation_end"])
        horizon = int(protocol["outcome"]["horizon_seconds"])
        max_lag = int(protocol["outcome"]["resolution"]["maximum_lag_seconds"])
        evidence_cutoff = _instant(protocol["evidence_snapshot"]["evidence_cutoff"])
        observations: list[_Observation] = []
        evidence_rows: list[dict[str, Any]] = []
        for row in rows:
            observation_id = str(row["observation_id"])
            anchor_at = normalized_utc(str(row["normalized_at"]))
            anchor = _instant(anchor_at)
            if not validation_start <= anchor < validation_end or anchor + timedelta(seconds=horizon) >= validation_end:
                raise EvaluationIntegrityError("Validation member violates its preregistered horizon-contained partition.")
            feature = by_observation.get(observation_id, {"values": {}, "sources": set(), "hashes": []})
            future_sources = [item for item in feature["sources"] if item not in source_times or _instant(source_times[item]) > anchor]
            outcome_payload = _load(row["outcome_payload_json"], "E.2 outcome payload")
            outcome_state, outcome_reason, net_outcome = "RESOLVED", None, None
            if future_sources:
                outcome_state, outcome_reason = "INVALID", "FUTURE_FEATURE_SOURCE"
            elif bool(row["outcome_missing"]):
                reason = str(row["outcome_missing_reason"] or "OUTCOME_MISSING_REASON_UNAVAILABLE")
                mature = evidence_cutoff >= anchor + timedelta(seconds=horizon + max_lag)
                if reason == "OUTCOME_MARKET_EVIDENCE_NOT_WITHIN_TOLERANCE" and not mature:
                    outcome_state, outcome_reason = "UNRESOLVED", "OUTCOME_HORIZON_NOT_MATURE_AT_SNAPSHOT"
                elif reason == "OUTCOME_MARKET_EVIDENCE_NOT_WITHIN_TOLERANCE":
                    outcome_state, outcome_reason = "MATURE_MISSING", reason
                else:
                    outcome_state, outcome_reason = "INVALID", reason
            else:
                try:
                    net_outcome = finite_number(outcome_payload.get("net_outcome"), name="E.2 net outcome")
                    resolved_at = normalized_utc(str(row["resolved_at"]))
                    resolved = _instant(resolved_at)
                    target = anchor + timedelta(seconds=horizon)
                    if not target <= resolved <= target + timedelta(seconds=max_lag) or resolved >= validation_end:
                        outcome_state, outcome_reason, net_outcome = "INVALID", "OUTCOME_RESOLUTION_TIME_INVALID", None
                    elif abs(net_outcome) > float(protocol["outcome"]["maximum_absolute_outcome"]):
                        outcome_state, outcome_reason, net_outcome = "INVALID", "OUTCOME_EXCEEDS_PREREGISTERED_BOUND", None
                except (TypeError, ValueError):
                    outcome_state, outcome_reason, net_outcome = "INVALID", "OUTCOME_NONFINITE_OR_MALFORMED", None
            stratum = str(row["stratum_id"])
            if stratum not in weights:
                raise EvaluationIntegrityError("Validation member has no exact E.2 stratum sampling weight.")
            observation = _Observation(
                observation_id=observation_id, ordinal=int(row["ordinal"]), anchor_at=anchor_at,
                wallet=str(row["wallet"]) if row["wallet"] is not None else None,
                symbol=str(row["symbol"]) if row["symbol"] is not None else None,
                source_event_id=str(row["source_event_id"]), stratum_id=stratum, sampling_weight=weights[stratum],
                feature_values=dict(feature["values"]), feature_sources=tuple(sorted(feature["sources"])),
                outcome_state=outcome_state, outcome_reason=outcome_reason, net_outcome=net_outcome,
                outcome_source_id=str(row["outcome_source_observation_id"]) if row["outcome_source_observation_id"] else None,
                resolved_at=normalized_utc(str(row["resolved_at"])) if row["resolved_at"] else None,
            )
            observations.append(observation)
            evidence_rows.append({
                "ordinal": observation.ordinal, "observation_id": observation_id,
                "anchor_at": anchor_at, "stratum_id": stratum,
                "feature_artifact_hashes": sorted(feature["hashes"]),
                "outcome_artifact_hash": row["outcome_artifact_hash"], "outcome_state": outcome_state,
            })
        expected_validation = int(design["by_partition"]["validation"]["selected_count"])
        if len(observations) != expected_validation:
            # Exact required feature coverage is checked per proposal below
            # because future families may use more than one feature.
            raise EvaluationIntegrityError("E.4 validation evidence count disagrees with the frozen sampling design.")
        cross_partition = connection.execute(
            """SELECT anchor.source_event_id
               FROM phase_e_materialization_membership AS member
               JOIN science_observations AS anchor ON anchor.observation_id=member.observation_id
               WHERE member.materialization_id=?
               GROUP BY anchor.source, anchor.source_event_id
               HAVING COUNT(DISTINCT member.partition_name) > 1 LIMIT 1""",
            (materialization_id,),
        ).fetchone()
        if cross_partition is not None:
            raise EvaluationIntegrityError("One causal source event appears in multiple temporal partitions.")
        identity = {
            "schema": "phase-e4-validation-evidence-snapshot-v1", "materialization_id": materialization_id,
            "partition": "validation", "rows": evidence_rows, "sampling_design_artifact_hash": design_row["artifact_hash"],
            "test_partition_queries": 0,
        }
        return observations, identity

    @staticmethod
    def _rows_by_ids(connection: sqlite3.Connection, identifiers: Sequence[str]) -> list[sqlite3.Row]:
        rows: list[sqlite3.Row] = []
        for start in range(0, len(identifiers), 400):
            part = identifiers[start:start + 400]
            if part:
                rows.extend(connection.execute(
                    "SELECT observation_id, normalized_at FROM science_observations WHERE observation_id IN ("
                    + ",".join("?" for _ in part) + ")", part,
                ).fetchall())
        return rows

    def _compute_results(
        self, protocol_id: str, protocol: Mapping[str, Any], members: Sequence[sqlite3.Row],
        proposals: Sequence[sqlite3.Row], observations: Sequence[_Observation],
    ) -> list[dict[str, Any]]:
        if len(members) != len(proposals) or len(members) != int(protocol["family"]["family_size"]):
            raise EvaluationIntegrityError("Correction family differs from the exact preregistered E.3 universe.")
        component_by_observation, component_count = self._dependence_components(observations, int(protocol["outcome"]["horizon_seconds"]))
        mapping = {row["proposal_id"]: row for row in members}
        drafts: list[dict[str, Any]] = []
        for proposal in proposals:
            member = mapping.get(proposal["proposal_id"])
            if member is None or int(member["ordinal"]) != int(proposal["ordinal"]):
                raise EvaluationIntegrityError("Protocol membership no longer covers exact E.3 proposal order.")
            predicate = Predicate.from_payload(_load(proposal["predicate_json"], "E.3 predicate"))
            group: list[_Observation] = []
            comparator: list[_Observation] = []
            predictor_missing = 0
            for observation in observations:
                matched = self._predicate_match(predicate, observation.feature_values)
                if matched is None:
                    predictor_missing += 1
                elif matched:
                    group.append(observation)
                else:
                    comparator.append(observation)
            categories = self._resolution_counts(observations)
            group_counts = self._resolution_counts(group)
            comparator_counts = self._resolution_counts(comparator)
            resolved_group = [item for item in group if item.outcome_state == "RESOLVED"]
            resolved_comparator = [item for item in comparator if item.outcome_state == "RESOLVED"]
            group_components = {component_by_observation[item.observation_id] for item in resolved_group if item.observation_id in component_by_observation}
            comparator_components = {component_by_observation[item.observation_id] for item in resolved_comparator if item.observation_id in component_by_observation}
            status: HypothesisEvaluationStatus
            reasons: list[str] = []
            effect: float | None = None
            raw_p: float | None = None
            ci_low: float | None = None
            ci_high: float | None = None
            statistic: dict[str, Any] = {
                "method": E4_STATISTICAL_METHOD, "component_policy": E4_CLUSTER_POLICY,
                "independent_component_count": component_count,
                "predicate_component_count": len(group_components),
                "comparator_component_count": len(comparator_components),
            }
            if predictor_missing:
                status, reasons = HypothesisEvaluationStatus.INVALID_EVIDENCE, ["VALIDATION_PREDICTOR_MISSING"]
            elif categories["unresolved"]:
                status, reasons = HypothesisEvaluationStatus.PENDING_OUTCOME_MATURITY, ["OUTCOME_HORIZON_NOT_MATURE"]
            elif categories["invalid"] or any(item.wallet is None for item in observations):
                status = HypothesisEvaluationStatus.INVALID_EVIDENCE
                reasons = sorted({"INVALID_OR_UNCLUSTERABLE_EVIDENCE", *(
                    item.outcome_reason for item in observations if item.outcome_state == "INVALID" and item.outcome_reason
                )})
            elif categories["mature_missing"]:
                status, reasons = HypothesisEvaluationStatus.INCONCLUSIVE_MISSING_EVIDENCE, ["MATURE_OUTCOME_EVIDENCE_MISSING"]
            else:
                if resolved_group and resolved_comparator:
                    effect = self._effect(resolved_group, resolved_comparator)
                minimum = int(protocol["sample_rules"]["minimum_observations_per_arm"])
                min_components = int(protocol["sample_rules"]["minimum_independent_components"])
                min_arm_components = int(protocol["sample_rules"]["minimum_components_per_arm"])
                if (
                    len(resolved_group) < minimum or len(resolved_comparator) < minimum
                    or component_count < min_components
                    or len(group_components) < min_arm_components
                    or len(comparator_components) < min_arm_components
                ):
                    status, reasons = HypothesisEvaluationStatus.INSUFFICIENT_SUPPORT, ["MINIMUM_SUPPORT_OR_INDEPENDENCE_NOT_MET"]
                else:
                    status = HypothesisEvaluationStatus.EVALUATED
                    assert effect is not None
                    raw_p, ci_low, ci_high, bootstrap = self._bootstrap(
                        protocol_id, str(proposal["proposal_id"]), protocol, observations,
                        component_by_observation, predicate,
                    )
                    statistic.update(bootstrap)
                    if raw_p is None:
                        status = HypothesisEvaluationStatus.INSUFFICIENT_SUPPORT
                        reasons = ["INSUFFICIENT_VALID_WHOLE_COMPONENT_RESAMPLES"]
            seed = self._derived_seed(protocol_id, str(proposal["proposal_id"]), int(protocol["uncertainty"]["base_seed"]))
            statistic["derived_seed"] = seed
            counts = {
                "eligible_observations": len(observations), "predicate_observations": len(group),
                "comparator_observations": len(comparator), "predictor_missing": predictor_missing,
                "resolved_positive": categories["positive"], "resolved_negative": categories["negative"],
                "resolved_total": categories["resolved"], "unresolved_not_mature": categories["unresolved"],
                "permanently_missing": categories["mature_missing"], "invalid": categories["invalid"],
                "excluded_by_preregistered_rule": 0,
                "predicate_resolution": group_counts, "comparator_resolution": comparator_counts,
            }
            drafts.append({
                "schema": E4_RESULT_SCHEMA, "protocol_id": protocol_id,
                "protocol_version": protocol["protocol_version"],
                "generation_run_id": protocol["family"]["generation_run_id"],
                "family_id": protocol["family"]["family_id"],
                "family_version": protocol["family"]["family_version"],
                "multiple_testing_family_id": protocol["family"]["multiple_testing_family_id"],
                "materialization_id": protocol["evidence_snapshot"]["materialization_id"],
                "partition": "validation", "proposal_id": proposal["proposal_id"], "ordinal": int(proposal["ordinal"]),
                "hypothesis_id": member["hypothesis_id"], "hypothesis_version": int(member["hypothesis_version"]),
                "experiment_id": member["experiment_id"], "definition_hash": member["definition_hash"],
                "predicate_hash": proposal["predicate_hash"], "threshold_provenance": _load(
                    proposal["threshold_provenance_json"], "E.3 threshold provenance",
                ),
                "lineage": {
                    "d_source": protocol["evidence_snapshot"]["eligible_source"],
                    "d_corpus_fingerprint": protocol["evidence_snapshot"]["corpus_fingerprint"],
                    "e1_experiment_id": member["experiment_id"], "e1_definition_hash": member["definition_hash"],
                    "e2_materialization_id": protocol["evidence_snapshot"]["materialization_id"],
                    "e2_completed_artifact_fingerprint": protocol["evidence_snapshot"]["completed_artifact_fingerprint"],
                    "e3_generation_run_id": protocol["family"]["generation_run_id"],
                    "e3_proposal_id": proposal["proposal_id"],
                    "e3_universe_fingerprint": protocol["family"]["hypothesis_universe_fingerprint"],
                    "e4_protocol_id": protocol_id,
                },
                "temporal_provenance": {
                    "partition_start": protocol["partitions"]["identity"]["validation_start"],
                    "partition_end": protocol["partitions"]["identity"]["validation_end"],
                    "evidence_cutoff": protocol["evidence_snapshot"]["evidence_cutoff"],
                },
                "evaluation_status": status.value, "support": counts,
                "effect_metric": protocol["effect"]["metric"], "effect_estimate": effect,
                "uncertainty": {"confidence_interval_low": ci_low, "confidence_interval_high": ci_high,
                                "confidence_level": protocol["uncertainty"]["confidence_level"]},
                "statistic": statistic, "raw_p_value": raw_p,
                "adjusted_p_value": None, "correction_method": E4_CORRECTION_METHOD,
                "correction_family_size": len(members), "correction_rank": None,
                "statistical_decision": "PENDING_CORRECTION" if status is HypothesisEvaluationStatus.EVALUATED else "NOT_EVALUATED",
                "practical_relevance_decision": "PENDING" if status is HypothesisEvaluationStatus.EVALUATED else "NOT_EVALUATED",
                "scientific_decision": "PENDING_CORRECTION" if status is HypothesisEvaluationStatus.EVALUATED else "INCONCLUSIVE",
                "downstream_eligibility": "NOT_ELIGIBLE", "reason_codes": reasons,
                "authority": {"prediction": False, "signal": False, "execution": False, "trading": False},
            })
        return self._apply_holm(protocol, drafts)

    @staticmethod
    def _predicate_match(predicate: Predicate, values: Mapping[str, float | None]) -> bool | None:
        if predicate.operator is PredicateOperator.AND:
            matched = [PhaseEEvaluator._predicate_match(child, values) for child in predicate.children]
            return None if any(item is None for item in matched) else all(bool(item) for item in matched)
        if predicate.feature is None:
            raise EvaluationIntegrityError("Atomic E.3 predicate lacks its feature.")
        key = f"{predicate.feature.feature_id}@{predicate.feature.version}"
        value = values.get(key)
        if value is None:
            return None
        lower = float(predicate.threshold)
        if predicate.operator is PredicateOperator.GT:
            return value > lower
        if predicate.operator is PredicateOperator.GE:
            return value >= lower
        if predicate.operator is PredicateOperator.LT:
            return value < lower
        if predicate.operator is PredicateOperator.LE:
            return value <= lower
        if predicate.operator is PredicateOperator.EQ:
            return value == lower
        if predicate.operator is PredicateOperator.BETWEEN:
            return lower <= value <= float(predicate.upper_threshold)
        raise EvaluationIntegrityError("Unsupported E.3 predicate operator reached E.4.")

    @staticmethod
    def _resolution_counts(rows: Sequence[_Observation]) -> dict[str, int]:
        return {
            "resolved": sum(item.outcome_state == "RESOLVED" for item in rows),
            "positive": sum(item.outcome_state == "RESOLVED" and float(item.net_outcome) > 0 for item in rows),
            "negative": sum(item.outcome_state == "RESOLVED" and float(item.net_outcome) <= 0 for item in rows),
            "unresolved": sum(item.outcome_state == "UNRESOLVED" for item in rows),
            "mature_missing": sum(item.outcome_state == "MATURE_MISSING" for item in rows),
            "invalid": sum(item.outcome_state == "INVALID" for item in rows),
        }

    def _dependence_components(self, observations: Sequence[_Observation], horizon: int) -> tuple[dict[str, int], int]:
        resolved = [item for item in observations if item.outcome_state == "RESOLVED"]
        disjoint = _DisjointSet(len(resolved))
        by_wallet: dict[str, int] = {}
        by_source: dict[str, int] = {}
        for index, observation in enumerate(resolved):
            if observation.wallet:
                if observation.wallet in by_wallet:
                    disjoint.union(index, by_wallet[observation.wallet])
                else:
                    by_wallet[observation.wallet] = index
            sources = {
                f"causal-observation:{observation.observation_id}",
                f"source-event:{observation.source_event_id}",
                *(
                    [f"causal-observation:{observation.outcome_source_id}"]
                    if observation.outcome_source_id else []
                ),
                *(f"causal-observation:{source}" for source in observation.feature_sources),
            }
            for key in sources:
                if key in by_source:
                    disjoint.union(index, by_source[key])
                else:
                    by_source[key] = index
        by_symbol: dict[str, list[tuple[datetime, datetime, int]]] = {}
        for index, observation in enumerate(resolved):
            if observation.symbol:
                start = _instant(observation.anchor_at)
                by_symbol.setdefault(observation.symbol, []).append((start, start + timedelta(seconds=horizon), index))
        for intervals in by_symbol.values():
            intervals.sort(key=lambda item: (item[0], item[2]))
            active_end: datetime | None = None
            active_index: int | None = None
            for start, end, index in intervals:
                if active_end is not None and active_index is not None and start <= active_end:
                    disjoint.union(index, active_index)
                    if end > active_end:
                        active_end = end
                        active_index = index
                else:
                    active_end, active_index = end, index
        roots = sorted({disjoint.find(index) for index in range(len(resolved))})
        root_order = {root: ordinal for ordinal, root in enumerate(roots)}
        return {item.observation_id: root_order[disjoint.find(index)] for index, item in enumerate(resolved)}, len(roots)

    @staticmethod
    def _weighted_mean(rows: Sequence[_Observation]) -> float:
        maximum = max(item.sampling_weight for item in rows)
        scaled = [item.sampling_weight / maximum for item in rows]
        denominator = math.fsum(scaled)
        numerator = math.fsum(weight * float(item.net_outcome) for weight, item in zip(scaled, rows))
        result = numerator / denominator
        if not math.isfinite(result):
            raise EvaluationIntegrityError("Weighted effect calculation produced a non-finite value.")
        return result

    @classmethod
    def _effect(cls, group: Sequence[_Observation], comparator: Sequence[_Observation]) -> float:
        return cls._weighted_mean(group) - cls._weighted_mean(comparator)

    @staticmethod
    def _derived_seed(protocol_id: str, proposal_id: str, base_seed: int) -> int:
        return int(canonical_hash({"protocol_id": protocol_id, "proposal_id": proposal_id, "base_seed": base_seed})[:16], 16)

    def _bootstrap(
        self, protocol_id: str, proposal_id: str, protocol: Mapping[str, Any], observations: Sequence[_Observation],
        component_by_observation: Mapping[str, int], predicate: Predicate,
    ) -> tuple[float | None, float | None, float | None, dict[str, Any]]:
        components: dict[int, list[_Observation]] = {}
        for item in observations:
            if item.outcome_state == "RESOLVED":
                components.setdefault(component_by_observation[item.observation_id], []).append(item)
        ordered = [components[key] for key in sorted(components)]
        group = [item for item in observations if item.outcome_state == "RESOLVED" and self._predicate_match(predicate, item.feature_values)]
        comparator = [item for item in observations if item.outcome_state == "RESOLVED" and self._predicate_match(predicate, item.feature_values) is False]
        observed = self._effect(group, comparator)
        iterations = int(protocol["uncertainty"]["resample_count"])
        rng = random.Random(self._derived_seed(protocol_id, proposal_id, int(protocol["uncertainty"]["base_seed"])))
        effects: list[float] = []
        for _ in range(iterations):
            sample = [item for _index in range(len(ordered)) for item in ordered[rng.randrange(len(ordered))]]
            sample_group = [item for item in sample if self._predicate_match(predicate, item.feature_values)]
            sample_comparator = [item for item in sample if self._predicate_match(predicate, item.feature_values) is False]
            if sample_group and sample_comparator:
                effects.append(self._effect(sample_group, sample_comparator))
        minimum_valid = math.ceil(iterations * float(protocol["uncertainty"]["minimum_valid_resample_fraction"]))
        detail = {
            "requested_resamples": iterations, "valid_resamples": len(effects),
            "minimum_valid_resamples": minimum_valid,
            "bootstrap_effects_fingerprint": canonical_hash(effects),
        }
        if len(effects) < minimum_valid:
            return None, None, None, detail
        effects.sort()
        alpha = float(protocol["multiple_testing"]["family_alpha"])
        low = self._nearest_rank(effects, alpha / 2.0)
        high = self._nearest_rank(effects, 1.0 - alpha / 2.0)
        extreme = sum(abs(value - observed) >= abs(observed) for value in effects)
        raw_p = (extreme + 1.0) / (len(effects) + 1.0)
        return raw_p, low, high, detail

    @staticmethod
    def _nearest_rank(values: Sequence[float], probability: float) -> float:
        index = max(0, min(len(values) - 1, math.ceil(probability * len(values)) - 1))
        return float(values[index])

    @staticmethod
    def _apply_holm(protocol: Mapping[str, Any], drafts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ordered = sorted(
            drafts,
            key=lambda item: (
                float(item["raw_p_value"]) if item["raw_p_value"] is not None else 1.0,
                int(item["ordinal"]), str(item["proposal_id"]),
            ),
        )
        running = 0.0
        count = len(ordered)
        for rank, item in enumerate(ordered, start=1):
            p_value = float(item["raw_p_value"]) if item["raw_p_value"] is not None else 1.0
            running = max(running, min(1.0, (count - rank + 1) * p_value))
            item["adjusted_p_value"] = min(1.0, running)
            item["correction_rank"] = rank
        alpha = float(protocol["multiple_testing"]["family_alpha"])
        practical_minimum = float(protocol["effect"]["minimum_practical_effect"])
        for item in drafts:
            if item["evaluation_status"] == HypothesisEvaluationStatus.EVALUATED.value:
                statistical = item["adjusted_p_value"] <= alpha
                practical = abs(float(item["effect_estimate"])) >= practical_minimum
                item["statistical_decision"] = "STATISTICALLY_SUPPORTED" if statistical else "NULL_NOT_REJECTED"
                item["practical_relevance_decision"] = "PRACTICALLY_RELEVANT" if practical else "BELOW_PRACTICAL_THRESHOLD"
                item["scientific_decision"] = "SCIENTIFICALLY_SUPPORTED" if statistical and practical else "NOT_SCIENTIFICALLY_SUPPORTED"
                if statistical and practical:
                    item["downstream_eligibility"] = "SCIENTIFIC_REVIEW_ONLY"
                if not statistical:
                    item["reason_codes"].append("HOLM_ADJUSTED_THRESHOLD_NOT_MET")
                if not practical:
                    item["reason_codes"].append("PRACTICAL_EFFECT_THRESHOLD_NOT_MET")
            item["reason_codes"] = sorted(set(item["reason_codes"]))
        return sorted(drafts, key=lambda item: int(item["ordinal"]))

    # ----- read, replay, and reconciliation --------------------------------------
    def get_protocol(self, protocol_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = self._require_protocol(connection, protocol_id)
            self._validate_protocol(connection, row)
            return self._protocol_payload(connection, row)

    def list_protocols(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute("SELECT * FROM phase_e_evaluation_protocols ORDER BY registered_at, protocol_id").fetchall()
            for row in rows:
                self._validate_protocol(connection, row)
            return [self._protocol_payload(connection, row) for row in rows]

    def get_run(self, evaluation_run_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = self._require_run(connection, evaluation_run_id)
            self._validate_run(connection, row)
            return self._run_payload(connection, row)

    def results(self, protocol_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            protocol = self._require_protocol(connection, protocol_id)
            self._validate_protocol(connection, protocol)
            run = connection.execute("SELECT * FROM phase_e_evaluation_runs WHERE protocol_id=?", (protocol_id,)).fetchone()
            if run is None:
                return []
            self._validate_run(connection, run)
            return [
                _load(row["result_json"], "E.4 result") for row in connection.execute(
                    "SELECT * FROM phase_e_hypothesis_evaluations WHERE evaluation_run_id=? ORDER BY ordinal",
                    (run["evaluation_run_id"],),
                ).fetchall()
            ]

    def verify(self, evaluation_run_id: str) -> dict[str, Any]:
        """Recompute from authoritative inputs and compare every decision/hash."""
        self.initialize()
        with self._connection() as connection:
            run = self._require_run(connection, evaluation_run_id)
            protocol_row = self._require_protocol(connection, run["protocol_id"])
            generation_run_id = _load(protocol_row["protocol_json"], "E.4 protocol")["family"]["generation_run_id"]
        self._authoritative_inputs(generation_run_id)
        with self._write() as connection:
            run = self._require_run(connection, evaluation_run_id)
            self._validate_run(connection, run)
            protocol_row = self._require_protocol(connection, run["protocol_id"])
            self._validate_protocol(connection, protocol_row)
            protocol = _load(protocol_row["protocol_json"], "E.4 protocol")
            self._validate_upstream_locked(connection, protocol)
            observations, evidence_identity = self._load_validation_evidence(connection, protocol)
            if canonical_hash(evidence_identity) != run["evidence_snapshot_hash"]:
                raise EvaluationIntegrityError("E.4 replay evidence snapshot differs from the persisted run.")
            members = connection.execute(
                "SELECT * FROM phase_e_evaluation_protocol_members WHERE protocol_id=? ORDER BY ordinal", (run["protocol_id"],),
            ).fetchall()
            recomputed = self._compute_results(
                run["protocol_id"], protocol, members, self._current_proposals(connection, protocol), observations,
            )
            persisted = connection.execute(
                "SELECT * FROM phase_e_hypothesis_evaluations WHERE evaluation_run_id=? ORDER BY ordinal", (evaluation_run_id,),
            ).fetchall()
            if len(recomputed) != len(persisted):
                raise EvaluationIntegrityError("E.4 replay result family size changed.")
            for expected, actual in zip(recomputed, persisted):
                if storage_json(expected) != actual["result_json"] or canonical_hash(expected) != actual["result_hash"]:
                    raise EvaluationIntegrityError("E.4 replay produced a different hypothesis decision.")
            return {
                "evaluation_run_id": evaluation_run_id, "verified": True, "reproducible": True,
                "protocol_hash": run["protocol_hash"], "evidence_snapshot_hash": run["evidence_snapshot_hash"],
                "manifest_hash": connection.execute(
                    "SELECT manifest_hash FROM phase_e_evaluation_manifests WHERE evaluation_run_id=?", (evaluation_run_id,),
                ).fetchone()[0],
                "correction_family_size": len(recomputed), "test_partition_queries": 0,
                "prediction_authority": False, "signal_authority": False, "trading_authority": False,
            }

    def reproduce(self, evaluation_run_id: str) -> dict[str, Any]:
        result = self.verify(evaluation_run_id)
        result["mode"] = "READ_ONLY_DETERMINISTIC_REPLAY"
        return result

    def _validate_protocol(self, connection: sqlite3.Connection, row: sqlite3.Row) -> None:
        try:
            protocol = _load(row["protocol_json"], "E.4 protocol")
            if (
                protocol.get("schema") != E4_PROTOCOL_SCHEMA or canonical_hash(protocol) != row["protocol_hash"]
                or row["protocol_id"] != "e4p-" + row["protocol_hash"][:32]
                or protocol["family"]["generation_run_id"] != row["generation_run_id"]
                or protocol["family"]["family_id"] != row["family_id"]
                or int(protocol["family"]["family_version"]) != int(row["family_version"])
                or protocol["evidence_snapshot"]["materialization_id"] != row["materialization_id"]
                or int(protocol["family"]["family_size"]) != int(row["family_size"])
                or int(protocol["protocol_version"]) != int(row["protocol_version"])
                or row["status"] != "REGISTERED" or normalized_utc(row["registered_at"]) != row["registered_at"]
                or protocol["partitions"]["evaluation_partition"] != "validation"
                or protocol["partitions"]["maximum_test_queries"] != 0
                or protocol["partitions"]["holdout_policy"] != E4_HOLDOUT_POLICY
                or protocol["evaluator"] != {
                    "code_version": E4_EVALUATOR_CODE_VERSION, "config_version": E4_EVALUATOR_CONFIG_VERSION,
                }
                or protocol["test"]["method"] != E4_STATISTICAL_METHOD
                or protocol["test"]["direction"] != "TWO_SIDED"
                or protocol["uncertainty"]["component_policy"] != E4_CLUSTER_POLICY
                or protocol["multiple_testing"]["method"] != E4_CORRECTION_METHOD
                or protocol["multiple_testing"]["denominator"] != int(row["family_size"])
                or protocol["multiple_testing"]["unevaluable_correction_input"] != 1.0
                or protocol["outcome"]["unresolved_policy"] != "PENDING_NEVER_NEGATIVE_V1"
                or any(protocol["authority"].values())
            ):
                raise EvaluationIntegrityError("E.4 protocol projection conflicts with its immutable contract.")
            members = connection.execute(
                "SELECT * FROM phase_e_evaluation_protocol_members WHERE protocol_id=? ORDER BY ordinal", (row["protocol_id"],),
            ).fetchall()
            if len(members) != int(row["family_size"]):
                raise EvaluationIntegrityError("E.4 protocol does not retain the exact family denominator.")
            for ordinal, member in enumerate(members):
                identity = {key: member[key] for key in (
                    "protocol_id", "ordinal", "proposal_id", "predicate_hash", "experiment_id", "hypothesis_id",
                    "hypothesis_version", "definition_hash", "e3_mapping_hash",
                )}
                if int(member["ordinal"]) != ordinal or canonical_hash(identity) != member["member_hash"]:
                    raise EvaluationIntegrityError("E.4 protocol member order/hash is inconsistent.")
            events = connection.execute(
                "SELECT * FROM phase_e_evaluation_protocol_events WHERE protocol_id=? ORDER BY event_type", (row["protocol_id"],),
            ).fetchall()
            if {item["event_type"] for item in events} != {"PREREGISTERED", "FAMILY_FROZEN"}:
                raise EvaluationIntegrityError("E.4 protocol lifecycle is incomplete or duplicated.")
            parsed_events: dict[str, Mapping[str, Any]] = {}
            for event in events:
                payload = _load(event["payload_json"], "E.4 protocol event")
                identity = {"protocol_id": row["protocol_id"], "event_type": event["event_type"],
                            "event_at": event["event_at"], "payload_hash": event["payload_hash"]}
                if canonical_hash(payload) != event["payload_hash"] or canonical_hash(identity) != event["event_id"]:
                    raise EvaluationIntegrityError("E.4 protocol event hash is inconsistent.")
                parsed_events[event["event_type"]] = payload
            preregistered = parsed_events["PREREGISTERED"]
            frozen = parsed_events["FAMILY_FROZEN"]
            if (
                preregistered != {
                    "protocol_hash": row["protocol_hash"], "generation_run_id": row["generation_run_id"],
                    "hypothesis_universe_fingerprint": protocol["family"]["hypothesis_universe_fingerprint"],
                    "outcome_values_exposed_to_protocol_builder": False,
                    "evaluation_partition": "validation", "holdout_access": "FORBIDDEN",
                }
                or frozen != {
                    "family_size": int(row["family_size"]),
                    "member_list_hash": self._member_list_hash(connection, row["protocol_id"]),
                    "correction_denominator_policy": "EXACT_E3_UNIVERSE_UNEVALUABLE_AS_ONE_V1",
                }
            ):
                raise EvaluationIntegrityError("E.4 protocol events do not prove pre-outcome family freeze.")
        except (KeyError, TypeError, ValueError, sqlite3.Error) as exc:
            if isinstance(exc, EvaluationIntegrityError):
                raise
            raise EvaluationIntegrityError(f"Malformed E.4 protocol state: {exc}") from exc

    def _validate_run(self, connection: sqlite3.Connection, row: sqlite3.Row) -> None:
        if row["status"] == EvaluationRunStatus.EVALUATING.value or row["completed_at"] is None:
            raise EvaluationIntegrityError("E.4 exposes no partially finalized evaluation run.")
        protocol = self._require_protocol(connection, row["protocol_id"])
        self._validate_protocol(connection, protocol)
        expected_run_id = "e4r-" + canonical_hash({
            "schema": "phase-e4-evaluation-run-identity-v1", "protocol_id": row["protocol_id"],
            "protocol_hash": row["protocol_hash"], "evidence_snapshot_hash": row["evidence_snapshot_hash"],
            "evaluator_code_version": E4_EVALUATOR_CODE_VERSION,
        })[:32]
        if (
            row["evaluation_run_id"] != expected_run_id
            or row["protocol_hash"] != protocol["protocol_hash"]
            or row["materialization_id"] != protocol["materialization_id"]
            or int(row["correction_family_size"]) != int(protocol["family_size"])
            or normalized_utc(row["started_at"]) != row["started_at"]
            or normalized_utc(row["completed_at"]) != row["completed_at"]
            or _instant(row["completed_at"]) < _instant(row["started_at"])
        ):
            raise EvaluationIntegrityError("E.4 run projection conflicts with its protocol family.")
        results = connection.execute(
            "SELECT * FROM phase_e_hypothesis_evaluations WHERE evaluation_run_id=? ORDER BY ordinal", (row["evaluation_run_id"],),
        ).fetchall()
        if len(results) != int(row["correction_family_size"]):
            raise EvaluationIntegrityError("E.4 finalized run does not retain every correction-family member.")
        for ordinal, result in enumerate(results):
            payload = _load(result["result_json"], "E.4 result")
            if (
                int(result["ordinal"]) != ordinal or int(payload["ordinal"]) != ordinal
                or result["proposal_id"] != payload["proposal_id"]
                or result["evaluation_status"] != payload["evaluation_status"]
                or result["raw_p_value"] != payload["raw_p_value"]
                or result["adjusted_p_value"] != payload["adjusted_p_value"]
                or int(result["correction_rank"]) != int(payload["correction_rank"])
                or canonical_hash(payload) != result["result_hash"]
                or any(payload["authority"].values())
            ):
                raise EvaluationIntegrityError("E.4 persisted result projection/hash is inconsistent.")
        manifest_row = connection.execute(
            "SELECT * FROM phase_e_evaluation_manifests WHERE evaluation_run_id=?", (row["evaluation_run_id"],),
        ).fetchone()
        if manifest_row is None:
            raise EvaluationIntegrityError("E.4 finalized run lacks its immutable manifest.")
        manifest = _load(manifest_row["manifest_json"], "E.4 manifest")
        result_payloads = [_load(item["result_json"], "E.4 result") for item in results]
        expected_result_hashes = [
            {"ordinal": int(item["ordinal"]), "proposal_id": item["proposal_id"], "result_hash": item["result_hash"]}
            for item in results
        ]
        expected_ranks = [
            {"proposal_id": item["proposal_id"], "rank": int(item["correction_rank"]),
             "adjusted_p_value": item["adjusted_p_value"]}
            for item in sorted(results, key=lambda value: int(value["correction_rank"]))
        ]
        if (
            canonical_hash(manifest) != manifest_row["manifest_hash"]
            or manifest["protocol_hash"] != row["protocol_hash"]
            or manifest["evidence_snapshot"]["evaluation_snapshot_hash"] != row["evidence_snapshot_hash"]
            or manifest["correction"]["family_size"] != int(row["correction_family_size"])
            or manifest["test_partition_queries"] != 0 or any(manifest["authority"].values())
            or manifest["result_hashes"] != expected_result_hashes
            or manifest["correction"]["ordered_ranks"] != expected_ranks
            or manifest["correction"]["member_list_hash"] != self._member_list_hash(connection, row["protocol_id"])
            or manifest["status_counts"] != self._status_counts(result_payloads)
            or manifest["completion_state"] != row["status"]
        ):
            raise EvaluationIntegrityError("E.4 manifest cannot be reconciled.")
        events = connection.execute(
            "SELECT * FROM phase_e_evaluation_events WHERE evaluation_run_id=? ORDER BY rowid", (row["evaluation_run_id"],),
        ).fetchall()
        if len(events) != 2 or events[0]["to_status"] != "EVALUATING" or events[1]["to_status"] != row["status"]:
            raise EvaluationIntegrityError("E.4 run lifecycle is incomplete or forged.")
        if (
            events[0]["from_status"] is not None or events[0]["event_at"] != row["started_at"]
            or events[1]["from_status"] != "EVALUATING" or events[1]["event_at"] != row["completed_at"]
        ):
            raise EvaluationIntegrityError("E.4 run lifecycle timing/sequence is forged.")
        for event in events:
            payload = _load(event["payload_json"], "E.4 run event")
            identity = {
                "evaluation_run_id": row["evaluation_run_id"], "event_type": event["event_type"],
                "from_status": event["from_status"], "to_status": event["to_status"], "reason": event["reason"],
                "event_at": event["event_at"], "payload_hash": event["payload_hash"],
            }
            if canonical_hash(payload) != event["payload_hash"] or canonical_hash(identity) != event["event_id"]:
                raise EvaluationIntegrityError("E.4 run lifecycle event hash is inconsistent.")

    def _current_proposals(self, connection: sqlite3.Connection, protocol: Mapping[str, Any]) -> list[sqlite3.Row]:
        rows = connection.execute(
            "SELECT * FROM phase_e_hypothesis_proposals WHERE generation_run_id=? ORDER BY ordinal",
            (protocol["family"]["generation_run_id"],),
        ).fetchall()
        if len(rows) != int(protocol["family"]["family_size"]):
            raise EvaluationIntegrityError("Current E.3 proposal count differs from the E.4 protocol denominator.")
        return rows

    @staticmethod
    def _final_status(results: Sequence[Mapping[str, Any]]) -> EvaluationRunStatus:
        statuses = {item["evaluation_status"] for item in results}
        if HypothesisEvaluationStatus.PENDING_OUTCOME_MATURITY.value in statuses:
            return EvaluationRunStatus.PENDING
        if HypothesisEvaluationStatus.INVALID_EVIDENCE.value in statuses:
            return EvaluationRunStatus.INVALID
        return EvaluationRunStatus.COMPLETE

    @staticmethod
    def _status_counts(results: Sequence[Mapping[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in results:
            key = str(item["evaluation_status"])
            counts[key] = counts.get(key, 0) + 1
        return {key: counts[key] for key in sorted(counts)}

    def _protocol_payload(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        run = connection.execute("SELECT evaluation_run_id, status FROM phase_e_evaluation_runs WHERE protocol_id=?", (row["protocol_id"],)).fetchone()
        return {
            "protocol_id": row["protocol_id"], "generation_run_id": row["generation_run_id"],
            "protocol": _load(row["protocol_json"], "E.4 protocol"), "protocol_hash": row["protocol_hash"],
            "family_size": row["family_size"], "status": row["status"], "registered_at": row["registered_at"],
            "member_list_hash": self._member_list_hash(connection, row["protocol_id"]),
            "evaluation_run_id": run["evaluation_run_id"] if run else None,
            "evaluation_status": run["status"] if run else "EVALUATION_PENDING",
            "test_partition_queries": 0, "prediction_authority": False, "signal_authority": False,
            "trading_authority": False,
        }

    def _run_payload(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        results = connection.execute(
            "SELECT result_json FROM phase_e_hypothesis_evaluations WHERE evaluation_run_id=? ORDER BY ordinal",
            (row["evaluation_run_id"],),
        ).fetchall()
        manifest = connection.execute(
            "SELECT manifest_json, manifest_hash FROM phase_e_evaluation_manifests WHERE evaluation_run_id=?",
            (row["evaluation_run_id"],),
        ).fetchone()
        return {
            "evaluation_run_id": row["evaluation_run_id"], "protocol_id": row["protocol_id"],
            "protocol_hash": row["protocol_hash"], "materialization_id": row["materialization_id"],
            "evidence_snapshot_hash": row["evidence_snapshot_hash"],
            "correction_family_size": row["correction_family_size"], "status": row["status"],
            "started_at": row["started_at"], "completed_at": row["completed_at"],
            "results": [_load(item["result_json"], "E.4 result") for item in results],
            "manifest": _load(manifest["manifest_json"], "E.4 manifest") if manifest else None,
            "manifest_hash": manifest["manifest_hash"] if manifest else None,
            "test_partition_queries": 0, "prediction_authority": False, "signal_authority": False,
            "execution_authority": False, "trading_authority": False,
        }

    @staticmethod
    def _member_list_hash(connection: sqlite3.Connection, protocol_id: str) -> str:
        rows = connection.execute(
            "SELECT ordinal, proposal_id, member_hash FROM phase_e_evaluation_protocol_members WHERE protocol_id=? ORDER BY ordinal",
            (protocol_id,),
        ).fetchall()
        return canonical_hash([dict(item) for item in rows])

    @staticmethod
    def _append_protocol_event(
        connection: sqlite3.Connection, protocol_id: str, event_type: str, event_at: str, payload: Mapping[str, Any],
    ) -> None:
        payload_hash = canonical_hash(dict(payload))
        identity = {"protocol_id": protocol_id, "event_type": event_type, "event_at": event_at, "payload_hash": payload_hash}
        connection.execute(
            "INSERT INTO phase_e_evaluation_protocol_events VALUES (?, ?, ?, ?, ?, ?)",
            (canonical_hash(identity), protocol_id, event_type, event_at, storage_json(dict(payload)), payload_hash),
        )

    @staticmethod
    def _append_run_event(
        connection: sqlite3.Connection, run_id: str, event_type: str, from_status: EvaluationRunStatus | None,
        to_status: EvaluationRunStatus, reason: str, event_at: str, payload: Mapping[str, Any],
    ) -> None:
        payload_hash = canonical_hash(dict(payload))
        identity = {
            "evaluation_run_id": run_id, "event_type": event_type,
            "from_status": from_status.value if from_status else None, "to_status": to_status.value,
            "reason": reason, "event_at": event_at, "payload_hash": payload_hash,
        }
        connection.execute(
            "INSERT INTO phase_e_evaluation_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (canonical_hash(identity), run_id, event_type, identity["from_status"], identity["to_status"], reason,
             event_at, storage_json(dict(payload)), payload_hash),
        )

    @staticmethod
    def _require_protocol(connection: sqlite3.Connection, protocol_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM phase_e_evaluation_protocols WHERE protocol_id=?", (protocol_id,)).fetchone()
        if row is None:
            raise EvaluationConflictError(f"Unknown E.4 protocol: {protocol_id}")
        return row

    @staticmethod
    def _require_run(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM phase_e_evaluation_runs WHERE evaluation_run_id=?", (run_id,)).fetchone()
        if row is None:
            raise EvaluationConflictError(f"Unknown E.4 evaluation run: {run_id}")
        return row


__all__ = [
    "E4_CLUSTER_POLICY", "E4_CORRECTION_METHOD", "E4_EVALUATOR_CODE_VERSION", "E4_HOLDOUT_POLICY",
    "EvaluationConflictError", "EvaluationError", "EvaluationIntegrityError", "EvaluationRunStatus",
    "EvaluationSettings", "HypothesisEvaluationStatus", "PhaseEEvaluator",
]
