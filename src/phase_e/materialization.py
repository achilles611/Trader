"""Phase E.2 deterministic scientific materialization.

Phase D owns the immutable observations.  This module never writes a
``science_*`` table.  It binds a full retained D.7 interval to an E-owned
universe fingerprint, freezes outcome-blind membership, and then writes only
compact E-owned feature and outcome artifacts.

The deliberately narrow causal replay supports D.6 ``wallet_action`` and
proves any persisted value agrees. Historical ``wallet_action_freshness`` is
explicitly missing because archive acquisition latency is not a causal event
feature. Other declared D feature versions may reuse exact validated D lineage;
they are never imputed, silently dropped, or used to replace membership.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import math
import shutil
import sqlite3
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

from .ledger import CorpusProvenanceError, LedgerIntegrityError, PhaseELedger
from .types import (
    CANONICALIZATION_VERSION,
    FeatureReference,
    OutcomeHorizon,
    PartitionIdentity,
    canonical_hash,
    canonical_json,
    normalized_utc,
    storage_json,
)


E2_SCHEMA_VERSION = "phase-e2"
E2_MATERIALIZER_CODE_VERSION = "phase-e2-materializer-v2"
E2_MATERIALIZER_CONFIG_VERSION = "phase-e2-materializer-config-v2"
E2_SOURCE_UNIVERSE_ALGORITHM = "PHASE_D_RETAINED_INTERVAL_V2"
LEGACY_E2_SOURCE_UNIVERSE_ALGORITHM = "PHASE_D_RETAINED_INTERVAL_V1"
ALL_ELIGIBLE_V1 = "ALL_ELIGIBLE_V1"
DETERMINISTIC_HASH_V1 = "DETERMINISTIC_HASH_V1"
TIME_STRATIFIED_HASH_V1 = "TIME_STRATIFIED_HASH_V1"
TIME_STRATIFIED_HASH_V2 = "TIME_STRATIFIED_HASH_V2"
SUPPORTED_SAMPLING_ALGORITHMS = frozenset({ALL_ELIGIBLE_V1, DETERMINISTIC_HASH_V1, TIME_STRATIFIED_HASH_V2})
LEGACY_SAMPLING_ALGORITHMS = frozenset({ALL_ELIGIBLE_V1, DETERMINISTIC_HASH_V1, TIME_STRATIFIED_HASH_V1})
HISTORICAL_ARCHIVE_SOURCE = "HISTORICAL_OFFICIAL_ARCHIVE"
HISTORICAL_EVENT_TIME_V1 = "HISTORICAL_EVENT_AT_V1"
FIRST_TRADE_WITHIN_TOLERANCE_V1 = "FIRST_TRADE_AT_OR_AFTER_WITHIN_TOLERANCE_V1"


class MaterializationStatus(StrEnum):
    REGISTERED = "REGISTERED"
    SELECTING = "SELECTING"
    MEMBERSHIP_FROZEN = "MEMBERSHIP_FROZEN"
    MATERIALIZING_FEATURES = "MATERIALIZING_FEATURES"
    ATTACHING_OUTCOMES = "ATTACHING_OUTCOMES"
    VERIFYING = "VERIFYING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    RECOVERABLE = "RECOVERABLE"


class MaterializationConflictError(RuntimeError):
    """A materialization identity, lifecycle, or concurrency rule was violated."""


class MaterializationIntegrityError(RuntimeError):
    """Persisted projection/artifact state is not supported by E.2 evidence."""


class OutcomeAccessError(MaterializationIntegrityError):
    """Selection attempted to inspect an outcome relation."""


def _require_nfc_json(value: Any, name: str) -> None:
    """Reject Unicode spellings that canonical identity would normalize away."""
    if isinstance(value, str):
        if unicodedata.normalize("NFC", value) != value:
            raise MaterializationIntegrityError(f"{name} must use NFC-normalized Unicode.")
    elif isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str) or unicodedata.normalize("NFC", key) != key:
                raise MaterializationIntegrityError(f"{name} has a noncanonical Unicode key.")
            _require_nfc_json(child, name)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _require_nfc_json(child, name)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value).replace("Z", "+00:00"))


def _sortable_instant(value: Any) -> str:
    """Return a fixed-width UTC key safe for SQLite ordering.

    D timestamps are canonical UTC but legitimately mix whole and fractional
    seconds.  Raw text ordering is therefore not chronological (``.`` sorts
    before ``Z``).  Every E.2 SQL time comparison uses this exact-width key.
    """
    return _instant(str(value)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _require_nfc(value: str, name: str) -> None:
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"{name} must use NFC-normalized Unicode.")


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _phase_d_hash(value: Any) -> str:
    """D's frozen JSON digest, retained here only for read validation."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _ordered_fingerprint(tag: str, rows: Iterable[Mapping[str, Any]]) -> str:
    """Hash an already ordered stream without retaining the stream in RAM."""
    digest = hashlib.sha256()
    digest.update((tag + "\n").encode("utf-8"))
    for row in rows:
        digest.update(canonical_json(row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


@dataclass(frozen=True)
class EligibilitySpec:
    """Outcome-blind anchor eligibility over the retained D source interval."""

    source: str = "HISTORICAL_OFFICIAL_ARCHIVE"
    kinds: tuple[str, ...] = ("WALLET_FILL",)

    def __post_init__(self) -> None:
        if not isinstance(self.source, str) or not self.source.strip():
            raise ValueError("Eligibility requires a nonempty Phase D source.")
        if not self.kinds or any(not isinstance(item, str) or not item.strip() for item in self.kinds):
            raise ValueError("Eligibility requires one or more observation kinds.")
        if len(set(self.kinds)) != len(self.kinds) or tuple(sorted(self.kinds)) != self.kinds:
            raise ValueError("Eligibility kinds must be unique and lexically sorted.")
        _require_nfc(self.source, "Eligibility source")
        for kind in self.kinds:
            _require_nfc(kind, "Eligibility kind")

    def payload(self) -> dict[str, Any]:
        return {"source": self.source, "kinds": list(self.kinds), "outcome_blind": True}


@dataclass(frozen=True)
class StratificationSpec:
    """Only causal time buckets are supported in the initial E.2 release."""

    kind: str = "NONE"
    bucket_seconds: int | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"NONE", "UTC_TIME_BUCKET"}:
            raise ValueError("E.2 supports only NONE or UTC_TIME_BUCKET stratification.")
        if self.kind == "NONE" and self.bucket_seconds is not None:
            raise ValueError("Unstratified materializations must not declare a bucket size.")
        if self.kind == "UTC_TIME_BUCKET":
            if isinstance(self.bucket_seconds, bool) or not isinstance(self.bucket_seconds, int) or self.bucket_seconds <= 0:
                raise ValueError("UTC_TIME_BUCKET stratification needs a positive integer bucket_seconds.")

    def payload(self) -> dict[str, Any]:
        return {"kind": self.kind, "bucket_seconds": self.bucket_seconds, "causal_at_anchor": True}


@dataclass(frozen=True)
class OutcomeResolutionSpec:
    """Versioned interpretation of a point-horizon trade-price outcome."""

    policy: str = FIRST_TRADE_WITHIN_TOLERANCE_V1
    maximum_lag_seconds: int = 5
    start_price_policy: str = "ANCHOR_FILL_PRICE_V1"
    tie_break_policy: str = "EARLIEST_EVENT_AT_THEN_OBSERVATION_ID_V1"

    def __post_init__(self) -> None:
        if self.policy != FIRST_TRADE_WITHIN_TOLERANCE_V1:
            raise ValueError("Unsupported E.2 outcome-resolution policy.")
        if (isinstance(self.maximum_lag_seconds, bool)
                or not isinstance(self.maximum_lag_seconds, int)
                or self.maximum_lag_seconds < 0):
            raise ValueError("Outcome maximum lag must be a nonnegative integer number of seconds.")
        if self.start_price_policy != "ANCHOR_FILL_PRICE_V1":
            raise ValueError("E.2 historical outcomes must start from the anchor fill price.")
        if self.tie_break_policy != "EARLIEST_EVENT_AT_THEN_OBSERVATION_ID_V1":
            raise ValueError("Unsupported E.2 simultaneous-price tie policy.")

    def payload(self) -> dict[str, Any]:
        return {
            "policy": self.policy,
            "maximum_lag_seconds": self.maximum_lag_seconds,
            "start_price_policy": self.start_price_policy,
            "tie_break_policy": self.tie_break_policy,
        }


@dataclass(frozen=True)
class SourceUniverseProvenance:
    """An E-owned fingerprint over the complete retained D interval.

    The D.7 corpus snapshot's observation fingerprint is intentionally not
    reused here: it names the 256 D.6 commissioning anchors.  This provenance
    fingerprints every D observation in the snapshot interval and source.
    """

    corpus_fingerprint: str
    corpus_provenance_hash: str
    interval_start: str
    interval_end: str
    source: str
    observation_count: int
    observation_fingerprint: str
    source_algorithm: str = E2_SOURCE_UNIVERSE_ALGORITHM

    def __post_init__(self) -> None:
        if not all(isinstance(item, str) and item for item in (
            self.corpus_fingerprint, self.corpus_provenance_hash, self.source,
            self.observation_fingerprint, self.source_algorithm,
        )):
            raise ValueError("Source-universe provenance requires nonempty immutable identifiers.")
        if isinstance(self.observation_count, bool) or not isinstance(self.observation_count, int) or self.observation_count < 0:
            raise ValueError("Source-universe observation_count must be a nonnegative integer.")
        if not _instant(self.interval_start) < _instant(self.interval_end):
            raise ValueError("Source-universe interval must be strictly ordered.")
        if self.source_algorithm not in {E2_SOURCE_UNIVERSE_ALGORITHM, LEGACY_E2_SOURCE_UNIVERSE_ALGORITHM}:
            raise ValueError("Unsupported immutable E.2 source-universe algorithm.")
        for name, value in (("corpus fingerprint", self.corpus_fingerprint), ("source", self.source),
                            ("source algorithm", self.source_algorithm)):
            _require_nfc(value, f"Source-universe {name}")

    def payload(self) -> dict[str, Any]:
        return {
            "corpus_fingerprint": self.corpus_fingerprint,
            "corpus_provenance_hash": self.corpus_provenance_hash,
            "interval_start": normalized_utc(self.interval_start),
            "interval_end": normalized_utc(self.interval_end),
            "source": self.source,
            "observation_count": self.observation_count,
            "observation_fingerprint": self.observation_fingerprint,
            "source_algorithm": self.source_algorithm,
        }


@dataclass(frozen=True)
class MaterializationSpec:
    """Complete immutable E.2 scientific materialization contract."""

    source_universe: SourceUniverseProvenance
    partition: PartitionIdentity
    eligibility: EligibilitySpec
    required_features: tuple[FeatureReference, ...]
    outcome_horizon: OutcomeHorizon
    sampling_algorithm: str
    sampling_seed: int
    target_count: int | None
    tier: str
    purpose: str
    outcome_resolution: OutcomeResolutionSpec = OutcomeResolutionSpec()
    stratification: StratificationSpec = StratificationSpec()
    anchor_time_policy: str = HISTORICAL_EVENT_TIME_V1
    feature_window_policy: str = "SAME_PARTITION_LOOKBACK_V1"
    ordering_policy: str = "HISTORICAL_EVENT_AT_OBSERVATION_ID_V2"
    missing_feature_policy: str = "KEEP_EXPLICIT_MISSING_V1"
    missing_outcome_policy: str = "KEEP_EXPLICIT_MISSING_V1"
    materializer_code_version: str = E2_MATERIALIZER_CODE_VERSION
    materializer_config_version: str = E2_MATERIALIZER_CONFIG_VERSION
    schema_version: str = E2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.source_universe, SourceUniverseProvenance):
            raise ValueError("Materialization requires typed source-universe provenance.")
        if not isinstance(self.partition, PartitionIdentity) or not isinstance(self.eligibility, EligibilitySpec):
            raise ValueError("Materialization requires typed partition and eligibility contracts.")
        if not isinstance(self.outcome_horizon, OutcomeHorizon) or self.partition.horizon.seconds != self.outcome_horizon.seconds:
            raise ValueError("Materialization and frozen E.1 partition outcome horizons must agree.")
        if not isinstance(self.outcome_resolution, OutcomeResolutionSpec):
            raise ValueError("Materialization requires typed outcome-resolution semantics.")
        if not isinstance(self.stratification, StratificationSpec):
            raise ValueError("Materialization requires typed stratification.")
        if any(not isinstance(feature, FeatureReference) for feature in self.required_features):
            raise ValueError("Materialization feature references must be typed.")
        for feature in self.required_features:
            _require_nfc(feature.feature_id, "Materialization feature ID")
        pairs = [(feature.feature_id, feature.version) for feature in self.required_features]
        if len(pairs) != len(set(pairs)):
            raise ValueError("Materialization feature references must be unique.")
        if self.partition.feature_lookback_seconds != max((feature.lookback_seconds for feature in self.required_features), default=0):
            raise ValueError("Partition feature lookback must equal E.2's maximum declared feature lookback.")
        legacy = self.materializer_code_version == "phase-e2-materializer-v1"
        supported = LEGACY_SAMPLING_ALGORITHMS if legacy else SUPPORTED_SAMPLING_ALGORITHMS
        if self.sampling_algorithm not in supported:
            raise ValueError("Unsupported immutable E.2 sampling algorithm/version.")
        if isinstance(self.sampling_seed, bool) or not isinstance(self.sampling_seed, int):
            raise ValueError("Materialization sampling_seed must be an integer.")
        if self.sampling_algorithm == ALL_ELIGIBLE_V1:
            if self.target_count is not None:
                raise ValueError("ALL_ELIGIBLE_V1 represents all eligible observations and has no target_count.")
        elif isinstance(self.target_count, bool) or not isinstance(self.target_count, int) or self.target_count <= 0:
            raise ValueError("Sampled materializations require a positive target_count.")
        time_stratified = self.sampling_algorithm in {TIME_STRATIFIED_HASH_V1, TIME_STRATIFIED_HASH_V2}
        if time_stratified and self.stratification.kind != "UTC_TIME_BUCKET":
            raise ValueError("Time-stratified sampling requires causal UTC_TIME_BUCKET stratification.")
        if not time_stratified and self.stratification.kind != "NONE":
            raise ValueError("Only time-stratified sampling may use stratification.")
        required_text = (self.tier, self.purpose, self.ordering_policy, self.missing_feature_policy,
                         self.missing_outcome_policy, self.anchor_time_policy, self.feature_window_policy,
                         self.materializer_code_version,
                         self.materializer_config_version, self.schema_version)
        if any(not isinstance(value, str) or not value.strip() for value in required_text):
            raise ValueError("Materialization needs explicit tier, policies, and materializer provenance.")
        expected_ordering = "NORMALIZED_AT_OBSERVATION_ID_V1" if legacy else "HISTORICAL_EVENT_AT_OBSERVATION_ID_V2"
        if self.ordering_policy != expected_ordering:
            raise ValueError("E.2 membership ordering policy conflicts with its materializer version.")
        if self.missing_feature_policy != "KEEP_EXPLICIT_MISSING_V1" or self.missing_outcome_policy != "KEEP_EXPLICIT_MISSING_V1":
            raise ValueError("E.2 never drops or replaces members for missing data.")
        if self.source_universe.source != self.eligibility.source:
            raise ValueError("Eligibility and source-universe source must match exactly.")
        if not legacy:
            if self.source_universe.source_algorithm != E2_SOURCE_UNIVERSE_ALGORITHM:
                raise ValueError("Current E.2 materializations require the full-row V2 source fingerprint.")
            if self.eligibility.source != HISTORICAL_ARCHIVE_SOURCE:
                raise ValueError("E.2 V2 supports retrospective official-archive event time only; live receipt-time science is separate.")
            if self.anchor_time_policy != HISTORICAL_EVENT_TIME_V1:
                raise ValueError("Historical E.2 anchors must use canonical source event time.")
            if self.feature_window_policy != "SAME_PARTITION_LOOKBACK_V1":
                raise ValueError("E.2 V2 requires feature evidence to stay inside the anchor partition.")
            if (_instant(self.partition.train_start) < _instant(self.source_universe.interval_start)
                    or _instant(self.partition.test_end) > _instant(self.source_universe.interval_end)):
                raise ValueError("All E.2 partitions and outcome tolerances must remain inside the bound source universe.")
            for name, value in (("tier", self.tier), ("purpose", self.purpose),
                                ("ordering policy", self.ordering_policy), ("anchor-time policy", self.anchor_time_policy)):
                _require_nfc(value, f"Materialization {name}")

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "canonicalization_version": CANONICALIZATION_VERSION,
            "materialization_algorithm_version": self.materializer_code_version,
            "source_universe": self.source_universe.payload(),
            "partition": self.partition.payload(),
            "eligibility": self.eligibility.payload(),
            "required_features": [feature.payload() for feature in sorted(self.required_features, key=lambda item: (item.feature_id, item.version))],
            "outcome_horizon": self.outcome_horizon.payload(),
            **({"outcome_resolution": self.outcome_resolution.payload()} if self.materializer_code_version != "phase-e2-materializer-v1" else {}),
            "sampling_algorithm": self.sampling_algorithm,
            "sampling_seed": self.sampling_seed,
            "target_count": self.target_count,
            "tier": self.tier,
            "purpose": self.purpose,
            "stratification": self.stratification.payload(),
            **({"anchor_time_policy": self.anchor_time_policy,
                "feature_window_policy": self.feature_window_policy} if self.materializer_code_version != "phase-e2-materializer-v1" else {}),
            "ordering_policy": self.ordering_policy,
            "missing_feature_policy": self.missing_feature_policy,
            "missing_outcome_policy": self.missing_outcome_policy,
            "materializer_code_version": self.materializer_code_version,
            "materializer_config_version": self.materializer_config_version,
        }

    @property
    def specification_hash(self) -> str:
        return canonical_hash(self.payload())

    @property
    def materialization_id(self) -> str:
        return "e2-" + canonical_hash({
            "schema": "phase-e2-materialization-identity-v1",
            "canonicalization_version": CANONICALIZATION_VERSION,
            "specification": self.payload(),
        })[:32]


@dataclass(frozen=True)
class _Candidate:
    observation_id: str
    normalized_at: str
    partition: str
    stratum: str
    selection_key: str

    def membership_payload(self, ordinal: int) -> dict[str, Any]:
        return {
            "ordinal": ordinal,
            "observation_id": self.observation_id,
            "normalized_at": self.normalized_at,
            "partition": self.partition,
            "stratum": self.stratum,
            "selection_key": self.selection_key,
        }


@dataclass(frozen=True)
class _WorstFirstCandidate:
    """Reverse heap ordering: heap root is the worst deterministic rank."""

    candidate: _Candidate

    def __lt__(self, other: "_WorstFirstCandidate") -> bool:
        return (self.candidate.selection_key, self.candidate.observation_id) > (
            other.candidate.selection_key, other.candidate.observation_id,
        )


class PhaseEMaterializer:
    """Durable E.2 membership, feature, outcome, and verification lifecycle."""

    TRADING_AUTHORITY = False
    _BATCH_SIZE = 500

    def __init__(self, database_path: str | Path, *, minimum_free_bytes: int = 16 * 1024 * 1024) -> None:
        self.path = Path(database_path)
        if minimum_free_bytes < 0:
            raise ValueError("minimum_free_bytes must be nonnegative.")
        self.minimum_free_bytes = minimum_free_bytes
        self._initialized = False

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=20, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=20000")
        connection.create_function("phase_e_instant", 1, _sortable_instant, deterministic=True)
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

    def initialize(self) -> None:
        if self._initialized:
            return
        # Keep all new persistence under the Phase E namespace.  Initializing
        # E.1 only creates its existing E-owned tables and lets us reuse its
        # hardened D corpus provenance resolver.
        PhaseELedger(self.path).initialize()
        with self._write() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS phase_e_materializations (
                    materialization_id TEXT PRIMARY KEY,
                    specification_json TEXT NOT NULL,
                    specification_hash TEXT NOT NULL,
                    source_universe_json TEXT NOT NULL,
                    source_universe_hash TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('REGISTERED','SELECTING','MEMBERSHIP_FROZEN','MATERIALIZING_FEATURES','ATTACHING_OUTCOMES','VERIFYING','COMPLETE','FAILED','RECOVERABLE')),
                    registered_at TEXT NOT NULL,
                    completed_at TEXT,
                    selected_count INTEGER,
                    excluded_counts_json TEXT,
                    membership_fingerprint TEXT,
                    sampling_design_fingerprint TEXT,
                    feature_artifact_fingerprint TEXT,
                    outcome_artifact_fingerprint TEXT,
                    completed_artifact_fingerprint TEXT,
                    byte_statistics_json TEXT
                );
                CREATE TABLE IF NOT EXISTS phase_e_materialization_events (
                    event_id TEXT PRIMARY KEY,
                    materialization_id TEXT NOT NULL REFERENCES phase_e_materializations(materialization_id),
                    event_type TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    event_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e_materialization_membership (
                    materialization_id TEXT NOT NULL REFERENCES phase_e_materializations(materialization_id),
                    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
                    observation_id TEXT NOT NULL,
                    normalized_at TEXT NOT NULL,
                    partition_name TEXT NOT NULL CHECK(partition_name IN ('train','validation','test')),
                    stratum_id TEXT NOT NULL,
                    selection_key TEXT NOT NULL,
                    PRIMARY KEY(materialization_id, observation_id),
                    UNIQUE(materialization_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS phase_e_materialization_sampling_design (
                    materialization_id TEXT PRIMARY KEY REFERENCES phase_e_materializations(materialization_id),
                    design_json TEXT NOT NULL,
                    design_hash TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e_materialization_features (
                    materialization_id TEXT NOT NULL REFERENCES phase_e_materializations(materialization_id),
                    observation_id TEXT NOT NULL,
                    feature_id TEXT NOT NULL,
                    feature_version INTEGER NOT NULL CHECK(feature_version > 0),
                    value_json TEXT NOT NULL,
                    missing INTEGER NOT NULL CHECK(missing IN (0,1)),
                    missing_reason TEXT,
                    source_observation_ids_json TEXT NOT NULL,
                    source_fingerprint TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    PRIMARY KEY(materialization_id, observation_id, feature_id, feature_version),
                    FOREIGN KEY(materialization_id, observation_id)
                        REFERENCES phase_e_materialization_membership(materialization_id, observation_id)
                );
                CREATE TABLE IF NOT EXISTS phase_e_materialization_outcomes (
                    materialization_id TEXT NOT NULL REFERENCES phase_e_materializations(materialization_id),
                    observation_id TEXT NOT NULL,
                    anchor_at TEXT NOT NULL,
                    resolved_at TEXT,
                    source_observation_id TEXT,
                    payload_json TEXT NOT NULL,
                    missing INTEGER NOT NULL CHECK(missing IN (0,1)),
                    missing_reason TEXT,
                    artifact_hash TEXT NOT NULL,
                    PRIMARY KEY(materialization_id, observation_id),
                    FOREIGN KEY(materialization_id, observation_id)
                        REFERENCES phase_e_materialization_membership(materialization_id, observation_id)
                );
                CREATE INDEX IF NOT EXISTS idx_phase_e_materializations_status ON phase_e_materializations(status, registered_at);
                CREATE INDEX IF NOT EXISTS idx_phase_e_materialization_events ON phase_e_materialization_events(materialization_id, event_at);
                CREATE INDEX IF NOT EXISTS idx_phase_e_materialization_members ON phase_e_materialization_membership(materialization_id, ordinal);
                CREATE TRIGGER IF NOT EXISTS phase_e_materializations_immutable_inputs
                    BEFORE UPDATE OF materialization_id, specification_json, specification_hash, source_universe_json, source_universe_hash, registered_at
                    ON phase_e_materializations BEGIN SELECT RAISE(ABORT, 'Phase E.2 materialization inputs are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materializations_no_delete
                    BEFORE DELETE ON phase_e_materializations BEGIN SELECT RAISE(ABORT, 'Phase E.2 materializations cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_events_append_only_update
                    BEFORE UPDATE ON phase_e_materialization_events BEGIN SELECT RAISE(ABORT, 'Phase E.2 lifecycle events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_events_append_only_delete
                    BEFORE DELETE ON phase_e_materialization_events BEGIN SELECT RAISE(ABORT, 'Phase E.2 lifecycle events cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_membership_append_only_update
                    BEFORE UPDATE ON phase_e_materialization_membership BEGIN SELECT RAISE(ABORT, 'Phase E.2 membership is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_membership_append_only_delete
                    BEFORE DELETE ON phase_e_materialization_membership BEGIN SELECT RAISE(ABORT, 'Phase E.2 membership cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_sampling_design_append_only_update
                    BEFORE UPDATE ON phase_e_materialization_sampling_design BEGIN SELECT RAISE(ABORT, 'Phase E.2 sampling design is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_sampling_design_append_only_delete
                    BEFORE DELETE ON phase_e_materialization_sampling_design BEGIN SELECT RAISE(ABORT, 'Phase E.2 sampling design cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_membership_selecting_insert_v2
                    BEFORE INSERT ON phase_e_materialization_membership
                    WHEN (SELECT status FROM phase_e_materializations WHERE materialization_id=NEW.materialization_id) <> 'SELECTING'
                    BEGIN SELECT RAISE(ABORT, 'Phase E.2 membership may be inserted only while selecting'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_sampling_design_selecting_insert_v2
                    BEFORE INSERT ON phase_e_materialization_sampling_design
                    WHEN (SELECT status FROM phase_e_materializations WHERE materialization_id=NEW.materialization_id) <> 'SELECTING'
                    BEGIN SELECT RAISE(ABORT, 'Phase E.2 sampling design may be frozen only while selecting'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_features_stage_insert_v2
                    BEFORE INSERT ON phase_e_materialization_features
                    WHEN (SELECT status FROM phase_e_materializations WHERE materialization_id=NEW.materialization_id) <> 'MATERIALIZING_FEATURES'
                    BEGIN SELECT RAISE(ABORT, 'Phase E.2 features may be inserted only during feature materialization'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_outcomes_stage_insert_v2
                    BEFORE INSERT ON phase_e_materialization_outcomes
                    WHEN (SELECT status FROM phase_e_materializations WHERE materialization_id=NEW.materialization_id) <> 'ATTACHING_OUTCOMES'
                    BEGIN SELECT RAISE(ABORT, 'Phase E.2 outcomes may be inserted only during outcome attachment'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_features_append_only_update
                    BEFORE UPDATE ON phase_e_materialization_features BEGIN SELECT RAISE(ABORT, 'Phase E.2 feature artifacts are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_features_append_only_delete
                    BEFORE DELETE ON phase_e_materialization_features BEGIN SELECT RAISE(ABORT, 'Phase E.2 feature artifacts cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_outcomes_append_only_update
                    BEFORE UPDATE ON phase_e_materialization_outcomes BEGIN SELECT RAISE(ABORT, 'Phase E.2 outcome artifacts are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_materialization_outcomes_append_only_delete
                    BEFORE DELETE ON phase_e_materialization_outcomes BEGIN SELECT RAISE(ABORT, 'Phase E.2 outcome artifacts cannot be deleted'); END;
                """
            )
            columns = {str(row["name"]) for row in connection.execute("PRAGMA table_info(phase_e_materializations)")}
            if "sampling_design_fingerprint" not in columns:
                connection.execute("ALTER TABLE phase_e_materializations ADD COLUMN sampling_design_fingerprint TEXT")
        self._initialized = True

    # ----- source planning ----------------------------------------------------------
    def bind_source_universe(self, *, corpus_fingerprint: str, eligibility: EligibilitySpec) -> SourceUniverseProvenance:
        """Fingerprint full D retention, not the bounded D.6 anchor projection."""
        self.initialize()
        corpus = PhaseELedger(self.path).resolve_phase_d_corpus(corpus_fingerprint)
        with self._write() as connection:
            # The same immediate transaction that scans the source excludes a
            # concurrent D writer while the fingerprint is formed.
            locked = PhaseELedger(self.path).resolve_phase_d_corpus(corpus_fingerprint)
            if canonical_hash(locked.payload()) != canonical_hash(corpus.payload()):
                raise CorpusProvenanceError("Phase D corpus changed while binding the full E.2 source universe.")
            count, fingerprint = self._source_universe_fingerprint(
                connection, locked, eligibility.source, E2_SOURCE_UNIVERSE_ALGORITHM,
            )
        return SourceUniverseProvenance(
            corpus_fingerprint=locked.corpus_fingerprint,
            corpus_provenance_hash=canonical_hash(locked.payload()),
            interval_start=locked.interval_start,
            interval_end=locked.interval_end,
            source=eligibility.source,
            observation_count=count,
            observation_fingerprint=fingerprint,
            source_algorithm=E2_SOURCE_UNIVERSE_ALGORITHM,
        )

    def plan(self, spec: MaterializationSpec) -> dict[str, Any]:
        """Return an outcome-blind population/resource estimate without writes."""
        self.initialize()
        self._verify_source_universe(spec)
        with self._connection() as connection:
            eligible, excluded, by_partition, by_stratum = self._selection_summary(connection, spec)
        selected_estimate = eligible if spec.target_count is None else min(eligible, spec.target_count)
        estimated_bytes = selected_estimate * (420 + len(spec.required_features) * 300)
        return {
            "materialization_id": spec.materialization_id,
            "specification_hash": spec.specification_hash,
            "source_universe": spec.source_universe.payload(),
            "eligible_count": eligible,
            "excluded_counts": excluded,
            "eligible_by_partition": by_partition,
            "eligible_by_stratum": by_stratum,
            "selected_count_estimate": selected_estimate,
            "estimated_artifact_bytes": estimated_bytes,
            "hot_free_bytes": shutil.disk_usage(self.path.parent).free,
            "sampling_algorithm": spec.sampling_algorithm,
            "tier": spec.tier,
            "trading_authority": False,
        }

    # ----- durable lifecycle --------------------------------------------------------
    def register(self, spec: MaterializationSpec, *, registered_at: str | None = None) -> dict[str, Any]:
        self.initialize()
        at = normalized_utc(registered_at or _now())
        materialization_id = spec.materialization_id
        payload = spec.payload()
        source_payload = spec.source_universe.payload()
        with self._write() as connection:
            self._verify_source_universe(spec, connection=connection)
            row = connection.execute("SELECT * FROM phase_e_materializations WHERE materialization_id=?", (materialization_id,)).fetchone()
            if row is None:
                if spec.materializer_code_version != E2_MATERIALIZER_CODE_VERSION:
                    raise MaterializationConflictError("Legacy E.2 specifications are read-only and cannot be newly registered.")
                connection.execute(
                    """INSERT INTO phase_e_materializations(materialization_id, specification_json, specification_hash,
                           source_universe_json, source_universe_hash, status, registered_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (materialization_id, storage_json(payload), spec.specification_hash, storage_json(source_payload),
                     canonical_hash(source_payload), MaterializationStatus.REGISTERED.value, at),
                )
                self._append_event(connection, materialization_id, "REGISTERED", None, MaterializationStatus.REGISTERED,
                                   "predeclared_before_selection", at,
                                   {"specification_hash": spec.specification_hash,
                                    "source_universe_hash": canonical_hash(source_payload)})
                row = connection.execute("SELECT * FROM phase_e_materializations WHERE materialization_id=?", (materialization_id,)).fetchone()
                assert row is not None
            else:
                self._validate_materialization(connection, row)
                if row["specification_hash"] != spec.specification_hash:
                    raise MaterializationConflictError("Deterministic E.2 identity conflicts with a different specification.")
        return self.get(materialization_id)

    def build(self, spec: MaterializationSpec, *, registered_at: str | None = None) -> dict[str, Any]:
        """Idempotently build a frozen materialization in bounded batches.

        No stage reads ``science_outcome_labels`` before membership freeze.
        A process death leaves only an incomplete lifecycle state; rerunning the
        same immutable specification deterministically completes the same rows.
        """
        registered = self.register(spec, registered_at=registered_at)
        materialization_id = str(registered["materialization_id"])
        if registered["status"] == MaterializationStatus.COMPLETE.value:
            self.verify(materialization_id)
            return self.get(materialization_id)
        self._guard_free_space(spec)
        self._select_and_freeze(spec)
        self._materialize_features(spec)
        self._attach_outcomes(spec)
        self._verify_and_complete(spec)
        return self.get(materialization_id)

    def get(self, materialization_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = self._require(connection, materialization_id)
            self._validate_materialization(connection, row)
            return self._payload(connection, row)

    def list(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute("SELECT * FROM phase_e_materializations ORDER BY registered_at, materialization_id").fetchall()
            payloads: list[dict[str, Any]] = []
            for row in rows:
                self._validate_materialization(connection, row)
                payloads.append(self._payload(connection, row))
            return payloads

    def membership(self, materialization_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            row = self._require(connection, materialization_id)
            self._validate_materialization(connection, row)
            rows = connection.execute(
                "SELECT * FROM phase_e_materialization_membership WHERE materialization_id=? ORDER BY ordinal", (materialization_id,),
            ).fetchall()
        return [self._member_payload(item) for item in rows]

    def verify(self, materialization_id: str) -> dict[str, Any]:
        """Reconcile evidence/artifacts and recheck frozen D source provenance."""
        self.initialize()
        with self._write() as connection:
            row = self._require(connection, materialization_id)
            self._validate_materialization(connection, row)
            spec = self._spec_from_row(row)
            self._verify_source_universe(spec, connection=connection)
            if row["status"] != MaterializationStatus.COMPLETE.value:
                raise MaterializationIntegrityError("Only COMPLETE materializations have a final verified artifact.")
            self._verify_deterministic_replay(connection, spec)
            return {
                "materialization_id": materialization_id,
                "verified": True,
                "membership_fingerprint": row["membership_fingerprint"],
                "sampling_design_fingerprint": row["sampling_design_fingerprint"],
                "feature_artifact_fingerprint": row["feature_artifact_fingerprint"],
                "outcome_artifact_fingerprint": row["outcome_artifact_fingerprint"],
                "completed_artifact_fingerprint": row["completed_artifact_fingerprint"],
                "trading_authority": False,
            }

    def reproduce(self, materialization_id: str) -> dict[str, Any]:
        verified = self.verify(materialization_id)
        return {**verified, "reproducible": True, "method": "revalidated immutable membership/artifact fingerprints"}

    # ----- selection: explicitly outcome blind -------------------------------------
    def _select_and_freeze(self, spec: MaterializationSpec) -> None:
        materialization_id = spec.materialization_id
        with self._write() as connection:
            row = self._require(connection, materialization_id)
            self._validate_materialization(connection, row)
            status = MaterializationStatus(str(row["status"]))
            if status is MaterializationStatus.REGISTERED:
                self._transition(connection, row, MaterializationStatus.SELECTING, "selection_started", {"outcome_access": "FORBIDDEN"})
            elif status not in {MaterializationStatus.SELECTING, MaterializationStatus.MEMBERSHIP_FROZEN,
                                MaterializationStatus.MATERIALIZING_FEATURES, MaterializationStatus.ATTACHING_OUTCOMES,
                                MaterializationStatus.VERIFYING, MaterializationStatus.COMPLETE}:
                raise MaterializationConflictError(f"Cannot select membership from {status.value}.")
        with self._connection() as connection:
            row = self._require(connection, materialization_id)
            if MaterializationStatus(str(row["status"])) is not MaterializationStatus.SELECTING:
                return
            if spec.sampling_algorithm == ALL_ELIGIBLE_V1:
                candidates = []
                excluded = {}
                eligible_count = 0
            else:
                candidates, excluded, eligible_count = self._select_candidates_outcome_blind(connection, spec)
        if spec.sampling_algorithm == ALL_ELIGIBLE_V1:
            selected_count, expected_fingerprint, excluded, eligible_count = self._stream_all_membership(spec)
        else:
            self._insert_membership(materialization_id, candidates)
            selected_count = len(candidates)
            expected_fingerprint = _ordered_fingerprint("phase-e2-membership-v1", (candidate.membership_payload(index) for index, candidate in enumerate(candidates)))
        with self._write() as connection:
            row = self._require(connection, materialization_id)
            status = MaterializationStatus(str(row["status"]))
            if status is not MaterializationStatus.SELECTING:
                return
            # The freeze boundary is the check/use barrier.  Revalidate the
            # bound source and independently recompute exact expected
            # membership in the same IMMEDIATE transaction before outcomes
            # become legal.
            self._verify_source_universe(spec, connection=connection)
            if spec.sampling_algorithm == ALL_ELIGIBLE_V1:
                expected_count, deterministic_fingerprint, expected_excluded, expected_eligible = self._expected_all_membership(connection, spec)
            else:
                expected_candidates, expected_excluded, expected_eligible = self._select_candidates_outcome_blind(connection, spec)
                expected_count = len(expected_candidates)
                deterministic_fingerprint = _ordered_fingerprint(
                    "phase-e2-membership-v1",
                    (candidate.membership_payload(index) for index, candidate in enumerate(expected_candidates)),
                )
            if (selected_count != expected_count or expected_fingerprint != deterministic_fingerprint
                    or excluded != expected_excluded or eligible_count != expected_eligible):
                raise MaterializationIntegrityError("Source evidence changed during selection; membership cannot freeze.")
            actual_count, actual_fingerprint = self._membership_fingerprint(connection, materialization_id)
            if actual_count != selected_count or actual_fingerprint != expected_fingerprint:
                raise MaterializationIntegrityError("Persisted membership differs from the deterministic outcome-blind selection.")
            sampling_design_fingerprint = self._freeze_sampling_design(
                connection, spec, eligible_count=eligible_count, selected_count=selected_count,
                excluded=excluded,
            )
            self._transition(
                connection, row, MaterializationStatus.MEMBERSHIP_FROZEN, "membership_frozen",
                {"selected_count": selected_count, "eligible_count": eligible_count, "excluded_counts": excluded,
                 "membership_fingerprint": actual_fingerprint,
                 "sampling_design_fingerprint": sampling_design_fingerprint,
                 "outcome_access": "NOT_YET_ALLOWED"},
                projection={"selected_count": selected_count, "excluded_counts_json": storage_json(excluded),
                            "membership_fingerprint": actual_fingerprint,
                            "sampling_design_fingerprint": sampling_design_fingerprint},
            )

    def _expected_all_membership(self, connection: sqlite3.Connection, spec: MaterializationSpec) -> tuple[int, str, dict[str, int], int]:
        sql = """SELECT observation_id, event_at, normalized_at, kind, source, raw_fingerprint
                 FROM science_observations
                 WHERE source=? AND phase_e_instant(normalized_at)>=? AND phase_e_instant(normalized_at)<?
                 ORDER BY phase_e_instant(normalized_at), observation_id"""
        self._assert_outcome_free_sql(sql)
        count = 0
        excluded: dict[str, int] = {}
        digest = hashlib.sha256()
        digest.update(b"phase-e2-membership-v1\n")
        for source_row in connection.execute(
            sql,
            (spec.eligibility.source, _sortable_instant(spec.source_universe.interval_start),
             _sortable_instant(spec.source_universe.interval_end)),
        ):
            candidate, reason = self._candidate_from_row(source_row, spec)
            if candidate is None:
                key = reason or "INELIGIBLE"
                excluded[key] = excluded.get(key, 0) + 1
                continue
            digest.update(canonical_json(candidate.membership_payload(count)).encode("utf-8"))
            digest.update(b"\n")
            count += 1
        return count, digest.hexdigest(), {key: excluded[key] for key in sorted(excluded)}, count

    def _select_candidates_outcome_blind(self, connection: sqlite3.Connection, spec: MaterializationSpec) -> tuple[list[_Candidate], dict[str, int], int]:
        """Select deterministic members using only source/anchor-time fields.

        This method selects a restricted column list and rejects an SQL relation
        name for outcome labels.  It cannot inspect a label value, availability,
        endpoint, or outcome-derived regime before membership freezes.
        """
        sql = """SELECT observation_id, event_at, normalized_at, kind, source, raw_fingerprint
                 FROM science_observations
                 WHERE source=? AND phase_e_instant(normalized_at)>=? AND phase_e_instant(normalized_at)<?"""
        self._assert_outcome_free_sql(sql)
        if spec.sampling_algorithm in {TIME_STRATIFIED_HASH_V1, TIME_STRATIFIED_HASH_V2}:
            return self._time_stratified_stream_select(connection, sql, spec)
        excluded: dict[str, int] = {}
        eligible_count = 0
        ranked: list[_WorstFirstCandidate] = []
        target = int(spec.target_count or 0)
        for row in connection.execute(sql, (spec.eligibility.source, _sortable_instant(spec.source_universe.interval_start),
                                            _sortable_instant(spec.source_universe.interval_end))):
            candidate, reason = self._candidate_from_row(row, spec)
            if candidate is None:
                excluded[reason or "INELIGIBLE"] = excluded.get(reason or "INELIGIBLE", 0) + 1
            else:
                eligible_count += 1
                if spec.sampling_algorithm == DETERMINISTIC_HASH_V1:
                    item = _WorstFirstCandidate(candidate)
                    if len(ranked) < target:
                        heapq.heappush(ranked, item)
                    elif (candidate.selection_key, candidate.observation_id) < (ranked[0].candidate.selection_key, ranked[0].candidate.observation_id):
                        heapq.heapreplace(ranked, item)
        if spec.sampling_algorithm == ALL_ELIGIBLE_V1:
            raise MaterializationIntegrityError("ALL_ELIGIBLE_V1 must use bounded streaming membership selection.")
        elif spec.sampling_algorithm == DETERMINISTIC_HASH_V1:
            selected = [item.candidate for item in ranked]
        else:  # pragma: no cover - handled by _time_stratified_stream_select
            raise MaterializationIntegrityError("Time-stratified selection dispatch failed.")
        # Member ordering is never sampling-rank order.  It is the contract's
        # stable causal ordering, making batch/worker timing irrelevant.
        selected.sort(key=lambda item: (_instant(item.normalized_at), item.observation_id))
        return selected, {key: excluded[key] for key in sorted(excluded)}, eligible_count

    def _time_stratified_stream_select(self, connection: sqlite3.Connection, sql: str,
                                       spec: MaterializationSpec) -> tuple[list[_Candidate], dict[str, int], int]:
        """Use bounded per-stratum heaps instead of retaining the population."""
        values = (spec.eligibility.source, _sortable_instant(spec.source_universe.interval_start),
                  _sortable_instant(spec.source_universe.interval_end))
        counts: dict[str, int] = {}
        excluded: dict[str, int] = {}
        eligible_count = 0
        for row in connection.execute(sql, values):
            candidate, reason = self._candidate_from_row(row, spec)
            if candidate is None:
                key = reason or "INELIGIBLE"
                excluded[key] = excluded.get(key, 0) + 1
            else:
                eligible_count += 1
                counts[candidate.stratum] = counts.get(candidate.stratum, 0) + 1
        target = min(int(spec.target_count or 0), eligible_count)
        strata = sorted(counts)
        desired: dict[str, int] = {}
        if strata:
            desired = self._time_stratum_targets(strata, target, spec)
        heaps: dict[str, list[_WorstFirstCandidate]] = {stratum: [] for stratum in strata}
        for row in connection.execute(sql, values):
            candidate, _ = self._candidate_from_row(row, spec)
            if candidate is None:
                continue
            heap = heaps[candidate.stratum]
            capacity = desired[candidate.stratum]
            item = _WorstFirstCandidate(candidate)
            if len(heap) < capacity:
                heapq.heappush(heap, item)
            elif capacity and (candidate.selection_key, candidate.observation_id) < (heap[0].candidate.selection_key, heap[0].candidate.observation_id):
                heapq.heapreplace(heap, item)
        selected = [item.candidate for heap in heaps.values() for item in heap]
        need = target - len(selected)
        if need > 0:
            selected_ids = {item.observation_id for item in selected}
            backfill: list[_WorstFirstCandidate] = []
            for row in connection.execute(sql, values):
                candidate, _ = self._candidate_from_row(row, spec)
                if candidate is None or candidate.observation_id in selected_ids:
                    continue
                item = _WorstFirstCandidate(candidate)
                if len(backfill) < need:
                    heapq.heappush(backfill, item)
                elif (candidate.selection_key, candidate.observation_id) < (backfill[0].candidate.selection_key, backfill[0].candidate.observation_id):
                    heapq.heapreplace(backfill, item)
            selected.extend(item.candidate for item in backfill)
        selected.sort(key=lambda item: (_instant(item.normalized_at), item.observation_id))
        return selected, {key: excluded[key] for key in sorted(excluded)}, eligible_count

    @staticmethod
    def _time_stratum_targets(strata: Sequence[str], target: int, spec: MaterializationSpec) -> dict[str, int]:
        if not strata:
            return {}
        base, remainder = divmod(target, len(strata))
        if spec.sampling_algorithm == TIME_STRATIFIED_HASH_V1:
            return {stratum: base + int(index < remainder) for index, stratum in enumerate(strata)}
        # V1 assigned every remainder to the lexically earliest buckets.  In
        # the target<strata case that systematically sampled only early time.
        # V2 chooses remainder strata by a seeded, total hash rank.
        remainder_order = sorted(
            strata,
            key=lambda stratum: (
                canonical_hash({"algorithm": spec.sampling_algorithm, "seed": spec.sampling_seed,
                                "allocation": "REMAINDER_STRATA_V2", "stratum": stratum}),
                stratum,
            ),
        )
        remainder_strata = set(remainder_order[:remainder])
        return {stratum: base + int(stratum in remainder_strata) for stratum in strata}

    @staticmethod
    def _probability(numerator: int, denominator: int) -> dict[str, int]:
        if denominator <= 0 or numerator < 0 or numerator > denominator:
            raise MaterializationIntegrityError("Sampling inclusion probability is invalid.")
        if numerator == 0:
            return {"numerator": 0, "denominator": 1}
        divisor = math.gcd(numerator, denominator)
        return {"numerator": numerator // divisor, "denominator": denominator // divisor}

    def _freeze_sampling_design(self, connection: sqlite3.Connection, spec: MaterializationSpec, *,
                                eligible_count: int, selected_count: int, excluded: Mapping[str, int]) -> str:
        eligible, summary_excluded, by_partition, by_stratum = self._selection_summary(connection, spec)
        if eligible != eligible_count or dict(summary_excluded) != dict(excluded):
            raise MaterializationIntegrityError("Sampling design population disagrees with outcome-blind selection.")
        selected_by_stratum = {
            str(row["stratum_id"]): int(row["count"])
            for row in connection.execute(
                """SELECT stratum_id, COUNT(*) AS count FROM phase_e_materialization_membership
                   WHERE materialization_id=? GROUP BY stratum_id""",
                (spec.materialization_id,),
            )
        }
        selected_by_partition = {
            str(row["partition_name"]): int(row["count"])
            for row in connection.execute(
                """SELECT partition_name, COUNT(*) AS count FROM phase_e_materialization_membership
                   WHERE materialization_id=? GROUP BY partition_name""",
                (spec.materialization_id,),
            )
        }
        if sum(selected_by_stratum.values()) != selected_count:
            raise MaterializationIntegrityError("Sampling design does not cover exact membership.")

        strata_payload: list[dict[str, Any]] = []
        if spec.sampling_algorithm == TIME_STRATIFIED_HASH_V2:
            targets = self._time_stratum_targets(sorted(by_stratum), selected_count, spec)
            primary = {stratum: min(by_stratum[stratum], targets[stratum]) for stratum in by_stratum}
            backfill_selected = selected_count - sum(primary.values())
            backfill_pool = sum(by_stratum[stratum] - primary[stratum] for stratum in by_stratum)
            for stratum in sorted(by_stratum):
                population = by_stratum[stratum]
                selected = selected_by_stratum.get(stratum, 0)
                primary_selected = primary[stratum]
                backfilled = selected - primary_selected
                if backfilled < 0:
                    raise MaterializationIntegrityError("Time-stratified membership violates its primary allocation.")
                if backfill_pool:
                    probability = self._probability(
                        primary_selected * backfill_pool + (population - primary_selected) * backfill_selected,
                        population * backfill_pool,
                    )
                else:
                    probability = self._probability(1, 1)
                strata_payload.append({
                    "stratum_id": stratum,
                    "partition": stratum.split(":", 1)[0],
                    "eligible_count": population,
                    "target_count": targets[stratum],
                    "primary_selected_count": primary_selected,
                    "backfill_selected_count": backfilled,
                    "selected_count": selected,
                    "inclusion_probability": probability,
                    "sampling_weight": ({"numerator": probability["denominator"], "denominator": probability["numerator"]}
                                        if probability["numerator"] else None),
                })
            allocation = "EQUAL_OCCUPIED_BUCKETS_SEEDED_REMAINDER_GLOBAL_BACKFILL_V2"
        else:
            probability = self._probability(selected_count, eligible_count) if eligible_count else self._probability(0, 1)
            for stratum in sorted(by_stratum):
                strata_payload.append({
                    "stratum_id": stratum,
                    "partition": stratum.split(":", 1)[0],
                    "eligible_count": by_stratum[stratum],
                    "target_count": None,
                    "primary_selected_count": selected_by_stratum.get(stratum, 0),
                    "backfill_selected_count": 0,
                    "selected_count": selected_by_stratum.get(stratum, 0),
                    "inclusion_probability": probability,
                    "sampling_weight": ({"numerator": probability["denominator"], "denominator": probability["numerator"]}
                                        if probability["numerator"] else None),
                })
            allocation = "ALL_ELIGIBLE" if spec.sampling_algorithm == ALL_ELIGIBLE_V1 else "GLOBAL_DETERMINISTIC_HASH_RANK"

        design = {
            "schema": "phase-e2-sampling-design-v1",
            "algorithm": spec.sampling_algorithm,
            "allocation_method": allocation,
            "eligible_count": eligible_count,
            "selected_count": selected_count,
            "excluded_counts": {key: int(excluded[key]) for key in sorted(excluded)},
            "by_partition": {
                partition: {"eligible_count": int(by_partition.get(partition, 0)),
                            "selected_count": int(selected_by_partition.get(partition, 0))}
                for partition in ("train", "validation", "test")
            },
            "strata": strata_payload,
            "empty_strata_policy": "UNOCCUPIED_BUCKETS_DO_NOT_EXIST",
            "selection_key_tie_break": "OBSERVATION_ID_ASCENDING",
        }
        design_hash = canonical_hash(design)
        artifact_hash = canonical_hash({"materialization_id": spec.materialization_id,
                                        "design_hash": design_hash, "design": design})
        existing = connection.execute(
            "SELECT * FROM phase_e_materialization_sampling_design WHERE materialization_id=?",
            (spec.materialization_id,),
        ).fetchone()
        if existing is None:
            connection.execute(
                "INSERT INTO phase_e_materialization_sampling_design VALUES (?, ?, ?, ?)",
                (spec.materialization_id, storage_json(design), design_hash, artifact_hash),
            )
        elif (existing["design_json"] != storage_json(design) or existing["design_hash"] != design_hash
              or existing["artifact_hash"] != artifact_hash):
            raise MaterializationIntegrityError("Frozen sampling-design artifact conflicts with deterministic selection.")
        return artifact_hash

    def _stream_all_membership(self, spec: MaterializationSpec) -> tuple[int, str, dict[str, int], int]:
        """Freeze full-population membership without holding its universe in RAM."""
        ordinal = 0
        excluded: dict[str, int] = {}
        digest = hashlib.sha256()
        digest.update(b"phase-e2-membership-v1\n")
        sql = """SELECT observation_id, event_at, normalized_at, kind, source, raw_fingerprint
                 FROM science_observations
                 WHERE source=? AND phase_e_instant(normalized_at)>=? AND phase_e_instant(normalized_at)<?
                 ORDER BY phase_e_instant(normalized_at), observation_id"""
        self._assert_outcome_free_sql(sql)
        with self._write() as connection:
            row = self._require(connection, spec.materialization_id)
            if MaterializationStatus(str(row["status"])) is not MaterializationStatus.SELECTING:
                return 0, digest.hexdigest(), {}, 0
            batch: list[_Candidate] = []
            for source_row in connection.execute(
                sql,
                (spec.eligibility.source, _sortable_instant(spec.source_universe.interval_start),
                 _sortable_instant(spec.source_universe.interval_end)),
            ):
                candidate, reason = self._candidate_from_row(source_row, spec)
                if candidate is None:
                    key = reason or "INELIGIBLE"
                    excluded[key] = excluded.get(key, 0) + 1
                else:
                    batch.append(candidate)
                    digest.update(canonical_json(candidate.membership_payload(ordinal + len(batch) - 1)).encode("utf-8"))
                    digest.update(b"\n")
                    if len(batch) >= self._BATCH_SIZE:
                        self._insert_membership_batch_connection(connection, spec.materialization_id, batch, ordinal)
                        ordinal += len(batch)
                        batch.clear()
            if batch:
                self._insert_membership_batch_connection(connection, spec.materialization_id, batch, ordinal)
                ordinal += len(batch)
        return ordinal, digest.hexdigest(), {key: excluded[key] for key in sorted(excluded)}, ordinal

    def _candidate_from_row(self, row: sqlite3.Row, spec: MaterializationSpec) -> tuple[_Candidate | None, str | None]:
        if str(row["kind"]) not in spec.eligibility.kinds:
            return None, "UNSUPPORTED_KIND"
        if spec.materializer_code_version != "phase-e2-materializer-v1":
            event_at = normalized_utc(str(row["event_at"]))
            normalized_at = normalized_utc(str(row["normalized_at"]))
            if event_at != normalized_at:
                raise MaterializationIntegrityError("Historical E.2 event_at and normalized_at must identify the same canonical event instant.")
        timestamp = normalized_utc(str(row["normalized_at"]))
        partition, partition_start, partition_end = self._partition_for(timestamp, spec.partition)
        if partition is None:
            return None, "OUTSIDE_DECLARED_PARTITIONS"
        if (_instant(timestamp) - timedelta(seconds=spec.partition.feature_lookback_seconds)
                < _instant(str(partition_start))):
            return None, "FEATURE_WINDOW_CROSSES_PARTITION_START"
        # This uses only the predeclared horizon and anchor timestamp, not a
        # market/outcome lookup. Exact equality fails because split ends are
        # exclusive under the frozen E.1 contract.
        if _instant(timestamp) + timedelta(seconds=spec.outcome_horizon.seconds) >= _instant(partition_end):
            return None, "OUTCOME_WINDOW_CROSSES_PARTITION_END"
        if spec.stratification.kind == "UTC_TIME_BUCKET":
            assert spec.stratification.bucket_seconds is not None
            seconds = int(_instant(timestamp).timestamp())
            bucket = seconds - seconds % spec.stratification.bucket_seconds
            stratum = f"{partition}:utc:{bucket}"
        else:
            stratum = f"{partition}:all"
        key = canonical_hash({"algorithm": spec.sampling_algorithm, "seed": spec.sampling_seed,
                              "observation_id": str(row["observation_id"]), "stratum": stratum})
        return _Candidate(str(row["observation_id"]), timestamp, partition, stratum, key), None

    @staticmethod
    def _partition_for(timestamp: str, partition: PartitionIdentity) -> tuple[str | None, str | None, str | None]:
        instant = _instant(timestamp)
        for name, start, end in (
            ("train", partition.train_start, partition.train_end),
            ("validation", partition.validation_start, partition.validation_end),
            ("test", partition.test_start, partition.test_end),
        ):
            if _instant(start) <= instant < _instant(end):
                return name, normalized_utc(start), normalized_utc(end)
        return None, None, None

    @staticmethod
    def _assert_outcome_free_sql(sql: str) -> None:
        if "science_outcome_labels" in sql.lower() or "outcome" in sql.lower():
            raise OutcomeAccessError("Membership selection may not read outcome relations.")

    # ----- materialized artifacts ----------------------------------------------------
    def _materialize_features(self, spec: MaterializationSpec) -> None:
        materialization_id = spec.materialization_id
        with self._write() as connection:
            row = self._require(connection, materialization_id)
            status = MaterializationStatus(str(row["status"]))
            if status is MaterializationStatus.MEMBERSHIP_FROZEN:
                self._transition(connection, row, MaterializationStatus.MATERIALIZING_FEATURES, "feature_materialization_started", {})
            elif status not in {MaterializationStatus.MATERIALIZING_FEATURES, MaterializationStatus.ATTACHING_OUTCOMES,
                                MaterializationStatus.VERIFYING, MaterializationStatus.COMPLETE}:
                raise MaterializationConflictError(f"Cannot materialize features from {status.value}.")
        if not spec.required_features:
            self._advance_features_if_complete(spec)
            return
        after = -1
        while True:
            with self._connection() as connection:
                row = self._require(connection, materialization_id)
                if MaterializationStatus(str(row["status"])) is not MaterializationStatus.MATERIALIZING_FEATURES:
                    break
                members = connection.execute(
                    """SELECT membership.*, observation.kind, observation.source, observation.source_event_id,
                              observation.wallet, observation.symbol, observation.event_at, observation.received_at,
                              observation.network, observation.raw_fingerprint, observation.schema_version,
                              observation.code_sha, observation.config_hash, observation.quality_flags_json,
                              observation.payload_json, observation.payload_hash, observation.persisted_at
                       FROM phase_e_materialization_membership AS membership
                       JOIN science_observations AS observation ON observation.observation_id=membership.observation_id
                       WHERE membership.materialization_id=? AND membership.ordinal>? ORDER BY membership.ordinal LIMIT ?""",
                    (materialization_id, after, self._BATCH_SIZE),
                ).fetchall()
            if not members:
                break
            self._insert_feature_batch(spec, members)
            after = int(members[-1]["ordinal"])
        self._advance_features_if_complete(spec)

    def _insert_feature_batch(self, spec: MaterializationSpec, members: Sequence[sqlite3.Row]) -> None:
        rows: list[tuple[Any, ...]] = []
        with self._connection() as connection:
            for member in members:
                self._validate_d_observation(member, expected_source=spec.eligibility.source)
                for feature in spec.required_features:
                    value, missing, reason, source_ids, source_fingerprint = self._feature_value(connection, member, feature, spec)
                    payload = {"value": value}
                    identity = {
                        "materialization_id": spec.materialization_id, "observation_id": member["observation_id"],
                        "feature_id": feature.feature_id, "feature_version": feature.version, "value": value,
                        "missing": missing, "missing_reason": reason, "source_observation_ids": list(source_ids),
                        "source_fingerprint": source_fingerprint,
                    }
                    artifact_hash = canonical_hash(identity)
                    rows.append((spec.materialization_id, member["observation_id"], feature.feature_id, feature.version,
                                 storage_json(payload), int(missing), reason, storage_json(list(source_ids)), source_fingerprint, artifact_hash))
        with self._write() as connection:
            for item in rows:
                existing = connection.execute(
                    """SELECT artifact_hash FROM phase_e_materialization_features
                       WHERE materialization_id=? AND observation_id=? AND feature_id=? AND feature_version=?""", item[:4],
                ).fetchone()
                if existing is None:
                    connection.execute("INSERT INTO phase_e_materialization_features VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", item)
                elif existing["artifact_hash"] != item[-1]:
                    raise MaterializationIntegrityError("Feature artifact conflicts with its deterministic causal replay.")

    def _feature_value(self, connection: sqlite3.Connection, member: sqlite3.Row, feature: FeatureReference,
                       spec: MaterializationSpec) -> tuple[float | None, bool, str | None, tuple[str, ...], str]:
        self._verify_feature_definition(connection, feature, spec)
        if feature.feature_id == "wallet_action_freshness" and spec.eligibility.source == HISTORICAL_ARCHIVE_SOURCE:
            source_ids = (str(member["observation_id"]),)
            return (None, True, "HISTORICAL_ACQUISITION_LATENCY_IS_NOT_A_CAUSAL_FEATURE", source_ids,
                    self._source_fingerprint(connection, source_ids))
        existing = connection.execute(
            """SELECT * FROM science_feature_values WHERE observation_id=? AND feature_id=? AND feature_version=?""",
            (member["observation_id"], feature.feature_id, feature.version),
        ).fetchone()
        payload = self._load_phase_d_json(member["payload_json"], "observation payload")
        if feature.feature_id == "wallet_action" and str(member["kind"]) == "WALLET_FILL":
            side = str(payload.get("side") or payload.get("action") or "").lower()
            value = 1.0 if side in {"buy", "long", "open_long"} else -1.0 if side in {"sell", "short", "open_short"} else None
            source_ids = (str(member["observation_id"]),)
            source_fingerprint = self._source_fingerprint(connection, source_ids)
            replayed = ((value, False, None, source_ids, source_fingerprint) if value is not None else
                        (None, True, "MALFORMED_WALLET_ACTION", source_ids, source_fingerprint))
            if existing is not None:
                persisted = self._read_phase_d_feature(connection, existing, member, feature, spec)
                if (persisted[0], persisted[1], persisted[3], persisted[4]) != (
                        replayed[0], replayed[1], replayed[3], replayed[4]):
                    raise MaterializationIntegrityError("Persisted Phase D wallet_action conflicts with deterministic causal replay.")
            return replayed
        if existing is not None:
            return self._read_phase_d_feature(connection, existing, member, feature, spec)
        source_ids = (str(member["observation_id"]),)
        return None, True, "SOURCE_FEATURE_UNAVAILABLE", source_ids, self._source_fingerprint(connection, source_ids)

    def _read_phase_d_feature(self, connection: sqlite3.Connection, value_row: sqlite3.Row, member: sqlite3.Row,
                              feature: FeatureReference, spec: MaterializationSpec) -> tuple[float | None, bool, str | None, tuple[str, ...], str]:
        body = self._load_phase_d_json(value_row["value_json"], "Phase D feature value")
        if set(body) != {"value"}:
            raise MaterializationIntegrityError("Phase D feature value payload is malformed.")
        value = body["value"]
        if value is not None and _finite(value) is None:
            raise MaterializationIntegrityError("Phase D feature contains NaN, Infinity, or a nonnumeric value.")
        source_ids_raw = self._load_phase_d_json(value_row["source_observation_ids_json"], "Phase D feature sources")
        if not isinstance(source_ids_raw, list) or not source_ids_raw or any(not isinstance(item, str) for item in source_ids_raw):
            raise MaterializationIntegrityError("Phase D feature source references are malformed.")
        source_ids = tuple(source_ids_raw)
        if len(set(source_ids)) != len(source_ids):
            raise MaterializationIntegrityError("Phase D feature source references are duplicated.")
        member_at = _instant(str(member["normalized_at"]))
        partition_name, partition_start, _ = self._partition_for(str(member["normalized_at"]), spec.partition)
        if partition_name is None or partition_start is None:
            raise MaterializationIntegrityError("Feature anchor is outside its frozen partition.")
        source_rows = self._source_rows(connection, source_ids)
        if len(source_rows) != len(source_ids):
            raise MaterializationIntegrityError("Phase D feature references a missing source observation.")
        for source in source_rows:
            self._validate_d_observation(source, expected_source=spec.eligibility.source)
            source_at = _instant(str(source["normalized_at"]))
            if source_at > member_at or source_at < member_at - timedelta(seconds=feature.lookback_seconds):
                raise MaterializationIntegrityError("Phase D feature violates E.1's declared causal lookback window.")
            if source_at < _instant(partition_start):
                raise MaterializationIntegrityError("Phase D feature crosses the anchor partition start.")
            if (source_at < _instant(spec.source_universe.interval_start)
                    or source_at >= _instant(spec.source_universe.interval_end)):
                raise MaterializationIntegrityError("Phase D feature source is outside the bound source universe.")
        missing = bool(value_row["missing"])
        if missing != (value is None):
            raise MaterializationIntegrityError("Phase D feature missing flag/value disagree.")
        expected_d_fingerprint = _phase_d_hash({
            "feature": feature.feature_id,
            "observation": member["raw_fingerprint"],
            "sources": [(source["observation_id"], source["raw_fingerprint"]) for source in source_rows],
        })
        if value_row["data_fingerprint"] != expected_d_fingerprint:
            raise MaterializationIntegrityError("Phase D feature data fingerprint conflicts with its exact source lineage.")
        return (_finite(value) if value is not None else None, missing,
                "SOURCE_DECLARED_MISSING" if missing else None, source_ids,
                self._source_fingerprint_from_rows(source_rows))

    def _advance_features_if_complete(self, spec: MaterializationSpec) -> None:
        with self._write() as connection:
            row = self._require(connection, spec.materialization_id)
            if MaterializationStatus(str(row["status"])) is not MaterializationStatus.MATERIALIZING_FEATURES:
                return
            selected = int(row["selected_count"] or 0)
            expected = selected * len(spec.required_features)
            count, fingerprint = self._feature_fingerprint(connection, spec.materialization_id)
            if count != expected:
                raise MaterializationIntegrityError("Feature artifact count is incomplete; refusing outcome attachment.")
            self._transition(connection, row, MaterializationStatus.ATTACHING_OUTCOMES, "feature_materialization_complete",
                             {"feature_count": count, "feature_artifact_fingerprint": fingerprint},
                             projection={"feature_artifact_fingerprint": fingerprint})

    def _attach_outcomes(self, spec: MaterializationSpec) -> None:
        materialization_id = spec.materialization_id
        with self._write() as connection:
            row = self._require(connection, materialization_id)
            status = MaterializationStatus(str(row["status"]))
            if status is not MaterializationStatus.ATTACHING_OUTCOMES:
                if status in {MaterializationStatus.VERIFYING, MaterializationStatus.COMPLETE}:
                    return
                raise MaterializationConflictError(f"Cannot attach outcomes from {status.value}.")
        after = -1
        while True:
            with self._connection() as connection:
                row = self._require(connection, materialization_id)
                if MaterializationStatus(str(row["status"])) is not MaterializationStatus.ATTACHING_OUTCOMES:
                    break
                members = connection.execute(
                    """SELECT membership.*, observation.kind, observation.source, observation.source_event_id,
                              observation.wallet, observation.symbol, observation.event_at, observation.received_at,
                              observation.network, observation.raw_fingerprint, observation.schema_version,
                              observation.code_sha, observation.config_hash, observation.quality_flags_json,
                              observation.payload_json, observation.payload_hash, observation.persisted_at
                       FROM phase_e_materialization_membership AS membership
                       JOIN science_observations AS observation ON observation.observation_id=membership.observation_id
                       WHERE membership.materialization_id=? AND membership.ordinal>? ORDER BY membership.ordinal LIMIT ?""",
                    (materialization_id, after, self._BATCH_SIZE),
                ).fetchall()
            if not members:
                break
            self._insert_outcome_batch(spec, members)
            after = int(members[-1]["ordinal"])
        with self._write() as connection:
            row = self._require(connection, materialization_id)
            if MaterializationStatus(str(row["status"])) is not MaterializationStatus.ATTACHING_OUTCOMES:
                return
            count, fingerprint = self._outcome_fingerprint(connection, materialization_id)
            if count != int(row["selected_count"] or 0):
                raise MaterializationIntegrityError("Outcome artifact count is incomplete; selected membership may not disappear.")
            self._transition(connection, row, MaterializationStatus.VERIFYING, "outcome_attachment_complete",
                             {"outcome_count": count, "outcome_artifact_fingerprint": fingerprint},
                             projection={"outcome_artifact_fingerprint": fingerprint})

    def _insert_outcome_batch(self, spec: MaterializationSpec, members: Sequence[sqlite3.Row]) -> None:
        rows: list[tuple[Any, ...]] = []
        with self._connection() as connection:
            for member in members:
                self._validate_d_observation(member, expected_source=spec.eligibility.source)
                payload, resolved_at, end_id, missing, reason = self._outcome_value(connection, member, spec)
                identity = {"materialization_id": spec.materialization_id, "observation_id": member["observation_id"],
                            "anchor_at": member["normalized_at"], "resolved_at": resolved_at,
                            "source_observation_id": end_id, "payload": payload, "missing": missing,
                            "missing_reason": reason}
                rows.append((spec.materialization_id, member["observation_id"], member["normalized_at"], resolved_at,
                             end_id, storage_json(payload), int(missing), reason, canonical_hash(identity)))
        with self._write() as connection:
            for item in rows:
                existing = connection.execute("SELECT artifact_hash FROM phase_e_materialization_outcomes WHERE materialization_id=? AND observation_id=?", item[:2]).fetchone()
                if existing is None:
                    connection.execute("INSERT INTO phase_e_materialization_outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", item)
                elif existing["artifact_hash"] != item[-1]:
                    raise MaterializationIntegrityError("Outcome artifact conflicts with deterministic post-freeze attachment.")

    def _outcome_value(self, connection: sqlite3.Connection, member: sqlite3.Row, spec: MaterializationSpec) -> tuple[dict[str, Any], str | None, str | None, bool, str | None]:
        anchor_at = _instant(str(member["normalized_at"]))
        _, _, partition_end = self._partition_for(str(member["normalized_at"]), spec.partition)
        if partition_end is None or anchor_at + timedelta(seconds=spec.outcome_horizon.seconds) >= _instant(partition_end):
            # This should be unreachable because the same rule precedes freeze.
            raise MaterializationIntegrityError("Frozen member crosses its E.1 outcome boundary.")
        if str(member["kind"]) != "WALLET_FILL":
            return {}, None, None, True, "OUTCOME_UNSUPPORTED_FOR_OBSERVATION_KIND"
        anchor_payload = self._load_phase_d_json(member["payload_json"], "anchor payload")
        start = _finite(anchor_payload.get("price"))
        symbol = member["symbol"]
        if not isinstance(symbol, str) or not symbol:
            return {}, None, None, True, "OUTCOME_SYMBOL_UNAVAILABLE"
        if start is None or start <= 0:
            return {}, None, None, True, "OUTCOME_START_PRICE_UNAVAILABLE"
        endpoint_at = anchor_at + timedelta(seconds=spec.outcome_horizon.seconds)
        maximum_at = min(
            endpoint_at + timedelta(seconds=spec.outcome_resolution.maximum_lag_seconds),
            _instant(partition_end) - timedelta(microseconds=1),
        )
        lower_sql = endpoint_at.strftime("%Y-%m-%dT%H:%M:%S")
        upper_sql = (maximum_at.replace(microsecond=0) + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S")
        possible = connection.execute(
            """SELECT * FROM science_observations WHERE source=? AND kind='MARKET_PRICE' AND symbol IS ?
               AND normalized_at>=? AND normalized_at<? ORDER BY normalized_at, observation_id""",
            (spec.eligibility.source, symbol, lower_sql, upper_sql),
        ).fetchall()
        candidates = [row for row in possible if endpoint_at <= _instant(str(row["normalized_at"])) <= maximum_at]
        if not candidates:
            return {}, None, None, True, "OUTCOME_MARKET_EVIDENCE_NOT_WITHIN_TOLERANCE"
        end = min(candidates, key=lambda row: (_instant(str(row["normalized_at"])), str(row["observation_id"])))
        self._validate_d_observation(end, expected_source=spec.eligibility.source)
        end_payload = self._load_phase_d_json(end["payload_json"], "outcome market payload")
        end_price = _finite(end_payload.get("price"))
        if end_price is None or end_price <= 0:
            return {}, None, None, True, "OUTCOME_MARKET_PRICE_MALFORMED"
        side = str(anchor_payload.get("side") or anchor_payload.get("action") or "").lower()
        direction = 1.0 if side in {"buy", "long", "open_long"} else -1.0 if side in {"sell", "short", "open_short"} else None
        if direction is None:
            return {}, None, None, True, "OUTCOME_DIRECTION_UNAVAILABLE"
        gross_long = end_price / start - 1.0
        cost = _finite(anchor_payload.get("estimated_cost"))
        if cost is None or cost < 0:
            return {}, None, None, True, "OUTCOME_COST_UNAVAILABLE"
        resolved_at = _instant(str(end["normalized_at"]))
        elapsed_seconds = (resolved_at - anchor_at).total_seconds()
        resolution_lag_seconds = (resolved_at - endpoint_at).total_seconds()
        payload = {
            "long_return": gross_long, "short_return": -gross_long,
            "end_of_horizon_return": gross_long, "net_outcome": gross_long * direction - cost,
            "direction": "long" if direction > 0 else "short", "horizon_seconds": spec.outcome_horizon.seconds,
            "target_endpoint_at": normalized_utc(endpoint_at.isoformat()),
            "actual_elapsed_seconds": elapsed_seconds,
            "resolution_lag_seconds": resolution_lag_seconds,
            "outcome_algorithm": "E2_HISTORICAL_TRADE_RETURN_V2",
            "resolution_policy": spec.outcome_resolution.policy,
            "maximum_resolution_lag_seconds": spec.outcome_resolution.maximum_lag_seconds,
        }
        return payload, normalized_utc(str(end["normalized_at"])), str(end["observation_id"]), False, None

    # ----- source/provenance validation ---------------------------------------------
    def _verify_source_universe(self, spec: MaterializationSpec, *, connection: sqlite3.Connection | None = None) -> None:
        corpus = PhaseELedger(self.path).resolve_phase_d_corpus(spec.source_universe.corpus_fingerprint)
        if canonical_hash(corpus.payload()) != spec.source_universe.corpus_provenance_hash:
            raise CorpusProvenanceError("The bound D corpus provenance changed; E.2 refuses materialization.")
        if (corpus.interval_start != normalized_utc(spec.source_universe.interval_start)
                or corpus.interval_end != normalized_utc(spec.source_universe.interval_end)):
            raise CorpusProvenanceError("E.2 source universe interval conflicts with frozen D provenance.")
        owns = connection is None
        if owns:
            with self._connection() as local:
                count, fingerprint = self._source_universe_fingerprint(
                    local, corpus, spec.eligibility.source, spec.source_universe.source_algorithm,
                )
        else:
            assert connection is not None
            count, fingerprint = self._source_universe_fingerprint(
                connection, corpus, spec.eligibility.source, spec.source_universe.source_algorithm,
            )
        if count != spec.source_universe.observation_count or fingerprint != spec.source_universe.observation_fingerprint:
            raise CorpusProvenanceError("The complete retained Phase D source universe no longer matches E.2's frozen fingerprint.")
        available = {(item.feature_id, item.version) for item in corpus.feature_versions}
        missing = [(feature.feature_id, feature.version) for feature in spec.required_features if (feature.feature_id, feature.version) not in available]
        if missing:
            raise CorpusProvenanceError(f"E.2 required feature versions are absent from bound D provenance: {missing!r}")

    def _source_universe_fingerprint(self, connection: sqlite3.Connection, corpus: Any, source: str,
                                     algorithm: str) -> tuple[int, str]:
        if algorithm == LEGACY_E2_SOURCE_UNIVERSE_ALGORITHM:
            return self._legacy_source_universe_fingerprint(connection, corpus, source)
        if algorithm != E2_SOURCE_UNIVERSE_ALGORITHM:
            raise MaterializationIntegrityError("Unsupported E.2 source-universe algorithm.")
        try:
            rows = connection.execute(
                """SELECT * FROM science_observations
                   WHERE source=? AND phase_e_instant(normalized_at)>=? AND phase_e_instant(normalized_at)<?
                   ORDER BY phase_e_instant(normalized_at), observation_id""",
                (source, _sortable_instant(corpus.interval_start), _sortable_instant(corpus.interval_end)),
            )
        except sqlite3.OperationalError as exc:
            raise MaterializationIntegrityError("Phase D source timestamps cannot be ordered as canonical instants.") from exc
        count = 0
        digest = hashlib.sha256()
        digest.update(b"phase-e2-source-universe-v2\n")
        try:
            for row in rows:
                identity = self._validated_d_observation_identity(row, expected_source=source)
                digest.update(canonical_json(identity).encode("utf-8"))
                digest.update(b"\n")
                count += 1
        except sqlite3.OperationalError as exc:
            raise MaterializationIntegrityError("Phase D source timestamps cannot be ordered as canonical instants.") from exc
        return count, digest.hexdigest()

    @staticmethod
    def _legacy_source_universe_fingerprint(connection: sqlite3.Connection, corpus: Any, source: str) -> tuple[int, str]:
        rows = connection.execute(
            """SELECT observation_id, normalized_at, kind, source, raw_fingerprint, payload_hash
               FROM science_observations WHERE source=? AND normalized_at>=? AND normalized_at<?
               ORDER BY normalized_at, observation_id""", (source, corpus.interval_start, corpus.interval_end),
        )
        count = 0
        digest = hashlib.sha256()
        digest.update(b"phase-e2-source-universe-v1\n")
        for row in rows:
            if row["raw_fingerprint"] != row["payload_hash"]:
                raise MaterializationIntegrityError("Phase D source observation fingerprint/hash evidence is inconsistent.")
            if normalized_utc(str(row["normalized_at"])) != row["normalized_at"]:
                raise MaterializationIntegrityError("Phase D source observation timestamp is not canonical UTC.")
            digest.update(canonical_json({"observation_id": row["observation_id"], "normalized_at": normalized_utc(row["normalized_at"]),
                                          "kind": row["kind"], "source": row["source"],
                                          "raw_fingerprint": row["raw_fingerprint"], "payload_hash": row["payload_hash"]}).encode("utf-8"))
            digest.update(b"\n")
            count += 1
        return count, digest.hexdigest()

    def _verify_feature_definition(self, connection: sqlite3.Connection, feature: FeatureReference, spec: MaterializationSpec) -> None:
        row = connection.execute("SELECT definition_json, definition_hash FROM science_features WHERE feature_id=? AND version=?", (feature.feature_id, feature.version)).fetchone()
        if row is None:
            raise CorpusProvenanceError("A required frozen D feature definition is absent.")
        definition = self._load_phase_d_json(row["definition_json"], "Phase D feature definition")
        if _phase_d_hash(definition) != row["definition_hash"]:
            raise CorpusProvenanceError("A required frozen D feature definition hash is corrupt.")

    @staticmethod
    def _load_phase_d_json(raw: Any, name: str) -> Any:
        if not isinstance(raw, str):
            raise MaterializationIntegrityError(f"{name} must be canonical JSON text.")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MaterializationIntegrityError(f"{name} is malformed.") from exc
        if json.dumps(value, sort_keys=True, separators=(",", ":"), default=str) != raw:
            raise MaterializationIntegrityError(f"{name} is not canonical Phase D JSON.")
        return value

    def _validated_d_observation_identity(self, row: sqlite3.Row, *, expected_source: str | None = None) -> dict[str, Any]:
        payload = self._load_phase_d_json(row["payload_json"], "Phase D observation payload")
        quality_flags = self._load_phase_d_json(row["quality_flags_json"], "Phase D observation quality flags")
        _require_nfc_json(payload, "Phase D observation payload")
        _require_nfc_json(quality_flags, "Phase D observation quality flags")
        if (not isinstance(payload, Mapping) or not isinstance(quality_flags, Mapping)
                or _phase_d_hash(payload) != row["payload_hash"]
                or hashlib.sha256(str(row["payload_json"]).encode("utf-8")).hexdigest() != row["payload_hash"]
                or row["raw_fingerprint"] != row["payload_hash"]):
            raise MaterializationIntegrityError("Phase D observation payload/hash evidence is inconsistent.")
        event_at = normalized_utc(str(row["event_at"]))
        received_at = normalized_utc(str(row["received_at"]))
        normalized_at = normalized_utc(str(row["normalized_at"]))
        persisted_at = normalized_utc(str(row["persisted_at"]))
        if any(value != row[name] for name, value in (
            ("event_at", event_at), ("received_at", received_at),
            ("normalized_at", normalized_at), ("persisted_at", persisted_at),
        )):
            raise MaterializationIntegrityError("Phase D observation timestamps must be canonical UTC text.")
        if event_at != normalized_at:
            raise MaterializationIntegrityError("Historical Phase D normalized_at must equal canonical event_at.")
        if _instant(received_at) < _instant(event_at) or _instant(persisted_at) < _instant(received_at):
            raise MaterializationIntegrityError("Phase D observation receipt/persistence chronology is impossible.")
        source = str(row["source"])
        for name in ("observation_id", "kind", "source", "source_event_id", "wallet", "symbol", "network",
                     "raw_fingerprint", "code_sha", "config_hash"):
            value = row[name]
            if value is not None and (not isinstance(value, str) or unicodedata.normalize("NFC", value) != value):
                raise MaterializationIntegrityError(f"Phase D observation {name} must be NFC-normalized text.")
        if expected_source is not None and source != expected_source:
            raise MaterializationIntegrityError("Phase D observation escaped its bound source.")
        if source == HISTORICAL_ARCHIVE_SOURCE:
            if quality_flags.get("historical") is not True or payload.get("origin") != HISTORICAL_ARCHIVE_SOURCE:
                raise MaterializationIntegrityError("Official archive evidence lacks its historical-origin contract.")
        identity = {
            "observation_id": row["observation_id"], "kind": row["kind"], "source": source,
            "source_event_id": row["source_event_id"], "wallet": row["wallet"], "symbol": row["symbol"],
            "event_at": event_at, "received_at": received_at, "normalized_at": normalized_at,
            "network": row["network"], "raw_fingerprint": row["raw_fingerprint"],
            "schema_version": row["schema_version"], "code_sha": row["code_sha"],
            "config_hash": row["config_hash"], "quality_flags": quality_flags,
            "payload_hash": row["payload_hash"], "persisted_at": persisted_at,
        }
        return identity

    def _validate_d_observation(self, row: sqlite3.Row, *, expected_source: str | None = None) -> None:
        self._validated_d_observation_identity(row, expected_source=expected_source)

    # ----- fingerprints, projection reconciliation ----------------------------------
    def _membership_fingerprint(self, connection: sqlite3.Connection, materialization_id: str) -> tuple[int, str]:
        rows = connection.execute("SELECT * FROM phase_e_materialization_membership WHERE materialization_id=? ORDER BY ordinal", (materialization_id,)).fetchall()
        for ordinal, row in enumerate(rows):
            if int(row["ordinal"]) != ordinal:
                raise MaterializationIntegrityError("Membership ordinals are not contiguous deterministic order.")
            if normalized_utc(str(row["normalized_at"])) != row["normalized_at"]:
                raise MaterializationIntegrityError("Membership timestamp is not canonical UTC text.")
        return len(rows), _ordered_fingerprint("phase-e2-membership-v1", (self._member_payload(row) for row in rows))

    def _feature_fingerprint(self, connection: sqlite3.Connection, materialization_id: str) -> tuple[int, str]:
        rows = connection.execute(
            """SELECT * FROM phase_e_materialization_features WHERE materialization_id=?
               ORDER BY observation_id, feature_id, feature_version""", (materialization_id,),
        ).fetchall()
        payloads = []
        for row in rows:
            value = self._load_e_json(row["value_json"], "feature artifact")
            sources = self._load_e_json(row["source_observation_ids_json"], "feature sources")
            if (not isinstance(value, Mapping) or set(value) != {"value"}
                    or not isinstance(sources, list) or not sources
                    or any(not isinstance(item, str) or not item for item in sources)
                    or len(set(sources)) != len(sources)):
                raise MaterializationIntegrityError("Feature artifact value/source structure is malformed.")
            numeric = value["value"]
            if numeric is not None and _finite(numeric) is None:
                raise MaterializationIntegrityError("Feature artifact value must be finite numeric evidence.")
            missing = bool(row["missing"])
            reason = row["missing_reason"]
            if missing != (numeric is None) or (missing and (not isinstance(reason, str) or not reason)) or (not missing and reason is not None):
                raise MaterializationIntegrityError("Feature artifact missingness semantics are inconsistent.")
            if not isinstance(row["source_fingerprint"], str) or not row["source_fingerprint"]:
                raise MaterializationIntegrityError("Feature artifact lacks source-lineage provenance.")
            identity = {"materialization_id": row["materialization_id"], "observation_id": row["observation_id"],
                        "feature_id": row["feature_id"], "feature_version": row["feature_version"], "value": numeric,
                        "missing": missing, "missing_reason": reason,
                        "source_observation_ids": sources, "source_fingerprint": row["source_fingerprint"]}
            if canonical_hash(identity) != row["artifact_hash"]:
                raise MaterializationIntegrityError("Feature artifact hash is inconsistent.")
            payloads.append({"observation_id": row["observation_id"], "feature_id": row["feature_id"],
                             "feature_version": row["feature_version"], "artifact_hash": row["artifact_hash"]})
        return len(rows), _ordered_fingerprint("phase-e2-features-v1", payloads)

    def _outcome_fingerprint(self, connection: sqlite3.Connection, materialization_id: str) -> tuple[int, str]:
        specification_row = connection.execute(
            "SELECT specification_json FROM phase_e_materializations WHERE materialization_id=?",
            (materialization_id,),
        ).fetchone()
        specification = (self._load_e_json(specification_row["specification_json"], "materialization specification")
                         if specification_row else {})
        legacy = specification.get("materializer_code_version") == "phase-e2-materializer-v1"
        rows = connection.execute("SELECT * FROM phase_e_materialization_outcomes WHERE materialization_id=? ORDER BY observation_id", (materialization_id,)).fetchall()
        payloads = []
        for row in rows:
            payload = self._load_e_json(row["payload_json"], "outcome artifact")
            if not isinstance(payload, Mapping):
                raise MaterializationIntegrityError("Outcome artifact payload must be an object.")
            missing = bool(row["missing"])
            reason = row["missing_reason"]
            member = connection.execute(
                "SELECT normalized_at FROM phase_e_materialization_membership WHERE materialization_id=? AND observation_id=?",
                (materialization_id, row["observation_id"]),
            ).fetchone()
            if (member is None or normalized_utc(str(row["anchor_at"])) != row["anchor_at"]
                    or normalized_utc(str(member["normalized_at"])) != member["normalized_at"]
                    or row["anchor_at"] != member["normalized_at"]):
                raise MaterializationIntegrityError("Outcome artifact anchor does not match frozen membership.")
            if missing:
                if payload or row["resolved_at"] is not None or row["source_observation_id"] is not None or not isinstance(reason, str) or not reason:
                    raise MaterializationIntegrityError("Missing outcome artifact is not explicit and empty.")
            else:
                if reason is not None or row["resolved_at"] is None or row["source_observation_id"] is None:
                    raise MaterializationIntegrityError("Resolved outcome artifact lacks exact source lineage.")
                if normalized_utc(str(row["resolved_at"])) != row["resolved_at"]:
                    raise MaterializationIntegrityError("Outcome resolution timestamp is not canonical UTC text.")
                required = ({"long_return", "short_return", "end_of_horizon_return", "net_outcome",
                             "direction", "horizon_seconds", "outcome_algorithm"} if legacy else
                            {"long_return", "short_return", "end_of_horizon_return", "net_outcome",
                             "direction", "horizon_seconds", "target_endpoint_at", "actual_elapsed_seconds",
                             "resolution_lag_seconds", "outcome_algorithm", "resolution_policy",
                             "maximum_resolution_lag_seconds"})
                numeric_names = ["long_return", "short_return", "end_of_horizon_return", "net_outcome"]
                if not legacy:
                    numeric_names.extend(("actual_elapsed_seconds", "resolution_lag_seconds"))
                if set(payload) != required or any(_finite(payload[name]) is None for name in numeric_names):
                    raise MaterializationIntegrityError("Resolved outcome artifact has malformed V2 semantics.")
            identity = {"materialization_id": row["materialization_id"], "observation_id": row["observation_id"],
                        "anchor_at": row["anchor_at"], "resolved_at": row["resolved_at"],
                        "source_observation_id": row["source_observation_id"], "payload": payload,
                        "missing": missing, "missing_reason": reason}
            if canonical_hash(identity) != row["artifact_hash"]:
                raise MaterializationIntegrityError("Outcome artifact hash is inconsistent.")
            payloads.append({"observation_id": row["observation_id"], "artifact_hash": row["artifact_hash"]})
        return len(rows), _ordered_fingerprint("phase-e2-outcomes-v1", payloads)

    def _sampling_design_fingerprint(self, connection: sqlite3.Connection, materialization_id: str) -> str:
        row = connection.execute(
            "SELECT * FROM phase_e_materialization_sampling_design WHERE materialization_id=?",
            (materialization_id,),
        ).fetchone()
        if row is None:
            raise MaterializationIntegrityError("Frozen membership lacks its sampling-design artifact.")
        design = self._load_e_json(row["design_json"], "sampling design")
        if not isinstance(design, Mapping) or canonical_hash(design) != row["design_hash"]:
            raise MaterializationIntegrityError("Sampling-design hash is inconsistent.")
        artifact = canonical_hash({"materialization_id": materialization_id,
                                   "design_hash": row["design_hash"], "design": design})
        if artifact != row["artifact_hash"]:
            raise MaterializationIntegrityError("Sampling-design artifact identity is inconsistent.")
        if (not isinstance(design.get("eligible_count"), int)
                or not isinstance(design.get("selected_count"), int)
                or not isinstance(design.get("strata"), list)):
            raise MaterializationIntegrityError("Sampling-design counts are malformed.")
        selected = int(connection.execute(
            "SELECT COUNT(*) FROM phase_e_materialization_membership WHERE materialization_id=?",
            (materialization_id,),
        ).fetchone()[0])
        if design["selected_count"] != selected or sum(int(item["selected_count"]) for item in design["strata"]) != selected:
            raise MaterializationIntegrityError("Sampling design does not reconcile exact membership.")
        return artifact

    def _verify_deterministic_replay(self, connection: sqlite3.Connection, spec: MaterializationSpec) -> None:
        """Prove persisted membership/features/outcomes equal current bound D.

        This closes changed-then-restored check/use races across bounded stage
        commits.  The final proof runs under an IMMEDIATE transaction, after a
        full source verification, so no D writer can change evidence during
        replay.
        """
        if spec.materializer_code_version == "phase-e2-materializer-v1":
            return
        if spec.sampling_algorithm == ALL_ELIGIBLE_V1:
            expected_count, expected_membership, expected_excluded, expected_eligible = self._expected_all_membership(
                connection, spec,
            )
        else:
            candidates, expected_excluded, expected_eligible = self._select_candidates_outcome_blind(connection, spec)
            expected_count = len(candidates)
            expected_membership = _ordered_fingerprint(
                "phase-e2-membership-v1",
                (candidate.membership_payload(index) for index, candidate in enumerate(candidates)),
            )
        actual_count, actual_membership = self._membership_fingerprint(connection, spec.materialization_id)
        if expected_count != actual_count or expected_membership != actual_membership:
            raise MaterializationIntegrityError("Frozen membership does not replay from its exact bound D evidence.")
        # Sampling metadata is scientific evidence, not descriptive decoration.
        # Reconstruct it from the current eligible population and exact frozen
        # membership so a consistently rehashed false weight/probability cannot
        # survive verification.
        sampling = self._freeze_sampling_design(
            connection, spec, eligible_count=expected_eligible,
            selected_count=expected_count, excluded=expected_excluded,
        )
        projection = self._require(connection, spec.materialization_id)
        if sampling != projection["sampling_design_fingerprint"]:
            raise MaterializationIntegrityError("Sampling-design projection does not match its immutable artifact.")

        members = connection.execute(
            """SELECT membership.*, observation.kind, observation.source, observation.source_event_id,
                      observation.wallet, observation.symbol, observation.event_at, observation.received_at,
                      observation.network, observation.raw_fingerprint, observation.schema_version,
                      observation.code_sha, observation.config_hash, observation.quality_flags_json,
                      observation.payload_json, observation.payload_hash, observation.persisted_at
               FROM phase_e_materialization_membership AS membership
               JOIN science_observations AS observation ON observation.observation_id=membership.observation_id
               WHERE membership.materialization_id=? ORDER BY membership.ordinal""",
            (spec.materialization_id,),
        ).fetchall()
        if len(members) != expected_count:
            raise MaterializationIntegrityError("A frozen member lost its exact D source observation.")
        for member in members:
            self._validate_d_observation(member, expected_source=spec.eligibility.source)
            for feature in spec.required_features:
                value, missing, reason, source_ids, source_fingerprint = self._feature_value(
                    connection, member, feature, spec,
                )
                expected_hash = canonical_hash({
                    "materialization_id": spec.materialization_id,
                    "observation_id": member["observation_id"],
                    "feature_id": feature.feature_id,
                    "feature_version": feature.version,
                    "value": value,
                    "missing": missing,
                    "missing_reason": reason,
                    "source_observation_ids": list(source_ids),
                    "source_fingerprint": source_fingerprint,
                })
                actual = connection.execute(
                    """SELECT artifact_hash FROM phase_e_materialization_features
                       WHERE materialization_id=? AND observation_id=? AND feature_id=? AND feature_version=?""",
                    (spec.materialization_id, member["observation_id"], feature.feature_id, feature.version),
                ).fetchone()
                if actual is None or actual["artifact_hash"] != expected_hash:
                    raise MaterializationIntegrityError("Feature artifact does not replay from its exact causal D lineage.")
            payload, resolved_at, end_id, missing, reason = self._outcome_value(connection, member, spec)
            expected_outcome = canonical_hash({
                "materialization_id": spec.materialization_id,
                "observation_id": member["observation_id"],
                "anchor_at": member["normalized_at"],
                "resolved_at": resolved_at,
                "source_observation_id": end_id,
                "payload": payload,
                "missing": missing,
                "missing_reason": reason,
            })
            actual_outcome = connection.execute(
                """SELECT artifact_hash FROM phase_e_materialization_outcomes
                   WHERE materialization_id=? AND observation_id=?""",
                (spec.materialization_id, member["observation_id"]),
            ).fetchone()
            if actual_outcome is None or actual_outcome["artifact_hash"] != expected_outcome:
                raise MaterializationIntegrityError("Outcome artifact does not replay from exact post-freeze D evidence.")

    def _verify_and_complete(self, spec: MaterializationSpec) -> None:
        with self._write() as connection:
            row = self._require(connection, spec.materialization_id)
            status = MaterializationStatus(str(row["status"]))
            if status is MaterializationStatus.COMPLETE:
                return
            if status is not MaterializationStatus.VERIFYING:
                raise MaterializationConflictError(f"Cannot verify materialization from {status.value}.")
            self._verify_source_universe(spec, connection=connection)
            self._verify_deterministic_replay(connection, spec)
            selected, membership = self._membership_fingerprint(connection, spec.materialization_id)
            sampling_design = self._sampling_design_fingerprint(connection, spec.materialization_id)
            features, feature_fingerprint = self._feature_fingerprint(connection, spec.materialization_id)
            outcomes, outcome_fingerprint = self._outcome_fingerprint(connection, spec.materialization_id)
            if selected != int(row["selected_count"] or -1) or membership != row["membership_fingerprint"]:
                raise MaterializationIntegrityError("Membership projection does not match frozen membership evidence.")
            if features != selected * len(spec.required_features) or outcomes != selected:
                raise MaterializationIntegrityError("Final artifact counts do not cover frozen membership exactly.")
            completed = canonical_hash({"membership": membership, "sampling_design": sampling_design,
                                        "features": feature_fingerprint,
                                        "outcomes": outcome_fingerprint, "specification_hash": spec.specification_hash})
            bytes_stats = {"membership_rows": selected, "feature_rows": features, "outcome_rows": outcomes,
                           "estimated_bytes": selected * (420 + len(spec.required_features) * 300)}
            self._transition(connection, row, MaterializationStatus.COMPLETE, "artifact_verified_complete",
                             {"selected_count": selected, "membership_fingerprint": membership,
                              "sampling_design_fingerprint": sampling_design,
                              "feature_artifact_fingerprint": feature_fingerprint,
                              "outcome_artifact_fingerprint": outcome_fingerprint,
                              "completed_artifact_fingerprint": completed},
                             projection={"feature_artifact_fingerprint": feature_fingerprint,
                                         "outcome_artifact_fingerprint": outcome_fingerprint,
                                         "completed_artifact_fingerprint": completed,
                                         "byte_statistics_json": storage_json(bytes_stats)})

    def _validate_materialization(self, connection: sqlite3.Connection, row: sqlite3.Row) -> None:
        try:
            spec = self._spec_from_row(row)
            if spec.materialization_id != row["materialization_id"] or spec.specification_hash != row["specification_hash"]:
                raise MaterializationIntegrityError("Materialization identity/specification projection conflicts with immutable content.")
            source_payload = self._load_e_json(row["source_universe_json"], "source universe")
            if source_payload != spec.source_universe.payload() or canonical_hash(source_payload) != row["source_universe_hash"]:
                raise MaterializationIntegrityError("Source-universe projection is inconsistent.")
            registered_at = normalized_utc(str(row["registered_at"]))
            if registered_at != row["registered_at"]:
                raise MaterializationIntegrityError("Materialization registration timestamp is not canonical UTC text.")
            status = MaterializationStatus(str(row["status"]))
            events = connection.execute("SELECT * FROM phase_e_materialization_events WHERE materialization_id=?", (row["materialization_id"],)).fetchall()
            expected_states = [MaterializationStatus.REGISTERED, MaterializationStatus.SELECTING,
                               MaterializationStatus.MEMBERSHIP_FROZEN, MaterializationStatus.MATERIALIZING_FEATURES,
                               MaterializationStatus.ATTACHING_OUTCOMES, MaterializationStatus.VERIFYING,
                               MaterializationStatus.COMPLETE]
            if status not in expected_states or len(events) != expected_states.index(status) + 1:
                raise MaterializationIntegrityError("Lifecycle projection has missing, extra, or impossible E.2 events.")
            by_target: dict[str, sqlite3.Row] = {}
            for event in events:
                target = str(event["to_status"])
                if target in by_target:
                    raise MaterializationIntegrityError("Duplicated semantic lifecycle transition detected.")
                by_target[target] = event
            previous: MaterializationStatus | None = None
            parsed_events: list[dict[str, Any]] = []
            prior_at = registered_at
            for expected in expected_states[:expected_states.index(status) + 1]:
                event = by_target.get(expected.value)
                if event is None:
                    raise MaterializationIntegrityError("Lifecycle transition is missing its expected target state.")
                payload = self._load_e_json(event["payload_json"], "materialization event")
                event_at = normalized_utc(str(event["event_at"]))
                if event_at != event["event_at"]:
                    raise MaterializationIntegrityError("Lifecycle event timestamp is not canonical UTC text.")
                if _instant(event_at) < _instant(prior_at):
                    raise MaterializationIntegrityError("Lifecycle evidence is not chronologically ordered.")
                identity = {"materialization_id": row["materialization_id"], "event_type": event["event_type"],
                            "from_status": previous.value if previous else None, "to_status": expected.value,
                            "reason": event["reason"], "event_at": event_at, "payload_hash": event["payload_hash"]}
                if canonical_hash(payload) != event["payload_hash"] or canonical_hash(identity) != event["event_id"]:
                    raise MaterializationIntegrityError("Materialization lifecycle event identity is inconsistent.")
                if event["from_status"] != (previous.value if previous else None) or event["to_status"] != expected.value:
                    raise MaterializationIntegrityError("Materialization lifecycle sequence is forged.")
                parsed_events.append({"type": event["event_type"], "reason": event["reason"],
                                      "event_at": event_at, "payload": payload})
                previous = expected
                prior_at = event_at
            if (parsed_events[0]["type"] != "REGISTERED"
                    or parsed_events[0]["reason"] != "predeclared_before_selection"
                    or parsed_events[0]["event_at"] != registered_at
                    or parsed_events[0]["payload"] != {"specification_hash": row["specification_hash"],
                                                       "source_universe_hash": row["source_universe_hash"]}):
                raise MaterializationIntegrityError("Registration event does not bind E.2 immutable inputs.")
            expected_reasons = (
                ("SELECTING", "selection_started"),
                ("MEMBERSHIP_FROZEN", "membership_frozen"),
                ("MATERIALIZING_FEATURES", "feature_materialization_started"),
                ("ATTACHING_OUTCOMES", "feature_materialization_complete"),
                ("VERIFYING", "outcome_attachment_complete"),
                ("COMPLETE", "artifact_verified_complete"),
            )
            for index, (event_type, reason) in enumerate(expected_reasons, start=1):
                if index >= len(parsed_events):
                    break
                if parsed_events[index]["type"] != event_type or parsed_events[index]["reason"] != reason:
                    raise MaterializationIntegrityError("Lifecycle event has valid hashes but false transition semantics.")
            if len(parsed_events) > 1 and parsed_events[1]["payload"] != {"outcome_access": "FORBIDDEN"}:
                raise MaterializationIntegrityError("Selection event does not prove outcome access was forbidden.")
            if len(parsed_events) > 3 and parsed_events[3]["payload"] != {}:
                raise MaterializationIntegrityError("Feature-start event contains unsupported semantic state.")

            stage_index = expected_states.index(status)
            member_rows = int(connection.execute(
                "SELECT COUNT(*) FROM phase_e_materialization_membership WHERE materialization_id=?",
                (row["materialization_id"],),
            ).fetchone()[0])
            feature_rows = int(connection.execute(
                "SELECT COUNT(*) FROM phase_e_materialization_features WHERE materialization_id=?",
                (row["materialization_id"],),
            ).fetchone()[0])
            outcome_rows = int(connection.execute(
                "SELECT COUNT(*) FROM phase_e_materialization_outcomes WHERE materialization_id=?",
                (row["materialization_id"],),
            ).fetchone()[0])
            if stage_index == 0 and member_rows:
                raise MaterializationIntegrityError("REGISTERED projection already contains membership.")
            if stage_index < 3 and feature_rows:
                raise MaterializationIntegrityError("Feature artifacts exist before their lifecycle stage.")
            if stage_index < 4 and outcome_rows:
                raise MaterializationIntegrityError("Outcome artifacts exist before membership and features are frozen.")
            if stage_index < 2 and any(row[name] is not None for name in (
                "selected_count", "excluded_counts_json", "membership_fingerprint", "sampling_design_fingerprint",
            )):
                raise MaterializationIntegrityError("Pre-freeze projection claims frozen membership state.")
            if expected_states.index(status) >= expected_states.index(MaterializationStatus.MEMBERSHIP_FROZEN):
                frozen = parsed_events[2]["payload"]
                count, fingerprint = self._membership_fingerprint(connection, str(row["materialization_id"]))
                if (frozen.get("selected_count") != count or frozen.get("membership_fingerprint") != fingerprint
                        or row["selected_count"] != count or row["membership_fingerprint"] != fingerprint):
                    raise MaterializationIntegrityError("Frozen membership/projection cannot be reconciled.")
                if spec.materializer_code_version != "phase-e2-materializer-v1":
                    sampling = self._sampling_design_fingerprint(connection, str(row["materialization_id"]))
                    sampling_row = connection.execute(
                        "SELECT design_json FROM phase_e_materialization_sampling_design WHERE materialization_id=?",
                        (row["materialization_id"],),
                    ).fetchone()
                    sampling_payload = self._load_e_json(sampling_row["design_json"], "sampling design")
                    expected_frozen_keys = {
                        "selected_count", "eligible_count", "excluded_counts", "membership_fingerprint",
                        "sampling_design_fingerprint", "outcome_access",
                    }
                    if (set(frozen) != expected_frozen_keys
                            or frozen.get("sampling_design_fingerprint") != sampling
                            or row["sampling_design_fingerprint"] != sampling
                            or frozen.get("eligible_count") != sampling_payload.get("eligible_count")
                            or frozen.get("excluded_counts") != self._load_e_json(row["excluded_counts_json"], "excluded counts")
                            or frozen.get("excluded_counts") != sampling_payload.get("excluded_counts")
                            or frozen.get("outcome_access") != "NOT_YET_ALLOWED"):
                        raise MaterializationIntegrityError("Frozen sampling design/lifecycle evidence cannot be reconciled.")
            elif spec.materializer_code_version != "phase-e2-materializer-v1" and connection.execute(
                "SELECT 1 FROM phase_e_materialization_sampling_design WHERE materialization_id=?",
                (row["materialization_id"],),
            ).fetchone() is not None:
                raise MaterializationIntegrityError("Sampling design exists before membership freeze.")

            selected_value = row["selected_count"]
            selected = int(selected_value) if selected_value is not None else -1
            expected_features = selected * len(spec.required_features) if selected >= 0 else 0
            if stage_index >= 4:
                feature_count, feature_fp = self._feature_fingerprint(connection, str(row["materialization_id"]))
                feature_event = parsed_events[4]["payload"]
                if (feature_count != expected_features
                        or feature_event != {"feature_count": feature_count, "feature_artifact_fingerprint": feature_fp}
                        or row["feature_artifact_fingerprint"] != feature_fp):
                    raise MaterializationIntegrityError("Feature-complete lifecycle evidence is false.")
            elif row["feature_artifact_fingerprint"] is not None:
                raise MaterializationIntegrityError("Feature projection was populated before feature completion.")
            if stage_index >= 5:
                outcome_count, outcome_fp = self._outcome_fingerprint(connection, str(row["materialization_id"]))
                outcome_event = parsed_events[5]["payload"]
                if (outcome_count != selected
                        or outcome_event != {"outcome_count": outcome_count, "outcome_artifact_fingerprint": outcome_fp}
                        or row["outcome_artifact_fingerprint"] != outcome_fp):
                    raise MaterializationIntegrityError("Outcome-complete lifecycle evidence is false.")
            elif row["outcome_artifact_fingerprint"] is not None:
                raise MaterializationIntegrityError("Outcome projection was populated before outcome completion.")
            if status is MaterializationStatus.COMPLETE:
                count, feature_fp = self._feature_fingerprint(connection, str(row["materialization_id"]))
                outcome_count, outcome_fp = self._outcome_fingerprint(connection, str(row["materialization_id"]))
                if count != selected * len(spec.required_features) or outcome_count != selected:
                    raise MaterializationIntegrityError("COMPLETE materialization lacks required artifacts.")
                complete_identity = {"membership": row["membership_fingerprint"], "features": feature_fp,
                                     "outcomes": outcome_fp, "specification_hash": row["specification_hash"]}
                if spec.materializer_code_version != "phase-e2-materializer-v1":
                    complete_identity["sampling_design"] = row["sampling_design_fingerprint"]
                completed = canonical_hash(complete_identity)
                completed_time_valid = (row["completed_at"] is not None and (
                    spec.materializer_code_version == "phase-e2-materializer-v1"
                    or (normalized_utc(str(row["completed_at"])) == row["completed_at"]
                        and row["completed_at"] == parsed_events[6]["event_at"])
                ))
                if (feature_fp != row["feature_artifact_fingerprint"] or outcome_fp != row["outcome_artifact_fingerprint"]
                        or completed != row["completed_artifact_fingerprint"]
                        or not completed_time_valid
                        or parsed_events[6]["payload"] != {
                            "selected_count": selected,
                            "membership_fingerprint": row["membership_fingerprint"],
                            **({"sampling_design_fingerprint": row["sampling_design_fingerprint"]}
                               if spec.materializer_code_version != "phase-e2-materializer-v1" else {}),
                            "feature_artifact_fingerprint": feature_fp,
                            "outcome_artifact_fingerprint": outcome_fp,
                            "completed_artifact_fingerprint": completed,
                        }):
                    raise MaterializationIntegrityError("COMPLETE projection lacks verified immutable artifact fingerprints.")
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise MaterializationIntegrityError(f"Malformed E.2 persisted state: {exc}") from exc

    # ----- small persistence helpers -------------------------------------------------
    def _insert_membership(self, materialization_id: str, candidates: Sequence[_Candidate]) -> None:
        for offset in range(0, len(candidates), self._BATCH_SIZE):
            batch = candidates[offset:offset + self._BATCH_SIZE]
            self._insert_membership_batch(materialization_id, batch, offset)

    def _insert_membership_batch(self, materialization_id: str, candidates: Sequence[_Candidate], start_ordinal: int) -> None:
        if not candidates:
            return
        with self._write() as connection:
            self._insert_membership_batch_connection(connection, materialization_id, candidates, start_ordinal)

    def _insert_membership_batch_connection(self, connection: sqlite3.Connection, materialization_id: str,
                                            candidates: Sequence[_Candidate], start_ordinal: int) -> None:
        for ordinal, candidate in enumerate(candidates, start=start_ordinal):
            item = candidate.membership_payload(ordinal)
            prior = connection.execute(
                "SELECT * FROM phase_e_materialization_membership WHERE materialization_id=? AND observation_id=?",
                (materialization_id, candidate.observation_id),
            ).fetchone()
            if prior is None:
                try:
                    connection.execute("INSERT INTO phase_e_materialization_membership VALUES (?, ?, ?, ?, ?, ?, ?)",
                                       (materialization_id, ordinal, candidate.observation_id, candidate.normalized_at,
                                        candidate.partition, candidate.stratum, candidate.selection_key))
                except sqlite3.IntegrityError as exc:
                    raise MaterializationIntegrityError("Membership ordinal collides with a divergent concurrent selection.") from exc
            elif self._member_payload(prior) != item:
                raise MaterializationIntegrityError("Existing membership conflicts with deterministic selection.")

    def _transition(self, connection: sqlite3.Connection, row: sqlite3.Row, to: MaterializationStatus, reason: str,
                    payload: Mapping[str, Any], projection: Mapping[str, Any] | None = None) -> None:
        current = MaterializationStatus(str(row["status"]))
        at = _now()
        assignments = {"status": to.value, **dict(projection or {})}
        if to is MaterializationStatus.COMPLETE:
            assignments["completed_at"] = at
        columns = ", ".join(f"{column}=?" for column in assignments)
        connection.execute(f"UPDATE phase_e_materializations SET {columns} WHERE materialization_id=? AND status=?",
                           (*assignments.values(), row["materialization_id"], current.value))
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise MaterializationConflictError("Concurrent materialization lifecycle transition lost its compare-and-swap.")
        self._append_event(connection, str(row["materialization_id"]), self._event_name(to), current, to, reason, at, payload)

    @staticmethod
    def _event_name(status: MaterializationStatus) -> str:
        return {
            MaterializationStatus.SELECTING: "SELECTING",
            MaterializationStatus.MEMBERSHIP_FROZEN: "MEMBERSHIP_FROZEN",
            MaterializationStatus.MATERIALIZING_FEATURES: "MATERIALIZING_FEATURES",
            MaterializationStatus.ATTACHING_OUTCOMES: "ATTACHING_OUTCOMES",
            MaterializationStatus.VERIFYING: "VERIFYING",
            MaterializationStatus.COMPLETE: "COMPLETE",
        }.get(status, status.value)

    def _append_event(self, connection: sqlite3.Connection, materialization_id: str, event_type: str,
                      from_status: MaterializationStatus | None, to_status: MaterializationStatus,
                      reason: str, event_at: str, payload: Mapping[str, Any]) -> None:
        body = dict(payload)
        payload_hash = canonical_hash(body)
        event_id = canonical_hash({"materialization_id": materialization_id, "event_type": event_type,
                                   "from_status": from_status.value if from_status else None,
                                   "to_status": to_status.value, "reason": reason,
                                   "event_at": event_at, "payload_hash": payload_hash})
        connection.execute("INSERT INTO phase_e_materialization_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                           (event_id, materialization_id, event_type, from_status.value if from_status else None,
                            to_status.value, reason, event_at, storage_json(body), payload_hash))

    def _selection_summary(self, connection: sqlite3.Connection, spec: MaterializationSpec) -> tuple[int, dict[str, int], dict[str, int], dict[str, int]]:
        partition: dict[str, int] = {}
        strata: dict[str, int] = {}
        # To expose the true eligible count after sampling, rescan only the
        # compact outcome-blind candidate fields.
        sql = """SELECT observation_id, event_at, normalized_at, kind, source, raw_fingerprint FROM science_observations
                 WHERE source=? AND phase_e_instant(normalized_at)>=? AND phase_e_instant(normalized_at)<?"""
        self._assert_outcome_free_sql(sql)
        eligible_count = 0
        excluded: dict[str, int] = {}
        for row in connection.execute(sql, (spec.eligibility.source, _sortable_instant(spec.source_universe.interval_start),
                                            _sortable_instant(spec.source_universe.interval_end))):
            candidate, reason = self._candidate_from_row(row, spec)
            if candidate is not None:
                eligible_count += 1
                partition[candidate.partition] = partition.get(candidate.partition, 0) + 1
                strata[candidate.stratum] = strata.get(candidate.stratum, 0) + 1
            else:
                key = reason or "INELIGIBLE"
                excluded[key] = excluded.get(key, 0) + 1
        return eligible_count, {key: excluded[key] for key in sorted(excluded)}, partition, strata

    def _guard_free_space(self, spec: MaterializationSpec) -> None:
        free = shutil.disk_usage(self.path.parent).free
        maximum_rows = (spec.source_universe.observation_count if spec.target_count is None
                        else min(spec.target_count, spec.source_universe.observation_count))
        estimated_artifacts = maximum_rows * (420 + len(spec.required_features) * 300)
        # Reserve a second estimated artifact footprint for SQLite WAL and
        # rollback headroom.  A later disk-full still rolls back the active
        # transaction and remains restartable.
        required = self.minimum_free_bytes + 2 * estimated_artifacts
        if free < required:
            raise OSError(f"E.2 materialization refuses to start: hot free space {free} is below guarded requirement {required}.")

    def _source_rows(self, connection: sqlite3.Connection, ids: Sequence[str]) -> list[sqlite3.Row]:
        if not ids:
            return []
        result: list[sqlite3.Row] = []
        for offset in range(0, len(ids), 400):
            part = ids[offset:offset + 400]
            result.extend(connection.execute("SELECT * FROM science_observations WHERE observation_id IN (" + ",".join("?" for _ in part) + ")", part).fetchall())
        by_id = {str(row["observation_id"]): row for row in result}
        return [by_id[item] for item in ids if item in by_id]

    def _source_fingerprint(self, connection: sqlite3.Connection, ids: Sequence[str]) -> str:
        return self._source_fingerprint_from_rows(self._source_rows(connection, ids))

    def _source_fingerprint_from_rows(self, rows: Sequence[sqlite3.Row]) -> str:
        return _ordered_fingerprint("phase-e2-feature-sources-v2", (
            self._validated_d_observation_identity(row) for row in rows
        ))

    @staticmethod
    def _member_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"ordinal": int(row["ordinal"]), "observation_id": row["observation_id"],
                "normalized_at": normalized_utc(str(row["normalized_at"])), "partition": row["partition_name"],
                "stratum": row["stratum_id"], "selection_key": row["selection_key"]}

    @staticmethod
    def _load_e_json(raw: Any, name: str) -> Any:
        if not isinstance(raw, str):
            raise MaterializationIntegrityError(f"{name} must be canonical JSON.")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MaterializationIntegrityError(f"{name} is malformed.") from exc
        if storage_json(value) != raw:
            raise MaterializationIntegrityError(f"{name} is not canonical E.2 JSON.")
        return value

    def _spec_from_row(self, row: sqlite3.Row) -> MaterializationSpec:
        payload = self._load_e_json(row["specification_json"], "materialization specification")
        if canonical_hash(payload) != row["specification_hash"]:
            raise MaterializationIntegrityError("Materialization specification hash is inconsistent.")
        try:
            source_raw = payload["source_universe"]
            source = SourceUniverseProvenance(**source_raw)
            partition_raw = payload["partition"]
            partition = PartitionIdentity(
                partition_id=partition_raw["partition_id"], train_start=partition_raw["train_start"], train_end=partition_raw["train_end"],
                validation_start=partition_raw["validation_start"], validation_end=partition_raw["validation_end"],
                test_start=partition_raw["test_start"], test_end=partition_raw["test_end"], purge_seconds=partition_raw["purge_seconds"],
                embargo_seconds=partition_raw["embargo_seconds"], random_seed=partition_raw["random_seed"],
                horizon=OutcomeHorizon(partition_raw["outcome_horizon"]["seconds"]),
                feature_lookback_seconds=partition_raw["feature_lookback_seconds"], sampling_algorithm=partition_raw["sampling_algorithm"],
                outcome_boundary_policy=partition_raw["outcome_boundary_policy"],
            )
            eligibility = EligibilitySpec(source=payload["eligibility"]["source"], kinds=tuple(payload["eligibility"]["kinds"]))
            features = tuple(FeatureReference(**item) for item in payload["required_features"])
            strat_raw = payload["stratification"]
            resolution_raw = payload.get("outcome_resolution", {})
            return MaterializationSpec(source_universe=source, partition=partition, eligibility=eligibility,
                                       required_features=features, outcome_horizon=OutcomeHorizon(payload["outcome_horizon"]["seconds"]),
                                       sampling_algorithm=payload["sampling_algorithm"], sampling_seed=payload["sampling_seed"],
                                       target_count=payload["target_count"], tier=payload["tier"], purpose=payload["purpose"],
                                       outcome_resolution=OutcomeResolutionSpec(
                                           policy=resolution_raw.get("policy", FIRST_TRADE_WITHIN_TOLERANCE_V1),
                                           maximum_lag_seconds=resolution_raw.get("maximum_lag_seconds", 5),
                                           start_price_policy=resolution_raw.get("start_price_policy", "ANCHOR_FILL_PRICE_V1"),
                                           tie_break_policy=resolution_raw.get("tie_break_policy", "EARLIEST_EVENT_AT_THEN_OBSERVATION_ID_V1"),
                                       ),
                                       stratification=StratificationSpec(kind=strat_raw["kind"], bucket_seconds=strat_raw["bucket_seconds"]),
                                       anchor_time_policy=payload.get("anchor_time_policy", HISTORICAL_EVENT_TIME_V1),
                                       feature_window_policy=payload.get("feature_window_policy", "SAME_PARTITION_LOOKBACK_V1"),
                                       ordering_policy=payload["ordering_policy"], missing_feature_policy=payload["missing_feature_policy"],
                                       missing_outcome_policy=payload["missing_outcome_policy"],
                                       materializer_code_version=payload["materializer_code_version"],
                                       materializer_config_version=payload["materializer_config_version"], schema_version=payload["schema_version"])
        except (KeyError, TypeError, ValueError) as exc:
            raise MaterializationIntegrityError("Persisted materialization specification is malformed.") from exc

    @staticmethod
    def _require(connection: sqlite3.Connection, materialization_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM phase_e_materializations WHERE materialization_id=?", (materialization_id,)).fetchone()
        if row is None:
            raise MaterializationConflictError(f"Unknown E.2 materialization: {materialization_id}")
        return row

    def _payload(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        memberships = int(connection.execute("SELECT COUNT(*) FROM phase_e_materialization_membership WHERE materialization_id=?", (row["materialization_id"],)).fetchone()[0])
        sampling_row = connection.execute(
            "SELECT design_json FROM phase_e_materialization_sampling_design WHERE materialization_id=?",
            (row["materialization_id"],),
        ).fetchone()
        return {
            "materialization_id": row["materialization_id"], "specification": self._load_e_json(row["specification_json"], "materialization specification"),
            "specification_hash": row["specification_hash"], "source_universe": self._load_e_json(row["source_universe_json"], "source universe"),
            "status": row["status"], "registered_at": row["registered_at"], "completed_at": row["completed_at"],
            "selected_count": row["selected_count"], "persisted_membership_count": memberships,
            "excluded_counts": self._load_e_json(row["excluded_counts_json"], "excluded counts") if row["excluded_counts_json"] else None,
            "membership_fingerprint": row["membership_fingerprint"],
            "sampling_design": (self._load_e_json(sampling_row["design_json"], "sampling design") if sampling_row else None),
            "sampling_design_fingerprint": row["sampling_design_fingerprint"],
            "feature_artifact_fingerprint": row["feature_artifact_fingerprint"],
            "outcome_artifact_fingerprint": row["outcome_artifact_fingerprint"],
            "completed_artifact_fingerprint": row["completed_artifact_fingerprint"],
            "byte_statistics": self._load_e_json(row["byte_statistics_json"], "byte statistics") if row["byte_statistics_json"] else None,
            "trading_authority": False, "qualified_signal": False,
        }
