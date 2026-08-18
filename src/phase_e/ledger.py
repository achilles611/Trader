"""Append-only Phase E experiment ledger.

Phase D remains the evidence owner.  This module only reads a D.7 corpus
snapshot and stores compact experiment contracts, lifecycle events, and
results in separate ``phase_e_*`` tables in the existing hot SQLite database.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from .types import (
    CANONICALIZATION_VERSION,
    ExperimentConclusion,
    ExperimentResult,
    ExperimentStatus,
    FeatureReference,
    HypothesisDefinition,
    PromotionState,
    RejectionReason,
    canonical_hash,
    finite_number,
    normalized_utc,
    storage_json,
)


class PhaseELedgerError(RuntimeError):
    pass


class CorpusProvenanceError(PhaseELedgerError):
    """The requested D corpus is absent, incomplete, or inconsistent."""


class ExperimentConflictError(PhaseELedgerError):
    """An immutable identity or lifecycle constraint was violated."""


class LedgerIntegrityError(PhaseELedgerError):
    """Persisted state disagrees with immutable scientific evidence."""


class UnknownExperimentError(PhaseELedgerError):
    pass


def _instant(value: str) -> datetime:
    """Compare canonical UTC text as time, never lexicographically."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


@dataclass(frozen=True)
class CorpusProvenance:
    corpus_fingerprint: str
    coverage_id: str
    coverage_state: str
    coverage_fraction: float
    interval_start: str
    interval_end: str
    observation_fingerprint: str
    source_code_version: str
    source_config_hash: str
    verified_observation_count: int
    feature_versions: tuple[FeatureReference, ...]
    source_snapshot: Mapping[str, Any]
    source_snapshot_hash: str

    def payload(self) -> dict[str, Any]:
        return {
            "canonicalization_version": CANONICALIZATION_VERSION,
            "corpus_fingerprint": self.corpus_fingerprint,
            "coverage_id": self.coverage_id,
            "coverage_state": self.coverage_state,
            "coverage_fraction": self.coverage_fraction,
            "interval_start": self.interval_start,
            "interval_end": self.interval_end,
            "observation_fingerprint": self.observation_fingerprint,
            "source_code_version": self.source_code_version,
            "source_config_hash": self.source_config_hash,
            "verified_observation_count": self.verified_observation_count,
            "feature_versions": [item.payload() for item in self.feature_versions],
            "source_snapshot": dict(self.source_snapshot),
            "source_snapshot_hash": self.source_snapshot_hash,
        }


def deterministic_experiment_id(definition: HypothesisDefinition, corpus: CorpusProvenance) -> str:
    """Derive identity from frozen science, excluding only registration time.

    The registration timestamp is persisted and immutable, but does not make a
    semantically identical predeclared experiment receive a different ID.
    """
    material = {
        "schema": "phase-e1-experiment-identity-v1",
        "canonicalization_version": CANONICALIZATION_VERSION,
        "hypothesis": definition.scientific_payload(),
        "corpus_fingerprint": corpus.corpus_fingerprint,
        "corpus_snapshot_hash": corpus.source_snapshot_hash,
    }
    return "e1-" + canonical_hash(material)[:32]


class PhaseELedger:
    """SQLite-backed immutable registration and lifecycle ledger for E.1."""

    TRADING_AUTHORITY = False
    _ACTIVE_STATES = {ExperimentStatus.REGISTERED, ExperimentStatus.RECOVERABLE}
    _TERMINAL_STATES = {ExperimentStatus.COMPLETED, ExperimentStatus.REJECTED, ExperimentStatus.FAILED}

    def __init__(self, database_path: str | Path) -> None:
        self.path = Path(database_path)
        self._initialized = False

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=10000")
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
        with self._write() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS phase_e_hypotheses (
                    hypothesis_id TEXT NOT NULL,
                    version INTEGER NOT NULL CHECK(version > 0),
                    definition_json TEXT NOT NULL,
                    definition_hash TEXT NOT NULL,
                    registered_at TEXT NOT NULL,
                    PRIMARY KEY(hypothesis_id, version)
                );
                CREATE TABLE IF NOT EXISTS phase_e_experiments (
                    experiment_id TEXT PRIMARY KEY,
                    hypothesis_id TEXT NOT NULL,
                    hypothesis_version INTEGER NOT NULL,
                    hypothesis_hash TEXT NOT NULL,
                    corpus_fingerprint TEXT NOT NULL,
                    corpus_provenance_json TEXT NOT NULL,
                    corpus_provenance_hash TEXT NOT NULL,
                    specification_json TEXT NOT NULL,
                    specification_hash TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('REGISTERED','RUNNING','RECOVERABLE','COMPLETED','REJECTED','FAILED')),
                    promotion_state TEXT NOT NULL CHECK(promotion_state IN ('NOT_ELIGIBLE','HISTORICAL_SURVIVOR','REJECTED')),
                    execution_attempts INTEGER NOT NULL DEFAULT 0 CHECK(execution_attempts >= 0),
                    registered_at TEXT NOT NULL,
                    started_at TEXT,
                    terminal_at TEXT,
                    FOREIGN KEY(hypothesis_id, hypothesis_version)
                        REFERENCES phase_e_hypotheses(hypothesis_id, version)
                );
                CREATE TABLE IF NOT EXISTS phase_e_experiment_events (
                    event_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL REFERENCES phase_e_experiments(experiment_id),
                    event_type TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT,
                    reason TEXT NOT NULL,
                    event_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e_experiment_results (
                    experiment_id TEXT PRIMARY KEY REFERENCES phase_e_experiments(experiment_id),
                    result_json TEXT NOT NULL,
                    result_hash TEXT NOT NULL,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e_promotion_events (
                    event_id TEXT PRIMARY KEY,
                    experiment_id TEXT NOT NULL REFERENCES phase_e_experiments(experiment_id),
                    from_state TEXT NOT NULL,
                    to_state TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    event_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_phase_e_experiments_status
                    ON phase_e_experiments(status, registered_at);
                CREATE INDEX IF NOT EXISTS idx_phase_e_events_experiment
                    ON phase_e_experiment_events(experiment_id, event_at);
                CREATE TRIGGER IF NOT EXISTS phase_e_hypotheses_immutable
                    BEFORE UPDATE ON phase_e_hypotheses
                    BEGIN SELECT RAISE(ABORT, 'Phase E hypothesis definition is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_hypotheses_no_delete
                    BEFORE DELETE ON phase_e_hypotheses
                    BEGIN SELECT RAISE(ABORT, 'Phase E hypothesis records cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_experiments_immutable
                    BEFORE UPDATE OF hypothesis_id, hypothesis_version, hypothesis_hash, corpus_fingerprint,
                        corpus_provenance_json, corpus_provenance_hash, specification_json, specification_hash,
                        registered_at ON phase_e_experiments
                    BEGIN SELECT RAISE(ABORT, 'Phase E experiment scientific inputs are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_experiments_no_delete
                    BEFORE DELETE ON phase_e_experiments
                    BEGIN SELECT RAISE(ABORT, 'Phase E experiment records cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_events_append_only_update
                    BEFORE UPDATE ON phase_e_experiment_events
                    BEGIN SELECT RAISE(ABORT, 'Phase E lifecycle events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_events_append_only_delete
                    BEFORE DELETE ON phase_e_experiment_events
                    BEGIN SELECT RAISE(ABORT, 'Phase E lifecycle events cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_results_append_only_update
                    BEFORE UPDATE ON phase_e_experiment_results
                    BEGIN SELECT RAISE(ABORT, 'Phase E results are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_results_append_only_delete
                    BEFORE DELETE ON phase_e_experiment_results
                    BEGIN SELECT RAISE(ABORT, 'Phase E results cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_promotion_events_append_only_update
                    BEFORE UPDATE ON phase_e_promotion_events
                    BEGIN SELECT RAISE(ABORT, 'Phase E promotion history is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e_promotion_events_append_only_delete
                    BEFORE DELETE ON phase_e_promotion_events
                    BEGIN SELECT RAISE(ABORT, 'Phase E promotion history cannot be deleted'); END;
                """
            )
        self._initialized = True

    def resolve_phase_d_corpus(self, corpus_fingerprint: str) -> CorpusProvenance:
        """Read a D.7 snapshot and fail closed on incomplete/corrupt provenance."""
        if not corpus_fingerprint.strip():
            raise CorpusProvenanceError("A Phase D corpus fingerprint is required.")
        try:
            with self._connection() as connection:
                row = connection.execute(
                    """SELECT snapshot.*,
                              coverage.coverage_id AS d_coverage_id,
                              coverage.interval_start AS d_interval_start,
                              coverage.interval_end AS d_interval_end,
                              coverage.source_name AS d_source_name,
                              coverage.state AS d_state,
                              coverage.coverage_fraction AS d_coverage_fraction,
                              coverage.expected_hours AS d_expected_hours,
                              coverage.verified_hours AS d_verified_hours,
                              coverage.missing_hours AS d_missing_hours,
                              coverage.malformed_hours AS d_malformed_hours,
                              coverage.parsed_hours AS d_parsed_hours,
                              coverage.observation_count AS d_observation_count,
                              coverage.duplicate_count AS d_duplicate_count,
                              coverage.timestamp_anomalies AS d_timestamp_anomalies,
                              coverage.first_event_at AS d_first_event_at,
                              coverage.last_event_at AS d_last_event_at,
                              coverage.wallet_attribution_quality AS d_wallet_attribution_quality,
                              coverage.market_evidence_availability AS d_market_evidence_availability,
                              coverage.details_json AS d_details_json,
                              coverage.computed_at AS d_computed_at
                       FROM science_corpus_snapshots AS snapshot
                       JOIN science_data_coverage AS coverage ON coverage.coverage_id=snapshot.coverage_id
                       WHERE snapshot.corpus_fingerprint=?""",
                    (corpus_fingerprint,),
                ).fetchone()
        except sqlite3.OperationalError as exc:
            raise CorpusProvenanceError("Phase D corpus tables are unavailable; E.1 refuses unproven evidence.") from exc
        if row is None:
            raise CorpusProvenanceError("Requested Phase D corpus snapshot does not exist.")
        payload = self._load_phase_d(row["payload_json"], "source corpus snapshot")
        if not isinstance(payload, Mapping):
            raise CorpusProvenanceError("Phase D corpus snapshot payload must be a JSON object.")
        coverage_details = self._load_phase_d(row["d_details_json"], "coverage details")
        payload_coverage = payload.get("coverage")
        try:
            coverage_fraction = finite_number(row["d_coverage_fraction"], name="Phase D coverage fraction", minimum=0.0, maximum=1.0)
            integer_names = (
                "expected_hours", "verified_hours", "missing_hours", "malformed_hours", "parsed_hours",
                "observation_count", "duplicate_count", "timestamp_anomalies",
            )
            coverage_integers: dict[str, int] = {}
            for name in integer_names:
                value = row[f"d_{name}"]
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ValueError(f"Phase D coverage {name} must be a nonnegative SQLite integer.")
                coverage_integers[name] = value
            if str(row["d_state"]) != "PROVEN_COMPLETE" or coverage_fraction != 1.0:
                raise ValueError("Phase E requires PROVEN_COMPLETE coverage with fraction exactly 1.0.")
            if (coverage_integers["expected_hours"] <= 0
                    or coverage_integers["verified_hours"] != coverage_integers["expected_hours"]
                    or coverage_integers["parsed_hours"] != coverage_integers["expected_hours"]
                    or any(coverage_integers[name] != 0 for name in ("missing_hours", "malformed_hours"))):
                raise ValueError("PROVEN_COMPLETE coverage has contradictory completeness counters.")
            features_raw = self._load_phase_d(row["feature_versions_json"], "feature versions")
            if not isinstance(features_raw, list):
                raise ValueError("Feature versions must be a JSON list.")
            parsed_features: list[FeatureReference] = []
            for item in features_raw:
                if not isinstance(item, Mapping) or set(item) != {"feature_id", "version"}:
                    raise ValueError("Each feature version must contain exactly feature_id and version.")
                feature_id, version = item["feature_id"], item["version"]
                if not isinstance(feature_id, str) or isinstance(version, bool) or not isinstance(version, int):
                    raise ValueError("Feature version types must not require coercion.")
                parsed_features.append(FeatureReference(feature_id, version))
            features = tuple(sorted(parsed_features, key=lambda item: (item.feature_id, item.version)))
            if len(features) != len({(item.feature_id, item.version) for item in features}):
                raise ValueError("Feature versions must be unique.")
            interval_start, interval_end = normalized_utc(str(row["interval_start"])), normalized_utc(str(row["interval_end"]))
            created_at = normalized_utc(str(row["created_at"]))
            computed_at = normalized_utc(str(row["d_computed_at"]))
            first_event_at = None if row["d_first_event_at"] is None else normalized_utc(str(row["d_first_event_at"]))
            last_event_at = None if row["d_last_event_at"] is None else normalized_utc(str(row["d_last_event_at"]))
            if (first_event_at is None) != (last_event_at is None):
                raise ValueError("Coverage must provide both first and last event timestamps or neither.")
            if first_event_at is not None:
                first, last = _instant(first_event_at), _instant(last_event_at)
                begin, finish = _instant(interval_start), _instant(interval_end)
                in_interval = begin <= first <= last < finish
                # D.7 retains a documented source event immediately before
                # the requested interval as provenance.  That row is not a
                # corpus member under the frozen end-exclusive interval rule.
                # Permit only this exact, auditable boundary form; any other
                # anomaly remains an integrity failure.
                documented_pre_interval_anomaly = (
                    coverage_integers["timestamp_anomalies"] == 1
                    and first < begin <= last < finish
                )
                if not in_interval and not documented_pre_interval_anomaly:
                    raise ValueError("Coverage event timestamps fall outside the end-exclusive corpus interval.")
                if in_interval and coverage_integers["timestamp_anomalies"] != 0:
                    raise ValueError("Coverage reports an unexplained in-interval timestamp anomaly.")
        except (KeyError, TypeError, ValueError) as exc:
            raise CorpusProvenanceError(f"Phase D corpus snapshot has malformed provenance: {exc}") from exc
        coverage_record = {
            "coverage_id": row["d_coverage_id"],
            "interval_start": row["d_interval_start"],
            "interval_end": row["d_interval_end"],
            "source_name": row["d_source_name"],
            "state": row["d_state"],
            "coverage_fraction": coverage_fraction,
            **coverage_integers,
            "first_event_at": first_event_at,
            "last_event_at": last_event_at,
            "wallet_attribution_quality": row["d_wallet_attribution_quality"],
            "market_evidence_availability": row["d_market_evidence_availability"],
            "details": coverage_details,
            "computed_at": computed_at,
        }
        source_feature_versions = [{"feature_id": item.feature_id, "version": item.version} for item in features]
        snapshot_columns = {
            "corpus_fingerprint": row["corpus_fingerprint"],
            "interval_start": interval_start,
            "interval_end": interval_end,
            "coverage_id": row["coverage_id"],
            "observation_fingerprint": row["observation_fingerprint"],
            "feature_versions": source_feature_versions,
            "symbols": self._load_phase_d(row["symbols_json"], "symbols"),
            "code_sha": row["code_sha"],
            "config_sha": row["config_sha"],
            "created_at": created_at,
        }
        for name, value in snapshot_columns.items():
            if payload.get(name) != value:
                raise CorpusProvenanceError(f"Phase D corpus payload conflicts with immutable {name} provenance.")
        if not isinstance(payload_coverage, Mapping):
            raise CorpusProvenanceError("Phase D corpus payload lacks its bound coverage record.")
        bound_coverage_record = dict(payload_coverage)
        try:
            bound_computed_at = normalized_utc(str(bound_coverage_record.get("computed_at")))
        except ValueError as exc:
            raise CorpusProvenanceError("Phase D corpus payload has an invalid bound coverage timestamp.") from exc
        # ``science_data_coverage`` is a mutable current coverage projection.
        # Its recomputation timestamp is operational metadata, while the
        # snapshot payload retains the immutable coverage evidence at the
        # moment the corpus was frozen.  Recalculation must not invalidate an
        # otherwise byte-for-byte identical scientific source; every semantic
        # coverage field remains exact and fail-closed.
        current_semantic_coverage = {key: value for key, value in coverage_record.items() if key != "computed_at"}
        bound_semantic_coverage = {key: value for key, value in bound_coverage_record.items() if key != "computed_at"}
        if current_semantic_coverage != bound_semantic_coverage:
            raise CorpusProvenanceError("Phase D corpus payload conflicts with current semantic coverage evidence.")
        bound_coverage_record["computed_at"] = bound_computed_at
        if (_instant(interval_start) >= _instant(interval_end)
                or bound_coverage_record["interval_start"] != interval_start
                or bound_coverage_record["interval_end"] != interval_end):
            raise CorpusProvenanceError("Phase D corpus and coverage intervals must match exactly and be ordered.")
        if not all(isinstance(value, str) and value for value in (row["observation_fingerprint"], row["code_sha"], row["config_sha"])):
            raise CorpusProvenanceError("Phase D corpus snapshot has missing provenance hashes.")

        feature_definitions: list[dict[str, Any]] = []
        try:
            with self._connection() as connection:
                for feature in features:
                    feature_row = connection.execute(
                        """SELECT definition_json, definition_hash, created_at, code_sha
                           FROM science_features WHERE feature_id=? AND version=?""",
                        (feature.feature_id, feature.version),
                    ).fetchone()
                    if feature_row is None:
                        raise CorpusProvenanceError(f"Phase D feature definition is missing: {feature.feature_id}@{feature.version}")
                    definition = self._load_phase_d(feature_row["definition_json"], "Phase D feature definition")
                    if self._phase_d_hash(definition) != feature_row["definition_hash"]:
                        raise CorpusProvenanceError(f"Phase D feature definition hash is corrupt: {feature.feature_id}@{feature.version}")
                    feature_definitions.append({
                        "feature_id": feature.feature_id,
                        "version": feature.version,
                        "definition": definition,
                        "definition_hash": feature_row["definition_hash"],
                        "created_at": feature_row["created_at"],
                        "code_sha": feature_row["code_sha"],
                    })
        except sqlite3.OperationalError as exc:
            raise CorpusProvenanceError("Phase D feature provenance is unavailable.") from exc
        source_snapshot = {
            "corpus_snapshot": payload,
            "coverage": bound_coverage_record,
            "snapshot_columns": snapshot_columns,
            "feature_definitions": feature_definitions,
        }
        return CorpusProvenance(
            corpus_fingerprint=str(row["corpus_fingerprint"]),
            coverage_id=str(row["coverage_id"]),
            coverage_state=str(row["d_state"]),
            coverage_fraction=coverage_fraction,
            interval_start=interval_start,
            interval_end=interval_end,
            observation_fingerprint=str(row["observation_fingerprint"]),
            source_code_version=str(row["code_sha"]),
            source_config_hash=str(row["config_sha"]),
            verified_observation_count=bound_coverage_record["observation_count"],
            feature_versions=features,
            source_snapshot=source_snapshot,
            source_snapshot_hash=canonical_hash(source_snapshot),
        )

    def register(self, definition: HypothesisDefinition, *, corpus_fingerprint: str, registered_at: str | None = None) -> dict[str, Any]:
        """Atomically register frozen hypothesis + experiment before evaluation."""
        self.initialize()
        provenance = self.resolve_phase_d_corpus(corpus_fingerprint)
        available_features = {(item.feature_id, item.version) for item in provenance.feature_versions}
        missing_features = [item.payload() for item in definition.required_features if (item.feature_id, item.version) not in available_features]
        if missing_features:
            raise CorpusProvenanceError(f"Phase D corpus does not declare the required features: {missing_features!r}")
        at = normalized_utc(registered_at or definition.created_at)
        experiment_id = deterministic_experiment_id(definition, provenance)
        definition_payload = definition.canonical_payload()
        definition_hash = definition.definition_hash
        provenance_payload, provenance_hash = provenance.payload(), canonical_hash(provenance.payload())
        specification = {
            "schema_version": "phase-e1",
            "canonicalization_version": CANONICALIZATION_VERSION,
            "experiment_id": experiment_id,
            "hypothesis": definition_payload,
            "hypothesis_hash": definition_hash,
            "corpus": provenance_payload,
        }
        specification_hash = canonical_hash(specification)
        with self._write() as connection:
            locked_provenance = self.resolve_phase_d_corpus(corpus_fingerprint)
            if canonical_hash(locked_provenance.payload()) != provenance_hash:
                raise CorpusProvenanceError("Phase D provenance changed during registration; retry against one exact snapshot.")
            prior_hypothesis = connection.execute(
                "SELECT definition_hash FROM phase_e_hypotheses WHERE hypothesis_id=? AND version=?",
                (definition.hypothesis_id, definition.version),
            ).fetchone()
            if prior_hypothesis is None:
                connection.execute(
                    "INSERT INTO phase_e_hypotheses VALUES (?, ?, ?, ?, ?)",
                    (definition.hypothesis_id, definition.version, storage_json(definition_payload), definition_hash, at),
                )
            elif prior_hypothesis["definition_hash"] != definition_hash:
                raise ExperimentConflictError("Hypothesis version already exists with different scientific criteria; create a new version.")
            prior = connection.execute("SELECT * FROM phase_e_experiments WHERE experiment_id=?", (experiment_id,)).fetchone()
            if prior is not None:
                if prior["specification_hash"] != specification_hash:
                    raise ExperimentConflictError("Deterministic experiment identity conflicts with different frozen inputs.")
                return self._experiment_payload(connection, prior)
            connection.execute(
                """INSERT INTO phase_e_experiments(
                    experiment_id, hypothesis_id, hypothesis_version, hypothesis_hash, corpus_fingerprint,
                    corpus_provenance_json, corpus_provenance_hash, specification_json, specification_hash,
                    status, promotion_state, registered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    experiment_id, definition.hypothesis_id, definition.version, definition_hash, provenance.corpus_fingerprint,
                    storage_json(provenance_payload), provenance_hash, storage_json(specification), specification_hash,
                    ExperimentStatus.REGISTERED.value, PromotionState.NOT_ELIGIBLE.value, at,
                ),
            )
            self._append_event(
                connection, experiment_id=experiment_id, event_type="REGISTERED", from_status=None,
                to_status=ExperimentStatus.REGISTERED, reason="predeclared_before_evaluation", event_at=at,
                payload={"attempt": 0, "specification_hash": specification_hash, "corpus_provenance_hash": provenance_hash}, attempt=0,
            )
            self._append_promotion_event(
                connection, experiment_id=experiment_id, from_state=PromotionState.NOT_ELIGIBLE,
                to_state=PromotionState.NOT_ELIGIBLE, reason="e1_registration_has_no_signal_authority", event_at=at,
                payload={"event_type": "REGISTRATION", "trading_authority": False},
            )
            row = connection.execute("SELECT * FROM phase_e_experiments WHERE experiment_id=?", (experiment_id,)).fetchone()
            assert row is not None
            return self._experiment_payload(connection, row)

    def start(self, experiment_id: str, *, started_at: str) -> dict[str, Any]:
        self.initialize()
        at = normalized_utc(started_at)
        with self._write() as connection:
            row = self._require_experiment(connection, experiment_id)
            self._validate_experiment_state(connection, row)
            status = self._status(row["status"])
            if status not in self._ACTIVE_STATES:
                raise ExperimentConflictError(f"Experiment {experiment_id} cannot start from {status.value}.")
            if _instant(at) < _instant(str(row["registered_at"])):
                raise ExperimentConflictError("Experiment cannot start before registration.")
            attempt = int(row["execution_attempts"]) + 1
            connection.execute(
                "UPDATE phase_e_experiments SET status=?, execution_attempts=?, started_at=? WHERE experiment_id=?",
                (ExperimentStatus.RUNNING.value, attempt, at, experiment_id),
            )
            self._append_event(
                connection, experiment_id=experiment_id, event_type="STARTED", from_status=status,
                to_status=ExperimentStatus.RUNNING, reason="evaluation_started", event_at=at,
                payload={"attempt": attempt}, attempt=attempt,
            )
            started = connection.execute("SELECT * FROM phase_e_experiments WHERE experiment_id=?", (experiment_id,)).fetchone()
            assert started is not None
            return self._experiment_payload(connection, started)

    def record_result(self, experiment_id: str, result: ExperimentResult, *, recorded_at: str) -> dict[str, Any]:
        self.initialize()
        at, result_payload = normalized_utc(recorded_at), result.payload()
        result_hash = canonical_hash(result_payload)
        destination = ExperimentStatus.COMPLETED if result.conclusion == ExperimentConclusion.SURVIVED else ExperimentStatus.REJECTED
        promotion = PromotionState.HISTORICAL_SURVIVOR if result.conclusion == ExperimentConclusion.SURVIVED else PromotionState.REJECTED
        with self._write() as connection:
            row = self._require_experiment(connection, experiment_id)
            self._validate_experiment_state(connection, row)
            if self._status(row["status"]) != ExperimentStatus.RUNNING:
                raise ExperimentConflictError("Only a running experiment can record its first result.")
            if row["started_at"] is None or _instant(at) < _instant(str(row["started_at"])):
                raise ExperimentConflictError("Experiment result cannot precede its running attempt.")
            self._verify_current_provenance_locked(row)
            prior = connection.execute("SELECT result_hash FROM phase_e_experiment_results WHERE experiment_id=?", (experiment_id,)).fetchone()
            if prior is not None:
                raise ExperimentConflictError("Experiment already has an immutable result.")
            connection.execute(
                "INSERT INTO phase_e_experiment_results VALUES (?, ?, ?, ?)",
                (experiment_id, storage_json(result_payload), result_hash, at),
            )
            connection.execute(
                "UPDATE phase_e_experiments SET status=?, promotion_state=?, terminal_at=? WHERE experiment_id=?",
                (destination.value, promotion.value, at, experiment_id),
            )
            attempt = int(row["execution_attempts"])
            self._append_event(
                connection, experiment_id=experiment_id,
                event_type="COMPLETED" if destination == ExperimentStatus.COMPLETED else "REJECTED",
                from_status=ExperimentStatus.RUNNING, to_status=destination,
                reason=result.rejection_reason.value if result.rejection_reason else "predeclared_success_threshold_met",
                event_at=at, payload={"attempt": attempt, "result_hash": result_hash, "conclusion": result.conclusion.value}, attempt=attempt,
            )
            self._append_promotion_event(
                connection, experiment_id=experiment_id, from_state=PromotionState.NOT_ELIGIBLE,
                to_state=promotion, reason="historical_result_only_no_signal_authority", event_at=at,
                payload={"event_type": "HISTORICAL_RESULT", "attempt": attempt, "result_hash": result_hash, "trading_authority": False},
            )
            complete = connection.execute("SELECT * FROM phase_e_experiments WHERE experiment_id=?", (experiment_id,)).fetchone()
            assert complete is not None
            return self._experiment_payload(connection, complete)

    def fail(self, experiment_id: str, *, reason: str, failed_at: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        self.initialize()
        at = normalized_utc(failed_at)
        safe_payload = dict(payload or {})
        with self._write() as connection:
            row = self._require_experiment(connection, experiment_id)
            self._validate_experiment_state(connection, row)
            if self._status(row["status"]) != ExperimentStatus.RUNNING:
                raise ExperimentConflictError("Only a running experiment can fail.")
            if row["started_at"] is None or _instant(at) < _instant(str(row["started_at"])):
                raise ExperimentConflictError("Experiment failure cannot precede its running attempt.")
            attempt = int(row["execution_attempts"])
            connection.execute(
                "UPDATE phase_e_experiments SET status=?, terminal_at=? WHERE experiment_id=?",
                (ExperimentStatus.FAILED.value, at, experiment_id),
            )
            self._append_event(
                connection, experiment_id=experiment_id, event_type="FAILED", from_status=ExperimentStatus.RUNNING,
                to_status=ExperimentStatus.FAILED, reason=reason[:300], event_at=at,
                payload={"attempt": attempt, **safe_payload}, attempt=attempt,
            )
            failed = connection.execute("SELECT * FROM phase_e_experiments WHERE experiment_id=?", (experiment_id,)).fetchone()
            assert failed is not None
            return self._experiment_payload(connection, failed)

    def recover_interrupted(self, experiment_id: str, *, recovered_at: str) -> dict[str, Any]:
        """Record a restart explicitly; it never fabricates partial success."""
        self.initialize()
        at = normalized_utc(recovered_at)
        with self._write() as connection:
            row = self._require_experiment(connection, experiment_id)
            self._validate_experiment_state(connection, row)
            status = self._status(row["status"])
            if status != ExperimentStatus.RUNNING:
                raise ExperimentConflictError("Only an interrupted RUNNING experiment needs recovery.")
            if row["started_at"] is None or _instant(at) < _instant(str(row["started_at"])):
                raise ExperimentConflictError("Recovery evidence cannot precede its running attempt.")
            attempt = int(row["execution_attempts"])
            connection.execute("UPDATE phase_e_experiments SET status=? WHERE experiment_id=?", (ExperimentStatus.RECOVERABLE.value, experiment_id))
            self._append_event(
                connection, experiment_id=experiment_id, event_type="RECOVERY_REQUIRED", from_status=status,
                to_status=ExperimentStatus.RECOVERABLE, reason="restart_detected_before_result", event_at=at,
                payload={"attempt": attempt, "partial_result_discarded": False, "trading_authority": False}, attempt=attempt,
            )
            recovered = connection.execute("SELECT * FROM phase_e_experiments WHERE experiment_id=?", (experiment_id,)).fetchone()
            assert recovered is not None
            return self._experiment_payload(connection, recovered)

    def request_promotion(self, experiment_id: str, *, requested_at: str, reason: str = "operator_request") -> dict[str, Any]:
        """Persist a denied request. E.1 has no promotion/signal capability."""
        self.initialize()
        at = normalized_utc(requested_at)
        with self._write() as connection:
            row = self._require_experiment(connection, experiment_id)
            self._validate_experiment_state(connection, row)
            state = PromotionState(str(row["promotion_state"]))
            if _instant(at) < _instant(str(row["registered_at"])):
                raise ExperimentConflictError("Promotion requests cannot precede registration.")
            self._append_promotion_event(
                connection, experiment_id=experiment_id, from_state=state, to_state=state,
                reason=f"denied_e1_no_signal_authority:{reason[:200]}", event_at=at,
                payload={"event_type": "DENIED_REQUEST", "attempt": int(row["execution_attempts"]), "trading_authority": False, "signal_created": False},
            )
            current = connection.execute(
                "SELECT * FROM phase_e_experiments WHERE experiment_id=?", (experiment_id,),
            ).fetchone()
            assert current is not None
            self._validate_experiment_state(connection, current)
        return {"experiment_id": experiment_id, "promotion_state": state.value, "qualified_signal": False, "trading_authority": False}

    def get(self, experiment_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = self._require_experiment(connection, experiment_id)
            return self._experiment_payload(connection, row)

    def list(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute("SELECT * FROM phase_e_experiments").fetchall()
            rows = sorted(rows, key=lambda row: (_instant(str(row["registered_at"])), str(row["experiment_id"])))
            return [self._experiment_payload(connection, row) for row in rows]

    def events(self, experiment_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            experiment = self._require_experiment(connection, experiment_id)
            self._validate_experiment_state(connection, experiment)
            rows = connection.execute("SELECT * FROM phase_e_experiment_events WHERE experiment_id=?", (experiment_id,)).fetchall()
            rows = sorted(rows, key=lambda row: (_instant(str(row["event_at"])), str(row["event_id"])))
        return [
            {
                "event_id": row["event_id"], "event_type": row["event_type"], "from_status": row["from_status"],
                "to_status": row["to_status"], "reason": row["reason"], "event_at": row["event_at"],
                "payload": self._load(row["payload_json"], "event payload"), "payload_hash": row["payload_hash"],
            }
            for row in rows
        ]

    def promotion_history(self, experiment_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            experiment = self._require_experiment(connection, experiment_id)
            self._validate_experiment_state(connection, experiment)
            rows = connection.execute("SELECT * FROM phase_e_promotion_events WHERE experiment_id=?", (experiment_id,)).fetchall()
            rows = sorted(rows, key=lambda row: (_instant(str(row["event_at"])), str(row["event_id"])))
        return [
            {
                "event_id": row["event_id"], "from_state": row["from_state"], "to_state": row["to_state"],
                "reason": row["reason"], "event_at": row["event_at"],
                "payload": self._load(row["payload_json"], "promotion payload"), "payload_hash": row["payload_hash"],
            }
            for row in rows
        ]

    def verify_current_provenance(self, experiment_id: str) -> None:
        """Verify the D snapshot still exactly matches its bound E provenance."""
        experiment = self.get(experiment_id)
        frozen = experiment["corpus_provenance"]
        current = self.resolve_phase_d_corpus(str(frozen["corpus_fingerprint"]))
        if canonical_hash(current.payload()) != experiment["corpus_provenance_hash"]:
            raise CorpusProvenanceError("Current Phase D corpus no longer matches the provenance frozen into this experiment.")

    def _append_event(
        self, connection: sqlite3.Connection, *, experiment_id: str, event_type: str,
        from_status: ExperimentStatus | None, to_status: ExperimentStatus, reason: str,
        event_at: str, payload: Mapping[str, Any], attempt: int,
    ) -> None:
        body, digest = dict(payload), canonical_hash(payload)
        event_id = canonical_hash({
            "experiment_id": experiment_id,
            "event_type": event_type,
            "attempt": attempt,
            "from_status": from_status.value if from_status else None,
            "to_status": to_status.value,
            "reason": reason,
            "event_at": event_at,
            "payload_hash": digest,
        })
        connection.execute(
            "INSERT INTO phase_e_experiment_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (event_id, experiment_id, event_type, from_status.value if from_status else None, to_status.value,
             reason, event_at, storage_json(body), digest),
        )

    def _append_promotion_event(
        self, connection: sqlite3.Connection, *, experiment_id: str, from_state: PromotionState,
        to_state: PromotionState, reason: str, event_at: str, payload: Mapping[str, Any],
    ) -> None:
        body, digest = dict(payload), canonical_hash(payload)
        event_id = canonical_hash({"experiment_id": experiment_id, "from": from_state.value, "to": to_state.value, "reason": reason, "event_at": event_at, "digest": digest})
        connection.execute(
            "INSERT OR IGNORE INTO phase_e_promotion_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (event_id, experiment_id, from_state.value, to_state.value, reason, event_at, storage_json(body), digest),
        )

    def _validate_experiment_state(self, connection: sqlite3.Connection, row: sqlite3.Row) -> None:
        """Reconcile the mutable projection against append-only evidence.

        SQLite CHECK constraints constrain vocabulary, not lifecycle truth.
        Every read and transition therefore derives the only legitimate state
        from the immutable definition, events, result, and promotion history.
        """
        experiment_id = str(row["experiment_id"])
        try:
            status = self._status(str(row["status"]))
            promotion_state = PromotionState(str(row["promotion_state"]))
            registered_at = self._canonical_persisted_time(row["registered_at"], "registration")
            attempts = row["execution_attempts"]
            if isinstance(attempts, bool) or not isinstance(attempts, int) or attempts < 0:
                raise LedgerIntegrityError("Execution-attempt projection is not a nonnegative integer.")

            hypothesis_row = connection.execute(
                """SELECT definition_json, definition_hash, registered_at
                   FROM phase_e_hypotheses WHERE hypothesis_id=? AND version=?""",
                (row["hypothesis_id"], row["hypothesis_version"]),
            ).fetchone()
            if hypothesis_row is None:
                raise LedgerIntegrityError("Experiment references a missing hypothesis version.")
            definition = self._mapping(self._load_phase_e(hypothesis_row["definition_json"], "hypothesis definition"), "hypothesis definition")
            if canonical_hash(definition) != hypothesis_row["definition_hash"] or hypothesis_row["definition_hash"] != row["hypothesis_hash"]:
                raise LedgerIntegrityError("Hypothesis definition/hash evidence is inconsistent.")
            self._canonical_persisted_time(hypothesis_row["registered_at"], "hypothesis registration")

            provenance = self._mapping(self._load_phase_e(row["corpus_provenance_json"], "corpus provenance"), "corpus provenance")
            specification = self._mapping(self._load_phase_e(row["specification_json"], "experiment specification"), "experiment specification")
            if canonical_hash(provenance) != row["corpus_provenance_hash"]:
                raise LedgerIntegrityError("Corpus provenance hash does not match persisted content.")
            source_snapshot = self._mapping(provenance.get("source_snapshot"), "source snapshot")
            if canonical_hash(source_snapshot) != provenance.get("source_snapshot_hash"):
                raise LedgerIntegrityError("Bound Phase D source-snapshot hash is inconsistent.")
            if canonical_hash(specification) != row["specification_hash"]:
                raise LedgerIntegrityError("Experiment specification hash does not match persisted content.")
            if (specification.get("schema_version") != "phase-e1"
                    or specification.get("canonicalization_version") != CANONICALIZATION_VERSION
                    or provenance.get("canonicalization_version") != CANONICALIZATION_VERSION
                    or specification.get("experiment_id") != experiment_id):
                raise LedgerIntegrityError("Experiment specification identity/schema is inconsistent.")
            if specification.get("hypothesis") != definition or specification.get("hypothesis_hash") != row["hypothesis_hash"]:
                raise LedgerIntegrityError("Experiment and hypothesis definitions disagree.")
            if specification.get("corpus") != provenance or provenance.get("corpus_fingerprint") != row["corpus_fingerprint"]:
                raise LedgerIntegrityError("Experiment and corpus provenance disagree.")
            expected_id = "e1-" + canonical_hash({
                "schema": "phase-e1-experiment-identity-v1",
                "canonicalization_version": CANONICALIZATION_VERSION,
                "hypothesis": definition,
                "corpus_fingerprint": provenance.get("corpus_fingerprint"),
                "corpus_snapshot_hash": provenance.get("source_snapshot_hash"),
            })[:32]
            if expected_id != experiment_id:
                raise LedgerIntegrityError("Deterministic experiment identity does not match its frozen inputs.")

            result_row = connection.execute(
                "SELECT * FROM phase_e_experiment_results WHERE experiment_id=?", (experiment_id,),
            ).fetchone()
            result = None
            if result_row is not None:
                result = self._validate_result_row(result_row)
                self._validate_result_contract(result, specification, provenance)

            event_rows = connection.execute(
                "SELECT * FROM phase_e_experiment_events WHERE experiment_id=?", (experiment_id,),
            ).fetchall()
            events: dict[tuple[int, str], dict[str, Any]] = {}
            for event_row in event_rows:
                payload = self._mapping(self._load_phase_e(event_row["payload_json"], "lifecycle event payload"), "lifecycle event payload")
                if canonical_hash(payload) != event_row["payload_hash"]:
                    raise LedgerIntegrityError("Lifecycle event payload hash mismatch.")
                attempt = payload.get("attempt")
                if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 0:
                    raise LedgerIntegrityError("Lifecycle event has an invalid attempt identity.")
                event_at = self._canonical_persisted_time(event_row["event_at"], "lifecycle event")
                event_type = str(event_row["event_type"])
                expected_event_id = canonical_hash({
                    "experiment_id": experiment_id,
                    "event_type": event_type,
                    "attempt": attempt,
                    "from_status": event_row["from_status"],
                    "to_status": event_row["to_status"],
                    "reason": event_row["reason"],
                    "event_at": event_at,
                    "payload_hash": event_row["payload_hash"],
                })
                if expected_event_id != event_row["event_id"]:
                    raise LedgerIntegrityError("Lifecycle event identity does not match its content.")
                key = (attempt, event_type)
                if key in events:
                    raise LedgerIntegrityError("Duplicated semantic lifecycle transition detected.")
                events[key] = {
                    "event_type": event_type,
                    "from_status": event_row["from_status"],
                    "to_status": event_row["to_status"],
                    "reason": event_row["reason"],
                    "event_at": event_at,
                    "payload": payload,
                }

            registration = events.pop((0, "REGISTERED"), None)
            if registration is None or registration["from_status"] is not None or registration["to_status"] != ExperimentStatus.REGISTERED.value:
                raise LedgerIntegrityError("Exactly one valid registration event is required.")
            if registration["reason"] != "predeclared_before_evaluation" or registration["event_at"] != registered_at:
                raise LedgerIntegrityError("Registration projection/event evidence disagrees.")
            if registration["payload"] != {
                "attempt": 0,
                "specification_hash": row["specification_hash"],
                "corpus_provenance_hash": row["corpus_provenance_hash"],
            }:
                raise LedgerIntegrityError("Registration event does not bind the frozen specification and corpus.")

            timeline = [registered_at]
            last_started_at: str | None = None
            terminal_event: dict[str, Any] | None = None
            for attempt in range(1, attempts + 1):
                started = events.pop((attempt, "STARTED"), None)
                expected_from = ExperimentStatus.REGISTERED.value if attempt == 1 else ExperimentStatus.RECOVERABLE.value
                if (started is None or started["from_status"] != expected_from
                        or started["to_status"] != ExperimentStatus.RUNNING.value
                        or started["reason"] != "evaluation_started"
                        or started["payload"] != {"attempt": attempt}):
                    raise LedgerIntegrityError("Execution attempt lacks its exact STARTED transition.")
                timeline.append(started["event_at"])
                last_started_at = started["event_at"]
                close_types = [name for name in ("RECOVERY_REQUIRED", "COMPLETED", "REJECTED", "FAILED") if (attempt, name) in events]
                if attempt < attempts:
                    if close_types != ["RECOVERY_REQUIRED"]:
                        raise LedgerIntegrityError("Every superseded execution attempt requires one recovery event.")
                    recovery = events.pop((attempt, "RECOVERY_REQUIRED"))
                    self._validate_recovery_event(recovery, attempt)
                    timeline.append(recovery["event_at"])
                elif status == ExperimentStatus.RECOVERABLE:
                    if close_types != ["RECOVERY_REQUIRED"]:
                        raise LedgerIntegrityError("RECOVERABLE projection requires a final recovery event.")
                    recovery = events.pop((attempt, "RECOVERY_REQUIRED"))
                    self._validate_recovery_event(recovery, attempt)
                    timeline.append(recovery["event_at"])
                elif status in self._TERMINAL_STATES:
                    expected_type = status.value
                    if close_types != [expected_type]:
                        raise LedgerIntegrityError("Terminal projection lacks its exact terminal lifecycle event.")
                    terminal_event = events.pop((attempt, expected_type))
                    if terminal_event["from_status"] != ExperimentStatus.RUNNING.value or terminal_event["to_status"] != status.value:
                        raise LedgerIntegrityError("Terminal lifecycle transition is invalid.")
                    timeline.append(terminal_event["event_at"])
                elif close_types:
                    raise LedgerIntegrityError("RUNNING projection contains an impossible terminal/recovery event.")

            if status == ExperimentStatus.REGISTERED and attempts != 0:
                raise LedgerIntegrityError("REGISTERED projection cannot have execution attempts.")
            if status != ExperimentStatus.REGISTERED and attempts == 0:
                raise LedgerIntegrityError("Non-registered projection requires an execution attempt.")
            if events:
                raise LedgerIntegrityError("Unexpected or extra lifecycle events detected.")
            if [_instant(item) for item in timeline] != sorted(_instant(item) for item in timeline):
                raise LedgerIntegrityError("Lifecycle event timestamps are reordered.")
            if row["started_at"] != last_started_at:
                raise LedgerIntegrityError("started_at projection disagrees with the latest STARTED event.")

            if status in self._TERMINAL_STATES:
                if terminal_event is None or row["terminal_at"] != terminal_event["event_at"]:
                    raise LedgerIntegrityError("terminal_at projection disagrees with terminal evidence.")
            elif row["terminal_at"] is not None:
                raise LedgerIntegrityError("Nonterminal experiment cannot carry terminal_at.")

            if status in {ExperimentStatus.COMPLETED, ExperimentStatus.REJECTED}:
                if result is None or terminal_event is None:
                    raise LedgerIntegrityError("Completed/rejected experiment requires one immutable result.")
                expected_conclusion = ExperimentConclusion.SURVIVED.value if status == ExperimentStatus.COMPLETED else ExperimentConclusion.REJECTED.value
                if result["conclusion"] != expected_conclusion:
                    raise LedgerIntegrityError("Result conclusion conflicts with terminal status.")
                if terminal_event["payload"] != {
                    "attempt": attempts, "result_hash": result_row["result_hash"], "conclusion": expected_conclusion,
                }:
                    raise LedgerIntegrityError("Terminal event does not bind the immutable result.")
                expected_reason = result["rejection_reason"] or "predeclared_success_threshold_met"
                if terminal_event["reason"] != expected_reason or result_row["recorded_at"] != row["terminal_at"]:
                    raise LedgerIntegrityError("Result timestamp/reason conflicts with terminal evidence.")
            elif result is not None:
                raise LedgerIntegrityError("Result exists without a completed/rejected terminal state.")

            promotion_rows = connection.execute(
                "SELECT * FROM phase_e_promotion_events WHERE experiment_id=?", (experiment_id,),
            ).fetchall()
            expected_promotion = (
                PromotionState.HISTORICAL_SURVIVOR if status == ExperimentStatus.COMPLETED
                else PromotionState.REJECTED if status == ExperimentStatus.REJECTED
                else PromotionState.NOT_ELIGIBLE
            )
            registration_promotions = 0
            result_promotions = 0
            for promotion_row in promotion_rows:
                promotion_payload = self._mapping(self._load_phase_e(promotion_row["payload_json"], "promotion event payload"), "promotion event payload")
                if canonical_hash(promotion_payload) != promotion_row["payload_hash"]:
                    raise LedgerIntegrityError("Promotion event payload hash mismatch.")
                promotion_at = self._canonical_persisted_time(promotion_row["event_at"], "promotion event")
                expected_promotion_id = canonical_hash({
                    "experiment_id": experiment_id, "from": promotion_row["from_state"], "to": promotion_row["to_state"],
                    "reason": promotion_row["reason"], "event_at": promotion_at, "digest": promotion_row["payload_hash"],
                })
                if expected_promotion_id != promotion_row["event_id"]:
                    raise LedgerIntegrityError("Promotion event identity does not match its content.")
                event_type = promotion_payload.get("event_type")
                if event_type == "REGISTRATION":
                    registration_promotions += 1
                    if (promotion_row["from_state"] != PromotionState.NOT_ELIGIBLE.value
                            or promotion_row["to_state"] != PromotionState.NOT_ELIGIBLE.value
                            or promotion_row["reason"] != "e1_registration_has_no_signal_authority"
                            or promotion_at != registered_at
                            or promotion_payload != {"event_type": "REGISTRATION", "trading_authority": False}):
                        raise LedgerIntegrityError("Registration promotion evidence is invalid.")
                elif event_type == "HISTORICAL_RESULT":
                    result_promotions += 1
                    if (result_row is None or promotion_row["from_state"] != PromotionState.NOT_ELIGIBLE.value
                            or promotion_row["to_state"] != expected_promotion.value
                            or promotion_row["reason"] != "historical_result_only_no_signal_authority"
                            or promotion_at != row["terminal_at"]
                            or promotion_payload.get("result_hash") != result_row["result_hash"]
                            or promotion_payload.get("attempt") != attempts
                            or promotion_payload.get("trading_authority") is not False):
                        raise LedgerIntegrityError("Historical-result promotion evidence is invalid.")
                elif event_type == "DENIED_REQUEST":
                    if (promotion_row["from_state"] != promotion_row["to_state"]
                            or not str(promotion_row["reason"]).startswith("denied_e1_no_signal_authority:")
                            or _instant(promotion_at) < _instant(registered_at)
                            or set(promotion_payload) != {"event_type", "attempt", "trading_authority", "signal_created"}
                            or isinstance(promotion_payload.get("attempt"), bool)
                            or not isinstance(promotion_payload.get("attempt"), int)
                            or not 0 <= promotion_payload["attempt"] <= attempts
                            or promotion_payload.get("trading_authority") is not False
                            or promotion_payload.get("signal_created") is not False):
                        raise LedgerIntegrityError("Denied promotion-request evidence is invalid.")
                    allowed_states = {PromotionState.NOT_ELIGIBLE.value}
                    if (row["terminal_at"] is not None
                            and _instant(promotion_at) >= _instant(str(row["terminal_at"]))):
                        allowed_states.add(expected_promotion.value)
                    if promotion_row["from_state"] not in allowed_states:
                        raise LedgerIntegrityError("Denied promotion request uses a forged promotion state.")
                else:
                    raise LedgerIntegrityError("Unknown promotion event semantics.")
            if registration_promotions != 1:
                raise LedgerIntegrityError("Exactly one registration promotion event is required.")
            expected_result_promotions = 1 if status in {ExperimentStatus.COMPLETED, ExperimentStatus.REJECTED} else 0
            if result_promotions != expected_result_promotions or promotion_state != expected_promotion:
                raise LedgerIntegrityError("Promotion projection/history disagrees with experiment evidence.")
        except LedgerIntegrityError:
            raise
        except CorpusProvenanceError as exc:
            raise LedgerIntegrityError(str(exc)) from exc
        except (KeyError, TypeError, ValueError) as exc:
            raise LedgerIntegrityError(f"Malformed Phase E ledger evidence: {exc}") from exc

    @staticmethod
    def _validate_recovery_event(event: Mapping[str, Any], attempt: int) -> None:
        if (event["from_status"] != ExperimentStatus.RUNNING.value
                or event["to_status"] != ExperimentStatus.RECOVERABLE.value
                or event["reason"] != "restart_detected_before_result"
                or event["payload"] != {"attempt": attempt, "partial_result_discarded": False, "trading_authority": False}):
            raise LedgerIntegrityError("Recovery lifecycle evidence is invalid.")

    def _validate_result_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = self._mapping(self._load_phase_e(row["result_json"], "experiment result"), "experiment result")
        if canonical_hash(payload) != row["result_hash"]:
            raise LedgerIntegrityError("Experiment result hash does not match persisted content.")
        expected_keys = {"sample_count", "effect_size", "p_value", "confidence_interval", "statistic", "conclusion", "rejection_reason"}
        if set(payload) != expected_keys or not isinstance(payload["confidence_interval"], list) or len(payload["confidence_interval"]) != 2:
            raise LedgerIntegrityError("Experiment result document has an unknown/missing field.")
        try:
            result = ExperimentResult(
                sample_count=payload["sample_count"],
                effect_size=payload["effect_size"],
                p_value=payload["p_value"],
                confidence_interval_low=payload["confidence_interval"][0],
                confidence_interval_high=payload["confidence_interval"][1],
                statistic=self._mapping(payload["statistic"], "result statistic"),
                conclusion=ExperimentConclusion(payload["conclusion"]),
                rejection_reason=None if payload["rejection_reason"] is None else RejectionReason(payload["rejection_reason"]),
            )
        except (TypeError, ValueError) as exc:
            raise LedgerIntegrityError(f"Experiment result document is malformed: {exc}") from exc
        if result.payload() != payload:
            raise LedgerIntegrityError("Experiment result contains coerced or noncanonical values.")
        self._canonical_persisted_time(row["recorded_at"], "result")
        return payload

    @staticmethod
    def _validate_result_contract(
        payload: Mapping[str, Any], specification: Mapping[str, Any], provenance: Mapping[str, Any],
    ) -> None:
        """Independently validate the only executable E.1 statistic.

        A caller cannot turn an arbitrary result document into a survivor by
        bypassing ``NullExperimentRunner`` and calling the ledger directly.
        """
        definition = specification.get("hypothesis")
        if not isinstance(definition, Mapping):
            raise LedgerIntegrityError("Result lacks a valid frozen hypothesis contract.")
        statistical_test = definition.get("statistical_test")
        if not isinstance(statistical_test, Mapping) or statistical_test.get("name") != "DETERMINISTIC_NULL_EFFECT":
            raise LedgerIntegrityError("E.1 can record only its predeclared deterministic null statistic.")
        sample_count = provenance.get("verified_observation_count")
        minimum_sample_size = definition.get("minimum_sample_size")
        if (isinstance(sample_count, bool) or not isinstance(sample_count, int)
                or isinstance(minimum_sample_size, bool) or not isinstance(minimum_sample_size, int)):
            raise LedgerIntegrityError("Null-result sample contract is malformed.")
        reason = (
            RejectionReason.INSUFFICIENT_SAMPLE
            if sample_count < minimum_sample_size
            else RejectionReason.NULL_HYPOTHESIS_NOT_REJECTED
        )
        expected = ExperimentResult(
            sample_count=sample_count,
            effect_size=0.0,
            p_value=1.0,
            confidence_interval_low=0.0,
            confidence_interval_high=0.0,
            statistic={
                "name": "DETERMINISTIC_NULL_EFFECT",
                "method_version": definition.get("code_version"),
                "canonicalization_version": CANONICALIZATION_VERSION,
                "corpus_provenance_hash": canonical_hash(provenance),
                "specification_hash": canonical_hash(specification),
                "minimum_sample_size": minimum_sample_size,
            },
            conclusion=ExperimentConclusion.REJECTED,
            rejection_reason=reason,
        ).payload()
        if payload != expected:
            raise LedgerIntegrityError("Persisted result does not match the predeclared deterministic E.1 result contract.")

    def _verify_current_provenance_locked(self, row: sqlite3.Row) -> None:
        """Verify D while the caller's BEGIN IMMEDIATE excludes D writers."""
        current = self.resolve_phase_d_corpus(str(row["corpus_fingerprint"]))
        if canonical_hash(current.payload()) != row["corpus_provenance_hash"]:
            raise CorpusProvenanceError("Phase D provenance changed before result commit; E.1 refuses the result.")

    @staticmethod
    def _load_phase_e(raw: Any, name: str) -> Any:
        if not isinstance(raw, str):
            raise LedgerIntegrityError(f"Persisted {name} must be canonical JSON text.")
        try:
            value = json.loads(raw)
            if storage_json(value) != raw:
                raise LedgerIntegrityError(f"Persisted {name} is not canonical storage JSON.")
            return value
        except (json.JSONDecodeError, ValueError) as exc:
            raise LedgerIntegrityError(f"Malformed persisted {name}; failing closed.") from exc

    @staticmethod
    def _load_phase_d(raw: Any, name: str) -> Any:
        if not isinstance(raw, str):
            raise CorpusProvenanceError(f"Persisted Phase D {name} must be canonical JSON text.")
        try:
            value = json.loads(raw)
            canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
            if canonical != raw:
                raise CorpusProvenanceError(f"Persisted Phase D {name} is not canonical storage JSON.")
            return value
        except (json.JSONDecodeError, ValueError) as exc:
            raise CorpusProvenanceError(f"Malformed persisted Phase D {name}; failing closed.") from exc

    @staticmethod
    def _mapping(value: Any, name: str) -> Mapping[str, Any]:
        if not isinstance(value, Mapping):
            raise LedgerIntegrityError(f"Persisted {name} must be a JSON object.")
        return value

    @staticmethod
    def _canonical_persisted_time(value: Any, name: str) -> str:
        if not isinstance(value, str):
            raise LedgerIntegrityError(f"Persisted {name} timestamp must be text.")
        normalized = normalized_utc(value)
        if normalized != value:
            raise LedgerIntegrityError(f"Persisted {name} timestamp is not canonical UTC.")
        return normalized

    @staticmethod
    def _phase_d_hash(value: Any) -> str:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _experiment_payload(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        self._validate_experiment_state(connection, row)
        result_row = connection.execute("SELECT * FROM phase_e_experiment_results WHERE experiment_id=?", (row["experiment_id"],)).fetchone()
        status = self._status(str(row["status"]))
        try:
            promotion_state = PromotionState(str(row["promotion_state"]))
        except ValueError as exc:
            raise ExperimentConflictError("Unknown persisted Phase E promotion state; failing closed.") from exc
        specification = self._load(row["specification_json"], "experiment specification")
        if not isinstance(specification, Mapping) or not isinstance(specification.get("hypothesis"), Mapping):
            raise ExperimentConflictError("Malformed persisted Phase E experiment specification; failing closed.")
        return {
            "experiment_id": row["experiment_id"],
            "hypothesis_id": row["hypothesis_id"],
            "hypothesis_version": row["hypothesis_version"],
            "hypothesis_hash": row["hypothesis_hash"],
            "corpus_fingerprint": row["corpus_fingerprint"],
            "corpus_provenance": self._load(row["corpus_provenance_json"], "corpus provenance"),
            "corpus_provenance_hash": row["corpus_provenance_hash"],
            "specification": specification,
            "specification_hash": row["specification_hash"],
            "definition": dict(specification["hypothesis"]),
            "status": status.value,
            "promotion_state": promotion_state.value,
            "execution_attempts": row["execution_attempts"],
            "registered_at": row["registered_at"],
            "started_at": row["started_at"],
            "terminal_at": row["terminal_at"],
            "result": self._load(result_row["result_json"], "experiment result") if result_row else None,
            "result_hash": result_row["result_hash"] if result_row else None,
            "result_recorded_at": result_row["recorded_at"] if result_row else None,
            "trading_authority": False,
            "qualified_signal": False,
        }

    @staticmethod
    def _load(raw: str | Any, name: str) -> Any:
        if not isinstance(raw, str):
            return raw
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CorpusProvenanceError(f"Malformed persisted {name}; failing closed.") from exc

    @staticmethod
    def _status(value: str) -> ExperimentStatus:
        try:
            return ExperimentStatus(value)
        except ValueError as exc:
            raise ExperimentConflictError("Unknown persisted Phase E experiment status; failing closed.") from exc

    @staticmethod
    def _require_experiment(connection: sqlite3.Connection, experiment_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM phase_e_experiments WHERE experiment_id=?", (experiment_id,)).fetchone()
        if row is None:
            raise UnknownExperimentError(f"Unknown Phase E experiment: {experiment_id}")
        return row
