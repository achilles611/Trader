"""Append-only Phase E experiment ledger.

Phase D remains the evidence owner.  This module only reads a D.7 corpus
snapshot and stores compact experiment contracts, lifecycle events, and
results in separate ``phase_e_*`` tables in the existing hot SQLite database.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping

from .types import (
    ExperimentConclusion,
    ExperimentResult,
    ExperimentStatus,
    FeatureReference,
    HypothesisDefinition,
    PromotionState,
    canonical_hash,
    canonical_json,
    normalized_utc,
)


class PhaseELedgerError(RuntimeError):
    pass


class CorpusProvenanceError(PhaseELedgerError):
    """The requested D corpus is absent, incomplete, or inconsistent."""


class ExperimentConflictError(PhaseELedgerError):
    """An immutable identity or lifecycle constraint was violated."""


class UnknownExperimentError(PhaseELedgerError):
    pass


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
        "schema": "phase-e1-experiment-identity",
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
                    """SELECT snapshot.*, coverage.state AS coverage_state,
                              coverage.coverage_fraction AS coverage_fraction,
                              coverage.observation_count AS verified_observation_count,
                              coverage.details_json AS coverage_details_json
                       FROM science_corpus_snapshots AS snapshot
                       JOIN science_data_coverage AS coverage ON coverage.coverage_id=snapshot.coverage_id
                       WHERE snapshot.corpus_fingerprint=?""",
                    (corpus_fingerprint,),
                ).fetchone()
        except sqlite3.OperationalError as exc:
            raise CorpusProvenanceError("Phase D corpus tables are unavailable; E.1 refuses unproven evidence.") from exc
        if row is None:
            raise CorpusProvenanceError("Requested Phase D corpus snapshot does not exist.")
        payload = self._load(row["payload_json"], "source corpus snapshot")
        coverage_details = self._load(row["coverage_details_json"], "coverage details")
        if str(row["coverage_state"]) != "PROVEN_COMPLETE" or float(row["coverage_fraction"]) < 1.0:
            raise CorpusProvenanceError("Phase E requires a PROVEN_COMPLETE Phase D corpus with full declared coverage.")
        if (payload.get("corpus_fingerprint") != row["corpus_fingerprint"]
                or payload.get("coverage_id") != row["coverage_id"]
                or payload.get("observation_fingerprint") != row["observation_fingerprint"]):
            raise CorpusProvenanceError("Phase D corpus snapshot has inconsistent immutable provenance fields.")
        payload_coverage = payload.get("coverage")
        if not isinstance(payload_coverage, Mapping) or payload_coverage.get("state") != "PROVEN_COMPLETE":
            raise CorpusProvenanceError("Phase D corpus snapshot lacks a matching proven coverage record.")
        if not str(row["observation_fingerprint"]) or not str(row["code_sha"]) or not str(row["config_sha"]):
            raise CorpusProvenanceError("Phase D corpus snapshot has missing provenance hashes.")
        try:
            features_raw = self._load(row["feature_versions_json"], "feature versions")
            features = tuple(sorted(
                (FeatureReference(str(item["feature_id"]), int(item["version"])) for item in features_raw),
                key=lambda item: (item.feature_id, item.version),
            ))
            interval_start, interval_end = normalized_utc(str(row["interval_start"])), normalized_utc(str(row["interval_end"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise CorpusProvenanceError("Phase D corpus snapshot has malformed feature or interval provenance.") from exc
        if interval_start >= interval_end or int(row["verified_observation_count"]) < 0:
            raise CorpusProvenanceError("Phase D corpus snapshot has invalid interval or observation evidence.")
        source_snapshot = {
            "corpus_snapshot": payload,
            "coverage": {
                "coverage_id": row["coverage_id"],
                "state": row["coverage_state"],
                "coverage_fraction": float(row["coverage_fraction"]),
                "observation_count": int(row["verified_observation_count"]),
                "details": coverage_details,
            },
            "snapshot_columns": {
                "corpus_fingerprint": row["corpus_fingerprint"],
                "interval_start": interval_start,
                "interval_end": interval_end,
                "observation_fingerprint": row["observation_fingerprint"],
                "code_sha": row["code_sha"],
                "config_sha": row["config_sha"],
                "feature_versions": [item.payload() for item in features],
            },
        }
        return CorpusProvenance(
            corpus_fingerprint=str(row["corpus_fingerprint"]),
            coverage_id=str(row["coverage_id"]),
            coverage_state=str(row["coverage_state"]),
            coverage_fraction=float(row["coverage_fraction"]),
            interval_start=interval_start,
            interval_end=interval_end,
            observation_fingerprint=str(row["observation_fingerprint"]),
            source_code_version=str(row["code_sha"]),
            source_config_hash=str(row["config_sha"]),
            verified_observation_count=int(row["verified_observation_count"]),
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
            "experiment_id": experiment_id,
            "hypothesis": definition_payload,
            "hypothesis_hash": definition_hash,
            "corpus": provenance_payload,
        }
        specification_hash = canonical_hash(specification)
        with self._write() as connection:
            prior_hypothesis = connection.execute(
                "SELECT definition_hash FROM phase_e_hypotheses WHERE hypothesis_id=? AND version=?",
                (definition.hypothesis_id, definition.version),
            ).fetchone()
            if prior_hypothesis is None:
                connection.execute(
                    "INSERT INTO phase_e_hypotheses VALUES (?, ?, ?, ?, ?)",
                    (definition.hypothesis_id, definition.version, canonical_json(definition_payload), definition_hash, at),
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
                    canonical_json(provenance_payload), provenance_hash, canonical_json(specification), specification_hash,
                    ExperimentStatus.REGISTERED.value, PromotionState.NOT_ELIGIBLE.value, at,
                ),
            )
            self._append_event(
                connection, experiment_id=experiment_id, event_type="REGISTERED", from_status=None,
                to_status=ExperimentStatus.REGISTERED, reason="predeclared_before_evaluation", event_at=at,
                payload={"specification_hash": specification_hash, "corpus_provenance_hash": provenance_hash}, attempt=0,
            )
            self._append_promotion_event(
                connection, experiment_id=experiment_id, from_state=PromotionState.NOT_ELIGIBLE,
                to_state=PromotionState.NOT_ELIGIBLE, reason="e1_registration_has_no_signal_authority", event_at=at,
                payload={"trading_authority": False}, suffix="registration",
            )
            row = connection.execute("SELECT * FROM phase_e_experiments WHERE experiment_id=?", (experiment_id,)).fetchone()
            assert row is not None
            return self._experiment_payload(connection, row)

    def start(self, experiment_id: str, *, started_at: str) -> dict[str, Any]:
        self.initialize()
        at = normalized_utc(started_at)
        with self._write() as connection:
            row = self._require_experiment(connection, experiment_id)
            status = self._status(row["status"])
            if status not in self._ACTIVE_STATES:
                raise ExperimentConflictError(f"Experiment {experiment_id} cannot start from {status.value}.")
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
            if self._status(row["status"]) != ExperimentStatus.RUNNING:
                raise ExperimentConflictError("Only a running experiment can record its first result.")
            prior = connection.execute("SELECT result_hash FROM phase_e_experiment_results WHERE experiment_id=?", (experiment_id,)).fetchone()
            if prior is not None:
                raise ExperimentConflictError("Experiment already has an immutable result.")
            connection.execute(
                "INSERT INTO phase_e_experiment_results VALUES (?, ?, ?, ?)",
                (experiment_id, canonical_json(result_payload), result_hash, at),
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
                event_at=at, payload={"result_hash": result_hash, "conclusion": result.conclusion.value}, attempt=attempt,
            )
            self._append_promotion_event(
                connection, experiment_id=experiment_id, from_state=PromotionState.NOT_ELIGIBLE,
                to_state=promotion, reason="historical_result_only_no_signal_authority", event_at=at,
                payload={"result_hash": result_hash, "trading_authority": False}, suffix=f"result-{attempt}",
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
            if self._status(row["status"]) != ExperimentStatus.RUNNING:
                raise ExperimentConflictError("Only a running experiment can fail.")
            connection.execute(
                "UPDATE phase_e_experiments SET status=?, terminal_at=? WHERE experiment_id=?",
                (ExperimentStatus.FAILED.value, at, experiment_id),
            )
            self._append_event(
                connection, experiment_id=experiment_id, event_type="FAILED", from_status=ExperimentStatus.RUNNING,
                to_status=ExperimentStatus.FAILED, reason=reason[:300], event_at=at, payload=safe_payload,
                attempt=int(row["execution_attempts"]),
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
            status = self._status(row["status"])
            if status != ExperimentStatus.RUNNING:
                raise ExperimentConflictError("Only an interrupted RUNNING experiment needs recovery.")
            connection.execute("UPDATE phase_e_experiments SET status=? WHERE experiment_id=?", (ExperimentStatus.RECOVERABLE.value, experiment_id))
            self._append_event(
                connection, experiment_id=experiment_id, event_type="RECOVERY_REQUIRED", from_status=status,
                to_status=ExperimentStatus.RECOVERABLE, reason="restart_detected_before_result", event_at=at,
                payload={"partial_result_discarded": False, "trading_authority": False}, attempt=int(row["execution_attempts"]),
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
            state = PromotionState(str(row["promotion_state"]))
            self._append_promotion_event(
                connection, experiment_id=experiment_id, from_state=state, to_state=state,
                reason=f"denied_e1_no_signal_authority:{reason[:200]}", event_at=at,
                payload={"trading_authority": False, "signal_created": False}, suffix=f"denied-{int(row['execution_attempts'])}-{at}",
            )
        return {"experiment_id": experiment_id, "promotion_state": state.value, "qualified_signal": False, "trading_authority": False}

    def get(self, experiment_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = self._require_experiment(connection, experiment_id)
            return self._experiment_payload(connection, row)

    def list(self) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            rows = connection.execute("SELECT * FROM phase_e_experiments ORDER BY registered_at, experiment_id").fetchall()
            return [self._experiment_payload(connection, row) for row in rows]

    def events(self, experiment_id: str) -> list[dict[str, Any]]:
        self.initialize()
        with self._connection() as connection:
            self._require_experiment(connection, experiment_id)
            rows = connection.execute("SELECT * FROM phase_e_experiment_events WHERE experiment_id=? ORDER BY event_at, event_id", (experiment_id,)).fetchall()
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
            self._require_experiment(connection, experiment_id)
            rows = connection.execute("SELECT * FROM phase_e_promotion_events WHERE experiment_id=? ORDER BY event_at, event_id", (experiment_id,)).fetchall()
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
        event_id = canonical_hash({"experiment_id": experiment_id, "event_type": event_type, "attempt": attempt, "event_at": event_at, "digest": digest})
        connection.execute(
            "INSERT INTO phase_e_experiment_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (event_id, experiment_id, event_type, from_status.value if from_status else None, to_status.value,
             reason, event_at, canonical_json(body), digest),
        )

    def _append_promotion_event(
        self, connection: sqlite3.Connection, *, experiment_id: str, from_state: PromotionState,
        to_state: PromotionState, reason: str, event_at: str, payload: Mapping[str, Any], suffix: str,
    ) -> None:
        body, digest = dict(payload), canonical_hash(payload)
        event_id = canonical_hash({"experiment_id": experiment_id, "from": from_state.value, "to": to_state.value, "reason": reason, "event_at": event_at, "suffix": suffix, "digest": digest})
        connection.execute(
            "INSERT INTO phase_e_promotion_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (event_id, experiment_id, from_state.value, to_state.value, reason, event_at, canonical_json(body), digest),
        )

    def _experiment_payload(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
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
