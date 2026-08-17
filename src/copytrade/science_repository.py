"""Durable, append-oriented persistence for scientific trading objects.

This schema intentionally lives alongside the existing copy-trade database but
does not modify frozen Phase D execution tables.  Scientific objects are
versioned evidence; execution remains simulator/shadow only.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from threading import Lock, local
from typing import Any, Iterator, Mapping

from .science_storage import ColdArchiveSpool


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _load(value: str | None, default: Any) -> Any:
    return json.loads(value) if value else default


class ScientificRepository:
    """SQLite repository whose immutable columns are also guarded by triggers."""

    def __init__(self, path: str | Path, *, archive_spool: ColdArchiveSpool | None = None) -> None:
        self.path = Path(path)
        self.archive_spool = archive_spool
        self._initialized = False
        self._initialize_lock = Lock()
        self._session_local = local()

    def _new_connection(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    @contextmanager
    def session(self) -> Iterator[None]:
        """Reuse one connection for a bounded worker batch on this thread.

        Individual repository methods still commit their own durable state,
        while avoiding repeated Windows SQLite connection setup during a batch.
        Other workers receive their own thread-local connection and retain the
        lease/transaction semantics below.
        """
        if getattr(self._session_local, "connection", None) is not None:
            yield
            return
        connection = self._new_connection()
        self._session_local.connection = connection
        try:
            yield
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            self._session_local.connection = None
            connection.close()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = getattr(self._session_local, "connection", None)
        owns_connection = connection is None
        if owns_connection:
            connection = self._new_connection()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            if owns_connection:
                connection.close()

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._initialize_lock:
            if self._initialized:
                return
            with self._connect() as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS science_features (
                    feature_id TEXT NOT NULL, version INTEGER NOT NULL, definition_json TEXT NOT NULL,
                    definition_hash TEXT NOT NULL, created_at TEXT NOT NULL, code_sha TEXT NOT NULL,
                    PRIMARY KEY(feature_id, version)
                );
                CREATE TABLE IF NOT EXISTS science_hypotheses (
                    hypothesis_id TEXT NOT NULL, version INTEGER NOT NULL, state TEXT NOT NULL,
                    definition_json TEXT NOT NULL, config_hash TEXT NOT NULL, registered_at TEXT NOT NULL,
                    predecessor_id TEXT, PRIMARY KEY(hypothesis_id, version)
                );
                CREATE TABLE IF NOT EXISTS science_hypothesis_events (
                    event_id TEXT PRIMARY KEY, hypothesis_id TEXT NOT NULL, version INTEGER NOT NULL,
                    from_state TEXT, to_state TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL,
                    evidence_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS science_experiments (
                    experiment_id TEXT PRIMARY KEY, hypothesis_id TEXT NOT NULL, hypothesis_version INTEGER NOT NULL,
                    kind TEXT NOT NULL, state TEXT NOT NULL, dataset_fingerprint TEXT NOT NULL,
                    configuration_json TEXT NOT NULL, configuration_hash TEXT NOT NULL, created_at TEXT NOT NULL,
                    completed_at TEXT
                );
                CREATE TABLE IF NOT EXISTS science_experiment_results (
                    experiment_id TEXT PRIMARY KEY REFERENCES science_experiments(experiment_id),
                    result_json TEXT NOT NULL, result_hash TEXT NOT NULL, recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_forward_predictions (
                    prediction_id TEXT PRIMARY KEY, experiment_id TEXT NOT NULL REFERENCES science_experiments(experiment_id),
                    predicted_at TEXT NOT NULL, horizon_seconds REAL NOT NULL, market TEXT NOT NULL,
                    payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_forward_outcomes (
                    prediction_id TEXT PRIMARY KEY REFERENCES science_forward_predictions(prediction_id),
                    realized_at TEXT NOT NULL, payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_graveyard (
                    hypothesis_id TEXT NOT NULL, version INTEGER NOT NULL, experiment_id TEXT NOT NULL,
                    reason TEXT NOT NULL, payload_json TEXT NOT NULL, recorded_at TEXT NOT NULL,
                    PRIMARY KEY(hypothesis_id, version, experiment_id)
                );
                CREATE TABLE IF NOT EXISTS science_indicators (
                    indicator_id TEXT NOT NULL, version INTEGER NOT NULL, state TEXT NOT NULL,
                    provenance_json TEXT NOT NULL, provenance_hash TEXT NOT NULL, created_at TEXT NOT NULL,
                    predecessor_id TEXT, PRIMARY KEY(indicator_id, version)
                );
                CREATE TABLE IF NOT EXISTS science_models (
                    model_id TEXT NOT NULL, version INTEGER NOT NULL, state TEXT NOT NULL,
                    definition_json TEXT NOT NULL, definition_hash TEXT NOT NULL, created_at TEXT NOT NULL,
                    predecessor_id TEXT, PRIMARY KEY(model_id, version)
                );
                CREATE TABLE IF NOT EXISTS science_wallet_sensors (
                    wallet TEXT PRIMARY KEY, metrics_json TEXT NOT NULL, evidence_confidence REAL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_decisions (
                    decision_id TEXT PRIMARY KEY, created_at TEXT NOT NULL, symbol TEXT NOT NULL,
                    decision TEXT NOT NULL, payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_latency_measurements (
                    measurement_id TEXT PRIMARY KEY, observed_at TEXT NOT NULL, stage TEXT NOT NULL,
                    elapsed_ms REAL NOT NULL, metadata_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_observations (
                    observation_id TEXT PRIMARY KEY, kind TEXT NOT NULL, source TEXT NOT NULL,
                    source_event_id TEXT NOT NULL, wallet TEXT, symbol TEXT, event_at TEXT NOT NULL,
                    received_at TEXT NOT NULL, normalized_at TEXT NOT NULL, network TEXT NOT NULL,
                    raw_fingerprint TEXT NOT NULL, schema_version INTEGER NOT NULL, code_sha TEXT NOT NULL,
                    config_hash TEXT NOT NULL, quality_flags_json TEXT NOT NULL, payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL, persisted_at TEXT NOT NULL,
                    UNIQUE(source, source_event_id)
                );
                CREATE TABLE IF NOT EXISTS science_feature_values (
                    feature_value_id TEXT PRIMARY KEY, feature_id TEXT NOT NULL, feature_version INTEGER NOT NULL,
                    observation_id TEXT NOT NULL REFERENCES science_observations(observation_id),
                    value_json TEXT NOT NULL, missing INTEGER NOT NULL, source_observation_ids_json TEXT NOT NULL,
                    data_fingerprint TEXT NOT NULL, materialized_at TEXT NOT NULL,
                    UNIQUE(feature_id, feature_version, observation_id)
                );
                CREATE TABLE IF NOT EXISTS science_outcome_labels (
                    label_id TEXT PRIMARY KEY, observation_id TEXT NOT NULL REFERENCES science_observations(observation_id),
                    horizon_seconds INTEGER NOT NULL, resolved_at TEXT NOT NULL, payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL, UNIQUE(observation_id, horizon_seconds)
                );
                CREATE TABLE IF NOT EXISTS science_work_queue (
                    work_id TEXT PRIMARY KEY, work_type TEXT NOT NULL, subject_type TEXT NOT NULL,
                    subject_id TEXT NOT NULL, subject_version INTEGER NOT NULL, state TEXT NOT NULL,
                    priority INTEGER NOT NULL, created_at TEXT NOT NULL, available_at TEXT NOT NULL,
                    claimed_at TEXT, lease_expires_at TEXT, completed_at TEXT, attempt_count INTEGER NOT NULL,
                    max_attempts INTEGER NOT NULL, input_fingerprint TEXT NOT NULL, worker_id TEXT,
                    last_error_class TEXT, last_error_message_redacted TEXT, result_reference TEXT,
                    UNIQUE(work_type, subject_type, subject_id, subject_version, input_fingerprint)
                );
                CREATE TABLE IF NOT EXISTS science_watermarks (
                    cursor_name TEXT PRIMARY KEY, cursor_value TEXT NOT NULL, updated_at TEXT NOT NULL,
                    status TEXT NOT NULL, details_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_worker_control (
                    control_id INTEGER PRIMARY KEY CHECK(control_id=1), paused INTEGER NOT NULL,
                    reason TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_search_families (
                    family_id TEXT NOT NULL, version INTEGER NOT NULL, definition_json TEXT NOT NULL,
                    definition_hash TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(family_id, version)
                );
                CREATE TABLE IF NOT EXISTS science_discoveries (
                    discovery_id TEXT PRIMARY KEY, family_id TEXT NOT NULL, family_version INTEGER NOT NULL,
                    state TEXT NOT NULL, payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL, registered_hypothesis_id TEXT
                );
                CREATE TABLE IF NOT EXISTS science_model_roles (
                    role TEXT PRIMARY KEY, model_id TEXT NOT NULL, version INTEGER NOT NULL,
                    evidence_json TEXT NOT NULL, assigned_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_model_calibrations (
                    calibration_id TEXT PRIMARY KEY, model_id TEXT NOT NULL, version INTEGER NOT NULL,
                    source_fingerprint TEXT NOT NULL, payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL, UNIQUE(model_id, version, source_fingerprint)
                );
                CREATE TABLE IF NOT EXISTS science_drift_events (
                    event_id TEXT PRIMARY KEY, object_type TEXT NOT NULL, object_id TEXT NOT NULL,
                    version INTEGER NOT NULL, state TEXT NOT NULL, reason TEXT NOT NULL,
                    evidence_json TEXT NOT NULL, created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_stage_health (
                    stage TEXT PRIMARY KEY, state TEXT NOT NULL, detail_json TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS science_journals (
                    journal_date TEXT PRIMARY KEY, payload_json TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_science_hypothesis_state ON science_hypotheses(state, registered_at DESC);
                CREATE INDEX IF NOT EXISTS idx_science_experiment_hypothesis ON science_experiments(hypothesis_id, hypothesis_version, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_science_prediction_experiment ON science_forward_predictions(experiment_id, predicted_at DESC);
                CREATE INDEX IF NOT EXISTS idx_science_decision_time ON science_decisions(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_science_observation_time ON science_observations(normalized_at, observation_id);
                CREATE INDEX IF NOT EXISTS idx_science_observation_symbol ON science_observations(symbol, normalized_at);
                CREATE INDEX IF NOT EXISTS idx_science_feature_observation ON science_feature_values(observation_id, feature_id, feature_version);
                CREATE INDEX IF NOT EXISTS idx_science_outcome_horizon ON science_outcome_labels(horizon_seconds, resolved_at);
                CREATE INDEX IF NOT EXISTS idx_science_work_claim ON science_work_queue(state, available_at, priority DESC, created_at);
                CREATE INDEX IF NOT EXISTS idx_science_work_lease ON science_work_queue(state, lease_expires_at);
                CREATE TRIGGER IF NOT EXISTS science_features_immutable BEFORE UPDATE OF definition_json, definition_hash ON science_features
                    BEGIN SELECT RAISE(ABORT, 'scientific feature definition is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_hypotheses_immutable BEFORE UPDATE OF definition_json, config_hash ON science_hypotheses
                    BEGIN SELECT RAISE(ABORT, 'registered hypothesis definition is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_experiments_immutable BEFORE UPDATE OF configuration_json, configuration_hash, dataset_fingerprint ON science_experiments
                    BEGIN SELECT RAISE(ABORT, 'experiment definition is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_predictions_immutable BEFORE UPDATE ON science_forward_predictions
                    BEGIN SELECT RAISE(ABORT, 'forward prediction is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_outcomes_immutable BEFORE UPDATE ON science_forward_outcomes
                    BEGIN SELECT RAISE(ABORT, 'forward outcome is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_indicators_immutable BEFORE UPDATE OF provenance_json, provenance_hash ON science_indicators
                    BEGIN SELECT RAISE(ABORT, 'indicator provenance is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_models_immutable BEFORE UPDATE OF definition_json, definition_hash ON science_models
                    BEGIN SELECT RAISE(ABORT, 'model definition is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_model_calibrations_immutable BEFORE UPDATE ON science_model_calibrations
                    BEGIN SELECT RAISE(ABORT, 'model calibration is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_observations_immutable BEFORE UPDATE ON science_observations
                    BEGIN SELECT RAISE(ABORT, 'scientific observations are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_feature_values_immutable BEFORE UPDATE ON science_feature_values
                    BEGIN SELECT RAISE(ABORT, 'materialized feature values are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_outcome_labels_immutable BEFORE UPDATE ON science_outcome_labels
                    BEGIN SELECT RAISE(ABORT, 'outcome labels are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS science_search_families_immutable BEFORE UPDATE ON science_search_families
                    BEGIN SELECT RAISE(ABORT, 'search-family definitions are immutable'); END;
                    """
                )
            self._initialized = True

    def register_feature(self, feature_id: str, version: int, definition: Mapping[str, Any], *, created_at: str, code_sha: str) -> dict[str, Any]:
        self.initialize()
        payload, digest = dict(definition), canonical_hash(definition)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_features WHERE feature_id=? AND version=?", (feature_id, version)).fetchone()
            if existing:
                if existing["definition_hash"] != digest:
                    raise ValueError("Feature semantics changed; register a new feature version.")
                return self._feature_payload(existing)
            connection.execute("INSERT INTO science_features VALUES (?, ?, ?, ?, ?, ?)", (feature_id, version, _dump(payload), digest, created_at, code_sha))
        return {"feature_id": feature_id, "version": version, "definition": payload, "definition_hash": digest, "created_at": created_at, "code_sha": code_sha}

    def register_hypothesis(self, hypothesis_id: str, version: int, definition: Mapping[str, Any], *, state: str, registered_at: str, predecessor_id: str | None = None) -> dict[str, Any]:
        self.initialize()
        config_hash = canonical_hash(definition)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_hypotheses WHERE hypothesis_id=? AND version=?", (hypothesis_id, version)).fetchone()
            if existing:
                if existing["config_hash"] != config_hash:
                    raise ValueError("Registered hypothesis semantics changed; create a new version.")
                return self._hypothesis_payload(existing)
            connection.execute("INSERT INTO science_hypotheses VALUES (?, ?, ?, ?, ?, ?, ?)", (hypothesis_id, version, state, _dump(dict(definition)), config_hash, registered_at, predecessor_id))
        return {"hypothesis_id": hypothesis_id, "version": version, "state": state, "definition": dict(definition), "config_hash": config_hash, "registered_at": registered_at, "predecessor_id": predecessor_id}

    def transition_hypothesis(self, hypothesis_id: str, version: int, *, state: str, reason: str, event_id: str, created_at: str, evidence: Mapping[str, Any] | None = None) -> dict[str, Any]:
        self.initialize()
        with self._connect() as connection:
            row = connection.execute("SELECT state FROM science_hypotheses WHERE hypothesis_id=? AND version=?", (hypothesis_id, version)).fetchone()
            if not row:
                raise KeyError("Unknown hypothesis version.")
            connection.execute("UPDATE science_hypotheses SET state=? WHERE hypothesis_id=? AND version=?", (state, hypothesis_id, version))
            connection.execute("INSERT OR IGNORE INTO science_hypothesis_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (event_id, hypothesis_id, version, row["state"], state, reason, created_at, _dump(dict(evidence or {}))))
        return {"hypothesis_id": hypothesis_id, "version": version, "from_state": row["state"], "state": state, "reason": reason}

    def create_experiment(self, experiment_id: str, *, hypothesis_id: str, hypothesis_version: int, kind: str, state: str, dataset_fingerprint: str, configuration: Mapping[str, Any], created_at: str) -> dict[str, Any]:
        self.initialize()
        digest, payload = canonical_hash(configuration), dict(configuration)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_experiments WHERE experiment_id=?", (experiment_id,)).fetchone()
            if existing:
                if existing["configuration_hash"] != digest or existing["dataset_fingerprint"] != dataset_fingerprint:
                    raise ValueError("Experiment identity already has different immutable input.")
                return self._experiment_payload(existing)
            hypothesis = connection.execute("SELECT 1 FROM science_hypotheses WHERE hypothesis_id=? AND version=?", (hypothesis_id, hypothesis_version)).fetchone()
            if not hypothesis:
                raise KeyError("Experiment requires a registered hypothesis.")
            connection.execute("INSERT INTO science_experiments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)", (experiment_id, hypothesis_id, hypothesis_version, kind, state, dataset_fingerprint, _dump(payload), digest, created_at))
        return {"experiment_id": experiment_id, "hypothesis_id": hypothesis_id, "hypothesis_version": hypothesis_version, "kind": kind, "state": state, "dataset_fingerprint": dataset_fingerprint, "configuration": payload, "created_at": created_at}

    def record_experiment_result(self, experiment_id: str, result: Mapping[str, Any], *, recorded_at: str, state: str = "COMPLETED") -> dict[str, Any]:
        self.initialize()
        payload, digest = dict(result), canonical_hash(result)
        with self._connect() as connection:
            prior = connection.execute("SELECT * FROM science_experiment_results WHERE experiment_id=?", (experiment_id,)).fetchone()
            if prior:
                if prior["result_hash"] != digest:
                    raise ValueError("Experiment result is immutable; create a new experiment.")
                return self._result_payload(prior)
            connection.execute("INSERT INTO science_experiment_results VALUES (?, ?, ?, ?)", (experiment_id, _dump(payload), digest, recorded_at))
            connection.execute("UPDATE science_experiments SET state=?, completed_at=? WHERE experiment_id=?", (state, recorded_at, experiment_id))
        return {"experiment_id": experiment_id, "result": payload, "result_hash": digest, "recorded_at": recorded_at}

    def create_forward_prediction(self, prediction_id: str, *, experiment_id: str, predicted_at: str, horizon_seconds: float, market: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        if horizon_seconds <= 0:
            raise ValueError("Forward prediction horizon must be positive.")
        self.initialize()
        body, digest = dict(payload), canonical_hash(payload)
        with self._connect() as connection:
            connection.execute("INSERT INTO science_forward_predictions VALUES (?, ?, ?, ?, ?, ?, ?)", (prediction_id, experiment_id, predicted_at, horizon_seconds, market, _dump(body), digest))
        return {"prediction_id": prediction_id, "experiment_id": experiment_id, "predicted_at": predicted_at, "horizon_seconds": horizon_seconds, "market": market, "payload": body, "payload_hash": digest}

    def record_forward_outcome(self, prediction_id: str, *, realized_at: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.initialize()
        body, digest = dict(payload), canonical_hash(payload)
        with self._connect() as connection:
            prediction = connection.execute("SELECT 1 FROM science_forward_predictions WHERE prediction_id=?", (prediction_id,)).fetchone()
            if not prediction:
                raise KeyError("Outcome requires an already persisted prediction.")
            connection.execute("INSERT INTO science_forward_outcomes VALUES (?, ?, ?, ?)", (prediction_id, realized_at, _dump(body), digest))
        return {"prediction_id": prediction_id, "realized_at": realized_at, "payload": body, "payload_hash": digest}

    def add_graveyard_entry(self, *, hypothesis_id: str, version: int, experiment_id: str, reason: str, payload: Mapping[str, Any], recorded_at: str) -> None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("INSERT OR IGNORE INTO science_graveyard VALUES (?, ?, ?, ?, ?, ?)", (hypothesis_id, version, experiment_id, reason, _dump(dict(payload)), recorded_at))
        if self.archive_spool:
            self.archive_spool.enqueue("rejected-hypothesis", [{"hypothesis_id": hypothesis_id, "version": version, "experiment_id": experiment_id, "reason": reason, **dict(payload)}])

    def register_indicator(self, indicator_id: str, version: int, *, state: str, provenance: Mapping[str, Any], created_at: str, predecessor_id: str | None = None) -> dict[str, Any]:
        self.initialize()
        body, digest = dict(provenance), canonical_hash(provenance)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_indicators WHERE indicator_id=? AND version=?", (indicator_id, version)).fetchone()
            if existing:
                if existing["provenance_hash"] != digest:
                    raise ValueError("Indicator provenance changed; register a new version.")
                return self._indicator_payload(existing)
            connection.execute("INSERT INTO science_indicators VALUES (?, ?, ?, ?, ?, ?, ?)", (indicator_id, version, state, _dump(body), digest, created_at, predecessor_id))
        return {"indicator_id": indicator_id, "version": version, "state": state, "provenance": body, "created_at": created_at, "predecessor_id": predecessor_id}

    def set_indicator_state(self, indicator_id: str, version: int, state: str) -> None:
        self.initialize()
        with self._connect() as connection:
            cursor = connection.execute("UPDATE science_indicators SET state=? WHERE indicator_id=? AND version=?", (state, indicator_id, version))
            if cursor.rowcount != 1:
                raise KeyError("Unknown indicator version.")

    def register_model(self, model_id: str, version: int, *, state: str, definition: Mapping[str, Any], created_at: str, predecessor_id: str | None = None) -> dict[str, Any]:
        self.initialize()
        body, digest = dict(definition), canonical_hash(definition)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_models WHERE model_id=? AND version=?", (model_id, version)).fetchone()
            if existing:
                if existing["definition_hash"] != digest:
                    raise ValueError("Model definition changed; register a new version.")
                return self._model_payload(existing)
            connection.execute("INSERT INTO science_models VALUES (?, ?, ?, ?, ?, ?, ?)", (model_id, version, state, _dump(body), digest, created_at, predecessor_id))
        return {"model_id": model_id, "version": version, "state": state, "definition": body, "created_at": created_at, "predecessor_id": predecessor_id}

    def upsert_wallet_sensor(self, wallet: str, metrics: Mapping[str, Any], *, evidence_confidence: float | None, updated_at: str) -> None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("""INSERT INTO science_wallet_sensors(wallet, metrics_json, evidence_confidence, updated_at) VALUES (?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET metrics_json=excluded.metrics_json, evidence_confidence=excluded.evidence_confidence, updated_at=excluded.updated_at""", (wallet.lower(), _dump(dict(metrics)), evidence_confidence, updated_at))

    def record_decision(self, decision_id: str, *, created_at: str, symbol: str, decision: str, payload: Mapping[str, Any]) -> None:
        self.initialize()
        body, digest = dict(payload), canonical_hash(payload)
        with self._connect() as connection:
            connection.execute("INSERT OR IGNORE INTO science_decisions VALUES (?, ?, ?, ?, ?, ?)", (decision_id, created_at, symbol, decision, _dump(body), digest))

    def record_latency(self, measurement_id: str, *, observed_at: str, stage: str, elapsed_ms: float, metadata: Mapping[str, Any] | None = None) -> None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("INSERT OR REPLACE INTO science_latency_measurements VALUES (?, ?, ?, ?, ?)", (measurement_id, observed_at, stage, elapsed_ms, _dump(dict(metadata or {}))))

    def latency_report(self) -> dict[str, dict[str, float | int]]:
        self.initialize()
        with self._connect() as connection:
            rows = connection.execute("SELECT stage, elapsed_ms FROM science_latency_measurements ORDER BY stage, elapsed_ms").fetchall()
        grouped: dict[str, list[float]] = {}
        for row in rows:
            grouped.setdefault(str(row["stage"]), []).append(float(row["elapsed_ms"]))
        def percentile(values: list[float], fraction: float) -> float:
            if len(values) == 1:
                return values[0]
            index = (len(values) - 1) * fraction
            lower, upper = int(index), min(len(values) - 1, int(index) + 1)
            return values[lower] + (values[upper] - values[lower]) * (index - lower)
        return {stage: {"count": len(values), "mean_ms": sum(values) / len(values), "max_ms": max(values),
                        "p50_ms": percentile(values, 0.50), "p95_ms": percentile(values, 0.95), "p99_ms": percentile(values, 0.99)}
                for stage, values in grouped.items()}

    # Phase D.6 runtime evidence.  These tables are additive to the D.5
    # scientific objects above; none of them alter frozen execution evidence.
    def record_observation(self, observation_id: str, *, kind: str, source: str, source_event_id: str, wallet: str | None,
                           symbol: str | None, event_at: str, received_at: str, normalized_at: str, network: str,
                           raw_fingerprint: str, schema_version: int, code_sha: str, config_hash: str,
                           quality_flags: Mapping[str, Any], payload: Mapping[str, Any], persisted_at: str) -> dict[str, Any]:
        self.initialize()
        body, digest = dict(payload), canonical_hash(payload)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_observations WHERE observation_id=?", (observation_id,)).fetchone()
            if existing:
                if existing["payload_hash"] != digest or existing["raw_fingerprint"] != raw_fingerprint:
                    raise ValueError("Observation identity has conflicting immutable evidence.")
                return self._observation_payload(existing)
            try:
                connection.execute("""INSERT INTO science_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                    observation_id, kind, source, source_event_id, wallet.lower() if wallet else None, symbol,
                    event_at, received_at, normalized_at, network, raw_fingerprint, schema_version, code_sha,
                    config_hash, _dump(dict(quality_flags)), _dump(body), digest, persisted_at,
                ))
            except sqlite3.IntegrityError as exc:
                prior = connection.execute("SELECT * FROM science_observations WHERE source=? AND source_event_id=?", (source, source_event_id)).fetchone()
                if prior and prior["payload_hash"] == digest:
                    return self._observation_payload(prior)
                raise ValueError("Source event identity has conflicting immutable evidence.") from exc
        return {"observation_id": observation_id, "kind": kind, "source": source, "source_event_id": source_event_id,
                "wallet": wallet.lower() if wallet else None, "symbol": symbol, "event_at": event_at,
                "received_at": received_at, "normalized_at": normalized_at, "network": network,
                "raw_fingerprint": raw_fingerprint, "schema_version": schema_version, "code_sha": code_sha,
                "config_hash": config_hash, "quality_flags": dict(quality_flags), "payload": body,
                "payload_hash": digest, "persisted_at": persisted_at}

    def list_observations(self, *, after: tuple[str, str] | None = None, limit: int = 500, kinds: tuple[str, ...] = ()) -> list[dict[str, Any]]:
        query: str = "SELECT * FROM science_observations"
        values: list[Any] = []
        clauses = []
        if after:
            clauses.append("(normalized_at > ? OR (normalized_at = ? AND observation_id > ?))")
            values.extend([after[0], after[0], after[1]])
        if kinds:
            clauses.append("kind IN (" + ",".join("?" for _ in kinds) + ")")
            values.extend(kinds)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY normalized_at, observation_id LIMIT ?"
        values.append(max(1, min(limit, 5_000)))
        return self._rows(query, self._observation_payload, values)

    def record_feature_value(self, feature_value_id: str, *, feature_id: str, feature_version: int, observation_id: str,
                             value: Any, missing: bool, source_observation_ids: tuple[str, ...], data_fingerprint: str,
                             materialized_at: str) -> dict[str, Any]:
        self.initialize()
        body = {"value": value}
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_feature_values WHERE feature_value_id=?", (feature_value_id,)).fetchone()
            if existing:
                if _load(existing["value_json"], {}) != body or bool(existing["missing"]) != bool(missing):
                    raise ValueError("Feature-value identity has conflicting immutable evidence.")
                return self._feature_value_payload(existing)
            connection.execute("""INSERT INTO science_feature_values VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                feature_value_id, feature_id, feature_version, observation_id, _dump(body), int(missing),
                _dump(list(source_observation_ids)), data_fingerprint, materialized_at,
            ))
        return {"feature_value_id": feature_value_id, "feature_id": feature_id, "feature_version": feature_version,
                "observation_id": observation_id, "value": value, "missing": bool(missing),
                "source_observation_ids": list(source_observation_ids), "data_fingerprint": data_fingerprint,
                "materialized_at": materialized_at}

    def record_feature_values(self, values: list[Mapping[str, Any]]) -> None:
        """Persist one observation's immutable values in one SQLite transaction."""
        self.initialize()
        with self._connect() as connection:
            for item in values:
                body = {"value": item["value"]}
                existing = connection.execute("SELECT * FROM science_feature_values WHERE feature_value_id=?", (item["feature_value_id"],)).fetchone()
                if existing:
                    if _load(existing["value_json"], {}) != body or bool(existing["missing"]) != bool(item["missing"]):
                        raise ValueError("Feature-value identity has conflicting immutable evidence.")
                    continue
                try:
                    connection.execute("""INSERT INTO science_feature_values VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                        item["feature_value_id"], item["feature_id"], int(item["feature_version"]), item["observation_id"],
                        _dump(body), int(bool(item["missing"])), _dump(list(item["source_observation_ids"])),
                        item["data_fingerprint"], item["materialized_at"],
                    ))
                except sqlite3.IntegrityError as exc:
                    prior = connection.execute("""SELECT * FROM science_feature_values
                        WHERE feature_id=? AND feature_version=? AND observation_id=?""", (
                        item["feature_id"], int(item["feature_version"]), item["observation_id"],
                    )).fetchone()
                    if prior and _load(prior["value_json"], {}) == body and bool(prior["missing"]) == bool(item["missing"]):
                        continue
                    raise ValueError("Feature observation/version has conflicting immutable evidence.") from exc

    def list_feature_values(self, *, observation_ids: tuple[str, ...] = (), feature_id: str | None = None) -> list[dict[str, Any]]:
        query: str = "SELECT * FROM science_feature_values"
        values: list[Any] = []
        clauses = []
        if observation_ids:
            clauses.append("observation_id IN (" + ",".join("?" for _ in observation_ids) + ")")
            values.extend(observation_ids)
        if feature_id:
            clauses.append("feature_id=?")
            values.append(feature_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY materialized_at, feature_value_id"
        return self._rows(query, self._feature_value_payload, values)

    def record_outcome_label(self, label_id: str, *, observation_id: str, horizon_seconds: int, resolved_at: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        if not 0 < horizon_seconds <= 600:
            raise ValueError("Outcome horizon must be in (0, 600].")
        self.initialize()
        body, digest = dict(payload), canonical_hash(payload)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_outcome_labels WHERE label_id=?", (label_id,)).fetchone()
            if existing:
                if existing["payload_hash"] != digest:
                    raise ValueError("Outcome-label identity has conflicting immutable evidence.")
                return self._outcome_payload(existing)
            connection.execute("INSERT INTO science_outcome_labels VALUES (?, ?, ?, ?, ?, ?)", (label_id, observation_id, horizon_seconds, resolved_at, _dump(body), digest))
        return {"label_id": label_id, "observation_id": observation_id, "horizon_seconds": horizon_seconds,
                "resolved_at": resolved_at, "payload": body, "payload_hash": digest}

    def list_outcome_labels(self, *, horizon_seconds: int | None = None) -> list[dict[str, Any]]:
        query, values = "SELECT * FROM science_outcome_labels", []
        if horizon_seconds is not None:
            query += " WHERE horizon_seconds=?"; values.append(horizon_seconds)
        query += " ORDER BY resolved_at, label_id"
        return self._rows(query, self._outcome_payload, values)

    def enqueue_work(self, work_id: str, *, work_type: str, subject_type: str, subject_id: str, subject_version: int,
                     priority: int, created_at: str, available_at: str, max_attempts: int, input_fingerprint: str) -> dict[str, Any]:
        if max_attempts <= 0:
            raise ValueError("Work max_attempts must be positive.")
        self.initialize()
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_work_queue WHERE work_id=?", (work_id,)).fetchone()
            if existing:
                return self._work_payload(existing)
            try:
                connection.execute("""INSERT INTO science_work_queue(work_id, work_type, subject_type, subject_id, subject_version, state, priority, created_at, available_at, attempt_count, max_attempts, input_fingerprint)
                    VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?, ?, 0, ?, ?)""", (work_id, work_type, subject_type, subject_id, subject_version, priority, created_at, available_at, max_attempts, input_fingerprint))
            except sqlite3.IntegrityError:
                existing = connection.execute("SELECT * FROM science_work_queue WHERE work_type=? AND subject_type=? AND subject_id=? AND subject_version=? AND input_fingerprint=?", (work_type, subject_type, subject_id, subject_version, input_fingerprint)).fetchone()
                if existing:
                    return self._work_payload(existing)
                raise
        return {"work_id": work_id, "work_type": work_type, "subject_type": subject_type, "subject_id": subject_id,
                "subject_version": subject_version, "state": "PENDING", "priority": priority, "created_at": created_at,
                "available_at": available_at, "attempt_count": 0, "max_attempts": max_attempts,
                "input_fingerprint": input_fingerprint}

    def supersede_available_work(self, *, work_type: str, subject_type: str, subject_id: str,
                                 subject_version: int, keep_fingerprint: str) -> int:
        """Retire stale, not-yet-leased semantic work when newer evidence arrives.

        Leased work is deliberately left alone: the worker that owns it must
        either finish against its frozen input or lose its lease.  This keeps
        queue history auditable while avoiding a backlog of expensive research
        jobs for intermediate feature/label snapshots.
        """
        self.initialize()
        with self._connect() as connection:
            cursor = connection.execute("""UPDATE science_work_queue SET state='SUPERSEDED',
                completed_at=COALESCE(completed_at, available_at), result_reference='newer evidence fingerprint queued'
                WHERE work_type=? AND subject_type=? AND subject_id=? AND subject_version=?
                  AND state IN ('PENDING', 'RETRYABLE') AND input_fingerprint<>?""", (
                work_type, subject_type, subject_id, subject_version, keep_fingerprint,
            ))
        return int(cursor.rowcount)

    def claim_work(self, *, worker_id: str, now: str, lease_expires_at: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("""SELECT * FROM science_work_queue WHERE
                (state IN ('PENDING', 'RETRYABLE') AND available_at <= ?)
                OR (state='LEASED' AND lease_expires_at < ?)
                ORDER BY priority DESC, created_at, work_id LIMIT 1""", (now, now)).fetchone()
            if not row:
                return None
            connection.execute("""UPDATE science_work_queue SET state='LEASED', worker_id=?, claimed_at=?, lease_expires_at=?,
                attempt_count=attempt_count+1, last_error_class=NULL, last_error_message_redacted=NULL WHERE work_id=?""", (worker_id, now, lease_expires_at, row["work_id"]))
            claimed = connection.execute("SELECT * FROM science_work_queue WHERE work_id=?", (row["work_id"],)).fetchone()
        return self._work_payload(claimed)

    def complete_work(self, work_id: str, *, worker_id: str, completed_at: str, result_reference: str = "") -> None:
        self.initialize()
        with self._connect() as connection:
            cursor = connection.execute("""UPDATE science_work_queue SET state='COMPLETED', completed_at=?, lease_expires_at=NULL,
                result_reference=? WHERE work_id=? AND state='LEASED' AND worker_id=?""", (completed_at, result_reference, work_id, worker_id))
            if cursor.rowcount != 1:
                raise RuntimeError("Work completion lost its lease.")

    def fail_work(self, work_id: str, *, worker_id: str, available_at: str, error_class: str, message_redacted: str, permanent: bool = False) -> str:
        self.initialize()
        with self._connect() as connection:
            row = connection.execute("SELECT attempt_count, max_attempts FROM science_work_queue WHERE work_id=? AND state='LEASED' AND worker_id=?", (work_id, worker_id)).fetchone()
            if not row:
                raise RuntimeError("Work failure lost its lease.")
            state = "FAILED" if permanent or row["attempt_count"] >= row["max_attempts"] else "RETRYABLE"
            connection.execute("""UPDATE science_work_queue SET state=?, available_at=?, lease_expires_at=NULL,
                last_error_class=?, last_error_message_redacted=? WHERE work_id=?""", (state, available_at, error_class, message_redacted[:500], work_id))
        return state

    def recover_expired_leases(self, *, now: str) -> int:
        self.initialize()
        with self._connect() as connection:
            cursor = connection.execute("""UPDATE science_work_queue SET state='RETRYABLE', worker_id=NULL, claimed_at=NULL,
                lease_expires_at=NULL, last_error_class='WORKER_CRASH_RECOVERY', last_error_message_redacted='lease expired'
                WHERE state='LEASED' AND lease_expires_at < ?""", (now,))
        return int(cursor.rowcount)

    def work_queue_status(self, *, now: str) -> dict[str, Any]:
        self.initialize()
        with self._connect() as connection:
            rows = connection.execute("SELECT state, COUNT(*) AS count FROM science_work_queue GROUP BY state").fetchall()
            oldest = connection.execute("SELECT MIN(created_at) AS created_at FROM science_work_queue WHERE state IN ('PENDING', 'RETRYABLE')").fetchone()
        return {"states": {row["state"]: row["count"] for row in rows}, "oldest_pending_at": oldest["created_at"], "checked_at": now}

    def set_watermark(self, name: str, value: str, *, updated_at: str, status: str = "READY", details: Mapping[str, Any] | None = None) -> None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("""INSERT INTO science_watermarks VALUES (?, ?, ?, ?, ?) ON CONFLICT(cursor_name) DO UPDATE SET
                cursor_value=excluded.cursor_value, updated_at=excluded.updated_at, status=excluded.status, details_json=excluded.details_json""", (name, value, updated_at, status, _dump(dict(details or {}))))

    def get_watermark(self, name: str) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM science_watermarks WHERE cursor_name=?", (name,)).fetchone()
        return None if row is None else {"name": row["cursor_name"], "value": row["cursor_value"], "updated_at": row["updated_at"], "status": row["status"], "details": _load(row["details_json"], {})}

    def list_watermarks(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_watermarks ORDER BY cursor_name", lambda row: {"name": row["cursor_name"], "value": row["cursor_value"], "updated_at": row["updated_at"], "status": row["status"], "details": _load(row["details_json"], {})})

    def set_worker_paused(self, paused: bool, *, reason: str, updated_at: str) -> None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("""INSERT INTO science_worker_control VALUES (1, ?, ?, ?) ON CONFLICT(control_id) DO UPDATE SET
                paused=excluded.paused, reason=excluded.reason, updated_at=excluded.updated_at""", (int(paused), reason, updated_at))

    def worker_control(self) -> dict[str, Any]:
        self.initialize()
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM science_worker_control WHERE control_id=1").fetchone()
        return {"paused": bool(row["paused"]) if row else False, "reason": row["reason"] if row else "", "updated_at": row["updated_at"] if row else None}

    def register_search_family(self, family_id: str, version: int, definition: Mapping[str, Any], *, created_at: str) -> dict[str, Any]:
        self.initialize()
        body, digest = dict(definition), canonical_hash(definition)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_search_families WHERE family_id=? AND version=?", (family_id, version)).fetchone()
            if existing:
                if existing["definition_hash"] != digest:
                    raise ValueError("Search-family definition is immutable; create a new version.")
                return {"family_id": family_id, "version": version, "definition": _load(existing["definition_json"], {}), "definition_hash": digest, "created_at": existing["created_at"]}
            connection.execute("INSERT INTO science_search_families VALUES (?, ?, ?, ?, ?)", (family_id, version, _dump(body), digest, created_at))
        return {"family_id": family_id, "version": version, "definition": body, "definition_hash": digest, "created_at": created_at}

    def record_discovery(self, discovery_id: str, *, family_id: str, family_version: int, state: str, payload: Mapping[str, Any], created_at: str, registered_hypothesis_id: str | None = None) -> dict[str, Any]:
        self.initialize()
        body, digest = dict(payload), canonical_hash(payload)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_discoveries WHERE discovery_id=?", (discovery_id,)).fetchone()
            if existing:
                if existing["payload_hash"] != digest:
                    raise ValueError("Discovery identity has conflicting immutable evidence.")
                return self._discovery_payload(existing)
            connection.execute("INSERT INTO science_discoveries VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (discovery_id, family_id, family_version, state, _dump(body), digest, created_at, registered_hypothesis_id))
        return {"discovery_id": discovery_id, "family_id": family_id, "family_version": family_version, "state": state,
                "payload": body, "payload_hash": digest, "created_at": created_at, "registered_hypothesis_id": registered_hypothesis_id}

    def list_discoveries(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_discoveries ORDER BY created_at DESC", self._discovery_payload)

    def set_stage_health(self, stage: str, state: str, *, detail: Mapping[str, Any], updated_at: str) -> None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("""INSERT INTO science_stage_health VALUES (?, ?, ?, ?) ON CONFLICT(stage) DO UPDATE SET
                state=excluded.state, detail_json=excluded.detail_json, updated_at=excluded.updated_at""", (stage, state, _dump(dict(detail)), updated_at))

    def stage_health(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_stage_health ORDER BY stage", lambda row: {"stage": row["stage"], "state": row["state"], "detail": _load(row["detail_json"], {}), "updated_at": row["updated_at"]})

    def list_forward_records(self) -> list[dict[str, Any]]:
        return self._rows("""SELECT p.*, o.realized_at, o.payload_json AS outcome_json FROM science_forward_predictions p
            LEFT JOIN science_forward_outcomes o ON o.prediction_id=p.prediction_id ORDER BY p.predicted_at, p.prediction_id""", lambda row: {"prediction_id": row["prediction_id"], "experiment_id": row["experiment_id"], "predicted_at": row["predicted_at"], "horizon_seconds": row["horizon_seconds"], "market": row["market"], "payload": _load(row["payload_json"], {}), "realized_at": row["realized_at"], "outcome": _load(row["outcome_json"], {}) if row["outcome_json"] else None})

    def set_model_state(self, model_id: str, version: int, state: str) -> None:
        self.initialize()
        with self._connect() as connection:
            cursor = connection.execute("UPDATE science_models SET state=? WHERE model_id=? AND version=?", (state, model_id, version))
            if cursor.rowcount != 1:
                raise KeyError("Unknown model version.")

    def assign_model_role(self, role: str, model_id: str, version: int, *, evidence: Mapping[str, Any], assigned_at: str) -> None:
        if role not in {"CHAMPION", "CHALLENGER"}:
            raise ValueError("Model role must be CHAMPION or CHALLENGER.")
        self.initialize()
        with self._connect() as connection:
            connection.execute("""INSERT INTO science_model_roles VALUES (?, ?, ?, ?, ?) ON CONFLICT(role) DO UPDATE SET
                model_id=excluded.model_id, version=excluded.version, evidence_json=excluded.evidence_json, assigned_at=excluded.assigned_at""", (role, model_id, version, _dump(dict(evidence)), assigned_at))

    def model_roles(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_model_roles ORDER BY role", lambda row: {"role": row["role"], "model_id": row["model_id"], "version": row["version"], "evidence": _load(row["evidence_json"], {}), "assigned_at": row["assigned_at"]})

    def record_model_calibration(self, calibration_id: str, *, model_id: str, version: int,
                                 source_fingerprint: str, payload: Mapping[str, Any], created_at: str) -> dict[str, Any]:
        self.initialize()
        body, digest = dict(payload), canonical_hash(payload)
        with self._connect() as connection:
            existing = connection.execute("SELECT * FROM science_model_calibrations WHERE calibration_id=?", (calibration_id,)).fetchone()
            if existing:
                if existing["payload_hash"] != digest:
                    raise ValueError("Model calibration identity has conflicting immutable evidence.")
                return self._calibration_payload(existing)
            try:
                connection.execute("INSERT INTO science_model_calibrations VALUES (?, ?, ?, ?, ?, ?, ?)",
                                   (calibration_id, model_id, version, source_fingerprint, _dump(body), digest, created_at))
            except sqlite3.IntegrityError as exc:
                existing = connection.execute("""SELECT * FROM science_model_calibrations WHERE
                    model_id=? AND version=? AND source_fingerprint=?""", (model_id, version, source_fingerprint)).fetchone()
                if existing and existing["payload_hash"] == digest:
                    return self._calibration_payload(existing)
                raise ValueError("Model calibration source fingerprint conflicts.") from exc
        return {"calibration_id": calibration_id, "model_id": model_id, "version": version,
                "source_fingerprint": source_fingerprint, "payload": body, "created_at": created_at}

    def list_model_calibrations(self, *, model_id: str | None = None) -> list[dict[str, Any]]:
        query, values = "SELECT * FROM science_model_calibrations", []
        if model_id:
            query += " WHERE model_id=?"; values.append(model_id)
        query += " ORDER BY created_at DESC, calibration_id DESC"
        return self._rows(query, self._calibration_payload, values)

    def record_drift(self, event_id: str, *, object_type: str, object_id: str, version: int, state: str, reason: str, evidence: Mapping[str, Any], created_at: str) -> None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("INSERT OR IGNORE INTO science_drift_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (event_id, object_type, object_id, version, state, reason, _dump(dict(evidence)), created_at))

    def list_drift(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_drift_events ORDER BY created_at DESC", lambda row: {"event_id": row["event_id"], "object_type": row["object_type"], "object_id": row["object_id"], "version": row["version"], "state": row["state"], "reason": row["reason"], "evidence": _load(row["evidence_json"], {}), "created_at": row["created_at"]})

    def write_journal(self, journal_date: str, payload: Mapping[str, Any], *, updated_at: str) -> None:
        self.initialize()
        with self._connect() as connection:
            connection.execute("""INSERT INTO science_journals VALUES (?, ?, ?) ON CONFLICT(journal_date) DO UPDATE SET
                payload_json=excluded.payload_json, updated_at=excluded.updated_at""", (journal_date, _dump(dict(payload)), updated_at))

    def latest_journal(self) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM science_journals ORDER BY journal_date DESC LIMIT 1").fetchone()
        return None if row is None else {"journal_date": row["journal_date"], "payload": _load(row["payload_json"], {}), "updated_at": row["updated_at"]}

    def list_features(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_features ORDER BY feature_id, version DESC", self._feature_payload)

    def list_hypotheses(self, *, state: str | None = None) -> list[dict[str, Any]]:
        query, values = "SELECT * FROM science_hypotheses", []
        if state:
            query += " WHERE state=?"; values.append(state)
        query += " ORDER BY registered_at DESC"
        return self._rows(query, self._hypothesis_payload, values)

    def list_experiments(self, *, kind: str | None = None) -> list[dict[str, Any]]:
        query, values = "SELECT e.*, r.result_json, r.recorded_at AS result_recorded_at FROM science_experiments e LEFT JOIN science_experiment_results r ON r.experiment_id=e.experiment_id", []
        if kind:
            query += " WHERE e.kind=?"; values.append(kind)
        query += " ORDER BY e.created_at DESC"
        return self._rows(query, self._experiment_payload, values)

    def list_indicators(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_indicators ORDER BY created_at DESC", self._indicator_payload)

    def list_models(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_models ORDER BY created_at DESC", self._model_payload)

    def list_wallet_sensors(self) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_wallet_sensors ORDER BY updated_at DESC", lambda row: {"wallet": row["wallet"], "metrics": _load(row["metrics_json"], {}), "evidence_confidence": row["evidence_confidence"], "updated_at": row["updated_at"]})

    def list_graveyard(self, *, search: str = "") -> list[dict[str, Any]]:
        query, values = "SELECT * FROM science_graveyard", []
        if search:
            query += " WHERE hypothesis_id LIKE ? OR reason LIKE ? OR payload_json LIKE ?"; values.extend([f"%{search}%"] * 3)
        query += " ORDER BY recorded_at DESC"
        return self._rows(query, lambda row: {"hypothesis_id": row["hypothesis_id"], "version": row["version"], "experiment_id": row["experiment_id"], "reason": row["reason"], "payload": _load(row["payload_json"], {}), "recorded_at": row["recorded_at"]}, values)

    def list_decisions(self, *, limit: int = 100) -> list[dict[str, Any]]:
        return self._rows("SELECT * FROM science_decisions ORDER BY created_at DESC LIMIT ?", lambda row: {"decision_id": row["decision_id"], "created_at": row["created_at"], "symbol": row["symbol"], "decision": row["decision"], "payload": _load(row["payload_json"], {})}, [max(1, min(limit, 500))])

    def health(self) -> dict[str, Any]:
        self.initialize()
        with self._connect() as connection:
            counts = {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("science_features", "science_hypotheses", "science_experiments", "science_indicators", "science_models", "science_forward_predictions", "science_graveyard", "science_decisions", "science_model_calibrations")}
        return {"database": str(self.path), "state": "READY", "counts": counts}

    def _rows(self, query: str, mapper: Any, values: list[Any] | tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [mapper(row) for row in rows]

    @staticmethod
    def _observation_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"observation_id": row["observation_id"], "kind": row["kind"], "source": row["source"],
                "source_event_id": row["source_event_id"], "wallet": row["wallet"], "symbol": row["symbol"],
                "event_at": row["event_at"], "received_at": row["received_at"], "normalized_at": row["normalized_at"],
                "network": row["network"], "raw_fingerprint": row["raw_fingerprint"], "schema_version": row["schema_version"],
                "code_sha": row["code_sha"], "config_hash": row["config_hash"],
                "quality_flags": _load(row["quality_flags_json"], {}), "payload": _load(row["payload_json"], {}),
                "payload_hash": row["payload_hash"], "persisted_at": row["persisted_at"]}

    @staticmethod
    def _feature_value_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"feature_value_id": row["feature_value_id"], "feature_id": row["feature_id"],
                "feature_version": row["feature_version"], "observation_id": row["observation_id"],
                "value": _load(row["value_json"], {}).get("value"), "missing": bool(row["missing"]),
                "source_observation_ids": _load(row["source_observation_ids_json"], []),
                "data_fingerprint": row["data_fingerprint"], "materialized_at": row["materialized_at"]}

    @staticmethod
    def _outcome_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"label_id": row["label_id"], "observation_id": row["observation_id"],
                "horizon_seconds": row["horizon_seconds"], "resolved_at": row["resolved_at"],
                "payload": _load(row["payload_json"], {}), "payload_hash": row["payload_hash"]}

    @staticmethod
    def _work_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {name: row[name] for name in row.keys()}

    @staticmethod
    def _discovery_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"discovery_id": row["discovery_id"], "family_id": row["family_id"], "family_version": row["family_version"],
                "state": row["state"], "payload": _load(row["payload_json"], {}), "payload_hash": row["payload_hash"],
                "created_at": row["created_at"], "registered_hypothesis_id": row["registered_hypothesis_id"]}

    @staticmethod
    def _feature_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"feature_id": row["feature_id"], "version": row["version"], "definition": _load(row["definition_json"], {}), "definition_hash": row["definition_hash"], "created_at": row["created_at"], "code_sha": row["code_sha"]}

    @staticmethod
    def _hypothesis_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"hypothesis_id": row["hypothesis_id"], "version": row["version"], "state": row["state"], "definition": _load(row["definition_json"], {}), "config_hash": row["config_hash"], "registered_at": row["registered_at"], "predecessor_id": row["predecessor_id"]}

    @staticmethod
    def _experiment_payload(row: sqlite3.Row) -> dict[str, Any]:
        payload = {"experiment_id": row["experiment_id"], "hypothesis_id": row["hypothesis_id"], "hypothesis_version": row["hypothesis_version"], "kind": row["kind"], "state": row["state"], "dataset_fingerprint": row["dataset_fingerprint"], "configuration": _load(row["configuration_json"], {}), "created_at": row["created_at"], "completed_at": row["completed_at"]}
        if "result_json" in row.keys() and row["result_json"]:
            payload["result"] = _load(row["result_json"], {})
            payload["result_recorded_at"] = row["result_recorded_at"]
        return payload

    @staticmethod
    def _result_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"experiment_id": row["experiment_id"], "result": _load(row["result_json"], {}), "result_hash": row["result_hash"], "recorded_at": row["recorded_at"]}

    @staticmethod
    def _indicator_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"indicator_id": row["indicator_id"], "version": row["version"], "state": row["state"], "provenance": _load(row["provenance_json"], {}), "created_at": row["created_at"], "predecessor_id": row["predecessor_id"]}

    @staticmethod
    def _model_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"model_id": row["model_id"], "version": row["version"], "state": row["state"], "definition": _load(row["definition_json"], {}), "created_at": row["created_at"], "predecessor_id": row["predecessor_id"]}

    @staticmethod
    def _calibration_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {"calibration_id": row["calibration_id"], "model_id": row["model_id"], "version": row["version"],
                "source_fingerprint": row["source_fingerprint"], "payload": _load(row["payload_json"], {}),
                "created_at": row["created_at"]}
