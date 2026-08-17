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

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=5000")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self._connect() as connection:
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
                CREATE INDEX IF NOT EXISTS idx_science_hypothesis_state ON science_hypotheses(state, registered_at DESC);
                CREATE INDEX IF NOT EXISTS idx_science_experiment_hypothesis ON science_experiments(hypothesis_id, hypothesis_version, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_science_prediction_experiment ON science_forward_predictions(experiment_id, predicted_at DESC);
                CREATE INDEX IF NOT EXISTS idx_science_decision_time ON science_decisions(created_at DESC);
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
                """
            )

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
            counts = {table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("science_features", "science_hypotheses", "science_experiments", "science_indicators", "science_models", "science_forward_predictions", "science_graveyard", "science_decisions")}
        return {"database": str(self.path), "state": "READY", "counts": counts}

    def _rows(self, query: str, mapper: Any, values: list[Any] | tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        self.initialize()
        with self._connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [mapper(row) for row in rows]

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
