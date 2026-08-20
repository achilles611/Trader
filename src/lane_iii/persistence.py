"""Small, safety-critical persistence surface for Lane III Phase A.

This is not a market-data warehouse or a broker ledger.  It keeps only the
state whose loss would permit a dangerous restart assumption: admitted
artifact identity, active thesis/confidence references, intent correlation,
last authoritative safety observation, unresolved intent state, and operator
controls.  It persists no credentials, follower information, or raw market
payloads.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .contracts import (
    ConfidenceState,
    ExecutionIntent,
    KnowledgeState,
    MarketHypothesis,
    OperatorCommand,
    PositionKnowledge,
    SafetySnapshot,
    SessionRiskState,
    WorkingOrderKnowledge,
    canonical_hash,
    normalized_utc,
)


class PersistenceConflict(RuntimeError):
    """An immutable safety record conflicts with prior durable evidence."""


class RecoveryRefused(RuntimeError):
    """A recovery request does not supply enough authoritative evidence."""


class _ClosingConnection(sqlite3.Connection):
    """Windows-safe SQLite context manager: commit semantics plus close on exit."""

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> bool | None:
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


@dataclass(frozen=True)
class OperatorState:
    armed: bool = False
    paused_new_entries: bool = True
    flatten_latched: bool = False
    flatten_request_id: str | None = None

    def payload(self) -> dict[str, object]:
        return {
            "armed": self.armed,
            "paused_new_entries": self.paused_new_entries,
            "flatten_latched": self.flatten_latched,
            "flatten_request_id": self.flatten_request_id,
        }


@dataclass(frozen=True)
class PersistedIntent:
    intent_id: str
    intent_hash: str
    disposition: str
    reason_code: str
    recorded_at: str


class LaneIIISafetyStore:
    """A narrowly scoped durable safety ledger with explicit recovery only."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS lane_iii_state (
                    state_key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS lane_iii_operator_events (
                    event_id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    state_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS lane_iii_intents (
                    intent_id TEXT PRIMARY KEY,
                    intent_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    disposition TEXT NOT NULL,
                    reason_code TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, factory=_ClosingConnection)
        connection.row_factory = sqlite3.Row
        return connection

    def operator_state(self) -> OperatorState:
        payload = self._state_payload("operator_state")
        if payload is None:
            return OperatorState()
        return OperatorState(
            armed=bool(payload["armed"]),
            paused_new_entries=bool(payload["paused_new_entries"]),
            flatten_latched=bool(payload["flatten_latched"]),
            flatten_request_id=payload.get("flatten_request_id"),
        )

    def apply_operator_command(self, command: OperatorCommand, *, requested_at: str) -> OperatorState:
        if type(command) is not OperatorCommand:
            raise ValueError("Operator command must be explicit.")
        timestamp = normalized_utc(requested_at, "Operator command time")
        current = self.operator_state()
        if command is OperatorCommand.ARM:
            if current.flatten_latched:
                raise RecoveryRefused("A latched operator flatten must be reconciled outside L3-A before re-arming.")
            updated = OperatorState(armed=True, paused_new_entries=current.paused_new_entries)
        elif command is OperatorCommand.DISARM:
            updated = OperatorState(armed=False, paused_new_entries=True, flatten_latched=current.flatten_latched,
                                    flatten_request_id=current.flatten_request_id)
        elif command is OperatorCommand.PAUSE_NEW_ENTRIES:
            updated = OperatorState(armed=current.armed, paused_new_entries=True, flatten_latched=current.flatten_latched,
                                    flatten_request_id=current.flatten_request_id)
        elif command is OperatorCommand.RESUME_NEW_ENTRIES:
            if current.flatten_latched:
                raise RecoveryRefused("A latched operator flatten cannot be resumed by a strategy-side control.")
            updated = OperatorState(armed=current.armed, paused_new_entries=False)
        elif command is OperatorCommand.FLATTEN:
            event_id = "l3of-" + canonical_hash({"command": command.value, "requested_at": timestamp})[:32]
            updated = OperatorState(armed=False, paused_new_entries=True, flatten_latched=True, flatten_request_id=event_id)
        else:
            # Verification and inspection commands have audit semantics, not a
            # permission to reinterpret any position or broker state.
            updated = current

        event_id = "l3oe-" + canonical_hash({"command": command.value, "requested_at": timestamp, "state": updated.payload()})[:32]
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._put_state(connection, "operator_state", updated.payload(), timestamp)
            connection.execute(
                "INSERT OR IGNORE INTO lane_iii_operator_events(event_id, command, requested_at, state_json) VALUES (?, ?, ?, ?)",
                (event_id, command.value, timestamp, json.dumps(updated.payload(), sort_keys=True, separators=(",", ":"))),
            )
            connection.commit()
        return updated

    def record_active_hypothesis(self, hypothesis: MarketHypothesis, *, recorded_at: str) -> None:
        if type(hypothesis) is not MarketHypothesis:
            raise ValueError("Exact MarketHypothesis required for persistence.")
        timestamp = normalized_utc(recorded_at, "Hypothesis record time")
        self._set_state("active_hypothesis", hypothesis.payload(), timestamp)

    def active_hypothesis(self) -> dict[str, object] | None:
        return self._state_payload("active_hypothesis")

    def record_confidence(self, confidence: ConfidenceState, *, recorded_at: str) -> None:
        if type(confidence) is not ConfidenceState:
            raise ValueError("Exact ConfidenceState required for persistence.")
        timestamp = normalized_utc(recorded_at, "Confidence record time")
        self._set_state("confidence_state", {"payload": confidence.payload(), "snapshot_hash": confidence.snapshot_hash}, timestamp)

    def confidence_state(self) -> dict[str, object] | None:
        return self._state_payload("confidence_state")

    def record_safety_snapshot(self, snapshot: SafetySnapshot) -> None:
        if type(snapshot) is not SafetySnapshot:
            raise ValueError("Exact SafetySnapshot required for persistence.")
        payload = _safety_payload(snapshot)
        self._set_state("latest_safety_snapshot", payload, normalized_utc(snapshot.observed_at, "Safety snapshot time"))

    def latest_safety_snapshot(self) -> SafetySnapshot | None:
        payload = self._state_payload("latest_safety_snapshot")
        return None if payload is None else _safety_snapshot(payload)

    def record_intent(
        self,
        intent: ExecutionIntent,
        *,
        disposition: str,
        reason_code: str,
        recorded_at: str,
    ) -> PersistedIntent:
        """Persist an immutable correlation record or return its equivalent prior record.

        The unique ID/hash pair ensures a replay cannot turn into a second
        future submission.  A conflicting reuse is an integrity fault, never a
        rewrite of the original request.
        """
        if type(intent) is not ExecutionIntent:
            raise ValueError("Exact ExecutionIntent required for persistence.")
        timestamp = normalized_utc(recorded_at, "Intent record time")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            prior = connection.execute(
                "SELECT intent_id, intent_hash, disposition, reason_code, recorded_at FROM lane_iii_intents WHERE intent_id=?",
                (intent.intent_id,),
            ).fetchone()
            if prior is not None:
                connection.commit()
                if prior["intent_hash"] != intent.intent_hash:
                    raise PersistenceConflict("Intent ID was reused with a non-equivalent immutable request.")
                return PersistedIntent(
                    intent_id=prior["intent_id"], intent_hash=prior["intent_hash"], disposition=prior["disposition"],
                    reason_code=prior["reason_code"], recorded_at=prior["recorded_at"],
                )
            connection.execute(
                """INSERT INTO lane_iii_intents
                    (intent_id, intent_hash, payload_json, disposition, reason_code, recorded_at, resolved_at)
                    VALUES (?, ?, ?, ?, ?, ?, NULL)""",
                (
                    intent.intent_id, intent.intent_hash,
                    json.dumps(intent.payload(), sort_keys=True, separators=(",", ":")),
                    disposition, reason_code, timestamp,
                ),
            )
            connection.commit()
        return PersistedIntent(intent.intent_id, intent.intent_hash, disposition, reason_code, timestamp)

    def unresolved_intents(self) -> tuple[PersistedIntent, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT intent_id, intent_hash, disposition, reason_code, recorded_at
                    FROM lane_iii_intents WHERE resolved_at IS NULL ORDER BY recorded_at, intent_id"""
            ).fetchall()
        return tuple(PersistedIntent(**dict(row)) for row in rows)

    def resolve_intent_after_authoritative_recovery(
        self, intent_id: str, *, snapshot: SafetySnapshot, resolved_at: str,
    ) -> None:
        """Mark only one correlation record resolved after verified-flat evidence.

        This function does not alter local exposure, create a broker action, or
        infer a position.  It exists to make recovery an explicit, audited
        process instead of a restart rebaseline.
        """
        if not isinstance(intent_id, str) or not intent_id:
            raise ValueError("Intent identity is required for recovery.")
        timestamp = normalized_utc(resolved_at, "Recovery time")
        if (
            snapshot.broker_state is not KnowledgeState.FRESH
            or snapshot.position_state is not PositionKnowledge.FLAT
            or snapshot.working_orders is not WorkingOrderKnowledge.CLEAR
        ):
            raise RecoveryRefused("Recovery requires fresh broker state, exact flat position, and clear working orders.")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT resolved_at FROM lane_iii_intents WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise RecoveryRefused("Unknown intent cannot be recovered.")
            if row["resolved_at"] is None:
                connection.execute("UPDATE lane_iii_intents SET resolved_at=? WHERE intent_id=?", (timestamp, intent_id))
            connection.commit()

    def _state_payload(self, key: str) -> dict[str, object] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT payload_json FROM lane_iii_state WHERE state_key=?", (key,)).fetchone()
        return None if row is None else json.loads(row["payload_json"])

    def _set_state(self, key: str, payload: dict[str, object], updated_at: str) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self._put_state(connection, key, payload, updated_at)
            connection.commit()

    @staticmethod
    def _put_state(connection: sqlite3.Connection, key: str, payload: dict[str, object], updated_at: str) -> None:
        connection.execute(
            """INSERT INTO lane_iii_state(state_key, payload_json, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(state_key) DO UPDATE SET payload_json=excluded.payload_json, updated_at=excluded.updated_at""",
            (key, json.dumps(payload, sort_keys=True, separators=(",", ":")), updated_at),
        )


def _safety_payload(snapshot: SafetySnapshot) -> dict[str, object]:
    return {
        "observed_at": normalized_utc(snapshot.observed_at, "Safety snapshot time"),
        "market_data": snapshot.market_data.value,
        "broker_state": snapshot.broker_state.value,
        "position_state": snapshot.position_state.value,
        "position_quantity": snapshot.position_quantity,
        "working_orders": snapshot.working_orders.value,
        "session_risk": {
            "session_id": snapshot.session_risk.session_id,
            "daily_loss": str(snapshot.session_risk.daily_loss),
            "session_loss": str(snapshot.session_risk.session_loss),
        },
    }


def _safety_snapshot(payload: dict[str, object]) -> SafetySnapshot:
    risk = payload["session_risk"]
    if not isinstance(risk, dict):
        raise PersistenceConflict("Persisted safety snapshot is malformed.")
    return SafetySnapshot(
        observed_at=str(payload["observed_at"]), market_data=KnowledgeState(str(payload["market_data"])),
        broker_state=KnowledgeState(str(payload["broker_state"])), position_state=PositionKnowledge(str(payload["position_state"])),
        position_quantity=payload["position_quantity"] if payload["position_quantity"] is None else int(payload["position_quantity"]),
        working_orders=WorkingOrderKnowledge(str(payload["working_orders"])),
        session_risk=SessionRiskState(str(risk["session_id"]), risk["daily_loss"], risk["session_loss"]),
    )
