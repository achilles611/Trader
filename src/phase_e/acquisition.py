"""Phase E.6 outcome-blind prospective acquisition.

This module is deliberately a small operational layer over the frozen E.5
contract.  It materializes the E.5 schedule, records predictor-side
membership and resolution *metadata*, and never accepts an outcome value or
calls the E.5 inference capability.

The database is an acquisition control record, not an outcome repository.
It must be deployed separately from the sealed outcome store described by the
E.5 protocol.
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Iterator, Mapping

from .prospective import (
    E5_OBSERVATION_SCHEMA,
    E5_PROTOCOL_SCHEMA,
    DesignObservation,
    EvidenceState,
    ProtocolIntegrityError,
    ScheduledBlock,
    classify_evidence,
    load_frozen_protocol,
    scheduled_blocks,
    validate_protocol_document,
    wallet_cohort,
)
from .types import canonical_hash, normalized_utc, storage_json


E6_CODE_VERSION = "phase-e6-prospective-acquisition-v1"
E6_ACQUISITION_SCHEMA = "phase-e6-prospective-acquisition-v1"
E6_EXPECTED_PROTOCOL_ID = "e5p-ae597d81614b76feba54168141de6a73"
E6_EXPECTED_PROTOCOL_HASH = "ae597d81614b76feba54168141de6a738876107639213a56a1c1aaa21c17c27f"


class AcquisitionError(RuntimeError):
    """Base E.6 acquisition error."""


class AcquisitionProtocolError(AcquisitionError):
    """The exact frozen E.5 contract was not supplied."""


class AcquisitionStateError(AcquisitionError):
    """An operation would rewrite the fixed acquisition history."""


class AdmissionRefused(AcquisitionError):
    """A prospective candidate cannot join frozen membership."""


class ResolutionRefused(AcquisitionError):
    """Resolution metadata conflicts with the immutable admission record."""


class BlockState(StrEnum):
    SCHEDULED = "scheduled"
    OPEN = "open"
    ACQUIRING = "acquiring"
    SEALED = "sealed"
    AWAITING_RESOLUTION = "awaiting_resolution"
    FINALIZED = "finalized"
    PROTOCOL_MISMATCH = "protocol_mismatch"
    CONTAMINATION_DETECTED = "contamination_detected"
    ACQUISITION_FAILED = "acquisition_failed"
    RESOLUTION_FAILED = "resolution_failed"
    INTEGRITY_FAILED = "integrity_failed"
    HARD_STOP_REACHED = "hard_stop_reached"


_MEMBERSHIP_SEALED_STATES = frozenset({
    BlockState.SEALED,
    BlockState.AWAITING_RESOLUTION,
    BlockState.FINALIZED,
    BlockState.CONTAMINATION_DETECTED,
    BlockState.ACQUISITION_FAILED,
    BlockState.RESOLUTION_FAILED,
    BlockState.INTEGRITY_FAILED,
    BlockState.HARD_STOP_REACHED,
})
_TERMINAL_BLOCK_STATES = frozenset({
    BlockState.FINALIZED,
    BlockState.PROTOCOL_MISMATCH,
    BlockState.CONTAMINATION_DETECTED,
    BlockState.ACQUISITION_FAILED,
    BlockState.RESOLUTION_FAILED,
    BlockState.INTEGRITY_FAILED,
    BlockState.HARD_STOP_REACHED,
})


def _utc(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Timestamp must be ISO-8601 text with an explicit offset.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must have an explicit UTC offset.")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class AcquisitionCandidate:
    """One outcome-free candidate from the new prospective source partition."""

    observation: DesignObservation
    source_namespace: str
    received_at: str

    def __post_init__(self) -> None:
        if not isinstance(self.source_namespace, str) or not self.source_namespace:
            raise ValueError("Candidate source namespace is required.")
        _utc(self.received_at)

    def identity_payload(self) -> dict[str, Any]:
        return {
            "schema": "phase-e6-acquisition-candidate-v1",
            "observation": self.observation.identity_payload(),
            "source_namespace": self.source_namespace,
        }

    @property
    def candidate_id(self) -> str:
        return "e6c-" + canonical_hash(self.identity_payload())[:32]


@dataclass(frozen=True)
class ResolutionMetadata:
    """Narrow resolution-side metadata.  Outcome values are intentionally absent."""

    observation_id: str
    resolution_event_at: str | None
    ingested_at: str | None
    structurally_unresolved: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.observation_id, str) or not self.observation_id:
            raise ValueError("Resolution observation identity is required.")
        if (self.resolution_event_at is None) != (self.ingested_at is None):
            raise ValueError("Resolution event and ingestion timestamps must be supplied together.")
        if self.resolution_event_at is not None:
            _utc(self.resolution_event_at)
            _utc(self.ingested_at or "")
        if not isinstance(self.structurally_unresolved, bool):
            raise ValueError("Structural-unresolved state must be boolean.")

    def payload(self) -> dict[str, Any]:
        return {
            "schema": "phase-e6-resolution-metadata-v1",
            "observation_id": self.observation_id,
            "resolution_event_at": (
                normalized_utc(self.resolution_event_at) if self.resolution_event_at is not None else None
            ),
            "ingested_at": normalized_utc(self.ingested_at) if self.ingested_at is not None else None,
            "structurally_unresolved": self.structurally_unresolved,
        }


class PhaseE6Acquisition:
    """Crash-safe, outcome-blind executor for the exact frozen E.5 protocol.

    It has no method that reads returns, estimates effects, or performs
    inference.  Every membership-changing operation is serialized using a
    SQLite immediate transaction and protected by database uniqueness rules.
    """

    TRADING_AUTHORITY = False
    EXECUTION_AUTHORITY = False
    SIGNAL_AUTHORITY = False
    PREDICTION_AUTHORITY = False

    def __init__(self, database_path: str | Path, protocol_path: str | Path) -> None:
        self.path = Path(database_path)
        self.protocol_path = Path(protocol_path)
        self._initialized = False

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
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
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _verify_exact_protocol(document: Mapping[str, Any]) -> dict[str, Any]:
        try:
            checked = validate_protocol_document(document)
        except (ProtocolIntegrityError, ValueError) as exc:
            raise AcquisitionProtocolError("E.6 refuses an invalid frozen E.5 protocol.") from exc
        identity = checked.get("identity", {})
        if (
            checked.get("schema") != E5_PROTOCOL_SCHEMA
            or identity.get("protocol_id") != E6_EXPECTED_PROTOCOL_ID
            or identity.get("protocol_hash") != E6_EXPECTED_PROTOCOL_HASH
        ):
            raise AcquisitionProtocolError("E.6 requires the exact E.5 frozen protocol identity and hash.")
        return checked

    def _load_exact_protocol(self) -> dict[str, Any]:
        try:
            return self._verify_exact_protocol(load_frozen_protocol(self.protocol_path))
        except (OSError, ProtocolIntegrityError, ValueError) as exc:
            raise AcquisitionProtocolError("E.6 cannot read the frozen E.5 protocol artifact.") from exc

    def initialize(self) -> None:
        """Verify and persist the immutable protocol and all sixty schedule rows."""
        if self._initialized:
            # Recheck the on-disk authority before every operational action.  A
            # changed or missing artifact fails closed rather than allowing a
            # long-running collector to proceed on stale assumptions.
            self._load_exact_protocol()
            return
        document = self._load_exact_protocol()
        identity = document["identity"]
        blocks = scheduled_blocks(document)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._write() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS phase_e6_protocols (
                    protocol_id TEXT PRIMARY KEY,
                    protocol_hash TEXT NOT NULL UNIQUE,
                    schema_version TEXT NOT NULL,
                    protocol_json TEXT NOT NULL,
                    protocol_path TEXT NOT NULL,
                    code_version TEXT NOT NULL,
                    registered_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e6_blocks (
                    block_id TEXT PRIMARY KEY,
                    protocol_id TEXT NOT NULL REFERENCES phase_e6_protocols(protocol_id),
                    ordinal INTEGER NOT NULL UNIQUE CHECK(ordinal >= 0 AND ordinal < 60),
                    cohort INTEGER NOT NULL,
                    epoch_start TEXT NOT NULL,
                    scheduled_start TEXT NOT NULL,
                    scheduled_end TEXT NOT NULL,
                    exposure_end TEXT NOT NULL,
                    block_hash TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL CHECK(state IN (
                        'scheduled', 'open', 'acquiring', 'sealed', 'awaiting_resolution', 'finalized',
                        'protocol_mismatch', 'contamination_detected', 'acquisition_failed', 'resolution_failed',
                        'integrity_failed', 'hard_stop_reached'
                    )),
                    state_version INTEGER NOT NULL DEFAULT 0,
                    opened_at TEXT,
                    sealed_at TEXT,
                    finalized_at TEXT,
                    UNIQUE(protocol_id, ordinal)
                );
                CREATE TABLE IF NOT EXISTS phase_e6_block_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_id TEXT NOT NULL REFERENCES phase_e6_blocks(block_id),
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS phase_e6_candidates (
                    candidate_id TEXT PRIMARY KEY,
                    block_id TEXT NOT NULL REFERENCES phase_e6_blocks(block_id),
                    source_event_id TEXT NOT NULL UNIQUE,
                    candidate_json TEXT NOT NULL,
                    candidate_hash TEXT NOT NULL UNIQUE,
                    source_namespace TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    decision TEXT NOT NULL CHECK(decision IN ('ADMITTED', 'REJECTED')),
                    decision_reason TEXT NOT NULL,
                    decided_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e6_observations (
                    observation_id TEXT PRIMARY KEY,
                    block_id TEXT NOT NULL REFERENCES phase_e6_blocks(block_id),
                    wallet_id TEXT NOT NULL UNIQUE,
                    source_event_id TEXT NOT NULL UNIQUE,
                    transaction_id TEXT,
                    endpoint_family_id TEXT,
                    campaign_id TEXT,
                    symbol TEXT NOT NULL,
                    anchor_at TEXT NOT NULL,
                    exposure_end_at TEXT NOT NULL,
                    observation_json TEXT NOT NULL UNIQUE,
                    observation_hash TEXT NOT NULL UNIQUE,
                    candidate_id TEXT NOT NULL UNIQUE REFERENCES phase_e6_candidates(candidate_id),
                    admitted_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e6_resolution_metadata (
                    observation_id TEXT PRIMARY KEY REFERENCES phase_e6_observations(observation_id),
                    metadata_json TEXT NOT NULL,
                    metadata_hash TEXT NOT NULL UNIQUE,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e6_late_resolution_metadata (
                    late_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observation_id TEXT NOT NULL REFERENCES phase_e6_observations(observation_id),
                    metadata_json TEXT NOT NULL,
                    metadata_hash TEXT NOT NULL UNIQUE,
                    recorded_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e6_maturity_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observation_id TEXT NOT NULL REFERENCES phase_e6_observations(observation_id),
                    state TEXT NOT NULL,
                    classified_at TEXT NOT NULL,
                    reason TEXT,
                    event_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS phase_e6_access_audit (
                    access_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    access_kind TEXT NOT NULL CHECK(access_kind IN ('ACQUISITION', 'RESOLUTION', 'SCIENTIFIC_EVALUATION')),
                    occurred_at TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    audit_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS phase_e6_integrity_events (
                    integrity_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    block_id TEXT REFERENCES phase_e6_blocks(block_id),
                    occurred_at TEXT NOT NULL,
                    category TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    integrity_hash TEXT NOT NULL UNIQUE
                );
                CREATE TRIGGER IF NOT EXISTS phase_e6_protocols_no_update
                    BEFORE UPDATE ON phase_e6_protocols BEGIN SELECT RAISE(ABORT, 'E.6 protocol is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_protocols_no_delete
                    BEFORE DELETE ON phase_e6_protocols BEGIN SELECT RAISE(ABORT, 'E.6 protocol cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_blocks_no_delete
                    BEFORE DELETE ON phase_e6_blocks BEGIN SELECT RAISE(ABORT, 'E.6 blocks cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_blocks_schedule_immutable
                    BEFORE UPDATE OF protocol_id,ordinal,cohort,epoch_start,scheduled_start,scheduled_end,exposure_end,block_hash
                    ON phase_e6_blocks BEGIN SELECT RAISE(ABORT, 'E.6 fixed schedule is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_block_events_no_update
                    BEFORE UPDATE ON phase_e6_block_events BEGIN SELECT RAISE(ABORT, 'E.6 block events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_block_events_no_delete
                    BEFORE DELETE ON phase_e6_block_events BEGIN SELECT RAISE(ABORT, 'E.6 block events cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_candidates_no_update
                    BEFORE UPDATE ON phase_e6_candidates BEGIN SELECT RAISE(ABORT, 'E.6 candidate decisions are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_candidates_no_delete
                    BEFORE DELETE ON phase_e6_candidates BEGIN SELECT RAISE(ABORT, 'E.6 candidate decisions cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_observations_no_update
                    BEFORE UPDATE ON phase_e6_observations BEGIN SELECT RAISE(ABORT, 'E.6 membership is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_observations_no_delete
                    BEFORE DELETE ON phase_e6_observations BEGIN SELECT RAISE(ABORT, 'E.6 membership cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_resolution_no_update
                    BEFORE UPDATE ON phase_e6_resolution_metadata BEGIN SELECT RAISE(ABORT, 'E.6 resolution metadata is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_resolution_no_delete
                    BEFORE DELETE ON phase_e6_resolution_metadata BEGIN SELECT RAISE(ABORT, 'E.6 resolution metadata cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_late_resolution_no_update
                    BEFORE UPDATE ON phase_e6_late_resolution_metadata BEGIN SELECT RAISE(ABORT, 'E.6 late resolution metadata is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_late_resolution_no_delete
                    BEFORE DELETE ON phase_e6_late_resolution_metadata BEGIN SELECT RAISE(ABORT, 'E.6 late resolution metadata cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_maturity_no_update
                    BEFORE UPDATE ON phase_e6_maturity_events BEGIN SELECT RAISE(ABORT, 'E.6 maturity events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_maturity_no_delete
                    BEFORE DELETE ON phase_e6_maturity_events BEGIN SELECT RAISE(ABORT, 'E.6 maturity events cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_access_no_update
                    BEFORE UPDATE ON phase_e6_access_audit BEGIN SELECT RAISE(ABORT, 'E.6 access audit is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_access_no_delete
                    BEFORE DELETE ON phase_e6_access_audit BEGIN SELECT RAISE(ABORT, 'E.6 access audit cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_integrity_no_update
                    BEFORE UPDATE ON phase_e6_integrity_events BEGIN SELECT RAISE(ABORT, 'E.6 integrity events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e6_integrity_no_delete
                    BEFORE DELETE ON phase_e6_integrity_events BEGIN SELECT RAISE(ABORT, 'E.6 integrity events cannot be deleted'); END;
            """)
            existing = connection.execute(
                "SELECT * FROM phase_e6_protocols WHERE protocol_id=?", (identity["protocol_id"],),
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO phase_e6_protocols VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        identity["protocol_id"], identity["protocol_hash"], document["schema"], storage_json(document),
                        str(self.protocol_path.resolve()), E6_CODE_VERSION, normalized_utc(identity["frozen_at"]),
                    ),
                )
            elif (
                existing["protocol_hash"] != identity["protocol_hash"]
                or existing["protocol_json"] != storage_json(document)
            ):
                raise AcquisitionProtocolError("Persisted E.6 protocol differs from the frozen E.5 artifact.")
            for block in blocks:
                self._materialize_block(connection, identity["protocol_id"], block)
            count = int(connection.execute("SELECT COUNT(*) FROM phase_e6_blocks").fetchone()[0])
            if count != 60:
                raise AcquisitionProtocolError("E.6 requires exactly sixty immutable schedule blocks.")
        self._initialized = True

    @staticmethod
    def _materialize_block(connection: sqlite3.Connection, protocol_id: str, block: ScheduledBlock) -> None:
        values = (
            block.block_id, protocol_id, block.ordinal, block.cohort, block.epoch_start,
            block.sample_start, block.sample_end, block.exposure_end, block.block_hash,
            BlockState.SCHEDULED.value,
        )
        existing = connection.execute(
            "SELECT * FROM phase_e6_blocks WHERE block_id=?", (block.block_id,),
        ).fetchone()
        if existing is None:
            connection.execute(
                """INSERT INTO phase_e6_blocks(block_id,protocol_id,ordinal,cohort,epoch_start,scheduled_start,
                   scheduled_end,exposure_end,block_hash,state) VALUES (?,?,?,?,?,?,?,?,?,?)""",
                values,
            )
            PhaseE6Acquisition._block_event(connection, block.block_id, "SCHEDULE_MATERIALIZED", block.epoch_start, {
                "ordinal": block.ordinal, "block_hash": block.block_hash, "cohort": block.cohort,
                "scheduled_start": block.sample_start, "scheduled_end": block.sample_end,
                "exposure_end": block.exposure_end,
            })
            return
        expected = dict(zip((
            "block_id", "protocol_id", "ordinal", "cohort", "epoch_start", "scheduled_start",
            "scheduled_end", "exposure_end", "block_hash",
        ), values[:-1], strict=True))
        if any(existing[key] != value for key, value in expected.items()):
            raise AcquisitionProtocolError("Persisted E.6 schedule differs from frozen E.5 schedule.")

    @staticmethod
    def _block_event(
        connection: sqlite3.Connection, block_id: str, event_type: str, occurred_at: str, payload: Mapping[str, Any],
    ) -> None:
        at = normalized_utc(occurred_at)
        event = {"block_id": block_id, "event_type": event_type, "occurred_at": at, "payload": dict(payload)}
        connection.execute(
            "INSERT OR IGNORE INTO phase_e6_block_events(block_id,event_type,occurred_at,payload_json,event_hash) VALUES (?,?,?,?,?)",
            (block_id, event_type, at, storage_json(payload), canonical_hash(event)),
        )

    @staticmethod
    def _audit(
        connection: sqlite3.Connection, kind: str, occurred_at: str, target_id: str, payload: Mapping[str, Any],
    ) -> None:
        at = normalized_utc(occurred_at)
        event = {"kind": kind, "occurred_at": at, "target_id": target_id, "payload": dict(payload)}
        connection.execute(
            "INSERT OR IGNORE INTO phase_e6_access_audit(access_kind,occurred_at,target_id,payload_json,audit_hash) VALUES (?,?,?,?,?)",
            (kind, at, target_id, storage_json(payload), canonical_hash(event)),
        )

    @staticmethod
    def _integrity_event(
        connection: sqlite3.Connection, block_id: str | None, occurred_at: str, category: str, payload: Mapping[str, Any],
    ) -> None:
        at = normalized_utc(occurred_at)
        event = {"block_id": block_id, "occurred_at": at, "category": category, "payload": dict(payload)}
        connection.execute(
            "INSERT OR IGNORE INTO phase_e6_integrity_events(block_id,occurred_at,category,payload_json,integrity_hash) VALUES (?,?,?,?,?)",
            (block_id, at, category, storage_json(payload), canonical_hash(event)),
        )

    def _document(self, connection: sqlite3.Connection) -> dict[str, Any]:
        row = connection.execute("SELECT protocol_json FROM phase_e6_protocols WHERE protocol_id=?", (E6_EXPECTED_PROTOCOL_ID,)).fetchone()
        if row is None:
            raise AcquisitionProtocolError("E.6 is not initialized with the frozen protocol.")
        try:
            document = json.loads(row["protocol_json"])
        except json.JSONDecodeError as exc:  # pragma: no cover - protected by immutable insert
            raise AcquisitionProtocolError("Persisted E.6 protocol is malformed.") from exc
        return self._verify_exact_protocol(document)

    @staticmethod
    def _require_block(connection: sqlite3.Connection, block_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM phase_e6_blocks WHERE block_id=?", (block_id,)).fetchone()
        if row is None:
            raise AcquisitionStateError("Unknown fixed E.6 block.")
        return row

    @staticmethod
    def _transition(
        connection: sqlite3.Connection, row: sqlite3.Row, target: BlockState, *, at: str, event_type: str,
        payload: Mapping[str, Any],
    ) -> sqlite3.Row:
        current = BlockState(row["state"])
        if current == target:
            return row
        if current in _TERMINAL_BLOCK_STATES:
            raise AcquisitionStateError(f"Block {row['block_id']} is terminal in state {current.value}.")
        fields = ["state=?", "state_version=state_version+1"]
        values: list[Any] = [target.value]
        if target == BlockState.OPEN:
            fields.append("opened_at=?")
            values.append(normalized_utc(at))
        if target in _MEMBERSHIP_SEALED_STATES and row["sealed_at"] is None:
            fields.append("sealed_at=?")
            values.append(normalized_utc(at))
        if target == BlockState.FINALIZED:
            fields.append("finalized_at=?")
            values.append(normalized_utc(at))
        values.extend([row["block_id"], current.value, int(row["state_version"])])
        connection.execute(
            f"UPDATE phase_e6_blocks SET {', '.join(fields)} WHERE block_id=? AND state=? AND state_version=?", values,
        )
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise AcquisitionStateError("Concurrent E.6 block transition lost compare-and-swap.")
        PhaseE6Acquisition._block_event(connection, row["block_id"], event_type, at, {
            "prior_state": current.value, "state": target.value, **dict(payload),
        })
        return PhaseE6Acquisition._require_block(connection, row["block_id"])

    def open_block(self, block_id: str, *, at: str) -> dict[str, Any]:
        """Open only the exact precommitted wall-clock window; never reschedule it."""
        self.initialize()
        now = _utc(at)
        missed = False
        with self._write() as connection:
            self._document(connection)
            row = self._require_block(connection, block_id)
            state = BlockState(row["state"])
            if state in {BlockState.OPEN, BlockState.ACQUIRING}:
                if now >= _utc(row["scheduled_end"]):
                    sealed = self._transition(connection, row, BlockState.SEALED, at=at, event_type="BLOCK_SEALED_ON_RECOVERY", payload={
                        "membership_cutoff": row["scheduled_end"],
                    })
                    row = self._transition(connection, sealed, BlockState.AWAITING_RESOLUTION, at=at, event_type="AWAITING_RESOLUTION", payload={
                        "membership_sealed": True, "recovery": True,
                    })
                return self._block_payload(connection, row)
            if state != BlockState.SCHEDULED:
                raise AcquisitionStateError(f"Block cannot be opened from {state.value}.")
            start, end = _utc(row["scheduled_start"]), _utc(row["scheduled_end"])
            if now < start:
                raise AcquisitionStateError("Block cannot open before its frozen scheduled start.")
            if now >= end:
                self._transition(connection, row, BlockState.ACQUISITION_FAILED, at=at, event_type="BLOCK_MISSED", payload={
                    "scheduled_start": row["scheduled_start"], "scheduled_end": row["scheduled_end"],
                    "reason": "PROCESS_NOT_OPEN_DURING_FIXED_WINDOW",
                })
                missed = True
            else:
                row = self._transition(connection, row, BlockState.OPEN, at=at, event_type="BLOCK_OPENED", payload={
                    "scheduled_start": row["scheduled_start"], "scheduled_end": row["scheduled_end"],
                })
                payload = self._block_payload(connection, row)
        if missed:
            raise AcquisitionStateError("Expired scheduled block was recorded as missed and cannot be recreated.")
        return payload

    def seal_block(self, block_id: str, *, at: str) -> dict[str, Any]:
        """Irreversibly end membership at the frozen scheduled end, never earlier."""
        self.initialize()
        now = _utc(at)
        with self._write() as connection:
            self._document(connection)
            row = self._require_block(connection, block_id)
            state = BlockState(row["state"])
            if state in {BlockState.SEALED, BlockState.AWAITING_RESOLUTION, BlockState.FINALIZED}:
                return self._block_payload(connection, row)
            if state not in {BlockState.OPEN, BlockState.ACQUIRING}:
                raise AcquisitionStateError(f"Block cannot be sealed from {state.value}.")
            if now < _utc(row["scheduled_end"]):
                raise AcquisitionStateError("Block cannot seal before its frozen scheduled end.")
            row = self._transition(connection, row, BlockState.SEALED, at=at, event_type="BLOCK_SEALED", payload={
                "membership_cutoff": row["scheduled_end"],
            })
            row = self._transition(connection, row, BlockState.AWAITING_RESOLUTION, at=at, event_type="AWAITING_RESOLUTION", payload={
                "membership_sealed": True,
            })
            return self._block_payload(connection, row)

    def recover(self, *, at: str) -> dict[str, int]:
        """Recover without shifting timing or reopening membership after a restart."""
        self.initialize()
        now = _utc(at)
        transitions: Counter[str] = Counter()
        with self._write() as connection:
            self._document(connection)
            hard_stop = _utc(self._document(connection)["stopping"]["hard_stop"])
            rows = connection.execute("SELECT * FROM phase_e6_blocks ORDER BY ordinal").fetchall()
            for row in rows:
                state = BlockState(row["state"])
                start, end = _utc(row["scheduled_start"]), _utc(row["scheduled_end"])
                if state == BlockState.SCHEDULED and now >= end:
                    target = BlockState.HARD_STOP_REACHED if now >= hard_stop and start >= hard_stop else BlockState.ACQUISITION_FAILED
                    event = "HARD_STOP_REACHED" if target == BlockState.HARD_STOP_REACHED else "BLOCK_MISSED"
                    self._transition(connection, row, target, at=at, event_type=event, payload={
                        "scheduled_start": row["scheduled_start"], "scheduled_end": row["scheduled_end"],
                        "reason": "RECOVERY_DID_NOT_RECREATE_EXPIRED_BLOCK",
                    })
                    transitions[target.value] += 1
                elif state in {BlockState.OPEN, BlockState.ACQUIRING} and now >= end:
                    sealed = self._transition(connection, row, BlockState.SEALED, at=at, event_type="BLOCK_SEALED_ON_RECOVERY", payload={
                        "membership_cutoff": row["scheduled_end"],
                    })
                    self._transition(connection, sealed, BlockState.AWAITING_RESOLUTION, at=at, event_type="AWAITING_RESOLUTION", payload={
                        "membership_sealed": True, "recovery": True,
                    })
                    transitions[BlockState.AWAITING_RESOLUTION.value] += 1
            self._integrity_event(connection, None, at, "RECOVERY", {"transitions": dict(transitions)})
        return dict(transitions)

    def admit_candidate(self, candidate: AcquisitionCandidate) -> dict[str, Any]:
        """Make one serial, immutable membership decision using predictor-side fields only."""
        self.initialize()
        observation = candidate.observation
        with self._write() as connection:
            document = self._document(connection)
            row = self._require_block(connection, observation.block_id)
            candidate_json = candidate.identity_payload()
            candidate_hash = canonical_hash(candidate_json)
            prior = connection.execute(
                "SELECT * FROM phase_e6_candidates WHERE candidate_id=?", (candidate.candidate_id,),
            ).fetchone()
            if prior is not None:
                if prior["candidate_hash"] != candidate_hash:
                    raise AcquisitionStateError("Candidate identity collision has different immutable payload.")
                return self._candidate_payload(prior)
            existing_source = connection.execute(
                "SELECT candidate_id FROM phase_e6_candidates WHERE source_event_id=?", (observation.source_event_id,),
            ).fetchone()
            if existing_source is not None:
                raise AdmissionRefused("Source event already has an immutable E.6 candidate decision.")
            reason = self._admission_reason(connection, document, row, candidate)
            decision = "ADMITTED" if reason is None else "REJECTED"
            decided_at = normalized_utc(candidate.received_at)
            connection.execute(
                """INSERT INTO phase_e6_candidates VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    candidate.candidate_id, observation.block_id, observation.source_event_id, storage_json(candidate_json),
                    candidate_hash, candidate.source_namespace, decided_at, decision, reason or "FROZEN_RULES_SATISFIED", decided_at,
                ),
            )
            self._audit(connection, "ACQUISITION", decided_at, candidate.candidate_id, {
                "decision": decision, "block_id": observation.block_id, "reason": reason or "FROZEN_RULES_SATISFIED",
            })
            candidate_row = connection.execute(
                "SELECT * FROM phase_e6_candidates WHERE candidate_id=?", (candidate.candidate_id,),
            ).fetchone()
            if reason is not None:
                return self._candidate_payload(candidate_row)
            state = BlockState(row["state"])
            if state == BlockState.OPEN:
                row = self._transition(connection, row, BlockState.ACQUIRING, at=decided_at, event_type="ACQUISITION_STARTED", payload={})
            observation_json = observation.identity_payload()
            connection.execute(
                """INSERT INTO phase_e6_observations(observation_id,block_id,wallet_id,source_event_id,transaction_id,
                   endpoint_family_id,campaign_id,symbol,anchor_at,exposure_end_at,observation_json,observation_hash,candidate_id,admitted_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    observation.observation_id, observation.block_id, observation.wallet_id, observation.source_event_id,
                    observation.transaction_id, observation.endpoint_family_id, observation.campaign_id, observation.symbol,
                    normalized_utc(observation.anchor_at), normalized_utc(observation.exposure_end_at), storage_json(observation_json),
                    canonical_hash(observation_json), candidate.candidate_id, decided_at,
                ),
            )
            self._block_event(connection, row["block_id"], "OBSERVATION_ADMITTED", decided_at, {
                "observation_id": observation.observation_id, "candidate_id": candidate.candidate_id,
                "observation_hash": canonical_hash(observation_json), "source_namespace": candidate.source_namespace,
            })
            return self._candidate_payload(candidate_row)

    def _admission_reason(
        self, connection: sqlite3.Connection, document: Mapping[str, Any], row: sqlite3.Row,
        candidate: AcquisitionCandidate,
    ) -> str | None:
        observation = candidate.observation
        state = BlockState(row["state"])
        if state in _MEMBERSHIP_SEALED_STATES:
            return "BLOCK_MEMBERSHIP_SEALED"
        if state not in {BlockState.OPEN, BlockState.ACQUIRING}:
            return "BLOCK_NOT_ACQUIRING"
        if observation.source_schema != E5_OBSERVATION_SCHEMA:
            return "NON_PROSPECTIVE_OR_HISTORICAL_OBSERVATION"
        if observation.protocol_hash != E6_EXPECTED_PROTOCOL_HASH:
            return "PROTOCOL_HASH_MISMATCH"
        if candidate.source_namespace != document["sampling"]["source_partition"]:
            return "SYNTHETIC_OR_NONPROSPECTIVE_SOURCE"
        anchor, exposure_end = _utc(observation.anchor_at), _utc(observation.exposure_end_at)
        if not (_utc(row["scheduled_start"]) <= anchor < _utc(row["scheduled_end"])):
            return "ANCHOR_OUTSIDE_FIXED_BLOCK_WINDOW"
        if _utc(candidate.received_at) >= _utc(row["scheduled_end"]):
            return "BLOCK_WINDOW_CLOSED"
        maximum_exposure = anchor.timestamp() + int(document["outcome"]["maximum_resolution_lag_seconds"])
        if exposure_end.timestamp() > maximum_exposure or exposure_end > _utc(row["exposure_end"]):
            return "EXPOSURE_OUTSIDE_FROZEN_ENVELOPE"
        if not observation.symbol_liquidity_eligible:
            return "PREANCHOR_SYMBOL_INELIGIBLE"
        if wallet_cohort(document, observation.wallet_id) != int(row["cohort"]):
            return "WALLET_COHORT_DOES_NOT_MATCH_BLOCK"
        if connection.execute("SELECT 1 FROM phase_e6_observations WHERE wallet_id=?", (observation.wallet_id,)).fetchone():
            return "WALLET_ALREADY_ADMITTED"
        if connection.execute("SELECT 1 FROM phase_e6_observations WHERE source_event_id=?", (observation.source_event_id,)).fetchone():
            return "SOURCE_EVENT_ALREADY_ADMITTED"
        relation = self._cross_block_relation(connection, observation)
        if relation is not None:
            return relation
        return None

    @staticmethod
    def _cross_block_relation(connection: sqlite3.Connection, observation: DesignObservation) -> str | None:
        exact_checks = (
            ("transaction_id", observation.transaction_id, "CROSS_BLOCK_TRANSACTION_RELATION"),
            ("endpoint_family_id", observation.endpoint_family_id, "CROSS_BLOCK_ENDPOINT_RELATION"),
            ("campaign_id", observation.campaign_id, "CROSS_BLOCK_CAMPAIGN_RELATION"),
        )
        for column, value, reason in exact_checks:
            if value is None:
                continue
            prior = connection.execute(
                f"SELECT 1 FROM phase_e6_observations WHERE block_id<>? AND {column}=? LIMIT 1",
                (observation.block_id, value),
            ).fetchone()
            if prior is not None:
                return reason
        overlaps = connection.execute(
            """SELECT 1 FROM phase_e6_observations
               WHERE block_id<>? AND symbol=? AND anchor_at<? AND exposure_end_at>? LIMIT 1""",
            (observation.block_id, observation.symbol, normalized_utc(observation.exposure_end_at), normalized_utc(observation.anchor_at)),
        ).fetchone()
        return "CROSS_BLOCK_EXPOSURE_RELATION" if overlaps is not None else None

    @staticmethod
    def _candidate_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "candidate_id": row["candidate_id"], "block_id": row["block_id"], "decision": row["decision"],
            "reason": row["decision_reason"], "decided_at": row["decided_at"],
        }

    def record_resolution_metadata(self, metadata: ResolutionMetadata, *, recorded_at: str) -> dict[str, Any]:
        """Record only permitted resolution timing metadata for an admitted row."""
        self.initialize()
        at = normalized_utc(recorded_at)
        with self._write() as connection:
            self._document(connection)
            observation = connection.execute(
                "SELECT * FROM phase_e6_observations WHERE observation_id=?", (metadata.observation_id,),
            ).fetchone()
            if observation is None:
                raise ResolutionRefused("Resolution metadata cannot create prospective membership.")
            payload = metadata.payload()
            digest = canonical_hash(payload)
            block = self._require_block(connection, observation["block_id"])
            if BlockState(block["state"]) == BlockState.FINALIZED:
                prior_late = connection.execute(
                    "SELECT recorded_at FROM phase_e6_late_resolution_metadata WHERE metadata_hash=?", (digest,),
                ).fetchone()
                if prior_late is None:
                    connection.execute(
                        "INSERT INTO phase_e6_late_resolution_metadata(observation_id,metadata_json,metadata_hash,recorded_at) VALUES (?,?,?,?)",
                        (metadata.observation_id, storage_json(payload), digest, at),
                    )
                    self._audit(connection, "RESOLUTION", at, metadata.observation_id, {"metadata_hash": digest, "late": True})
                    self._integrity_event(connection, observation["block_id"], at, "LATE_RESOLUTION_EVIDENCE", {
                        "observation_id": metadata.observation_id, "metadata_hash": digest,
                    })
                return {
                    "observation_id": metadata.observation_id,
                    "recorded_at": prior_late["recorded_at"] if prior_late is not None else at,
                    "late": True,
                }
            existing = connection.execute(
                "SELECT * FROM phase_e6_resolution_metadata WHERE observation_id=?", (metadata.observation_id,),
            ).fetchone()
            if existing is not None:
                if existing["metadata_hash"] != digest:
                    self._integrity_event(connection, observation["block_id"], at, "RESOLUTION_CONFLICT", {
                        "observation_id": metadata.observation_id, "existing_hash": existing["metadata_hash"], "incoming_hash": digest,
                    })
                    if BlockState(block["state"]) not in _TERMINAL_BLOCK_STATES:
                        self._transition(connection, block, BlockState.RESOLUTION_FAILED, at=at, event_type="RESOLUTION_CONFLICT", payload={
                            "observation_id": metadata.observation_id,
                        })
                    raise ResolutionRefused("Conflicting resolution metadata was recorded as an integrity failure.")
                return {"observation_id": metadata.observation_id, "recorded_at": existing["recorded_at"], "idempotent": True}
            connection.execute(
                "INSERT INTO phase_e6_resolution_metadata VALUES (?,?,?,?)",
                (metadata.observation_id, storage_json(payload), digest, at),
            )
            self._audit(connection, "RESOLUTION", at, metadata.observation_id, {"metadata_hash": digest})
            self._block_event(connection, observation["block_id"], "RESOLUTION_METADATA_RECORDED", at, {
                "observation_id": metadata.observation_id, "metadata_hash": digest,
            })
            return {"observation_id": metadata.observation_id, "recorded_at": at, "idempotent": False}

    def finalize_maturity(self, block_id: str, *, as_of: str) -> dict[str, Any]:
        """Track frozen maturity states without loading or storing an outcome value."""
        self.initialize()
        at = normalized_utc(as_of)
        with self._write() as connection:
            document = self._document(connection)
            block = self._require_block(connection, block_id)
            state = BlockState(block["state"])
            if state == BlockState.FINALIZED:
                return self._block_payload(connection, block)
            if state != BlockState.AWAITING_RESOLUTION:
                raise AcquisitionStateError("Maturity may be finalized only after immutable membership sealing.")
            rows = connection.execute(
                """SELECT observations.*, metadata.metadata_json
                   FROM phase_e6_observations AS observations
                   LEFT JOIN phase_e6_resolution_metadata AS metadata USING(observation_id)
                   WHERE observations.block_id=? ORDER BY observations.observation_id""",
                (block_id,),
            ).fetchall()
            states: Counter[str] = Counter()
            for row in rows:
                observation = self._observation_from_row(row)
                metadata = json.loads(row["metadata_json"]) if row["metadata_json"] is not None else None
                classification = classify_evidence(
                    document, observation, as_of=at,
                    resolution_event_at=metadata["resolution_event_at"] if metadata else None,
                    ingested_at=metadata["ingested_at"] if metadata else None,
                    structurally_unresolved=bool(metadata and metadata["structurally_unresolved"]),
                )
                states[classification.state.value] += 1
                latest = connection.execute(
                    "SELECT state, event_hash FROM phase_e6_maturity_events WHERE observation_id=? ORDER BY event_id DESC LIMIT 1",
                    (observation.observation_id,),
                ).fetchone()
                event = {
                    "observation_id": observation.observation_id, "state": classification.state.value,
                    "classified_at": classification.classified_at, "reason": classification.reason,
                }
                digest = canonical_hash(event)
                if latest is None or latest["event_hash"] != digest:
                    connection.execute(
                        "INSERT INTO phase_e6_maturity_events(observation_id,state,classified_at,reason,event_hash) VALUES (?,?,?,?,?)",
                        (observation.observation_id, classification.state.value, classification.classified_at, classification.reason, digest),
                    )
            if states[EvidenceState.IMMATURE.value] > 0:
                self._block_event(connection, block_id, "MATURITY_HEARTBEAT", at, {"maturity_states": dict(states)})
                return self._block_payload(connection, block)
            block = self._transition(connection, block, BlockState.FINALIZED, at=at, event_type="BLOCK_FINALIZED", payload={
                "maturity_states": dict(states), "outcome_values_read": 0,
            })
            return self._block_payload(connection, block)

    @staticmethod
    def _observation_from_row(row: sqlite3.Row) -> DesignObservation:
        """Reconstruct the outcome-free E.5 record from immutable membership storage."""
        payload = json.loads(row["observation_json"])
        return DesignObservation(
            observation_id=payload["observation_id"], source_schema=payload["source_schema"],
            protocol_hash=payload["protocol_hash"], block_id=payload["block_id"],
            anchor_at=row["anchor_at"], exposure_end_at=row["exposure_end_at"], wallet_id=row["wallet_id"],
            symbol=payload["symbol"], source_event_id=payload["source_event_id"],
            sampling_weight=float(payload["sampling_weight"]), predicate=bool(payload["predicate"]),
            liquidity_stratum=payload["liquidity_stratum"], graph_density_stratum=payload["graph_density_stratum"],
            time_stratum=payload["time_stratum"], eligibility_snapshot_hash=payload["eligibility_snapshot_hash"],
            symbol_liquidity_eligible=bool(payload["symbol_liquidity_eligible"]),
            transaction_id=payload.get("transaction_id"), endpoint_family_id=payload.get("endpoint_family_id"),
            campaign_id=payload.get("campaign_id"),
        )

    def report_late_relation(
        self, observation_id: str, related_observation_id: str, *, relation_type: str, detected_at: str,
    ) -> None:
        """Preserve a later-discovered cross-block contamination; never erase either row."""
        self.initialize()
        allowed = {"WALLET", "TRANSACTION", "ENDPOINT", "CAMPAIGN", "EXPOSURE"}
        if relation_type not in allowed:
            raise ValueError("Relation type is not part of the frozen E.5 dependence vocabulary.")
        at = normalized_utc(detected_at)
        with self._write() as connection:
            self._document(connection)
            left = connection.execute("SELECT * FROM phase_e6_observations WHERE observation_id=?", (observation_id,)).fetchone()
            right = connection.execute("SELECT * FROM phase_e6_observations WHERE observation_id=?", (related_observation_id,)).fetchone()
            if left is None or right is None:
                raise AcquisitionStateError("Late relations require two already-admitted observations.")
            if left["block_id"] == right["block_id"]:
                return
            payload = {"observation_id": observation_id, "related_observation_id": related_observation_id, "relation_type": relation_type}
            self._integrity_event(connection, left["block_id"], at, "CROSS_BLOCK_CONTAMINATION", payload)
            for row in (self._require_block(connection, left["block_id"]), self._require_block(connection, right["block_id"])):
                if BlockState(row["state"]) not in _TERMINAL_BLOCK_STATES:
                    self._transition(connection, row, BlockState.CONTAMINATION_DETECTED, at=at, event_type="CROSS_BLOCK_CONTAMINATION", payload=payload)

    def _block_payload(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        observation_count = int(connection.execute(
            "SELECT COUNT(*) FROM phase_e6_observations WHERE block_id=?", (row["block_id"],),
        ).fetchone()[0])
        maturity = {
            item["state"]: int(item["count"])
            for item in connection.execute(
                """SELECT state,COUNT(*) AS count FROM (
                    SELECT observation_id,state,ROW_NUMBER() OVER(PARTITION BY observation_id ORDER BY event_id DESC) AS ordinal
                    FROM phase_e6_maturity_events WHERE observation_id IN
                        (SELECT observation_id FROM phase_e6_observations WHERE block_id=?)
                ) WHERE ordinal=1 GROUP BY state""",
                (row["block_id"],),
            )
        }
        return {
            "block_id": row["block_id"], "ordinal": int(row["ordinal"]), "state": row["state"],
            "scheduled_start": row["scheduled_start"], "scheduled_end": row["scheduled_end"],
            "sealed": BlockState(row["state"]) in _MEMBERSHIP_SEALED_STATES,
            "observation_count": observation_count, "maturity": maturity,
        }

    def status(self) -> dict[str, Any]:
        """Return only operational health and outcome-blind counts."""
        self.initialize()
        with self._connection() as connection:
            self._document(connection)
            state_counts = {
                item["state"]: int(item["count"])
                for item in connection.execute("SELECT state,COUNT(*) AS count FROM phase_e6_blocks GROUP BY state")
            }
            access_counts = {
                item["access_kind"]: int(item["count"])
                for item in connection.execute("SELECT access_kind,COUNT(*) AS count FROM phase_e6_access_audit GROUP BY access_kind")
            }
            return {
                "protocol_id": E6_EXPECTED_PROTOCOL_ID,
                "protocol_hash": E6_EXPECTED_PROTOCOL_HASH,
                "block_count": int(connection.execute("SELECT COUNT(*) FROM phase_e6_blocks").fetchone()[0]),
                "block_states": state_counts,
                "hard_stop": "2027-12-25T00:00:00Z",
                "observation_count": int(connection.execute("SELECT COUNT(*) FROM phase_e6_observations").fetchone()[0]),
                "outcome_access": {
                    "acquisition_side_reads": access_counts.get("ACQUISITION", 0),
                    "resolution_side_reads": access_counts.get("RESOLUTION", 0),
                    "scientific_evaluation_reads": 0,
                },
                "reserved_test_queries": 0,
                "trades_placed": 0,
                "authority": {
                    "trading": False, "execution": False, "signal": False, "prediction": False,
                },
            }

    def replay_hash(self) -> str:
        """Hash persisted decisions canonically; no outcome-side data participate."""
        self.initialize()
        with self._connection() as connection:
            document = self._document(connection)
            blocks = [
                dict(row) for row in connection.execute(
                    "SELECT block_id,ordinal,cohort,scheduled_start,scheduled_end,exposure_end,block_hash,state FROM phase_e6_blocks ORDER BY ordinal",
                )
            ]
            candidates = [
                dict(row) for row in connection.execute(
                    "SELECT candidate_id,block_id,source_event_id,candidate_hash,decision,decision_reason FROM phase_e6_candidates ORDER BY candidate_id",
                )
            ]
            observations = [
                dict(row) for row in connection.execute(
                    "SELECT observation_id,block_id,observation_hash,candidate_id FROM phase_e6_observations ORDER BY observation_id",
                )
            ]
        return canonical_hash({
            "schema": E6_ACQUISITION_SCHEMA,
            "protocol_hash": document["identity"]["protocol_hash"],
            "blocks": blocks, "candidates": candidates, "observations": observations,
        })

    def integrity_audit(self) -> dict[str, Any]:
        """Verify storage, schedule, provenance and zero evaluation-outcome access."""
        self.initialize()
        with self._connection() as connection:
            document = self._document(connection)
            expected = {item.block_id: item for item in scheduled_blocks(document)}
            rows = connection.execute("SELECT * FROM phase_e6_blocks ORDER BY ordinal").fetchall()
            failures: list[str] = []
            if len(rows) != 60 or {row["block_id"] for row in rows} != set(expected):
                failures.append("FIXED_SCHEDULE_MISMATCH")
            for row in rows:
                block = expected.get(row["block_id"])
                if block is None or any((
                    row["ordinal"] != block.ordinal, row["cohort"] != block.cohort,
                    row["scheduled_start"] != block.sample_start, row["scheduled_end"] != block.sample_end,
                    row["exposure_end"] != block.exposure_end, row["block_hash"] != block.block_hash,
                )):
                    failures.append("SCHEDULE_ROW_MUTATED")
            duplicate_relations = connection.execute(
                """SELECT COUNT(*) FROM phase_e6_observations AS left_row JOIN phase_e6_observations AS right_row
                   ON left_row.block_id<>right_row.block_id AND left_row.observation_id<right_row.observation_id
                   AND ((left_row.transaction_id IS NOT NULL AND left_row.transaction_id=right_row.transaction_id)
                        OR (left_row.endpoint_family_id IS NOT NULL AND left_row.endpoint_family_id=right_row.endpoint_family_id)
                        OR (left_row.campaign_id IS NOT NULL AND left_row.campaign_id=right_row.campaign_id)
                        OR (left_row.symbol=right_row.symbol AND left_row.anchor_at<right_row.exposure_end_at
                            AND left_row.exposure_end_at>right_row.anchor_at))"""
            ).fetchone()[0]
            if duplicate_relations:
                failures.append("UNRECORDED_CROSS_BLOCK_RELATION")
            quick_check = connection.execute("PRAGMA quick_check").fetchone()[0]
            if quick_check != "ok":
                failures.append("SQLITE_QUICK_CHECK_FAILED")
            return {
                "ok": not failures,
                "failures": tuple(sorted(set(failures))),
                "replay_hash": self.replay_hash(),
                "sqlite_quick_check": quick_check,
                "scientific_evaluation_reads": 0,
                "reserved_test_queries": 0,
            }

    @staticmethod
    def reserved_test_query_count() -> int:
        return 0
