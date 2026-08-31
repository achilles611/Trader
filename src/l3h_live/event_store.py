"""Canonical write-ahead event storage for L3H.

The store is intentionally separate from the historic L3G SQLite ledger.  It
uses optimistic stream versions, content hashes, and a durable command seal so
that a process loss after sealing but before acknowledgement is *UNKNOWN*, not
an invitation to send the command again.
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import threading
from typing import Mapping
from uuid import uuid4

from .contracts import canonical_hash, canonical_json


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class LiveEvent:
    event_id: str
    stream_id: str
    stream_version: int
    kind: str
    occurred_at: str
    payload: Mapping[str, object]
    previous_hash: str
    record_hash: str


class LiveEventStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS l3h_event (
                    event_id TEXT PRIMARY KEY,
                    stream_id TEXT NOT NULL,
                    stream_version INTEGER NOT NULL,
                    kind TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    UNIQUE(stream_id, stream_version)
                );
                CREATE TABLE IF NOT EXISTS l3h_command (
                    command_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    command_json TEXT NOT NULL,
                    sealed_event_id TEXT NOT NULL,
                    acknowledgement_json TEXT,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def append(
        self, stream_id: str, kind: str, payload: Mapping[str, object], *, event_id: str | None = None,
        expected_version: int | None = None, occurred_at: str | None = None,
    ) -> LiveEvent:
        if not stream_id or not kind:
            raise ValueError("L3H event stream and kind are required.")
        body = dict(payload)
        when = occurred_at or _now()
        identity = event_id or "l3h-event-" + uuid4().hex
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute("SELECT * FROM l3h_event WHERE event_id=?", (identity,)).fetchone()
            if existing is not None:
                candidate = self._row(existing)
                if (candidate.stream_id, candidate.kind, dict(candidate.payload)) != (stream_id, kind, body):
                    connection.execute("ROLLBACK")
                    raise ValueError("EVENT_ID_REUSED_WITH_DIFFERENT_CONTENT")
                connection.execute("COMMIT")
                return candidate
            tip = connection.execute(
                "SELECT stream_version, record_hash FROM l3h_event WHERE stream_id=? ORDER BY stream_version DESC LIMIT 1",
                (stream_id,),
            ).fetchone()
            version = 1 if tip is None else int(tip["stream_version"]) + 1
            if expected_version is not None and version != expected_version + 1:
                connection.execute("ROLLBACK")
                raise ValueError("OPTIMISTIC_STREAM_VERSION_CONFLICT")
            previous = "GENESIS" if tip is None else str(tip["record_hash"])
            record_hash = canonical_hash({
                "event_id": identity, "stream_id": stream_id, "stream_version": version, "kind": kind,
                "occurred_at": when, "payload": body, "previous_hash": previous,
            })
            encoded = canonical_json(body).decode("utf-8")
            connection.execute(
                "INSERT INTO l3h_event(event_id,stream_id,stream_version,kind,occurred_at,payload_json,previous_hash,record_hash) VALUES(?,?,?,?,?,?,?,?)",
                (identity, stream_id, version, kind, when, encoded, previous, record_hash),
            )
            connection.execute("COMMIT")
        return LiveEvent(identity, stream_id, version, kind, when, body, previous, record_hash)

    def seal_command(self, *, request_id: str, command: Mapping[str, object]) -> tuple[LiveEvent, bool]:
        """Persist a dispatch intent once.  A retry returns the original seal."""

        command_id = command.get("command_id")
        if not isinstance(command_id, str) or not command_id.startswith("l3h-cmd-"):
            raise ValueError("Invalid L3H command identity.")
        if not isinstance(request_id, str) or len(request_id) < 8:
            raise ValueError("Activation request ID must contain at least eight characters.")
        payload = dict(command)
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute("SELECT * FROM l3h_command WHERE request_id=?", (request_id,)).fetchone()
            if existing is not None:
                stored = json.loads(str(existing["command_json"]))
                if stored != payload:
                    connection.execute("ROLLBACK")
                    raise ValueError("IDEMPOTENCY_REQUEST_REUSED_WITH_DIFFERENT_COMMAND")
                event = connection.execute("SELECT * FROM l3h_event WHERE event_id=?", (existing["sealed_event_id"],)).fetchone()
                connection.execute("COMMIT")
                if event is None:
                    raise RuntimeError("COMMAND_SEAL_MISSING")
                return self._row(event), True
            existing_by_id = connection.execute("SELECT * FROM l3h_command WHERE command_id=?", (command_id,)).fetchone()
            if existing_by_id is not None:
                connection.execute("ROLLBACK")
                raise ValueError("COMMAND_ID_ALREADY_SEALED")
            stream_id = "command:" + command_id
            tip = connection.execute(
                "SELECT stream_version, record_hash FROM l3h_event WHERE stream_id=? ORDER BY stream_version DESC LIMIT 1", (stream_id,)
            ).fetchone()
            previous = "GENESIS" if tip is None else str(tip["record_hash"])
            event_id = "l3h-event-" + uuid4().hex
            event_payload = {"command": payload, "dispatch_state": "SEALED"}
            when = _now()
            record_hash = canonical_hash({
                "event_id": event_id, "stream_id": stream_id, "stream_version": 1, "kind": "COMMAND_SEALED",
                "occurred_at": when, "payload": event_payload, "previous_hash": previous,
            })
            connection.execute(
                "INSERT INTO l3h_event(event_id,stream_id,stream_version,kind,occurred_at,payload_json,previous_hash,record_hash) VALUES(?,?,?,?,?,?,?,?)",
                (event_id, stream_id, 1, "COMMAND_SEALED", when, canonical_json(event_payload).decode("utf-8"), previous, record_hash),
            )
            connection.execute(
                "INSERT INTO l3h_command(command_id,request_id,state,command_json,sealed_event_id,acknowledgement_json,updated_at) VALUES(?,?,?,?,?,?,?)",
                (command_id, request_id, "SEALED", canonical_json(payload).decode("utf-8"), event_id, None, when),
            )
            connection.execute("COMMIT")
        return LiveEvent(event_id, stream_id, 1, "COMMAND_SEALED", when, event_payload, previous, record_hash), False

    def mark_command(self, command_id: str, *, state: str, acknowledgement: Mapping[str, object] | None = None) -> LiveEvent:
        if state not in {"DISPATCHING", "ACKNOWLEDGED", "UNKNOWN", "REFUSED"}:
            raise ValueError("Unsupported L3H command state.")
        with self._lock, closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            command = connection.execute("SELECT * FROM l3h_command WHERE command_id=?", (command_id,)).fetchone()
            if command is None:
                connection.execute("ROLLBACK")
                raise ValueError("UNKNOWN_COMMAND")
            current = str(command["state"])
            if current in {"ACKNOWLEDGED", "UNKNOWN", "REFUSED"}:
                connection.execute("ROLLBACK")
                raise ValueError("COMMAND_ALREADY_TERMINAL")
            stream_id = "command:" + command_id
            tip = connection.execute("SELECT stream_version, record_hash FROM l3h_event WHERE stream_id=? ORDER BY stream_version DESC LIMIT 1", (stream_id,)).fetchone()
            if tip is None:
                connection.execute("ROLLBACK")
                raise RuntimeError("COMMAND_SEAL_MISSING")
            version = int(tip["stream_version"]) + 1
            previous = str(tip["record_hash"])
            event_id = "l3h-event-" + uuid4().hex
            payload: dict[str, object] = {"command_id": command_id, "from_state": current, "to_state": state}
            if acknowledgement is not None:
                payload["acknowledgement"] = dict(acknowledgement)
            when = _now()
            record_hash = canonical_hash({
                "event_id": event_id, "stream_id": stream_id, "stream_version": version, "kind": "COMMAND_STATE",
                "occurred_at": when, "payload": payload, "previous_hash": previous,
            })
            connection.execute(
                "INSERT INTO l3h_event(event_id,stream_id,stream_version,kind,occurred_at,payload_json,previous_hash,record_hash) VALUES(?,?,?,?,?,?,?,?)",
                (event_id, stream_id, version, "COMMAND_STATE", when, canonical_json(payload).decode("utf-8"), previous, record_hash),
            )
            connection.execute(
                "UPDATE l3h_command SET state=?, acknowledgement_json=?, updated_at=? WHERE command_id=?",
                (state, None if acknowledgement is None else canonical_json(dict(acknowledgement)).decode("utf-8"), when, command_id),
            )
            connection.execute("COMMIT")
        return LiveEvent(event_id, stream_id, version, "COMMAND_STATE", when, payload, previous, record_hash)

    def command(self, command_id: str) -> Mapping[str, object] | None:
        with closing(self._connect()) as connection:
            row = connection.execute("SELECT * FROM l3h_command WHERE command_id=?", (command_id,)).fetchone()
        if row is None:
            return None
        return {
            "command_id": str(row["command_id"]), "request_id": str(row["request_id"]), "state": str(row["state"]),
            "command": json.loads(str(row["command_json"])),
            "acknowledgement": None if row["acknowledgement_json"] is None else json.loads(str(row["acknowledgement_json"])),
            "sealed_event_id": str(row["sealed_event_id"]), "updated_at": str(row["updated_at"]),
        }

    def stream(self, stream_id: str) -> tuple[LiveEvent, ...]:
        """Read one ordered evidence stream without exposing SQLite details."""

        if not isinstance(stream_id, str) or not stream_id:
            raise ValueError("L3H event stream is required.")
        with closing(self._connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM l3h_event WHERE stream_id=? ORDER BY stream_version", (stream_id,),
            ).fetchall()
        return tuple(self._row(row) for row in rows)

    def verify(self) -> tuple[bool, str]:
        with closing(self._connect()) as connection:
            streams = [str(row[0]) for row in connection.execute("SELECT DISTINCT stream_id FROM l3h_event ORDER BY stream_id")]
            for stream_id in streams:
                previous = "GENESIS"
                version = 0
                for row in connection.execute("SELECT * FROM l3h_event WHERE stream_id=? ORDER BY stream_version", (stream_id,)):
                    version += 1
                    event = self._row(row)
                    if event.stream_version != version or event.previous_hash != previous:
                        return False, "CHAIN_LINK_INVALID"
                    expected = canonical_hash({
                        "event_id": event.event_id, "stream_id": event.stream_id, "stream_version": event.stream_version,
                        "kind": event.kind, "occurred_at": event.occurred_at, "payload": dict(event.payload), "previous_hash": previous,
                    })
                    if expected != event.record_hash:
                        return False, "CHAIN_HASH_INVALID"
                    previous = event.record_hash
        return True, "PASS"

    @staticmethod
    def _row(row: sqlite3.Row) -> LiveEvent:
        return LiveEvent(
            event_id=str(row["event_id"]), stream_id=str(row["stream_id"]), stream_version=int(row["stream_version"]),
            kind=str(row["kind"]), occurred_at=str(row["occurred_at"]), payload=json.loads(str(row["payload_json"])),
            previous_hash=str(row["previous_hash"]), record_hash=str(row["record_hash"]),
        )
