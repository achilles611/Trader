"""Durable, hash-chained SQLite ledger for every Lane III-G side effect."""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import threading
from typing import Iterator, Mapping

from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import ACCOUNT_BINDING, PAPER_RECORD_SCHEMA, POLICY, RISK_PROFILE


_DOMAIN_TABLES = {
    "SESSION": "lane_iii_paper_sessions",
    "EVIDENCE": "lane_iii_paper_evidence",
    "DECISION": "lane_iii_paper_decisions",
    "INTENT": "lane_iii_paper_intents",
    "RISK_GRANT": "lane_iii_paper_risk_grants",
    "COMMAND": "lane_iii_paper_commands",
    "COMMAND_RECEIPT": "lane_iii_paper_command_receipts",
    "ORDER_EVENT": "lane_iii_paper_order_events",
    "EXECUTION": "lane_iii_paper_executions",
    "POSITION_SNAPSHOT": "lane_iii_paper_position_snapshots",
    "RISK_EVENT": "lane_iii_paper_risk_events",
    "INCIDENT": "lane_iii_paper_incidents",
}
_HIGH_VOLUME_DOMAINS = frozenset({"EVIDENCE", "DECISION"})
_SECRET_KEYS = frozenset({"hmac_key", "password", "token", "connection_credentials", "private_key", "secret", "authorization"})


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _assert_redacted(value: object) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).replace("-", "_").lower()
            if normalized in _SECRET_KEYS or normalized.endswith("_password") or normalized.endswith("_secret"):
                raise ValueError("Secrets and credentials may not enter the paper ledger.")
            _assert_redacted(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _assert_redacted(item)


class PaperLedger:
    """Thread-safe append-only domain ledger with one global hash chain."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        # The default 1,000-page auto-checkpoint repeatedly stalls the single
        # authenticated observation consumer under MNQ depth load.  A bounded
        # 128 MiB WAL window lets the checkpoint copy amortize naturally while
        # every record remains committed before the next policy side effect.
        self._connection.execute("PRAGMA wal_autocheckpoint=32768")
        self._connection.execute("PRAGMA journal_size_limit=134217728")
        self._connection.execute("PRAGMA cache_size=-65536")
        self._connection.execute("PRAGMA temp_store=MEMORY")
        self._connection.execute("PRAGMA mmap_size=268435456")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._synchronous_mode = "FULL"
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._create_schema()
        self._chain_status = self._verify_chain_uncached()
        rows = self._connection.execute("SELECT domain, COUNT(*) AS count FROM lane_iii_paper_audit GROUP BY domain").fetchall()
        self._counts_cache = {str(row["domain"]): int(row["count"]) for row in rows}
        self._ordering_lock = threading.RLock()
        self._deferred_condition = threading.Condition(threading.Lock())
        self._deferred: deque[dict[str, object]] = deque()
        self._deferred_identities: set[str] = set()
        self._deferred_active = False
        self._deferred_error: BaseException | None = None
        self._deferred_stopping = False
        self._deferred_thread = threading.Thread(
            target=self._deferred_writer,
            name="LaneIIIPaperLedgerWriter",
            daemon=True,
        )
        self._deferred_thread.start()

    def _set_synchronous_mode(self, domain: str) -> None:
        # A separate committed transaction is retained for every record. WAL
        # NORMAL removes the per-record storage-barrier bottleneck for the two
        # high-volume, reconstructible experimental domains while remaining
        # atomic and durable across application crashes. All operational and
        # safety records retain FULL storage-barrier durability.
        requested = "NORMAL" if domain in _HIGH_VOLUME_DOMAINS else "FULL"
        if requested == self._synchronous_mode:
            return
        self._connection.execute(f"PRAGMA synchronous={requested}")
        self._synchronous_mode = requested

    @contextmanager
    def _domain_transaction(self, domain: str) -> Iterator[sqlite3.Connection]:
        self._set_synchronous_mode(domain)
        with self._transaction() as connection:
            yield connection

    def _create_schema(self) -> None:
        with self._lock, self._transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS lane_iii_paper_audit (
                    ledger_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    identity TEXT NOT NULL UNIQUE,
                    domain TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    execution_session_id TEXT,
                    payload_json TEXT NOT NULL,
                    previous_record_hash TEXT,
                    record_hash TEXT NOT NULL UNIQUE
                )
                """
            )
            for table in _DOMAIN_TABLES.values():
                connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        domain_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        identity TEXT NOT NULL UNIQUE,
                        kind TEXT NOT NULL,
                        occurred_at TEXT NOT NULL,
                        execution_session_id TEXT,
                        payload_json TEXT NOT NULL,
                        record_hash TEXT NOT NULL UNIQUE
                    )
                    """
                )
            connection.execute("CREATE INDEX IF NOT EXISTS lane_iii_paper_audit_domain_time ON lane_iii_paper_audit(domain, occurred_at)")

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield self._connection
        except Exception:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()

    @staticmethod
    def _domain(kind: str) -> str:
        upper = kind.upper()
        if upper in _DOMAIN_TABLES:
            return upper
        for prefix in (
            "COMMAND_RECEIPT", "POSITION_SNAPSHOT", "ORDER_EVENT", "RISK_GRANT",
            "RISK_EVENT", "EXECUTION", "EVIDENCE", "DECISION", "INTENT", "COMMAND", "SESSION", "INCIDENT",
        ):
            if upper.startswith(prefix):
                return prefix
        return "INCIDENT"

    def append(
        self,
        kind: str,
        payload: Mapping[str, object],
        *,
        identity: str | None = None,
        occurred_at: str | None = None,
        execution_session_id: str | None = None,
    ) -> str:
        prepared = self._prepare(kind, payload, identity, occurred_at, execution_session_id)
        with self._ordering_lock:
            self.flush_deferred()
            with self._lock:
                return self._append_prepared((prepared,))[0]

    def _prepare(
        self,
        kind: str,
        payload: Mapping[str, object],
        identity: str | None,
        occurred_at: str | None,
        execution_session_id: str | None,
    ) -> dict[str, object]:
        if not isinstance(kind, str) or not kind.strip() or not isinstance(payload, Mapping):
            raise ValueError("A paper ledger record requires a kind and mapping payload.")
        _assert_redacted(payload)
        at = normalized_utc(occurred_at or _now(), "Paper ledger occurrence time")
        domain = self._domain(kind)
        common: dict[str, object] = {
            "schema": PAPER_RECORD_SCHEMA,
            "kind": kind,
            "occurred_at": at,
            "execution_session_id": execution_session_id,
            "paper_policy_hash": POLICY.configuration_hash,
            "risk_profile_hash": RISK_PROFILE.configuration_hash,
            "account_binding_hash": ACCOUNT_BINDING.binding_hash,
            "scientific_eligibility": False,
            "paper_only": True,
            "live_capital": False,
            "payload": dict(payload),
        }
        record_identity = identity or "l3g-ledger-" + canonical_hash(common)
        return {
            "kind": kind,
            "at": at,
            "domain": domain,
            "common": common,
            "identity": record_identity,
            "execution_session_id": execution_session_id,
        }

    def _append_prepared(self, records: tuple[dict[str, object], ...]) -> list[str]:
        if not records:
            return []
        synchronous_domain = "DECISION" if all(str(record["domain"]) in _HIGH_VOLUME_DOMAINS for record in records) else "INCIDENT"
        hashes: list[str] = []
        with self._domain_transaction(synchronous_domain) as connection:
            prior = connection.execute(
                "SELECT record_hash FROM lane_iii_paper_audit ORDER BY ledger_sequence DESC LIMIT 1"
            ).fetchone()
            previous_hash = None if prior is None else str(prior["record_hash"])
            for record in records:
                record_identity = str(record["identity"])
                duplicate = connection.execute(
                    "SELECT record_hash FROM lane_iii_paper_audit WHERE identity = ?",
                    (record_identity,),
                ).fetchone()
                if duplicate is not None:
                    hashes.append(str(duplicate["record_hash"]))
                    continue
                domain = str(record["domain"])
                kind = str(record["kind"])
                at = str(record["at"])
                execution_session_id = record["execution_session_id"]
                common = dict(record["common"])  # type: ignore[arg-type]
                chained = {**common, "identity": record_identity, "previous_record_hash": previous_hash}
                record_hash = canonical_hash(chained)
                final = {**chained, "record_hash": record_hash}
                serialized = json.dumps(final, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
                connection.execute(
                    """
                    INSERT INTO lane_iii_paper_audit
                        (identity, domain, kind, occurred_at, execution_session_id, payload_json, previous_record_hash, record_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (record_identity, domain, kind, at, execution_session_id, serialized, previous_hash, record_hash),
                )
                connection.execute(
                    f"INSERT INTO {_DOMAIN_TABLES[domain]} (identity, kind, occurred_at, execution_session_id, payload_json, record_hash) VALUES (?, ?, ?, ?, ?, ?)",
                    (record_identity, kind, at, execution_session_id, serialized, record_hash),
                )
                previous_hash = record_hash
                hashes.append(record_hash)
                self._counts_cache[domain] = self._counts_cache.get(domain, 0) + 1
        return hashes

    def append_deferred(
        self,
        kind: str,
        payload: Mapping[str, object],
        *,
        identity: str | None = None,
        occurred_at: str | None = None,
        execution_session_id: str | None = None,
    ) -> None:
        prepared = self._prepare(kind, payload, identity, occurred_at, execution_session_id)
        if str(prepared["domain"]) not in _HIGH_VOLUME_DOMAINS:
            raise ValueError("Only evidence and no-side-effect decisions may use deferred persistence.")
        with self._ordering_lock, self._deferred_condition:
            if self._deferred_error is not None:
                raise RuntimeError("Deferred paper ledger writer failed.") from self._deferred_error
            record_identity = str(prepared["identity"])
            if record_identity in self._deferred_identities:
                return
            self._deferred.append(prepared)
            self._deferred_identities.add(record_identity)
            self._deferred_condition.notify()

    def _deferred_writer(self) -> None:
        while True:
            with self._deferred_condition:
                while not self._deferred and not self._deferred_stopping:
                    self._deferred_condition.wait()
                if self._deferred_stopping and not self._deferred:
                    return
                if len(self._deferred) < 512 and not self._deferred_stopping:
                    self._deferred_condition.wait(timeout=0.01)
                batch = tuple(self._deferred.popleft() for _ in range(min(512, len(self._deferred))))
                self._deferred_active = True
            try:
                with self._lock:
                    self._append_prepared(batch)
            except BaseException as error:
                with self._deferred_condition:
                    self._deferred_error = error
                    self._deferred_active = False
                    self._deferred_condition.notify_all()
                return
            with self._deferred_condition:
                for record in batch:
                    self._deferred_identities.discard(str(record["identity"]))
                self._deferred_active = False
                self._deferred_condition.notify_all()

    def flush_deferred(self) -> None:
        with self._deferred_condition:
            self._deferred_condition.notify()
            while (self._deferred or self._deferred_active) and self._deferred_error is None:
                self._deferred_condition.wait(timeout=1.0)
            if self._deferred_error is not None:
                raise RuntimeError("Deferred paper ledger writer failed.") from self._deferred_error

    def contains(self, identity: str) -> bool:
        with self._ordering_lock:
            self.flush_deferred()
        with self._lock:
            return self._connection.execute("SELECT 1 FROM lane_iii_paper_audit WHERE identity = ?", (identity,)).fetchone() is not None

    def fetch(self, identity: str) -> dict[str, object] | None:
        with self._ordering_lock:
            self.flush_deferred()
        with self._lock:
            row = self._connection.execute("SELECT payload_json FROM lane_iii_paper_audit WHERE identity = ?", (identity,)).fetchone()
            return None if row is None else json.loads(str(row["payload_json"]))

    def recent(self, limit: int = 100, *, domain: str | None = None) -> list[dict[str, object]]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("Paper ledger query limit is invalid.")
        with self._ordering_lock:
            self.flush_deferred()
        with self._lock:
            if domain is None:
                rows = self._connection.execute("SELECT payload_json FROM lane_iii_paper_audit ORDER BY ledger_sequence DESC LIMIT ?", (limit,)).fetchall()
            else:
                if domain not in _DOMAIN_TABLES:
                    raise ValueError("Unknown paper ledger domain.")
                rows = self._connection.execute("SELECT payload_json FROM lane_iii_paper_audit WHERE domain = ? ORDER BY ledger_sequence DESC LIMIT ?", (domain, limit)).fetchall()
            return [json.loads(str(row["payload_json"])) for row in rows]

    def _verify_chain_uncached(self) -> tuple[bool, str | None]:
        with self._lock:
            rows = self._connection.execute("SELECT * FROM lane_iii_paper_audit ORDER BY ledger_sequence").fetchall()
        previous: str | None = None
        for row in rows:
            record = json.loads(str(row["payload_json"]))
            record_hash = record.pop("record_hash", None)
            if record.get("previous_record_hash") != previous or record_hash != canonical_hash(record) or record_hash != row["record_hash"]:
                return False, str(row["identity"])
            previous = str(record_hash)
        return True, None

    def verify_chain(self) -> tuple[bool, str | None]:
        with self._ordering_lock:
            self.flush_deferred()
        result = self._verify_chain_uncached()
        with self._lock:
            self._chain_status = result
        return result

    def chain_status(self) -> tuple[bool, str | None]:
        with self._lock:
            return self._chain_status

    def counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts_cache)

    def close(self) -> None:
        with self._ordering_lock:
            self.flush_deferred()
            with self._deferred_condition:
                self._deferred_stopping = True
                self._deferred_condition.notify_all()
            self._deferred_thread.join(timeout=30.0)
            if self._deferred_thread.is_alive():
                raise RuntimeError("Deferred paper ledger writer did not stop.")
        with self._lock:
            self._connection.close()

    def __enter__(self) -> "PaperLedger":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
