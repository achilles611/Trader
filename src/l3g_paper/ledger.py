"""Durable, hash-chained SQLite ledger for every Lane III-G side effect."""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import threading
from typing import Iterator, Mapping
from uuid import uuid4

from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import ACCOUNT_BINDING, PAPER_RECORD_SCHEMA, POLICY, RISK_PROFILE
from .sessions import PaperSessionContext, PaperSessionKind, UNSPECIFIED_OFF_SESSION_CONTEXT, context_from_identity


_DOMAIN_TABLES = {
    "OBSERVATION": "lane_iii_paper_observations",
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
_HIGH_VOLUME_DOMAINS = frozenset({"OBSERVATION", "EVIDENCE", "DECISION"})
COMMISSIONING_TAIL_POLICY_VERSION = "l3g-commissioning-passive-tail-v2"
COMMISSIONING_NO_AUTHORITY_EFFECT = "NONE"
_COMMISSIONING_WATERMARK_METADATA_KEY = "commissioning_authority_watermark"
_PASSIVE_MARKET_OBSERVATION_TYPES = frozenset({"QUOTE", "TRADE", "DEPTH"})
_PASSIVE_EVIDENCE_FAMILIES = frozenset({
    "STRUCTURAL_CONTEXT", "ORDER_FLOW", "RESTING_LIQUIDITY", "VOLATILITY_CONTEXT", "MARKET_REGIME",
})
_PASSIVE_DECISIONS = frozenset({"NO_TRADE", "LONG", "SHORT", "EXIT"})
_AUTHORITY_SHAPED_PAYLOAD_KEYS = frozenset({
    "command_id", "grant_id", "intent_id", "order_id", "execution_id", "commissioning_id",
    "working_order_count", "position_quantity", "risk_authority", "arm_grant", "lockout_reason",
})
_SECRET_KEYS = frozenset({"hmac_key", "password", "token", "connection_credentials", "private_key", "secret", "authorization"})
_EPOCH_DIRECTORY = re.compile(r"^epoch-(\d+)$", re.IGNORECASE)
_EPOCH_ID = re.compile(r"^L3G-PAPER-EPOCH-[A-Za-z0-9][A-Za-z0-9._-]*$")


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


def commissioning_safe_tail_classification(
    domain: str, kind: str, payload: Mapping[str, object],
) -> str | None:
    """Classify only exact, demonstrably no-side-effect live-tail records.

    Returning ``None`` is intentionally fail-closed.  In particular, the
    high-volume domain name is never sufficient: account/order observations,
    unmarked decisions, incidents, and all unknown future shapes advance the
    authority watermark.
    """
    if any(key in payload for key in _AUTHORITY_SHAPED_PAYLOAD_KEYS):
        return None
    if domain == "OBSERVATION" and kind == "OBSERVATION_ENVELOPE":
        observation_type = payload.get("observation_type")
        required = (payload.get("observation_id"), payload.get("local_monotonic_sequence"), payload.get("source_payload_hash"))
        if observation_type in _PASSIVE_MARKET_OBSERVATION_TYPES and isinstance(required[0], str) and type(required[1]) is int and isinstance(required[2], str):
            return f"OBSERVATION:{kind}:{observation_type}"
        if (
            observation_type == "ACCOUNT"
            and isinstance(required[0], str)
            and type(required[1]) is int
            and isinstance(required[2], str)
            and payload.get("authority_effect") == COMMISSIONING_NO_AUTHORITY_EFFECT
            and payload.get("observation_semantics") == "INFORMATIONAL_ACCOUNT_ITEM"
            and payload.get("observation_payload_keys") == ["item", "value"]
            and (
                (payload.get("observation_account_alias"), payload.get("observation_account_class"))
                in {
                    ("Sim101", "LOCAL_SIMULATION"),
                    ("Lucid25kflex01", "PROVIDER_EVALUATION"),
                }
            )
        ):
            return "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE"
        return None
    if domain == "EVIDENCE" and kind == "EVIDENCE":
        if (
            isinstance(payload.get("evidence_id"), str)
            and payload.get("family") in _PASSIVE_EVIDENCE_FAMILIES
            and payload.get("scientific_eligibility") is False
            and payload.get("book_completeness") == "UNVERIFIED"
            and payload.get("sequence_authority") == "LOCAL_CALLBACK_ORDER_ONLY"
        ):
            return "EVIDENCE:EVIDENCE"
        return None
    if domain == "DECISION" and kind == "DECISION":
        decision = payload.get("decision")
        direction = payload.get("direction")
        expected_direction = {"NO_TRADE": "FLAT", "LONG": "LONG", "SHORT": "SHORT", "EXIT": "FLAT"}.get(str(decision))
        if (
            decision in _PASSIVE_DECISIONS
            and direction == expected_direction
            and payload.get("authority_effect") == COMMISSIONING_NO_AUTHORITY_EFFECT
            and payload.get("commissioning") is False
            and payload.get("strategy_generated") is True
            and payload.get("scientific_evidence") is False
            and payload.get("scientific_eligibility") is False
            and isinstance(payload.get("paper_decision_id"), str)
        ):
            return f"DECISION:DECISION:{decision}:AUTHORITY_EFFECT_NONE"
        return None
    return None


def is_commissioning_safe_unverified_tail_record(record: Mapping[str, object]) -> bool:
    """Public fail-closed predicate for stored or prepared ledger records."""
    domain, kind = record.get("domain"), record.get("kind")
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        serialized = record.get("payload_json")
        if isinstance(serialized, str):
            try:
                decoded = json.loads(serialized)
            except json.JSONDecodeError:
                return False
            payload = decoded.get("payload") if isinstance(decoded, Mapping) else None
    return (
        isinstance(domain, str)
        and isinstance(kind, str)
        and isinstance(payload, Mapping)
        and commissioning_safe_tail_classification(domain, kind, payload) is not None
    )


def _read_only_accessibility_check(path: Path) -> str:
    """Reject an unreadable image without starting a heavyweight validation scan.

    Full and incremental hash-chain verification belongs to the independent
    local verifier process.  Opening the paper runtime must not turn every
    BeezConsole restart into a historical ledger scan.
    """
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro&immutable=1", uri=True)
        connection.execute("PRAGMA schema_version").fetchone()
    except sqlite3.Error as exc:
        # Preserve the existing safe failure wording for callers and support
        # tools while avoiding a full quick_check in the runtime constructor.
        raise RuntimeError(f"LANE_III_PAPER existing ledger quick_check failed for {path}: {exc}") from exc
    finally:
        if connection is not None:
            connection.close()
    return "not_run_local_verifier_required"


def _epoch_id(path: Path) -> str:
    for part in reversed(path.parts[:-1]):
        match = _EPOCH_DIRECTORY.fullmatch(part)
        if match:
            return f"L3G-PAPER-EPOCH-{match.group(1)}"
    return "UNSPECIFIED"


def resolve_ledger_epoch(path: Path, configured_epoch: str | None = None) -> str:
    """Choose an explicit deployment epoch before a new ledger is created."""
    explicit = (configured_epoch or "").strip()
    if explicit:
        if not _EPOCH_ID.fullmatch(explicit):
            raise ValueError("Paper ledger epoch must use the L3G-PAPER-EPOCH-<id> form.")
        return explicit
    return _epoch_id(path)


def adopt_legacy_epoch(
    path: str | Path, audit_root: str | Path, *, target_epoch: str, operator_id: str, maintenance_window_confirmed: bool,
) -> dict[str, object]:
    """Perform the explicit, one-time metadata adoption for a legacy ledger.

    This intentionally is not called by runtime startup.  It requires an
    operator-confirmed maintenance window, a current verifier PASS carrying a
    retained Full proof, and an immutable external receipt before metadata is
    changed.  Ledger records and their chain are never rewritten.
    """
    if not maintenance_window_confirmed:
        raise ValueError("Legacy epoch adoption requires an explicit maintenance-window confirmation.")
    target = resolve_ledger_epoch(Path(path), target_epoch)
    if not operator_id.strip():
        raise ValueError("Legacy epoch adoption requires a non-empty operator identifier.")
    ledger_path = Path(path).expanduser().resolve()
    root = Path(audit_root).expanduser().resolve()
    try:
        latest = json.loads((root / "ledger-verification-latest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("Legacy epoch adoption requires a readable local verifier artifact.") from exc
    if (
        not isinstance(latest, dict) or latest.get("status") != "PASS" or latest.get("chain_valid") is not True
        or latest.get("checkpoint_valid") is not True or latest.get("errors") not in ([], None)
        or latest.get("ledger_path") != str(ledger_path)
        or type(latest.get("last_full_verified_sequence")) is not int
        or not isinstance(latest.get("last_full_verified_hash"), str)
        or not isinstance(latest.get("last_full_verification_id"), str)
    ):
        raise RuntimeError("Legacy epoch adoption requires a clean PASS with retained Full-chain proof.")
    connection = sqlite3.connect(str(ledger_path))
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("BEGIN IMMEDIATE")
        metadata = {str(row["metadata_key"]): str(row["metadata_value"]) for row in connection.execute(
            "SELECT metadata_key, metadata_value FROM lane_iii_paper_ledger_metadata"
        )}
        if metadata.get("ledger_epoch") != "UNSPECIFIED":
            raise RuntimeError("Legacy epoch adoption is allowed only while ledger_epoch is UNSPECIFIED.")
        full_row = connection.execute(
            "SELECT record_hash FROM lane_iii_paper_audit WHERE ledger_sequence=?", (latest["last_full_verified_sequence"],)
        ).fetchone()
        if full_row is None or str(full_row["record_hash"]) != latest["last_full_verified_hash"]:
            raise RuntimeError("Legacy epoch adoption refused because retained Full-chain ancestry no longer matches.")
        receipt = {
            "schema": "beelzebub-l3g-legacy-epoch-adoption-receipt-v1",
            "created_at": _now(),
            "operator_id": operator_id,
            "ledger_path": str(ledger_path),
            "ledger_uuid": metadata.get("ledger_uuid"),
            "schema_version": metadata.get("schema_version"),
            "before_epoch": "UNSPECIFIED",
            "after_epoch": target,
            "verification_id": latest["last_full_verification_id"],
            "full_verified_sequence": latest["last_full_verified_sequence"],
            "full_verified_hash": latest["last_full_verified_hash"],
        }
        receipts = root / "ledger-epoch-adoptions"
        receipts.mkdir(parents=True, exist_ok=True)
        receipt_path = receipts / f"{receipt['created_at'].replace(':', '').replace('-', '').replace('.', '')}-{uuid4().hex}.json"
        descriptor = os.open(receipt_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(receipt, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        connection.execute(
            "UPDATE lane_iii_paper_ledger_metadata SET metadata_value=? WHERE metadata_key='ledger_epoch' AND metadata_value='UNSPECIFIED'",
            (target,),
        )
        if connection.total_changes != 1:
            raise RuntimeError("Legacy epoch adoption found a conflicting target epoch.")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {"adopted": True, "ledger_path": str(ledger_path), "epoch": target, "receipt_path": str(receipt_path)}


class PaperLedger:
    """Thread-safe append-only domain ledger with one global hash chain."""

    def __init__(self, path: str | Path, *, epoch_id: str | None = None) -> None:
        self.path = Path(path).resolve()
        self._creation_epoch = resolve_ledger_epoch(self.path, epoch_id)
        existing_accessibility = _read_only_accessibility_check(self.path) if self.path.exists() else None
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
        self._current_session_context = UNSPECIFIED_OFF_SESSION_CONTEXT
        self._create_schema()
        row = self._connection.execute(
            "SELECT metadata_value FROM lane_iii_paper_ledger_metadata WHERE metadata_key='ledger_epoch'"
        ).fetchone()
        self._ledger_epoch = self._creation_epoch if row is None else str(row["metadata_value"])
        # Do not execute PRAGMA quick_check or a full hash-chain walk here.
        # The dedicated local verifier owns those potentially long operations.
        self._quick_check_state = existing_accessibility or "not_run_local_verifier_required"
        rows = self._connection.execute("SELECT domain, COUNT(*) AS count FROM lane_iii_paper_audit GROUP BY domain").fetchall()
        self._counts_cache = {str(row["domain"]): int(row["count"]) for row in rows}
        metadata_rows = self._connection.execute(
            "SELECT metadata_key, metadata_value FROM lane_iii_paper_ledger_metadata"
        ).fetchall()
        metadata = {str(row["metadata_key"]): str(row["metadata_value"]) for row in metadata_rows}
        self._ledger_uuid = metadata["ledger_uuid"]
        self._schema_version = metadata["schema_version"]
        latest = self._connection.execute(
            "SELECT ledger_sequence, occurred_at, record_hash FROM lane_iii_paper_audit ORDER BY ledger_sequence DESC LIMIT 1"
        ).fetchone()
        self._highest_sequence = 0 if latest is None else int(latest["ledger_sequence"])
        self._last_record_time = None if latest is None else str(latest["occurred_at"])
        self._final_record_hash = None if latest is None else str(latest["record_hash"])
        self._authority_watermark = self._load_or_rebuild_authority_watermark(
            metadata.get(_COMMISSIONING_WATERMARK_METADATA_KEY)
        )
        self._chain_status: tuple[bool | None, str | None] = (True, None) if self._highest_sequence == 0 else (None, None)
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

    def set_session_context(self, context: PaperSessionContext) -> None:
        """Set the default envelope for asynchronous paper-path records."""
        if type(context) is not PaperSessionContext:
            raise ValueError("Paper ledger session context must be immutable and exact.")
        with self._lock:
            self._current_session_context = context

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
            # This compact operational index is updated in the same transaction
            # as each immutable audit record below.  It avoids a historical
            # table scan during restart recovery on a high-volume ledger.
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS lane_iii_paper_commissioning_ownership (
                    commissioning_id TEXT PRIMARY KEY,
                    reservation_record_json TEXT NOT NULL,
                    entry_consumed INTEGER NOT NULL,
                    entry_decision_id TEXT,
                    released INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            ownership_columns = {
                str(row[1]) for row in connection.execute(
                    "PRAGMA table_info(lane_iii_paper_commissioning_ownership)"
                )
            }
            if "entry_decision_id" not in ownership_columns:
                connection.execute(
                    "ALTER TABLE lane_iii_paper_commissioning_ownership ADD COLUMN entry_decision_id TEXT"
                )
            # This metadata has no trading semantics.  It gives the local
            # verifier a stable ledger identity and sealed epoch/schema facts
            # without granting the verifier write access to the ledger.
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS lane_iii_paper_ledger_metadata (
                    metadata_key TEXT PRIMARY KEY,
                    metadata_value TEXT NOT NULL
                )
                """
            )
            metadata = {
                "ledger_uuid": "l3g-ledger-" + uuid4().hex,
                "ledger_epoch": self._creation_epoch,
                "schema_version": PAPER_RECORD_SCHEMA,
                "created_at": _now(),
            }
            for key, value in metadata.items():
                connection.execute(
                    "INSERT OR IGNORE INTO lane_iii_paper_ledger_metadata(metadata_key, metadata_value) VALUES (?, ?)",
                    (key, value),
                )

    @staticmethod
    def _watermark_payload(
        *,
        classified_through_sequence: int,
        last_authority_mutation_sequence: int,
        last_authority_mutation_kind: str | None,
        last_authority_mutation_domain: str | None,
        last_authority_mutation_hash: str | None,
        safe_classification_last_sequences: Mapping[str, int],
        updated_at: str,
    ) -> dict[str, object]:
        return {
            "policy_version": COMMISSIONING_TAIL_POLICY_VERSION,
            "classified_through_sequence": classified_through_sequence,
            "last_authority_mutation_sequence": last_authority_mutation_sequence,
            "last_authority_mutation_kind": last_authority_mutation_kind,
            "last_authority_mutation_domain": last_authority_mutation_domain,
            "last_authority_mutation_hash": last_authority_mutation_hash,
            "safe_classification_last_sequences": dict(sorted(safe_classification_last_sequences.items())),
            "updated_at": updated_at,
        }

    @staticmethod
    def _stored_record_classification(row: sqlite3.Row) -> str | None:
        try:
            document = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError:
            return None
        payload = document.get("payload") if isinstance(document, Mapping) else None
        if not isinstance(payload, Mapping):
            return None
        return commissioning_safe_tail_classification(str(row["domain"]), str(row["kind"]), payload)

    def _store_authority_watermark(self, connection: sqlite3.Connection, watermark: Mapping[str, object]) -> None:
        connection.execute(
            """
            INSERT INTO lane_iii_paper_ledger_metadata(metadata_key, metadata_value) VALUES (?, ?)
            ON CONFLICT(metadata_key) DO UPDATE SET metadata_value=excluded.metadata_value
            """,
            (
                _COMMISSIONING_WATERMARK_METADATA_KEY,
                json.dumps(dict(watermark), sort_keys=True, separators=(",", ":")),
            ),
        )

    def _rebuild_authority_watermark(self) -> dict[str, object]:
        """Find the newest unsafe row by scanning backward from the live tip."""
        cursor = self._highest_sequence
        safe_last: dict[str, int] = {}
        last_sequence = 0
        last_kind: str | None = None
        last_domain: str | None = None
        last_hash: str | None = None
        while cursor > 0 and last_sequence == 0:
            rows = self._connection.execute(
                """
                SELECT ledger_sequence, domain, kind, payload_json, record_hash
                FROM lane_iii_paper_audit WHERE ledger_sequence <= ?
                ORDER BY ledger_sequence DESC LIMIT 4096
                """,
                (cursor,),
            ).fetchall()
            if not rows:
                break
            for row in rows:
                sequence = int(row["ledger_sequence"])
                classification = self._stored_record_classification(row)
                if classification is None:
                    last_sequence = sequence
                    last_kind = str(row["kind"])
                    last_domain = str(row["domain"])
                    last_hash = str(row["record_hash"])
                    break
                safe_last[classification] = max(sequence, safe_last.get(classification, 0))
            cursor = int(rows[-1]["ledger_sequence"]) - 1
        return self._watermark_payload(
            classified_through_sequence=self._highest_sequence,
            last_authority_mutation_sequence=last_sequence,
            last_authority_mutation_kind=last_kind,
            last_authority_mutation_domain=last_domain,
            last_authority_mutation_hash=last_hash,
            safe_classification_last_sequences=safe_last,
            updated_at=_now(),
        )

    def _load_or_rebuild_authority_watermark(self, serialized: str | None) -> dict[str, object]:
        watermark: dict[str, object] | None = None
        if serialized:
            try:
                candidate = json.loads(serialized)
            except json.JSONDecodeError:
                candidate = None
            if isinstance(candidate, dict) and candidate.get("policy_version") == COMMISSIONING_TAIL_POLICY_VERSION:
                classified = candidate.get("classified_through_sequence")
                authority = candidate.get("last_authority_mutation_sequence")
                safe_last = candidate.get("safe_classification_last_sequences")
                if (
                    type(classified) is int
                    and 0 <= classified <= self._highest_sequence
                    and type(authority) is int
                    and 0 <= authority <= classified
                    and isinstance(safe_last, dict)
                    and all(
                        isinstance(key, str) and type(value) is int and 0 <= value <= classified
                        for key, value in safe_last.items()
                    )
                ):
                    watermark = candidate
                    if authority:
                        row = self._connection.execute(
                            "SELECT kind, domain, record_hash FROM lane_iii_paper_audit WHERE ledger_sequence=?",
                            (authority,),
                        ).fetchone()
                        expected = (
                            candidate.get("last_authority_mutation_kind"),
                            candidate.get("last_authority_mutation_domain"),
                            candidate.get("last_authority_mutation_hash"),
                        )
                        if row is None or (str(row["kind"]), str(row["domain"]), str(row["record_hash"])) != expected:
                            watermark = None
        if watermark is None or int(watermark["classified_through_sequence"]) != self._highest_sequence:
            # A missing/old policy or an image appended by an older runtime is
            # never trusted. Rebuild the safe suffix before commissioning.
            watermark = self._rebuild_authority_watermark()
            with self._transaction() as connection:
                self._store_authority_watermark(connection, watermark)
        return watermark

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
            "RISK_EVENT", "EXECUTION", "OBSERVATION", "EVIDENCE", "DECISION", "INTENT", "COMMAND", "SESSION", "INCIDENT",
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
        identity_payload = dict(payload)
        with self._lock:
            default_context = self._current_session_context
        session_kind_text = identity_payload.get("session_kind", default_context.session_kind.value)
        try:
            session_kind = PaperSessionKind(str(session_kind_text))
        except ValueError as exc:
            raise ValueError("Paper ledger record session kind is invalid.") from exc
        session_id = str(identity_payload.get("session_id", default_context.session_id))
        trade_date = str(identity_payload.get("trade_date", default_context.trade_date))
        profile_hash = str(identity_payload.get("session_profile_hash", default_context.session_profile_hash))
        generation = identity_payload.get("session_generation", default_context.session_generation)
        if type(generation) is not int:
            raise ValueError("Paper ledger record session generation is invalid.")
        # Pre-regime test fixtures used session_id for the authenticated
        # socket session. Such a shape cannot reach CreateOrder (the compiled
        # session fence rejects it); retain only enough compatibility to audit
        # it under a safe OFF_SESSION envelope.
        if session_kind is PaperSessionKind.OFF_SESSION and not session_id.startswith("MNQU6:OFF_SESSION:"):
            session_id = UNSPECIFIED_OFF_SESSION_CONTEXT.session_id
            trade_date = UNSPECIFIED_OFF_SESSION_CONTEXT.trade_date
            profile_hash = UNSPECIFIED_OFF_SESSION_CONTEXT.session_profile_hash
            generation = UNSPECIFIED_OFF_SESSION_CONTEXT.session_generation
        context = context_from_identity(session_kind, session_id, trade_date, profile_hash, generation)
        session_family = context.session_family.value
        supplied_family = identity_payload.get("session_family")
        if supplied_family is not None and supplied_family != session_family:
            raise ValueError("Paper ledger record session family is inconsistent with session identity.")
        identity_payload.setdefault("session_family", session_family)
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
            "session_kind": session_kind.value,
            "session_family": session_family,
            "session_id": session_id,
            "trade_date": trade_date,
            "session_profile_hash": profile_hash,
            "session_generation": generation,
            "payload": identity_payload,
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
        watermark = dict(self._authority_watermark)
        safe_last = dict(watermark.get("safe_classification_last_sequences") or {})
        inserted = False
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
                cursor = connection.execute(
                    """
                    INSERT INTO lane_iii_paper_audit
                        (identity, domain, kind, occurred_at, execution_session_id, payload_json, previous_record_hash, record_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (record_identity, domain, kind, at, execution_session_id, serialized, previous_hash, record_hash),
                )
                sequence = int(cursor.lastrowid)
                connection.execute(
                    f"INSERT INTO {_DOMAIN_TABLES[domain]} (identity, kind, occurred_at, execution_session_id, payload_json, record_hash) VALUES (?, ?, ?, ?, ?, ?)",
                    (record_identity, kind, at, execution_session_id, serialized, record_hash),
                )
                ownership_payload = common.get("payload")
                if kind == "COMMISSIONING_OWNERSHIP_RESERVED" and isinstance(ownership_payload, Mapping):
                    commissioning_id = ownership_payload.get("commissioning_id")
                    if isinstance(commissioning_id, str) and commissioning_id:
                        connection.execute(
                            """
                            INSERT INTO lane_iii_paper_commissioning_ownership
                                (commissioning_id, reservation_record_json, entry_consumed, released, updated_at)
                            VALUES (?, ?, 0, 0, ?)
                            ON CONFLICT(commissioning_id) DO UPDATE SET
                                reservation_record_json=excluded.reservation_record_json,
                                entry_consumed=0, entry_decision_id=NULL,
                                released=0, updated_at=excluded.updated_at
                            """,
                            (commissioning_id, serialized, at),
                        )
                elif kind == "COMMISSIONING_ENTRY_CONSUMED" and isinstance(ownership_payload, Mapping):
                    commissioning_id = ownership_payload.get("commissioning_id")
                    if isinstance(commissioning_id, str) and commissioning_id:
                        connection.execute(
                            """
                            INSERT INTO lane_iii_paper_commissioning_ownership
                                (commissioning_id, reservation_record_json, entry_consumed, entry_decision_id, released, updated_at)
                            VALUES (?, ?, 1, ?, 0, ?)
                            ON CONFLICT(commissioning_id) DO UPDATE SET
                                entry_consumed=1, entry_decision_id=excluded.entry_decision_id,
                                released=0, updated_at=excluded.updated_at
                            """,
                            (commissioning_id, serialized, ownership_payload.get("entry_decision_id"), at),
                        )
                elif kind == "COMMISSIONING_OWNERSHIP_RELEASED" and isinstance(ownership_payload, Mapping):
                    commissioning_id = ownership_payload.get("commissioning_id")
                    if isinstance(commissioning_id, str) and commissioning_id:
                        connection.execute(
                            "UPDATE lane_iii_paper_commissioning_ownership SET released=1, updated_at=? WHERE commissioning_id=?",
                            (at, commissioning_id),
                        )
                previous_hash = record_hash
                hashes.append(record_hash)
                inserted = True
                inner_payload = common.get("payload")
                classification = (
                    commissioning_safe_tail_classification(domain, kind, inner_payload)
                    if isinstance(inner_payload, Mapping)
                    else None
                )
                if classification is None:
                    watermark.update({
                        "last_authority_mutation_sequence": sequence,
                        "last_authority_mutation_kind": kind,
                        "last_authority_mutation_domain": domain,
                        "last_authority_mutation_hash": record_hash,
                    })
                else:
                    safe_last[classification] = sequence
                watermark.update({
                    "policy_version": COMMISSIONING_TAIL_POLICY_VERSION,
                    "classified_through_sequence": sequence,
                    "safe_classification_last_sequences": safe_last,
                    "updated_at": at,
                })
                self._counts_cache[domain] = self._counts_cache.get(domain, 0) + 1
                self._highest_sequence = sequence
                self._last_record_time = at
                self._final_record_hash = record_hash
            if inserted:
                self._store_authority_watermark(connection, watermark)
        if inserted:
            self._authority_watermark = watermark
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
            raise ValueError("Only raw observations, evidence, and no-side-effect decisions may use deferred persistence.")
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

    def recent(
        self,
        limit: int = 100,
        *,
        domain: str | None = None,
        session_kind: PaperSessionKind | str | None = None,
        session_family: str | None = None,
        trade_date: str | None = None,
        session_id: str | None = None,
    ) -> list[dict[str, object]]:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("Paper ledger query limit is invalid.")
        with self._ordering_lock:
            self.flush_deferred()
        with self._lock:
            clauses: list[str] = []
            values: list[object] = []
            if domain is not None:
                if domain not in _DOMAIN_TABLES:
                    raise ValueError("Unknown paper ledger domain.")
                clauses.append("domain = ?"); values.append(domain)
            if session_kind is not None:
                value = PaperSessionKind(str(session_kind)).value
                clauses.append("json_extract(payload_json, '$.session_kind') = ?"); values.append(value)
            if session_family is not None:
                if session_family not in {"NEW_YORK", "ASIA", "OFF_SESSION"}:
                    raise ValueError("Unknown paper session family.")
                clauses.append("json_extract(payload_json, '$.session_family') = ?"); values.append(session_family)
            if trade_date is not None:
                date.fromisoformat(trade_date)
                clauses.append("json_extract(payload_json, '$.trade_date') = ?"); values.append(trade_date)
            if session_id is not None:
                clauses.append("json_extract(payload_json, '$.session_id') = ?"); values.append(session_id)
            where = "" if not clauses else " WHERE " + " AND ".join(clauses)
            rows = self._connection.execute(
                "SELECT payload_json FROM lane_iii_paper_audit" + where + " ORDER BY ledger_sequence DESC LIMIT ?",
                (*values, limit),
            ).fetchall()
            return [json.loads(str(row["payload_json"])) for row in rows]

    def recent_kinds(self, kinds: tuple[str, ...], limit: int = 100) -> list[dict[str, object]]:
        """Return a bounded reverse-chronological audit slice for exact kinds.

        Runtime restart recovery needs a narrow operational query, not a scan of
        high-volume observation history.  The records remain normal hash-chain
        entries; this is only a read convenience for fail-closed recovery.
        """
        if not kinds or not all(isinstance(kind, str) and kind for kind in kinds):
            raise ValueError("Paper ledger record kinds must be non-empty strings.")
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("Paper ledger query limit is invalid.")
        with self._ordering_lock:
            self.flush_deferred()
        placeholders = ", ".join("?" for _ in kinds)
        with self._lock:
            rows = self._connection.execute(
                "SELECT payload_json FROM lane_iii_paper_audit WHERE kind IN (" + placeholders
                + ") ORDER BY ledger_sequence DESC LIMIT ?",
                (*kinds, limit),
            ).fetchall()
            return [json.loads(str(row["payload_json"])) for row in rows]

    def unresolved_commissioning_ownership(self) -> tuple[dict[str, object], bool] | None:
        """Read the transactional recovery marker without scanning audit history."""
        with self._ordering_lock:
            self.flush_deferred()
        with self._lock:
            row = self._connection.execute(
                """
                SELECT reservation_record_json, entry_consumed
                FROM lane_iii_paper_commissioning_ownership
                WHERE released=0
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            return json.loads(str(row["reservation_record_json"])), bool(row["entry_consumed"])

    def commissioning_ownership(
        self, commissioning_id: str,
    ) -> tuple[dict[str, object], bool, bool] | None:
        """Resolve one deterministic commissioning request without replaying it."""
        if not isinstance(commissioning_id, str) or not commissioning_id:
            raise ValueError("Commissioning identity is required.")
        with self._ordering_lock:
            self.flush_deferred()
        with self._lock:
            row = self._connection.execute(
                """
                SELECT reservation_record_json, entry_consumed, entry_decision_id, released
                FROM lane_iii_paper_commissioning_ownership
                WHERE commissioning_id=?
                """,
                (commissioning_id,),
            ).fetchone()
            if row is None:
                return None
            record = json.loads(str(row["reservation_record_json"]))
            payload = record.get("payload") if isinstance(record, dict) else None
            if isinstance(payload, dict) and isinstance(row["entry_decision_id"], str):
                payload["entry_decision_id"] = str(row["entry_decision_id"])
            return record, bool(row["entry_consumed"]), bool(row["released"])

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

    def chain_status(self) -> tuple[bool | None, str | None]:
        with self._lock:
            return self._chain_status

    def counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts_cache)

    def commissioning_tail_snapshot(
        self,
        verified_through_sequence: int,
        *,
        last_full_verified_sequence: int | None = None,
    ) -> dict[str, object]:
        """Capture the trusted-anchor boundary without scanning the live tail."""
        if type(verified_through_sequence) is not int or verified_through_sequence < 0:
            raise ValueError("Commissioning verified sequence is invalid.")
        if last_full_verified_sequence is not None and (
            type(last_full_verified_sequence) is not int or last_full_verified_sequence < 0
        ):
            raise ValueError("Commissioning Full verified sequence is invalid.")
        with self._ordering_lock:
            self.flush_deferred()
            with self._lock:
                tip = self._highest_sequence
                if verified_through_sequence > tip:
                    raise RuntimeError("Commissioning verified anchor is beyond the current ledger tip.")
                watermark = dict(self._authority_watermark)
                if int(watermark.get("classified_through_sequence") or -1) != tip:
                    raise RuntimeError("Commissioning authority classification does not reach the captured ledger tip.")
                hashes: dict[int, str | None] = {}
                requested = {verified_through_sequence}
                if last_full_verified_sequence is not None:
                    requested.add(last_full_verified_sequence)
                for sequence in requested:
                    if sequence == 0:
                        hashes[sequence] = None
                        continue
                    row = self._connection.execute(
                        "SELECT record_hash FROM lane_iii_paper_audit WHERE ledger_sequence=?", (sequence,)
                    ).fetchone()
                    hashes[sequence] = None if row is None else str(row["record_hash"])
                safe_last = dict(watermark.get("safe_classification_last_sequences") or {})
                tail_kinds = sorted(
                    classification
                    for classification, sequence in safe_last.items()
                    if type(sequence) is int and verified_through_sequence < sequence <= tip
                )
                return {
                    "policy_version": COMMISSIONING_TAIL_POLICY_VERSION,
                    "ledger_identity": self._ledger_uuid,
                    "ledger_epoch": self._ledger_epoch,
                    "ledger_schema_version": self._schema_version,
                    "verified_through_sequence": verified_through_sequence,
                    "verified_anchor_record_hash": hashes.get(verified_through_sequence),
                    "last_full_verified_sequence": last_full_verified_sequence,
                    "last_full_anchor_record_hash": (
                        None if last_full_verified_sequence is None else hashes.get(last_full_verified_sequence)
                    ),
                    "arm_snapshot_tip": tip,
                    "arm_snapshot_tip_hash": self._final_record_hash,
                    "unverified_tail_rows": tip - verified_through_sequence,
                    "tail_start_sequence": verified_through_sequence + 1 if tip > verified_through_sequence else None,
                    "tail_end_sequence": tip if tip > verified_through_sequence else None,
                    "tail_record_kinds": tail_kinds,
                    **watermark,
                }

    def health_status(self) -> dict[str, object]:
        """Return cached integrity state plus inexpensive filesystem metadata."""
        with self._lock:
            chain_valid, broken_identity = self._chain_status
            highest_sequence = self._highest_sequence
            last_record_time = self._last_record_time
            final_record_hash = self._final_record_hash
            quick_check_state = self._quick_check_state
            authority_watermark = dict(self._authority_watermark)
        try:
            file_size: int | None = self.path.stat().st_size
        except OSError:
            file_size = None
        wal_path = Path(str(self.path) + "-wal")
        try:
            wal_size = wal_path.stat().st_size
        except OSError:
            wal_size = 0
        try:
            free_bytes: int | None = shutil.disk_usage(self.path.parent).free
        except OSError:
            free_bytes = None
        return {
            "path": str(self.path),
            "epoch_id": self._ledger_epoch,
            "epoch_state": "LEGACY / UNSPECIFIED" if self._ledger_epoch == "UNSPECIFIED" else "EXPLICIT",
            "file_size": file_size,
            "free_bytes": free_bytes,
            "quick_check_state": quick_check_state,
            "chain_valid": chain_valid,
            "broken_identity": broken_identity,
            "highest_sequence": highest_sequence,
            "last_record_time": last_record_time,
            "final_record_hash": final_record_hash,
            "wal_size": wal_size,
            "counts": self.counts(),
            "authority_watermark": authority_watermark,
        }

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
