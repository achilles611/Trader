"""Identity-bound shadow commitments and exact LSC provenance resolution.

The commitment log is append-only and deliberately separate from the Lane III
authority ledger.  It can prove which immutable LSC segments were acknowledged
by the shadow path, but it cannot create, mutate, or authorize an authority
record.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import re
from typing import Callable, Mapping
from uuid import uuid4

from src.lsc.codec import decode_context
from src.lsc.segment_store import (
    FaultHook,
    GENESIS_COMMITMENT,
    LSCStoreError,
    LSCStoreIntegrityError,
    MAX_METADATA_BYTES,
    QuarantinedTemporary,
    SealedSegment,
    SegmentStore,
    StoredEvent,
    _atomic_write,
    _canonical_json,
    _domain_hash,
    _hex64,
    _publish_noreplace,
    _strict_json,
    _sync_directory,
    _utc_now,
)


SHADOW_COMMITMENT_SCHEMA = "beelzebub-lsc-shadow-commitment-v1"
IDENTITY_SCHEMA = "beelzebub-lsc-shadow-identity-v1"
LSC_FORMAT_IDENTITY = "LSC-V1-LSCR-SEG01"
_JOURNAL_NAME = re.compile(r"^(?P<sequence>[0-9]{20})-(?P<hash>[0-9a-f]{64})\.json$")
_JOURNAL_DOMAIN = b"beelzebub.lsc.v1.shadow.commitment\0"
_IDENTITY_DOMAIN = b"beelzebub.lsc.v1.shadow.identity\0"
_STORAGE_IDENTITY_DOMAIN = b"beelzebub.lsc.v1.storage.identity\0"
STORAGE_IDENTITY_PREFIX = "beelzebub-lsc-storage-v1:"


class LSCCommitmentError(LSCStoreError):
    """A shadow commitment operation was refused."""


class LSCCommitmentIntegrityError(LSCStoreIntegrityError):
    """The shadow commitment chain or provenance did not verify."""


def _identity_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > 512:
        raise ValueError(f"{label} must be non-empty, trimmed, and at most 512 characters.")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ValueError(f"{label} contains a control character.")
    return value


def _commitment_time(value: object) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise LSCCommitmentIntegrityError("LSC_SHADOW_COMMITMENT_TIME_INVALID")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise LSCCommitmentIntegrityError("LSC_SHADOW_COMMITMENT_TIME_INVALID") from exc
    normalized = parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if value != normalized:
        raise LSCCommitmentIntegrityError("LSC_SHADOW_COMMITMENT_TIME_NOT_CANONICAL")
    return value


def _resolved_storage_path(path: Path, label: str, *, require_exists: bool = True) -> str:
    """Return one stable spelling of an existing destination path."""

    try:
        resolved = path.resolve(strict=require_exists)
    except (OSError, RuntimeError) as exc:
        raise LSCCommitmentError(f"LSC_STORAGE_{label}_UNRESOLVED") from exc
    if require_exists and not resolved.is_dir():
        raise LSCCommitmentError(f"LSC_STORAGE_{label}_NOT_DIRECTORY")
    # normcase collapses Windows drive/path case and separator aliases.  The
    # resolved absolute spelling also collapses symlink, junction, dot, and
    # dot-dot aliases before it is committed.
    return os.path.normcase(str(resolved))


def canonical_storage_identity(
    store: SegmentStore,
    journal_root: str | Path | None = None,
    *,
    require_journal_root: bool = False,
) -> str:
    """Bind a shadow identity to the store's actual resolved destination."""

    if not isinstance(store, SegmentStore):
        raise TypeError("store must be SegmentStore")
    journal_path = Path(journal_root) if journal_root is not None else store.root / "shadow-commitments"
    payload = {
        "journal_root": _resolved_storage_path(
            journal_path,
            "JOURNAL_ROOT",
            require_exists=require_journal_root,
        ),
        "root": _resolved_storage_path(store.root, "ROOT"),
        "schema": "beelzebub-lsc-storage-identity-v1",
        "segment_root": _resolved_storage_path(store.segment_root, "SEGMENT_ROOT"),
    }
    return STORAGE_IDENTITY_PREFIX + _domain_hash(
        _STORAGE_IDENTITY_DOMAIN,
        _canonical_json(payload),
    )


@dataclass(frozen=True)
class ShadowIdentityContext:
    """Immutable identity of one independent shadow evidence journal."""

    journal_id: str
    profile_id: str
    context_digest: str
    observation_schema: str
    source: str
    provider: str
    environment: str
    producer_protocol_version: str | None
    producer_source_fingerprint: str | None
    producer_build_fingerprint: str | None
    producer_build_timestamp: str | None
    native_contract_id: str
    canonical_contract_id: str
    tick_size: str
    session_id: str
    generation: int
    legacy_mode: bool
    format_identity: str
    storage_identity: str

    def __post_init__(self) -> None:
        for name in (
            "journal_id", "profile_id", "observation_schema", "source", "provider",
            "environment", "native_contract_id", "canonical_contract_id", "tick_size",
            "session_id", "format_identity", "storage_identity",
        ):
            _identity_text(getattr(self, name), name)
        _hex64(self.context_digest, "CONTEXT_DIGEST")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("generation must be a non-negative integer")
        if type(self.legacy_mode) is not bool:
            raise ValueError("legacy_mode must be boolean")
        producer_names = (
            "producer_protocol_version", "producer_source_fingerprint",
            "producer_build_fingerprint", "producer_build_timestamp",
        )
        if self.legacy_mode:
            if any(getattr(self, name) is not None for name in producer_names):
                raise ValueError("legacy identity cannot claim producer attestation")
        else:
            for name in producer_names:
                _identity_text(getattr(self, name), name)
        if self.format_identity != LSC_FORMAT_IDENTITY:
            raise ValueError(f"format_identity must be {LSC_FORMAT_IDENTITY}")
        storage_digest = self.storage_identity.removeprefix(STORAGE_IDENTITY_PREFIX)
        if (
            not self.storage_identity.startswith(STORAGE_IDENTITY_PREFIX)
            or len(storage_digest) != 64
            or any(character not in "0123456789abcdef" for character in storage_digest)
        ):
            raise ValueError("storage_identity must be a canonical LSC storage identity")

    def payload(self) -> dict[str, object]:
        return {
            "canonical_contract_id": self.canonical_contract_id,
            "context_digest": self.context_digest,
            "environment": self.environment,
            "format_identity": self.format_identity,
            "generation": self.generation,
            "journal_id": self.journal_id,
            "legacy_mode": self.legacy_mode,
            "native_contract_id": self.native_contract_id,
            "observation_schema": self.observation_schema,
            "profile_id": self.profile_id,
            "producer_build_fingerprint": self.producer_build_fingerprint,
            "producer_build_timestamp": self.producer_build_timestamp,
            "producer_protocol_version": self.producer_protocol_version,
            "producer_source_fingerprint": self.producer_source_fingerprint,
            "provider": self.provider,
            "schema": IDENTITY_SCHEMA,
            "session_id": self.session_id,
            "source": self.source,
            "storage_identity": self.storage_identity,
            "tick_size": self.tick_size,
        }

    @property
    def digest(self) -> str:
        return _domain_hash(_IDENTITY_DOMAIN, _canonical_json(self.payload()))


@dataclass(frozen=True)
class ShadowCommitmentRecord:
    journal_sequence: int
    journal_hash: str
    previous_journal_hash: str
    identity_digest: str
    segment_id: str
    segment_ordinal: int
    segment_commitment: str
    previous_segment_commitment: str
    content_digest: str
    merkle_root: str
    context_digest: str
    record_count: int
    event_index_start: int
    event_index_end: int
    local_sequence_start: int
    local_sequence_end: int
    path: Path
    durability_method: str


@dataclass(frozen=True)
class DecisionProvenanceReference:
    segment_id: str
    event_index: int
    local_sequence: int
    observation_id: str
    payload_hash: str
    event_digest: str

    def __post_init__(self) -> None:
        _hex64(self.segment_id, "SEGMENT_ID")
        _hex64(self.payload_hash, "PAYLOAD_HASH")
        _hex64(self.event_digest, "EVENT_DIGEST")
        if type(self.event_index) is not int or self.event_index < 0:
            raise ValueError("event_index must be a non-negative integer")
        if type(self.local_sequence) is not int or self.local_sequence < 0:
            raise ValueError("local_sequence must be a non-negative integer")
        if not isinstance(self.observation_id, str) or not self.observation_id:
            raise ValueError("observation_id is required")

    def payload(self) -> dict[str, object]:
        return {
            "event_digest": self.event_digest,
            "event_index": self.event_index,
            "local_sequence": self.local_sequence,
            "observation_id": self.observation_id,
            "payload_hash": self.payload_hash,
            "segment_id": self.segment_id,
        }


@dataclass(frozen=True)
class CommitmentRecoveryReport:
    quarantined: tuple[QuarantinedTemporary, ...]
    committed_segment_ids: tuple[str, ...]
    uncommitted_segment_ids: tuple[str, ...]
    identity_digest: str


def _segment_fields(segment: SealedSegment) -> dict[str, object]:
    return {
        "content_digest": segment.content_digest,
        "context_digest": segment.context_digest,
        "event_index_end": segment.record_count - 1,
        "event_index_start": 0,
        "local_sequence_end": segment.last_local_sequence,
        "local_sequence_start": segment.first_local_sequence,
        "merkle_root": segment.merkle_root,
        "previous_segment_commitment": segment.previous_commitment,
        "record_count": segment.record_count,
        "segment_commitment": segment.segment_commitment,
        "segment_id": segment.segment_id,
        "segment_ordinal": segment.ordinal,
    }


class ShadowCommitmentLog:
    """Append-only shadow acknowledgement chain over verified segment files."""

    def __init__(
        self,
        store: SegmentStore,
        identity: ShadowIdentityContext,
        *,
        root: str | Path | None = None,
        wall_clock: Callable[[], str] = _utc_now,
    ) -> None:
        if not isinstance(store, SegmentStore):
            raise TypeError("store must be SegmentStore")
        if not isinstance(identity, ShadowIdentityContext):
            raise TypeError("identity must be ShadowIdentityContext")
        self.store = store
        self.identity = identity
        self.root = Path(root) if root is not None else store.root / "shadow-commitments"
        self.quarantine_root = self.root / "quarantine"
        self._wall_clock = wall_clock
        resolved_root = self.root.resolve()
        resolved_segments = store.segment_root.resolve()
        if (
            self.root.is_symlink()
            or resolved_root == resolved_segments
            or resolved_root in resolved_segments.parents
            or resolved_segments in resolved_root.parents
        ):
            raise LSCCommitmentError("LSC_COMMITMENT_ROOT_MUST_BE_DISTINCT_FROM_SEGMENT_ROOT")
        actual_storage_identity = canonical_storage_identity(store, self.root)
        if identity.storage_identity != actual_storage_identity:
            raise LSCCommitmentError("LSC_STORAGE_IDENTITY_DESTINATION_MISMATCH")
        self.root.mkdir(parents=True, exist_ok=True)
        self.quarantine_root.mkdir(parents=True, exist_ok=True)
        self._verify_storage_identity()

    def _verify_storage_identity(self) -> None:
        try:
            actual = canonical_storage_identity(
                self.store,
                self.root,
                require_journal_root=True,
            )
        except LSCCommitmentError as exc:
            raise LSCCommitmentIntegrityError("LSC_STORAGE_IDENTITY_UNVERIFIED") from exc
        if actual != self.identity.storage_identity:
            raise LSCCommitmentIntegrityError("LSC_STORAGE_IDENTITY_DESTINATION_MISMATCH")

    @property
    def identity_digest(self) -> str:
        return self.identity.digest

    def _read_record(
        self,
        path: Path,
        sequence: int,
        previous_hash: str,
        segments: Mapping[str, SealedSegment],
    ) -> ShadowCommitmentRecord:
        match = _JOURNAL_NAME.fullmatch(path.name)
        if (
            match is None or path.is_symlink() or not path.is_file()
            or path.stat().st_size <= 0 or path.stat().st_size > MAX_METADATA_BYTES
        ):
            raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_PATH_INVALID")
        value = _strict_json(path.read_bytes(), "LSC_SHADOW_JOURNAL")
        if not isinstance(value, dict):
            raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_SHAPE_INVALID")
        required = {
            "committed_at", "content_digest", "context_digest", "event_index_end",
            "event_index_start", "identity_context", "identity_digest", "journal_hash",
            "journal_sequence", "local_sequence_end", "local_sequence_start", "merkle_root",
            "previous_journal_hash", "previous_segment_commitment", "record_count", "schema",
            "segment_commitment", "segment_id", "segment_ordinal",
        }
        if set(value) != required or value.get("schema") != SHADOW_COMMITMENT_SCHEMA:
            raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_FIELDS_INVALID")
        _commitment_time(value.get("committed_at"))
        for name in (
            "journal_sequence", "segment_ordinal", "record_count", "event_index_start",
            "event_index_end", "local_sequence_start", "local_sequence_end",
        ):
            if type(value.get(name)) is not int or int(value[name]) < 0:
                raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_NUMERIC_FIELD_INVALID")
        for name in (
            "content_digest", "context_digest", "identity_digest", "journal_hash", "merkle_root",
            "previous_journal_hash", "previous_segment_commitment", "segment_commitment", "segment_id",
        ):
            _hex64(value.get(name), name.upper())
        if value.get("identity_context") != self.identity.payload() or value.get("identity_digest") != self.identity_digest:
            raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_CROSS_IDENTITY_REFUSED")
        if value.get("journal_sequence") != sequence or int(match.group("sequence")) != sequence:
            raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_DUPLICATE_FORK_OR_SEQUENCE_INVALID")
        if value.get("previous_journal_hash") != previous_hash:
            raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_PREVIOUS_LINK_MISMATCH")
        segment_id = _hex64(value.get("segment_id"), "SEGMENT_ID")
        segment = segments.get(segment_id)
        if segment is None:
            raise LSCCommitmentIntegrityError("LSC_COMMITMENT_WITHOUT_VERIFIED_SEGMENT")
        self._validate_segment_identity(segment)
        if segment.ordinal != sequence:
            raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_NOT_EXACT_SEGMENT_PREFIX")
        expected_segment = _segment_fields(segment)
        if any(value.get(name) != expected for name, expected in expected_segment.items()):
            raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_SEGMENT_MISMATCH")
        core = {name: item for name, item in value.items() if name != "journal_hash"}
        journal_hash = _domain_hash(_JOURNAL_DOMAIN, _canonical_json(core))
        if value.get("journal_hash") != journal_hash or match.group("hash") != journal_hash:
            raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_HASH_INVALID")
        return ShadowCommitmentRecord(
            journal_sequence=sequence,
            journal_hash=journal_hash,
            previous_journal_hash=previous_hash,
            identity_digest=self.identity_digest,
            segment_id=segment_id,
            segment_ordinal=segment.ordinal,
            segment_commitment=segment.segment_commitment,
            previous_segment_commitment=segment.previous_commitment,
            content_digest=segment.content_digest,
            merkle_root=segment.merkle_root,
            context_digest=segment.context_digest,
            record_count=segment.record_count,
            event_index_start=0,
            event_index_end=segment.record_count - 1,
            local_sequence_start=segment.first_local_sequence,
            local_sequence_end=segment.last_local_sequence,
            path=path,
            durability_method="verified_existing_file",
        )

    def verify(self) -> tuple[ShadowCommitmentRecord, ...]:
        self._verify_storage_identity()
        sealed = self.store.verify_chain()
        segments = {item.segment_id: item for item in sealed}
        paths = sorted(self.root.glob("*.json"), key=lambda item: item.name)
        result: list[ShadowCommitmentRecord] = []
        previous_hash = GENESIS_COMMITMENT
        for sequence, path in enumerate(paths):
            record = self._read_record(path, sequence, previous_hash, segments)
            if record.segment_id != sealed[sequence].segment_id:
                raise LSCCommitmentIntegrityError("LSC_SHADOW_JOURNAL_SEGMENT_FORK_REFUSED")
            result.append(record)
            previous_hash = record.journal_hash
        self._verify_storage_identity()
        return tuple(result)

    def _validate_segment_identity(self, segment: SealedSegment) -> None:
        if segment.context_digest != self.identity.context_digest:
            raise LSCCommitmentIntegrityError("LSC_SEGMENT_CONTEXT_DIGEST_IDENTITY_MISMATCH")
        try:
            context = decode_context(self.store.read_context(segment.segment_id))
        except Exception as exc:
            raise LSCCommitmentIntegrityError("LSC_COMMITTED_SEGMENT_CONTEXT_UNVERIFIED") from exc
        expected = {
            "observation_schema": self.identity.observation_schema,
            "source": self.identity.source,
            "provider": self.identity.provider,
            "environment": self.identity.environment,
            "producer_protocol_version": self.identity.producer_protocol_version,
            "producer_source_fingerprint": self.identity.producer_source_fingerprint,
            "producer_build_fingerprint": self.identity.producer_build_fingerprint,
            "producer_build_timestamp": self.identity.producer_build_timestamp,
            "native_contract_id": self.identity.native_contract_id,
            "canonical_contract_id": self.identity.canonical_contract_id,
            "tick_size": self.identity.tick_size,
            "session_id": self.identity.session_id,
            "generation": self.identity.generation,
            "legacy_mode": self.identity.legacy_mode,
        }
        if any(getattr(context, name) != value for name, value in expected.items()):
            raise LSCCommitmentIntegrityError("LSC_SEGMENT_CROSS_IDENTITY_REFUSED")

    def commit_segment(
        self,
        segment_id: str,
        *,
        fault_hook: FaultHook | None = None,
    ) -> ShadowCommitmentRecord:
        segment_id = _hex64(segment_id, "SEGMENT_ID")
        sealed = self.store.verify_chain()
        existing = self.verify()
        if any(item.segment_id == segment_id for item in existing):
            raise LSCCommitmentError("LSC_SHADOW_COMMITMENT_DUPLICATE_SEGMENT_REFUSED")
        sealed_by_id = {item.segment_id: item for item in sealed}
        segment = sealed_by_id.get(segment_id)
        if segment is None:
            raise LSCCommitmentError("LSC_COMMITMENT_WITHOUT_VERIFIED_SEGMENT")
        self._validate_segment_identity(segment)
        sequence = len(existing)
        if sequence >= len(sealed) or sealed[sequence].segment_id != segment_id or segment.ordinal != sequence:
            raise LSCCommitmentError("LSC_SHADOW_COMMITMENT_SKIP_OR_FORK_REFUSED")
        previous_hash = GENESIS_COMMITMENT if not existing else existing[-1].journal_hash
        core: dict[str, object] = {
            "committed_at": _commitment_time(self._wall_clock()),
            **_segment_fields(segment),
            "identity_context": self.identity.payload(),
            "identity_digest": self.identity_digest,
            "journal_sequence": sequence,
            "previous_journal_hash": previous_hash,
            "schema": SHADOW_COMMITMENT_SCHEMA,
        }
        journal_hash = _domain_hash(_JOURNAL_DOMAIN, _canonical_json(core))
        value = {**core, "journal_hash": journal_hash}
        destination = self.root / f"{sequence:020d}-{journal_hash}.json"
        method = _atomic_write(
            destination,
            (("after_commitment_write", _canonical_json(value)),),
            fault_hook=fault_hook,
            temp_label="lsc-shadow-commitment",
        )
        verified = self.verify()
        if not verified or verified[-1].journal_hash != journal_hash:
            raise LSCCommitmentIntegrityError("LSC_SHADOW_COMMITMENT_POST_WRITE_VERIFY_FAILED")
        return ShadowCommitmentRecord(**{**verified[-1].__dict__, "durability_method": method})

    def _quarantine(self, path: Path) -> QuarantinedTemporary:
        size = path.stat().st_size
        digest: str | None = None
        if size <= MAX_METADATA_BYTES:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        destination = self.quarantine_root / (
            f"{path.name}.{digest or 'oversize'}.{uuid4().hex}.quarantine"
        )
        _publish_noreplace(path, destination)
        _sync_directory(path.parent)
        if path.parent != destination.parent:
            _sync_directory(destination.parent)
        return QuarantinedTemporary(
            path.name,
            destination,
            size,
            digest,
            "INCOMPLETE_SHADOW_COMMITMENT_NEVER_ACKNOWLEDGED",
        )

    def recover(self) -> CommitmentRecoveryReport:
        # Destination identity is checked before quarantine can mutate any
        # recovery artifact, then again after full segment/journal validation.
        self._verify_storage_identity()
        quarantined: list[QuarantinedTemporary] = []
        for path in sorted(self.root.glob("*.tmp"), key=lambda item: item.name):
            if path.is_symlink() or not path.is_file():
                raise LSCCommitmentIntegrityError("LSC_SHADOW_COMMITMENT_TEMPORARY_PATH_INVALID")
            quarantined.append(self._quarantine(path))
        sealed = self.store.verify_chain()
        for segment in sealed:
            self._validate_segment_identity(segment)
        committed = self.verify()
        committed_ids = tuple(item.segment_id for item in committed)
        sealed_ids = tuple(item.segment_id for item in sealed)
        if sealed_ids[:len(committed_ids)] != committed_ids:
            raise LSCCommitmentIntegrityError("LSC_SHADOW_COMMITMENT_NOT_SEALED_PREFIX")
        report = CommitmentRecoveryReport(
            quarantined=tuple(quarantined),
            committed_segment_ids=committed_ids,
            uncommitted_segment_ids=sealed_ids[len(committed_ids):],
            identity_digest=self.identity_digest,
        )
        self._verify_storage_identity()
        return report

    def _committed_record(self, segment_id: str) -> ShadowCommitmentRecord:
        segment_id = _hex64(segment_id, "SEGMENT_ID")
        for record in self.verify():
            if record.segment_id == segment_id:
                return record
        raise LSCCommitmentError("LSC_PROVENANCE_SEGMENT_NOT_COMMITTED")

    def reference_for(
        self,
        segment_id: str,
        event_index: int,
        local_sequence: int,
    ) -> DecisionProvenanceReference:
        commitment = self._committed_record(segment_id)
        if not commitment.event_index_start <= event_index <= commitment.event_index_end:
            raise LSCCommitmentError("LSC_PROVENANCE_EVENT_INDEX_OUTSIDE_COMMITMENT")
        if not commitment.local_sequence_start <= local_sequence <= commitment.local_sequence_end:
            raise LSCCommitmentError("LSC_PROVENANCE_SEQUENCE_OUTSIDE_COMMITMENT")
        try:
            event = self.store.lookup(segment_id, event_index, local_sequence)
        except LSCStoreError as exc:
            raise LSCCommitmentIntegrityError("LSC_PROVENANCE_COORDINATE_RESOLUTION_FAILED") from exc
        return DecisionProvenanceReference(
            segment_id=segment_id,
            event_index=event_index,
            local_sequence=local_sequence,
            observation_id=event.observation_id,
            payload_hash=event.payload_hash,
            event_digest=event.event_digest,
        )

    def resolve_reference(self, reference: DecisionProvenanceReference) -> StoredEvent:
        if not isinstance(reference, DecisionProvenanceReference):
            raise TypeError("reference must be DecisionProvenanceReference")
        commitment = self._committed_record(reference.segment_id)
        if not commitment.event_index_start <= reference.event_index <= commitment.event_index_end:
            raise LSCCommitmentError("LSC_PROVENANCE_EVENT_INDEX_OUTSIDE_COMMITMENT")
        if not commitment.local_sequence_start <= reference.local_sequence <= commitment.local_sequence_end:
            raise LSCCommitmentError("LSC_PROVENANCE_SEQUENCE_OUTSIDE_COMMITMENT")
        try:
            event = self.store.lookup(
                reference.segment_id,
                reference.event_index,
                reference.local_sequence,
            )
        except LSCStoreError as exc:
            raise LSCCommitmentIntegrityError("LSC_PROVENANCE_COORDINATE_RESOLUTION_FAILED") from exc
        expected = reference.payload()
        actual = {
            "event_digest": event.event_digest,
            "event_index": event.event_index,
            "local_sequence": event.local_sequence,
            "observation_id": event.observation_id,
            "payload_hash": event.payload_hash,
            "segment_id": event.segment_id,
        }
        if actual != expected:
            raise LSCCommitmentIntegrityError("LSC_DECISION_PROVENANCE_REFERENCE_MISMATCH")
        return event


__all__ = [
    "CommitmentRecoveryReport",
    "DecisionProvenanceReference",
    "LSCCommitmentError",
    "LSCCommitmentIntegrityError",
    "ShadowCommitmentLog",
    "ShadowCommitmentRecord",
    "ShadowIdentityContext",
    "LSC_FORMAT_IDENTITY",
    "STORAGE_IDENTITY_PREFIX",
    "canonical_storage_identity",
]
