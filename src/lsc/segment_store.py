"""Bounded, immutable segment storage for LSC shadow evidence.

This module owns no socket, runtime callback, broker operation, or authority
ledger write.  It accepts records already encoded and authenticated by the LSC
codec, seals them into independently verifiable files, and optionally commits
those files to a separate shadow-only journal.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import time
from typing import Callable, Mapping, Protocol, Sequence
from uuid import uuid4
import zlib


SEGMENT_SCHEMA = "beelzebub-lsc-segment-v1"
CONTAINER_MAGIC = b"LSCSEG01"
CONTAINER_VERSION = 1
COMPRESSION = "zlib-1.2.13-level-6"
GENESIS_COMMITMENT = "0" * 64

MAX_ENCODED_CONTEXT_BYTES = 65_536
MAX_ENCODED_RECORD_BYTES = 2_097_152
DEFAULT_MAX_RECORDS = 4_096
DEFAULT_MAX_RAW_BODY_BYTES = 32 * 1024 * 1024
DEFAULT_MAX_COMPRESSED_BODY_BYTES = 34 * 1024 * 1024
DEFAULT_MAX_SEGMENT_AGE_SECONDS = 2.0
MAX_METADATA_BYTES = 8 * 1024 * 1024

_PREFIX = struct.Struct("<8sHHIIIII")
_FRAME_LENGTH = struct.Struct("<I")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_SEGMENT_NAME = re.compile(r"^(?P<ordinal>[0-9]{20})-(?P<segment>[0-9a-f]{64})\.lsc$")

_MERKLE_NODE_DOMAIN = b"beelzebub.lsc.v1.merkle.node\0"
_MERKLE_ODD_DOMAIN = b"beelzebub.lsc.v1.merkle.odd\0"
_EMPTY_MERKLE_DOMAIN = b"beelzebub.lsc.v1.merkle.empty\0"
_CONTENT_DOMAIN = b"beelzebub.lsc.v1.segment.content\0"
_COMMITMENT_DOMAIN = b"beelzebub.lsc.v1.segment.commitment\0"


class LSCStoreError(RuntimeError):
    """The segment store refused an unsafe or invalid operation."""


class LSCStoreLimitError(LSCStoreError):
    """A configured resource bound would be exceeded."""


class LSCStoreIntegrityError(LSCStoreError):
    """Stored evidence failed structural or cryptographic verification."""


class LSCStoreRecoveryError(LSCStoreError):
    """Startup recovery found an ambiguous or unsafe state."""


class EncodedRecordLike(Protocol):
    record_bytes: bytes
    observation_id: str
    observation_type: str
    local_sequence: int
    payload_hash: str
    wire_hash: str
    event_digest: str
    context_digest: str


FaultHook = Callable[[str], None]
ContextDigestVerifier = Callable[[bytes], str]


@dataclass(frozen=True)
class VerifiedRecordMetadata:
    observation_id: str
    observation_type: str
    local_sequence: int
    payload_hash: str
    wire_hash: str
    event_digest: str
    context_digest: str


RecordVerifier = Callable[[bytes, bytes], VerifiedRecordMetadata]


@dataclass(frozen=True)
class SegmentLimits:
    max_records: int = DEFAULT_MAX_RECORDS
    max_record_bytes: int = MAX_ENCODED_RECORD_BYTES
    max_context_bytes: int = MAX_ENCODED_CONTEXT_BYTES
    max_raw_body_bytes: int = DEFAULT_MAX_RAW_BODY_BYTES
    max_compressed_body_bytes: int = DEFAULT_MAX_COMPRESSED_BODY_BYTES
    max_segment_age_seconds: float = DEFAULT_MAX_SEGMENT_AGE_SECONDS

    def __post_init__(self) -> None:
        integer_limits = (
            self.max_records,
            self.max_record_bytes,
            self.max_context_bytes,
            self.max_raw_body_bytes,
            self.max_compressed_body_bytes,
        )
        if any(type(value) is not int or value <= 0 for value in integer_limits):
            raise ValueError("LSC segment limits must be positive integers.")
        if self.max_record_bytes > MAX_ENCODED_RECORD_BYTES:
            raise ValueError("LSC record limit cannot exceed the codec boundary.")
        if self.max_context_bytes > MAX_ENCODED_CONTEXT_BYTES:
            raise ValueError("LSC context limit cannot exceed the codec boundary.")
        if not isinstance(self.max_segment_age_seconds, (int, float)) or isinstance(
            self.max_segment_age_seconds, bool
        ) or not 0 < float(self.max_segment_age_seconds) <= 60.0:
            raise ValueError("LSC segment age must be in (0, 60] seconds.")


@dataclass(frozen=True)
class SealedSegment:
    segment_id: str
    ordinal: int
    previous_commitment: str
    segment_commitment: str
    content_digest: str
    merkle_root: str
    context_digest: str
    record_count: int
    first_local_sequence: int
    last_local_sequence: int
    raw_body_bytes: int
    compressed_body_bytes: int
    path: Path
    durability_method: str
    recovered_without_receipt: bool = False


@dataclass(frozen=True)
class StoredEvent:
    segment_id: str
    event_index: int
    observation_id: str
    observation_type: str
    local_sequence: int
    payload_hash: str
    wire_hash: str
    event_digest: str
    context_digest: str
    record_bytes: bytes


@dataclass(frozen=True)
class QuarantinedTemporary:
    source_name: str
    quarantine_path: Path
    size_bytes: int
    sha256: str | None
    reason: str


@dataclass(frozen=True)
class RecoveryReport:
    quarantined: tuple[QuarantinedTemporary, ...]
    sealed_segment_ids: tuple[str, ...]


@dataclass(frozen=True)
class _ParsedSegment:
    sealed: SealedSegment
    context_bytes: bytes
    raw_body: bytes
    index: tuple[Mapping[str, object], ...]


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise LSCStoreIntegrityError("LSC_DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def _strict_json(data: bytes, label: str) -> object:
    if len(data) > MAX_METADATA_BYTES:
        raise LSCStoreLimitError(f"{label}_METADATA_LIMIT_EXCEEDED")
    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_strict_object)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise LSCStoreIntegrityError(f"{label}_JSON_INVALID") from exc
    if _canonical_json(value) != data:
        raise LSCStoreIntegrityError(f"{label}_JSON_NOT_CANONICAL")
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hex64(value: object, label: str) -> str:
    if not isinstance(value, str) or _HEX64.fullmatch(value) is None:
        raise LSCStoreIntegrityError(f"{label}_INVALID")
    return value


def _domain_hash(domain: bytes, *parts: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(domain)
    for part in parts:
        digest.update(struct.pack("<Q", len(part)))
        digest.update(part)
    return digest.hexdigest()


def _merkle_root(event_digests: Sequence[str]) -> str:
    if not event_digests:
        return hashlib.sha256(_EMPTY_MERKLE_DOMAIN).hexdigest()
    # The codec's event_digest is already a domain-separated leaf.  Do not
    # recanonicalize or wrap it in a second leaf hash here.
    level = [bytes.fromhex(_hex64(item, "EVENT_DIGEST")) for item in event_digests]
    while len(level) > 1:
        next_level: list[bytes] = []
        for index in range(0, len(level), 2):
            left = level[index]
            if index + 1 < len(level):
                next_level.append(hashlib.sha256(_MERKLE_NODE_DOMAIN + left + level[index + 1]).digest())
            else:
                next_level.append(hashlib.sha256(_MERKLE_ODD_DOMAIN + left).digest())
        level = next_level
    return level[0].hex()


def _default_context_digest(encoded_context: bytes) -> str:
    # Import lazily so the segment module remains independently importable
    # during construction and never duplicates codec canonicalization.
    from src.lsc.codec import context_digest

    return context_digest(encoded_context)


def _default_record_verifier(record_bytes: bytes, encoded_context: bytes) -> VerifiedRecordMetadata:
    from src.lsc.codec import context_digest, decode_observation

    observation = decode_observation(record_bytes, encoded_context)
    if len(record_bytes) < 96:
        raise LSCStoreIntegrityError("LSC_ENCODED_RECORD_TRAILER_MISSING")
    return VerifiedRecordMetadata(
        observation_id=observation.observation_id,
        observation_type=observation.observation_type,
        local_sequence=observation.local_monotonic_sequence,
        payload_hash=record_bytes[-96:-64].hex(),
        wire_hash=record_bytes[-64:-32].hex(),
        event_digest=record_bytes[-32:].hex(),
        context_digest=context_digest(encoded_context),
    )


def _write_all(descriptor: int, value: bytes) -> None:
    view = memoryview(value)
    written = 0
    while written < len(view):
        count = os.write(descriptor, view[written:])
        if count <= 0:
            raise OSError("short write while sealing LSC artifact")
        written += count


def _fault(hook: FaultHook | None, stage: str) -> None:
    if hook is not None:
        hook(stage)


def _same_volume(source: Path, destination_parent: Path) -> None:
    if os.stat(source).st_dev != os.stat(destination_parent).st_dev:
        raise LSCStoreError("LSC_ATOMIC_PUBLISH_REQUIRES_SAME_VOLUME")


def _windows_move_noreplace_write_through(source: Path, destination: Path) -> None:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    move = kernel32.MoveFileExW
    move.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
    move.restype = ctypes.c_int
    # MOVEFILE_WRITE_THROUGH without MOVEFILE_REPLACE_EXISTING.  This is a
    # same-volume rename and refuses an existing final name.
    if not move(str(source), str(destination), 0x00000008):
        error = ctypes.get_last_error()
        if destination.exists():
            raise FileExistsError(destination)
        raise OSError(error, ctypes.FormatError(error), str(destination))


def _publish_noreplace(source: Path, destination: Path) -> str:
    if destination.exists():
        raise FileExistsError(destination)
    _same_volume(source, destination.parent)
    if os.name == "nt":
        _windows_move_noreplace_write_through(source, destination)
        return "windows_same_volume_movefileex_write_through"
    # link(2) publishes a complete inode atomically and, unlike rename(2),
    # refuses an existing destination.  The temporary alias is removed only
    # after the final name exists; recovery quarantines it if a crash intervenes.
    os.link(source, destination)
    os.unlink(source)
    return "posix_same_volume_link_publish_directory_fsync"


def _sync_directory(path: Path) -> str:
    if os.name == "nt":
        # Python/Win32 cannot portably fsync a directory handle. Publication
        # above therefore uses the strongest supported primitive here:
        # same-volume MoveFileExW with MOVEFILE_WRITE_THROUGH.
        return "windows_movefileex_write_through_no_directory_fsync_claim"
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return "posix_directory_fsync"


def _atomic_write(
    destination: Path,
    sections: Sequence[tuple[str, bytes]],
    *,
    fault_hook: FaultHook | None = None,
    temp_label: str,
) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{temp_label}.{uuid4().hex}.tmp"
    _fault(fault_hook, "before_temp_create")
    descriptor = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        _fault(fault_hook, "after_temp_create")
        for stage, section in sections:
            _write_all(descriptor, section)
            _fault(fault_hook, stage)
        _fault(fault_hook, "before_file_fsync")
        os.fsync(descriptor)
        _fault(fault_hook, "after_file_fsync")
    finally:
        os.close(descriptor)
    _fault(fault_hook, "before_publish")
    method = _publish_noreplace(temporary, destination)
    _fault(fault_hook, "after_publish")
    directory_method = _sync_directory(destination.parent)
    _fault(fault_hook, "after_directory_sync")
    return f"{method};{directory_method}"


def _bounded_decompress(value: bytes, maximum: int) -> bytes:
    try:
        decompressor = zlib.decompressobj()
        output = decompressor.decompress(value, maximum + 1)
        if len(output) > maximum or decompressor.unconsumed_tail:
            raise LSCStoreLimitError("LSC_DECOMPRESSED_BODY_LIMIT_EXCEEDED")
        output += decompressor.flush(maximum + 1 - len(output))
    except zlib.error as exc:
        raise LSCStoreIntegrityError("LSC_COMPRESSED_BODY_INVALID") from exc
    if len(output) > maximum:
        raise LSCStoreLimitError("LSC_DECOMPRESSED_BODY_LIMIT_EXCEEDED")
    if not decompressor.eof or decompressor.unused_data or decompressor.unconsumed_tail:
        raise LSCStoreIntegrityError("LSC_COMPRESSED_BODY_TRAILING_OR_TRUNCATED")
    return output


class SegmentBuilder:
    def __init__(
        self,
        store: "SegmentStore",
        *,
        ordinal: int,
        previous_commitment: str,
        context_bytes: bytes,
        context_digest: str,
        previous_local_sequence: int | None,
    ) -> None:
        self._store = store
        self.ordinal = ordinal
        self.previous_commitment = previous_commitment
        self.context_bytes = context_bytes
        self.context_digest = context_digest
        self._opened_at = store._wall_clock()
        self._opened_monotonic = store._monotonic()
        self._records: list[EncodedRecordLike] = []
        self._framed_bytes = 0
        self._observation_ids: set[str] = set()
        self._sequence_keys: set[tuple[str, int]] = set()
        self._last_local_sequence = previous_local_sequence

    @property
    def record_count(self) -> int:
        return len(self._records)

    def append(self, record: EncodedRecordLike) -> None:
        if self._store._monotonic() - self._opened_monotonic > self._store.limits.max_segment_age_seconds:
            raise LSCStoreLimitError("LSC_SEGMENT_AGE_LIMIT_EXCEEDED")
        if len(self._records) >= self._store.limits.max_records:
            raise LSCStoreLimitError("LSC_SEGMENT_RECORD_LIMIT_EXCEEDED")
        if getattr(record, "observation_type", None) not in {"QUOTE", "TRADE", "DEPTH"}:
            raise LSCStoreError("LSC_NON_PASSIVE_RECORD_REFUSED")
        record_bytes = getattr(record, "record_bytes", None)
        if not isinstance(record_bytes, bytes) or not record_bytes:
            raise LSCStoreError("LSC_ENCODED_RECORD_BYTES_INVALID")
        if len(record_bytes) > self._store.limits.max_record_bytes:
            raise LSCStoreLimitError("LSC_ENCODED_RECORD_LIMIT_EXCEEDED")
        if getattr(record, "context_digest", None) != self.context_digest:
            raise LSCStoreError("LSC_SEGMENT_CONTEXT_MISMATCH")
        observation_id = getattr(record, "observation_id", None)
        if not isinstance(observation_id, str) or not observation_id:
            raise LSCStoreError("LSC_OBSERVATION_ID_INVALID")
        local_sequence = getattr(record, "local_sequence", None)
        if type(local_sequence) is not int or local_sequence < 0:
            raise LSCStoreError("LSC_LOCAL_SEQUENCE_INVALID")
        if self._last_local_sequence is not None and local_sequence <= self._last_local_sequence:
            raise LSCStoreError("LSC_NON_MONOTONIC_LOCAL_SEQUENCE")
        for field in ("payload_hash", "wire_hash", "event_digest"):
            _hex64(getattr(record, field, None), field.upper())
        try:
            verified = self._store._record_verifier(record_bytes, self.context_bytes)
        except LSCStoreError:
            raise
        except Exception as exc:
            raise LSCStoreIntegrityError("LSC_ENCODED_RECORD_VERIFICATION_FAILED") from exc
        supplied = {
            "observation_id": observation_id,
            "observation_type": record.observation_type,
            "local_sequence": local_sequence,
            "payload_hash": record.payload_hash,
            "wire_hash": record.wire_hash,
            "event_digest": record.event_digest,
            "context_digest": record.context_digest,
        }
        if any(getattr(verified, key) != value for key, value in supplied.items()):
            raise LSCStoreIntegrityError("LSC_ENCODED_RECORD_METADATA_MISMATCH")
        if observation_id in self._observation_ids:
            raise LSCStoreError("LSC_DUPLICATE_OBSERVATION_ID")
        sequence_key = (self.context_digest, local_sequence)
        if sequence_key in self._sequence_keys:
            raise LSCStoreError("LSC_DUPLICATE_CONTEXT_SEQUENCE")
        framed = _FRAME_LENGTH.size + len(record_bytes)
        if self._framed_bytes + framed > self._store.limits.max_raw_body_bytes:
            raise LSCStoreLimitError("LSC_RAW_BODY_LIMIT_EXCEEDED")
        self._records.append(record)
        self._framed_bytes += framed
        self._observation_ids.add(observation_id)
        self._sequence_keys.add(sequence_key)
        self._last_local_sequence = local_sequence

    def seal(self, *, fault_hook: FaultHook | None = None) -> SealedSegment:
        return self._store._seal(self, fault_hook=fault_hook)


class SegmentStore:
    """Immutable, bounded segment files plus deterministic recovery."""

    def __init__(
        self,
        root: str | Path,
        *,
        limits: SegmentLimits | None = None,
        context_digest_verifier: ContextDigestVerifier = _default_context_digest,
        record_verifier: RecordVerifier = _default_record_verifier,
        wall_clock: Callable[[], str] = _utc_now,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.root = Path(root)
        self.segment_root = self.root / "segments"
        self.quarantine_root = self.root / "quarantine"
        self.limits = limits or SegmentLimits()
        self._context_digest_verifier = context_digest_verifier
        self._record_verifier = record_verifier
        self._wall_clock = wall_clock
        self._monotonic = monotonic
        self.segment_root.mkdir(parents=True, exist_ok=True)
        self.quarantine_root.mkdir(parents=True, exist_ok=True)

    def new_segment(self, context_bytes: bytes, *, previous_commitment: str | None = None) -> SegmentBuilder:
        if not isinstance(context_bytes, bytes) or not context_bytes:
            raise LSCStoreError("LSC_CONTEXT_BYTES_INVALID")
        if len(context_bytes) > self.limits.max_context_bytes:
            raise LSCStoreLimitError("LSC_CONTEXT_LIMIT_EXCEEDED")
        context_digest = _hex64(self._context_digest_verifier(context_bytes), "CONTEXT_DIGEST")
        chain = self.verify_chain()
        expected_previous = GENESIS_COMMITMENT if not chain else chain[-1].segment_commitment
        if previous_commitment is not None and _hex64(previous_commitment, "PREVIOUS_COMMITMENT") != expected_previous:
            raise LSCStoreIntegrityError("LSC_PREVIOUS_COMMITMENT_MISMATCH")
        previous_local_sequence: int | None = None
        if chain and chain[-1].context_digest == context_digest:
            previous_parsed = self._read_path(chain[-1].path, recovered_without_receipt=False)
            previous_local_sequence = int(previous_parsed.index[-1]["local_sequence"])
        return SegmentBuilder(
            self,
            ordinal=len(chain),
            previous_commitment=expected_previous,
            context_bytes=context_bytes,
            context_digest=context_digest,
            previous_local_sequence=previous_local_sequence,
        )

    def _seal(self, builder: SegmentBuilder, *, fault_hook: FaultHook | None) -> SealedSegment:
        if builder._store is not self:
            raise LSCStoreError("LSC_BUILDER_STORE_MISMATCH")
        if not builder._records:
            raise LSCStoreError("LSC_EMPTY_SEGMENT_REFUSED")
        if self._monotonic() - builder._opened_monotonic > self.limits.max_segment_age_seconds:
            raise LSCStoreLimitError("LSC_SEGMENT_AGE_LIMIT_EXCEEDED")
        chain = self.verify_chain()
        expected_previous = GENESIS_COMMITMENT if not chain else chain[-1].segment_commitment
        if builder.ordinal != len(chain) or builder.previous_commitment != expected_previous:
            raise LSCStoreIntegrityError("LSC_STALE_SEGMENT_BUILDER")

        raw = bytearray()
        index: list[dict[str, object]] = []
        for event_index, record in enumerate(builder._records):
            record_bytes = record.record_bytes
            frame_offset = len(raw)
            raw.extend(_FRAME_LENGTH.pack(len(record_bytes)))
            raw.extend(record_bytes)
            index.append({
                "context_digest": builder.context_digest,
                "event_digest": record.event_digest,
                "event_index": event_index,
                "frame_length": len(record_bytes),
                "frame_offset": frame_offset,
                "local_sequence": record.local_sequence,
                "observation_id": record.observation_id,
                "observation_type": record.observation_type,
                "payload_hash": record.payload_hash,
                "wire_hash": record.wire_hash,
            })
        raw_bytes = bytes(raw)
        if len(raw_bytes) > self.limits.max_raw_body_bytes:
            raise LSCStoreLimitError("LSC_RAW_BODY_LIMIT_EXCEEDED")
        compressed = zlib.compress(raw_bytes, level=6)
        if len(compressed) > self.limits.max_compressed_body_bytes:
            raise LSCStoreLimitError("LSC_COMPRESSED_BODY_LIMIT_EXCEEDED")

        sealed_at = self._wall_clock()
        header = {
            "compression": COMPRESSION,
            "context_digest": builder.context_digest,
            "created_at": builder._opened_at,
            "first_local_sequence": builder._records[0].local_sequence,
            "last_local_sequence": builder._records[-1].local_sequence,
            "ordinal": builder.ordinal,
            "previous_commitment": builder.previous_commitment,
            "record_count": len(index),
            "schema": SEGMENT_SCHEMA,
            "sealed_at": sealed_at,
        }
        header_bytes = _canonical_json(header)
        index_bytes = _canonical_json(index)
        footer_core = {
            "compressed_body_bytes": len(compressed),
            "compressed_body_sha256": _sha256(compressed),
            "context_bytes": len(builder.context_bytes),
            "context_sha256": _sha256(builder.context_bytes),
            "header_sha256": _sha256(header_bytes),
            "index_sha256": _sha256(index_bytes),
            "merkle_root": _merkle_root([str(item["event_digest"]) for item in index]),
            "raw_body_bytes": len(raw_bytes),
            "raw_body_sha256": _sha256(raw_bytes),
        }
        footer_core_bytes = _canonical_json(footer_core)
        content_digest = _domain_hash(
            _CONTENT_DOMAIN,
            header_bytes,
            builder.context_bytes,
            compressed,
            index_bytes,
            footer_core_bytes,
        )
        segment_commitment = _domain_hash(
            _COMMITMENT_DOMAIN,
            bytes.fromhex(builder.previous_commitment),
            bytes.fromhex(content_digest),
            bytes.fromhex(str(footer_core["merkle_root"])),
            struct.pack("<Q", len(index)),
        )
        footer = {
            **footer_core,
            "content_digest": content_digest,
            "segment_commitment": segment_commitment,
            "segment_id": segment_commitment,
        }
        footer_bytes = _canonical_json(footer)
        for label, value in (("HEADER", header_bytes), ("INDEX", index_bytes), ("FOOTER", footer_bytes)):
            if len(value) > MAX_METADATA_BYTES:
                raise LSCStoreLimitError(f"LSC_{label}_LIMIT_EXCEEDED")
        prefix = _PREFIX.pack(
            CONTAINER_MAGIC,
            CONTAINER_VERSION,
            0,
            len(header_bytes),
            len(builder.context_bytes),
            len(compressed),
            len(index_bytes),
            len(footer_bytes),
        )
        destination = self.segment_root / f"{builder.ordinal:020d}-{segment_commitment}.lsc"
        method = _atomic_write(
            destination,
            (
                ("after_prefix_write", prefix),
                ("after_header_write", header_bytes),
                ("after_context_write", builder.context_bytes),
                ("after_body_write", compressed),
                ("after_index_write", index_bytes),
                ("after_footer_write", footer_bytes),
            ),
            fault_hook=fault_hook,
            temp_label="lsc-segment",
        )
        parsed = self._read_path(destination, recovered_without_receipt=False)
        return SealedSegment(**{**parsed.sealed.__dict__, "durability_method": method})

    def _read_path(self, path: Path, *, recovered_without_receipt: bool) -> _ParsedSegment:
        match = _SEGMENT_NAME.fullmatch(path.name)
        if match is None or not path.is_file() or path.is_symlink():
            raise LSCStoreIntegrityError("LSC_SEGMENT_PATH_INVALID")
        maximum_file = (
            _PREFIX.size + 3 * MAX_METADATA_BYTES + self.limits.max_context_bytes
            + self.limits.max_compressed_body_bytes
        )
        size = path.stat().st_size
        if size <= _PREFIX.size or size > maximum_file:
            raise LSCStoreLimitError("LSC_SEGMENT_FILE_SIZE_INVALID")
        data = path.read_bytes()
        if len(data) != size:
            raise LSCStoreIntegrityError("LSC_SEGMENT_FILE_CHANGED_DURING_READ")
        try:
            magic, version, flags, header_len, context_len, body_len, index_len, footer_len = _PREFIX.unpack_from(data)
        except struct.error as exc:
            raise LSCStoreIntegrityError("LSC_SEGMENT_PREFIX_INVALID") from exc
        if magic != CONTAINER_MAGIC or version != CONTAINER_VERSION or flags != 0:
            raise LSCStoreIntegrityError("LSC_SEGMENT_VERSION_OR_FLAGS_UNSUPPORTED")
        if (
            header_len <= 0 or header_len > MAX_METADATA_BYTES
            or context_len <= 0 or context_len > self.limits.max_context_bytes
            or body_len <= 0 or body_len > self.limits.max_compressed_body_bytes
            or index_len <= 0 or index_len > MAX_METADATA_BYTES
            or footer_len <= 0 or footer_len > MAX_METADATA_BYTES
        ):
            raise LSCStoreLimitError("LSC_SEGMENT_DECLARED_LENGTH_INVALID")
        expected = _PREFIX.size + header_len + context_len + body_len + index_len + footer_len
        if expected != len(data):
            raise LSCStoreIntegrityError("LSC_SEGMENT_LENGTH_MISMATCH")
        cursor = _PREFIX.size
        header_bytes = data[cursor:cursor + header_len]; cursor += header_len
        context_bytes = data[cursor:cursor + context_len]; cursor += context_len
        compressed = data[cursor:cursor + body_len]; cursor += body_len
        index_bytes = data[cursor:cursor + index_len]; cursor += index_len
        footer_bytes = data[cursor:cursor + footer_len]
        header = _strict_json(header_bytes, "LSC_HEADER")
        index = _strict_json(index_bytes, "LSC_INDEX")
        footer = _strict_json(footer_bytes, "LSC_FOOTER")
        if not isinstance(header, dict) or not isinstance(index, list) or not isinstance(footer, dict):
            raise LSCStoreIntegrityError("LSC_SEGMENT_METADATA_SHAPE_INVALID")
        required_header = {
            "compression", "context_digest", "created_at", "first_local_sequence",
            "last_local_sequence", "ordinal", "previous_commitment", "record_count", "schema",
            "sealed_at",
        }
        required_footer = {
            "compressed_body_bytes", "compressed_body_sha256", "content_digest", "context_bytes",
            "context_sha256", "header_sha256", "index_sha256", "merkle_root", "raw_body_bytes",
            "raw_body_sha256", "segment_commitment", "segment_id",
        }
        if set(header) != required_header or set(footer) != required_footer:
            raise LSCStoreIntegrityError("LSC_SEGMENT_METADATA_FIELDS_INVALID")
        if header["schema"] != SEGMENT_SCHEMA or header["compression"] != COMPRESSION:
            raise LSCStoreIntegrityError("LSC_SEGMENT_SCHEMA_OR_COMPRESSION_UNSUPPORTED")
        ordinal = header["ordinal"]
        record_count = header["record_count"]
        if type(ordinal) is not int or ordinal < 0 or type(record_count) is not int or not 0 < record_count <= self.limits.max_records:
            raise LSCStoreIntegrityError("LSC_SEGMENT_COUNT_OR_ORDINAL_INVALID")
        if (
            type(header["first_local_sequence"]) is not int
            or header["first_local_sequence"] < 0
            or type(header["last_local_sequence"]) is not int
            or header["last_local_sequence"] < header["first_local_sequence"]
        ):
            raise LSCStoreIntegrityError("LSC_HEADER_SEQUENCE_BOUNDS_INVALID")
        if ordinal != int(match.group("ordinal")):
            raise LSCStoreIntegrityError("LSC_SEGMENT_FILENAME_ORDINAL_MISMATCH")
        previous = _hex64(header["previous_commitment"], "PREVIOUS_COMMITMENT")
        context_digest = _hex64(header["context_digest"], "CONTEXT_DIGEST")
        if self._context_digest_verifier(context_bytes) != context_digest:
            raise LSCStoreIntegrityError("LSC_CONTEXT_DIGEST_MISMATCH")
        raw = _bounded_decompress(compressed, self.limits.max_raw_body_bytes)
        if not isinstance(index, list) or len(index) != record_count:
            raise LSCStoreIntegrityError("LSC_INDEX_RECORD_COUNT_MISMATCH")
        expected_footer_values = {
            "compressed_body_bytes": len(compressed),
            "compressed_body_sha256": _sha256(compressed),
            "context_bytes": len(context_bytes),
            "context_sha256": _sha256(context_bytes),
            "header_sha256": _sha256(header_bytes),
            "index_sha256": _sha256(index_bytes),
            "raw_body_bytes": len(raw),
            "raw_body_sha256": _sha256(raw),
        }
        if any(footer.get(key) != value for key, value in expected_footer_values.items()):
            raise LSCStoreIntegrityError("LSC_SEGMENT_COMPONENT_DIGEST_MISMATCH")

        event_digests: list[str] = []
        expected_offset = 0
        seen_ids: set[str] = set()
        seen_sequences: set[int] = set()
        prior_sequence: int | None = None
        normalized_index: list[Mapping[str, object]] = []
        fields = {
            "context_digest", "event_digest", "event_index", "frame_length", "frame_offset",
            "local_sequence", "observation_id", "observation_type", "payload_hash", "wire_hash",
        }
        for event_index, entry in enumerate(index):
            if not isinstance(entry, dict) or set(entry) != fields:
                raise LSCStoreIntegrityError("LSC_INDEX_ENTRY_SHAPE_INVALID")
            if entry["event_index"] != event_index or entry["frame_offset"] != expected_offset:
                raise LSCStoreIntegrityError("LSC_INDEX_ORDER_OR_OFFSET_INVALID")
            length = entry["frame_length"]
            sequence = entry["local_sequence"]
            observation_id = entry["observation_id"]
            observation_type = entry["observation_type"]
            if type(length) is not int or not 0 < length <= self.limits.max_record_bytes:
                raise LSCStoreIntegrityError("LSC_INDEX_FRAME_LENGTH_INVALID")
            if type(sequence) is not int or sequence < 0 or not isinstance(observation_id, str) or not observation_id:
                raise LSCStoreIntegrityError("LSC_INDEX_IDENTITY_INVALID")
            if prior_sequence is not None and sequence <= prior_sequence:
                raise LSCStoreIntegrityError("LSC_INDEX_SEQUENCE_NOT_STRICTLY_INCREASING")
            if observation_type not in {"QUOTE", "TRADE", "DEPTH"} or entry["context_digest"] != context_digest:
                raise LSCStoreIntegrityError("LSC_INDEX_TYPE_OR_CONTEXT_INVALID")
            if observation_id in seen_ids or sequence in seen_sequences:
                raise LSCStoreIntegrityError("LSC_INDEX_DUPLICATE_IDENTITY")
            for field in ("event_digest", "payload_hash", "wire_hash"):
                _hex64(entry[field], field.upper())
            if expected_offset + _FRAME_LENGTH.size + length > len(raw):
                raise LSCStoreIntegrityError("LSC_INDEX_FRAME_OUT_OF_RANGE")
            declared = _FRAME_LENGTH.unpack_from(raw, expected_offset)[0]
            if declared != length:
                raise LSCStoreIntegrityError("LSC_FRAME_LENGTH_MISMATCH")
            record_bytes = raw[
                expected_offset + _FRAME_LENGTH.size:
                expected_offset + _FRAME_LENGTH.size + length
            ]
            try:
                verified = self._record_verifier(record_bytes, context_bytes)
            except LSCStoreError:
                raise
            except Exception as exc:
                raise LSCStoreIntegrityError("LSC_ENCODED_RECORD_VERIFICATION_FAILED") from exc
            if any(
                getattr(verified, key) != entry[key]
                for key in (
                    "observation_id", "observation_type", "local_sequence", "payload_hash",
                    "wire_hash", "event_digest", "context_digest",
                )
            ):
                raise LSCStoreIntegrityError("LSC_INDEX_RECORD_METADATA_MISMATCH")
            expected_offset += _FRAME_LENGTH.size + length
            seen_ids.add(observation_id)
            seen_sequences.add(sequence)
            prior_sequence = sequence
            event_digests.append(str(entry["event_digest"]))
            normalized_index.append(entry)
        if expected_offset != len(raw):
            raise LSCStoreIntegrityError("LSC_RAW_BODY_UNINDEXED_BYTES")
        if (
            header["first_local_sequence"] != normalized_index[0]["local_sequence"]
            or header["last_local_sequence"] != normalized_index[-1]["local_sequence"]
        ):
            raise LSCStoreIntegrityError("LSC_HEADER_SEQUENCE_BOUNDS_MISMATCH")
        merkle = _merkle_root(event_digests)
        if footer["merkle_root"] != merkle:
            raise LSCStoreIntegrityError("LSC_MERKLE_ROOT_MISMATCH")
        footer_core = {key: value for key, value in footer.items() if key not in {"content_digest", "segment_commitment", "segment_id"}}
        content_digest = _domain_hash(
            _CONTENT_DOMAIN, header_bytes, context_bytes, compressed, index_bytes, _canonical_json(footer_core)
        )
        if footer["content_digest"] != content_digest:
            raise LSCStoreIntegrityError("LSC_CONTENT_DIGEST_MISMATCH")
        commitment = _domain_hash(
            _COMMITMENT_DOMAIN,
            bytes.fromhex(previous),
            bytes.fromhex(content_digest),
            bytes.fromhex(merkle),
            struct.pack("<Q", record_count),
        )
        if footer["segment_commitment"] != commitment or footer["segment_id"] != commitment:
            raise LSCStoreIntegrityError("LSC_SEGMENT_COMMITMENT_MISMATCH")
        if match.group("segment") != commitment:
            raise LSCStoreIntegrityError("LSC_SEGMENT_FILENAME_COMMITMENT_MISMATCH")
        sealed = SealedSegment(
            segment_id=commitment,
            ordinal=ordinal,
            previous_commitment=previous,
            segment_commitment=commitment,
            content_digest=content_digest,
            merkle_root=merkle,
            context_digest=context_digest,
            record_count=record_count,
            first_local_sequence=int(header["first_local_sequence"]),
            last_local_sequence=int(header["last_local_sequence"]),
            raw_body_bytes=len(raw),
            compressed_body_bytes=len(compressed),
            path=path,
            durability_method="verified_existing_file",
            recovered_without_receipt=recovered_without_receipt,
        )
        return _ParsedSegment(sealed, context_bytes, raw, tuple(normalized_index))

    def verify_chain(self) -> tuple[SealedSegment, ...]:
        paths = sorted(self.segment_root.glob("*.lsc"), key=lambda item: item.name)
        result: list[SealedSegment] = []
        previous = GENESIS_COMMITMENT
        previous_context: str | None = None
        previous_sequence: int | None = None
        for ordinal, path in enumerate(paths):
            parsed = self._read_path(path, recovered_without_receipt=False)
            if parsed.sealed.ordinal != ordinal or parsed.sealed.previous_commitment != previous:
                raise LSCStoreIntegrityError("LSC_SEGMENT_CHAIN_GAP_FORK_OR_LINK_MISMATCH")
            first_sequence = int(parsed.index[0]["local_sequence"])
            if (
                previous_context == parsed.sealed.context_digest
                and previous_sequence is not None
                and first_sequence <= previous_sequence
            ):
                raise LSCStoreIntegrityError("LSC_SEGMENT_CHAIN_SEQUENCE_NOT_STRICTLY_INCREASING")
            result.append(parsed.sealed)
            previous = parsed.sealed.segment_commitment
            previous_context = parsed.sealed.context_digest
            previous_sequence = int(parsed.index[-1]["local_sequence"])
        return tuple(result)

    def read_segment(self, segment_id: str) -> SealedSegment:
        segment_id = _hex64(segment_id, "SEGMENT_ID")
        paths = list(self.segment_root.glob(f"*-{segment_id}.lsc"))
        if len(paths) != 1:
            raise LSCStoreIntegrityError("LSC_SEGMENT_NOT_UNIQUELY_RESOLVED")
        return self._read_path(paths[0], recovered_without_receipt=False).sealed

    def read_context(self, segment_id: str) -> bytes:
        """Return verified encoded context bytes for one exact sealed segment."""

        segment_id = _hex64(segment_id, "SEGMENT_ID")
        paths = list(self.segment_root.glob(f"*-{segment_id}.lsc"))
        if len(paths) != 1:
            raise LSCStoreIntegrityError("LSC_SEGMENT_NOT_UNIQUELY_RESOLVED")
        return self._read_path(paths[0], recovered_without_receipt=False).context_bytes

    def lookup(self, segment_id: str, event_index: int, local_sequence: int) -> StoredEvent:
        if type(event_index) is not int or event_index < 0 or type(local_sequence) is not int or local_sequence < 0:
            raise LSCStoreError("LSC_LOOKUP_COORDINATE_INVALID")
        segment_id = _hex64(segment_id, "SEGMENT_ID")
        paths = list(self.segment_root.glob(f"*-{segment_id}.lsc"))
        if len(paths) != 1:
            raise LSCStoreIntegrityError("LSC_SEGMENT_NOT_UNIQUELY_RESOLVED")
        parsed = self._read_path(paths[0], recovered_without_receipt=False)
        if event_index >= len(parsed.index):
            raise LSCStoreError("LSC_EVENT_INDEX_OUT_OF_RANGE")
        entry = parsed.index[event_index]
        if entry["local_sequence"] != local_sequence:
            raise LSCStoreIntegrityError("LSC_LOOKUP_LOCAL_SEQUENCE_MISMATCH")
        offset = int(entry["frame_offset"]) + _FRAME_LENGTH.size
        length = int(entry["frame_length"])
        record_bytes = parsed.raw_body[offset:offset + length]
        return StoredEvent(
            segment_id=segment_id,
            event_index=event_index,
            observation_id=str(entry["observation_id"]),
            observation_type=str(entry["observation_type"]),
            local_sequence=local_sequence,
            payload_hash=str(entry["payload_hash"]),
            wire_hash=str(entry["wire_hash"]),
            event_digest=str(entry["event_digest"]),
            context_digest=str(entry["context_digest"]),
            record_bytes=record_bytes,
        )

    def _quarantine(self, path: Path, reason: str) -> QuarantinedTemporary:
        size = path.stat().st_size
        digest: str | None = None
        if size <= (
            _PREFIX.size + 3 * MAX_METADATA_BYTES + self.limits.max_context_bytes
            + self.limits.max_compressed_body_bytes
        ):
            digest = _sha256(path.read_bytes())
        destination = self.quarantine_root / f"{path.name}.{digest or 'oversize'}.{uuid4().hex}.quarantine"
        _publish_noreplace(path, destination)
        _sync_directory(path.parent)
        if path.parent != destination.parent:
            _sync_directory(destination.parent)
        return QuarantinedTemporary(path.name, destination, size, digest, reason)

    def recover(self) -> RecoveryReport:
        quarantined: list[QuarantinedTemporary] = []
        for path in sorted(self.segment_root.glob("*.tmp"), key=lambda item: item.name):
            if path.is_symlink() or not path.is_file():
                raise LSCStoreRecoveryError("LSC_TEMPORARY_PATH_INVALID")
            quarantined.append(self._quarantine(path, "INCOMPLETE_TEMPORARY_NEVER_SEALED"))
        sealed = self.verify_chain()
        sealed_ids = tuple(item.segment_id for item in sealed)
        return RecoveryReport(
            quarantined=tuple(quarantined),
            sealed_segment_ids=sealed_ids,
        )


__all__ = [
    "GENESIS_COMMITMENT",
    "LSCStoreError",
    "LSCStoreIntegrityError",
    "LSCStoreLimitError",
    "LSCStoreRecoveryError",
    "QuarantinedTemporary",
    "RecoveryReport",
    "SealedSegment",
    "SegmentBuilder",
    "SegmentLimits",
    "SegmentStore",
    "StoredEvent",
]
