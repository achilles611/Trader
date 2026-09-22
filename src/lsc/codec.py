"""Deterministic Lossless Semantic Compaction (LSC) v1 record codec.

This module is deliberately standalone and offline.  It neither owns a socket
nor writes a segment or authority ledger.  A caller must supply the frozen
runtime identities in :class:`LSCContext`; records are accepted only when they
belong to that exact context.
"""

from __future__ import annotations

from calendar import timegm
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import re
import struct
from types import MappingProxyType
from typing import Literal, Mapping

from src.l3f_provider.ninjatrader_observation import (
    AccountClass,
    NinjaTraderObservation,
)


FORMAT_MAGIC = b"LSCR"
CONTEXT_MAGIC = b"LSCC"
FORMAT_VERSION = 1
MAX_ENCODED_CONTEXT_BYTES = 65_536
MAX_CANONICAL_PAYLOAD_BYTES = 1_048_576
MAX_ENCODED_RECORD_BYTES = 2_097_152
MAX_TEXT_BYTES = 1_048_576
MAX_CONTAINER_ITEMS = 65_536
MAX_NESTING_DEPTH = 32
MAX_INTEGER_BITS = 4_096
MAX_SEQUENCE = (1 << 63) - 1

_CONTEXT_DOMAIN = b"BEELZEBUB_LSC_V1_CONTEXT\0"
_EVENT_DOMAIN = b"BEELZEBUB_LSC_V1_EVENT\0"
_PASSIVE_TYPES = frozenset({"QUOTE", "TRADE", "DEPTH"})
_KIND_CODES = {"QUOTE": 1, "TRADE": 2, "DEPTH": 3}
_KINDS_BY_CODE = {value: key for key, value in _KIND_CODES.items()}
_CANONICAL_UTC = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?Z$"
)

# This tuple is part of the v1 wire format.  Reordering or inserting entries is
# a format change.  Dynamic runtime identities live in the context dictionary.
_STATIC_STRINGS = (
    "contract_id", "bid", "ask", "bid_size", "ask_size",
    "bid_source_time", "ask_source_time", "publication_policy",
    "price", "size", "aggressor_side", "aggressor_source",
    "bid_at_trade", "ask_at_trade", "derivation_quote_observation_id",
    "bids", "asks", "operation", "side", "mutation_price",
    "mutation_volume", "mutation_position", "is_reset",
    "UNKNOWN", "BID_ASK_CLASSIFICATION", "PROVIDER_NATIVE", "BUY", "SELL",
    "Add", "Update", "Remove", "Bid", "Ask", "BOUNDED_LATEST_STATE_2HZ",
)
_STATIC_INDEX = {value: index for index, value in enumerate(_STATIC_STRINGS)}

_VALUE_NULL = 0
_VALUE_FALSE = 1
_VALUE_TRUE = 2
_VALUE_INTEGER = 3
_VALUE_FLOAT64 = 4
_VALUE_STRING = 5
_VALUE_LIST = 6
_VALUE_MAP = 7
_VALUE_PRICE_TICKS = 8
_VALUE_PRICE_ESCAPE = 9

_STRING_RAW = 0
_STRING_STATIC = 1
_STRING_CONTEXT = 2

_TIMESTAMP_NONE = 0
_TIMESTAMP_DELTA_NS = 1
_TIMESTAMP_RAW = 2


class LSCCodecError(ValueError):
    """The input cannot be represented or verified as canonical LSC v1."""


def _nonempty_text(value: object, name: str, *, maximum: int = 4_096) -> str:
    if not isinstance(value, str) or not value:
        raise LSCCodecError(f"{name} must be non-empty text")
    if len(value.encode("utf-8", "surrogatepass")) > maximum:
        raise LSCCodecError(f"{name} exceeds its byte limit")
    return value


def _u64(value: object, name: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_SEQUENCE:
        raise LSCCodecError(f"{name} must be an integer from 0 through {MAX_SEQUENCE}")
    return value


def _tick_text(value: object) -> str:
    text = _nonempty_text(value, "tick_size", maximum=128)
    try:
        tick = Decimal(text)
    except InvalidOperation as exc:
        raise LSCCodecError("tick_size must be a finite positive decimal") from exc
    if not tick.is_finite() or tick <= 0:
        raise LSCCodecError("tick_size must be a finite positive decimal")
    return text


@dataclass(frozen=True)
class LSCContext:
    """Identity-homogeneous context for one or more LSC records.

    A segment must not mix contexts. ``timestamp_origin`` is an exact canonical
    UTC string with up to nanosecond precision; compact timestamps are encoded
    as integer nanosecond deltas from it.
    """

    source: str
    provider: str
    environment: str
    source_identity: str
    build_identity: str
    contract_identity: str
    contract_id: str
    tick_size: str
    session_id: str
    generation: int
    timestamp_origin: str

    def __post_init__(self) -> None:
        for name in (
            "source", "provider", "environment", "source_identity",
            "build_identity", "contract_identity", "contract_id", "session_id",
        ):
            _nonempty_text(getattr(self, name), name)
        _tick_text(self.tick_size)
        _u64(self.generation, "generation")
        if _parse_canonical_timestamp(self.timestamp_origin) is None:
            raise LSCCodecError(
                "timestamp_origin must be canonical UTC text with no more than "
                "nine fractional digits"
            )

    @property
    def dictionary(self) -> tuple[str, ...]:
        return (
            self.source,
            self.provider,
            self.environment,
            self.source_identity,
            self.build_identity,
            self.contract_identity,
            self.contract_id,
            self.tick_size,
            self.session_id,
        )


@dataclass(frozen=True)
class EncodedRecord:
    record_bytes: bytes
    observation_type: Literal["QUOTE", "TRADE", "DEPTH"]
    observation_id: str
    local_sequence: int
    payload_hash: str
    wire_hash: str
    event_digest: str
    context_digest: str


def _encode_uvarint(value: int) -> bytes:
    if type(value) is not int or value < 0:
        raise LSCCodecError("unsigned integer must be non-negative")
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def _zigzag(value: int) -> int:
    return value * 2 if value >= 0 else (-value * 2) - 1


def _unzigzag(value: int) -> int:
    return value // 2 if value % 2 == 0 else -(value // 2) - 1


def _encode_svarint(value: int) -> bytes:
    return _encode_uvarint(_zigzag(value))


def _text_bytes(value: str) -> bytes:
    raw = value.encode("utf-8", "surrogatepass")
    if len(raw) > MAX_TEXT_BYTES:
        raise LSCCodecError("text exceeds the LSC v1 byte limit")
    return _encode_uvarint(len(raw)) + raw


class _Cursor:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def take(self, count: int) -> bytes:
        if type(count) is not int or count < 0 or count > len(self.data) - self.offset:
            raise LSCCodecError("truncated LSC v1 input")
        result = self.data[self.offset:self.offset + count]
        self.offset += count
        return result

    def byte(self) -> int:
        return self.take(1)[0]

    def uvarint(self, *, maximum_bytes: int = 512) -> int:
        result = 0
        shift = 0
        for index in range(maximum_bytes):
            byte = self.byte()
            result |= (byte & 0x7F) << shift
            if byte < 0x80:
                if index and byte == 0:
                    raise LSCCodecError("non-canonical unsigned integer")
                return result
            shift += 7
        raise LSCCodecError("unsigned integer exceeds its encoded bound")

    def svarint(self, *, maximum_bytes: int = 512) -> int:
        return _unzigzag(self.uvarint(maximum_bytes=maximum_bytes))

    def text(self, *, maximum: int = MAX_TEXT_BYTES) -> str:
        length = self.uvarint(maximum_bytes=10)
        if length > maximum:
            raise LSCCodecError("text exceeds its decoded byte limit")
        try:
            return self.take(length).decode("utf-8", "surrogatepass")
        except UnicodeDecodeError as exc:
            raise LSCCodecError("invalid UTF-8 text") from exc

    def finish(self) -> None:
        if self.offset != len(self.data):
            raise LSCCodecError("trailing bytes after canonical LSC v1 value")


def _parse_canonical_timestamp(value: str) -> tuple[int, int] | None:
    match = _CANONICAL_UTC.fullmatch(value)
    if match is None:
        return None
    fraction = match.group("fraction") or ""
    try:
        second = datetime(
            int(match.group("year")), int(match.group("month")), int(match.group("day")),
            int(match.group("hour")), int(match.group("minute")), int(match.group("second")),
            tzinfo=timezone.utc,
        )
    except ValueError:
        return None
    nanoseconds = timegm(second.utctimetuple()) * 1_000_000_000
    nanoseconds += int(fraction.ljust(9, "0")) if fraction else 0
    return nanoseconds, len(fraction)


def _format_canonical_timestamp(nanoseconds: int, precision: int) -> str:
    if not 0 <= precision <= 9:
        raise LSCCodecError("invalid timestamp precision")
    seconds, fraction = divmod(nanoseconds, 1_000_000_000)
    try:
        value = datetime.fromtimestamp(seconds, timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise LSCCodecError("timestamp falls outside the supported UTC range") from exc
    if precision == 0:
        if fraction != 0:
            raise LSCCodecError("timestamp precision cannot represent its nanoseconds")
        suffix = ""
    else:
        if fraction % (10 ** (9 - precision)):
            raise LSCCodecError("timestamp precision cannot represent its nanoseconds")
        suffix = "." + f"{fraction:09d}"[:precision]
    return value.strftime("%Y-%m-%dT%H:%M:%S") + suffix + "Z"


def _encode_context_content(context: LSCContext) -> bytes:
    body = bytearray(CONTEXT_MAGIC)
    body.append(FORMAT_VERSION)
    for value in context.dictionary:
        body.extend(_text_bytes(value))
    body.extend(_encode_uvarint(context.generation))
    body.extend(_text_bytes(context.timestamp_origin))
    return bytes(body)


def encode_context(context: LSCContext) -> bytes:
    if not isinstance(context, LSCContext):
        raise LSCCodecError("context must be LSCContext")
    content = _encode_context_content(context)
    digest = hashlib.sha256(_CONTEXT_DOMAIN + content).digest()
    encoded = content + digest
    if len(encoded) > MAX_ENCODED_CONTEXT_BYTES:
        raise LSCCodecError("encoded context exceeds the LSC v1 limit")
    return encoded


def _decode_context_verified(data: bytes) -> tuple[LSCContext, str]:
    if not isinstance(data, bytes) or not data or len(data) > MAX_ENCODED_CONTEXT_BYTES:
        raise LSCCodecError("encoded context is absent or exceeds the LSC v1 limit")
    if len(data) < len(CONTEXT_MAGIC) + 1 + 32:
        raise LSCCodecError("truncated LSC v1 context")
    content, supplied_digest = data[:-32], data[-32:]
    expected_digest = hashlib.sha256(_CONTEXT_DOMAIN + content).digest()
    if not hmac.compare_digest(supplied_digest, expected_digest):
        raise LSCCodecError("context digest mismatch")
    cursor = _Cursor(content)
    if cursor.take(4) != CONTEXT_MAGIC:
        raise LSCCodecError("unknown LSC context magic")
    if cursor.byte() != FORMAT_VERSION:
        raise LSCCodecError("unknown LSC context version")
    values = [cursor.text(maximum=4_096) for _ in range(9)]
    generation = cursor.uvarint(maximum_bytes=10)
    timestamp_origin = cursor.text(maximum=128)
    cursor.finish()
    context = LSCContext(
        source=values[0], provider=values[1], environment=values[2],
        source_identity=values[3], build_identity=values[4],
        contract_identity=values[5], contract_id=values[6], tick_size=values[7],
        session_id=values[8], generation=generation,
        timestamp_origin=timestamp_origin,
    )
    if encode_context(context) != data:
        raise LSCCodecError("context is not canonically encoded")
    return context, supplied_digest.hex()


def decode_context(data: bytes) -> LSCContext:
    return _decode_context_verified(data)[0]


def context_digest(data: bytes) -> str:
    """Return the embedded digest only after full bounded context verification."""

    return _decode_context_verified(data)[1]


def _coerce_context(value: LSCContext | bytes) -> tuple[LSCContext, bytes, str]:
    encoded = encode_context(value) if isinstance(value, LSCContext) else value
    if not isinstance(encoded, bytes):
        raise LSCCodecError("context must be LSCContext or encoded bytes")
    context, digest = _decode_context_verified(encoded)
    return context, encoded, digest


def _encode_string_token(value: str, context: LSCContext) -> bytes:
    static = _STATIC_INDEX.get(value)
    if static is not None:
        return bytes((_STRING_STATIC,)) + _encode_uvarint(static)
    try:
        dynamic = context.dictionary.index(value)
    except ValueError:
        return bytes((_STRING_RAW,)) + _text_bytes(value)
    return bytes((_STRING_CONTEXT,)) + _encode_uvarint(dynamic)


def _decode_string_token(cursor: _Cursor, context: LSCContext) -> str:
    kind = cursor.byte()
    if kind == _STRING_RAW:
        return cursor.text()
    if kind == _STRING_STATIC:
        index = cursor.uvarint(maximum_bytes=10)
        if index >= len(_STATIC_STRINGS):
            raise LSCCodecError("static dictionary index is out of range")
        return _STATIC_STRINGS[index]
    if kind == _STRING_CONTEXT:
        index = cursor.uvarint(maximum_bytes=10)
        if index >= len(context.dictionary):
            raise LSCCodecError("context dictionary index is out of range")
        return context.dictionary[index]
    raise LSCCodecError("unknown string token")


def _plain_json(value: object, *, depth: int = 0) -> object:
    if depth > MAX_NESTING_DEPTH:
        raise LSCCodecError("payload nesting exceeds the LSC v1 limit")
    if value is None or type(value) in {bool, str, float}:
        if isinstance(value, str) and len(value.encode("utf-8", "surrogatepass")) > MAX_TEXT_BYTES:
            raise LSCCodecError("payload string exceeds the LSC v1 limit")
        return value
    if type(value) is int:
        if value.bit_length() > MAX_INTEGER_BITS:
            raise LSCCodecError("payload integer exceeds the LSC v1 limit")
        return value
    if isinstance(value, list):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise LSCCodecError("payload list exceeds the LSC v1 item limit")
        return [_plain_json(item, depth=depth + 1) for item in value]
    if isinstance(value, Mapping):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise LSCCodecError("payload object exceeds the LSC v1 item limit")
        result: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise LSCCodecError("payload object keys must be strings")
            result[key] = _plain_json(item, depth=depth + 1)
        return result
    raise LSCCodecError(f"payload contains unsupported type {type(value).__name__}")


def _canonical_payload(payload: Mapping[str, object]) -> tuple[dict[str, object], bytes, str]:
    plain = _plain_json(payload)
    if not isinstance(plain, dict):
        raise LSCCodecError("observation payload must be an object")
    try:
        encoded = json.dumps(
            plain, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise LSCCodecError("payload is not canonical JSON") from exc
    if len(encoded) > MAX_CANONICAL_PAYLOAD_BYTES:
        raise LSCCodecError("canonical payload exceeds the LSC v1 limit")
    return plain, encoded, hashlib.sha256(encoded).hexdigest()


def _is_price_path(kind: str, path: tuple[object, ...]) -> bool:
    if kind == "QUOTE":
        return path in {("bid",), ("ask",)}
    if kind == "TRADE":
        return path in {("price",), ("bid_at_trade",), ("ask_at_trade",)}
    if kind == "DEPTH":
        return path == ("mutation_price",) or (
            len(path) == 3
            and path[0] in {"bids", "asks"}
            and type(path[1]) is int
            and path[2] == "price"
        )
    return False


def _exact_ticks(value: object, tick_size: str) -> int | None:
    if type(value) not in {int, float, str}:
        return None
    try:
        number = Decimal(str(value))
        tick = Decimal(tick_size)
    except (InvalidOperation, ValueError):
        return None
    if not number.is_finite() or number <= 0:
        return None
    quotient = number / tick
    integral = quotient.to_integral_value()
    if quotient != integral or int(integral).bit_length() > MAX_INTEGER_BITS:
        return None
    return int(integral)


def _encode_value(
    value: object,
    context: LSCContext,
    kind: str,
    path: tuple[object, ...],
    *,
    depth: int = 0,
    price_wrapper: bool = True,
) -> bytes:
    if depth > MAX_NESTING_DEPTH:
        raise LSCCodecError("payload nesting exceeds the LSC v1 limit")
    if price_wrapper and _is_price_path(kind, path):
        ticks = _exact_ticks(value, context.tick_size)
        if ticks is not None:
            return bytes((_VALUE_PRICE_TICKS,)) + _encode_svarint(ticks) + _encode_value(
                value, context, kind, path, depth=depth + 1, price_wrapper=False,
            )
        return bytes((_VALUE_PRICE_ESCAPE,)) + _encode_value(
            value, context, kind, path, depth=depth + 1, price_wrapper=False,
        )
    if value is None:
        return bytes((_VALUE_NULL,))
    if value is False:
        return bytes((_VALUE_FALSE,))
    if value is True:
        return bytes((_VALUE_TRUE,))
    if type(value) is int:
        if value.bit_length() > MAX_INTEGER_BITS:
            raise LSCCodecError("payload integer exceeds the LSC v1 limit")
        return bytes((_VALUE_INTEGER,)) + _encode_svarint(value)
    if type(value) is float:
        return bytes((_VALUE_FLOAT64,)) + struct.pack("<d", value)
    if isinstance(value, str):
        return bytes((_VALUE_STRING,)) + _encode_string_token(value, context)
    if isinstance(value, list):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise LSCCodecError("payload list exceeds the LSC v1 item limit")
        body = bytearray((_VALUE_LIST,))
        body.extend(_encode_uvarint(len(value)))
        for index, item in enumerate(value):
            body.extend(_encode_value(item, context, kind, path + (index,), depth=depth + 1))
        return bytes(body)
    if isinstance(value, Mapping):
        if len(value) > MAX_CONTAINER_ITEMS:
            raise LSCCodecError("payload object exceeds the LSC v1 item limit")
        keys = sorted(value, key=lambda item: item.encode("utf-8", "surrogatepass"))
        body = bytearray((_VALUE_MAP,))
        body.extend(_encode_uvarint(len(keys)))
        for key in keys:
            if not isinstance(key, str):
                raise LSCCodecError("payload object keys must be strings")
            body.extend(_encode_string_token(key, context))
            body.extend(_encode_value(value[key], context, kind, path + (key,), depth=depth + 1))
        return bytes(body)
    raise LSCCodecError(f"payload contains unsupported type {type(value).__name__}")


def _decode_value(
    cursor: _Cursor,
    context: LSCContext,
    kind: str,
    path: tuple[object, ...],
    *,
    depth: int = 0,
) -> object:
    if depth > MAX_NESTING_DEPTH:
        raise LSCCodecError("decoded payload nesting exceeds the LSC v1 limit")
    tag = cursor.byte()
    if tag == _VALUE_NULL:
        return None
    if tag == _VALUE_FALSE:
        return False
    if tag == _VALUE_TRUE:
        return True
    if tag == _VALUE_INTEGER:
        result = cursor.svarint()
        if result.bit_length() > MAX_INTEGER_BITS:
            raise LSCCodecError("decoded integer exceeds the LSC v1 limit")
        return result
    if tag == _VALUE_FLOAT64:
        return struct.unpack("<d", cursor.take(8))[0]
    if tag == _VALUE_STRING:
        return _decode_string_token(cursor, context)
    if tag == _VALUE_LIST:
        count = cursor.uvarint(maximum_bytes=10)
        if count > MAX_CONTAINER_ITEMS:
            raise LSCCodecError("decoded list exceeds the LSC v1 item limit")
        return [
            _decode_value(cursor, context, kind, path + (index,), depth=depth + 1)
            for index in range(count)
        ]
    if tag == _VALUE_MAP:
        count = cursor.uvarint(maximum_bytes=10)
        if count > MAX_CONTAINER_ITEMS:
            raise LSCCodecError("decoded object exceeds the LSC v1 item limit")
        result: dict[str, object] = {}
        previous: bytes | None = None
        for _ in range(count):
            key = _decode_string_token(cursor, context)
            encoded_key = key.encode("utf-8", "surrogatepass")
            if previous is not None and encoded_key <= previous:
                raise LSCCodecError("decoded object keys are not strictly canonical")
            previous = encoded_key
            result[key] = _decode_value(cursor, context, kind, path + (key,), depth=depth + 1)
        return result
    if tag == _VALUE_PRICE_TICKS:
        if not _is_price_path(kind, path):
            raise LSCCodecError("tick price tag appears outside a price field")
        ticks = cursor.svarint()
        result = _decode_value(cursor, context, kind, path, depth=depth + 1)
        if _exact_ticks(result, context.tick_size) != ticks:
            raise LSCCodecError("price tick projection does not match the exact scalar")
        return result
    if tag == _VALUE_PRICE_ESCAPE:
        if not _is_price_path(kind, path):
            raise LSCCodecError("price escape appears outside a price field")
        result = _decode_value(cursor, context, kind, path, depth=depth + 1)
        if _exact_ticks(result, context.tick_size) is not None:
            raise LSCCodecError("canonical tick-aligned price used the escape path")
        return result
    raise LSCCodecError("unknown typed payload tag")


def _encode_timestamp(value: str | None, context: LSCContext) -> bytes:
    if value is None:
        return bytes((_TIMESTAMP_NONE,))
    _nonempty_text(value, "timestamp", maximum=128)
    parsed = _parse_canonical_timestamp(value)
    origin = _parse_canonical_timestamp(context.timestamp_origin)
    assert origin is not None
    if parsed is None:
        return bytes((_TIMESTAMP_RAW,)) + _text_bytes(value)
    nanoseconds, precision = parsed
    return (
        bytes((_TIMESTAMP_DELTA_NS,))
        + _encode_svarint(nanoseconds - origin[0])
        + bytes((precision,))
    )


def _decode_timestamp(cursor: _Cursor, context: LSCContext) -> str | None:
    tag = cursor.byte()
    if tag == _TIMESTAMP_NONE:
        return None
    if tag == _TIMESTAMP_RAW:
        return cursor.text(maximum=128)
    if tag == _TIMESTAMP_DELTA_NS:
        delta = cursor.svarint(maximum_bytes=32)
        precision = cursor.byte()
        origin = _parse_canonical_timestamp(context.timestamp_origin)
        assert origin is not None
        return _format_canonical_timestamp(origin[0] + delta, precision)
    raise LSCCodecError("unknown timestamp encoding")


def _encode_optional_text(value: str | None, context: LSCContext) -> bytes:
    if value is None:
        return b"\x00"
    return b"\x01" + _encode_string_token(value, context)


def _decode_optional_text(cursor: _Cursor, context: LSCContext) -> str | None:
    marker = cursor.byte()
    if marker == 0:
        return None
    if marker == 1:
        return _decode_string_token(cursor, context)
    raise LSCCodecError("invalid optional text marker")


def _encode_optional_sequence(value: int | None) -> bytes:
    if value is None:
        return b"\x00"
    return b"\x01" + _encode_uvarint(_u64(value, "provider_sequence"))


def _decode_optional_sequence(cursor: _Cursor) -> int | None:
    marker = cursor.byte()
    if marker == 0:
        return None
    if marker == 1:
        return _u64(cursor.uvarint(maximum_bytes=10), "provider_sequence")
    raise LSCCodecError("invalid optional sequence marker")


def _encode_observation(
    observation: NinjaTraderObservation,
    context: LSCContext,
    context_digest_hex: str,
) -> EncodedRecord:
    if not isinstance(observation, NinjaTraderObservation):
        raise LSCCodecError("observation must be NinjaTraderObservation")
    kind = observation.observation_type
    if kind not in _PASSIVE_TYPES:
        raise LSCCodecError("LSC v1 admits only QUOTE, TRADE, and DEPTH")
    if observation.session_id != context.session_id:
        raise LSCCodecError("observation session does not match its LSC context")
    if (
        observation.source != context.source
        or observation.provider != context.provider
        or observation.environment != context.environment
    ):
        raise LSCCodecError("observation source provenance does not match its LSC context")
    sequence = _u64(observation.local_monotonic_sequence, "local_sequence")
    payload, canonical_payload, payload_hash = _canonical_payload(observation.payload)
    if payload.get("contract_id") != context.contract_id:
        raise LSCCodecError("observation contract does not match its LSC context")
    payload_bytes = _encode_value(payload, context, kind, ())

    body = bytearray(FORMAT_MAGIC)
    body.extend((FORMAT_VERSION, 0, _KIND_CODES[kind]))
    body.extend(bytes.fromhex(context_digest_hex))
    body.extend(_encode_uvarint(sequence))
    derived_id = f"nt-{context.session_id}-{sequence}"
    if observation.observation_id == derived_id:
        body.append(0)
    else:
        body.append(1)
        body.extend(_encode_string_token(observation.observation_id, context))
    body.extend(_encode_timestamp(observation.ninja_receipt_time, context))
    body.extend(_encode_timestamp(observation.provider_timestamp, context))
    body.extend(_encode_optional_sequence(observation.provider_sequence))
    body.extend(_encode_timestamp(observation.exchange_timestamp, context))
    body.extend(_encode_optional_text(observation.account_alias, context))
    body.extend(_encode_optional_text(
        None if observation.account_class is None else observation.account_class.value,
        context,
    ))
    body.extend(_encode_uvarint(len(payload_bytes)))
    body.extend(payload_bytes)
    body.extend(bytes.fromhex(payload_hash))
    wire_hash = observation.wire_hash()
    body.extend(bytes.fromhex(wire_hash))
    event_digest_bytes = hashlib.sha256(_EVENT_DOMAIN + bytes(body)).digest()
    record_bytes = bytes(body) + event_digest_bytes
    if len(record_bytes) > MAX_ENCODED_RECORD_BYTES:
        raise LSCCodecError("encoded record exceeds the LSC v1 limit")
    # The canonical bytes are intentionally computed before binary encoding;
    # keep this assertion adjacent to the emitted hash to prevent drift.
    if hashlib.sha256(canonical_payload).hexdigest() != payload_hash:
        raise AssertionError("canonical payload hash changed during encoding")
    return EncodedRecord(
        record_bytes=record_bytes,
        observation_type=kind,  # type: ignore[arg-type]
        observation_id=observation.observation_id,
        local_sequence=sequence,
        payload_hash=payload_hash,
        wire_hash=wire_hash,
        event_digest=event_digest_bytes.hex(),
        context_digest=context_digest_hex,
    )


def encode_observation(
    observation: NinjaTraderObservation,
    context: LSCContext | bytes,
) -> EncodedRecord:
    frozen_context, _, digest = _coerce_context(context)
    return _encode_observation(observation, frozen_context, digest)


def decode_observation(
    record_bytes: bytes,
    context: LSCContext | bytes,
) -> NinjaTraderObservation:
    frozen_context, _, context_digest_hex = _coerce_context(context)
    if (
        not isinstance(record_bytes, bytes)
        or not record_bytes
        or len(record_bytes) > MAX_ENCODED_RECORD_BYTES
    ):
        raise LSCCodecError("encoded record is absent or exceeds the LSC v1 limit")
    if len(record_bytes) < 4 + 3 + 32 + 32:
        raise LSCCodecError("truncated LSC v1 record")
    content, supplied_event_digest = record_bytes[:-32], record_bytes[-32:]
    expected_event_digest = hashlib.sha256(_EVENT_DOMAIN + content).digest()
    if not hmac.compare_digest(supplied_event_digest, expected_event_digest):
        raise LSCCodecError("event digest mismatch")
    cursor = _Cursor(content)
    if cursor.take(4) != FORMAT_MAGIC:
        raise LSCCodecError("unknown LSC record magic")
    if cursor.byte() != FORMAT_VERSION:
        raise LSCCodecError("unknown LSC record version")
    if cursor.byte() != 0:
        raise LSCCodecError("unknown LSC record flags")
    kind_code = cursor.byte()
    kind = _KINDS_BY_CODE.get(kind_code)
    if kind is None:
        raise LSCCodecError("unknown passive observation type")
    if cursor.take(32).hex() != context_digest_hex:
        raise LSCCodecError("record context digest does not match supplied context")
    sequence = _u64(cursor.uvarint(maximum_bytes=10), "local_sequence")
    observation_id_mode = cursor.byte()
    if observation_id_mode == 0:
        observation_id = f"nt-{frozen_context.session_id}-{sequence}"
    elif observation_id_mode == 1:
        observation_id = _decode_string_token(cursor, frozen_context)
    else:
        raise LSCCodecError("unknown observation ID encoding")
    ninja_receipt_time = _decode_timestamp(cursor, frozen_context)
    if ninja_receipt_time is None:
        raise LSCCodecError("NinjaTrader receipt timestamp cannot be null")
    provider_timestamp = _decode_timestamp(cursor, frozen_context)
    provider_sequence = _decode_optional_sequence(cursor)
    exchange_timestamp = _decode_timestamp(cursor, frozen_context)
    account_alias = _decode_optional_text(cursor, frozen_context)
    account_class_text = _decode_optional_text(cursor, frozen_context)
    if (account_alias is None) != (account_class_text is None):
        raise LSCCodecError("account alias and class must be jointly present")
    payload_length = cursor.uvarint(maximum_bytes=10)
    if payload_length > MAX_ENCODED_RECORD_BYTES:
        raise LSCCodecError("encoded payload exceeds the LSC v1 limit")
    payload_cursor = _Cursor(cursor.take(payload_length))
    payload = _decode_value(payload_cursor, frozen_context, kind, ())
    payload_cursor.finish()
    if not isinstance(payload, dict):
        raise LSCCodecError("decoded observation payload is not an object")
    supplied_payload_hash = cursor.take(32).hex()
    supplied_wire_hash = cursor.take(32).hex()
    cursor.finish()

    _, canonical_payload, decoded_payload_hash = _canonical_payload(payload)
    if decoded_payload_hash != supplied_payload_hash:
        raise LSCCodecError("canonical payload hash mismatch")
    if hashlib.sha256(canonical_payload).hexdigest() != supplied_payload_hash:
        raise LSCCodecError("canonical payload bytes do not match their hash")
    try:
        account_class = None if account_class_text is None else AccountClass(account_class_text)
        observation = NinjaTraderObservation(
            observation_id=observation_id,
            session_id=frozen_context.session_id,
            observation_type=kind,
            ninja_receipt_time=ninja_receipt_time,
            local_monotonic_sequence=sequence,
            payload=MappingProxyType(payload),
            account_alias=account_alias,
            account_class=account_class,
            provider_timestamp=provider_timestamp,
            provider_sequence=provider_sequence,
            exchange_timestamp=exchange_timestamp,
            source=frozen_context.source,
            provider=frozen_context.provider,
            environment=frozen_context.environment,
        )
    except (ValueError, TypeError) as exc:
        raise LSCCodecError("decoded observation violates the admitted schema") from exc
    if observation.wire_hash() != supplied_wire_hash:
        raise LSCCodecError("observation wire hash mismatch")
    # Canonical re-encoding refuses alternate varints, dictionary choices,
    # price escapes, timestamp encodings, and observation-ID spellings.
    if _encode_observation(observation, frozen_context, context_digest_hex).record_bytes != record_bytes:
        raise LSCCodecError("record is not the canonical LSC v1 encoding")
    return observation


__all__ = [
    "CONTEXT_MAGIC",
    "EncodedRecord",
    "FORMAT_MAGIC",
    "FORMAT_VERSION",
    "LSCCodecError",
    "LSCContext",
    "MAX_CANONICAL_PAYLOAD_BYTES",
    "MAX_CONTAINER_ITEMS",
    "MAX_ENCODED_CONTEXT_BYTES",
    "MAX_ENCODED_RECORD_BYTES",
    "MAX_NESTING_DEPTH",
    "context_digest",
    "decode_context",
    "decode_observation",
    "encode_context",
    "encode_observation",
]
