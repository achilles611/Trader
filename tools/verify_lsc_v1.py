#!/usr/bin/env python3
"""Independent LSC v1 verifier and legacy-versus-LSC parity reporter.

This module intentionally does not import the production codec or decoder.
It implements the published byte contract a second time so correlated decoder
errors cannot turn a production round trip into independent proof.
"""

from __future__ import annotations

import argparse
from calendar import timegm
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
from pathlib import Path
import re
import struct
from typing import Mapping, Sequence


FORMAT_VERSION = 1
MAX_CONTEXT_BYTES = 65_536
MAX_PAYLOAD_BYTES = 1_048_576
MAX_RECORD_BYTES = 2_097_152
MAX_TEXT_BYTES = 1_048_576
MAX_ITEMS = 65_536
MAX_DEPTH = 32
MAX_INTEGER_BITS = 4_096
MAX_SEQUENCE = (1 << 63) - 1
CONTEXT_DOMAIN = b"BEELZEBUB_LSC_V1_CONTEXT\0"
EVENT_DOMAIN = b"BEELZEBUB_LSC_V1_EVENT\0"

STATIC_STRINGS = (
    "contract_id", "bid", "ask", "bid_size", "ask_size",
    "bid_source_time", "ask_source_time", "publication_policy",
    "price", "size", "aggressor_side", "aggressor_source",
    "bid_at_trade", "ask_at_trade", "derivation_quote_observation_id",
    "bids", "asks", "operation", "side", "mutation_price",
    "mutation_volume", "mutation_position", "is_reset",
    "UNKNOWN", "BID_ASK_CLASSIFICATION", "PROVIDER_NATIVE", "BUY", "SELL",
    "Add", "Update", "Remove", "Bid", "Ask", "BOUNDED_LATEST_STATE_2HZ",
)
STATIC_INDEX = {value: index for index, value in enumerate(STATIC_STRINGS)}
KIND_BY_CODE = {1: "QUOTE", 2: "TRADE", 3: "DEPTH"}
CANONICAL_UTC = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})T"
    r"(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?Z$"
)
PROOF_FIELDS = (
    "evidence_families", "hypotheses", "decisions", "reason_codes",
    "final_replay_state", "final_replay_hash",
)


class VerificationError(ValueError):
    pass


@dataclass(frozen=True)
class IndependentContext:
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
    digest: str

    @property
    def dictionary(self) -> tuple[str, ...]:
        return (
            self.source, self.provider, self.environment, self.source_identity,
            self.build_identity, self.contract_identity, self.contract_id,
            self.tick_size, self.session_id,
        )


@dataclass(frozen=True)
class VerifiedObservation:
    observation_id: str
    session_id: str
    observation_type: str
    ninja_receipt_time: str
    local_sequence: int
    payload: Mapping[str, object]
    account_alias: str | None
    account_class: str | None
    provider_timestamp: str | None
    provider_sequence: int | None
    exchange_timestamp: str | None
    payload_hash: str
    wire_hash: str
    event_digest: str
    context: IndependentContext

    def parity_view(self) -> dict[str, object]:
        checkpoint = None
        if self.observation_type == "DEPTH":
            checkpoint_value = {
                "contract_id": self.payload.get("contract_id"),
                "bids": self.payload.get("bids"),
                "asks": self.payload.get("asks"),
            }
            checkpoint = hashlib.sha256(_canonical_json(checkpoint_value)).hexdigest()
        return {
            "observation_id": self.observation_id,
            "session_id": self.session_id,
            "observation_type": self.observation_type,
            "local_monotonic_sequence": self.local_sequence,
            "ninja_receipt_time": self.ninja_receipt_time,
            "provider_timestamp": self.provider_timestamp,
            "provider_sequence": self.provider_sequence,
            "exchange_timestamp": self.exchange_timestamp,
            "account_alias": self.account_alias,
            "account_class": self.account_class,
            "payload": dict(self.payload),
            "payload_hash": self.payload_hash,
            "wire_hash": self.wire_hash,
            "source": self.context.source,
            "provider": self.context.provider,
            "environment": self.context.environment,
            "source_identity": self.context.source_identity,
            "build_identity": self.context.build_identity,
            "contract_identity": self.context.contract_identity,
            "contract_id": self.context.contract_id,
            "generation": self.context.generation,
            "book_checkpoint": checkpoint,
        }


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _uvarint(value: int) -> bytes:
    if type(value) is not int or value < 0:
        raise VerificationError("unsigned integer is invalid")
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def _svarint(value: int) -> bytes:
    return _uvarint(value * 2 if value >= 0 else (-value * 2) - 1)


def _text(value: str) -> bytes:
    raw = value.encode("utf-8", "surrogatepass")
    if len(raw) > MAX_TEXT_BYTES:
        raise VerificationError("text exceeds bound")
    return _uvarint(len(raw)) + raw


class Cursor:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def take(self, count: int) -> bytes:
        if type(count) is not int or count < 0 or count > len(self.data) - self.offset:
            raise VerificationError("truncated input")
        value = self.data[self.offset:self.offset + count]
        self.offset += count
        return value

    def byte(self) -> int:
        return self.take(1)[0]

    def uvarint(self, maximum_bytes: int = 512) -> int:
        start = self.offset
        value = 0
        shift = 0
        for _ in range(maximum_bytes):
            item = self.byte()
            value |= (item & 127) << shift
            if item < 128:
                if self.data[start:self.offset] != _uvarint(value):
                    raise VerificationError("non-canonical unsigned integer")
                return value
            shift += 7
        raise VerificationError("unsigned integer exceeds bound")

    def svarint(self, maximum_bytes: int = 512) -> int:
        raw = self.uvarint(maximum_bytes)
        return raw // 2 if raw % 2 == 0 else -(raw // 2) - 1

    def text(self, maximum: int = MAX_TEXT_BYTES) -> str:
        length = self.uvarint(10)
        if length > maximum:
            raise VerificationError("text exceeds decoded bound")
        try:
            return self.take(length).decode("utf-8", "surrogatepass")
        except UnicodeDecodeError as exc:
            raise VerificationError("invalid UTF-8") from exc

    def finish(self) -> None:
        if self.offset != len(self.data):
            raise VerificationError("trailing bytes")


def _parse_timestamp(value: str) -> tuple[int, int] | None:
    match = CANONICAL_UTC.fullmatch(value)
    if match is None:
        return None
    fraction = match.group("fraction") or ""
    try:
        base = datetime(
            int(match.group("year")), int(match.group("month")), int(match.group("day")),
            int(match.group("hour")), int(match.group("minute")), int(match.group("second")),
            tzinfo=timezone.utc,
        )
    except ValueError:
        return None
    nanoseconds = timegm(base.utctimetuple()) * 1_000_000_000
    nanoseconds += int(fraction.ljust(9, "0")) if fraction else 0
    return nanoseconds, len(fraction)


def _format_timestamp(nanoseconds: int, precision: int) -> str:
    if not 0 <= precision <= 9:
        raise VerificationError("timestamp precision invalid")
    seconds, fraction = divmod(nanoseconds, 1_000_000_000)
    try:
        base = datetime.fromtimestamp(seconds, timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise VerificationError("timestamp out of range") from exc
    if precision == 0:
        if fraction:
            raise VerificationError("timestamp precision loses data")
        suffix = ""
    else:
        if fraction % (10 ** (9 - precision)):
            raise VerificationError("timestamp precision loses data")
        suffix = "." + f"{fraction:09d}"[:precision]
    return base.strftime("%Y-%m-%dT%H:%M:%S") + suffix + "Z"


def _validate_iso(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise VerificationError("raw timestamp is not ISO-8601") from exc
    if parsed.tzinfo is None:
        raise VerificationError("raw timestamp lacks an explicit UTC offset")


def _encode_context_content(context: IndependentContext) -> bytes:
    result = bytearray(b"LSCC\x01")
    for value in context.dictionary:
        result.extend(_text(value))
    result.extend(_uvarint(context.generation))
    result.extend(_text(context.timestamp_origin))
    return bytes(result)


def verify_context(data: bytes) -> IndependentContext:
    if not isinstance(data, bytes) or not data or len(data) > MAX_CONTEXT_BYTES or len(data) < 37:
        raise VerificationError("context size invalid")
    content, supplied = data[:-32], data[-32:]
    expected = hashlib.sha256(CONTEXT_DOMAIN + content).digest()
    if not hmac.compare_digest(supplied, expected):
        raise VerificationError("context digest mismatch")
    cursor = Cursor(content)
    if cursor.take(4) != b"LSCC" or cursor.byte() != FORMAT_VERSION:
        raise VerificationError("context magic or version unsupported")
    values = [cursor.text(4_096) for _ in range(9)]
    if any(not value for value in values):
        raise VerificationError("empty context identity")
    generation = cursor.uvarint(10)
    if generation > MAX_SEQUENCE:
        raise VerificationError("generation exceeds bound")
    origin = cursor.text(128)
    cursor.finish()
    if _parse_timestamp(origin) is None:
        raise VerificationError("context timestamp origin is not canonical UTC")
    try:
        tick = Decimal(values[7])
    except InvalidOperation as exc:
        raise VerificationError("tick size invalid") from exc
    if not tick.is_finite() or tick <= 0:
        raise VerificationError("tick size invalid")
    context = IndependentContext(*values, generation, origin, supplied.hex())
    if _encode_context_content(context) + supplied != data:
        raise VerificationError("context encoding is not canonical")
    return context


def _canonical_string_token(value: str, context: IndependentContext) -> bytes:
    if value in STATIC_INDEX:
        return b"\x01" + _uvarint(STATIC_INDEX[value])
    if value in context.dictionary:
        return b"\x02" + _uvarint(context.dictionary.index(value))
    return b"\x00" + _text(value)


def _string_token(cursor: Cursor, context: IndependentContext) -> str:
    start = cursor.offset
    tag = cursor.byte()
    if tag == 0:
        value = cursor.text()
    elif tag == 1:
        index = cursor.uvarint(10)
        if index >= len(STATIC_STRINGS):
            raise VerificationError("static dictionary index out of range")
        value = STATIC_STRINGS[index]
    elif tag == 2:
        index = cursor.uvarint(10)
        if index >= len(context.dictionary):
            raise VerificationError("context dictionary index out of range")
        value = context.dictionary[index]
    else:
        raise VerificationError("unknown string token")
    if cursor.data[start:cursor.offset] != _canonical_string_token(value, context):
        raise VerificationError("non-canonical string token")
    return value


def _price_path(kind: str, path: tuple[object, ...]) -> bool:
    if kind == "QUOTE":
        return path in {("bid",), ("ask",)}
    if kind == "TRADE":
        return path in {("price",), ("bid_at_trade",), ("ask_at_trade",)}
    return kind == "DEPTH" and (
        path == ("mutation_price",)
        or len(path) == 3 and path[0] in {"bids", "asks"}
        and type(path[1]) is int and path[2] == "price"
    )


def _ticks(value: object, tick_text: str) -> int | None:
    if type(value) not in {int, float, str}:
        return None
    try:
        number = Decimal(str(value))
        tick = Decimal(tick_text)
    except (InvalidOperation, ValueError):
        return None
    if not number.is_finite() or number <= 0:
        return None
    quotient = number / tick
    integral = quotient.to_integral_value()
    if quotient != integral or int(integral).bit_length() > MAX_INTEGER_BITS:
        return None
    return int(integral)


def _value(
    cursor: Cursor,
    context: IndependentContext,
    kind: str,
    path: tuple[object, ...],
    depth: int = 0,
    *,
    price_wrapper: bool = True,
) -> object:
    if depth > MAX_DEPTH:
        raise VerificationError("payload nesting exceeds bound")
    tag = cursor.byte()
    if price_wrapper and _price_path(kind, path) and tag not in {8, 9}:
        raise VerificationError("price path lacks its canonical tick or escape wrapper")
    if tag == 0:
        return None
    if tag == 1:
        return False
    if tag == 2:
        return True
    if tag == 3:
        value = cursor.svarint()
        if value.bit_length() > MAX_INTEGER_BITS:
            raise VerificationError("integer exceeds bound")
        return value
    if tag == 4:
        return struct.unpack("<d", cursor.take(8))[0]
    if tag == 5:
        return _string_token(cursor, context)
    if tag == 6:
        count = cursor.uvarint(10)
        if count > MAX_ITEMS:
            raise VerificationError("list exceeds bound")
        return [_value(cursor, context, kind, path + (index,), depth + 1) for index in range(count)]
    if tag == 7:
        count = cursor.uvarint(10)
        if count > MAX_ITEMS:
            raise VerificationError("object exceeds bound")
        result: dict[str, object] = {}
        prior: bytes | None = None
        for _ in range(count):
            key = _string_token(cursor, context)
            raw = key.encode("utf-8", "surrogatepass")
            if prior is not None and raw <= prior:
                raise VerificationError("object keys not canonical")
            prior = raw
            result[key] = _value(cursor, context, kind, path + (key,), depth + 1)
        return result
    if tag == 8:
        if not _price_path(kind, path):
            raise VerificationError("tick tag outside price path")
        projection = cursor.svarint()
        result = _value(cursor, context, kind, path, depth + 1, price_wrapper=False)
        if _ticks(result, context.tick_size) != projection:
            raise VerificationError("price projection mismatch")
        return result
    if tag == 9:
        if not _price_path(kind, path):
            raise VerificationError("price escape outside price path")
        result = _value(cursor, context, kind, path, depth + 1, price_wrapper=False)
        if _ticks(result, context.tick_size) is not None:
            raise VerificationError("aligned price used non-canonical escape")
        return result
    raise VerificationError("unknown payload tag")


def _timestamp(cursor: Cursor, context: IndependentContext) -> str | None:
    tag = cursor.byte()
    if tag == 0:
        return None
    if tag == 1:
        delta = cursor.svarint(32)
        precision = cursor.byte()
        origin = _parse_timestamp(context.timestamp_origin)
        assert origin is not None
        return _format_timestamp(origin[0] + delta, precision)
    if tag == 2:
        value = cursor.text(128)
        _validate_iso(value)
        if _parse_timestamp(value) is not None:
            raise VerificationError("canonical timestamp used raw escape")
        return value
    raise VerificationError("unknown timestamp tag")


def _optional_text(cursor: Cursor, context: IndependentContext) -> str | None:
    marker = cursor.byte()
    if marker == 0:
        return None
    if marker == 1:
        return _string_token(cursor, context)
    raise VerificationError("invalid optional text marker")


def _optional_sequence(cursor: Cursor) -> int | None:
    marker = cursor.byte()
    if marker == 0:
        return None
    if marker == 1:
        value = cursor.uvarint(10)
        if value > MAX_SEQUENCE:
            raise VerificationError("provider sequence exceeds bound")
        return value
    raise VerificationError("invalid optional sequence marker")


def verify_record(record: bytes, context_bytes: bytes) -> VerifiedObservation:
    context = verify_context(context_bytes)
    if not isinstance(record, bytes) or not record or len(record) > MAX_RECORD_BYTES or len(record) < 71:
        raise VerificationError("record size invalid")
    content, supplied_event = record[:-32], record[-32:]
    expected_event = hashlib.sha256(EVENT_DOMAIN + content).digest()
    if not hmac.compare_digest(supplied_event, expected_event):
        raise VerificationError("event digest mismatch")
    cursor = Cursor(content)
    if cursor.take(4) != b"LSCR" or cursor.byte() != FORMAT_VERSION or cursor.byte() != 0:
        raise VerificationError("record magic, version, or flags unsupported")
    kind = KIND_BY_CODE.get(cursor.byte())
    if kind is None:
        raise VerificationError("record type unsupported")
    if cursor.take(32).hex() != context.digest:
        raise VerificationError("record context mismatch")
    sequence = cursor.uvarint(10)
    if sequence > MAX_SEQUENCE:
        raise VerificationError("local sequence exceeds bound")
    id_mode = cursor.byte()
    derived = f"nt-{context.session_id}-{sequence}"
    if id_mode == 0:
        observation_id = derived
    elif id_mode == 1:
        observation_id = _string_token(cursor, context)
        if observation_id == derived:
            raise VerificationError("derived observation ID used raw form")
    else:
        raise VerificationError("observation ID mode unsupported")
    ninja_time = _timestamp(cursor, context)
    if ninja_time is None:
        raise VerificationError("receipt timestamp absent")
    provider_time = _timestamp(cursor, context)
    provider_sequence = _optional_sequence(cursor)
    exchange_time = _timestamp(cursor, context)
    account_alias = _optional_text(cursor, context)
    account_class = _optional_text(cursor, context)
    if (account_alias is None) != (account_class is None):
        raise VerificationError("partial account provenance")
    if account_class not in {None, "PROVIDER_EVALUATION", "LOCAL_SIMULATION"}:
        raise VerificationError("account class unsupported")
    payload_length = cursor.uvarint(10)
    if payload_length > MAX_RECORD_BYTES:
        raise VerificationError("encoded payload exceeds bound")
    payload_cursor = Cursor(cursor.take(payload_length))
    payload = _value(payload_cursor, context, kind, ())
    payload_cursor.finish()
    if not isinstance(payload, dict) or payload.get("contract_id") != context.contract_id:
        raise VerificationError("payload contract or shape mismatch")
    payload_hash = cursor.take(32).hex()
    wire_hash = cursor.take(32).hex()
    cursor.finish()
    canonical_payload = _canonical_json(payload)
    if len(canonical_payload) > MAX_PAYLOAD_BYTES:
        raise VerificationError("canonical payload exceeds bound")
    if hashlib.sha256(canonical_payload).hexdigest() != payload_hash:
        raise VerificationError("payload hash mismatch")
    wire_material = {
        "schema": "lane-iii-phase-f2-ninjatrader-observation-v1",
        "observation_id": observation_id,
        "session_id": context.session_id,
        "observation_type": kind,
        "ninja_receipt_time": ninja_time,
        "local_monotonic_sequence": sequence,
        "payload": payload,
        "account_alias": account_alias,
        "account_class": account_class,
        "provider_timestamp": provider_time,
        "provider_sequence": provider_sequence,
        "exchange_timestamp": exchange_time,
    }
    if hashlib.sha256(_canonical_json(wire_material)).hexdigest() != wire_hash:
        raise VerificationError("wire hash mismatch")
    return VerifiedObservation(
        observation_id, context.session_id, kind, ninja_time, sequence, payload,
        account_alias, account_class, provider_time, provider_sequence, exchange_time,
        payload_hash, wire_hash, supplied_event.hex(), context,
    )


def verify_golden_vectors(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    context_bytes = bytes.fromhex(value["context_hex"])
    context = verify_context(context_bytes)
    if context.digest != value["context_digest"]:
        raise VerificationError("golden context digest mismatch")
    records: dict[str, object] = {}
    for name in ("quote", "trade", "depth"):
        expected = value["records"][name]
        verified = verify_record(bytes.fromhex(expected["record_hex"]), context_bytes)
        if (
            verified.observation_type.lower() != name
            or verified.payload_hash != expected["payload_hash"]
            or verified.wire_hash != expected["wire_hash"]
            or verified.event_digest != expected["event_digest"]
        ):
            raise VerificationError(f"golden {name} metadata mismatch")
        records[name] = verified.parity_view()
    return {
        "schema": "beelzebub-lsc-independent-verification-v1",
        "result": "PASS",
        "context_digest": context.digest,
        "record_count": len(records),
        "records": records,
    }


def _strict_json(data: str) -> object:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise VerificationError("duplicate JSON key")
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=hook)


def _load_document(path: Path) -> tuple[list[Mapping[str, object]], Mapping[str, object]]:
    text = path.read_text(encoding="utf-8")
    try:
        value = _strict_json(text)
    except json.JSONDecodeError:
        rows = [_strict_json(line) for line in text.splitlines() if line.strip()]
        if not all(isinstance(row, dict) for row in rows):
            raise VerificationError("JSONL corpus rows must be objects")
        return list(rows), {}
    if isinstance(value, list):
        if not all(isinstance(row, dict) for row in value):
            raise VerificationError("corpus events must be objects")
        return list(value), {}
    if isinstance(value, dict) and isinstance(value.get("events"), list):
        rows = value["events"]
        if not all(isinstance(row, dict) for row in rows):
            raise VerificationError("corpus events must be objects")
        metadata = {key: value[key] for key in PROOF_FIELDS if key in value}
        return list(rows), metadata
    if isinstance(value, dict):
        return [value], {}
    raise VerificationError("corpus must be JSON object, array, or JSONL")


def _legacy_view(row: Mapping[str, object]) -> dict[str, object]:
    account = row.get("account")
    if account is not None and not isinstance(account, Mapping):
        raise VerificationError("legacy account must be object or null")
    payload = row.get("payload")
    if not isinstance(payload, Mapping):
        raise VerificationError("legacy payload must be object")
    payload_plain = dict(payload)
    payload_hash = hashlib.sha256(_canonical_json(payload_plain)).hexdigest()
    supplied_payload_hash = row.get("payload_hash", payload_hash)
    if supplied_payload_hash != payload_hash:
        raise VerificationError("legacy payload hash mismatch")
    required = (
        "observation_id", "session_id", "observation_type", "ninja_receipt_time",
        "local_monotonic_sequence", "source", "provider", "environment",
        "source_identity", "build_identity", "contract_identity", "contract_id",
        "generation",
    )
    missing = [key for key in required if key not in row]
    if missing:
        raise VerificationError("legacy parity identity missing: " + ",".join(missing))
    account_alias = None if account is None else account.get("alias")
    account_class = None if account is None else account.get("class")
    wire_material = {
        "schema": "lane-iii-phase-f2-ninjatrader-observation-v1",
        "observation_id": row["observation_id"],
        "session_id": row["session_id"],
        "observation_type": row["observation_type"],
        "ninja_receipt_time": row["ninja_receipt_time"],
        "local_monotonic_sequence": row["local_monotonic_sequence"],
        "payload": payload_plain,
        "account_alias": account_alias,
        "account_class": account_class,
        "provider_timestamp": row.get("provider_timestamp"),
        "provider_sequence": row.get("provider_sequence"),
        "exchange_timestamp": row.get("exchange_timestamp"),
    }
    wire_hash = hashlib.sha256(_canonical_json(wire_material)).hexdigest()
    supplied_wire_hash = row.get("wire_hash", wire_hash)
    if supplied_wire_hash != wire_hash:
        raise VerificationError("legacy wire hash mismatch")
    checkpoint = None
    if row["observation_type"] == "DEPTH":
        checkpoint = hashlib.sha256(_canonical_json({
            "contract_id": payload_plain.get("contract_id"),
            "bids": payload_plain.get("bids"),
            "asks": payload_plain.get("asks"),
        })).hexdigest()
    view = {
        "observation_id": row["observation_id"], "session_id": row["session_id"],
        "observation_type": row["observation_type"],
        "local_monotonic_sequence": row["local_monotonic_sequence"],
        "ninja_receipt_time": row["ninja_receipt_time"],
        "provider_timestamp": row.get("provider_timestamp"),
        "provider_sequence": row.get("provider_sequence"),
        "exchange_timestamp": row.get("exchange_timestamp"),
        "account_alias": account_alias, "account_class": account_class,
        "payload": payload_plain, "payload_hash": payload_hash, "wire_hash": wire_hash,
        "source": row["source"], "provider": row["provider"],
        "environment": row["environment"], "source_identity": row["source_identity"],
        "build_identity": row["build_identity"],
        "contract_identity": row["contract_identity"], "contract_id": row["contract_id"],
        "generation": row["generation"], "book_checkpoint": checkpoint,
    }
    for key in PROOF_FIELDS:
        if key in row:
            view[key] = row[key]
    return view


def _lsc_view(row: Mapping[str, object]) -> dict[str, object]:
    try:
        verified = verify_record(bytes.fromhex(str(row["record_hex"])), bytes.fromhex(str(row["context_hex"])))
    except (KeyError, ValueError) as exc:
        raise VerificationError("LSC corpus row lacks valid context_hex/record_hex") from exc
    view = verified.parity_view()
    for key in PROOF_FIELDS:
        if key in row:
            view[key] = row[key]
    return view


def _first_mismatch(left: object, right: object, path: str = "$") -> dict[str, object] | None:
    if type(left) is not type(right):
        return {"path": path, "reason": "TYPE", "legacy_type": type(left).__name__, "lsc_type": type(right).__name__, "legacy": repr(left), "lsc": repr(right)}
    if isinstance(left, float):
        if struct.pack("<d", left) != struct.pack("<d", right):
            return {"path": path, "reason": "FLOAT_BITS", "legacy": repr(left), "lsc": repr(right)}
        return None
    if isinstance(left, Mapping):
        left_keys, right_keys = set(left), set(right)  # type: ignore[arg-type]
        if left_keys != right_keys:
            return {"path": path, "reason": "KEYS", "legacy_only": sorted(left_keys - right_keys), "lsc_only": sorted(right_keys - left_keys)}
        for key in sorted(left_keys):
            mismatch = _first_mismatch(left[key], right[key], f"{path}.{key}")  # type: ignore[index]
            if mismatch is not None:
                return mismatch
        return None
    if isinstance(left, Sequence) and not isinstance(left, (str, bytes)):
        if len(left) != len(right):  # type: ignore[arg-type]
            return {"path": path, "reason": "LENGTH", "legacy": len(left), "lsc": len(right)}  # type: ignore[arg-type]
        for index, (a, b) in enumerate(zip(left, right)):  # type: ignore[arg-type]
            mismatch = _first_mismatch(a, b, f"{path}[{index}]")
            if mismatch is not None:
                return mismatch
        return None
    if left != right:
        return {"path": path, "reason": "VALUE", "legacy": repr(left), "lsc": repr(right)}
    return None


def compare_corpora(legacy_path: Path, lsc_path: Path) -> dict[str, object]:
    legacy_rows, legacy_meta = _load_document(legacy_path)
    lsc_rows, lsc_meta = _load_document(lsc_path)
    mismatch: dict[str, object] | None = None
    matched = 0
    if len(legacy_rows) != len(lsc_rows):
        mismatch = {"path": "$.events", "reason": "COUNT", "legacy": len(legacy_rows), "lsc": len(lsc_rows)}
    else:
        for index, (legacy, lsc) in enumerate(zip(legacy_rows, lsc_rows)):
            mismatch = _first_mismatch(_legacy_view(legacy), _lsc_view(lsc), f"$.events[{index}]")
            if mismatch is not None:
                break
            matched += 1
        if mismatch is None:
            mismatch = _first_mismatch(dict(legacy_meta), dict(lsc_meta), "$.proof")
    passed = mismatch is None
    return {
        "schema": "beelzebub-lsc-parity-report-v1",
        "result": "PASS" if passed else "FAIL",
        "legacy_sha256": hashlib.sha256(legacy_path.read_bytes()).hexdigest(),
        "lsc_sha256": hashlib.sha256(lsc_path.read_bytes()).hexdigest(),
        "legacy_event_count": len(legacy_rows),
        "lsc_event_count": len(lsc_rows),
        "matched_event_count": matched,
        "checks": {
            "count": len(legacy_rows) == len(lsc_rows),
            "order_sequences_payload_types_hashes_timestamps_provenance": passed,
            "depth_book_checkpoints": passed,
            "optional_evidence_decision_final_replay": _first_mismatch(dict(legacy_meta), dict(lsc_meta), "$.proof") is None,
        },
        "first_mismatch": mismatch,
    }


def _write_new(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        handle.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    golden = sub.add_parser("golden")
    golden.add_argument("--vectors", type=Path, required=True)
    golden.add_argument("--output", type=Path)
    parity = sub.add_parser("parity")
    parity.add_argument("--legacy", type=Path, required=True)
    parity.add_argument("--lsc", type=Path, required=True)
    parity.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "golden":
            result = verify_golden_vectors(args.vectors)
            if args.output:
                _write_new(args.output, result)
            else:
                print(json.dumps(result, sort_keys=True, separators=(",", ":")))
        else:
            result = compare_corpora(args.legacy, args.lsc)
            _write_new(args.output, result)
            if result["result"] != "PASS":
                return 2
        return 0
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(json.dumps({"result": "ERROR", "error": type(exc).__name__, "detail": str(exc)}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
