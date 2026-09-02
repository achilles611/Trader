"""Durable, hash-chained SQLite ledger for every Lane III-G side effect."""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import StrEnum
from functools import lru_cache
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import threading
import time
from typing import Iterator, Mapping
from uuid import uuid4

from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import ACCOUNT_BINDING, PAPER_RECORD_SCHEMA, POLICY, RISK_PROFILE
from .sessions import (
    PaperCalendarState, PaperSessionContext, PaperSessionKind,
    UNSPECIFIED_OFF_SESSION_CONTEXT, context_from_identity,
)


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
_DEFERRED_READINESS_ATTESTATION_KINDS = frozenset({
    "COMMISSIONING_SESSION_WARMED",
    "COMMISSIONING_SESSION_WARMUP_RESET",
})
COMMISSIONING_TAIL_POLICY_VERSION = "l3g-commissioning-passive-tail-v3"
_COMMISSIONING_TAIL_PREVIOUS_POLICY_VERSION = "l3g-commissioning-passive-tail-v2"
COMMISSIONING_NO_AUTHORITY_EFFECT = "NONE"
COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_SEMANTICS = "READ_ONLY_ACCOUNT_AUTHORITY_OBSERVATION"
COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_PAYLOAD_KEYS = {
    "ORDER": frozenset({"native_order_id", "status", "quantity", "filled_quantity"}),
    "EXECUTION": frozenset({"native_execution_id", "price", "quantity"}),
    "POSITION": frozenset({"quantity", "direction", "average_price"}),
}
COMMISSIONING_READINESS_RECORD_SEMANTICS = "COMMISSIONING_READINESS_STATE_ATTESTATION"
COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION = 1
COMMISSIONING_WARMUP_REQUIRED_FAMILIES = (
    "STRUCTURAL_CONTEXT", "ORDER_FLOW", "RESTING_LIQUIDITY",
)
COMMISSIONING_WARMUP_POLICY_HASH = canonical_hash({
    "version": "l3g-commissioning-session-warmup-v1",
    "required_families": COMMISSIONING_WARMUP_REQUIRED_FAMILIES,
    "binding": (
        "session_kind", "session_id", "trade_date", "session_profile_hash", "session_generation",
    ),
    "natural_evidence_expiration_clears_latch": False,
    "runtime_restart_clears_latch": True,
})
_COMMISSIONING_WATERMARK_METADATA_KEY = "commissioning_authority_watermark"
_COMMISSIONING_WATERMARK_SCAN_LIMIT = 4096
_PASSIVE_MARKET_OBSERVATION_TYPES = frozenset({"QUOTE", "TRADE", "DEPTH"})
_PASSIVE_EVIDENCE_FAMILIES = frozenset({
    "STRUCTURAL_CONTEXT", "ORDER_FLOW", "RESTING_LIQUIDITY", "VOLATILITY_CONTEXT", "MARKET_REGIME",
})
_PASSIVE_DECISIONS = frozenset({"NO_TRADE", "LONG", "SHORT", "EXIT"})
_AUTHORITY_SHAPED_PAYLOAD_KEYS = frozenset({
    "command_id", "grant_id", "intent_id", "order_id", "execution_id", "commissioning_id",
    "working_order_count", "position_quantity", "risk_authority", "arm_grant", "lockout_reason",
})
_SESSION_CONTEXT_KEYS = frozenset({
    "session_kind", "session_family", "session_id", "trade_date", "timezone",
    "observation_start", "entry_start", "entry_cutoff", "hard_flat_deadline",
    "session_end", "session_profile_hash", "session_generation", "calendar_state",
})
_SESSION_IDENTITY_KEYS = frozenset({
    "session_kind", "session_family", "session_id", "trade_date",
    "session_profile_hash", "session_generation",
})
_OBSERVATION_ENVELOPE_KEYS = _SESSION_CONTEXT_KEYS | frozenset({
    "observation_id", "observation_type", "observed_at", "ninja_receipt_time",
    "provider_timestamp", "exchange_timestamp", "local_monotonic_sequence", "source_payload_hash",
})
_ACCOUNT_OBSERVATION_KEYS = _OBSERVATION_ENVELOPE_KEYS | frozenset({
    "authority_effect", "observation_semantics", "observation_payload_keys",
    "observation_account_alias", "observation_account_class",
})
_EVIDENCE_KEYS = frozenset({
    "evidence_id", "hypothesis_kind", "family", "label", "strength", "supports",
    "observed_at", "expires_at", "source_observation_ids", "source_local_sequences",
    "source_payload_hashes", "quality", "sequence_authority", "book_completeness",
    "scientific_eligibility", "blocking", "session_kind", "session_id", "trade_date",
    "session_profile_hash", "session_generation", "source_session_ids", "session_family",
})
_DECISION_KEYS = frozenset({
    "paper_decision_id", "paper_policy_id", "paper_policy_hash", "decision", "created_at",
    "expires_at", "hypothesis_kind", "direction", "relative_support", "family_summary",
    "source_observation_ids", "source_local_sequences", "source_payload_hashes",
    "sequence_authority", "book_completeness", "scientific_eligibility", "reason_code",
    "session_kind", "session_id", "trade_date", "session_profile_hash", "session_generation",
    "commissioning", "strategy_generated", "scientific_evidence", "session_family",
    "authority_effect",
})
_DECISION_WITH_EFFECT_KEYS = _DECISION_KEYS - {"authority_effect"}
_WARMUP_ATTESTATION_KEYS = _SESSION_CONTEXT_KEYS | frozenset({
    "authority_effect", "record_semantics", "record_semantics_version",
    "commissioning_warmup_state", "policy_hash", "required_families", "reason",
})
_WARMED_ATTESTATION_KEYS = _WARMUP_ATTESTATION_KEYS | frozenset({
    "warmed_at", "evidence_provenance",
})
_RESET_ATTESTATION_KEYS = _WARMUP_ATTESTATION_KEYS | frozenset({
    "reset_at", "seen_families", "warmed_at",
})
_KNOWN_AUTHORITY_MUTATION_DOMAINS = frozenset({
    "SESSION", "INTENT", "RISK_GRANT", "COMMAND", "COMMAND_RECEIPT",
    "ORDER_EVENT", "EXECUTION", "POSITION_SNAPSHOT", "RISK_EVENT",
})
_KNOWN_COMMISSIONING_MUTATION_PREFIXES = (
    "COMMISSIONING_OWNERSHIP_", "COMMISSIONING_ENTRY_", "COMMISSIONING_PREFLIGHT_",
    "COMMISSIONING_CLOSURE",
)
_WATERMARK_EVENT_PREFIXES = (
    "last_authority_mutation", "last_authority_observation", "last_unknown",
)
_V3_WATERMARK_KEYS = frozenset({
    "policy_version", "classified_through_sequence", "classified_through_hash",
    "last_authority_mutation_sequence", "last_authority_mutation_kind",
    "last_authority_mutation_domain", "last_authority_mutation_hash",
    "last_authority_observation_sequence", "last_authority_observation_kind",
    "last_authority_observation_domain", "last_authority_observation_hash",
    "last_unknown_sequence", "last_unknown_kind", "last_unknown_domain", "last_unknown_hash",
    "safe_classification_last_sequences", "updated_at",
})
_V2_WATERMARK_KEYS = frozenset({
    "policy_version", "classified_through_sequence",
    "last_authority_mutation_sequence", "last_authority_mutation_kind",
    "last_authority_mutation_domain", "last_authority_mutation_hash",
    "safe_classification_last_sequences", "updated_at",
})
_V2_SAFE_CLASSIFICATIONS = frozenset({
    *(f"OBSERVATION:OBSERVATION_ENVELOPE:{kind}" for kind in _PASSIVE_MARKET_OBSERVATION_TYPES),
    "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE",
    "EVIDENCE:EVIDENCE",
    *(f"DECISION:DECISION:{kind}:AUTHORITY_EFFECT_NONE" for kind in _PASSIVE_DECISIONS),
})
_V3_AUTHORITY_OBSERVATION_CLASSIFICATIONS = frozenset({
    "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE",
    *(
        f"AUTHORITY_OBSERVATION:OBSERVATION_ENVELOPE:{kind}:AUTHORITY_EFFECT_NONE"
        for kind in COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_PAYLOAD_KEYS
    ),
    "AUTHORITY_OBSERVATION:COMMISSIONING_SESSION_WARMED:AUTHORITY_EFFECT_NONE",
    "AUTHORITY_OBSERVATION:COMMISSIONING_SESSION_WARMUP_RESET:AUTHORITY_EFFECT_NONE",
})
_SECRET_KEYS = frozenset({"hmac_key", "password", "token", "connection_credentials", "private_key", "secret", "authorization"})
_EPOCH_DIRECTORY = re.compile(r"^epoch-(\d+)$", re.IGNORECASE)
_EPOCH_ID = re.compile(r"^L3G-PAPER-EPOCH-[A-Za-z0-9][A-Za-z0-9._-]*$")
# These three artifacts are module-level frozen contracts.  Retain their
# exact computed values once, rather than canonicalizing their static payloads
# for every admitted market callback.  The values remain part of every record
# envelope and hash-chain document exactly as before.
_PAPER_POLICY_HASH = POLICY.configuration_hash
_RISK_PROFILE_HASH = RISK_PROFILE.configuration_hash
_ACCOUNT_BINDING_HASH = ACCOUNT_BINDING.binding_hash

# Deferred records are still strictly ordered.  These bounds merely prevent a
# failed or under-provisioned writer from turning the observer process into an
# unbounded in-memory spool.
_DEFERRED_NORMAL_BATCH_SIZE = 512
_DEFERRED_MAX_COALESCE_SECONDS = 0.010
# Preserve the 512-record normal latency batch, but let only an established
# backlog use the measured recovery ceiling. This makes the production default
# match the recovery benchmark without enlarging an ordinary low-latency write.
_DEFERRED_CATCH_UP_BATCH_SIZE = 2_048
_DEFERRED_CATCH_UP_THRESHOLD = 1024
_DEFERRED_MAX_RECORDS = 16384
_DEFERRED_DEGRADED_RECORDS = 4096
# Checkpoints are queue items too.  Bound their independently admitted control
# work so a blocked writer cannot turn concurrent commissioning/status callers
# into an unbounded in-memory queue even when market-record admission is idle.
_DEFERRED_MAX_PENDING_BARRIERS = 128
_DEFERRED_HEADROOM_GRACE_SECONDS = 5.0
_WRITER_TELEMETRY_WINDOW_SECONDS = 30.0
_WRITER_TELEMETRY_HISTORY = 120
_WRITER_TELEMETRY_SAMPLE_INTERVAL = 8
_SQLITE_IN_LOOKUP_CHUNK = 900
# Keep checkpoint I/O off the sole FIFO writer.  SQLite's automatic
# checkpoints run inside the committing writer transaction and were measured
# to create multi-second stalls once a warm ledger crossed its WAL window.
# The dedicated maintenance connection below uses only PASSIVE checkpoints:
# it never blocks or rolls back a writer and the final TRUNCATE proof remains
# the sole clean-shutdown checkpoint claim.
_WAL_AUTOCHECKPOINT_PAGES = 0
_WAL_JOURNAL_SIZE_LIMIT_BYTES = 134_217_728
_WAL_PASSIVE_CHECKPOINT_TRIGGER_RECORDS = 1_024
_WAL_PASSIVE_CHECKPOINT_TRIGGER_BYTES = _WAL_JOURNAL_SIZE_LIMIT_BYTES
_WAL_PASSIVE_CHECKPOINT_MIN_INTERVAL_SECONDS = 0.025
_WAL_PASSIVE_CHECKPOINT_START_DELAY_SECONDS = 5.0
# ``journal_size_limit`` is a post-checkpoint retention target, not a hard
# ceiling while an external reader pins a WAL snapshot. A PASSIVE-complete WAL
# may also retain its already allocated file bytes harmlessly. Gate authority
# on uncheckpointed frames (the meaningful pinned growth), while retaining a
# separate 1 GiB absolute allocation ceiling as a final disk-growth backstop.
# This deliberately leaves headroom above the largest observed healthy retained
# allocation (~453 MiB) so a complete PASSIVE checkpoint cannot spuriously
# exhaust admission solely because SQLite kept reusable WAL space allocated.
_WAL_UNCHECKPOINTED_CAPACITY_CEILING_BYTES = 134_217_728
_WAL_FILE_CAPACITY_CEILING_BYTES = 1_073_741_824
# The authority ledger is intentionally compact.  Production suppresses the
# raw market/evidence/no-effect decision firehose below, but a separate hard
# database runway gate remains necessary in case a future producer regresses.
_AUTHORITY_LEDGER_WARNING_BYTES = 32 * 1024**3
_AUTHORITY_LEDGER_CAPACITY_BYTES = 40 * 1024**3
_AUTHORITY_LEDGER_DISK_WARNING_FREE_BYTES = 8 * 1024**3
_AUTHORITY_LEDGER_DISK_MINIMUM_FREE_BYTES = 4 * 1024**3
_AUTHORITY_LEDGER_RUNWAY_WARNING_SECONDS = 24 * 60 * 60
_AUTHORITY_LEDGER_CAPACITY_SAMPLE_SECONDS = 1.0
_AUTHORITY_LEDGER_GROWTH_WINDOW_SECONDS = 120.0


class LedgerCapacityError(RuntimeError):
    """A deferred writer work item was refused before it became durable."""

    def __init__(self, message: str, capacity: Mapping[str, object] | None = None) -> None:
        super().__init__(message)
        self.capacity = {} if capacity is None else dict(capacity)


def deferred_capacity_allows_authority(capacity: Mapping[str, object]) -> bool:
    """Return the one fail-closed predicate used by every authority gate.

    Capacity snapshots are deliberately lightweight condition-lock state.  The
    predicate is kept outside ``PaperLedger`` so the commissioning gate and
    runtime cannot accidentally drift from the writer's own admission rule.
    """
    growth = capacity.get("queue_growth_records_per_second")
    return (
        capacity.get("schema") == "l3g-ledger-writer-capacity-v1"
        and capacity.get("state") == "HEALTHY"
        and capacity.get("admission_open") is True
        and capacity.get("negative_headroom_sustained") is False
        and capacity.get("writer_error") is None
        and type(growth) in {int, float}
        and math.isfinite(float(growth))
        and float(growth) <= 0.0
    )


def _idempotency_fingerprint(
    common: Mapping[str, object], *, ignore_occurred_at: bool = False,
) -> str:
    """Fingerprint immutable caller content, optionally omitting generated time.

    An explicitly supplied occurrence time is part of a record's content and
    must therefore conflict when reused with the same external identity.  A
    caller which omitted the time is allowed to retry after the ledger creates
    a fresh envelope timestamp.
    """
    return canonical_hash({
        key: value for key, value in common.items()
        if not (ignore_occurred_at and key == "occurred_at")
    })


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


class CommissioningTailCategory(StrEnum):
    PASSIVE_DATA = "PASSIVE_DATA"
    AUTHORITY_OBSERVATION = "AUTHORITY_OBSERVATION"
    AUTHORITY_MUTATION = "AUTHORITY_MUTATION"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class CommissioningTailClassification:
    category: CommissioningTailCategory
    shape: str

    @property
    def accepted_without_verification(self) -> bool:
        return self.category in {
            CommissioningTailCategory.PASSIVE_DATA,
            CommissioningTailCategory.AUTHORITY_OBSERVATION,
        }


def _exact_keys(payload: Mapping[str, object], expected: frozenset[str]) -> bool:
    return set(payload) == expected


def _is_hash(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _is_utc(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        normalized_utc(value, "Commissioning tail record time")
    except (TypeError, ValueError):
        return False
    return True


@lru_cache(maxsize=256)
def _cached_session_context(
    session_kind: str,
    session_id: str,
    trade_date: str,
    session_profile_hash: str,
    session_generation: int,
    calendar_state: str,
) -> PaperSessionContext:
    """Rehydrate one immutable context without repeating compiled-profile work."""
    return context_from_identity(
        PaperSessionKind(session_kind), session_id, trade_date, session_profile_hash,
        session_generation, calendar_state=PaperCalendarState(calendar_state),
    )


@lru_cache(maxsize=256)
def _cached_session_context_payload(
    session_kind: str,
    session_id: str,
    trade_date: str,
    session_profile_hash: str,
    session_generation: int,
    calendar_state: str,
) -> tuple[tuple[str, object], ...] | None:
    """Cache the immutable context reconstruction used by every market row.

    This is deliberately a cache of the existing validator's result rather
    than a looser validation path.  It therefore preserves the exact tail
    classification contract while avoiding thousands of identical calendar
    reconstructions per writer batch.
    """
    try:
        context = _cached_session_context(
            session_kind, session_id, trade_date, session_profile_hash,
            session_generation, calendar_state,
        )
    except (TypeError, ValueError):
        return None
    return tuple(context.payload().items())


@lru_cache(maxsize=256)
def _cached_session_family(
    session_kind: str,
    session_id: str,
    trade_date: str,
    session_profile_hash: str,
    session_generation: int,
) -> str | None:
    """Return the exact family produced by the existing identity validator."""
    try:
        return _cached_session_context(
            session_kind, session_id, trade_date, session_profile_hash,
            session_generation, PaperCalendarState.NORMAL.value,
        ).session_family.value
    except (TypeError, ValueError):
        return None


def _session_context_matches(payload: Mapping[str, object]) -> bool:
    if type(payload.get("session_generation")) is not int:
        return False
    try:
        expected = _cached_session_context_payload(
            str(payload["session_kind"]), str(payload["session_id"]),
            str(payload["trade_date"]), str(payload["session_profile_hash"]),
            int(payload["session_generation"]), str(payload["calendar_state"]),
        )
    except KeyError:
        return False
    return expected is not None and all(payload.get(key) == value for key, value in expected)


def _session_identity_matches(payload: Mapping[str, object]) -> bool:
    if type(payload.get("session_generation")) is not int:
        return False
    try:
        family = _cached_session_family(
            str(payload["session_kind"]), str(payload["session_id"]),
            str(payload["trade_date"]), str(payload["session_profile_hash"]),
            int(payload["session_generation"]),
        )
    except KeyError:
        return False
    return family is not None and payload.get("session_family") == family


def _aligned_provenance(
    identifiers: object, sequences: object, hashes: object, *, sessions: object | None = None,
) -> bool:
    if (
        not isinstance(identifiers, list) or not identifiers
        or not isinstance(sequences, list) or not isinstance(hashes, list)
        or len(identifiers) != len(sequences) or len(identifiers) != len(hashes)
        or not all(isinstance(value, str) and value for value in identifiers)
        or not all(type(value) is int and value >= 0 for value in sequences)
        or not all(_is_hash(value) for value in hashes)
    ):
        return False
    return sessions is None or (
        isinstance(sessions, list)
        and len(sessions) == len(identifiers)
        and all(isinstance(value, str) and value for value in sessions)
    )


def _market_observation_shape(payload: Mapping[str, object]) -> str | None:
    observation_type = payload.get("observation_type")
    if (
        not isinstance(observation_type, str)
        or observation_type not in _PASSIVE_MARKET_OBSERVATION_TYPES
        or not _exact_keys(payload, _OBSERVATION_ENVELOPE_KEYS)
        or not _session_context_matches(payload)
        or not isinstance(payload.get("observation_id"), str)
        or not payload.get("observation_id")
        or type(payload.get("local_monotonic_sequence")) is not int
        or int(payload["local_monotonic_sequence"]) < 0
        or not _is_hash(payload.get("source_payload_hash"))
        or not _is_utc(payload.get("observed_at"))
        or not _is_utc(payload.get("ninja_receipt_time"))
        or not all(value is None or _is_utc(value) for value in (
            payload.get("provider_timestamp"), payload.get("exchange_timestamp"),
        ))
    ):
        return None
    return f"OBSERVATION:OBSERVATION_ENVELOPE:{observation_type}"


def _informational_account_shape(payload: Mapping[str, object]) -> str | None:
    account_alias = payload.get("observation_account_alias")
    account_class = payload.get("observation_account_class")
    if (
        payload.get("observation_type") != "ACCOUNT"
        or not _exact_keys(payload, _ACCOUNT_OBSERVATION_KEYS)
        or not _session_context_matches(payload)
        or not isinstance(payload.get("observation_id"), str)
        or not payload.get("observation_id")
        or type(payload.get("local_monotonic_sequence")) is not int
        or int(payload["local_monotonic_sequence"]) < 0
        or not _is_hash(payload.get("source_payload_hash"))
        or not _is_utc(payload.get("observed_at"))
        or not _is_utc(payload.get("ninja_receipt_time"))
        or not all(value is None or _is_utc(value) for value in (
            payload.get("provider_timestamp"), payload.get("exchange_timestamp"),
        ))
        or payload.get("authority_effect") != COMMISSIONING_NO_AUTHORITY_EFFECT
        or payload.get("observation_semantics") != "INFORMATIONAL_ACCOUNT_ITEM"
        or payload.get("observation_payload_keys") != ["item", "value"]
        or not isinstance(account_alias, str)
        or not isinstance(account_class, str)
        or (account_alias, account_class) not in {
            ("Sim101", "LOCAL_SIMULATION"),
            ("Lucid25kflex01", "PROVIDER_EVALUATION"),
        }
    ):
        return None
    return "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE"


def _account_authority_observation_shape(payload: Mapping[str, object]) -> str | None:
    observation_type = payload.get("observation_type")
    account_alias = payload.get("observation_account_alias")
    account_class = payload.get("observation_account_class")
    payload_keys = payload.get("observation_payload_keys")
    expected_payload_keys = (
        COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_PAYLOAD_KEYS.get(observation_type)
        if isinstance(observation_type, str) else None
    )
    if (
        expected_payload_keys is None
        or not _exact_keys(payload, _ACCOUNT_OBSERVATION_KEYS)
        or not _session_context_matches(payload)
        or not isinstance(payload.get("observation_id"), str)
        or not payload.get("observation_id")
        or type(payload.get("local_monotonic_sequence")) is not int
        or int(payload["local_monotonic_sequence"]) < 0
        or not _is_hash(payload.get("source_payload_hash"))
        or not _is_utc(payload.get("observed_at"))
        or not _is_utc(payload.get("ninja_receipt_time"))
        or not all(value is None or _is_utc(value) for value in (
            payload.get("provider_timestamp"), payload.get("exchange_timestamp"),
        ))
        or payload.get("authority_effect") != COMMISSIONING_NO_AUTHORITY_EFFECT
        or payload.get("observation_semantics") != COMMISSIONING_ACCOUNT_AUTHORITY_OBSERVATION_SEMANTICS
        or not isinstance(payload_keys, list)
        or payload_keys != sorted(expected_payload_keys)
        or not isinstance(account_alias, str)
        or not isinstance(account_class, str)
        or (account_alias, account_class) not in {
            ("Sim101", "LOCAL_SIMULATION"),
            ("Lucid25kflex01", "PROVIDER_EVALUATION"),
        }
    ):
        return None
    return (
        f"AUTHORITY_OBSERVATION:OBSERVATION_ENVELOPE:{observation_type}:"
        "AUTHORITY_EFFECT_NONE"
    )


def _evidence_shape(payload: Mapping[str, object]) -> str | None:
    family = payload.get("family")
    if (
        not _exact_keys(payload, _EVIDENCE_KEYS)
        or not _session_identity_matches(payload)
        or not isinstance(payload.get("evidence_id"), str)
        or not str(payload["evidence_id"]).startswith("l3g-pe-")
        or not isinstance(family, str)
        or family not in _PASSIVE_EVIDENCE_FAMILIES
        or payload.get("scientific_eligibility") is not False
        or payload.get("book_completeness") != "UNVERIFIED"
        or payload.get("sequence_authority") != "LOCAL_CALLBACK_ORDER_ONLY"
        or payload.get("quality") != "PROVISIONAL_CONTIGUOUS_LOCAL_CALLBACKS"
        or type(payload.get("supports")) is not bool
        or type(payload.get("blocking")) is not bool
        or not isinstance(payload.get("label"), str)
        or not _is_utc(payload.get("observed_at"))
        or not _is_utc(payload.get("expires_at"))
        or not _aligned_provenance(
            payload.get("source_observation_ids"), payload.get("source_local_sequences"),
            payload.get("source_payload_hashes"), sessions=payload.get("source_session_ids"),
        )
        or set(payload.get("source_session_ids") or ()) != {payload.get("session_id")}
    ):
        return None
    return "EVIDENCE:EVIDENCE"


def _passive_decision_shape(payload: Mapping[str, object]) -> str | None:
    decision = payload.get("decision")
    expected_direction = {
        "NO_TRADE": "FLAT", "LONG": "LONG", "SHORT": "SHORT", "EXIT": "FLAT",
    }.get(str(decision))
    if (
        not isinstance(decision, str)
        or decision not in _PASSIVE_DECISIONS
        or not _exact_keys(payload, _DECISION_KEYS)
        or not _session_identity_matches(payload)
        or payload.get("direction") != expected_direction
        or payload.get("authority_effect") != COMMISSIONING_NO_AUTHORITY_EFFECT
        or payload.get("commissioning") is not False
        or payload.get("strategy_generated") is not True
        or payload.get("scientific_evidence") is not False
        or payload.get("scientific_eligibility") is not False
        or payload.get("sequence_authority") != "LOCAL_CALLBACK_ORDER_ONLY"
        or payload.get("book_completeness") != "UNVERIFIED"
        or not isinstance(payload.get("paper_decision_id"), str)
        or not str(payload["paper_decision_id"]).startswith("l3g-pd-")
        or payload.get("paper_policy_id") != POLICY.policy_id
        or payload.get("paper_policy_hash") != _PAPER_POLICY_HASH
        or not isinstance(payload.get("family_summary"), Mapping)
        or not isinstance(payload.get("reason_code"), str)
        or not _is_utc(payload.get("created_at"))
        or not _is_utc(payload.get("expires_at"))
        or not _aligned_provenance(
            payload.get("source_observation_ids"), payload.get("source_local_sequences"),
            payload.get("source_payload_hashes"),
        )
    ):
        return None
    return f"DECISION:DECISION:{decision}:AUTHORITY_EFFECT_NONE"


def _authority_decision_shape(payload: Mapping[str, object]) -> bool:
    """Recognize the producer's exact effect-capable decision shape."""
    decision = payload.get("decision")
    expected_direction = {
        "NO_TRADE": "FLAT", "LONG": "LONG", "SHORT": "SHORT", "EXIT": "FLAT",
    }.get(str(decision))
    return (
        isinstance(decision, str)
        and decision in _PASSIVE_DECISIONS
        and _exact_keys(payload, _DECISION_WITH_EFFECT_KEYS)
        and _session_identity_matches(payload)
        and payload.get("direction") == expected_direction
        and payload.get("scientific_eligibility") is False
        and payload.get("sequence_authority") == "LOCAL_CALLBACK_ORDER_ONLY"
        and payload.get("book_completeness") == "UNVERIFIED"
        and isinstance(payload.get("paper_decision_id"), str)
        and str(payload["paper_decision_id"]).startswith("l3g-pd-")
        and payload.get("paper_policy_id") == POLICY.policy_id
        and payload.get("paper_policy_hash") == _PAPER_POLICY_HASH
        and isinstance(payload.get("family_summary"), Mapping)
        and isinstance(payload.get("reason_code"), str)
        and type(payload.get("commissioning")) is bool
        and type(payload.get("strategy_generated")) is bool
        and payload.get("strategy_generated") is (not payload.get("commissioning"))
        and payload.get("scientific_evidence") is False
        and _is_utc(payload.get("created_at"))
        and _is_utc(payload.get("expires_at"))
        and _aligned_provenance(
            payload.get("source_observation_ids"), payload.get("source_local_sequences"),
            payload.get("source_payload_hashes"),
        )
    )


def _warmup_provenance_valid(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != set(COMMISSIONING_WARMUP_REQUIRED_FAMILIES):
        return False
    for item in value.values():
        if (
            not isinstance(item, Mapping)
            or set(item) != {"evidence_id", "observed_at", "source_observation_ids", "source_local_sequences"}
            or not isinstance(item.get("evidence_id"), str)
            or not str(item["evidence_id"]).startswith("l3g-pe-")
            or not _is_utc(item.get("observed_at"))
            or not isinstance(item.get("source_observation_ids"), list)
            or not item.get("source_observation_ids")
            or not all(isinstance(identifier, str) and identifier for identifier in item["source_observation_ids"])
            or not isinstance(item.get("source_local_sequences"), list)
            or len(item["source_observation_ids"]) != len(item["source_local_sequences"])
            or not all(type(sequence) is int and sequence >= 0 for sequence in item["source_local_sequences"])
        ):
            return False
    return True


def _warmup_attestation_shape(kind: str, payload: Mapping[str, object]) -> str | None:
    seen_families = payload.get("seen_families")
    common = (
        _session_context_matches(payload)
        and payload.get("authority_effect") == COMMISSIONING_NO_AUTHORITY_EFFECT
        and payload.get("record_semantics") == COMMISSIONING_READINESS_RECORD_SEMANTICS
        and type(payload.get("record_semantics_version")) is int
        and payload.get("record_semantics_version") == COMMISSIONING_READINESS_RECORD_SEMANTICS_VERSION
        and payload.get("policy_hash") == COMMISSIONING_WARMUP_POLICY_HASH
        and payload.get("required_families") == list(COMMISSIONING_WARMUP_REQUIRED_FAMILIES)
        and isinstance(payload.get("reason"), str)
        and bool(payload.get("reason"))
    )
    if (
        kind == "COMMISSIONING_SESSION_WARMED"
        and common
        and _exact_keys(payload, _WARMED_ATTESTATION_KEYS)
        and payload.get("commissioning_warmup_state") == "WARMED"
        and payload.get("reason") == "ALL_REQUIRED_FAMILIES_GENUINELY_OBSERVED"
        and _is_utc(payload.get("warmed_at"))
        and _warmup_provenance_valid(payload.get("evidence_provenance"))
    ):
        return "AUTHORITY_OBSERVATION:COMMISSIONING_SESSION_WARMED:AUTHORITY_EFFECT_NONE"
    if (
        kind == "COMMISSIONING_SESSION_WARMUP_RESET"
        and common
        and _exact_keys(payload, _RESET_ATTESTATION_KEYS)
        and payload.get("commissioning_warmup_state") == "NOT_WARMED"
        and _is_utc(payload.get("reset_at"))
        and (payload.get("warmed_at") is None or _is_utc(payload.get("warmed_at")))
        and isinstance(seen_families, list)
        and all(isinstance(family, str) for family in seen_families)
        and seen_families == sorted(set(seen_families))
        and set(seen_families).issubset(COMMISSIONING_WARMUP_REQUIRED_FAMILIES)
    ):
        return "AUTHORITY_OBSERVATION:COMMISSIONING_SESSION_WARMUP_RESET:AUTHORITY_EFFECT_NONE"
    return None


def commissioning_tail_classification(
    domain: str, kind: str, payload: Mapping[str, object],
) -> CommissioningTailClassification:
    """Return the explicit v3 authority category for one exact stored shape."""
    if not isinstance(domain, str) or not isinstance(kind, str) or not isinstance(payload, Mapping):
        return CommissioningTailClassification(
            CommissioningTailCategory.UNKNOWN, "UNKNOWN:MALFORMED_RECORD_ENVELOPE",
        )
    if domain == "OBSERVATION" and kind == "OBSERVATION_ENVELOPE":
        passive_shape = _market_observation_shape(payload)
        if passive_shape is not None:
            return CommissioningTailClassification(CommissioningTailCategory.PASSIVE_DATA, passive_shape)
        account_shape = _informational_account_shape(payload)
        if account_shape is not None:
            return CommissioningTailClassification(CommissioningTailCategory.AUTHORITY_OBSERVATION, account_shape)
        authority_observation_shape = _account_authority_observation_shape(payload)
        if authority_observation_shape is not None:
            return CommissioningTailClassification(
                CommissioningTailCategory.AUTHORITY_OBSERVATION, authority_observation_shape,
            )
        return CommissioningTailClassification(CommissioningTailCategory.UNKNOWN, f"UNKNOWN:{domain}:{kind}")
    if domain == "EVIDENCE" and kind == "EVIDENCE":
        evidence_shape = _evidence_shape(payload)
        if evidence_shape is not None:
            return CommissioningTailClassification(CommissioningTailCategory.PASSIVE_DATA, evidence_shape)
        return CommissioningTailClassification(CommissioningTailCategory.UNKNOWN, f"UNKNOWN:{domain}:{kind}")
    if domain == "DECISION" and kind == "DECISION":
        decision_shape = _passive_decision_shape(payload)
        if decision_shape is not None:
            return CommissioningTailClassification(CommissioningTailCategory.PASSIVE_DATA, decision_shape)
        if _authority_decision_shape(payload):
            return CommissioningTailClassification(
                CommissioningTailCategory.AUTHORITY_MUTATION, f"AUTHORITY_MUTATION:{domain}:{kind}",
            )
        return CommissioningTailClassification(CommissioningTailCategory.UNKNOWN, f"UNKNOWN:{domain}:{kind}")
    if domain == "INCIDENT" and kind in {
        "COMMISSIONING_SESSION_WARMED", "COMMISSIONING_SESSION_WARMUP_RESET",
    }:
        observation_shape = _warmup_attestation_shape(kind, payload)
        if observation_shape is not None:
            return CommissioningTailClassification(
                CommissioningTailCategory.AUTHORITY_OBSERVATION, observation_shape,
            )
        return CommissioningTailClassification(CommissioningTailCategory.UNKNOWN, f"UNKNOWN:{domain}:{kind}")
    if domain in _KNOWN_AUTHORITY_MUTATION_DOMAINS or any(
        kind.startswith(prefix) for prefix in _KNOWN_COMMISSIONING_MUTATION_PREFIXES
    ):
        return CommissioningTailClassification(
            CommissioningTailCategory.AUTHORITY_MUTATION, f"AUTHORITY_MUTATION:{domain}:{kind}",
        )
    return CommissioningTailClassification(CommissioningTailCategory.UNKNOWN, f"UNKNOWN:{domain}:{kind}")


def commissioning_safe_tail_classification(
    domain: str, kind: str, payload: Mapping[str, object],
) -> str | None:
    """Backward-compatible accepted-tail shape helper."""
    classification = commissioning_tail_classification(domain, kind, payload)
    return classification.shape if classification.accepted_without_verification else None


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
        and commissioning_tail_classification(domain, kind, payload).accepted_without_verification
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


@dataclass
class _DeferredLedgerBarrier:
    """An in-order fence captured by the sole deferred writer."""

    token: int
    requested_sequences: tuple[int, ...]
    admitted_at: float = field(default_factory=time.perf_counter)
    completed: bool = False
    ledger_sequence: int | None = None
    record_hash: str | None = None
    requested_record_hashes: dict[int, str | None] | None = None
    authority_watermark: dict[str, object] | None = None
    external_authority_sequence: int = 0
    external_authority_hash: str | None = None
    deferred_capacity: dict[str, object] | None = None
    wait_seconds: float | None = None


class PaperLedger:
    """Thread-safe append-only domain ledger with one global hash chain."""

    def __init__(
        self,
        path: str | Path,
        *,
        epoch_id: str | None = None,
        max_deferred_records: int = _DEFERRED_MAX_RECORDS,
        catch_up_batch_size: int = _DEFERRED_CATCH_UP_BATCH_SIZE,
        catch_up_threshold: int = _DEFERRED_CATCH_UP_THRESHOLD,
        degraded_queue_depth: int | None = None,
        max_pending_barriers: int | None = None,
        persist_high_frequency_records: bool = False,
    ) -> None:
        if type(max_deferred_records) is not int or max_deferred_records < 1:
            raise ValueError("Paper ledger deferred capacity must be a positive integer.")
        if type(catch_up_batch_size) is not int or catch_up_batch_size < 1:
            raise ValueError("Paper ledger catch-up batch size must be a positive integer.")
        if type(catch_up_threshold) is not int or catch_up_threshold < 1:
            raise ValueError("Paper ledger catch-up threshold must be a positive integer.")
        if degraded_queue_depth is not None and (
            type(degraded_queue_depth) is not int or degraded_queue_depth < 1
        ):
            raise ValueError("Paper ledger degraded queue depth must be a positive integer.")
        if max_pending_barriers is not None and (
            type(max_pending_barriers) is not int
            or not 1 <= max_pending_barriers <= max_deferred_records
        ):
            raise ValueError(
                "Paper ledger pending barrier capacity must be a positive integer within deferred capacity."
            )
        if type(persist_high_frequency_records) is not bool:
            raise ValueError("Paper ledger high-frequency persistence policy must be boolean.")
        self.path = Path(path).resolve()
        self._persist_high_frequency_records = persist_high_frequency_records
        self._creation_epoch = resolve_ledger_epoch(self.path, epoch_id)
        existing_accessibility = _read_only_accessibility_check(self.path) if self.path.exists() else None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(str(self.path), check_same_thread=False, isolation_level=None)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        # Do not let SQLite run a checkpoint inside this sole FIFO writer.
        # A separate maintenance connection performs bounded PASSIVE work;
        # its activity cannot make a committed record disappear or reorder
        # the hash chain, while final shutdown still performs TRUNCATE proof.
        self._connection.execute(f"PRAGMA wal_autocheckpoint={_WAL_AUTOCHECKPOINT_PAGES}")
        self._connection.execute(f"PRAGMA journal_size_limit={_WAL_JOURNAL_SIZE_LIMIT_BYTES}")
        self._connection.execute("PRAGMA cache_size=-65536")
        self._connection.execute("PRAGMA temp_store=MEMORY")
        self._connection.execute("PRAGMA mmap_size=268435456")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._synchronous_mode = "FULL"
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._session_context_lock = threading.Lock()
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
        # Process-local execution-input generation. Active commissioning
        # ownership is never restored across restart, so this cursor only needs
        # to distinguish new bridge receipts within this process lifetime.
        self._last_external_authority_sequence = 0
        self._last_external_authority_hash: str | None = None
        self._chain_status: tuple[bool | None, str | None] = (True, None) if self._highest_sequence == 0 else (None, None)
        self._ordering_lock = threading.RLock()
        self._deferred_condition = threading.Condition(threading.Lock())
        self._deferred: deque[dict[str, object] | _DeferredLedgerBarrier] = deque()
        self._deferred_identities: set[str] = set()
        self._deferred_identity_fingerprints: dict[str, tuple[str, str, bool]] = {}
        self._deferred_pending_admitted_at: deque[float] = deque()
        self._deferred_record_count = 0
        self._deferred_inflight_record_count = 0
        self._deferred_inflight_oldest_admitted_at: float | None = None
        self._deferred_queue_high_water = 0
        self._deferred_barrier_count = 0
        self._deferred_barrier_high_water = 0
        self._deferred_active = False
        self._deferred_error: BaseException | None = None
        self._deferred_stopping = False
        self._deferred_flush_requested = False
        self._admission_open = True
        self._closing = False
        self._closed = False
        self._max_deferred_records = max_deferred_records
        self._max_pending_barriers = (
            min(_DEFERRED_MAX_PENDING_BARRIERS, max_deferred_records)
            if max_pending_barriers is None else max_pending_barriers
        )
        self._catch_up_batch_size = max(_DEFERRED_NORMAL_BATCH_SIZE, catch_up_batch_size)
        self._catch_up_threshold = catch_up_threshold
        self._degraded_queue_depth = min(
            max_deferred_records,
            degraded_queue_depth if degraded_queue_depth is not None
            else min(_DEFERRED_DEGRADED_RECORDS, max_deferred_records),
        )
        self._capacity_fault_latched = False
        self._database_capacity_fault_latched = False
        self._database_capacity_fault_reason: str | None = None
        self._database_size_bytes: int | None = None
        self._database_free_bytes: int | None = None
        self._database_capacity_last_sample_at: float | None = None
        self._database_growth_samples: deque[tuple[float, int]] = deque(
            maxlen=_WRITER_TELEMETRY_HISTORY
        )
        self._suppressed_high_frequency_records = 0
        self._suppressed_high_frequency_by_domain: dict[str, int] = {
            "OBSERVATION": 0, "EVIDENCE": 0, "DECISION": 0,
        }
        self._capacity_degraded_since: float | None = None
        self._negative_headroom_since: float | None = None
        self._admitted_records_total = 0
        self._durable_records_total = 0
        self._admission_rejections_total = 0
        self._barrier_rejections_total = 0
        self._admission_rate_buckets: deque[tuple[int, int]] = deque()
        self._durable_rate_buckets: deque[tuple[int, int]] = deque()
        self._queue_depth_samples: deque[tuple[float, int]] = deque(maxlen=_WRITER_TELEMETRY_HISTORY)
        self._writer_batches: deque[dict[str, object]] = deque(maxlen=_WRITER_TELEMETRY_HISTORY)
        self._writer_batch_counter = 0
        self._last_checkpoint: dict[str, object] | None = None
        self._checkpoint_condition = threading.Condition(threading.Lock())
        self._checkpoint_started_at = time.perf_counter()
        self._checkpoint_requested = False
        self._checkpoint_stopping = False
        self._checkpoint_active = False
        self._checkpoint_last_requested_durable_total = 0
        self._checkpoint_last_completed_durable_total = 0
        self._checkpoint_durable_total = 0
        self._last_passive_checkpoint: dict[str, object] | None = None
        self._checkpoint_worker_error: str | None = None
        self._checkpoint_wal_size_bytes = self._current_wal_size_bytes()
        self._checkpoint_uncheckpointed_bytes = 0
        self._wal_capacity_fault_latched = (
            self._checkpoint_wal_size_bytes > _WAL_FILE_CAPACITY_CEILING_BYTES
        )
        self._wal_capacity_fault_reason: str | None = (
            "WAL_FILE_CAPACITY_CEILING_EXCEEDED_AT_STARTUP"
            if self._wal_capacity_fault_latched else None
        )
        # The writer's hot admission path reads this immutable replacement
        # snapshot without acquiring the maintenance condition. The worker
        # never mutates a published mapping; it replaces it while holding its
        # own condition, so capacity telemetry cannot convoy every callback
        # behind checkpoint bookkeeping.
        self._checkpoint_state_snapshot: dict[str, object] = {}
        with self._checkpoint_condition:
            self._publish_passive_checkpoint_state_locked()
        self._sample_database_capacity_locked(time.perf_counter(), force=True)
        self._shutdown_receipt: dict[str, object] | None = None
        self._next_barrier_token = 0
        self._last_barrier_token: int | None = None
        self._last_barrier_sequence: int | None = None
        self._last_barrier_wait_seconds: float | None = None
        self._deferred_thread = threading.Thread(
            target=self._deferred_writer,
            name="LaneIIIPaperLedgerWriter",
            daemon=True,
        )
        self._checkpoint_thread = threading.Thread(
            target=self._passive_checkpoint_worker,
            name="LaneIIIPaperLedgerCheckpoint",
            daemon=True,
        )
        self._deferred_thread.start()
        self._checkpoint_thread.start()

    def set_session_context(self, context: PaperSessionContext) -> None:
        """Set the default envelope for asynchronous paper-path records."""
        if type(context) is not PaperSessionContext:
            raise ValueError("Paper ledger session context must be immutable and exact.")
        with self._session_context_lock:
            self._current_session_context = context

    def _deferred_backlog_depth_locked(self) -> int:
        return self._deferred_record_count + self._deferred_inflight_record_count

    def _deferred_work_item_depth_locked(self) -> int:
        """Return every bounded item held by the writer, including barriers."""
        return self._deferred_backlog_depth_locked() + self._deferred_barrier_count

    @staticmethod
    def _trim_rate_buckets_locked(buckets: deque[tuple[int, int]], now: float) -> None:
        cutoff = int(now - _WRITER_TELEMETRY_WINDOW_SECONDS) - 1
        while buckets and buckets[0][0] < cutoff:
            buckets.popleft()

    def _record_rate_locked(self, buckets: deque[tuple[int, int]], now: float, count: int) -> None:
        if count <= 0:
            return
        second = int(now)
        if buckets and buckets[-1][0] == second:
            buckets[-1] = (second, buckets[-1][1] + count)
        else:
            buckets.append((second, count))
        self._trim_rate_buckets_locked(buckets, now)

    def _rate_locked(self, buckets: deque[tuple[int, int]], now: float) -> tuple[float, float]:
        self._trim_rate_buckets_locked(buckets, now)
        if not buckets:
            return 0.0, 0.0
        elapsed = max(1.0, min(_WRITER_TELEMETRY_WINDOW_SECONDS, now - buckets[0][0]))
        return sum(count for _, count in buckets) / elapsed, elapsed

    def _record_queue_depth_locked(self, now: float) -> None:
        depth = self._deferred_backlog_depth_locked()
        if (
            not self._queue_depth_samples
            or now - self._queue_depth_samples[-1][0] >= 0.25
            or depth == 0
        ):
            self._queue_depth_samples.append((now, depth))
        cutoff = now - _WRITER_TELEMETRY_WINDOW_SECONDS
        while len(self._queue_depth_samples) > 1 and self._queue_depth_samples[0][0] < cutoff:
            self._queue_depth_samples.popleft()

    def _queue_growth_rate_locked(self, now: float) -> float:
        self._record_queue_depth_locked(now)
        if len(self._queue_depth_samples) < 2:
            return 0.0
        first_at, first_depth = self._queue_depth_samples[0]
        last_at, last_depth = self._queue_depth_samples[-1]
        elapsed = last_at - first_at
        return 0.0 if elapsed <= 0 else (last_depth - first_depth) / elapsed

    def _oldest_deferred_admitted_at_locked(self) -> float | None:
        oldest = self._deferred_inflight_oldest_admitted_at
        pending = self._deferred_pending_admitted_at[0] if self._deferred_pending_admitted_at else None
        if oldest is None:
            return pending
        return oldest if pending is None else min(oldest, pending)

    def _current_database_size_bytes(self) -> int | None:
        try:
            return self.path.stat().st_size
        except OSError:
            return None

    def _current_database_free_bytes(self) -> int | None:
        try:
            return shutil.disk_usage(self.path.parent).free
        except OSError:
            return None

    def _sample_database_capacity_locked(self, now: float, *, force: bool = False) -> dict[str, object]:
        """Sample bounded authority-ledger runway without statting every callback."""
        if (
            force
            or self._database_capacity_last_sample_at is None
            or now - self._database_capacity_last_sample_at >= _AUTHORITY_LEDGER_CAPACITY_SAMPLE_SECONDS
        ):
            self._database_capacity_last_sample_at = now
            self._database_size_bytes = self._current_database_size_bytes()
            self._database_free_bytes = self._current_database_free_bytes()
            if self._database_size_bytes is not None:
                self._database_growth_samples.append((now, self._database_size_bytes))
            cutoff = now - _AUTHORITY_LEDGER_GROWTH_WINDOW_SECONDS
            while (
                len(self._database_growth_samples) > 1
                and self._database_growth_samples[0][0] < cutoff
            ):
                self._database_growth_samples.popleft()

        growth = 0.0
        if len(self._database_growth_samples) >= 2:
            first_at, first_size = self._database_growth_samples[0]
            last_at, last_size = self._database_growth_samples[-1]
            elapsed = last_at - first_at
            if elapsed > 0:
                growth = max(0.0, (last_size - first_size) / elapsed)

        size_headroom = (
            None if self._database_size_bytes is None
            else max(0, _AUTHORITY_LEDGER_CAPACITY_BYTES - self._database_size_bytes)
        )
        disk_headroom = (
            None if self._database_free_bytes is None
            else max(0, self._database_free_bytes - _AUTHORITY_LEDGER_DISK_MINIMUM_FREE_BYTES)
        )
        headroom_candidates = [value for value in (size_headroom, disk_headroom) if value is not None]
        effective_headroom = min(headroom_candidates) if headroom_candidates else None
        runway_seconds = (
            None if growth <= 0 or effective_headroom is None
            else effective_headroom / growth
        )

        current_fault_reason = None
        if (
            self._database_size_bytes is not None
            and self._database_size_bytes >= _AUTHORITY_LEDGER_CAPACITY_BYTES
        ):
            current_fault_reason = "AUTHORITY_LEDGER_DATABASE_CAPACITY_EXCEEDED"
        elif (
            self._database_free_bytes is not None
            and self._database_free_bytes <= _AUTHORITY_LEDGER_DISK_MINIMUM_FREE_BYTES
        ):
            current_fault_reason = "AUTHORITY_LEDGER_DISK_RUNWAY_EXHAUSTED"
        if current_fault_reason is not None:
            self._database_capacity_fault_latched = True
            self._database_capacity_fault_reason = current_fault_reason

        warning_reasons: list[str] = []
        if (
            self._database_size_bytes is not None
            and self._database_size_bytes >= _AUTHORITY_LEDGER_WARNING_BYTES
        ):
            warning_reasons.append("AUTHORITY_LEDGER_DATABASE_SIZE_WARNING")
        if (
            self._database_free_bytes is not None
            and self._database_free_bytes <= _AUTHORITY_LEDGER_DISK_WARNING_FREE_BYTES
        ):
            warning_reasons.append("AUTHORITY_LEDGER_DISK_FREE_WARNING")
        if runway_seconds is not None and runway_seconds <= _AUTHORITY_LEDGER_RUNWAY_WARNING_SECONDS:
            warning_reasons.append("AUTHORITY_LEDGER_RUNWAY_UNDER_24_HOURS")

        return {
            "database_size_bytes": self._database_size_bytes,
            "database_warning_bytes": _AUTHORITY_LEDGER_WARNING_BYTES,
            "database_capacity_bytes": _AUTHORITY_LEDGER_CAPACITY_BYTES,
            "database_free_bytes": self._database_free_bytes,
            "database_disk_warning_free_bytes": _AUTHORITY_LEDGER_DISK_WARNING_FREE_BYTES,
            "database_disk_minimum_free_bytes": _AUTHORITY_LEDGER_DISK_MINIMUM_FREE_BYTES,
            "database_growth_bytes_per_second": round(growth, 3),
            "database_effective_headroom_bytes": effective_headroom,
            "database_runway_seconds": None if runway_seconds is None else round(runway_seconds, 3),
            "database_runway_state": (
                "EXHAUSTED" if self._database_capacity_fault_latched
                else "WARNING" if warning_reasons else "HEALTHY"
            ),
            "database_capacity_fault_latched": self._database_capacity_fault_latched,
            "database_capacity_fault_reason": self._database_capacity_fault_reason,
            "database_warning_reasons": warning_reasons,
        }

    def _capacity_snapshot_locked(self, now: float) -> dict[str, object]:
        """Return bounded O(1) writer capacity facts while the queue is stable."""
        checkpoint_state = self._passive_checkpoint_snapshot()
        checkpoint_error = checkpoint_state["passive_checkpoint_worker_error"]
        wal_capacity_fault = checkpoint_state["wal_capacity_fault_latched"] is True
        database_capacity = self._sample_database_capacity_locked(now)
        database_capacity_fault = database_capacity["database_capacity_fault_latched"] is True
        backlog = self._deferred_backlog_depth_locked()
        pending_barriers = self._deferred_barrier_count
        barrier_capacity_exhausted = pending_barriers >= self._max_pending_barriers
        admitted_rate, admitted_window = self._rate_locked(self._admission_rate_buckets, now)
        durable_rate, durable_window = self._rate_locked(self._durable_rate_buckets, now)
        measurement_window = max(admitted_window, durable_window)
        headroom = durable_rate - admitted_rate
        queue_growth = self._queue_growth_rate_locked(now)
        oldest = self._oldest_deferred_admitted_at_locked()
        oldest_age = None if oldest is None else max(0.0, now - oldest)
        enough_history = measurement_window >= _DEFERRED_HEADROOM_GRACE_SECONDS
        negative_now = backlog > 0 and enough_history and headroom < 0
        if negative_now:
            if self._negative_headroom_since is None:
                self._negative_headroom_since = now
        else:
            self._negative_headroom_since = None
        negative_sustained = (
            self._negative_headroom_since is not None
            and now - self._negative_headroom_since >= _DEFERRED_HEADROOM_GRACE_SECONDS
        )
        if backlog >= self._max_deferred_records:
            self._capacity_fault_latched = True
        if self._deferred_error is not None:
            state = "FAILED"
        elif self._deferred_stopping or not self._admission_open:
            state = "SHUTTING_DOWN"
        elif self._capacity_fault_latched or wal_capacity_fault or database_capacity_fault:
            state = "EXHAUSTED"
        elif (
            backlog >= self._degraded_queue_depth
            or barrier_capacity_exhausted
            or negative_sustained
            or checkpoint_error is not None
            or database_capacity["database_warning_reasons"]
        ):
            state = "DEGRADED"
        else:
            state = "HEALTHY"
        if state == "HEALTHY":
            self._capacity_degraded_since = None
        elif self._capacity_degraded_since is None:
            self._capacity_degraded_since = now
        admission_open = (
            self._admission_open
            and self._deferred_error is None
            and not self._deferred_stopping
            and not self._capacity_fault_latched
            and not wal_capacity_fault
            and not database_capacity_fault
            and state == "HEALTHY"
        )
        return {
            "schema": "l3g-ledger-writer-capacity-v1",
            "state": state,
            "admission_open": admission_open,
            "capacity_fault_latched": self._capacity_fault_latched,
            "wal_capacity_fault_latched": wal_capacity_fault,
            "wal_capacity_fault_reason": checkpoint_state["wal_capacity_fault_reason"],
            "wal_size_bytes": checkpoint_state["wal_size_bytes"],
            "wal_uncheckpointed_bytes": checkpoint_state["wal_uncheckpointed_bytes"],
            "wal_uncheckpointed_capacity_ceiling_bytes": checkpoint_state[
                "wal_uncheckpointed_capacity_ceiling_bytes"
            ],
            "wal_file_capacity_ceiling_bytes": checkpoint_state["wal_file_capacity_ceiling_bytes"],
            **database_capacity,
            "negative_headroom_sustained": negative_sustained,
            "admitted_records_per_second": round(admitted_rate, 3),
            "durable_records_per_second": round(durable_rate, 3),
            "headroom_records_per_second": round(headroom, 3),
            "measurement_window_seconds": round(measurement_window, 3),
            "queue_depth": backlog,
            "pending_queue_depth": self._deferred_record_count,
            "inflight_queue_depth": self._deferred_inflight_record_count,
            "deferred_work_item_depth": self._deferred_work_item_depth_locked(),
            "pending_barrier_count": pending_barriers,
            "max_pending_barriers": self._max_pending_barriers,
            "barrier_capacity_exhausted": barrier_capacity_exhausted,
            "barrier_rejections_total": self._barrier_rejections_total,
            "barrier_queue_high_water": self._deferred_barrier_high_water,
            "passive_checkpoint_active": checkpoint_state["passive_checkpoint_active"],
            "passive_checkpoint_pending": checkpoint_state["passive_checkpoint_pending"],
            "passive_checkpoint_worker_error": checkpoint_error,
            "queue_growth_records_per_second": round(queue_growth, 3),
            "oldest_queued_record_age_seconds": None if oldest_age is None else round(oldest_age, 6),
            "max_deferred_records": self._max_deferred_records,
            "degraded_queue_depth": self._degraded_queue_depth,
            "admitted_records_total": self._admitted_records_total,
            "durable_records_total": self._durable_records_total,
            "admission_rejections_total": self._admission_rejections_total,
            "high_frequency_persistence_enabled": self._persist_high_frequency_records,
            "suppressed_high_frequency_records_total": self._suppressed_high_frequency_records,
            "suppressed_high_frequency_records_by_domain": dict(
                self._suppressed_high_frequency_by_domain
            ),
            "writer_error": None if self._deferred_error is None else type(self._deferred_error).__name__,
            "degraded_since_seconds": (
                None if self._capacity_degraded_since is None
                else round(max(0.0, now - self._capacity_degraded_since), 6)
            ),
        }

    @staticmethod
    def _percentile(values: list[float], percentile: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int((len(ordered) * percentile + 0.999999) - 1)))
        return ordered[index]

    def _writer_telemetry_locked(self, now: float) -> dict[str, object]:
        capacity = self._capacity_snapshot_locked(now)
        checkpoint_state = self._passive_checkpoint_snapshot()
        batches = [dict(batch) for batch in self._writer_batches]
        batch_sizes = [float(batch["batch_size"]) for batch in batches if isinstance(batch.get("batch_size"), int)]
        transaction_seconds = [
            float(batch["transaction_seconds"])
            for batch in batches if isinstance(batch.get("transaction_seconds"), (int, float))
        ]
        return {
            **capacity,
            "capacity_state": capacity["state"],
            "sampled_batches": len(batches),
            "average_batch_size": None if not batch_sizes else round(sum(batch_sizes) / len(batch_sizes), 3),
            "p95_batch_size": self._percentile(batch_sizes, 0.95),
            "average_transaction_seconds": (
                None if not transaction_seconds else round(sum(transaction_seconds) / len(transaction_seconds), 6)
            ),
            "p95_transaction_seconds": self._percentile(transaction_seconds, 0.95),
            "last_checkpoint": None if self._last_checkpoint is None else dict(self._last_checkpoint),
            "last_passive_checkpoint": checkpoint_state["last_passive_checkpoint"],
            "recent_batches": batches,
        }

    def deferred_capacity(self) -> dict[str, object]:
        """Return an atomic capacity snapshot without touching SQLite or runtime locks."""
        with self._deferred_condition:
            return self._capacity_snapshot_locked(time.perf_counter())

    @property
    def ledger_identity(self) -> str:
        return self._ledger_uuid

    def capacity_allows_authority(self) -> bool:
        """Fail closed for new authority when durable writer headroom is inadequate."""
        with self._deferred_condition:
            capacity = self._capacity_snapshot_locked(time.perf_counter())
            return deferred_capacity_allows_authority(capacity)

    @contextmanager
    def authority_capacity_fence(self) -> Iterator[dict[str, object]]:
        """Keep a healthy capacity observation and authority writes in one order.

        The fence never touches SQLite while it checks capacity.  It merely
        prevents a deferred producer from racing a just-approved authority
        sequence into a saturated queue before that authority sequence is
        durably recorded.
        """
        with self._ordering_lock:
            with self._deferred_condition:
                capacity = self._capacity_snapshot_locked(time.perf_counter())
                if not deferred_capacity_allows_authority(capacity):
                    raise LedgerCapacityError(
                        "Ledger writer capacity is inadequate for authority operations.", capacity,
                    )
            yield capacity

    def shutdown_status(self) -> dict[str, object] | None:
        with self._deferred_condition:
            return None if self._shutdown_receipt is None else dict(self._shutdown_receipt)

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
    def _domain_transaction(
        self, domain: str, metrics: dict[str, float] | None = None,
    ) -> Iterator[sqlite3.Connection]:
        self._set_synchronous_mode(domain)
        with self._transaction(metrics) as connection:
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
            # This additive side table keeps generated-vs-explicit timestamp
            # provenance out of the immutable audit payload/hash contract.
            # Historical rows without a provenance row are treated
            # conservatively as exact-content-only for idempotency purposes.
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS lane_iii_paper_idempotency_origin (
                    identity TEXT PRIMARY KEY,
                    generated_occurred_at INTEGER NOT NULL CHECK (generated_occurred_at IN (0, 1))
                )
                """
            )
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
        classified_through_hash: str | None,
        last_authority_mutation: tuple[int, str | None, str | None, str | None] = (0, None, None, None),
        last_authority_observation: tuple[int, str | None, str | None, str | None] = (0, None, None, None),
        last_unknown: tuple[int, str | None, str | None, str | None] = (0, None, None, None),
        safe_classification_last_sequences: Mapping[str, int] | None = None,
        updated_at: str,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "policy_version": COMMISSIONING_TAIL_POLICY_VERSION,
            "classified_through_sequence": classified_through_sequence,
            "classified_through_hash": classified_through_hash,
            "safe_classification_last_sequences": dict(sorted((safe_classification_last_sequences or {}).items())),
            "updated_at": updated_at,
        }
        for prefix, values in zip(
            _WATERMARK_EVENT_PREFIXES,
            (last_authority_mutation, last_authority_observation, last_unknown),
            strict=True,
        ):
            sequence, kind, domain, record_hash = values
            payload.update({
                f"{prefix}_sequence": sequence,
                f"{prefix}_kind": kind,
                f"{prefix}_domain": domain,
                f"{prefix}_hash": record_hash,
            })
        return payload

    @staticmethod
    def _stored_record_classification(row: sqlite3.Row) -> CommissioningTailClassification:
        try:
            document = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError:
            return CommissioningTailClassification(
                CommissioningTailCategory.UNKNOWN, "UNKNOWN:MALFORMED_STORED_PAYLOAD",
            )
        payload = document.get("payload") if isinstance(document, Mapping) else None
        if not isinstance(payload, Mapping):
            return CommissioningTailClassification(
                CommissioningTailCategory.UNKNOWN, "UNKNOWN:MALFORMED_STORED_PAYLOAD",
            )
        return commissioning_tail_classification(str(row["domain"]), str(row["kind"]), payload)

    @staticmethod
    def _event_tuple(watermark: Mapping[str, object], prefix: str) -> tuple[int, str | None, str | None, str | None]:
        return (
            int(watermark.get(f"{prefix}_sequence") or 0),
            watermark.get(f"{prefix}_kind") if isinstance(watermark.get(f"{prefix}_kind"), str) else None,
            watermark.get(f"{prefix}_domain") if isinstance(watermark.get(f"{prefix}_domain"), str) else None,
            watermark.get(f"{prefix}_hash") if isinstance(watermark.get(f"{prefix}_hash"), str) else None,
        )

    @staticmethod
    def _set_event(
        watermark: dict[str, object], prefix: str, row: sqlite3.Row | Mapping[str, object],
    ) -> None:
        watermark.update({
            f"{prefix}_sequence": int(row["ledger_sequence"]),
            f"{prefix}_kind": str(row["kind"]),
            f"{prefix}_domain": str(row["domain"]),
            f"{prefix}_hash": str(row["record_hash"]),
        })

    @classmethod
    def _apply_stored_classification(
        cls,
        watermark: dict[str, object],
        row: sqlite3.Row | Mapping[str, object],
        classification: CommissioningTailClassification,
    ) -> None:
        sequence = int(row["ledger_sequence"])
        if classification.accepted_without_verification:
            safe_last = dict(watermark.get("safe_classification_last_sequences") or {})
            safe_last[classification.shape] = sequence
            watermark["safe_classification_last_sequences"] = safe_last
        if classification.category is CommissioningTailCategory.AUTHORITY_OBSERVATION:
            cls._set_event(watermark, "last_authority_observation", row)
        elif classification.category is CommissioningTailCategory.AUTHORITY_MUTATION:
            cls._set_event(watermark, "last_authority_mutation", row)
        elif classification.category is CommissioningTailCategory.UNKNOWN:
            cls._set_event(watermark, "last_unknown", row)

    def _row_matches_event(self, candidate: Mapping[str, object], prefix: str, classified: int) -> bool:
        sequence = candidate.get(f"{prefix}_sequence")
        kind = candidate.get(f"{prefix}_kind")
        domain = candidate.get(f"{prefix}_domain")
        record_hash = candidate.get(f"{prefix}_hash")
        if type(sequence) is not int or not 0 <= sequence <= classified:
            return False
        if sequence == 0:
            return kind is None and domain is None and record_hash is None
        if not all(isinstance(value, str) and value for value in (kind, domain, record_hash)):
            return False
        row = self._connection.execute(
            "SELECT kind, domain, record_hash FROM lane_iii_paper_audit WHERE ledger_sequence=?",
            (sequence,),
        ).fetchone()
        return row is not None and (
            str(row["kind"]), str(row["domain"]), str(row["record_hash"]),
        ) == (kind, domain, record_hash)

    @staticmethod
    def _legacy_v2_safe_classification(row: sqlite3.Row) -> str | None:
        try:
            document = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError:
            return None
        payload = document.get("payload") if isinstance(document, Mapping) else None
        if not isinstance(payload, Mapping) or any(key in payload for key in _AUTHORITY_SHAPED_PAYLOAD_KEYS):
            return None
        domain, kind = str(row["domain"]), str(row["kind"])
        if domain == "OBSERVATION" and kind == "OBSERVATION_ENVELOPE":
            observation_type = payload.get("observation_type")
            required = (
                payload.get("observation_id"), payload.get("local_monotonic_sequence"),
                payload.get("source_payload_hash"),
            )
            if (
                isinstance(observation_type, str)
                and observation_type in _PASSIVE_MARKET_OBSERVATION_TYPES
                and isinstance(required[0], str) and type(required[1]) is int
                and isinstance(required[2], str)
            ):
                return f"OBSERVATION:{kind}:{observation_type}"
            if (
                observation_type == "ACCOUNT"
                and isinstance(required[0], str) and type(required[1]) is int
                and isinstance(required[2], str)
                and payload.get("authority_effect") == COMMISSIONING_NO_AUTHORITY_EFFECT
                and payload.get("observation_semantics") == "INFORMATIONAL_ACCOUNT_ITEM"
                and payload.get("observation_payload_keys") == ["item", "value"]
                and isinstance(payload.get("observation_account_alias"), str)
                and isinstance(payload.get("observation_account_class"), str)
                and (
                    payload.get("observation_account_alias"), payload.get("observation_account_class"),
                ) in {
                    ("Sim101", "LOCAL_SIMULATION"),
                    ("Lucid25kflex01", "PROVIDER_EVALUATION"),
                }
            ):
                return "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE"
        elif domain == "EVIDENCE" and kind == "EVIDENCE" and (
            isinstance(payload.get("evidence_id"), str)
            and isinstance(payload.get("family"), str)
            and payload.get("family") in _PASSIVE_EVIDENCE_FAMILIES
            and payload.get("scientific_eligibility") is False
            and payload.get("book_completeness") == "UNVERIFIED"
            and payload.get("sequence_authority") == "LOCAL_CALLBACK_ORDER_ONLY"
        ):
            return "EVIDENCE:EVIDENCE"
        elif domain == "DECISION" and kind == "DECISION":
            decision = payload.get("decision")
            if (
                isinstance(decision, str)
                and decision in _PASSIVE_DECISIONS
                and payload.get("direction") == {
                    "NO_TRADE": "FLAT", "LONG": "LONG", "SHORT": "SHORT", "EXIT": "FLAT",
                }.get(str(decision))
                and payload.get("authority_effect") == COMMISSIONING_NO_AUTHORITY_EFFECT
                and payload.get("commissioning") is False
                and payload.get("strategy_generated") is True
                and payload.get("scientific_evidence") is False
                and payload.get("scientific_eligibility") is False
                and isinstance(payload.get("paper_decision_id"), str)
            ):
                return f"DECISION:DECISION:{decision}:AUTHORITY_EFFECT_NONE"
        return None

    def _safe_map_is_valid(
        self, safe_last: object, classified: int, *, legacy_v2: bool,
    ) -> bool:
        if not isinstance(safe_last, dict) or not all(
            isinstance(key, str)
            and key in _V2_SAFE_CLASSIFICATIONS | _V3_AUTHORITY_OBSERVATION_CLASSIFICATIONS
            and type(value) is int and 0 < value <= classified
            for key, value in safe_last.items()
        ):
            return False
        for shape, sequence in safe_last.items():
            row = self._connection.execute(
                "SELECT ledger_sequence, domain, kind, payload_json, record_hash "
                "FROM lane_iii_paper_audit WHERE ledger_sequence=?",
                (sequence,),
            ).fetchone()
            if row is None:
                return False
            if legacy_v2:
                if self._legacy_v2_safe_classification(row) != shape:
                    return False
            else:
                classification = self._stored_record_classification(row)
                if classification.shape != shape and self._legacy_v2_safe_classification(row) != shape:
                    return False
        return True

    def _legacy_only_safe_boundary(self, safe_last: object) -> int:
        """Return the newest v2-subset-safe row not accepted by exact v3."""
        if not isinstance(safe_last, dict):
            return 0
        boundary = 0
        for shape, sequence in safe_last.items():
            row = self._connection.execute(
                "SELECT ledger_sequence, domain, kind, payload_json, record_hash "
                "FROM lane_iii_paper_audit WHERE ledger_sequence=?",
                (sequence,),
            ).fetchone()
            if row is None:
                continue
            classification = self._stored_record_classification(row)
            if not classification.accepted_without_verification or classification.shape != shape:
                boundary = max(boundary, int(sequence))
        return boundary

    def _v3_watermark_is_valid(self, candidate: object) -> bool:
        if (
            not isinstance(candidate, dict)
            or set(candidate) != _V3_WATERMARK_KEYS
            or candidate.get("policy_version") != COMMISSIONING_TAIL_POLICY_VERSION
            or not _is_utc(candidate.get("updated_at"))
        ):
            return False
        classified = candidate.get("classified_through_sequence")
        classified_hash = candidate.get("classified_through_hash")
        if type(classified) is not int or not 0 <= classified <= self._highest_sequence:
            return False
        if classified == 0:
            if classified_hash is not None:
                return False
        else:
            row = self._connection.execute(
                "SELECT record_hash FROM lane_iii_paper_audit WHERE ledger_sequence=?", (classified,),
            ).fetchone()
            if row is None or str(row["record_hash"]) != classified_hash:
                return False
        safe_last = candidate.get("safe_classification_last_sequences")
        return (
            all(self._row_matches_event(candidate, prefix, classified) for prefix in _WATERMARK_EVENT_PREFIXES)
            and self._safe_map_is_valid(safe_last, classified, legacy_v2=False)
            and int(candidate["last_unknown_sequence"]) >= self._legacy_only_safe_boundary(safe_last)
        )

    def _v2_watermark_is_valid(self, candidate: object) -> bool:
        if (
            not isinstance(candidate, dict)
            or set(candidate) != _V2_WATERMARK_KEYS
            or candidate.get("policy_version") != _COMMISSIONING_TAIL_PREVIOUS_POLICY_VERSION
            or not _is_utc(candidate.get("updated_at"))
        ):
            return False
        classified = candidate.get("classified_through_sequence")
        return (
            type(classified) is int
            and 0 <= classified <= self._highest_sequence
            and self._row_matches_event(candidate, "last_authority_mutation", classified)
            and self._safe_map_is_valid(
                candidate.get("safe_classification_last_sequences"), classified, legacy_v2=True,
            )
        )

    def _migrate_v2_watermark(self, candidate: Mapping[str, object]) -> dict[str, object]:
        classified = int(candidate["classified_through_sequence"])
        classified_hash: str | None = None
        migration_boundary: tuple[int, str | None, str | None, str | None] = (
            0, None, None, None,
        )
        if classified:
            row = self._connection.execute(
                "SELECT ledger_sequence, kind, domain, record_hash "
                "FROM lane_iii_paper_audit WHERE ledger_sequence=?", (classified,),
            ).fetchone()
            classified_hash = None if row is None else str(row["record_hash"])
            if row is not None:
                migration_boundary = (
                    int(row["ledger_sequence"]), str(row["kind"]),
                    str(row["domain"]), str(row["record_hash"]),
                )
        safe_last = dict(candidate.get("safe_classification_last_sequences") or {})
        observation: tuple[int, str | None, str | None, str | None] = (0, None, None, None)
        account_shape = "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE"
        account_sequence = safe_last.get(account_shape)
        if type(account_sequence) is int and account_sequence > 0:
            row = self._connection.execute(
                "SELECT ledger_sequence, kind, domain, record_hash FROM lane_iii_paper_audit WHERE ledger_sequence=?",
                (account_sequence,),
            ).fetchone()
            if row is not None:
                observation = (
                    int(row["ledger_sequence"]), str(row["kind"]), str(row["domain"]), str(row["record_hash"]),
                )
        # v2 intentionally collapsed every rejected record into one mutation
        # watermark and accepted record subsets that are weaker than v3's
        # exact shapes. The classified v2 cursor is therefore one conservative
        # UNKNOWN umbrella: it retains and dominates the old unsafe boundary
        # until Auto verifies the entire inherited region.
        legacy_unsafe = self._event_tuple(candidate, "last_authority_mutation")
        if migration_boundary[0] < legacy_unsafe[0]:
            migration_boundary = legacy_unsafe
        return self._watermark_payload(
            classified_through_sequence=classified,
            classified_through_hash=classified_hash,
            last_authority_observation=observation,
            last_unknown=migration_boundary,
            safe_classification_last_sequences=safe_last,
            updated_at=_now(),
        )

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

    def _advance_authority_watermark(self, watermark: dict[str, object]) -> dict[str, object]:
        classified = int(watermark["classified_through_sequence"])
        if classified == self._highest_sequence:
            return watermark
        if self._highest_sequence - classified > _COMMISSIONING_WATERMARK_SCAN_LIMIT:
            # A stale writer may have left an arbitrarily large suffix. One
            # exact tip lookup is enough to deny commissioning conservatively;
            # startup must never walk a 13M-row production ledger.
            row = self._connection.execute(
                "SELECT ledger_sequence, domain, kind, payload_json, record_hash "
                "FROM lane_iii_paper_audit ORDER BY ledger_sequence DESC LIMIT 1"
            ).fetchone()
            if row is not None:
                self._set_event(watermark, "last_unknown", row)
        else:
            rows = self._connection.execute(
                "SELECT ledger_sequence, domain, kind, payload_json, record_hash "
                "FROM lane_iii_paper_audit WHERE ledger_sequence > ? ORDER BY ledger_sequence ASC",
                (classified,),
            ).fetchall()
            for row in rows:
                self._apply_stored_classification(watermark, row, self._stored_record_classification(row))
        watermark.update({
            "policy_version": COMMISSIONING_TAIL_POLICY_VERSION,
            "classified_through_sequence": self._highest_sequence,
            "classified_through_hash": self._final_record_hash,
            "safe_classification_last_sequences": dict(sorted(
                dict(watermark.get("safe_classification_last_sequences") or {}).items()
            )),
            "updated_at": _now(),
        })
        return watermark

    def _rebuild_authority_watermark(self) -> dict[str, object]:
        """Bounded fail-closed recovery for absent or invalid metadata."""
        watermark = self._watermark_payload(
            classified_through_sequence=0,
            classified_through_hash=None,
            updated_at=_now(),
        )
        rows = self._connection.execute(
            "SELECT ledger_sequence, domain, kind, payload_json, record_hash "
            "FROM lane_iii_paper_audit ORDER BY ledger_sequence DESC LIMIT ?",
            (_COMMISSIONING_WATERMARK_SCAN_LIMIT,),
        ).fetchall()
        if rows:
            oldest_scanned = int(rows[-1]["ledger_sequence"])
            predecessor = self._connection.execute(
                "SELECT ledger_sequence, domain, kind, payload_json, record_hash "
                "FROM lane_iii_paper_audit WHERE ledger_sequence < ? "
                "ORDER BY ledger_sequence DESC LIMIT 1",
                (oldest_scanned,),
            ).fetchone()
            if predecessor is not None:
                self._set_event(watermark, "last_unknown", predecessor)
            for row in reversed(rows):
                self._apply_stored_classification(watermark, row, self._stored_record_classification(row))
        watermark.update({
            "classified_through_sequence": self._highest_sequence,
            "classified_through_hash": self._final_record_hash,
            "safe_classification_last_sequences": dict(sorted(
                dict(watermark.get("safe_classification_last_sequences") or {}).items()
            )),
            "updated_at": _now(),
        })
        return watermark

    def _load_or_rebuild_authority_watermark(self, serialized: str | None) -> dict[str, object]:
        candidate: object = None
        if serialized:
            try:
                candidate = json.loads(serialized)
            except json.JSONDecodeError:
                candidate = None
        if self._v3_watermark_is_valid(candidate):
            watermark = dict(candidate)  # type: ignore[arg-type]
        elif self._v2_watermark_is_valid(candidate):
            watermark = self._migrate_v2_watermark(candidate)  # type: ignore[arg-type]
        else:
            watermark = self._rebuild_authority_watermark()
        watermark = self._advance_authority_watermark(watermark)
        if candidate != watermark:
            with self._transaction() as connection:
                self._store_authority_watermark(connection, watermark)
        return watermark

    @contextmanager
    def _transaction(self, metrics: dict[str, float] | None = None) -> Iterator[sqlite3.Connection]:
        transaction_started = time.perf_counter() if metrics is not None else 0.0
        begin_started = time.perf_counter() if metrics is not None else 0.0
        self._connection.execute("BEGIN IMMEDIATE")
        if metrics is not None:
            metrics["begin_seconds"] = time.perf_counter() - begin_started
        try:
            yield self._connection
        except Exception:
            self._connection.rollback()
            raise
        else:
            commit_started = time.perf_counter() if metrics is not None else 0.0
            try:
                self._connection.commit()
            except Exception:
                self._connection.rollback()
                raise
            if metrics is not None:
                metrics["commit_seconds"] = time.perf_counter() - commit_started
                metrics["transaction_seconds"] = time.perf_counter() - transaction_started

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
            with self._deferred_condition:
                if not self._admission_open or self._deferred_stopping:
                    raise RuntimeError("Paper ledger admission is sealed for controlled shutdown.")
            self.flush_deferred(timeout_seconds=30.0)
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
        with self._session_context_lock:
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
        context = _cached_session_context(
            session_kind.value, session_id, trade_date, profile_hash,
            generation, PaperCalendarState.NORMAL.value,
        )
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
            "paper_policy_hash": _PAPER_POLICY_HASH,
            "risk_profile_hash": _RISK_PROFILE_HASH,
            "account_binding_hash": _ACCOUNT_BINDING_HASH,
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
        # Retain both exact and generated-time retry fingerprints. An explicit
        # event time remains part of identity-conflict detection; only a
        # caller which omitted it can retry with a fresh ledger envelope time.
        idempotency_fingerprint = _idempotency_fingerprint(common)
        retry_fingerprint = _idempotency_fingerprint(common, ignore_occurred_at=True)
        # Preserve the historical automatic identity contract byte-for-byte;
        # only retry comparison intentionally ignores a fresh envelope time.
        record_identity = identity or "l3g-ledger-" + canonical_hash(common)
        return {
            "kind": kind,
            "at": at,
            "domain": domain,
            "common": common,
            "identity": record_identity,
            "idempotency_fingerprint": idempotency_fingerprint,
            "idempotency_retry_fingerprint": retry_fingerprint,
            "idempotency_generated_occurred_at": occurred_at is None,
            "execution_session_id": execution_session_id,
        }

    @staticmethod
    def _stored_idempotency_fingerprint(
        serialized: str, *, ignore_occurred_at: bool = False,
    ) -> str:
        """Recreate the pre-chain document fingerprint from an immutable row."""
        try:
            stored = json.loads(serialized)
        except json.JSONDecodeError as exc:
            raise ValueError("Existing ledger identity has malformed immutable content.") from exc
        if not isinstance(stored, Mapping):
            raise ValueError("Existing ledger identity has malformed immutable content.")
        common = {
            key: value for key, value in stored.items()
            if key not in {"identity", "previous_record_hash", "record_hash"}
        }
        return _idempotency_fingerprint(common, ignore_occurred_at=ignore_occurred_at)

    def _publish_passive_checkpoint_state_locked(self) -> None:
        """Publish a replace-only maintenance snapshot while its condition is held."""
        self._checkpoint_state_snapshot = {
            "passive_checkpoint_active": self._checkpoint_active,
            "passive_checkpoint_pending": self._checkpoint_requested,
            "passive_checkpoint_worker_error": self._checkpoint_worker_error,
            "wal_size_bytes": self._checkpoint_wal_size_bytes,
            "wal_uncheckpointed_bytes": self._checkpoint_uncheckpointed_bytes,
            "wal_uncheckpointed_capacity_ceiling_bytes": _WAL_UNCHECKPOINTED_CAPACITY_CEILING_BYTES,
            "wal_file_capacity_ceiling_bytes": _WAL_FILE_CAPACITY_CEILING_BYTES,
            "wal_capacity_fault_latched": self._wal_capacity_fault_latched,
            "wal_capacity_fault_reason": self._wal_capacity_fault_reason,
            "last_passive_checkpoint": (
                None if self._last_passive_checkpoint is None
                else dict(self._last_passive_checkpoint)
            ),
        }

    def _passive_checkpoint_snapshot(self) -> dict[str, object]:
        """Return a stable, replace-only maintenance snapshot without writer contention."""
        snapshot = self._checkpoint_state_snapshot
        result = dict(snapshot)
        checkpoint = result.get("last_passive_checkpoint")
        if isinstance(checkpoint, Mapping):
            result["last_passive_checkpoint"] = dict(checkpoint)
        return result

    def _current_wal_size_bytes(self) -> int:
        try:
            return Path(str(self.path) + "-wal").stat().st_size
        except OSError:
            return 0

    def _schedule_passive_checkpoint_locked(self) -> None:
        """Request bounded maintenance after enough newly durable rows.

        The deferred condition owns ``_durable_records_total``.  This method
        deliberately only signals the independent worker; it never executes
        SQLite maintenance while the FIFO writer owns the chain lock.
        """
        wal_size = self._current_wal_size_bytes()
        checkpoint_age = time.perf_counter() - self._checkpoint_started_at
        with self._checkpoint_condition:
            self._checkpoint_durable_total = self._durable_records_total
            self._checkpoint_wal_size_bytes = wal_size
            if self._checkpoint_stopping:
                self._publish_passive_checkpoint_state_locked()
                return
            # Let the authenticated observation stream establish its initial
            # normal batch cadence before competing background copy work is
            # eligible. This is a bounded bootstrap delay, not a suppression:
            # the next committed batch after it expires schedules maintenance.
            if checkpoint_age < _WAL_PASSIVE_CHECKPOINT_START_DELAY_SECONDS:
                self._publish_passive_checkpoint_state_locked()
                return
            # Trigger once the WAL has enough new work to amortize its copy,
            # or before the retention target is reached for the first pass.
            # After a complete PASSIVE pass the physical WAL file may retain
            # its allocated size, so size alone is intentionally not used to
            # requeue no-op maintenance forever.
            due_to_records = (
                self._durable_records_total - self._checkpoint_last_requested_durable_total
                >= _WAL_PASSIVE_CHECKPOINT_TRIGGER_RECORDS
            )
            due_to_initial_wal_size = (
                self._checkpoint_last_completed_durable_total == 0
                and wal_size >= _WAL_PASSIVE_CHECKPOINT_TRIGGER_BYTES
            )
            if not due_to_records and not due_to_initial_wal_size:
                self._publish_passive_checkpoint_state_locked()
                return
            self._checkpoint_last_requested_durable_total = self._durable_records_total
            self._checkpoint_requested = True
            self._publish_passive_checkpoint_state_locked()
            self._checkpoint_condition.notify_all()

    def _passive_checkpoint_worker(self) -> None:
        """Run non-blocking WAL copy work on a connection the writer never uses."""
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(str(self.path), isolation_level=None)
            # PASSIVE checkpointing must return a truthful busy result rather
            # than waiting behind an external reader or competing writer.
            connection.execute("PRAGMA busy_timeout=0")
            page_size_row = connection.execute("PRAGMA page_size").fetchone()
            page_size = 4096 if page_size_row is None else int(page_size_row[0])
            last_started = 0.0
            while True:
                with self._checkpoint_condition:
                    while not self._checkpoint_requested and not self._checkpoint_stopping:
                        self._checkpoint_condition.wait()
                    if self._checkpoint_stopping:
                        return
                    # The request bit coalesces writer notifications, while
                    # this short minimum interval prevents a busy stream from
                    # turning maintenance into an unbounded checkpoint loop.
                    while not self._checkpoint_stopping:
                        remaining = (
                            last_started + _WAL_PASSIVE_CHECKPOINT_MIN_INTERVAL_SECONDS
                            - time.perf_counter()
                        )
                        if remaining <= 0:
                            break
                        self._checkpoint_condition.wait(timeout=remaining)
                    if self._checkpoint_stopping:
                        return
                    self._checkpoint_requested = False
                    self._checkpoint_active = True
                    checkpoint_target_durable_total = self._checkpoint_durable_total
                    self._publish_passive_checkpoint_state_locked()
                started = time.perf_counter()
                last_started = started
                result: dict[str, object]
                error: str | None = None
                try:
                    row = connection.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
                    if row is None:
                        raise RuntimeError("SQLite passive checkpoint returned no status row.")
                    busy, log_frames, checkpointed_frames = (int(row[index]) for index in range(3))
                    uncheckpointed_frames = max(0, log_frames - checkpointed_frames)
                    reuse: dict[str, object] | None = None
                    if busy == 0 and uncheckpointed_frames == 0:
                        # A continuously active writer can append between the
                        # PASSIVE copy's snapshot and its completion, preventing
                        # SQLite from resetting the retained WAL allocation even
                        # though every reported frame was copied.  Hold the
                        # existing database lock only for a non-waiting RESTART
                        # fence after the bulk copy.  busy_timeout=0 keeps an
                        # external reader from turning this into an ingest stall;
                        # a busy result remains visible and the physical 1 GiB
                        # backstop still fails closed if reuse cannot be proven.
                        reuse_started = time.perf_counter()
                        with self._lock:
                            reuse_row = connection.execute("PRAGMA wal_checkpoint(RESTART)").fetchone()
                        if reuse_row is None:
                            raise RuntimeError("SQLite WAL reuse checkpoint returned no status row.")
                        reuse_busy, reuse_log, reuse_checkpointed = (
                            int(reuse_row[index]) for index in range(3)
                        )
                        reuse = {
                            "mode": "RESTART",
                            "busy": reuse_busy,
                            "log_frames": reuse_log,
                            "checkpointed_frames": reuse_checkpointed,
                            "complete": reuse_busy == 0 and reuse_log == reuse_checkpointed,
                            "duration_seconds": round(time.perf_counter() - reuse_started, 6),
                        }
                    result = {
                        "mode": "PASSIVE",
                        "busy": busy,
                        "log_frames": log_frames,
                        "checkpointed_frames": checkpointed_frames,
                        "uncheckpointed_frames": uncheckpointed_frames,
                        "uncheckpointed_bytes": uncheckpointed_frames * page_size,
                        "duration_seconds": round(time.perf_counter() - started, 6),
                        "complete": busy == 0 and log_frames == checkpointed_frames,
                        "reuse_checkpoint": reuse,
                    }
                except BaseException as exc:  # keep data admission separate from maintenance telemetry
                    error = f"{type(exc).__name__}: {exc}"
                    result = {
                        "mode": "PASSIVE",
                        "busy": None,
                        "log_frames": None,
                        "checkpointed_frames": None,
                        "uncheckpointed_frames": None,
                        "uncheckpointed_bytes": None,
                        "duration_seconds": round(time.perf_counter() - started, 6),
                        "complete": False,
                        "error": error,
                    }
                wal_size = self._current_wal_size_bytes()
                result["wal_size_bytes"] = wal_size
                result["wal_uncheckpointed_capacity_ceiling_bytes"] = _WAL_UNCHECKPOINTED_CAPACITY_CEILING_BYTES
                result["wal_file_capacity_ceiling_bytes"] = _WAL_FILE_CAPACITY_CEILING_BYTES
                with self._checkpoint_condition:
                    self._checkpoint_active = False
                    self._last_passive_checkpoint = result
                    self._checkpoint_worker_error = error
                    self._checkpoint_wal_size_bytes = wal_size
                    uncheckpointed_bytes = result.get("uncheckpointed_bytes")
                    self._checkpoint_uncheckpointed_bytes = (
                        int(uncheckpointed_bytes)
                        if isinstance(uncheckpointed_bytes, int) else 0
                    )
                    if result.get("complete") is True:
                        self._checkpoint_last_completed_durable_total = max(
                            self._checkpoint_last_completed_durable_total,
                            checkpoint_target_durable_total,
                        )
                    if self._checkpoint_uncheckpointed_bytes > _WAL_UNCHECKPOINTED_CAPACITY_CEILING_BYTES:
                        self._wal_capacity_fault_latched = True
                        self._wal_capacity_fault_reason = "WAL_UNCHECKPOINTED_CAPACITY_CEILING_EXCEEDED"
                    elif wal_size > _WAL_FILE_CAPACITY_CEILING_BYTES:
                        self._wal_capacity_fault_latched = True
                        self._wal_capacity_fault_reason = "WAL_FILE_CAPACITY_CEILING_EXCEEDED"
                    self._publish_passive_checkpoint_state_locked()
                    self._checkpoint_condition.notify_all()
        except BaseException as exc:  # pragma: no cover - host-level SQLite failure
            with self._checkpoint_condition:
                self._checkpoint_active = False
                self._checkpoint_worker_error = f"{type(exc).__name__}: {exc}"
                self._publish_passive_checkpoint_state_locked()
                self._checkpoint_condition.notify_all()
        finally:
            if connection is not None:
                try:
                    connection.close()
                except BaseException:
                    pass

    def _stop_passive_checkpoint_worker(self, *, timeout_seconds: float = 30.0) -> bool:
        """Stop the maintenance connection before final TRUNCATE proof."""
        with self._checkpoint_condition:
            self._checkpoint_stopping = True
            self._checkpoint_requested = False
            self._publish_passive_checkpoint_state_locked()
            self._checkpoint_condition.notify_all()
        self._checkpoint_thread.join(timeout=timeout_seconds)
        return not self._checkpoint_thread.is_alive()

    def _append_prepared(self, records: tuple[dict[str, object], ...]) -> list[str]:
        """Commit one ordered batch and publish its durable tip only after commit."""
        if not records:
            return []
        self._writer_batch_counter += 1
        sampled = self._writer_batch_counter % _WRITER_TELEMETRY_SAMPLE_INTERVAL == 1
        metrics: dict[str, float] | None = {} if sampled else None
        batch_started = time.perf_counter() if sampled else 0.0
        synchronous_domain = (
            "DECISION" if all(self._uses_normal_deferred_durability(record) for record in records)
            else "INCIDENT"
        )
        hashes: list[str] = []
        watermark = dict(self._authority_watermark)
        watermark["safe_classification_last_sequences"] = dict(
            watermark.get("safe_classification_last_sequences") or {}
        )
        pending_counts = dict(self._counts_cache)
        pending_highest_sequence = self._highest_sequence
        pending_last_record_time = self._last_record_time
        pending_final_record_hash = self._final_record_hash
        inserted = False
        inserted_count = 0
        last_external_authority: tuple[int, str] | None = None
        with self._domain_transaction(synchronous_domain, metrics) as connection:
            duplicate_started = time.perf_counter() if metrics is not None else 0.0
            # The side table preserves whether a stored envelope timestamp was
            # generated by the ledger.  That provenance is required to keep
            # an externally supplied timestamp conflict-significant while
            # retaining the historic convenience retry contract for callers
            # that omitted it.  Pre-hotfix rows have no provenance and are
            # therefore treated as exact-content-only.
            existing: dict[str, tuple[str, str, bool]] = {}
            identities = list(dict.fromkeys(str(record["identity"]) for record in records))
            for start in range(0, len(identities), _SQLITE_IN_LOOKUP_CHUNK):
                chunk = identities[start:start + _SQLITE_IN_LOOKUP_CHUNK]
                placeholders = ",".join("?" for _ in chunk)
                rows = connection.execute(
                    """
                    SELECT audit.identity, audit.record_hash, audit.payload_json,
                           origin.generated_occurred_at
                    FROM lane_iii_paper_audit AS audit
                    LEFT JOIN lane_iii_paper_idempotency_origin AS origin
                      ON origin.identity = audit.identity
                    """
                    + f"WHERE audit.identity IN ({placeholders})",
                    tuple(chunk),
                ).fetchall()
                for row in rows:
                    existing[str(row["identity"])] = (
                        str(row["record_hash"]),
                        str(row["payload_json"]),
                        row["generated_occurred_at"] == 1,
                    )
            if metrics is not None:
                metrics["duplicate_lookup_seconds"] = time.perf_counter() - duplicate_started
            prior = connection.execute(
                "SELECT record_hash FROM lane_iii_paper_audit ORDER BY ledger_sequence DESC LIMIT 1"
            ).fetchone()
            previous_hash = None if prior is None else str(prior["record_hash"])
            local_fingerprints: dict[tuple[str, bool], str] = {}
            for record in records:
                record_identity = str(record["identity"])
                incoming_generated_occurred_at = (
                    record.get("idempotency_generated_occurred_at") is True
                )
                duplicate = existing.get(record_identity)
                if duplicate is not None:
                    # Ignore the envelope timestamp only when *both* sides
                    # were automatic.  In particular, an omitted timestamp
                    # must never turn a retry of an explicitly timestamped
                    # external event into a false idempotent match.
                    ignore_occurred_at = (
                        incoming_generated_occurred_at and duplicate[2]
                    )
                    fingerprint = str(
                        record["idempotency_retry_fingerprint"]
                        if ignore_occurred_at else record["idempotency_fingerprint"]
                    )
                    fingerprint_key = (record_identity, ignore_occurred_at)
                    known_fingerprint = local_fingerprints.get(fingerprint_key)
                    if known_fingerprint is None:
                        known_fingerprint = self._stored_idempotency_fingerprint(
                            duplicate[1], ignore_occurred_at=ignore_occurred_at,
                        )
                        local_fingerprints[fingerprint_key] = known_fingerprint
                    if known_fingerprint != fingerprint:
                        raise ValueError(
                            "Paper ledger identity conflicts with an existing immutable record."
                        )
                    hashes.append(duplicate[0])
                    continue
                domain = str(record["domain"])
                kind = str(record["kind"])
                at = str(record["at"])
                execution_session_id = record["execution_session_id"]
                hashing_started = time.perf_counter() if metrics is not None else 0.0
                common = dict(record["common"])  # type: ignore[arg-type]
                chained = {**common, "identity": record_identity, "previous_record_hash": previous_hash}
                record_hash = canonical_hash(chained)
                final = {**chained, "record_hash": record_hash}
                serialized = json.dumps(final, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
                if metrics is not None:
                    metrics["hash_serialization_seconds"] = metrics.get(
                        "hash_serialization_seconds", 0.0,
                    ) + time.perf_counter() - hashing_started
                audit_started = time.perf_counter() if metrics is not None else 0.0
                cursor = connection.execute(
                    """
                    INSERT INTO lane_iii_paper_audit
                        (identity, domain, kind, occurred_at, execution_session_id, payload_json, previous_record_hash, record_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (record_identity, domain, kind, at, execution_session_id, serialized, previous_hash, record_hash),
                )
                connection.execute(
                    """
                    INSERT INTO lane_iii_paper_idempotency_origin
                        (identity, generated_occurred_at)
                    VALUES (?, ?)
                    """,
                    (record_identity, 1 if incoming_generated_occurred_at else 0),
                )
                if metrics is not None:
                    metrics["audit_insert_seconds"] = metrics.get("audit_insert_seconds", 0.0) + time.perf_counter() - audit_started
                sequence = int(cursor.lastrowid)
                domain_started = time.perf_counter() if metrics is not None else 0.0
                connection.execute(
                    f"INSERT INTO {_DOMAIN_TABLES[domain]} (identity, kind, occurred_at, execution_session_id, payload_json, record_hash) VALUES (?, ?, ?, ?, ?, ?)",
                    (record_identity, kind, at, execution_session_id, serialized, record_hash),
                )
                if metrics is not None:
                    metrics["domain_insert_seconds"] = metrics.get("domain_insert_seconds", 0.0) + time.perf_counter() - domain_started
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
                watermark_started = time.perf_counter() if metrics is not None else 0.0
                inner_payload = common.get("payload")
                classification = commissioning_tail_classification(domain, kind, inner_payload) if isinstance(
                    inner_payload, Mapping
                ) else CommissioningTailClassification(
                    CommissioningTailCategory.UNKNOWN, "UNKNOWN:MALFORMED_STORED_PAYLOAD",
                )
                self._apply_stored_classification(
                    watermark,
                    {
                        "ledger_sequence": sequence,
                        "kind": kind,
                        "domain": domain,
                        "record_hash": record_hash,
                    },
                    classification,
                )
                if metrics is not None:
                    metrics["watermark_seconds"] = metrics.get("watermark_seconds", 0.0) + time.perf_counter() - watermark_started
                if domain in {"COMMAND_RECEIPT", "ORDER_EVENT", "EXECUTION", "POSITION_SNAPSHOT"} or (
                    domain == "INCIDENT" and kind == "INCIDENT_SAFETY_EVENT"
                ) or (
                    domain == "SESSION" and kind == "SESSION_HANDSHAKE"
                ):
                    last_external_authority = (sequence, record_hash)
                watermark.update({
                    "policy_version": COMMISSIONING_TAIL_POLICY_VERSION,
                    "classified_through_sequence": sequence,
                    "classified_through_hash": record_hash,
                    "updated_at": at,
                })
                pending_counts[domain] = pending_counts.get(domain, 0) + 1
                pending_highest_sequence = sequence
                pending_last_record_time = at
                pending_final_record_hash = record_hash
                previous_hash = record_hash
                existing[record_identity] = (
                    record_hash, serialized, incoming_generated_occurred_at,
                )
                local_fingerprints[(record_identity, False)] = str(
                    record["idempotency_fingerprint"]
                )
                if incoming_generated_occurred_at:
                    local_fingerprints[(record_identity, True)] = str(
                        record["idempotency_retry_fingerprint"]
                    )
                hashes.append(record_hash)
                inserted = True
                inserted_count += 1
            if inserted:
                watermark_started = time.perf_counter() if metrics is not None else 0.0
                self._store_authority_watermark(connection, watermark)
                if metrics is not None:
                    metrics["watermark_seconds"] = metrics.get("watermark_seconds", 0.0) + time.perf_counter() - watermark_started
        # Do not claim a new tip until SQLite has committed every row and its
        # watermark.  A failing commit must leave all process-local durable
        # state at the previous proven sequence.
        if inserted:
            self._counts_cache = pending_counts
            self._highest_sequence = pending_highest_sequence
            self._last_record_time = pending_last_record_time
            self._final_record_hash = pending_final_record_hash
            self._authority_watermark = watermark
        if last_external_authority is not None:
            self._last_external_authority_sequence, self._last_external_authority_hash = last_external_authority
        now = time.perf_counter()
        with self._deferred_condition:
            if inserted_count:
                self._durable_records_total += inserted_count
                self._record_rate_locked(self._durable_rate_buckets, now, inserted_count)
                self._record_queue_depth_locked(now)
            batch_telemetry: dict[str, object] = {
                "recorded_at_monotonic": round(now, 6),
                "batch_size": len(records),
                "inserted_records": inserted_count,
                "duplicate_records": len(records) - inserted_count,
                "durability_mode": self._synchronous_mode,
                "sampled": metrics is not None,
                # Automatic checkpoints are disabled on the writer. The
                # separate PASSIVE connection reports its own measured result
                # in writer telemetry, so this commit has no hidden automatic
                # checkpoint interval to attribute here.
                "wal_checkpoint_seconds": None,
                "wal_checkpoint_activity": "PASSIVE_DEDICATED_CONNECTION",
            }
            if metrics is not None:
                metrics["batch_processing_seconds"] = time.perf_counter() - batch_started
                metrics["wal_checkpoint_stall_upper_bound_seconds"] = 0.0
                try:
                    metrics["wal_size_bytes"] = float(Path(str(self.path) + "-wal").stat().st_size)
                except OSError:
                    metrics["wal_size_bytes"] = 0.0
                batch_telemetry.update({key: round(value, 6) for key, value in metrics.items()})
            self._writer_batches.append(batch_telemetry)
            if inserted_count:
                self._schedule_passive_checkpoint_locked()
        return hashes

    def append_deferred(
        self,
        kind: str,
        payload: Mapping[str, object],
        *,
        identity: str | None = None,
        occurred_at: str | None = None,
        execution_session_id: str | None = None,
    ) -> dict[str, object]:
        prepared = self._prepare(kind, payload, identity, occurred_at, execution_session_id)
        if str(prepared["domain"]) not in _HIGH_VOLUME_DOMAINS:
            raise ValueError("Only raw observations, evidence, and no-side-effect decisions may use deferred persistence.")
        if str(prepared["domain"]) == "DECISION":
            common = prepared.get("common")
            stored_payload = common.get("payload") if isinstance(common, Mapping) else None
            classification = commissioning_tail_classification(
                "DECISION", kind, stored_payload if isinstance(stored_payload, Mapping) else {},
            )
            if classification.category is CommissioningTailCategory.AUTHORITY_MUTATION:
                raise ValueError("Authority-capable decisions may not use deferred persistence.")
        domain = str(prepared["domain"])
        common = prepared.get("common")
        stored_payload = common.get("payload") if isinstance(common, Mapping) else None
        observation_type = (
            stored_payload.get("observation_type") if isinstance(stored_payload, Mapping) else None
        )
        suppress = (
            not self._persist_high_frequency_records
            and (
                domain in {"EVIDENCE", "DECISION"}
                or (domain == "OBSERVATION" and observation_type in _PASSIVE_MARKET_OBSERVATION_TYPES)
            )
        )
        if suppress:
            with self._deferred_condition:
                self._suppressed_high_frequency_records += 1
                self._suppressed_high_frequency_by_domain[domain] += 1
                receipt = self._capacity_snapshot_locked(time.perf_counter())
                receipt.update({
                    "persistence_action": "SUPPRESSED",
                    "persistence_reason": "AUTHORITY_LEDGER_HIGH_FREQUENCY_DISABLED",
                })
                return receipt
        return self._enqueue_deferred_prepared(prepared)

    def append_commissioning_attestation_deferred(
        self,
        kind: str,
        payload: Mapping[str, object],
        *,
        identity: str | None = None,
        occurred_at: str | None = None,
        execution_session_id: str | None = None,
    ) -> dict[str, object]:
        """Defer only an exact no-authority commissioning readiness attestation."""
        # The writer runs later on another thread.  Detach nested caller-owned
        # containers before redaction, identity, and whitelist validation so
        # post-admission mutation cannot alter the durable record.
        payload_snapshot = deepcopy(dict(payload))
        prepared = self._prepare(kind, payload_snapshot, identity, occurred_at, execution_session_id)
        common = prepared.get("common")
        stored_payload = common.get("payload") if isinstance(common, Mapping) else None
        classification = commissioning_tail_classification(
            str(prepared["domain"]), kind,
            stored_payload if isinstance(stored_payload, Mapping) else {},
        )
        if (
            kind not in _DEFERRED_READINESS_ATTESTATION_KINDS
            or str(prepared["domain"]) != "INCIDENT"
            or classification.category is not CommissioningTailCategory.AUTHORITY_OBSERVATION
        ):
            raise ValueError(
                "Only exact no-authority commissioning readiness attestations may use this deferred path."
            )
        # Preserve the existing FULL-durability classification for the
        # commissioning-relevant attestation itself.  The writer segments this
        # ordered singleton from its NORMAL market-record neighbours, so it
        # cannot silently downgrade their durability or force them all into
        # one expensive FULL transaction.
        return self._enqueue_deferred_prepared(prepared)

    def _enqueue_deferred_prepared(self, prepared: dict[str, object]) -> dict[str, object]:
        with self._ordering_lock, self._deferred_condition:
            if self._deferred_error is not None:
                raise LedgerCapacityError(
                    "Deferred paper ledger writer failed; record was not admitted.",
                    self._capacity_snapshot_locked(time.perf_counter()),
                ) from self._deferred_error
            if self._deferred_stopping or not self._admission_open:
                raise LedgerCapacityError(
                    "Deferred paper ledger admission is sealed; record was not admitted.",
                    self._capacity_snapshot_locked(time.perf_counter()),
                )
            record_identity = str(prepared["identity"])
            generated_occurred_at = prepared.get("idempotency_generated_occurred_at") is True
            fingerprint = str(prepared["idempotency_fingerprint"])
            retry_fingerprint = str(prepared["idempotency_retry_fingerprint"])
            if record_identity in self._deferred_identities:
                known_fingerprints = self._deferred_identity_fingerprints.get(record_identity)
                ignore_occurred_at = (
                    known_fingerprints is not None
                    and generated_occurred_at
                    and known_fingerprints[2]
                )
                known_fingerprint = (
                    None if known_fingerprints is None
                    else known_fingerprints[1] if ignore_occurred_at else known_fingerprints[0]
                )
                submitted_fingerprint = retry_fingerprint if ignore_occurred_at else fingerprint
                if known_fingerprint != submitted_fingerprint:
                    raise ValueError("Paper ledger identity conflicts with an already admitted deferred record.")
                return self._capacity_snapshot_locked(time.perf_counter())
            now = time.perf_counter()
            capacity = self._capacity_snapshot_locked(now)
            if capacity["wal_capacity_fault_latched"] is True:
                self._admission_rejections_total += 1
                raise LedgerCapacityError(
                    "Deferred paper ledger WAL capacity is exhausted; record was not admitted.",
                    capacity,
                )
            if capacity["state"] != "HEALTHY":
                self._admission_rejections_total += 1
                raise LedgerCapacityError(
                    "Deferred paper ledger capacity is not healthy; record was not admitted.",
                    capacity,
                )
            if self._deferred_backlog_depth_locked() >= self._max_deferred_records:
                self._capacity_fault_latched = True
                self._admission_rejections_total += 1
                raise LedgerCapacityError(
                    "Deferred paper ledger capacity is exhausted; record was not admitted.",
                    self._capacity_snapshot_locked(now),
                )
            prepared["_admitted_monotonic"] = now
            self._deferred.append(prepared)
            self._deferred_identities.add(record_identity)
            self._deferred_identity_fingerprints[record_identity] = (
                fingerprint, retry_fingerprint, generated_occurred_at,
            )
            self._deferred_pending_admitted_at.append(now)
            self._deferred_record_count += 1
            self._admitted_records_total += 1
            self._record_rate_locked(self._admission_rate_buckets, now, 1)
            self._deferred_queue_high_water = max(
                self._deferred_queue_high_water, self._deferred_backlog_depth_locked(),
            )
            self._record_queue_depth_locked(now)
            capacity = self._capacity_snapshot_locked(now)
            self._deferred_condition.notify()
            return capacity

    def _commissioning_deferred_barrier(
        self,
        requested_sequences: tuple[int, ...],
        *,
        timeout_seconds: float = 30.0,
        allow_sealed_admission: bool = False,
        allow_pending_barrier_overflow: bool = False,
    ) -> _DeferredLedgerBarrier:
        """Wait only for records admitted before an ordered commissioning fence."""
        if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0:
            raise ValueError("Deferred barrier timeout must be positive.")
        with self._ordering_lock, self._deferred_condition:
            if self._deferred_error is not None:
                raise RuntimeError("Deferred paper ledger writer failed.") from self._deferred_error
            if self._deferred_stopping or (not self._admission_open and not allow_sealed_admission):
                raise RuntimeError("Deferred paper ledger admission is sealed.")
            if (
                self._deferred_barrier_count >= self._max_pending_barriers
                and not allow_pending_barrier_overflow
            ):
                self._barrier_rejections_total += 1
                raise LedgerCapacityError(
                    "Deferred paper ledger checkpoint capacity is exhausted; barrier was not admitted.",
                    self._capacity_snapshot_locked(time.perf_counter()),
                )
            self._next_barrier_token += 1
            barrier = _DeferredLedgerBarrier(self._next_barrier_token, requested_sequences)
            self._deferred.append(barrier)
            self._deferred_barrier_count += 1
            self._deferred_barrier_high_water = max(
                self._deferred_barrier_high_water, self._deferred_barrier_count,
            )
            self._deferred_condition.notify_all()
        deadline = time.monotonic() + float(timeout_seconds)
        with self._deferred_condition:
            while not barrier.completed and self._deferred_error is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Deferred paper ledger barrier exceeded its bounded authority wait.")
                self._deferred_condition.wait(timeout=min(1.0, remaining))
            if self._deferred_error is not None:
                raise RuntimeError("Deferred paper ledger writer failed.") from self._deferred_error
            if not barrier.completed or barrier.ledger_sequence is None or barrier.authority_watermark is None:
                raise RuntimeError("Deferred paper ledger barrier did not produce a complete snapshot.")
            return barrier

    @contextmanager
    def commissioning_authority_fence(self) -> Iterator[None]:
        """Seal ledger admission across the final authority proof and reserve."""
        with self._ordering_lock:
            with self._deferred_condition:
                if self._deferred_error is not None:
                    raise RuntimeError("Deferred paper ledger writer failed.") from self._deferred_error
                if self._deferred_stopping or not self._admission_open:
                    raise RuntimeError("Deferred paper ledger admission is sealed.")
            yield

    def commissioning_authority_checkpoint(self) -> dict[str, object]:
        """Capture the exact authority/unknown watermark at an ordered fence."""
        barrier = self._commissioning_deferred_barrier(())
        watermark = dict(barrier.authority_watermark or {})
        return {
            "policy_version": COMMISSIONING_TAIL_POLICY_VERSION,
            "ledger_identity": self._ledger_uuid,
            "ledger_epoch": self._ledger_epoch,
            "ledger_schema_version": self._schema_version,
            "ledger_sequence": int(barrier.ledger_sequence),
            "ledger_record_hash": barrier.record_hash,
            "last_authority_mutation_sequence": int(
                watermark.get("last_authority_mutation_sequence") or 0
            ),
            "last_authority_mutation_hash": watermark.get("last_authority_mutation_hash"),
            "last_unknown_sequence": int(watermark.get("last_unknown_sequence") or 0),
            "last_unknown_hash": watermark.get("last_unknown_hash"),
            "last_external_authority_sequence": barrier.external_authority_sequence,
            "last_external_authority_hash": barrier.external_authority_hash,
            "deferred_barrier_token": barrier.token,
            "deferred_barrier_wait_seconds": round(float(barrier.wait_seconds or 0.0), 6),
            "deferred_capacity": dict(barrier.deferred_capacity or {}),
        }

    @staticmethod
    def _uses_normal_deferred_durability(record: Mapping[str, object]) -> bool:
        return (
            str(record.get("domain")) in _HIGH_VOLUME_DOMAINS
            or record.get("_normal_deferred_durability") is True
        )

    def _append_deferred_durability_segments(self, batch: tuple[dict[str, object], ...]) -> None:
        """Commit contiguous NORMAL/FULL spans without changing FIFO order.

        A readiness attestation is an ordered FULL record, but it must not
        downgrade unrelated market observations already adjacent in the queue.
        Each span commits independently, preserving the global hash tip and
        authority watermark before the following span starts.
        """
        segment: list[dict[str, object]] = []
        segment_is_high_volume: bool | None = None
        for record in batch:
            is_high_volume = self._uses_normal_deferred_durability(record)
            if segment and is_high_volume != segment_is_high_volume:
                self._append_prepared(tuple(segment))
                segment = []
            segment.append(record)
            segment_is_high_volume = is_high_volume
        if segment:
            self._append_prepared(tuple(segment))

    def _deferred_writer(self) -> None:
        while True:
            with self._deferred_condition:
                while not self._deferred and not self._deferred_stopping:
                    self._deferred_condition.wait()
                if self._deferred_stopping and not self._deferred:
                    return
                batch_limit = (
                    self._catch_up_batch_size
                    if self._deferred_backlog_depth_locked() >= self._catch_up_threshold
                    else _DEFERRED_NORMAL_BATCH_SIZE
                )
                # A plain enqueue notification must not defeat the small-queue
                # coalescing deadline.  It only wakes this loop to re-check
                # barriers, shutdown, explicit drains, and the batch target.
                # Bound latency from the oldest admission, rather than adding
                # a fresh 10 ms sleep after every completed transaction. At a
                # steady stream the latter creates an avoidable processing+
                # sleep sawtooth that can leave writer capacity below ingress.
                deadline = (
                    self._deferred_pending_admitted_at[0] + _DEFERRED_MAX_COALESCE_SECONDS
                    if self._deferred_pending_admitted_at else time.perf_counter()
                )
                while (
                    self._deferred_record_count < batch_limit
                    and not self._deferred_barrier_count
                    and not self._deferred_stopping
                    and not self._deferred_flush_requested
                ):
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0:
                        break
                    self._deferred_condition.wait(timeout=remaining)
                records: list[dict[str, object]] = []
                barrier: _DeferredLedgerBarrier | None = None
                while self._deferred and len(records) < batch_limit:
                    item = self._deferred.popleft()
                    if isinstance(item, _DeferredLedgerBarrier):
                        barrier = item
                        self._deferred_barrier_count -= 1
                        break
                    records.append(item)
                    self._deferred_record_count -= 1
                    if not self._deferred_pending_admitted_at:
                        raise RuntimeError("Deferred ledger admission timestamps lost queue alignment.")
                    self._deferred_pending_admitted_at.popleft()
                batch = tuple(records)
                self._deferred_inflight_record_count = len(batch)
                admitted_times = [
                    float(record["_admitted_monotonic"])
                    for record in batch if isinstance(record.get("_admitted_monotonic"), float)
                ]
                self._deferred_inflight_oldest_admitted_at = min(admitted_times) if admitted_times else None
                self._deferred_active = True
                self._record_queue_depth_locked(time.perf_counter())
            try:
                with self._lock:
                    if batch:
                        self._append_deferred_durability_segments(batch)
                    if barrier is not None:
                        barrier.ledger_sequence = self._highest_sequence
                        barrier.record_hash = self._final_record_hash
                        barrier.requested_record_hashes = {}
                        for sequence in barrier.requested_sequences:
                            if sequence == 0:
                                barrier.requested_record_hashes[sequence] = None
                                continue
                            row = self._connection.execute(
                                "SELECT record_hash FROM lane_iii_paper_audit WHERE ledger_sequence=?", (sequence,)
                            ).fetchone()
                            barrier.requested_record_hashes[sequence] = None if row is None else str(row["record_hash"])
                        barrier.authority_watermark = dict(self._authority_watermark)
                        barrier.authority_watermark["safe_classification_last_sequences"] = dict(
                            self._authority_watermark.get("safe_classification_last_sequences") or {}
                        )
                        barrier.external_authority_sequence = self._last_external_authority_sequence
                        barrier.external_authority_hash = self._last_external_authority_hash
                        barrier.wait_seconds = time.perf_counter() - barrier.admitted_at
            except BaseException as error:
                with self._deferred_condition:
                    self._deferred_error = error
                    self._deferred_active = False
                    self._deferred_flush_requested = False
                    self._deferred_condition.notify_all()
                return
            with self._deferred_condition:
                for record in batch:
                    identity = str(record["identity"])
                    self._deferred_identities.discard(identity)
                    self._deferred_identity_fingerprints.pop(identity, None)
                self._deferred_inflight_record_count = 0
                self._deferred_inflight_oldest_admitted_at = None
                if barrier is not None:
                    barrier.deferred_capacity = self._capacity_snapshot_locked(time.perf_counter())
                    barrier.completed = True
                    if self._last_barrier_token is None or barrier.token >= self._last_barrier_token:
                        self._last_barrier_token = barrier.token
                        self._last_barrier_sequence = barrier.ledger_sequence
                        self._last_barrier_wait_seconds = barrier.wait_seconds
                self._deferred_active = False
                self._record_queue_depth_locked(time.perf_counter())
                if not self._deferred:
                    self._deferred_flush_requested = False
                self._deferred_condition.notify_all()

    def flush_deferred(self, *, timeout_seconds: float | None = None) -> None:
        if timeout_seconds is not None and (not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0):
            raise ValueError("Deferred flush timeout must be positive when supplied.")
        deadline = None if timeout_seconds is None else time.monotonic() + float(timeout_seconds)
        with self._deferred_condition:
            self._deferred_flush_requested = True
            self._deferred_condition.notify_all()
            while (self._deferred or self._deferred_active) and self._deferred_error is None:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("Deferred paper ledger drain exceeded its bounded authority wait.")
                self._deferred_condition.wait(timeout=1.0 if remaining is None else min(1.0, remaining))
            if self._deferred_error is not None:
                raise RuntimeError("Deferred paper ledger writer failed.") from self._deferred_error
            self._deferred_flush_requested = False

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
        self._commissioning_deferred_barrier(())
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
        self._commissioning_deferred_barrier(())
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
        requested = {verified_through_sequence}
        if last_full_verified_sequence is not None:
            requested.add(last_full_verified_sequence)
        barrier = self._commissioning_deferred_barrier(tuple(sorted(requested)))
        tip = int(barrier.ledger_sequence)
        if verified_through_sequence > tip:
            raise RuntimeError("Commissioning verified anchor is beyond the current ledger tip.")
        watermark = dict(barrier.authority_watermark or {})
        if int(watermark.get("classified_through_sequence") or -1) != tip:
            raise RuntimeError("Commissioning authority classification does not reach the captured ledger tip.")
        hashes = dict(barrier.requested_record_hashes or {})
        safe_last = dict(watermark.get("safe_classification_last_sequences") or {})
        tail_kinds = sorted(
            classification
            for classification, sequence in safe_last.items()
            if type(sequence) is int and verified_through_sequence < sequence <= tip
        )
        passive_shapes = _V2_SAFE_CLASSIFICATIONS - {
            "OBSERVATION:OBSERVATION_ENVELOPE:ACCOUNT_ITEM_INFORMATIONAL:AUTHORITY_EFFECT_NONE",
        }
        tail_categories: list[str] = []
        if any(
            shape in passive_shapes and verified_through_sequence < sequence <= tip
            for shape, sequence in safe_last.items()
            if type(sequence) is int
        ):
            tail_categories.append(CommissioningTailCategory.PASSIVE_DATA.value)
        for prefix, category in (
            ("last_authority_observation", CommissioningTailCategory.AUTHORITY_OBSERVATION),
            ("last_authority_mutation", CommissioningTailCategory.AUTHORITY_MUTATION),
            ("last_unknown", CommissioningTailCategory.UNKNOWN),
        ):
            sequence = watermark.get(f"{prefix}_sequence")
            if type(sequence) is int and verified_through_sequence < sequence <= tip:
                tail_categories.append(category.value)
        mutation_sequence = int(watermark.get("last_authority_mutation_sequence") or 0)
        unknown_sequence = int(watermark.get("last_unknown_sequence") or 0)
        blocking_prefix = "last_unknown" if unknown_sequence >= mutation_sequence else "last_authority_mutation"
        blocking_sequence = max(mutation_sequence, unknown_sequence)
        blocking_classification = (
            None if blocking_sequence == 0
            else CommissioningTailCategory.UNKNOWN.value
            if blocking_prefix == "last_unknown"
            else CommissioningTailCategory.AUTHORITY_MUTATION.value
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
            "arm_snapshot_tip_hash": barrier.record_hash,
            "deferred_barrier_token": barrier.token,
            "deferred_barrier_ledger_sequence": tip,
            "deferred_barrier_wait_seconds": round(float(barrier.wait_seconds or 0.0), 6),
            "deferred_capacity": dict(barrier.deferred_capacity or {}),
            "unverified_tail_rows": tip - verified_through_sequence,
            "tail_start_sequence": verified_through_sequence + 1 if tip > verified_through_sequence else None,
            "tail_end_sequence": tip if tip > verified_through_sequence else None,
            "tail_record_kinds": tail_kinds,
            "tail_record_categories": tail_categories,
            "last_blocking_sequence": blocking_sequence,
            "last_blocking_kind": (
                None if blocking_sequence == 0 else watermark.get(f"{blocking_prefix}_kind")
            ),
            "last_blocking_domain": (
                None if blocking_sequence == 0 else watermark.get(f"{blocking_prefix}_domain")
            ),
            "last_blocking_hash": (
                None if blocking_sequence == 0 else watermark.get(f"{blocking_prefix}_hash")
            ),
            "last_blocking_classification": blocking_classification,
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
        with self._deferred_condition:
            now = time.perf_counter()
            writer_telemetry = self._writer_telemetry_locked(now)
            deferred_queue_depth = self._deferred_backlog_depth_locked()
            deferred_pending_depth = self._deferred_record_count
            deferred_inflight_depth = self._deferred_inflight_record_count
            deferred_pending_barriers = self._deferred_barrier_count
            deferred_work_item_depth = self._deferred_work_item_depth_locked()
            deferred_barrier_high_water = self._deferred_barrier_high_water
            deferred_writer_active = self._deferred_active
            deferred_queue_high_water = self._deferred_queue_high_water
            deferred_writer_error = None if self._deferred_error is None else type(self._deferred_error).__name__
            last_barrier_token = self._last_barrier_token
            last_barrier_sequence = self._last_barrier_sequence
            last_barrier_wait_seconds = self._last_barrier_wait_seconds
        try:
            file_size: int | None = self.path.stat().st_size
        except OSError:
            file_size = None
        wal_path = Path(str(self.path) + "-wal")
        try:
            wal_size = wal_path.stat().st_size
        except OSError:
            wal_size = 0
        writer_telemetry["wal_size_bytes"] = wal_size
        writer_telemetry["wal_autocheckpoint_pages"] = _WAL_AUTOCHECKPOINT_PAGES
        writer_telemetry["wal_passive_checkpoint_trigger_records"] = _WAL_PASSIVE_CHECKPOINT_TRIGGER_RECORDS
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
            "deferred_queue_depth": deferred_queue_depth,
            "deferred_pending_queue_depth": deferred_pending_depth,
            "deferred_inflight_queue_depth": deferred_inflight_depth,
            "deferred_pending_barrier_count": deferred_pending_barriers,
            "deferred_work_item_depth": deferred_work_item_depth,
            "deferred_barrier_high_water": deferred_barrier_high_water,
            "deferred_writer_active": deferred_writer_active,
            "deferred_queue_high_water": deferred_queue_high_water,
            "deferred_writer_error": deferred_writer_error,
            "last_deferred_barrier_token": last_barrier_token,
            "last_deferred_barrier_ledger_sequence": last_barrier_sequence,
            "last_deferred_barrier_wait_seconds": last_barrier_wait_seconds,
            "writer_telemetry": writer_telemetry,
            "deferred_capacity": {
                key: value for key, value in writer_telemetry.items()
                if key not in {"recent_batches", "last_checkpoint", "last_passive_checkpoint"}
            },
            "persistence_policy": {
                "schema": "l3g-authority-ledger-persistence-policy-v1",
                "authority_ledger": "PERMANENT_SAFETY_AND_AUTHORITY_RECORDS_ONLY",
                "raw_market_observations": (
                    "ENABLED_TEST_ONLY" if self._persist_high_frequency_records else "DISABLED"
                ),
                "derived_evidence": (
                    "ENABLED_TEST_ONLY" if self._persist_high_frequency_records else "DISABLED"
                ),
                "no_effect_decisions": (
                    "ENABLED_TEST_ONLY" if self._persist_high_frequency_records else "DISABLED"
                ),
                "scientific_bulk_persistence": "DISABLED_UNTIL_SEPARATE_BOUNDED_STORE",
                "suppressed_records_total": writer_telemetry[
                    "suppressed_high_frequency_records_total"
                ],
                "suppressed_records_by_domain": writer_telemetry[
                    "suppressed_high_frequency_records_by_domain"
                ],
            },
        }

    def _durable_tip_locked(self) -> tuple[int, str | None]:
        row = self._connection.execute(
            "SELECT ledger_sequence, record_hash FROM lane_iii_paper_audit "
            "ORDER BY ledger_sequence DESC LIMIT 1"
        ).fetchone()
        return (0, None) if row is None else (int(row["ledger_sequence"]), str(row["record_hash"]))

    def _checkpoint_for_shutdown_locked(self) -> dict[str, object]:
        started = time.perf_counter()
        row = self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        elapsed = time.perf_counter() - started
        if row is None:
            raise RuntimeError("SQLite shutdown checkpoint returned no status row.")
        busy, log_frames, checkpointed_frames = (int(row[index]) for index in range(3))
        checkpoint = {
            "mode": "TRUNCATE",
            "busy": busy,
            "log_frames": log_frames,
            "checkpointed_frames": checkpointed_frames,
            "duration_seconds": round(elapsed, 6),
            "complete": busy == 0 and log_frames == checkpointed_frames,
        }
        if not checkpoint["complete"]:
            raise RuntimeError("SQLite shutdown checkpoint did not complete.")
        return checkpoint

    def close(self) -> dict[str, object]:
        """Seal, drain, prove, checkpoint, and close the admitted writer prefix.

        A returned receipt is the only clean-shutdown claim. If any stage
        fails, admission remains sealed and the receipt records the failure;
        callers receive an exception instead of an ambiguous clean result.
        """
        failure: BaseException | None = None
        expected_tip: tuple[int, str | None] = (0, None)
        durable_tip: tuple[int, str | None] = (0, None)
        checkpoint: dict[str, object] | None = None
        writer_stopped = False
        checkpoint_worker_stopped = False
        admitted_prefix_records = 0
        receipt: dict[str, object]

        # Seal admission under the ordering lock, then release it before the
        # potentially long drain. Producers which arrive after the seal must
        # reject promptly rather than queue behind shutdown's barrier wait.
        while True:
            owns_shutdown = False
            with self._ordering_lock:
                with self._deferred_condition:
                    if self._shutdown_receipt is not None:
                        return dict(self._shutdown_receipt)
                    if not self._closing:
                        self._closing = True
                        self._admission_open = False
                        self._deferred_flush_requested = True
                        admitted_prefix_records = self._deferred_backlog_depth_locked()
                        self._record_queue_depth_locked(time.perf_counter())
                        self._deferred_condition.notify_all()
                        owns_shutdown = True
            if owns_shutdown:
                break
            with self._deferred_condition:
                while self._shutdown_receipt is None:
                    self._deferred_condition.wait(timeout=1.0)
                return dict(self._shutdown_receipt)

        try:
            try:
                barrier = self._commissioning_deferred_barrier(
                    (), timeout_seconds=30.0, allow_sealed_admission=True,
                    # Admission is sealed, so this one terminal fence cannot
                    # be amplified by new callers.  Reserve it even when the
                    # bounded pre-shutdown barrier queue was already full.
                    allow_pending_barrier_overflow=True,
                )
                expected_tip = (int(barrier.ledger_sequence), barrier.record_hash)
                with self._lock:
                    durable_tip = self._durable_tip_locked()
                if durable_tip != expected_tip:
                    raise RuntimeError(
                        "Deferred paper ledger shutdown tip differs from its sealed admitted prefix."
                    )
            except BaseException as error:
                failure = error

            with self._deferred_condition:
                self._deferred_stopping = True
                self._deferred_flush_requested = True
                self._deferred_condition.notify_all()
            try:
                self._deferred_thread.join(timeout=30.0)
                writer_stopped = not self._deferred_thread.is_alive()
                if not writer_stopped and failure is None:
                    failure = RuntimeError("Deferred paper ledger writer did not stop during controlled shutdown.")
            except BaseException as error:
                if failure is None:
                    failure = error
                writer_stopped = False

            # The maintenance connection can otherwise race a final TRUNCATE
            # checkpoint or retain a WAL read mark. Stop and join it before
            # either shutdown proof or main-connection close.
            try:
                checkpoint_worker_stopped = self._stop_passive_checkpoint_worker()
                if not checkpoint_worker_stopped and failure is None:
                    failure = RuntimeError(
                        "Passive paper ledger checkpoint worker did not stop during controlled shutdown."
                    )
            except BaseException as error:
                if failure is None:
                    failure = error
                checkpoint_worker_stopped = False

            if writer_stopped and checkpoint_worker_stopped:
                try:
                    with self._lock:
                        durable_tip = self._durable_tip_locked()
                        if failure is None and durable_tip != expected_tip:
                            raise RuntimeError(
                                "Deferred paper ledger durable tip changed after its shutdown fence."
                            )
                        if failure is None:
                            checkpoint = self._checkpoint_for_shutdown_locked()
                    if checkpoint is not None:
                        with self._deferred_condition:
                            self._last_checkpoint = dict(checkpoint)
                except BaseException as error:
                    if failure is None:
                        failure = error

                try:
                    with self._lock:
                        self._connection.close()
                    self._closed = True
                except BaseException as error:
                    if failure is None:
                        failure = error
        except BaseException as error:
            if failure is None:
                failure = error
        finally:
            # Publish even if an unexpected lifecycle primitive failed. A
            # waiting concurrent closer must receive an exact failed receipt,
            # never block forever behind an abandoned ``_closing`` marker.
            try:
                closed_at = _now()
            except BaseException as error:  # pragma: no cover - clock failure is defensive
                if failure is None:
                    failure = error
                closed_at = None
            with self._ordering_lock:
                receipt = {
                    "schema": "l3g-ledger-controlled-shutdown-v1",
                    "closed_at": closed_at,
                    "clean_shutdown": failure is None and writer_stopped and checkpoint is not None,
                    "admission_sealed": True,
                    "admitted_prefix_records": admitted_prefix_records,
                    "expected_tip_sequence": expected_tip[0],
                    "expected_tip_hash": expected_tip[1],
                    "durable_tip_sequence": durable_tip[0],
                    "durable_tip_hash": durable_tip[1],
                    "writer_stopped": writer_stopped,
                    "checkpoint_worker_stopped": checkpoint_worker_stopped,
                    "last_passive_checkpoint": self._passive_checkpoint_snapshot()["last_passive_checkpoint"],
                    "checkpoint_worker_error": self._passive_checkpoint_snapshot()["passive_checkpoint_worker_error"],
                    "checkpoint": checkpoint,
                    "error": None if failure is None else f"{type(failure).__name__}: {failure}",
                }
                with self._deferred_condition:
                    self._shutdown_receipt = receipt
                    self._closing = False
                    self._deferred_condition.notify_all()
        if failure is not None:
            raise RuntimeError("Controlled paper ledger shutdown failed; see shutdown_status().") from failure
        return dict(receipt)

    def __enter__(self) -> "PaperLedger":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
