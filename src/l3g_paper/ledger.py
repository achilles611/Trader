"""Durable, hash-chained SQLite ledger for every Lane III-G side effect."""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import StrEnum
import json
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


def _session_context_matches(payload: Mapping[str, object]) -> bool:
    if type(payload.get("session_generation")) is not int:
        return False
    try:
        context = context_from_identity(
            PaperSessionKind(str(payload["session_kind"])),
            str(payload["session_id"]),
            str(payload["trade_date"]),
            str(payload["session_profile_hash"]),
            int(payload["session_generation"]),
            calendar_state=PaperCalendarState(str(payload["calendar_state"])),
        )
    except (KeyError, TypeError, ValueError):
        return False
    return all(payload.get(key) == value for key, value in context.payload().items())


def _session_identity_matches(payload: Mapping[str, object]) -> bool:
    if type(payload.get("session_generation")) is not int:
        return False
    try:
        context = context_from_identity(
            PaperSessionKind(str(payload["session_kind"])),
            str(payload["session_id"]),
            str(payload["trade_date"]),
            str(payload["session_profile_hash"]),
            int(payload["session_generation"]),
        )
    except (KeyError, TypeError, ValueError):
        return False
    return payload.get("session_family") == context.session_family.value


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
        or payload.get("paper_policy_hash") != POLICY.configuration_hash
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
        and payload.get("paper_policy_hash") == POLICY.configuration_hash
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
    wait_seconds: float | None = None


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
        self._deferred_record_count = 0
        self._deferred_queue_high_water = 0
        self._deferred_barrier_count = 0
        self._deferred_active = False
        self._deferred_error: BaseException | None = None
        self._deferred_stopping = False
        self._next_barrier_token = 0
        self._last_barrier_token: int | None = None
        self._last_barrier_sequence: int | None = None
        self._last_barrier_wait_seconds: float | None = None
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
        with self._session_context_lock:
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
        watermark["safe_classification_last_sequences"] = dict(
            watermark.get("safe_classification_last_sequences") or {}
        )
        inserted = False
        last_external_authority: tuple[int, str] | None = None
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
                self._counts_cache[domain] = self._counts_cache.get(domain, 0) + 1
                self._highest_sequence = sequence
                self._last_record_time = at
                self._final_record_hash = record_hash
            if inserted:
                self._store_authority_watermark(connection, watermark)
        if inserted:
            self._authority_watermark = watermark
        if last_external_authority is not None:
            self._last_external_authority_sequence, self._last_external_authority_hash = (
                last_external_authority
            )
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
        self._enqueue_deferred_prepared(prepared)

    def append_commissioning_attestation_deferred(
        self,
        kind: str,
        payload: Mapping[str, object],
        *,
        identity: str | None = None,
        occurred_at: str | None = None,
        execution_session_id: str | None = None,
    ) -> None:
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
        self._enqueue_deferred_prepared(prepared)

    def _enqueue_deferred_prepared(self, prepared: dict[str, object]) -> None:
        with self._ordering_lock, self._deferred_condition:
            if self._deferred_error is not None:
                raise RuntimeError("Deferred paper ledger writer failed.") from self._deferred_error
            if self._deferred_stopping:
                raise RuntimeError("Deferred paper ledger writer is stopping.")
            record_identity = str(prepared["identity"])
            if record_identity in self._deferred_identities:
                return
            self._deferred.append(prepared)
            self._deferred_identities.add(record_identity)
            self._deferred_record_count += 1
            self._deferred_queue_high_water = max(
                self._deferred_queue_high_water, self._deferred_record_count,
            )
            self._deferred_condition.notify()

    def _commissioning_deferred_barrier(
        self, requested_sequences: tuple[int, ...],
    ) -> _DeferredLedgerBarrier:
        """Wait only for records admitted before an ordered commissioning fence."""
        with self._ordering_lock, self._deferred_condition:
            if self._deferred_error is not None:
                raise RuntimeError("Deferred paper ledger writer failed.") from self._deferred_error
            if self._deferred_stopping:
                raise RuntimeError("Deferred paper ledger writer is stopping.")
            self._next_barrier_token += 1
            barrier = _DeferredLedgerBarrier(self._next_barrier_token, requested_sequences)
            self._deferred.append(barrier)
            self._deferred_barrier_count += 1
            self._deferred_condition.notify()
        with self._deferred_condition:
            while not barrier.completed and self._deferred_error is None:
                self._deferred_condition.wait(timeout=1.0)
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
                if self._deferred_stopping:
                    raise RuntimeError("Deferred paper ledger writer is stopping.")
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
        }

    def _deferred_writer(self) -> None:
        while True:
            with self._deferred_condition:
                while not self._deferred and not self._deferred_stopping:
                    self._deferred_condition.wait()
                if self._deferred_stopping and not self._deferred:
                    return
                if self._deferred_record_count < 512 and not self._deferred_barrier_count and not self._deferred_stopping:
                    self._deferred_condition.wait(timeout=0.01)
                records: list[dict[str, object]] = []
                barrier: _DeferredLedgerBarrier | None = None
                while self._deferred and len(records) < 512:
                    item = self._deferred.popleft()
                    if isinstance(item, _DeferredLedgerBarrier):
                        barrier = item
                        self._deferred_barrier_count -= 1
                        break
                    records.append(item)
                    self._deferred_record_count -= 1
                batch = tuple(records)
                self._deferred_active = True
            try:
                with self._lock:
                    if batch:
                        self._append_prepared(batch)
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
                    self._deferred_condition.notify_all()
                return
            with self._deferred_condition:
                for record in batch:
                    self._deferred_identities.discard(str(record["identity"]))
                if barrier is not None:
                    barrier.completed = True
                    if self._last_barrier_token is None or barrier.token >= self._last_barrier_token:
                        self._last_barrier_token = barrier.token
                        self._last_barrier_sequence = barrier.ledger_sequence
                        self._last_barrier_wait_seconds = barrier.wait_seconds
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
            deferred_queue_depth = self._deferred_record_count
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
            "deferred_writer_active": deferred_writer_active,
            "deferred_queue_high_water": deferred_queue_high_water,
            "deferred_writer_error": deferred_writer_error,
            "last_deferred_barrier_token": last_barrier_token,
            "last_deferred_barrier_ledger_sequence": last_barrier_sequence,
            "last_deferred_barrier_wait_seconds": last_barrier_wait_seconds,
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
