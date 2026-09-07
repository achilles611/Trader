"""Detached, fail-closed startup evidence for the five-minute perpetual profile.

The artifact in this module is deliberately narrower than a policy checkpoint.
It carries the contiguous chain of already-completed V2 evaluations from the
latest non-tied candle through the current completed candle, plus every
canonical NinjaTrader observation needed to establish their provenance.  It
never imports mutable V1 policy state and it never grants execution authority
by itself.

The source runtime first builds a seed core, appends the core hash as one
immutable source-ledger export row, and writes the artifact.  After controlled
shutdown, a separate proof binds that single export-row anchor to the exact
clean tip of a successful full-ledger verification.  The core hash covered by
that row transitively commits every embedded canonical observation and policy
envelope.  All writes are create-exclusive and fsync-backed; readers accept
only exact shapes and recompute every available digest.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from typing import Mapping, Sequence

from src.l3f_provider.ninjatrader_observation import (
    L3F2_SCHEMA,
    NinjaTraderObservation,
    NinjaTraderObservationError,
)
from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import (
    ACCOUNT_BINDING,
    FIVE_MINUTE_PERPETUAL_PROFILE,
    FIVE_MINUTE_PROFILE,
    BookCompleteness,
    EvidenceFamily,
    HypothesisKind,
    PaperDecision,
    PaperDecisionKind,
    PaperDirection,
    PaperEvidence,
    PaperSourceQuality,
    SequenceAuthority,
    deterministic_id,
)
from .sessions import (
    PaperCalendarState,
    PaperSessionContext,
    PaperSessionKind,
    context_from_identity,
)


PERPETUAL_STARTUP_SEED_CORE_SCHEMA = (
    "lane-iii-five-minute-perpetual-startup-seed-core-v1"
)
PERPETUAL_STARTUP_SEED_ARTIFACT_SCHEMA = (
    "lane-iii-five-minute-perpetual-startup-seed-artifact-v1"
)
PERPETUAL_STARTUP_SEED_PROOF_SCHEMA = (
    "lane-iii-five-minute-perpetual-startup-seed-proof-v1"
)
PERPETUAL_BOUNDARY_BUNDLE_SCHEMA = (
    "lane-iii-five-minute-perpetual-boundary-bundle-v1"
)
PERPETUAL_OBSERVATION_PROOF_SCHEMA = (
    "lane-iii-five-minute-perpetual-observation-proof-v1"
)
PERPETUAL_STARTUP_SEED_EXPORT_SCHEMA = (
    "lane-iii-five-minute-perpetual-startup-seed-export-v1"
)
PERPETUAL_STARTUP_SEED_EXPORT_KIND = (
    "RISK_EVENT_FIVE_MINUTE_PERPETUAL_STARTUP_SEED_EXPORTED"
)

_OPERATION_ID = re.compile(r"^profile-switch-[0-9a-f]{32}$")
_VERIFICATION_ID = re.compile(r"^lv-[0-9a-f]{32}$")
_LEDGER_ID = re.compile(r"^l3g-ledger-[0-9a-f]{32}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_EPOCH = re.compile(r"^L3G-PAPER-EPOCH-[A-Za-z0-9][A-Za-z0-9._-]*$")
_MARKET_TYPES = frozenset({"QUOTE", "TRADE", "DEPTH"})
_REQUIRED_FAMILIES = frozenset({
    EvidenceFamily.STRUCTURAL_CONTEXT.value,
    EvidenceFamily.ORDER_FLOW.value,
    EvidenceFamily.RESTING_LIQUIDITY.value,
})
_EVIDENCE_SEMANTICS = frozenset({
    (
        HypothesisKind.BULLISH_REVERSAL.value,
        EvidenceFamily.STRUCTURAL_CONTEXT.value,
        "RANGE_RECLAIM_UP", True,
    ),
    (
        HypothesisKind.BEARISH_CONTINUATION.value,
        EvidenceFamily.STRUCTURAL_CONTEXT.value,
        "RANGE_RECLAIM_UP", False,
    ),
    (
        HypothesisKind.BEARISH_CONTINUATION.value,
        EvidenceFamily.STRUCTURAL_CONTEXT.value,
        "RANGE_EXPANSION_DOWN", True,
    ),
    (
        HypothesisKind.BULLISH_REVERSAL.value,
        EvidenceFamily.STRUCTURAL_CONTEXT.value,
        "RANGE_EXPANSION_DOWN", False,
    ),
    (
        HypothesisKind.BEARISH_CONTINUATION.value,
        EvidenceFamily.STRUCTURAL_CONTEXT.value,
        "BELOW_PROVISIONAL_SESSION_VWAP", True,
    ),
    (
        HypothesisKind.BULLISH_REVERSAL.value,
        EvidenceFamily.STRUCTURAL_CONTEXT.value,
        "BELOW_PROVISIONAL_SESSION_VWAP", False,
    ),
    (
        HypothesisKind.BULLISH_REVERSAL.value,
        EvidenceFamily.ORDER_FLOW.value,
        "SELLING_WITHOUT_DOWNWARD_PROGRESS", True,
    ),
    (
        HypothesisKind.BEARISH_CONTINUATION.value,
        EvidenceFamily.ORDER_FLOW.value,
        "AGGRESSIVE_SELL_IMBALANCE", True,
    ),
    (
        HypothesisKind.BEARISH_CONTINUATION.value,
        EvidenceFamily.RESTING_LIQUIDITY.value,
        "BID_LIQUIDITY_PULL", True,
    ),
    (
        HypothesisKind.BULLISH_REVERSAL.value,
        EvidenceFamily.RESTING_LIQUIDITY.value,
        "BID_REPLENISHMENT", True,
    ),
})
_MAX_SEED_BYTES = 16 * 1024 * 1024
_MAX_PROOF_BYTES = 1024 * 1024

_WIRE_FIELDS = frozenset({
    "schema", "observation_id", "session_id", "observation_type",
    "ninja_receipt_time", "local_monotonic_sequence", "provider_timestamp",
    "provider_sequence", "exchange_timestamp", "account", "payload",
})
_SESSION_FIELDS = frozenset({
    "session_kind", "session_family", "session_id", "trade_date", "timezone",
    "observation_start", "entry_start", "entry_cutoff", "hard_flat_deadline",
    "session_end", "session_profile_hash", "session_generation", "calendar_state",
})
_EVIDENCE_FIELDS = frozenset({
    "evidence_id", "hypothesis_kind", "family", "label", "strength", "supports",
    "observed_at", "expires_at", "source_observation_ids", "source_local_sequences",
    "source_payload_hashes", "quality", "sequence_authority", "book_completeness",
    "scientific_eligibility", "blocking", "session_kind", "session_id", "trade_date",
    "session_profile_hash", "session_generation", "source_session_ids", "session_family",
})
_DECISION_FIELDS = frozenset({
    "paper_decision_id", "paper_policy_id", "paper_policy_hash", "decision",
    "created_at", "expires_at", "hypothesis_kind", "direction", "relative_support",
    "family_summary", "source_observation_ids", "source_local_sequences",
    "source_payload_hashes", "sequence_authority", "book_completeness",
    "scientific_eligibility", "reason_code", "session_kind", "session_id",
    "trade_date", "session_profile_hash", "session_generation", "commissioning",
    "strategy_generated", "scientific_evidence", "session_family",
})
_BOUNDARY_SUMMARY_FIELDS = frozenset({
    "candle_open_utc", "candle_close_utc", "decision_observed_at",
    "decision_latency_ms", "missed_boundary_count", "decision_interval_seconds",
    "decision_clock", "startup_reconstruction", "decision_protocol",
    "prior_position", "signal_basis", "completed_interval_aggregate",
    "decision_reference_price", "decision_reference_kind",
    "decision_reference_observation_id", "decision_reference_observed_at",
    "decision_reference_before_scheduled_boundary", "bullish_support",
    "bearish_support", "score_delta", "bullish_families", "bearish_families",
    "action", "target_position", "bias",
})
_LEDGER_RECORD_FIELDS = frozenset({
    "schema", "kind", "occurred_at", "execution_session_id", "paper_policy_hash",
    "risk_profile_hash", "entry_profile", "entry_profile_version",
    "effective_confidence_threshold", "entry_dominance_margin", "entry_family_count",
    "retention_confidence_threshold", "account_binding_hash", "scientific_eligibility",
    "paper_only", "live_capital", "session_kind", "session_family", "session_id",
    "trade_date", "session_profile_hash", "session_generation", "payload", "identity",
    "previous_record_hash", "record_hash",
})
_OBSERVATION_ENVELOPE_FIELDS = _SESSION_FIELDS | frozenset({
    "observation_id", "observation_type", "observed_at", "ninja_receipt_time",
    "provider_timestamp", "exchange_timestamp", "local_monotonic_sequence",
    "source_payload_hash",
})
_EXPORT_PAYLOAD_FIELDS = frozenset({
    "schema", "operation_id", "seed_core_sha256", "source_profile",
    "source_policy_id", "source_policy_sha256", "source_risk_profile_id",
    "source_risk_sha256", "target_profile", "target_policy_id",
    "target_policy_sha256", "target_risk_profile_id", "target_risk_sha256",
    "account_binding_sha256", "current_five_minute_boundary_utc",
    "latest_completed_bundle_sha256", "latest_non_tied_bundle_sha256",
    "boundary_chain_count", "boundary_chain_sha256",
    "observation_count", "observation_set_sha256", "source_ledger_identity",
    "source_ledger_epoch", "session_family",
})


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_NONCANONICAL_VALUE") from exc


def _digest(value: object) -> str:
    return sha256(_canonical(value)).hexdigest()


def _mapping(value: object, code: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise RuntimeError(code)
    return value


def _exact(value: object, fields: frozenset[str], code: str) -> Mapping[str, object]:
    result = _mapping(value, code)
    if set(result) != fields:
        raise RuntimeError(code)
    return result


def _text(value: object, code: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise RuntimeError(code)
    return value


def _hash(value: object, code: str) -> str:
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise RuntimeError(code)
    return value


def _utc(value: object, code: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(code)
    try:
        result = normalized_utc(value, code)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(code) from exc
    if result != value:
        raise RuntimeError(code)
    return result


def _moment(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _decimal(value: object, code: str) -> Decimal:
    if not isinstance(value, str):
        raise RuntimeError(code)
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise RuntimeError(code) from exc
    if not result.is_finite() or str(result) != value:
        raise RuntimeError(code)
    return result


def _sequence(value: object, code: str, *, positive: bool = False) -> int:
    if type(value) is not int or value < (1 if positive else 0):
        raise RuntimeError(code)
    return value


def _profile_binding(profile: object) -> dict[str, object]:
    # The two accepted profiles are compiled constants, never caller-defined.
    return {
        "selection_key": profile.selection_key,  # type: ignore[attr-defined]
        "policy_id": profile.policy.policy_id,  # type: ignore[attr-defined]
        "policy_sha256": profile.policy.configuration_hash,  # type: ignore[attr-defined]
        "risk_profile_id": profile.risk.profile_id,  # type: ignore[attr-defined]
        "risk_sha256": profile.risk.configuration_hash,  # type: ignore[attr-defined]
    }


def _account_binding() -> dict[str, object]:
    return {
        "account_name": ACCOUNT_BINDING.account_name,
        "account_class": ACCOUNT_BINDING.account_class,
        "instrument": ACCOUNT_BINDING.instrument,
        "canonical_contract": ACCOUNT_BINDING.canonical_contract,
        "maximum_quantity": ACCOUNT_BINDING.maximum_quantity,
        "paper_only": ACCOUNT_BINDING.paper_only,
        "live_capital": "DENIED",
        "account_binding_sha256": ACCOUNT_BINDING.binding_hash,
    }


def _source_ledger(value: object) -> dict[str, object]:
    raw = _exact(
        value, frozenset({"path", "ledger_identity", "ledger_epoch"}),
        "PERPETUAL_STARTUP_SEED_SOURCE_LEDGER_INVALID",
    )
    path = _text(raw["path"], "PERPETUAL_STARTUP_SEED_SOURCE_LEDGER_INVALID")
    parsed_path = Path(path)
    if not parsed_path.is_absolute() or str(parsed_path.resolve()) != path:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_SOURCE_LEDGER_INVALID")
    identity = _text(
        raw["ledger_identity"], "PERPETUAL_STARTUP_SEED_SOURCE_LEDGER_INVALID",
    )
    epoch = _text(raw["ledger_epoch"], "PERPETUAL_STARTUP_SEED_SOURCE_LEDGER_INVALID")
    if _LEDGER_ID.fullmatch(identity) is None or _EPOCH.fullmatch(epoch) is None:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_SOURCE_LEDGER_INVALID")
    return {"path": path, "ledger_identity": identity, "ledger_epoch": epoch}


def _session_context(value: object) -> dict[str, object]:
    raw = _exact(value, _SESSION_FIELDS, "PERPETUAL_STARTUP_SEED_SESSION_INVALID")
    try:
        context = PaperSessionContext(
            PaperSessionKind(str(raw["session_kind"])),
            str(raw["session_id"]),
            str(raw["trade_date"]),
            str(raw["timezone"]),
            str(raw["observation_start"]),
            str(raw["entry_start"]),
            str(raw["entry_cutoff"]),
            str(raw["hard_flat_deadline"]),
            str(raw["session_end"]),
            str(raw["session_profile_hash"]),
            _sequence(raw["session_generation"], "PERPETUAL_STARTUP_SEED_SESSION_INVALID"),
            PaperCalendarState(str(raw["calendar_state"])),
        )
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_SESSION_INVALID") from exc
    result = context.payload()
    if result != dict(raw):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_SESSION_INVALID")
    return result


def _same_session(payload: Mapping[str, object], context: Mapping[str, object]) -> bool:
    return all(payload.get(field) == context.get(field) for field in (
        "session_kind", "session_family", "session_id", "trade_date",
        "session_profile_hash", "session_generation",
    ))


def _aligned_provenance(
    ids: object, sequences: object, hashes: object, code: str,
) -> tuple[list[str], list[int], list[str]]:
    if not isinstance(ids, list) or not isinstance(sequences, list) or not isinstance(hashes, list):
        raise RuntimeError(code)
    if not ids or not (len(ids) == len(sequences) == len(hashes)):
        raise RuntimeError(code)
    canonical_ids = [_text(item, code) for item in ids]
    canonical_sequences = [_sequence(item, code) for item in sequences]
    canonical_hashes = [_hash(item, code) for item in hashes]
    if len(canonical_ids) != len(set(canonical_ids)):
        raise RuntimeError(code)
    return canonical_ids, canonical_sequences, canonical_hashes


def _evidence(
    value: object, *, context: Mapping[str, object], boundary: str,
) -> dict[str, object]:
    raw = _exact(value, _EVIDENCE_FIELDS, "PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID")
    ids, sequences, hashes = _aligned_provenance(
        raw["source_observation_ids"], raw["source_local_sequences"],
        raw["source_payload_hashes"], "PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID",
    )
    source_sessions = raw["source_session_ids"]
    if (
        not isinstance(source_sessions, list)
        or source_sessions != [context["session_id"] for _ in ids]
        or not _same_session(raw, context)
        or type(raw["supports"]) is not bool
        or type(raw["scientific_eligibility"]) is not bool
        or type(raw["blocking"]) is not bool
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID")
    try:
        evidence = PaperEvidence(
            str(raw["evidence_id"]), HypothesisKind(str(raw["hypothesis_kind"])),
            EvidenceFamily(str(raw["family"])), str(raw["label"]),
            _decimal(raw["strength"], "PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID"),
            raw["supports"],
            _utc(raw["observed_at"], "PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID"),
            _utc(raw["expires_at"], "PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID"),
            tuple(ids), tuple(sequences), tuple(hashes),
            PaperSourceQuality(str(raw["quality"])),
            SequenceAuthority(str(raw["sequence_authority"])),
            BookCompleteness(str(raw["book_completeness"])),
            raw["scientific_eligibility"], raw["blocking"],
            PaperSessionKind(str(raw["session_kind"])), str(raw["session_id"]),
            str(raw["trade_date"]), str(raw["session_profile_hash"]),
            _sequence(raw["session_generation"], "PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID"),
            tuple(str(item) for item in source_sessions),
        )
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID") from exc
    result = evidence.payload()
    if result != dict(raw):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID")
    identity = {
        "policy_hash": FIVE_MINUTE_PERPETUAL_PROFILE.policy.configuration_hash,
        "hypothesis": result["hypothesis_kind"],
        "family": result["family"],
        "label": result["label"],
        "strength": result["strength"],
        "supports": result["supports"],
        "observed_at": result["observed_at"],
        "sources": tuple(ids),
        "sequences": tuple(sequences),
        "hashes": tuple(hashes),
        "session_kind": result["session_kind"],
        "session_id": result["session_id"],
        "trade_date": result["trade_date"],
        "session_profile_hash": result["session_profile_hash"],
        "session_generation": result["session_generation"],
    }
    if result["evidence_id"] != deterministic_id("l3g-pe-", identity):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_EVIDENCE_ID_INVALID")
    observed = _moment(str(result["observed_at"]))
    expires = _moment(str(result["expires_at"]))
    closed = _moment(boundary)
    lifetime_by_family = {
        EvidenceFamily.STRUCTURAL_CONTEXT.value:
            FIVE_MINUTE_PERPETUAL_PROFILE.policy.structural_evidence_lifetime_seconds,
        EvidenceFamily.ORDER_FLOW.value:
            FIVE_MINUTE_PERPETUAL_PROFILE.policy.flow_evidence_lifetime_seconds,
        EvidenceFamily.RESTING_LIQUIDITY.value:
            FIVE_MINUTE_PERPETUAL_PROFILE.policy.liquidity_evidence_lifetime_seconds,
    }
    expected_lifetime = lifetime_by_family.get(str(result["family"]))
    if (
        expected_lifetime is None
        or (
            result["hypothesis_kind"], result["family"], result["label"],
            result["supports"],
        ) not in _EVIDENCE_SEMANTICS
        or result["blocking"] is not False
        or (
            result["family"]
            in {
                EvidenceFamily.STRUCTURAL_CONTEXT.value,
                EvidenceFamily.RESTING_LIQUIDITY.value,
            }
            and result["strength"]
            != str(FIVE_MINUTE_PERPETUAL_PROFILE.policy.structural_strength)
        )
        or observed > closed
        or expires < closed
        or expires - observed != timedelta(seconds=expected_lifetime)
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_EVIDENCE_NOT_ACTIVE_AT_BOUNDARY")
    return result


def _decision(
    value: object, *, context: Mapping[str, object], boundary: str,
) -> dict[str, object]:
    raw = _exact(value, _DECISION_FIELDS, "PERPETUAL_STARTUP_SEED_DECISION_INVALID")
    ids, sequences, hashes = _aligned_provenance(
        raw["source_observation_ids"], raw["source_local_sequences"],
        raw["source_payload_hashes"], "PERPETUAL_STARTUP_SEED_DECISION_INVALID",
    )
    if not isinstance(raw["family_summary"], Mapping) or not _same_session(raw, context):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_DECISION_INVALID")
    if any(type(raw[field]) is not bool for field in (
        "scientific_eligibility", "commissioning", "strategy_generated",
        "scientific_evidence",
    )):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_DECISION_INVALID")
    hypothesis_value = raw["hypothesis_kind"]
    try:
        hypothesis = None if hypothesis_value is None else HypothesisKind(str(hypothesis_value))
        decision = PaperDecision(
            str(raw["paper_decision_id"]), str(raw["paper_policy_id"]),
            str(raw["paper_policy_hash"]), PaperDecisionKind(str(raw["decision"])),
            _utc(raw["created_at"], "PERPETUAL_STARTUP_SEED_DECISION_INVALID"),
            _utc(raw["expires_at"], "PERPETUAL_STARTUP_SEED_DECISION_INVALID"),
            hypothesis, PaperDirection(str(raw["direction"])),
            _decimal(raw["relative_support"], "PERPETUAL_STARTUP_SEED_DECISION_INVALID"),
            dict(raw["family_summary"]), tuple(ids), tuple(sequences), tuple(hashes),
            SequenceAuthority(str(raw["sequence_authority"])),
            BookCompleteness(str(raw["book_completeness"])),
            raw["scientific_eligibility"],
            str(raw["reason_code"]), PaperSessionKind(str(raw["session_kind"])),
            str(raw["session_id"]), str(raw["trade_date"]),
            str(raw["session_profile_hash"]),
            _sequence(raw["session_generation"], "PERPETUAL_STARTUP_SEED_DECISION_INVALID"),
            raw["commissioning"], raw["strategy_generated"],
            raw["scientific_evidence"],
        )
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_DECISION_INVALID") from exc
    result = decision.payload()
    if (
        result != dict(raw)
        or result["paper_policy_id"] != FIVE_MINUTE_PERPETUAL_PROFILE.policy.policy_id
        or result["paper_policy_hash"] != FIVE_MINUTE_PERPETUAL_PROFILE.policy.configuration_hash
        or result["commissioning"] is not False
        or result["strategy_generated"] is not True
        or result["scientific_evidence"] is not False
        or result["scientific_eligibility"] is not False
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_DECISION_INVALID")
    identity = {
        "policy_id": result["paper_policy_id"],
        "policy_hash": result["paper_policy_hash"],
        "decision": result["decision"],
        "created_at": result["created_at"],
        "expires_at": result["expires_at"],
        "hypothesis_kind": result["hypothesis_kind"],
        "direction": result["direction"],
        "relative_support": result["relative_support"],
        "family_summary": result["family_summary"],
        "source_observation_ids": tuple(ids),
        "source_local_sequences": tuple(sequences),
        "source_payload_hashes": tuple(hashes),
        "sequence_authority": result["sequence_authority"],
        "book_completeness": result["book_completeness"],
        "scientific_eligibility": result["scientific_eligibility"],
        "reason_code": result["reason_code"],
        "session_kind": result["session_kind"],
        "session_id": result["session_id"],
        "trade_date": result["trade_date"],
        "session_profile_hash": result["session_profile_hash"],
        "session_generation": result["session_generation"],
    }
    if result["paper_decision_id"] != deterministic_id("l3g-pd-", identity):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_DECISION_ID_INVALID")
    created = _moment(str(result["created_at"]))
    expires = _moment(str(result["expires_at"]))
    if (
        created < _moment(boundary)
        or expires - created
        != timedelta(seconds=FIVE_MINUTE_PERPETUAL_PROFILE.policy.decision_ttl_seconds)
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_DECISION_TIME_INVALID")
    return result


def _score(
    evidence: Sequence[Mapping[str, object]], hypothesis: HypothesisKind,
) -> tuple[Decimal, dict[str, object]]:
    summary: dict[str, object] = {}
    balance = Decimal("0")
    positive = 0
    blocking = False
    for family in EvidenceFamily:
        values = [
            item for item in evidence
            if item["hypothesis_kind"] == hypothesis.value and item["family"] == family.value
        ]
        supports = max(
            (_decimal(item["strength"], "PERPETUAL_STARTUP_SEED_SCORE_INVALID")
             for item in values if item["supports"] is True),
            default=Decimal("0"),
        )
        contradictions = max(
            (_decimal(item["strength"], "PERPETUAL_STARTUP_SEED_SCORE_INVALID")
             for item in values if item["supports"] is False),
            default=Decimal("0"),
        )
        if supports > 0:
            positive += 1
        blocking = blocking or any(
            item["blocking"] is True and item["supports"] is False for item in values
        )
        balance += supports - contradictions
        summary[family.value] = {
            "support": str(supports),
            "contradiction": str(contradictions),
            "labels": sorted(str(item["label"]) for item in values),
        }
    denominator = FIVE_MINUTE_PERPETUAL_PROFILE.policy.score_denominator
    score = max(Decimal("0"), min(Decimal("1"), Decimal("0.5") + balance / denominator))
    summary["positive_family_count"] = positive
    summary["blocking_contradiction"] = blocking
    return score, summary


def _validate_signal(
    decision: Mapping[str, object], evidence: Sequence[Mapping[str, object]], bias: str,
) -> None:
    raw_summary = decision.get("family_summary")
    if isinstance(raw_summary, Mapping):
        summary_fields = set(raw_summary)
        missing = sorted(_BOUNDARY_SUMMARY_FIELDS - summary_fields)
        unexpected = sorted(summary_fields - _BOUNDARY_SUMMARY_FIELDS)
        if missing:
            raise RuntimeError(
                "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:SUMMARY_MISSING_"
                + str(missing[0]).upper()
            )
        if unexpected:
            raise RuntimeError(
                "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:SUMMARY_UNEXPECTED_"
                + str(unexpected[0]).upper()
            )
    summary = _exact(
        raw_summary, _BOUNDARY_SUMMARY_FIELDS,
        "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID",
    )
    bull, bull_families = _score(evidence, HypothesisKind.BULLISH_REVERSAL)
    bear, bear_families = _score(evidence, HypothesisKind.BEARISH_CONTINUATION)
    computed = "TIE" if bull == bear else "LONG" if bull > bear else "SHORT"
    expected_support = bear if bias == "SHORT" else bull
    comparisons = (
        ("BIAS", bias, computed),
        ("RELATIVE_SUPPORT", decision["relative_support"], str(expected_support)),
        ("SUMMARY_BIAS", summary.get("bias"), bias),
        ("BULLISH_SUPPORT", summary.get("bullish_support"), str(bull)),
        ("BEARISH_SUPPORT", summary.get("bearish_support"), str(bear)),
        ("SCORE_DELTA", summary.get("score_delta"), str(bull - bear)),
        ("BULLISH_FAMILIES", summary.get("bullish_families"), bull_families),
        ("BEARISH_FAMILIES", summary.get("bearish_families"), bear_families),
        ("PRIOR_POSITION", summary.get("prior_position"), PaperDirection.FLAT.value),
        (
            "DECISION_PROTOCOL", summary.get("decision_protocol"),
            "EXIT_RECONCILE_THEN_ENTER",
        ),
        ("DECISION_INTERVAL_SECONDS", summary.get("decision_interval_seconds"), 300),
        (
            "DECISION_CLOCK", summary.get("decision_clock"),
            FIVE_MINUTE_PERPETUAL_PROFILE.policy.decision_clock,
        ),
        ("MISSED_BOUNDARY_COUNT", summary.get("missed_boundary_count"), 0),
        (
            "SIGNAL_BASIS", summary.get("signal_basis"),
            "LATEST_AVAILABLE_PRE_CALLBACK_PROVISIONAL_EVIDENCE",
        ),
    )
    for field, actual, expected_value in comparisons:
        if actual != expected_value:
            raise RuntimeError(
                f"PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:{field}"
            )
    if summary.get("completed_interval_aggregate") is not False:
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:COMPLETED_INTERVAL_AGGREGATE"
        )
    if summary.get("startup_reconstruction") is not True:
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:STARTUP_RECONSTRUCTION"
        )
    try:
        closed = _moment(_utc(
            summary.get("candle_close_utc"), "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID",
        ))
        observed = _moment(str(decision["created_at"]))
        reference_at = _moment(_utc(
            summary.get("decision_reference_observed_at"),
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID",
        ))
    except (TypeError, ValueError, RuntimeError) as exc:
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:TIMESTAMP"
        ) from exc
    for field in (
        "decision_latency_ms", "missed_boundary_count", "decision_interval_seconds",
    ):
        if type(summary.get(field)) is not int:
            raise RuntimeError(
                "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:TYPE_" + field.upper()
            )
    if summary.get("decision_latency_ms") != max(
        0, int((observed - closed).total_seconds() * 1000),
    ):
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:DECISION_LATENCY_MS"
        )
    if summary.get("decision_reference_before_scheduled_boundary") is not True:
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:REFERENCE_BEFORE_BOUNDARY"
        )
    if reference_at >= closed:
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:REFERENCE_TIMESTAMP"
        )
    if summary.get("decision_reference_kind") not in {
        "LAST_TRADE_BEFORE_BOUNDARY", "QUOTE_MID_BEFORE_BOUNDARY",
    }:
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:REFERENCE_KIND"
        )
    if _decimal(
        summary.get("decision_reference_price"),
        "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:REFERENCE_PRICE",
    ) <= 0:
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:REFERENCE_PRICE"
        )
    if not isinstance(summary.get("decision_reference_observation_id"), str):
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:REFERENCE_OBSERVATION_ID_TYPE"
        )
    if not summary.get("decision_reference_observation_id"):
        raise RuntimeError(
            "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:REFERENCE_OBSERVATION_ID"
        )
    if bias == "TIE":
        if decision["hypothesis_kind"] is not None:
            raise RuntimeError(
                "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:TIE_HYPOTHESIS"
            )
        expected = (
            PaperDecisionKind.NO_TRADE.value, PaperDirection.FLAT.value,
            "FIVE_MINUTE_BIAS_TIE_FLAT", "BLOCKED", PaperDirection.FLAT.value,
        )
    else:
        expected = (
            bias, bias, f"FIVE_MINUTE_ENTER_{bias}", "ENTER", bias,
        )
    actual = (
        decision["decision"], decision["direction"], decision["reason_code"],
        summary.get("action"), summary.get("target_position"),
    )
    if actual != expected:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_SIGNAL_INVALID:DECISION_SHAPE")


def _ledger_record(
    value: object, *, expected_kind: str, payload_fields: frozenset[str],
) -> dict[str, object]:
    wrapper = _exact(
        value, frozenset({"ledger_sequence", "record_hash", "record"}),
        "PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID",
    )
    ledger_sequence = _sequence(
        wrapper["ledger_sequence"], "PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID",
        positive=True,
    )
    wrapper_hash = _hash(
        wrapper["record_hash"], "PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID",
    )
    record = _exact(
        wrapper["record"], _LEDGER_RECORD_FIELDS,
        "PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID",
    )
    previous = record["previous_record_hash"]
    if not (
        (ledger_sequence == 1 and previous is None)
        or (ledger_sequence > 1 and isinstance(previous, str) and _HASH.fullmatch(previous))
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID")
    record_hash = _hash(
        record["record_hash"], "PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID",
    )
    unsigned = {key: item for key, item in record.items() if key != "record_hash"}
    source_policy = FIVE_MINUTE_PROFILE.policy
    source_risk = FIVE_MINUTE_PROFILE.risk
    payload = _exact(
        record["payload"], payload_fields, "PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID",
    )
    expected_envelope = {
        "schema": "lane-iii-phase-g-paper-record-v1",
        "kind": expected_kind,
        "paper_policy_hash": source_policy.configuration_hash,
        "risk_profile_hash": source_risk.configuration_hash,
        "entry_profile": source_policy.entry_profile,
        "entry_profile_version": source_policy.entry_profile_version,
        "effective_confidence_threshold": str(source_policy.entry_support_threshold),
        "entry_dominance_margin": str(source_policy.entry_dominance_margin),
        "entry_family_count": source_policy.entry_family_count,
        "retention_confidence_threshold": str(source_policy.retention_support_threshold),
        "account_binding_hash": ACCOUNT_BINDING.binding_hash,
        "scientific_eligibility": False,
        "paper_only": True,
        "live_capital": False,
    }
    try:
        record_context = context_from_identity(
            PaperSessionKind(str(record["session_kind"])),
            str(record["session_id"]), str(record["trade_date"]),
            str(record["session_profile_hash"]),
            _sequence(
                record["session_generation"],
                "PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID",
            ),
        )
    except (TypeError, ValueError, RuntimeError) as exc:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID") from exc
    if (
        any(record.get(key) != item for key, item in expected_envelope.items())
        or record_hash != wrapper_hash
        or record_hash != canonical_hash(unsigned)
        or _utc(record["occurred_at"], "PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID")
        != record["occurred_at"]
        or not isinstance(record["identity"], str)
        or not record["identity"]
        or (
            record["execution_session_id"] is not None
            and not isinstance(record["execution_session_id"], str)
        )
        or (
            _SESSION_FIELDS.issubset(payload_fields)
            and not _same_session(payload, record)
        )
        or record["session_family"] != record_context.session_family.value
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_LEDGER_RECORD_INVALID")
    return {
        "ledger_sequence": ledger_sequence,
        "record_hash": wrapper_hash,
        "record": dict(record),
    }


def _wire(value: object) -> tuple[dict[str, object], NinjaTraderObservation]:
    raw = _exact(value, _WIRE_FIELDS, "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID")
    account = raw["account"]
    if account is not None:
        account_map = _exact(
            account, frozenset({"alias", "class"}),
            "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID",
        )
        canonical_account: dict[str, object] | None = {
            "alias": _text(account_map["alias"], "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID"),
            "class": _text(account_map["class"], "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID"),
        }
    else:
        canonical_account = None
    provider_sequence = raw["provider_sequence"]
    if provider_sequence is not None:
        provider_sequence = _sequence(
            provider_sequence, "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID",
        )
    if not isinstance(raw["payload"], Mapping):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID")
    canonical_wire = {
        "schema": L3F2_SCHEMA,
        "observation_id": _text(
            raw["observation_id"], "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID",
        ),
        "session_id": _text(raw["session_id"], "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID"),
        "observation_type": _text(
            raw["observation_type"], "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID",
        ),
        "ninja_receipt_time": _utc(
            raw["ninja_receipt_time"], "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID",
        ),
        "local_monotonic_sequence": _sequence(
            raw["local_monotonic_sequence"], "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID",
        ),
        "provider_timestamp": None if raw["provider_timestamp"] is None else _utc(
            raw["provider_timestamp"], "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID",
        ),
        "provider_sequence": provider_sequence,
        "exchange_timestamp": None if raw["exchange_timestamp"] is None else _utc(
            raw["exchange_timestamp"], "PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID",
        ),
        "account": canonical_account,
        "payload": dict(raw["payload"]),
    }
    if canonical_wire != dict(raw) or canonical_wire["observation_type"] not in _MARKET_TYPES:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID")
    if canonical_account is not None:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_ACCOUNT_INVALID")
    try:
        parsed = NinjaTraderObservation.from_wire(_canonical(canonical_wire).decode("ascii"))
    except (NinjaTraderObservationError, TypeError, ValueError, RuntimeError) as exc:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_INVALID") from exc
    return canonical_wire, parsed


def build_perpetual_observation_proof(
    *, wire: object, source_envelope: object,
) -> dict[str, object]:
    """Bind one canonical wire to the policy envelope exported with the seed.

    Ordinary production ledgers intentionally suppress the high-frequency raw
    observation rows.  The envelope is therefore embedded here and made
    durable by the single seed-export row rather than pretending a suppressed
    per-observation ledger coordinate exists.
    """
    canonical_wire, observation = _wire(wire)
    envelope = _exact(
        source_envelope, _OBSERVATION_ENVELOPE_FIELDS,
        "PERPETUAL_STARTUP_SEED_OBSERVATION_PROVENANCE_INVALID",
    )
    context = _session_context({key: envelope[key] for key in _SESSION_FIELDS})
    payload_hash = canonical_hash(dict(observation.payload))
    if (
        envelope["observation_id"] != observation.observation_id
        or envelope["observation_type"] != observation.observation_type
        or envelope["observed_at"] != observation.ninja_receipt_time
        or envelope["ninja_receipt_time"] != observation.ninja_receipt_time
        or envelope["provider_timestamp"] != observation.provider_timestamp
        or envelope["exchange_timestamp"] != observation.exchange_timestamp
        or envelope["local_monotonic_sequence"] != observation.local_monotonic_sequence
        or envelope["source_payload_hash"] != payload_hash
        or not _same_session(envelope, context)
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_PROVENANCE_INVALID")
    base: dict[str, object] = {
        "schema": PERPETUAL_OBSERVATION_PROOF_SCHEMA,
        "wire": canonical_wire,
        "wire_sha256": observation.wire_hash(),
        "payload_sha256": payload_hash,
        "source_envelope": dict(envelope),
        "source_envelope_identity": "l3g-paper-observation-" + canonical_hash(envelope),
    }
    base["observation_proof_sha256"] = _digest(base)
    return base


def _observation_proof(value: object) -> dict[str, object]:
    raw = _exact(value, frozenset({
        "schema", "wire", "wire_sha256", "payload_sha256", "source_envelope",
        "source_envelope_identity", "observation_proof_sha256",
    }), "PERPETUAL_STARTUP_SEED_OBSERVATION_PROOF_INVALID")
    supplied = _hash(
        raw["observation_proof_sha256"],
        "PERPETUAL_STARTUP_SEED_OBSERVATION_PROOF_INVALID",
    )
    base = {key: item for key, item in raw.items() if key != "observation_proof_sha256"}
    if raw["schema"] != PERPETUAL_OBSERVATION_PROOF_SCHEMA or supplied != _digest(base):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_PROOF_INTEGRITY_FAILED")
    rebuilt = build_perpetual_observation_proof(
        wire=raw["wire"], source_envelope=raw["source_envelope"],
    )
    if rebuilt != dict(raw):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_PROOF_INVALID")
    return rebuilt


def _provenance_matches(
    payload: Mapping[str, object], observations: Mapping[str, Mapping[str, object]],
) -> None:
    ids, sequences, hashes = _aligned_provenance(
        payload["source_observation_ids"], payload["source_local_sequences"],
        payload["source_payload_hashes"], "PERPETUAL_STARTUP_SEED_PROVENANCE_INVALID",
    )
    for identifier, sequence, payload_hash in zip(ids, sequences, hashes):
        proof = observations.get(identifier)
        if proof is None:
            raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVENANCE_INCOMPLETE")
        wire = proof["wire"]
        if (
            wire["local_monotonic_sequence"] != sequence  # type: ignore[index]
            or proof["payload_sha256"] != payload_hash
        ):
            raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVENANCE_INVALID")


def _provenance_triples(
    payload: Mapping[str, object],
) -> list[tuple[str, int, str]]:
    ids, sequences, hashes = _aligned_provenance(
        payload["source_observation_ids"], payload["source_local_sequences"],
        payload["source_payload_hashes"], "PERPETUAL_STARTUP_SEED_PROVENANCE_INVALID",
    )
    return list(zip(ids, sequences, hashes))


def build_perpetual_boundary_bundle(
    *,
    candle_open_utc: str,
    candle_close_utc: str,
    decision_observed_at: str,
    bias: str,
    fallback_observation_id: str,
    session_context: object,
    evidence: Sequence[object],
    decision: object,
    source_observation_ids: Sequence[str],
) -> dict[str, object]:
    """Build one closed-candle evaluation bundle (without granting authority)."""
    context = _session_context(session_context)
    opened = _utc(candle_open_utc, "PERPETUAL_STARTUP_SEED_BOUNDARY_INVALID")
    closed = _utc(candle_close_utc, "PERPETUAL_STARTUP_SEED_BOUNDARY_INVALID")
    observed = _utc(decision_observed_at, "PERPETUAL_STARTUP_SEED_BOUNDARY_INVALID")
    if (
        (_moment(closed) - _moment(opened)).total_seconds() != 300
        or int(_moment(closed).timestamp()) % 300 != 0
        or _moment(observed) < _moment(closed)
        or bias not in {"LONG", "SHORT", "TIE"}
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_BOUNDARY_INVALID")
    canonical_evidence = sorted(
        (_evidence(item, context=context, boundary=closed) for item in evidence),
        key=lambda item: str(item["evidence_id"]),
    )
    if not canonical_evidence or len(canonical_evidence) != len({
        item["evidence_id"] for item in canonical_evidence
    }):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_EVIDENCE_INVALID")
    families = {str(item["family"]) for item in canonical_evidence}
    if not _REQUIRED_FAMILIES.issubset(families):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_EVIDENCE_FAMILIES_INCOMPLETE")
    canonical_decision = _decision(decision, context=context, boundary=closed)
    summary = _mapping(
        canonical_decision["family_summary"], "PERPETUAL_STARTUP_SEED_SIGNAL_INVALID",
    )
    if (
        summary.get("candle_open_utc") != opened
        or summary.get("candle_close_utc") != closed
        or summary.get("decision_observed_at") != observed
        or canonical_decision["created_at"] != observed
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_BOUNDARY_DECISION_MISMATCH")
    _validate_signal(canonical_decision, canonical_evidence, bias)
    source_ids = [_text(item, "PERPETUAL_STARTUP_SEED_BUNDLE_SOURCES_INVALID") for item in source_observation_ids]
    if not source_ids or len(source_ids) != len(set(source_ids)) or source_ids != sorted(source_ids):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_BUNDLE_SOURCES_INVALID")
    fallback = _text(
        fallback_observation_id, "PERPETUAL_STARTUP_SEED_BUNDLE_SOURCES_INVALID",
    )
    direct = {fallback}
    direct.update(str(item) for item in canonical_decision["source_observation_ids"])
    for item in canonical_evidence:
        direct.update(str(identifier) for identifier in item["source_observation_ids"])
    reference = summary.get("decision_reference_observation_id")
    if reference is not None:
        direct.add(_text(reference, "PERPETUAL_STARTUP_SEED_BUNDLE_SOURCES_INVALID"))
    if not direct.issubset(set(source_ids)):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_BUNDLE_SOURCES_INVALID")
    base: dict[str, object] = {
        "schema": PERPETUAL_BOUNDARY_BUNDLE_SCHEMA,
        "candle_open_utc": opened,
        "candle_close_utc": closed,
        "decision_observed_at": observed,
        "bias": bias,
        "fallback_observation_id": fallback,
        "session_context": context,
        "evidence": canonical_evidence,
        "decision": canonical_decision,
        "source_observation_ids": source_ids,
    }
    base["bundle_sha256"] = _digest(base)
    return base


def _bundle(
    value: object, observations: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    raw = _exact(value, frozenset({
        "schema", "candle_open_utc", "candle_close_utc", "decision_observed_at",
        "bias", "fallback_observation_id", "session_context", "evidence",
        "decision", "source_observation_ids", "bundle_sha256",
    }), "PERPETUAL_STARTUP_SEED_BUNDLE_INVALID")
    supplied = _hash(raw["bundle_sha256"], "PERPETUAL_STARTUP_SEED_BUNDLE_INVALID")
    base = {key: item for key, item in raw.items() if key != "bundle_sha256"}
    if raw["schema"] != PERPETUAL_BOUNDARY_BUNDLE_SCHEMA or supplied != _digest(base):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_BUNDLE_INTEGRITY_FAILED")
    evidence = raw["evidence"]
    source_ids = raw["source_observation_ids"]
    if not isinstance(evidence, list) or not isinstance(source_ids, list):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_BUNDLE_INVALID")
    rebuilt = build_perpetual_boundary_bundle(
        candle_open_utc=str(raw["candle_open_utc"]),
        candle_close_utc=str(raw["candle_close_utc"]),
        decision_observed_at=str(raw["decision_observed_at"]),
        bias=str(raw["bias"]),
        fallback_observation_id=str(raw["fallback_observation_id"]),
        session_context=raw["session_context"], evidence=evidence,
        decision=raw["decision"],
        source_observation_ids=[str(item) for item in source_ids],
    )
    if rebuilt != dict(raw):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_BUNDLE_INVALID")
    for item in rebuilt["evidence"]:  # type: ignore[index]
        _provenance_matches(item, observations)
        expected_observation_type = (
            "DEPTH" if item["family"] == EvidenceFamily.RESTING_LIQUIDITY.value
            else "TRADE"
        )
        if any(
            observations[identifier]["wire"]["observation_type"]  # type: ignore[index]
            != expected_observation_type
            for identifier, _sequence_value, _payload_hash in _provenance_triples(item)
        ):
            raise RuntimeError("PERPETUAL_STARTUP_SEED_EVIDENCE_PROVENANCE_INVALID")
        source_times = [
            _moment(str(observations[identifier]["wire"]["ninja_receipt_time"]))
            for identifier, _sequence_value, _payload_hash in _provenance_triples(item)
        ]
        if (
            not source_times
            or max(source_times) != _moment(str(item["observed_at"]))
            or any(moment >= _moment(str(rebuilt["candle_close_utc"])) for moment in source_times)
        ):
            raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVENANCE_TIME_INVALID")
    _provenance_matches(rebuilt["decision"], observations)  # type: ignore[arg-type]
    context = rebuilt["session_context"]
    for identifier in rebuilt["source_observation_ids"]:  # type: ignore[index]
        proof = observations.get(str(identifier))
        if proof is None:
            raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVENANCE_INCOMPLETE")
        envelope = proof["source_envelope"]
        if not _same_session(envelope, context):  # type: ignore[arg-type]
            raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVENANCE_SESSION_MISMATCH")
        if _moment(str(proof["wire"]["ninja_receipt_time"])) >= _moment(  # type: ignore[index]
            str(rebuilt["candle_close_utc"]),
        ):
            raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVENANCE_TIME_INVALID")
    decision_payload = rebuilt["decision"]  # type: ignore[assignment]
    wire_sessions = {
        str(observations[str(identifier)]["wire"]["session_id"])  # type: ignore[index]
        for identifier in rebuilt["source_observation_ids"]  # type: ignore[index]
    }
    if len(wire_sessions) != 1:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVIDER_SESSION_MISMATCH")
    fallback_proof = observations[str(rebuilt["fallback_observation_id"])]
    fallback_sequence = int(fallback_proof["wire"]["local_monotonic_sequence"])  # type: ignore[index]
    if fallback_sequence != max(
        int(observations[str(identifier)]["wire"]["local_monotonic_sequence"])  # type: ignore[index]
        for identifier in rebuilt["source_observation_ids"]  # type: ignore[index]
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_FALLBACK_PROVENANCE_INVALID")
    summary = rebuilt["decision"]["family_summary"]  # type: ignore[index]
    reference_proof = observations[str(summary["decision_reference_observation_id"])]
    if (
        summary["decision_reference_kind"] != "LAST_TRADE_BEFORE_BOUNDARY"
        or reference_proof["wire"]["observation_type"] != "TRADE"  # type: ignore[index]
        or reference_proof["wire"]["ninja_receipt_time"]  # type: ignore[index]
        != summary["decision_reference_observed_at"]
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_REFERENCE_PROVENANCE_INVALID")
    if rebuilt["bias"] == "TIE":
        expected_decision_sources = [(
            str(fallback_proof["wire"]["observation_id"]),  # type: ignore[index]
            int(fallback_proof["wire"]["local_monotonic_sequence"]),  # type: ignore[index]
            str(fallback_proof["payload_sha256"]),
        )]
    else:
        hypothesis = (
            HypothesisKind.BULLISH_REVERSAL.value
            if rebuilt["bias"] == "LONG"
            else HypothesisKind.BEARISH_CONTINUATION.value
        )
        expected_decision_sources = sorted({
            triple
            for item in rebuilt["evidence"]  # type: ignore[index]
            if item["hypothesis_kind"] == hypothesis
            for triple in _provenance_triples(item)
        }, key=lambda item: (item[1], item[0]))
    if _provenance_triples(decision_payload) != expected_decision_sources:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_DECISION_PROVENANCE_INVALID")
    direct = {str(rebuilt["fallback_observation_id"])}
    direct.update(str(item) for item in rebuilt["decision"]["source_observation_ids"])  # type: ignore[index]
    for item in rebuilt["evidence"]:  # type: ignore[index]
        direct.update(str(identifier) for identifier in item["source_observation_ids"])
    reference = summary.get("decision_reference_observation_id")
    if isinstance(reference, str) and reference:
        direct.add(reference)
    if _transitive_closure(direct, observations) != set(rebuilt["source_observation_ids"]):  # type: ignore[arg-type]
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_CLOSURE_INVALID")
    return rebuilt


def _transitive_closure(
    bundle_ids: set[str], observations: Mapping[str, Mapping[str, object]],
) -> set[str]:
    closure = set(bundle_ids)
    pending = list(bundle_ids)
    while pending:
        identifier = pending.pop()
        proof = observations.get(identifier)
        if proof is None:
            raise RuntimeError("PERPETUAL_STARTUP_SEED_PROVENANCE_INCOMPLETE")
        wire = proof["wire"]
        if wire["observation_type"] != "TRADE":  # type: ignore[index]
            continue
        payload = wire["payload"]  # type: ignore[index]
        reference = payload.get("derivation_quote_observation_id")  # type: ignore[union-attr]
        if payload.get("aggressor_source") == "BID_ASK_CLASSIFICATION" and not (  # type: ignore[union-attr]
            isinstance(reference, str) and reference
        ):
            raise RuntimeError("PERPETUAL_STARTUP_SEED_TRANSITIVE_PROVENANCE_INVALID")
        if isinstance(reference, str) and reference and reference not in closure:
            closure.add(reference)
            pending.append(reference)
    return closure


def build_perpetual_startup_seed_core(
    *,
    operation_id: str,
    created_at: str,
    source_ledger: object,
    current_five_minute_boundary_utc: str,
    latest_completed: object,
    latest_non_tied: object,
    boundary_chain: Sequence[object],
    observations: Sequence[object],
) -> dict[str, object]:
    """Build the sealed seed core which the source export row will attest."""
    operation = _text(operation_id, "PERPETUAL_STARTUP_SEED_OPERATION_INVALID")
    if _OPERATION_ID.fullmatch(operation) is None:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OPERATION_INVALID")
    created = _utc(created_at, "PERPETUAL_STARTUP_SEED_CREATED_AT_INVALID")
    boundary = _utc(
        current_five_minute_boundary_utc,
        "PERPETUAL_STARTUP_SEED_CURRENT_BOUNDARY_INVALID",
    )
    boundary_moment = _moment(boundary)
    created_moment = _moment(created)
    if (
        int(boundary_moment.timestamp()) % 300 != 0
        or not boundary_moment <= created_moment < boundary_moment + timedelta(seconds=300)
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_CURRENT_BOUNDARY_INVALID")
    canonical_observations = [_observation_proof(item) for item in observations]
    canonical_observations.sort(key=lambda item: (
        str(item["wire"]["session_id"]),  # type: ignore[index]
        int(item["wire"]["local_monotonic_sequence"]),  # type: ignore[index]
        str(item["wire"]["observation_id"]),  # type: ignore[index]
    ))
    by_id: dict[str, Mapping[str, object]] = {}
    coordinates: set[tuple[str, int]] = set()
    for proof in canonical_observations:
        identifier = str(proof["wire"]["observation_id"])  # type: ignore[index]
        coordinate = (
            str(proof["wire"]["session_id"]),  # type: ignore[index]
            int(proof["wire"]["local_monotonic_sequence"]),  # type: ignore[index]
        )
        if identifier in by_id or coordinate in coordinates:
            raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_SET_INVALID")
        by_id[identifier] = proof
        coordinates.add(coordinate)
    completed = _bundle(latest_completed, by_id)
    non_tied = _bundle(latest_non_tied, by_id)
    if isinstance(boundary_chain, (str, bytes)) or not boundary_chain:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_BOUNDARY_CHAIN_INVALID")
    chain = [_bundle(item, by_id) for item in boundary_chain]
    if completed["candle_close_utc"] != boundary:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_CURRENT_BOUNDARY_MISMATCH")
    if any(
        _moment(str(bundle["decision_observed_at"])) > created_moment
        for bundle in chain
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_CREATED_AT_INVALID")
    if (
        chain[0] != non_tied
        or chain[-1] != completed
        or chain[0]["bias"] not in {"LONG", "SHORT"}
        or any(item["bias"] != "TIE" for item in chain[1:])
        or any(
            previous["candle_close_utc"] != following["candle_open_utc"]
            for previous, following in zip(chain, chain[1:])
        )
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_BOUNDARY_CHAIN_INVALID")
    claimed = {
        str(identifier)
        for item in chain
        for identifier in item["source_observation_ids"]
    }
    closure = _transitive_closure(claimed, by_id)
    if closure != set(by_id):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OBSERVATION_CLOSURE_INVALID")
    base: dict[str, object] = {
        "schema": PERPETUAL_STARTUP_SEED_CORE_SCHEMA,
        "operation_id": operation,
        "created_at": created,
        "account_binding": _account_binding(),
        "source_profile": _profile_binding(FIVE_MINUTE_PROFILE),
        "target_profile": _profile_binding(FIVE_MINUTE_PERPETUAL_PROFILE),
        "source_ledger": _source_ledger(source_ledger),
        "current_five_minute_boundary_utc": boundary,
        "latest_completed": completed,
        "latest_non_tied": non_tied,
        "boundary_chain": chain,
        "observations": canonical_observations,
    }
    base["seed_core_sha256"] = _digest(base)
    return base


def validate_perpetual_startup_seed_core(
    value: object, *, operation_id: str | None = None, expected_at: str | None = None,
) -> dict[str, object]:
    """Validate and return one canonical startup-seed core."""
    raw = _exact(value, frozenset({
        "schema", "operation_id", "created_at", "account_binding", "source_profile",
        "target_profile", "source_ledger", "current_five_minute_boundary_utc",
        "latest_completed", "latest_non_tied", "boundary_chain", "observations",
        "seed_core_sha256",
    }), "PERPETUAL_STARTUP_SEED_CORE_INVALID")
    supplied = _hash(raw["seed_core_sha256"], "PERPETUAL_STARTUP_SEED_CORE_INVALID")
    base = {key: item for key, item in raw.items() if key != "seed_core_sha256"}
    if raw["schema"] != PERPETUAL_STARTUP_SEED_CORE_SCHEMA or supplied != _digest(base):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_CORE_INTEGRITY_FAILED")
    observations = raw["observations"]
    boundary_chain = raw["boundary_chain"]
    if not isinstance(observations, list) or not isinstance(boundary_chain, list):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_CORE_INVALID")
    rebuilt = build_perpetual_startup_seed_core(
        operation_id=str(raw["operation_id"]), created_at=str(raw["created_at"]),
        source_ledger=raw["source_ledger"],
        current_five_minute_boundary_utc=str(raw["current_five_minute_boundary_utc"]),
        latest_completed=raw["latest_completed"], latest_non_tied=raw["latest_non_tied"],
        boundary_chain=boundary_chain, observations=observations,
    )
    if rebuilt != dict(raw):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_CORE_INVALID")
    if operation_id is not None and rebuilt["operation_id"] != operation_id:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OPERATION_MISMATCH")
    if expected_at is not None:
        expected = _utc(expected_at, "PERPETUAL_STARTUP_SEED_EXPECTED_AT_INVALID")
        expected_epoch = int(_moment(expected).timestamp())
        expected_boundary = datetime.fromtimestamp(
            expected_epoch - expected_epoch % 300, tz=timezone.utc,
        ).isoformat().replace("+00:00", "Z")
        if rebuilt["current_five_minute_boundary_utc"] != expected_boundary:
            raise RuntimeError("PERPETUAL_STARTUP_SEED_BOUNDARY_STALE")
    return rebuilt


def perpetual_startup_seed_export_payload(core: object) -> dict[str, object]:
    """Return the exact payload which must be appended to the V1 source ledger."""
    seed = validate_perpetual_startup_seed_core(core)
    source = seed["source_profile"]
    target = seed["target_profile"]
    ledger = seed["source_ledger"]
    boundary_chain = seed["boundary_chain"]
    observations = seed["observations"]
    boundary_hashes = [item["bundle_sha256"] for item in boundary_chain]  # type: ignore[index]
    observation_hashes = [item["observation_proof_sha256"] for item in observations]  # type: ignore[index]
    return {
        "schema": PERPETUAL_STARTUP_SEED_EXPORT_SCHEMA,
        "operation_id": seed["operation_id"],
        "seed_core_sha256": seed["seed_core_sha256"],
        "source_profile": source["selection_key"],  # type: ignore[index]
        "source_policy_id": source["policy_id"],  # type: ignore[index]
        "source_policy_sha256": source["policy_sha256"],  # type: ignore[index]
        "source_risk_profile_id": source["risk_profile_id"],  # type: ignore[index]
        "source_risk_sha256": source["risk_sha256"],  # type: ignore[index]
        "target_profile": target["selection_key"],  # type: ignore[index]
        "target_policy_id": target["policy_id"],  # type: ignore[index]
        "target_policy_sha256": target["policy_sha256"],  # type: ignore[index]
        "target_risk_profile_id": target["risk_profile_id"],  # type: ignore[index]
        "target_risk_sha256": target["risk_sha256"],  # type: ignore[index]
        "account_binding_sha256": ACCOUNT_BINDING.binding_hash,
        "current_five_minute_boundary_utc": seed["current_five_minute_boundary_utc"],
        "latest_completed_bundle_sha256": seed["latest_completed"]["bundle_sha256"],  # type: ignore[index]
        "latest_non_tied_bundle_sha256": seed["latest_non_tied"]["bundle_sha256"],  # type: ignore[index]
        "boundary_chain_count": len(boundary_chain),  # type: ignore[arg-type]
        "boundary_chain_sha256": _digest(boundary_hashes),
        "observation_count": len(observations),  # type: ignore[arg-type]
        "observation_set_sha256": _digest(observation_hashes),
        "source_ledger_identity": ledger["ledger_identity"],  # type: ignore[index]
        "source_ledger_epoch": ledger["ledger_epoch"],  # type: ignore[index]
    }


def perpetual_startup_seed_export_identity(core: object) -> str:
    seed = validate_perpetual_startup_seed_core(core)
    return "l3g-perpetual-startup-seed-export-" + str(seed["seed_core_sha256"])[:32]


def _export_record(value: object, core: Mapping[str, object]) -> dict[str, object]:
    row = _ledger_record(
        value, expected_kind=PERPETUAL_STARTUP_SEED_EXPORT_KIND,
        payload_fields=_EXPORT_PAYLOAD_FIELDS,
    )
    payload = row["record"]["payload"]  # type: ignore[index]
    expected = perpetual_startup_seed_export_payload(core)
    supplied_family = payload.get("session_family")
    if (
        {key: payload[key] for key in expected} != expected
        or supplied_family != row["record"]["session_family"]  # type: ignore[index]
        or row["record"]["identity"] != perpetual_startup_seed_export_identity(core)  # type: ignore[index]
        or _moment(str(row["record"]["occurred_at"])) < _moment(str(core["created_at"]))  # type: ignore[index]
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_EXPORT_RECORD_INVALID")
    return row


def validate_perpetual_startup_seed_artifact(
    value: object, *, operation_id: str | None = None, expected_at: str | None = None,
) -> dict[str, object]:
    raw = _exact(value, frozenset({
        "schema", "core", "export_record", "artifact_sha256",
    }), "PERPETUAL_STARTUP_SEED_ARTIFACT_INVALID")
    supplied = _hash(raw["artifact_sha256"], "PERPETUAL_STARTUP_SEED_ARTIFACT_INVALID")
    base = {key: item for key, item in raw.items() if key != "artifact_sha256"}
    if raw["schema"] != PERPETUAL_STARTUP_SEED_ARTIFACT_SCHEMA or supplied != _digest(base):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_ARTIFACT_INTEGRITY_FAILED")
    core = validate_perpetual_startup_seed_core(
        raw["core"], operation_id=operation_id, expected_at=expected_at,
    )
    export = _export_record(raw["export_record"], core)
    result = {
        "schema": PERPETUAL_STARTUP_SEED_ARTIFACT_SCHEMA,
        "core": core,
        "export_record": export,
        "artifact_sha256": supplied,
    }
    if result != dict(raw):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_ARTIFACT_INVALID")
    return result


def _write_exclusive(
    path: Path, value: Mapping[str, object], *, maximum_bytes: int,
) -> None:
    path = Path(path)
    data = _canonical(value) + b"\n"
    if len(data) > maximum_bytes:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_ARTIFACT_TOO_LARGE")
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{os.urandom(8).hex()}.tmp",
    )
    descriptor = os.open(temporary, flags, 0o600)
    write_complete = False
    try:
        offset = 0
        while offset < len(data):
            written = os.write(descriptor, data[offset:])
            if written <= 0:
                raise OSError("short durable artifact write")
            offset += written
        os.fsync(descriptor)
        write_complete = True
    finally:
        os.close(descriptor)
        if not write_complete:
            temporary.unlink(missing_ok=True)
    try:
        # A same-directory hard-link promotion is atomic and cannot replace
        # an existing final artifact. An interruption can leave only a hidden
        # temporary file, never a partial path that blocks a truthful retry.
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    if os.name != "nt":  # pragma: no cover - production host is Windows
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def write_perpetual_startup_seed_artifact(
    path: str | Path, *, core: object, export_record: object,
) -> dict[str, object]:
    seed = validate_perpetual_startup_seed_core(core)
    export = _export_record(export_record, seed)
    artifact: dict[str, object] = {
        "schema": PERPETUAL_STARTUP_SEED_ARTIFACT_SCHEMA,
        "core": seed,
        "export_record": export,
    }
    artifact["artifact_sha256"] = _digest(artifact)
    validated = validate_perpetual_startup_seed_artifact(artifact)
    _write_exclusive(Path(path), validated, maximum_bytes=_MAX_SEED_BYTES)
    return validated


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _read_json(path: Path, *, maximum_bytes: int, code: str) -> object:
    try:
        size = path.stat().st_size
        if size <= 0 or size > maximum_bytes:
            raise ValueError("artifact size")
        return json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=_strict_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError("non-finite JSON")),
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(code) from exc


def read_perpetual_startup_seed_artifact(
    path: str | Path, *, operation_id: str | None = None, expected_at: str | None = None,
) -> dict[str, object]:
    raw = _read_json(
        Path(path), maximum_bytes=_MAX_SEED_BYTES,
        code="PERPETUAL_STARTUP_SEED_ARTIFACT_UNREADABLE",
    )
    return validate_perpetual_startup_seed_artifact(
        raw, operation_id=operation_id, expected_at=expected_at,
    )


def _shutdown_tip(
    receipt: object, *, source_ledger: Mapping[str, object], minimum_sequence: int,
) -> dict[str, object]:
    raw = _mapping(receipt, "PERPETUAL_STARTUP_SEED_SHUTDOWN_UNPROVEN")
    expected_sequence = _sequence(
        raw.get("expected_tip_sequence"), "PERPETUAL_STARTUP_SEED_SHUTDOWN_UNPROVEN",
        positive=True,
    )
    durable_sequence = _sequence(
        raw.get("durable_tip_sequence"), "PERPETUAL_STARTUP_SEED_SHUTDOWN_UNPROVEN",
        positive=True,
    )
    expected_hash = _hash(
        raw.get("expected_tip_hash"), "PERPETUAL_STARTUP_SEED_SHUTDOWN_UNPROVEN",
    )
    durable_hash = _hash(
        raw.get("durable_tip_hash"), "PERPETUAL_STARTUP_SEED_SHUTDOWN_UNPROVEN",
    )
    checkpoint = raw.get("checkpoint")
    verifier_shutdown = raw.get("verifier_shutdown")
    watchdog_shutdown = raw.get("runtime_watchdog_shutdown")
    risk_boundary = raw.get("risk_continuity_boundary")
    if (
        raw.get("schema") != "l3g-ledger-controlled-shutdown-v1"
        or raw.get("clean_shutdown") is not True
        or raw.get("admission_sealed") is not True
        or raw.get("writer_stopped") is not True
        or not isinstance(checkpoint, Mapping) or checkpoint.get("complete") is not True
        or not isinstance(verifier_shutdown, Mapping)
        or verifier_shutdown.get("completed") is not True
        or not isinstance(watchdog_shutdown, Mapping)
        or watchdog_shutdown.get("completed") is not True
        or expected_sequence != durable_sequence
        or expected_hash != durable_hash
        or durable_sequence < minimum_sequence
        or not isinstance(risk_boundary, Mapping)
        or set(risk_boundary) != {
            "path", "ledger_identity", "ledger_epoch", "risk_boundary_sequence",
            "risk_boundary_hash",
        }
        or risk_boundary.get("path") != source_ledger["path"]
        or risk_boundary.get("ledger_identity") != source_ledger["ledger_identity"]
        or risk_boundary.get("ledger_epoch") != source_ledger["ledger_epoch"]
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_SHUTDOWN_UNPROVEN")
    risk_sequence = _sequence(
        risk_boundary.get("risk_boundary_sequence"),
        "PERPETUAL_STARTUP_SEED_SHUTDOWN_UNPROVEN",
    )
    risk_hash = risk_boundary.get("risk_boundary_hash")
    if not (
        (risk_sequence == 0 and risk_hash is None)
        or (
            0 < risk_sequence <= durable_sequence
            and isinstance(risk_hash, str) and _HASH.fullmatch(risk_hash)
        )
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_SHUTDOWN_UNPROVEN")
    closed_at = _utc(
        raw.get("closed_at"), "PERPETUAL_STARTUP_SEED_SHUTDOWN_UNPROVEN",
    )
    return {
        "closed_at": closed_at,
        "tip_sequence": durable_sequence,
        "tip_sha256": durable_hash,
    }


def _verification(
    report: object, *, source_ledger: Mapping[str, object],
    shutdown_tip: Mapping[str, object],
) -> dict[str, object]:
    raw = _mapping(report, "PERPETUAL_STARTUP_SEED_SOURCE_VERIFICATION_FAILED")
    sequence = _sequence(
        raw.get("verified_through_sequence"),
        "PERPETUAL_STARTUP_SEED_SOURCE_VERIFICATION_FAILED", positive=True,
    )
    tip_hash = _hash(
        raw.get("tip_hash"), "PERPETUAL_STARTUP_SEED_SOURCE_VERIFICATION_FAILED",
    )
    if (
        raw.get("status") != "PASS"
        or raw.get("verification_mode") != "full"
        or raw.get("chain_valid") is not True
        or raw.get("quick_check") != "ok"
        or raw.get("ledger_path") != source_ledger["path"]
        or raw.get("ledger_identity") != source_ledger["ledger_identity"]
        or raw.get("ledger_epoch") != source_ledger["ledger_epoch"]
        or sequence != shutdown_tip["tip_sequence"]
        or tip_hash != shutdown_tip["tip_sha256"]
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_SOURCE_VERIFICATION_FAILED")
    verification_id = _text(
        raw.get("verification_id"), "PERPETUAL_STARTUP_SEED_SOURCE_VERIFICATION_FAILED",
    )
    if _VERIFICATION_ID.fullmatch(verification_id) is None:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_SOURCE_VERIFICATION_FAILED")
    completed_at = _utc(
        raw.get("completed_at"), "PERPETUAL_STARTUP_SEED_SOURCE_VERIFICATION_FAILED",
    )
    return {
        "verification_id": verification_id,
        "completed_at": completed_at,
        "verification_mode": "full",
        "status": "PASS",
        "chain_valid": True,
        "quick_check": "ok",
        "ledger_path": source_ledger["path"],
        "ledger_identity": source_ledger["ledger_identity"],
        "ledger_epoch": source_ledger["ledger_epoch"],
        "verified_through_sequence": sequence,
        "tip_sha256": tip_hash,
    }


def validate_perpetual_startup_seed_proof(
    value: object, *, artifact: object, operation_id: str | None = None,
    expected_at: str | None = None,
) -> dict[str, object]:
    seed_artifact = validate_perpetual_startup_seed_artifact(
        artifact, operation_id=operation_id, expected_at=expected_at,
    )
    raw = _exact(value, frozenset({
        "schema", "operation_id", "created_at", "manifest_sha256",
        "seed_artifact_sha256", "source_ledger", "bound_source_max_sequence",
        "shutdown_tip", "verification", "proof_sha256",
    }), "PERPETUAL_STARTUP_SEED_PROOF_INVALID")
    supplied = _hash(raw["proof_sha256"], "PERPETUAL_STARTUP_SEED_PROOF_INVALID")
    base = {key: item for key, item in raw.items() if key != "proof_sha256"}
    if raw["schema"] != PERPETUAL_STARTUP_SEED_PROOF_SCHEMA or supplied != _digest(base):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_PROOF_INTEGRITY_FAILED")
    core = seed_artifact["core"]
    source_ledger = _source_ledger(raw["source_ledger"])
    shutdown = _exact(
        raw["shutdown_tip"], frozenset({"closed_at", "tip_sequence", "tip_sha256"}),
        "PERPETUAL_STARTUP_SEED_PROOF_INVALID",
    )
    verification = _exact(raw["verification"], frozenset({
        "verification_id", "completed_at", "verification_mode", "status", "chain_valid",
        "quick_check", "ledger_path", "ledger_identity", "ledger_epoch",
        "verified_through_sequence", "tip_sha256",
    }), "PERPETUAL_STARTUP_SEED_PROOF_INVALID")
    maximum_sequence = int(seed_artifact["export_record"]["ledger_sequence"])  # type: ignore[index]
    created = _utc(raw["created_at"], "PERPETUAL_STARTUP_SEED_PROOF_INVALID")
    if (
        raw["operation_id"] != core["operation_id"]
        or raw["seed_artifact_sha256"] != seed_artifact["artifact_sha256"]
        or source_ledger != core["source_ledger"]
        or raw["bound_source_max_sequence"] != maximum_sequence
        or _hash(raw["manifest_sha256"], "PERPETUAL_STARTUP_SEED_PROOF_INVALID")
        != raw["manifest_sha256"]
        or _sequence(shutdown["tip_sequence"], "PERPETUAL_STARTUP_SEED_PROOF_INVALID", positive=True)
        != verification["verified_through_sequence"]
        or _hash(shutdown["tip_sha256"], "PERPETUAL_STARTUP_SEED_PROOF_INVALID")
        != verification["tip_sha256"]
        or shutdown["tip_sequence"] < maximum_sequence
        or verification["verification_mode"] != "full"
        or verification["status"] != "PASS"
        or verification["chain_valid"] is not True
        or verification["quick_check"] != "ok"
        or verification["ledger_path"] != source_ledger["path"]
        or verification["ledger_identity"] != source_ledger["ledger_identity"]
        or verification["ledger_epoch"] != source_ledger["ledger_epoch"]
        or _moment(created) < _moment(_utc(
            verification["completed_at"], "PERPETUAL_STARTUP_SEED_PROOF_INVALID",
        ))
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_PROOF_INVALID")
    closed_at = _utc(shutdown["closed_at"], "PERPETUAL_STARTUP_SEED_PROOF_INVALID")
    if (
        _VERIFICATION_ID.fullmatch(str(verification["verification_id"])) is None
        or _moment(str(verification["completed_at"])) < _moment(closed_at)
    ):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_PROOF_INVALID")
    if operation_id is not None and raw["operation_id"] != operation_id:
        raise RuntimeError("PERPETUAL_STARTUP_SEED_OPERATION_MISMATCH")
    return dict(raw)


def write_perpetual_startup_seed_proof(
    path: str | Path,
    *,
    artifact: object,
    manifest_sha256: str,
    shutdown_receipt: object,
    verification_report: object,
    created_at: str | None = None,
) -> dict[str, object]:
    seed_artifact = validate_perpetual_startup_seed_artifact(artifact)
    core = seed_artifact["core"]
    source_ledger = core["source_ledger"]
    maximum_sequence = int(seed_artifact["export_record"]["ledger_sequence"])  # type: ignore[index]
    shutdown = _shutdown_tip(
        shutdown_receipt, source_ledger=source_ledger, minimum_sequence=maximum_sequence,
    )
    verification = _verification(
        verification_report, source_ledger=source_ledger, shutdown_tip=shutdown,
    )
    created = _utc(
        created_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "PERPETUAL_STARTUP_SEED_PROOF_INVALID",
    )
    if _moment(created) < _moment(str(verification["completed_at"])):
        raise RuntimeError("PERPETUAL_STARTUP_SEED_PROOF_INVALID")
    proof: dict[str, object] = {
        "schema": PERPETUAL_STARTUP_SEED_PROOF_SCHEMA,
        "operation_id": core["operation_id"],
        "created_at": created,
        "manifest_sha256": _hash(
            manifest_sha256, "PERPETUAL_STARTUP_SEED_PROOF_INVALID",
        ),
        "seed_artifact_sha256": seed_artifact["artifact_sha256"],
        "source_ledger": source_ledger,
        "bound_source_max_sequence": maximum_sequence,
        "shutdown_tip": shutdown,
        "verification": verification,
    }
    proof["proof_sha256"] = _digest(proof)
    validated = validate_perpetual_startup_seed_proof(
        proof, artifact=seed_artifact,
    )
    _write_exclusive(Path(path), validated, maximum_bytes=_MAX_PROOF_BYTES)
    return validated


def read_perpetual_startup_seed_proof(
    path: str | Path, *, artifact: object, operation_id: str | None = None,
    expected_at: str | None = None,
) -> dict[str, object]:
    raw = _read_json(
        Path(path), maximum_bytes=_MAX_PROOF_BYTES,
        code="PERPETUAL_STARTUP_SEED_PROOF_UNREADABLE",
    )
    return validate_perpetual_startup_seed_proof(
        raw, artifact=artifact, operation_id=operation_id, expected_at=expected_at,
    )


__all__ = [
    "PERPETUAL_BOUNDARY_BUNDLE_SCHEMA",
    "PERPETUAL_OBSERVATION_PROOF_SCHEMA",
    "PERPETUAL_STARTUP_SEED_ARTIFACT_SCHEMA",
    "PERPETUAL_STARTUP_SEED_CORE_SCHEMA",
    "PERPETUAL_STARTUP_SEED_EXPORT_KIND",
    "PERPETUAL_STARTUP_SEED_EXPORT_SCHEMA",
    "PERPETUAL_STARTUP_SEED_PROOF_SCHEMA",
    "build_perpetual_boundary_bundle",
    "build_perpetual_observation_proof",
    "build_perpetual_startup_seed_core",
    "perpetual_startup_seed_export_identity",
    "perpetual_startup_seed_export_payload",
    "read_perpetual_startup_seed_artifact",
    "read_perpetual_startup_seed_proof",
    "validate_perpetual_startup_seed_artifact",
    "validate_perpetual_startup_seed_core",
    "validate_perpetual_startup_seed_proof",
    "write_perpetual_startup_seed_artifact",
    "write_perpetual_startup_seed_proof",
]
