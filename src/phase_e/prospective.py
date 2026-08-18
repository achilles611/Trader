"""Phase E.5 prospective experimental protocol and enforcement primitives.

This module is intentionally separate from the historical E.2--E.4 pipeline.
It accepts only outcome-blind, prospectively admitted design records until a
frozen protocol has passed its maturity, dependence, concentration, support,
and missingness gates.  It has no prediction, signal, execution, or trading
authority.

The checked-in E.5 protocol is the scientific authority.  The classes here
make its important boundaries executable without starting the experiment.
"""

from __future__ import annotations

import copy
import json
import math
import random
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from .types import canonical_hash, finite_number, normalized_utc, storage_json


E5_PROTOCOL_SCHEMA = "phase-e5-prospective-protocol-v1"
E5_OBSERVATION_SCHEMA = "phase-e5-prospective-observation-v1"
E5_RESULT_SCHEMA = "phase-e5-prospective-result-v1"
E5_REPLAY_SCHEMA = "phase-e5-scientific-replay-v1"
E5_CODE_VERSION = "phase-e5-prospective-framework-v1"
E5_CONFIG_VERSION = "phase-e5-preregistered-design-v1"
E5_STATISTICAL_METHOD = "RESTRICTED_STUDENTIZED_WILD_CLUSTER_BOOTSTRAP_T_LOCO_V1"
E5_MULTIPLICITY_METHOD = "HOLM_BONFERRONI_FWER_V1"
E5_HOLDOUT_POLICY = "FUTURE_TEST_RESERVED_ZERO_QUERY_V1"


class ProspectiveError(RuntimeError):
    """Base E.5 error."""


class ProtocolIntegrityError(ProspectiveError):
    """A frozen protocol, prospective lineage, or replay identity was violated."""


class ProtocolConflictError(ProspectiveError):
    """A caller attempted to replace immutable preregistered semantics."""


class OutcomeAccessForbidden(ProspectiveError):
    """Evaluation outcomes are unavailable through this capability."""


class InferenceRefused(ProspectiveError):
    """Scientific gates did not authorize inferential computation."""


class ExperimentState(StrEnum):
    PREREGISTERING = "PREREGISTERING"
    FROZEN_NOT_STARTED = "FROZEN_NOT_STARTED"
    COLLECTING = "COLLECTING"
    AWAITING_MATURITY = "AWAITING_MATURITY"
    INSUFFICIENT_SUPPORT = "INSUFFICIENT_SUPPORT"
    MISSINGNESS_GATE_FAILED = "MISSINGNESS_GATE_FAILED"
    CONCENTRATION_GATE_FAILED = "CONCENTRATION_GATE_FAILED"
    DEPENDENCE_GATE_FAILED = "DEPENDENCE_GATE_FAILED"
    PROTOCOL_INTEGRITY_FAILED = "PROTOCOL_INTEGRITY_FAILED"
    ELIGIBLE_FOR_INFERENCE = "ELIGIBLE_FOR_INFERENCE"
    INFERENCE_COMPLETED = "INFERENCE_COMPLETED"
    INCONCLUSIVE = "INCONCLUSIVE"
    SUPPORTED = "SUPPORTED"
    REJECTED = "REJECTED"


class EvidenceState(StrEnum):
    ADMISSIBLE_OBSERVED = "ADMISSIBLE_OBSERVED"
    IMMATURE = "IMMATURE"
    STRUCTURALLY_UNRESOLVED = "STRUCTURALLY_UNRESOLVED"
    MATURE_MISSING = "MATURE_MISSING"
    MISSING = "MISSING"
    STALE = "STALE"
    LATE = "LATE"
    INVALIDATING_MISSINGNESS = "INVALIDATING_MISSINGNESS"


TERMINAL_STATES = frozenset({
    ExperimentState.INSUFFICIENT_SUPPORT,
    ExperimentState.MISSINGNESS_GATE_FAILED,
    ExperimentState.CONCENTRATION_GATE_FAILED,
    ExperimentState.DEPENDENCE_GATE_FAILED,
    ExperimentState.PROTOCOL_INTEGRITY_FAILED,
    ExperimentState.INCONCLUSIVE,
    ExperimentState.SUPPORTED,
    ExperimentState.REJECTED,
})


def _utc(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Timestamp must be ISO-8601 text with an explicit offset.")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must have an explicit UTC offset.")
    return parsed.astimezone(timezone.utc)


def _positive_int(value: Any, name: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer of at least {minimum}.")
    return value


def protocol_semantic_payload(document: Mapping[str, Any]) -> dict[str, Any]:
    """Return the hash-bound protocol body without its two derived identifiers."""
    if not isinstance(document, Mapping):
        raise ProtocolIntegrityError("E.5 protocol must be a mapping.")
    payload = copy.deepcopy(dict(document))
    identity = payload.get("identity")
    if not isinstance(identity, dict):
        raise ProtocolIntegrityError("E.5 protocol identity is missing.")
    identity.pop("protocol_hash", None)
    identity.pop("protocol_id", None)
    return payload


def compute_protocol_hash(document: Mapping[str, Any]) -> str:
    return canonical_hash(protocol_semantic_payload(document))


def validate_protocol_document(document: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the frozen machine contract and its derived identity."""
    payload = copy.deepcopy(dict(document))
    if payload.get("schema") != E5_PROTOCOL_SCHEMA:
        raise ProtocolIntegrityError("Unsupported E.5 protocol schema.")
    identity = payload.get("identity")
    if not isinstance(identity, dict):
        raise ProtocolIntegrityError("E.5 protocol identity is missing.")
    expected_hash = compute_protocol_hash(payload)
    expected_id = "e5p-" + expected_hash[:32]
    if identity.get("protocol_hash") != expected_hash or identity.get("protocol_id") != expected_id:
        raise ProtocolIntegrityError("E.5 protocol identity does not match its semantic payload.")
    if identity.get("hash_algorithm") != "PHASE_E_TYPE_TAGGED_SHA256_V1":
        raise ProtocolIntegrityError("E.5 protocol hash algorithm is not frozen correctly.")
    if payload.get("lifecycle", {}).get("initial_state") != ExperimentState.FROZEN_NOT_STARTED.value:
        raise ProtocolIntegrityError("Checked-in E.5 protocol must be frozen and not started.")
    if payload.get("authority") != {"prediction": False, "signal": False, "execution": False, "trading": False}:
        raise ProtocolIntegrityError("E.5 protocol cannot hold operational authority.")
    if payload.get("protected_data", {}).get("maximum_reserved_test_queries") != 0:
        raise ProtocolIntegrityError("E.5 design must reserve the test partition with zero queries.")
    if payload.get("protected_data", {}).get("prefreeze_evaluation_outcome_reads") != 0:
        raise ProtocolIntegrityError("E.5 preregistration must record zero evaluation-outcome reads.")

    schedule = payload.get("sampling", {}).get("schedule", {})
    if schedule.get("algorithm") != "HASHED_SLOT_WITHIN_FIXED_8DAY_EPOCH_V1":
        raise ProtocolIntegrityError("E.5 schedule algorithm is not supported.")
    _positive_int(schedule.get("block_count"), "Block count", minimum=2)
    _positive_int(schedule.get("epoch_seconds"), "Epoch seconds")
    _positive_int(schedule.get("slot_seconds"), "Slot seconds")
    _positive_int(schedule.get("slots_per_epoch_prefix"), "Slots per epoch prefix")
    if schedule["slot_seconds"] * schedule["slots_per_epoch_prefix"] >= schedule["epoch_seconds"]:
        raise ProtocolIntegrityError("Schedule prefix must leave a positive inter-epoch cooldown.")
    _utc(schedule.get("acquisition_start"))
    _utc(schedule.get("hard_stop"))
    if payload.get("inference", {}).get("method") != E5_STATISTICAL_METHOD:
        raise ProtocolIntegrityError("E.5 inferential method changed.")
    if payload.get("multiplicity", {}).get("method") != E5_MULTIPLICITY_METHOD:
        raise ProtocolIntegrityError("E.5 multiplicity method changed.")
    if payload.get("missingness", {}).get("correction") != "NONE_COMPLETE_RESOLUTION_REQUIRED_V1":
        raise ProtocolIntegrityError("E.5 v1 does not authorize post-hoc missingness correction.")
    if payload.get("historical_compatibility", {}).get("e4_rows_eligible") is not False:
        raise ProtocolIntegrityError("Historical E.4 rows must be ineligible for E.5.")
    if payload.get("protocol_version") != 1:
        raise ProtocolIntegrityError("This implementation supports only the frozen E.5 v1 protocol.")
    if {
        "block_count": schedule.get("block_count"),
        "epoch_seconds": schedule.get("epoch_seconds"),
        "slot_seconds": schedule.get("slot_seconds"),
        "slots_per_epoch_prefix": schedule.get("slots_per_epoch_prefix"),
        "sample_duration_seconds": schedule.get("sample_duration_seconds"),
        "minimum_separation_seconds": schedule.get("minimum_separation_seconds"),
        "replacement_blocks": schedule.get("replacement_blocks"),
    } != {
        "block_count": 60, "epoch_seconds": 691200, "slot_seconds": 1800,
        "slots_per_epoch_prefix": 47, "sample_duration_seconds": 1800,
        "minimum_separation_seconds": 604800, "replacement_blocks": 0,
    }:
        raise ProtocolIntegrityError("E.5 v1 schedule semantics changed.")
    symbol_rule = payload.get("sampling", {}).get("symbol_eligibility", {})
    if {
        "rule": symbol_rule.get("rule"),
        "lookback_24h_minimum_prints": symbol_rule.get("lookback_24h_minimum_prints"),
        "lookback_30m_minimum_prints": symbol_rule.get("lookback_30m_minimum_prints"),
        "lookback_30m_maximum_interprint_gap_seconds": symbol_rule.get("lookback_30m_maximum_interprint_gap_seconds"),
        "source_discontinuity_allowed": symbol_rule.get("source_discontinuity_allowed"),
    } != {
        "rule": "PREANCHOR_CONTINUOUS_TRADE_LIQUIDITY_V1",
        "lookback_24h_minimum_prints": 172800,
        "lookback_30m_minimum_prints": 3600,
        "lookback_30m_maximum_interprint_gap_seconds": 2,
        "source_discontinuity_allowed": False,
    }:
        raise ProtocolIntegrityError("E.5 v1 pre-anchor symbol eligibility changed.")
    support = payload.get("admissibility", {}).get("minimum_support", {})
    if support != {
        "observations": 600, "observations_per_arm": 240, "blocks": 48,
        "mixed_blocks": 40, "effective_blocks": 40.0,
        "effective_blocks_per_arm": 40.0, "effective_contrast_blocks": 40.0,
        "effective_symbols": 12.0,
    }:
        raise ProtocolIntegrityError("E.5 v1 minimum-support contract changed.")
    maximum_shares = payload.get("admissibility", {}).get("maximum_shares", {})
    if maximum_shares != {
        "block": 0.05, "predicate_block": 0.05, "complement_block": 0.05,
        "component": 0.05, "wallet": 0.01, "symbol": 0.10,
        "endpoint_family": 0.025, "local_time_window": 0.025,
        "contrast_information": 0.05,
    }:
        raise ProtocolIntegrityError("E.5 v1 concentration contract changed.")
    family = payload.get("hypothesis_family", {})
    members = family.get("members", [])
    if [item.get("hypothesis_id") for item in members] != ["wallet-action-gt-zero", "wallet-action-lt-zero"]:
        raise ProtocolIntegrityError("E.5 v1 hypothesis family changed.")
    multiplicity = payload.get("multiplicity", {})
    if multiplicity.get("denominator") != len(members) or multiplicity.get("family_alpha") != 0.05:
        raise ProtocolIntegrityError("E.5 v1 multiplicity denominator or alpha changed.")
    inference = payload.get("inference", {})
    if (
        inference.get("replications") != 9999
        or inference.get("bootstrap_weights") != "WEBB_SIX_POINT_MEAN_ZERO_UNIT_VARIANCE_V1"
        or inference.get("studentization") != "DELETE_ONE_PRIMARY_BLOCK_JACKKNIFE_EACH_DRAW_V1"
        or inference.get("minimum_valid_replication_fraction") != 0.99
    ):
        raise ProtocolIntegrityError("E.5 v1 bootstrap contract changed.")
    return payload


def load_frozen_protocol(path: str | Path) -> dict[str, Any]:
    raw = Path(path).read_text(encoding="utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ProtocolIntegrityError("Frozen E.5 protocol JSON is malformed.") from exc
    return validate_protocol_document(payload)


@dataclass(frozen=True)
class ScheduledBlock:
    block_id: str
    ordinal: int
    cohort: int
    epoch_start: str
    sample_start: str
    sample_end: str
    exposure_end: str
    block_hash: str


def scheduled_blocks(protocol: Mapping[str, Any]) -> tuple[ScheduledBlock, ...]:
    """Materialize the outcome-blind fixed schedule from the frozen contract."""
    checked = validate_protocol_document(protocol)
    spec = checked["sampling"]["schedule"]
    start = _utc(spec["acquisition_start"])
    epoch_seconds = int(spec["epoch_seconds"])
    slot_seconds = int(spec["slot_seconds"])
    slot_count = int(spec["slots_per_epoch_prefix"])
    duration = int(spec["sample_duration_seconds"])
    exposure_tail = int(checked["outcome"]["maximum_resolution_lag_seconds"])
    seed = int(spec["schedule_seed"])
    cohort_count = int(checked["sampling"]["wallet_cohort_count"])
    protocol_hash = checked["identity"]["protocol_hash"]
    output: list[ScheduledBlock] = []
    for ordinal in range(int(spec["block_count"])):
        epoch_start = start + timedelta(seconds=ordinal * epoch_seconds)
        digest = canonical_hash({
            "algorithm": spec["algorithm"], "schedule_seed": seed,
            "protocol_hash": protocol_hash, "ordinal": ordinal,
        })
        slot = int(digest[:16], 16) % slot_count
        sample_start = epoch_start + timedelta(seconds=slot * slot_seconds)
        sample_end = sample_start + timedelta(seconds=duration)
        exposure_end = sample_end + timedelta(seconds=exposure_tail)
        identity = {
            "schema": "phase-e5-primary-block-identity-v1", "protocol_hash": protocol_hash,
            "ordinal": ordinal, "epoch_start": normalized_utc(epoch_start.isoformat()),
            "sample_start": normalized_utc(sample_start.isoformat()),
            "sample_end": normalized_utc(sample_end.isoformat()),
            "exposure_end": normalized_utc(exposure_end.isoformat()), "cohort": ordinal % cohort_count,
        }
        block_hash = canonical_hash(identity)
        output.append(ScheduledBlock(
            block_id="e5b-" + block_hash[:32], ordinal=ordinal, cohort=ordinal % cohort_count,
            epoch_start=identity["epoch_start"], sample_start=identity["sample_start"],
            sample_end=identity["sample_end"], exposure_end=identity["exposure_end"],
            block_hash=block_hash,
        ))
    hard_stop = _utc(spec["hard_stop"])
    if output[-1] and _utc(output[-1].exposure_end) >= hard_stop:
        raise ProtocolIntegrityError("Final scheduled exposure is not contained before the hard stop.")
    for left, right in zip(output, output[1:]):
        separation = (_utc(right.sample_start) - _utc(left.exposure_end)).total_seconds()
        if separation < int(spec["minimum_separation_seconds"]):
            raise ProtocolIntegrityError("Frozen schedule violates its minimum block separation.")
    return tuple(output)


def assigned_block(protocol: Mapping[str, Any], timestamp: str) -> ScheduledBlock | None:
    instant = _utc(timestamp)
    for block in scheduled_blocks(protocol):
        if _utc(block.sample_start) <= instant < _utc(block.sample_end):
            return block
    return None


def wallet_cohort(protocol: Mapping[str, Any], wallet_id: str) -> int:
    if not isinstance(wallet_id, str) or not wallet_id:
        raise ValueError("Wallet identity is required for prospective cohort assignment.")
    checked = validate_protocol_document(protocol)
    return _wallet_cohort_checked(checked, wallet_id)


def _wallet_cohort_checked(protocol: Mapping[str, Any], wallet_id: str) -> int:
    sampling = protocol["sampling"]
    digest = canonical_hash({
        "algorithm": "SALTED_WALLET_COHORT_SHA256_V1",
        "salt": sampling["wallet_cohort_salt"], "wallet_id": wallet_id,
    })
    return int(digest[:16], 16) % int(sampling["wallet_cohort_count"])


@dataclass(frozen=True)
class SymbolEligibilitySnapshot:
    eligible: bool
    prior_24h_prints: int
    prior_30m_prints: int
    maximum_30m_interprint_gap_seconds: float | None
    source_discontinuity: bool
    snapshot_hash: str


def preanchor_symbol_eligibility(
    protocol: Mapping[str, Any], *, symbol: str, anchor_at: str,
    trade_timestamps: Sequence[str], source_discontinuity: bool,
) -> SymbolEligibilitySnapshot:
    """Evaluate the exact frozen liquidity rule from timestamps strictly before the anchor."""
    checked = validate_protocol_document(protocol)
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("Symbol is required for eligibility.")
    if not isinstance(source_discontinuity, bool):
        raise ValueError("Source-discontinuity state must be a boolean.")
    anchor = _utc(anchor_at)
    instants = sorted(_utc(value) for value in trade_timestamps)
    if any(item >= anchor for item in instants):
        raise ProtocolIntegrityError("Pre-anchor symbol eligibility received post-anchor timestamps.")
    start_24h = anchor - timedelta(hours=24)
    start_30m = anchor - timedelta(minutes=30)
    prior_24h = [item for item in instants if item >= start_24h]
    prior_30m = [item for item in prior_24h if item >= start_30m]
    boundaries = [start_30m, *prior_30m, anchor]
    maximum_gap = (
        max((right - left).total_seconds() for left, right in zip(boundaries, boundaries[1:]))
        if len(boundaries) >= 2 else None
    )
    rule = checked["sampling"]["symbol_eligibility"]
    eligible = (
        len(prior_24h) >= int(rule["lookback_24h_minimum_prints"])
        and len(prior_30m) >= int(rule["lookback_30m_minimum_prints"])
        and maximum_gap is not None
        and maximum_gap <= float(rule["lookback_30m_maximum_interprint_gap_seconds"])
        and not source_discontinuity
    )
    payload = {
        "schema": "phase-e5-preanchor-symbol-eligibility-v1",
        "protocol_hash": checked["identity"]["protocol_hash"], "symbol": symbol,
        "anchor_at": normalized_utc(anchor_at),
        "timestamp_fingerprint": canonical_hash([normalized_utc(item.isoformat()) for item in prior_24h]),
        "prior_24h_prints": len(prior_24h), "prior_30m_prints": len(prior_30m),
        "maximum_30m_interprint_gap_seconds": maximum_gap,
        "source_discontinuity": source_discontinuity, "eligible": eligible,
    }
    return SymbolEligibilitySnapshot(
        eligible=eligible, prior_24h_prints=len(prior_24h), prior_30m_prints=len(prior_30m),
        maximum_30m_interprint_gap_seconds=maximum_gap, source_discontinuity=source_discontinuity,
        snapshot_hash=canonical_hash(payload),
    )


@dataclass(frozen=True)
class DesignObservation:
    """Outcome-blind prospective membership and dependence metadata."""

    observation_id: str
    source_schema: str
    protocol_hash: str
    block_id: str
    anchor_at: str
    exposure_end_at: str
    wallet_id: str
    symbol: str
    source_event_id: str
    sampling_weight: float
    predicate: bool
    liquidity_stratum: str
    graph_density_stratum: str
    time_stratum: str
    eligibility_snapshot_hash: str
    symbol_liquidity_eligible: bool
    transaction_id: str | None = None
    endpoint_family_id: str | None = None
    campaign_id: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "observation_id", "source_schema", "protocol_hash", "block_id", "wallet_id",
            "symbol", "source_event_id", "liquidity_stratum", "graph_density_stratum", "time_stratum",
            "eligibility_snapshot_hash",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} is required.")
        if not isinstance(self.predicate, bool):
            raise ValueError("Predicate membership must be frozen as a boolean.")
        if not isinstance(self.symbol_liquidity_eligible, bool):
            raise ValueError("Symbol-liquidity eligibility must be a frozen boolean.")
        finite_number(self.sampling_weight, name="sampling weight", minimum=0.0)
        if self.sampling_weight <= 0:
            raise ValueError("Sampling weight must be positive.")
        anchor, end = _utc(self.anchor_at), _utc(self.exposure_end_at)
        if end <= anchor:
            raise ValueError("Exposure must end after its anchor.")

    def identity_payload(self) -> dict[str, Any]:
        return {
            "observation_id": self.observation_id, "source_schema": self.source_schema,
            "protocol_hash": self.protocol_hash, "block_id": self.block_id,
            "anchor_at": normalized_utc(self.anchor_at), "exposure_end_at": normalized_utc(self.exposure_end_at),
            "wallet_id_hash": canonical_hash({"wallet_id": self.wallet_id}), "symbol": self.symbol,
            "source_event_id": self.source_event_id, "sampling_weight": self.sampling_weight,
            "predicate": self.predicate, "liquidity_stratum": self.liquidity_stratum,
            "graph_density_stratum": self.graph_density_stratum, "time_stratum": self.time_stratum,
            "eligibility_snapshot_hash": self.eligibility_snapshot_hash,
            "symbol_liquidity_eligible": self.symbol_liquidity_eligible,
            "transaction_id": self.transaction_id, "endpoint_family_id": self.endpoint_family_id,
            "campaign_id": self.campaign_id,
        }


@dataclass(frozen=True)
class EvidenceClassification:
    observation_id: str
    state: EvidenceState
    classified_at: str
    reason: str | None = None


@dataclass(frozen=True)
class OutcomeRecord:
    """Outcome-side data; never part of sampling or gate-threshold selection."""

    observation_id: str
    net_outcome: float

    def __post_init__(self) -> None:
        finite_number(self.net_outcome, name="net outcome")


def classify_evidence(
    protocol: Mapping[str, Any], observation: DesignObservation, *, as_of: str,
    resolution_event_at: str | None, ingested_at: str | None, structurally_unresolved: bool = False,
) -> EvidenceClassification:
    """Keep immaturity, final missingness, stale evidence, and late evidence distinct."""
    checked = validate_protocol_document(protocol)
    outcome = checked["outcome"]
    anchor = _utc(observation.anchor_at)
    earliest = anchor + timedelta(seconds=int(outcome["horizon_seconds"]))
    latest = anchor + timedelta(seconds=int(outcome["maximum_resolution_lag_seconds"]))
    deadline = latest + timedelta(seconds=int(outcome["ingestion_grace_seconds"]))
    now = _utc(as_of)
    if now < deadline:
        return EvidenceClassification(observation.observation_id, EvidenceState.IMMATURE, normalized_utc(as_of))
    if structurally_unresolved:
        return EvidenceClassification(
            observation.observation_id, EvidenceState.STRUCTURALLY_UNRESOLVED,
            normalized_utc(as_of), "Prerequisite outcome source or lineage is unavailable.",
        )
    if resolution_event_at is None or ingested_at is None:
        return EvidenceClassification(
            observation.observation_id, EvidenceState.MATURE_MISSING,
            normalized_utc(as_of), "No qualifying resolution event arrived by the final deadline.",
        )
    event_at, arrived_at = _utc(resolution_event_at), _utc(ingested_at)
    if event_at < earliest or event_at > latest:
        return EvidenceClassification(
            observation.observation_id, EvidenceState.STALE, normalized_utc(as_of),
            "Resolution event falls outside the frozen economic exposure window.",
        )
    if arrived_at > deadline:
        return EvidenceClassification(
            observation.observation_id, EvidenceState.LATE, normalized_utc(as_of),
            "Qualifying evidence arrived after the frozen finalization deadline.",
        )
    return EvidenceClassification(observation.observation_id, EvidenceState.ADMISSIBLE_OBSERVED, normalized_utc(as_of))


class _DisjointSet:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self.parent[max(left_root, right_root)] = min(left_root, right_root)


@dataclass(frozen=True)
class DependenceDiagnostics:
    component_by_observation: Mapping[str, str]
    component_weights: Mapping[str, float]
    cross_block_edges: tuple[tuple[str, str, str], ...]
    relation_counts: Mapping[str, int]
    diagnostics_hash: str


def dependence_diagnostics(observations: Sequence[DesignObservation]) -> DependenceDiagnostics:
    """Build a conservative graph for enforcement, never as a resampling unit."""
    ordered = sorted(observations, key=lambda item: item.observation_id)
    graph = _DisjointSet(len(ordered))
    cross: set[tuple[str, str, str]] = set()
    relation_counts: dict[str, int] = defaultdict(int)

    exact_relations = {
        "SAME_WALLET": lambda item: item.wallet_id,
        "SAME_SOURCE_EVENT": lambda item: item.source_event_id,
        "SAME_TRANSACTION": lambda item: item.transaction_id,
        "SAME_ENDPOINT_FAMILY": lambda item: item.endpoint_family_id,
        "SAME_CAMPAIGN": lambda item: item.campaign_id,
    }
    for relation, getter in exact_relations.items():
        groups: dict[str, list[int]] = defaultdict(list)
        for index, item in enumerate(ordered):
            value = getter(item)
            if value:
                groups[str(value)].append(index)
        for indices in groups.values():
            for offset, left in enumerate(indices):
                for right in indices[offset + 1:]:
                    graph.union(left, right)
                    relation_counts[relation] += 1
                    if ordered[left].block_id != ordered[right].block_id:
                        pair = tuple(sorted((ordered[left].observation_id, ordered[right].observation_id)))
                        cross.add((pair[0], pair[1], relation))

    by_symbol: dict[str, list[int]] = defaultdict(list)
    for index, item in enumerate(ordered):
        by_symbol[item.symbol].append(index)
    for indices in by_symbol.values():
        indices.sort(key=lambda item: (_utc(ordered[item].anchor_at), ordered[item].observation_id))
        for offset, left in enumerate(indices):
            left_start, left_end = _utc(ordered[left].anchor_at), _utc(ordered[left].exposure_end_at)
            for right in indices[offset + 1:]:
                right_start = _utc(ordered[right].anchor_at)
                if right_start > left_end:
                    break
                right_end = _utc(ordered[right].exposure_end_at)
                if max(left_start, right_start) <= min(left_end, right_end):
                    graph.union(left, right)
                    relation_counts["OVERLAPPING_REALIZED_EXPOSURE"] += 1
                    if ordered[left].block_id != ordered[right].block_id:
                        pair = tuple(sorted((ordered[left].observation_id, ordered[right].observation_id)))
                        cross.add((pair[0], pair[1], "OVERLAPPING_REALIZED_EXPOSURE"))

    grouped: dict[int, list[int]] = defaultdict(list)
    for index in range(len(ordered)):
        grouped[graph.find(index)].append(index)
    component_by_observation: dict[str, str] = {}
    component_weights: dict[str, float] = {}
    for indices in grouped.values():
        member_ids = sorted(ordered[index].observation_id for index in indices)
        component_hash = canonical_hash({"schema": "phase-e5-dependence-component-v1", "members": member_ids})
        component_id = "e5c-" + component_hash[:32]
        component_weights[component_id] = sum(ordered[index].sampling_weight for index in indices)
        for index in indices:
            component_by_observation[ordered[index].observation_id] = component_id
    payload = {
        "component_by_observation": component_by_observation,
        "component_weights": component_weights,
        "cross_block_edges": sorted(cross),
        "relation_counts": dict(sorted(relation_counts.items())),
    }
    return DependenceDiagnostics(
        component_by_observation=component_by_observation,
        component_weights=component_weights, cross_block_edges=tuple(sorted(cross)),
        relation_counts=dict(sorted(relation_counts.items())), diagnostics_hash=canonical_hash(payload),
    )


def _shares(weights: Mapping[str, float]) -> tuple[float, float, float]:
    values = [float(value) for value in weights.values() if value > 0]
    total = sum(values)
    if total <= 0:
        return 1.0, 1.0, 1.0
    shares = [item / total for item in values]
    hhi = sum(item * item for item in shares)
    return max(shares), hhi, 1.0 / hhi


def _group_weights(observations: Sequence[DesignObservation], key: Callable[[DesignObservation], str]) -> dict[str, float]:
    output: dict[str, float] = defaultdict(float)
    for item in observations:
        output[key(item)] += item.sampling_weight
    return dict(output)


@dataclass(frozen=True)
class AdmissibilityReport:
    state: ExperimentState
    reasons: tuple[str, ...]
    metrics: Mapping[str, Any]
    diagnostics_hash: str
    report_hash: str


def evaluate_admissibility(
    protocol: Mapping[str, Any], observations: Sequence[DesignObservation],
    classifications: Sequence[EvidenceClassification], *, as_of: str,
) -> AdmissibilityReport:
    """Apply all preregistered gates before any evaluation-outcome value read."""
    checked = validate_protocol_document(protocol)
    decision_at = _utc(as_of)
    protocol_hash = checked["identity"]["protocol_hash"]
    gates = checked["admissibility"]
    reasons: list[str] = []
    integrity_reasons: list[str] = []
    block_index = {item.block_id: item for item in scheduled_blocks(checked)}
    maximum_exposure_seconds = int(checked["outcome"]["maximum_resolution_lag_seconds"])
    for item in observations:
        if item.source_schema != E5_OBSERVATION_SCHEMA:
            integrity_reasons.append("NON_PROSPECTIVE_OR_HISTORICAL_OBSERVATION")
        if item.protocol_hash != protocol_hash:
            integrity_reasons.append("OBSERVATION_PROTOCOL_HASH_MISMATCH")
        if not item.symbol_liquidity_eligible:
            integrity_reasons.append("SYMBOL_FAILED_PREANCHOR_LIQUIDITY_RULE")
        block = block_index.get(item.block_id)
        if block is None:
            integrity_reasons.append("OBSERVATION_OUTSIDE_FROZEN_BLOCK_SCHEDULE")
        else:
            anchor, exposure_end = _utc(item.anchor_at), _utc(item.exposure_end_at)
            if not (_utc(block.sample_start) <= anchor < _utc(block.sample_end)):
                integrity_reasons.append("OBSERVATION_ANCHOR_OUTSIDE_ASSIGNED_BLOCK")
            if exposure_end > anchor + timedelta(seconds=maximum_exposure_seconds) or exposure_end > _utc(block.exposure_end):
                integrity_reasons.append("OBSERVATION_EXPOSURE_EXCEEDS_FROZEN_WINDOW")
            if _wallet_cohort_checked(checked, item.wallet_id) != block.cohort:
                integrity_reasons.append("WALLET_COHORT_DOES_NOT_MATCH_BLOCK")
    if len({item.observation_id for item in observations}) != len(observations):
        integrity_reasons.append("DUPLICATE_OBSERVATION_ID")
    wallet_counts: dict[str, int] = defaultdict(int)
    source_event_counts: dict[str, int] = defaultdict(int)
    for item in observations:
        wallet_counts[item.wallet_id] += 1
        source_event_counts[item.source_event_id] += 1
    if any(count > 1 for count in wallet_counts.values()):
        integrity_reasons.append("WALLET_REPEATED_AFTER_FIRST_ADMISSION")
    if any(count > 1 for count in source_event_counts.values()):
        integrity_reasons.append("SOURCE_EVENT_ADMITTED_MORE_THAN_ONCE")
    class_by_id = {item.observation_id: item for item in classifications}
    if set(class_by_id) != {item.observation_id for item in observations}:
        integrity_reasons.append("EVIDENCE_CLASSIFICATION_MEMBERSHIP_MISMATCH")
    if any(_utc(item.classified_at) > decision_at for item in classifications):
        integrity_reasons.append("EVIDENCE_CLASSIFICATION_FROM_FUTURE")

    graph = dependence_diagnostics(observations)
    block_weights = _group_weights(observations, lambda item: item.block_id)
    wallet_weights = _group_weights(observations, lambda item: canonical_hash({"wallet_id": item.wallet_id}))
    symbol_weights = _group_weights(observations, lambda item: item.symbol)
    endpoint_weights = _group_weights(observations, lambda item: item.endpoint_family_id or item.source_event_id)
    local_window_seconds = int(gates["local_time_window_seconds"])
    local_weights = _group_weights(
        observations,
        lambda item: f"{item.symbol}:{int(_utc(item.anchor_at).timestamp()) // local_window_seconds}",
    )
    arm_true = [item for item in observations if item.predicate]
    arm_false = [item for item in observations if not item.predicate]
    true_block = _group_weights(arm_true, lambda item: item.block_id)
    false_block = _group_weights(arm_false, lambda item: item.block_id)
    blocks = sorted(set(block_weights))
    mixed_blocks = sorted(set(true_block) & set(false_block))
    contrast_information: dict[str, float] = {}
    for block in mixed_blocks:
        w1, w0 = true_block[block], false_block[block]
        contrast_information[block] = w1 * w0 / (w1 + w0)

    summaries = {
        "block": _shares(block_weights), "predicate_block": _shares(true_block),
        "complement_block": _shares(false_block), "wallet": _shares(wallet_weights),
        "symbol": _shares(symbol_weights), "component": _shares(graph.component_weights),
        "endpoint_family": _shares(endpoint_weights), "local_time_window": _shares(local_weights),
        "contrast_information": _shares(contrast_information),
    }
    metrics: dict[str, Any] = {
        "nominal_observations": len(observations), "predicate_observations": len(arm_true),
        "complement_observations": len(arm_false), "nominal_blocks": len(blocks),
        "mixed_blocks": len(mixed_blocks), "nominal_components": len(graph.component_weights),
        "cross_block_edge_count": len(graph.cross_block_edges),
        "relation_counts": graph.relation_counts, "dependence_diagnostics_hash": graph.diagnostics_hash,
        "decision_as_of": normalized_utc(as_of),
    }
    for name, (maximum, hhi, effective) in summaries.items():
        metrics[f"maximum_{name}_share"] = maximum
        metrics[f"{name}_hhi"] = hhi
        metrics[f"effective_{name}_count"] = effective

    state_counts: dict[str, int] = defaultdict(int)
    for item in classifications:
        state_counts[item.state.value] += 1
    metrics["evidence_state_counts"] = dict(sorted(state_counts.items()))
    metrics["resolution_rate"] = (
        state_counts[EvidenceState.ADMISSIBLE_OBSERVED.value] / len(observations) if observations else 0.0
    )
    resolution_dimensions: dict[str, Callable[[DesignObservation], str]] = {
        "block": lambda item: item.block_id,
        "symbol": lambda item: item.symbol,
        "liquidity_stratum": lambda item: item.liquidity_stratum,
        "graph_density_stratum": lambda item: item.graph_density_stratum,
        "time_stratum": lambda item: item.time_stratum,
    }
    for dimension, getter in resolution_dimensions.items():
        grouped_counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        for item in observations:
            key = getter(item)
            grouped_counts[key][1] += 1
            classification = class_by_id.get(item.observation_id)
            if classification is not None and classification.state == EvidenceState.ADMISSIBLE_OBSERVED:
                grouped_counts[key][0] += 1
        rates = {key: resolved / count for key, (resolved, count) in grouped_counts.items()}
        metrics[f"minimum_{dimension}_resolution_rate"] = min(rates.values(), default=0.0)
    unresolved_weights: dict[str, float] = defaultdict(float)
    for item in observations:
        classification = class_by_id.get(item.observation_id)
        if classification is None or classification.state != EvidenceState.ADMISSIBLE_OBSERVED:
            unresolved_weights[item.block_id] += item.sampling_weight
    metrics["maximum_unresolved_block_share"] = _shares(unresolved_weights)[0] if unresolved_weights else 0.0

    if integrity_reasons:
        state = ExperimentState.PROTOCOL_INTEGRITY_FAILED
        reasons.extend(sorted(set(integrity_reasons)))
    elif decision_at < _utc(checked["stopping"]["hard_stop"]):
        state = ExperimentState.COLLECTING
        reasons.append("FIXED_ACQUISITION_HARD_STOP_NOT_REACHED")
    elif state_counts[EvidenceState.IMMATURE.value]:
        state = ExperimentState.AWAITING_MATURITY
        reasons.append("EVIDENCE_NOT_YET_MATURE")
    elif any(state_counts[item.value] for item in EvidenceState if item not in {
        EvidenceState.ADMISSIBLE_OBSERVED, EvidenceState.IMMATURE,
    }):
        state = ExperimentState.MISSINGNESS_GATE_FAILED
        reasons.append("COMPLETE_RESOLUTION_REQUIRED_NO_CORRECTION_AUTHORIZED")
    elif graph.cross_block_edges:
        state = ExperimentState.DEPENDENCE_GATE_FAILED
        reasons.append("DEPENDENCE_RELATION_CROSSES_PRIMARY_BLOCKS")
    else:
        concentration_failures: list[str] = []
        limits = gates["maximum_shares"]
        for name in ("block", "predicate_block", "complement_block", "component", "wallet", "symbol", "endpoint_family", "local_time_window", "contrast_information"):
            if metrics[f"maximum_{name}_share"] > float(limits[name]) + 1e-15:
                concentration_failures.append(f"MAXIMUM_{name.upper()}_SHARE_EXCEEDED")
        weight_values = [item.sampling_weight for item in observations]
        if weight_values and max(weight_values) / min(weight_values) > float(gates["maximum_sampling_weight_ratio"]):
            concentration_failures.append("SAMPLING_WEIGHT_RATIO_EXCEEDED")
        if concentration_failures:
            state = ExperimentState.CONCENTRATION_GATE_FAILED
            reasons.extend(concentration_failures)
        else:
            support_failures: list[str] = []
            minimums = gates["minimum_support"]
            comparisons = {
                "OBSERVATIONS": metrics["nominal_observations"],
                "OBSERVATIONS_PER_ARM": min(metrics["predicate_observations"], metrics["complement_observations"]),
                "BLOCKS": metrics["nominal_blocks"], "MIXED_BLOCKS": metrics["mixed_blocks"],
                "EFFECTIVE_BLOCKS": metrics["effective_block_count"],
                "EFFECTIVE_BLOCKS_PER_ARM": min(metrics["effective_predicate_block_count"], metrics["effective_complement_block_count"]),
                "EFFECTIVE_CONTRAST_BLOCKS": metrics["effective_contrast_information_count"],
                "EFFECTIVE_SYMBOLS": metrics["effective_symbol_count"],
            }
            required = {
                "OBSERVATIONS": minimums["observations"],
                "OBSERVATIONS_PER_ARM": minimums["observations_per_arm"],
                "BLOCKS": minimums["blocks"], "MIXED_BLOCKS": minimums["mixed_blocks"],
                "EFFECTIVE_BLOCKS": minimums["effective_blocks"],
                "EFFECTIVE_BLOCKS_PER_ARM": minimums["effective_blocks_per_arm"],
                "EFFECTIVE_CONTRAST_BLOCKS": minimums["effective_contrast_blocks"],
                "EFFECTIVE_SYMBOLS": minimums["effective_symbols"],
            }
            for name, value in comparisons.items():
                if value + 1e-12 < float(required[name]):
                    support_failures.append(f"MINIMUM_{name}_NOT_MET")
            if support_failures:
                state = ExperimentState.INSUFFICIENT_SUPPORT
                reasons.extend(support_failures)
            else:
                state = ExperimentState.ELIGIBLE_FOR_INFERENCE

    report_body = {
        "schema": "phase-e5-admissibility-report-v1", "protocol_hash": protocol_hash,
        "state": state.value, "reasons": sorted(set(reasons)), "metrics": metrics,
        "diagnostics_hash": graph.diagnostics_hash,
    }
    return AdmissibilityReport(
        state=state, reasons=tuple(sorted(set(reasons))), metrics=metrics,
        diagnostics_hash=graph.diagnostics_hash, report_hash=canonical_hash(report_body),
    )


@dataclass(frozen=True)
class WildBootstrapResult:
    estimate: float
    standard_error: float
    t_statistic: float
    raw_p_value: float
    confidence_interval: tuple[float, float]
    valid_replications: int
    requested_replications: int
    valid_fraction: float
    distribution_hash: str
    result_hash: str


def _jackknife_se(total_numerator: float, total_denominator: float, by_block: Sequence[tuple[float, float]]) -> float:
    estimates = []
    for numerator, denominator in by_block:
        remaining = total_denominator - denominator
        if remaining <= 0:
            return float("nan")
        estimates.append((total_numerator - numerator) / remaining)
    if len(estimates) < 2:
        return float("nan")
    center = sum(estimates) / len(estimates)
    variance = (len(estimates) - 1.0) / len(estimates) * sum((item - center) ** 2 for item in estimates)
    return math.sqrt(max(variance, 0.0))


def _type7_quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("Quantile requires values.")
    if not 0.0 <= probability <= 1.0:
        raise ValueError("Quantile probability must be in [0,1].")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    location = (len(ordered) - 1) * probability
    lower = math.floor(location)
    upper = math.ceil(location)
    fraction = location - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _wild_cluster_bootstrap_t(
    protocol: Mapping[str, Any], observations: Sequence[DesignObservation], outcomes: Sequence[OutcomeRecord],
    *, hypothesis_id: str, replications: int | None = None,
) -> WildBootstrapResult:
    """Run the frozen within-block, restricted wild cluster bootstrap-t.

    This private function is outcome-side. Production callers reach it only via
    :meth:`PhaseE5Registry.infer`, which requires an eligible gate report.
    """
    checked = validate_protocol_document(protocol)
    inference = checked["inference"]
    requested = int(replications if replications is not None else inference["replications"])
    _positive_int(requested, "Bootstrap replications", minimum=99)
    by_outcome = {item.observation_id: item.net_outcome for item in outcomes}
    if set(by_outcome) != {item.observation_id for item in observations}:
        raise InferenceRefused("Inference requires one finite outcome for every admitted observation.")

    groups: dict[str, list[tuple[DesignObservation, float]]] = defaultdict(list)
    for item in observations:
        groups[item.block_id].append((item, by_outcome[item.observation_id]))
    ordered_groups = sorted(groups.items(), key=lambda pair: canonical_hash({"block_id": pair[0]}))
    if len(ordered_groups) < 2:
        raise InferenceRefused("Studentized cluster inference needs at least two blocks.")

    block_terms: list[tuple[float, float]] = []
    for _block_id, rows in ordered_groups:
        w1 = sum(item.sampling_weight for item, _ in rows if item.predicate)
        w0 = sum(item.sampling_weight for item, _ in rows if not item.predicate)
        if w1 <= 0 or w0 <= 0:
            continue
        y1 = sum(item.sampling_weight * value for item, value in rows if item.predicate) / w1
        y0 = sum(item.sampling_weight * value for item, value in rows if not item.predicate) / w0
        information = w1 * w0 / (w1 + w0)
        block_terms.append((information * (y1 - y0), information))
    if len(block_terms) < 2:
        raise InferenceRefused("The within-block estimand needs at least two mixed blocks.")
    total_numerator = sum(item[0] for item in block_terms)
    total_denominator = sum(item[1] for item in block_terms)
    estimate = total_numerator / total_denominator
    standard_error = _jackknife_se(total_numerator, total_denominator, block_terms)
    if not math.isfinite(standard_error) or standard_error <= 0:
        raise InferenceRefused("Observed jackknife studentization is degenerate.")
    observed_t = estimate / standard_error

    base_seed = int(inference["base_seed"])
    derived = canonical_hash({
        "algorithm": inference["seed_derivation"], "protocol_hash": checked["identity"]["protocol_hash"],
        "hypothesis_id": hypothesis_id, "base_seed": base_seed,
    })
    rng = random.Random(int(derived[:16], 16))
    webb = (-math.sqrt(1.5), -1.0, -math.sqrt(0.5), math.sqrt(0.5), 1.0, math.sqrt(1.5))
    bootstrap_t: list[float] = []
    for _ in range(requested):
        weighted_terms = [(rng.choice(webb) * numerator, denominator) for numerator, denominator in block_terms]
        numerator_star = sum(item[0] for item in weighted_terms)
        estimate_star = numerator_star / total_denominator
        se_star = _jackknife_se(numerator_star, total_denominator, weighted_terms)
        if math.isfinite(se_star) and se_star > 0:
            bootstrap_t.append(estimate_star / se_star)
    valid_fraction = len(bootstrap_t) / requested
    if valid_fraction < float(inference["minimum_valid_replication_fraction"]):
        raise InferenceRefused("Too many wild-bootstrap draws have degenerate studentization.")
    p_value = (1 + sum(abs(item) >= abs(observed_t) for item in bootstrap_t)) / (len(bootstrap_t) + 1)
    alpha = float(checked["multiplicity"]["family_alpha"])
    lower_q = _type7_quantile(bootstrap_t, alpha / 2.0)
    upper_q = _type7_quantile(bootstrap_t, 1.0 - alpha / 2.0)
    interval = (estimate - upper_q * standard_error, estimate - lower_q * standard_error)
    distribution_hash = canonical_hash({
        "schema": "phase-e5-bootstrap-t-distribution-v1", "hypothesis_id": hypothesis_id,
        "derived_seed_hash": derived, "values": bootstrap_t,
    })
    body = {
        "schema": E5_RESULT_SCHEMA, "protocol_hash": checked["identity"]["protocol_hash"],
        "hypothesis_id": hypothesis_id, "estimate": estimate, "standard_error": standard_error,
        "t_statistic": observed_t, "raw_p_value": p_value, "confidence_interval": list(interval),
        "valid_replications": len(bootstrap_t), "requested_replications": requested,
        "valid_fraction": valid_fraction, "distribution_hash": distribution_hash,
    }
    return WildBootstrapResult(
        estimate=estimate, standard_error=standard_error, t_statistic=observed_t,
        raw_p_value=p_value, confidence_interval=interval, valid_replications=len(bootstrap_t),
        requested_replications=requested, valid_fraction=valid_fraction,
        distribution_hash=distribution_hash, result_hash=canonical_hash(body),
    )


def synthetic_wild_cluster_bootstrap_t(
    protocol: Mapping[str, Any], observations: Sequence[DesignObservation], outcomes: Sequence[OutcomeRecord],
    *, hypothesis_id: str, fixture_namespace: str, replications: int | None = None,
) -> WildBootstrapResult:
    """Exercise the frozen algorithm on synthetic fixtures without a production capability."""
    if fixture_namespace != "SYNTHETIC_E5_ONLY_NEVER_PRODUCTION":
        raise OutcomeAccessForbidden("Direct inference is reserved for the isolated synthetic-fixture namespace.")
    return _wild_cluster_bootstrap_t(
        protocol, observations, outcomes, hypothesis_id=hypothesis_id, replications=replications,
    )


def holm_adjust(raw_p_values: Mapping[str, float], family_order: Sequence[str], *, alpha: float) -> dict[str, dict[str, Any]]:
    """Apply Holm to the exact frozen family; missing/unevaluable members use p=1."""
    finite_number(alpha, name="family alpha", minimum=0.0, maximum=1.0)
    if not family_order or len(set(family_order)) != len(family_order):
        raise ProtocolIntegrityError("Frozen multiplicity family must be nonempty and unique.")
    rank_order = sorted(
        family_order,
        key=lambda name: (finite_number(raw_p_values.get(name, 1.0), name=f"p-value {name}", minimum=0.0, maximum=1.0), family_order.index(name), name),
    )
    adjusted: dict[str, dict[str, Any]] = {}
    running = 0.0
    family_size = len(family_order)
    prior_rejected = True
    for rank, name in enumerate(rank_order, 1):
        raw = float(raw_p_values.get(name, 1.0))
        running = max(running, min(1.0, (family_size - rank + 1) * raw))
        threshold = alpha / (family_size - rank + 1)
        rejected = prior_rejected and raw <= threshold
        prior_rejected = rejected
        adjusted[name] = {
            "raw_p_value": raw, "adjusted_p_value": running, "rank": rank,
            "holm_threshold": threshold, "reject_null": rejected,
        }
    return adjusted


def scientific_replay_hash(
    protocol: Mapping[str, Any], observations: Sequence[DesignObservation], classifications: Sequence[EvidenceClassification],
    admissibility: AdmissibilityReport, results: Mapping[str, WildBootstrapResult] | None = None,
) -> str:
    checked = validate_protocol_document(protocol)
    payload = {
        "schema": E5_REPLAY_SCHEMA, "protocol_hash": checked["identity"]["protocol_hash"],
        "observations": [item.identity_payload() for item in sorted(observations, key=lambda item: item.observation_id)],
        "classifications": [
            {"observation_id": item.observation_id, "state": item.state.value,
             "classified_at": normalized_utc(item.classified_at), "reason": item.reason}
            for item in sorted(classifications, key=lambda item: item.observation_id)
        ],
        "admissibility_report_hash": admissibility.report_hash,
        "results": {
            key: {"result_hash": value.result_hash, "distribution_hash": value.distribution_hash}
            for key, value in sorted((results or {}).items())
        },
    }
    return canonical_hash(payload)


class PhaseE5Registry:
    """Immutable protocol registry and capability-gated outcome access.

    The registry is suitable for a dedicated E.5 control database.  It is not
    automatically initialized against the production database by this phase.
    """

    TRADING_AUTHORITY = False
    PREDICTION_AUTHORITY = False
    SIGNAL_AUTHORITY = False
    EXECUTION_AUTHORITY = False

    def __init__(self, database_path: str | Path) -> None:
        self.path = Path(database_path)
        self._initialized = False

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def initialize(self) -> None:
        if self._initialized:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._write() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS phase_e5_protocols (
                    protocol_id TEXT PRIMARY KEY,
                    protocol_version INTEGER NOT NULL,
                    protocol_hash TEXT NOT NULL UNIQUE,
                    schema_version TEXT NOT NULL,
                    protocol_json TEXT NOT NULL,
                    hypothesis_family_hash TEXT NOT NULL,
                    experimental_unit_hash TEXT NOT NULL,
                    code_commit TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    frozen_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e5_experiments (
                    protocol_id TEXT PRIMARY KEY REFERENCES phase_e5_protocols(protocol_id),
                    state TEXT NOT NULL,
                    state_version INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phase_e5_protocol_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    protocol_id TEXT NOT NULL REFERENCES phase_e5_protocols(protocol_id),
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    event_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS phase_e5_outcome_access_audit (
                    access_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    protocol_id TEXT NOT NULL REFERENCES phase_e5_protocols(protocol_id),
                    requested_at TEXT NOT NULL,
                    decision TEXT NOT NULL CHECK(decision IN ('BLOCKED','GRANTED')),
                    purpose TEXT NOT NULL,
                    audit_hash TEXT NOT NULL UNIQUE
                );
                CREATE TRIGGER IF NOT EXISTS phase_e5_protocols_no_update
                    BEFORE UPDATE ON phase_e5_protocols
                    BEGIN SELECT RAISE(ABORT, 'E.5 frozen protocols are immutable'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e5_protocols_no_delete
                    BEFORE DELETE ON phase_e5_protocols
                    BEGIN SELECT RAISE(ABORT, 'E.5 frozen protocols cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e5_events_no_update
                    BEFORE UPDATE ON phase_e5_protocol_events
                    BEGIN SELECT RAISE(ABORT, 'E.5 protocol events are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e5_events_no_delete
                    BEFORE DELETE ON phase_e5_protocol_events
                    BEGIN SELECT RAISE(ABORT, 'E.5 protocol events cannot be deleted'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e5_outcome_audit_no_update
                    BEFORE UPDATE ON phase_e5_outcome_access_audit
                    BEGIN SELECT RAISE(ABORT, 'E.5 outcome audit is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS phase_e5_outcome_audit_no_delete
                    BEFORE DELETE ON phase_e5_outcome_access_audit
                    BEGIN SELECT RAISE(ABORT, 'E.5 outcome audit cannot be deleted'); END;
            """)
        self._initialized = True

    def freeze(self, document: Mapping[str, Any]) -> dict[str, Any]:
        self.initialize()
        checked = validate_protocol_document(document)
        identity = checked["identity"]
        protocol_id, protocol_hash = identity["protocol_id"], identity["protocol_hash"]
        family_hash = canonical_hash(checked["hypothesis_family"])
        unit_hash = canonical_hash(checked["experimental_unit"])
        with self._write() as connection:
            existing = connection.execute(
                "SELECT * FROM phase_e5_protocols WHERE protocol_id=?", (protocol_id,),
            ).fetchone()
            if existing is not None:
                if existing["protocol_hash"] != protocol_hash or existing["protocol_json"] != storage_json(checked):
                    raise ProtocolConflictError("This E.5 protocol identity is already frozen with different semantics.")
                return self._payload(connection, existing)
            connection.execute(
                """INSERT INTO phase_e5_protocols VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    protocol_id, int(checked["protocol_version"]), protocol_hash, checked["schema"],
                    storage_json(checked), family_hash, unit_hash, identity["code_commit"],
                    normalized_utc(identity["created_at"]), normalized_utc(identity["frozen_at"]),
                ),
            )
            connection.execute(
                "INSERT INTO phase_e5_experiments VALUES (?, ?, 0, ?)",
                (protocol_id, ExperimentState.FROZEN_NOT_STARTED.value, normalized_utc(identity["frozen_at"])),
            )
            self._event(connection, protocol_id, "PROTOCOL_FROZEN", identity["frozen_at"], {
                "protocol_hash": protocol_hash, "hypothesis_family_hash": family_hash,
                "experimental_unit_hash": unit_hash, "evaluation_outcome_reads": 0,
                "reserved_test_queries": 0, "authority": checked["authority"],
            })
            row = connection.execute("SELECT * FROM phase_e5_protocols WHERE protocol_id=?", (protocol_id,)).fetchone()
            return self._payload(connection, row)

    def _event(
        self, connection: sqlite3.Connection, protocol_id: str, event_type: str, occurred_at: str,
        payload: Mapping[str, Any],
    ) -> None:
        at = normalized_utc(occurred_at)
        event_payload = {"protocol_id": protocol_id, "event_type": event_type, "occurred_at": at, "payload": dict(payload)}
        connection.execute(
            "INSERT INTO phase_e5_protocol_events(protocol_id,event_type,occurred_at,payload_json,event_hash) VALUES (?,?,?,?,?)",
            (protocol_id, event_type, at, storage_json(payload), canonical_hash(event_payload)),
        )

    def get(self, protocol_id: str) -> dict[str, Any]:
        self.initialize()
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM phase_e5_protocols WHERE protocol_id=?", (protocol_id,)).fetchone()
            if row is None:
                raise ProtocolIntegrityError("Unknown E.5 protocol.")
            return self._payload(connection, row)

    def _payload(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        document = json.loads(row["protocol_json"])
        validate_protocol_document(document)
        state = connection.execute("SELECT * FROM phase_e5_experiments WHERE protocol_id=?", (row["protocol_id"],)).fetchone()
        events = connection.execute(
            "SELECT event_type,occurred_at,payload_json,event_hash FROM phase_e5_protocol_events WHERE protocol_id=? ORDER BY event_id",
            (row["protocol_id"],),
        ).fetchall()
        return {
            "protocol_id": row["protocol_id"], "protocol_hash": row["protocol_hash"],
            "state": state["state"], "state_version": int(state["state_version"]), "protocol": document,
            "events": [
                {"event_type": item["event_type"], "occurred_at": item["occurred_at"],
                 "payload": json.loads(item["payload_json"]), "event_hash": item["event_hash"]}
                for item in events
            ],
            "authority": {"prediction": False, "signal": False, "execution": False, "trading": False},
        }

    def _audit_access(self, protocol_id: str, *, requested_at: str, decision: str, purpose: str) -> None:
        with self._write() as connection:
            ordinal = connection.execute(
                "SELECT COUNT(*) FROM phase_e5_outcome_access_audit WHERE protocol_id=?", (protocol_id,),
            ).fetchone()[0]
            payload = {
                "protocol_id": protocol_id, "requested_at": normalized_utc(requested_at),
                "decision": decision, "purpose": purpose, "ordinal": ordinal,
            }
            connection.execute(
                """INSERT INTO phase_e5_outcome_access_audit(protocol_id,requested_at,decision,purpose,audit_hash)
                   VALUES (?,?,?,?,?)""",
                (protocol_id, payload["requested_at"], decision, purpose, canonical_hash(payload)),
            )

    def read_outcomes(
        self, protocol_id: str, *, reader: Callable[[], Sequence[OutcomeRecord]], requested_at: str,
        purpose: str = "FROZEN_FAMILY_INFERENCE",
    ) -> tuple[OutcomeRecord, ...]:
        """Invoke the outcome capability only after the state authorizes inference."""
        entry = self.get(protocol_id)
        if entry["state"] != ExperimentState.ELIGIBLE_FOR_INFERENCE.value:
            self._audit_access(protocol_id, requested_at=requested_at, decision="BLOCKED", purpose=purpose)
            raise OutcomeAccessForbidden("Evaluation outcomes remain sealed until all E.5 gates pass.")
        values = tuple(reader())
        self._audit_access(protocol_id, requested_at=requested_at, decision="GRANTED", purpose=purpose)
        return values

    def outcome_access_counts(self, protocol_id: str) -> dict[str, int]:
        self.get(protocol_id)
        with self._connection() as connection:
            counts = {row["decision"].lower(): int(row["count"]) for row in connection.execute(
                "SELECT decision,COUNT(*) AS count FROM phase_e5_outcome_access_audit WHERE protocol_id=? GROUP BY decision",
                (protocol_id,),
            )}
        return {"evaluation_outcome_reads": counts.get("granted", 0), "blocked_attempts": counts.get("blocked", 0)}

    def record_admissibility(
        self, protocol_id: str, report: AdmissibilityReport, *, recorded_at: str,
    ) -> dict[str, Any]:
        """Persist one deterministic gate decision without exposing outcomes."""
        self.initialize()
        if not isinstance(report, AdmissibilityReport):
            raise ValueError("E.5 admissibility requires a typed report.")
        allowed_sources = {
            ExperimentState.FROZEN_NOT_STARTED,
            ExperimentState.COLLECTING,
            ExperimentState.AWAITING_MATURITY,
        }
        with self._write() as connection:
            row = connection.execute(
                "SELECT * FROM phase_e5_experiments WHERE protocol_id=?", (protocol_id,),
            ).fetchone()
            if row is None:
                raise ProtocolIntegrityError("Unknown E.5 protocol.")
            current = ExperimentState(row["state"])
            if current not in allowed_sources:
                raise ProtocolConflictError(f"Cannot replace E.5 scientific state {current.value}.")
            at = normalized_utc(recorded_at)
            connection.execute(
                """UPDATE phase_e5_experiments
                   SET state=?,state_version=state_version+1,updated_at=?
                   WHERE protocol_id=? AND state=? AND state_version=?""",
                (report.state.value, at, protocol_id, current.value, int(row["state_version"])),
            )
            if connection.execute("SELECT changes()").fetchone()[0] != 1:
                raise ProtocolConflictError("Concurrent E.5 state transition lost compare-and-swap.")
            self._event(connection, protocol_id, "ADMISSIBILITY_DECIDED", at, {
                "prior_state": current.value, "state": report.state.value,
                "report_hash": report.report_hash, "diagnostics_hash": report.diagnostics_hash,
                "reasons": list(report.reasons), "evaluation_outcome_reads": 0,
            })
        return self.get(protocol_id)

    def infer(
        self, protocol_id: str, *, report: AdmissibilityReport,
        observations: Sequence[DesignObservation], outcome_reader: Callable[[], Sequence[OutcomeRecord]],
        hypothesis_id: str, requested_at: str,
    ) -> WildBootstrapResult:
        """Run the outcome-side method only for the exact persisted eligible report."""
        if report.state != ExperimentState.ELIGIBLE_FOR_INFERENCE:
            raise InferenceRefused("The supplied E.5 gate report is not eligible for inference.")
        entry = self.get(protocol_id)
        if entry["state"] != ExperimentState.ELIGIBLE_FOR_INFERENCE.value:
            raise InferenceRefused("The persisted E.5 state is not eligible for inference.")
        decided = [event for event in entry["events"] if event["event_type"] == "ADMISSIBILITY_DECIDED"]
        if not decided or decided[-1]["payload"].get("report_hash") != report.report_hash:
            raise ProtocolIntegrityError("Inference report is not the exact persisted E.5 gate decision.")
        outcomes = self.read_outcomes(
            protocol_id, reader=outcome_reader, requested_at=requested_at,
            purpose=f"FROZEN_FAMILY_INFERENCE:{hypothesis_id}",
        )
        return _wild_cluster_bootstrap_t(
            entry["protocol"], observations, outcomes, hypothesis_id=hypothesis_id,
        )

    @staticmethod
    def reserved_test_query_count() -> int:
        return 0
