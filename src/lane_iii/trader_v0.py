"""Lane III Phase D: the one commissioned Trader V0 signal policy.

This module is deliberately downstream of the frozen L3-C interpretation
state.  It consumes typed hypothesis snapshots and an explicit quality-only
market-state boundary.  Its entire authority is to return one of NO_TRADE,
LONG, SHORT, or EXIT.  It has no quantity, execution-intent, order, account,
broker, risk, outcome, copier, transport, or live-capital interface.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Iterable

from .contracts import (
    EvidenceFamily,
    EvidenceRelation,
    HypothesisDirection,
    LaneIIIInstrument,
    LaneIIIRefused,
    LaneIIIStrategyArtifact,
    canonical_hash,
    normalized_utc,
)
from .hypothesis_engine import (
    BEARISH_CONTINUATION,
    BULLISH_REVERSAL,
    L3C_VERSION,
    EvidenceState,
    EvidenceUsability,
    HypothesisEngineConfig,
    HypothesisEngineSnapshot,
    HypothesisIdentity,
    HypothesisRecord,
    HypothesisState,
)
from .market_data import DataQuality


L3D_SCHEMA = "lane-iii-phase-d-trader-v0-v1"
L3D_VERSION = "lane-iii-phase-d-v1"
TRADER_V0_STRATEGY_ID = "l3-trader-v0"
TRADER_V0_STRATEGY_VERSION = "1"


class SignalAuthorityRefused(LaneIIIRefused):
    """The exact commissioned strategy artifact is absent or altered."""


class TraderEvaluationRefused(LaneIIIRefused):
    """An L3-C/quality input cannot be evaluated without ambiguity."""


class SignalDecisionType(StrEnum):
    NO_TRADE = "NO_TRADE"
    LONG = "LONG"
    SHORT = "SHORT"
    EXIT = "EXIT"


class SignalReason(StrEnum):
    ENTRY_BULLISH_REVERSAL = "ENTRY_BULLISH_REVERSAL"
    ENTRY_BEARISH_CONTINUATION = "ENTRY_BEARISH_CONTINUATION"
    ACTIVE_THESIS_RETAINED = "ACTIVE_THESIS_RETAINED"
    NO_ELIGIBLE_HYPOTHESIS = "NO_ELIGIBLE_HYPOTHESIS"
    UNAUTHORIZED_HYPOTHESIS = "UNAUTHORIZED_HYPOTHESIS"
    HYPOTHESIS_NOT_ACTIVE = "HYPOTHESIS_NOT_ACTIVE"
    HYPOTHESIS_INVALIDATED = "HYPOTHESIS_INVALIDATED"
    HYPOTHESIS_EXPIRED = "HYPOTHESIS_EXPIRED"
    HYPOTHESIS_STALE = "HYPOTHESIS_STALE"
    EVIDENCE_STALE = "EVIDENCE_STALE"
    DATA_QUALITY_DEGRADED = "DATA_QUALITY_DEGRADED"
    BELOW_ENTRY_THRESHOLD = "BELOW_ENTRY_THRESHOLD"
    INSUFFICIENT_DOMINANCE = "INSUFFICIENT_DOMINANCE"
    INSUFFICIENT_FAMILY_BREADTH = "INSUFFICIENT_FAMILY_BREADTH"
    BLOCKING_CONTRADICTION = "BLOCKING_CONTRADICTION"
    ALREADY_SIGNALED_HYPOTHESIS = "ALREADY_SIGNALED_HYPOTHESIS"
    REENTRY_COOLDOWN = "REENTRY_COOLDOWN"
    CONFIDENCE_DECAY = "CONFIDENCE_DECAY"
    FAMILY_BREADTH_LOST = "FAMILY_BREADTH_LOST"
    DOMINANCE_LOST = "DOMINANCE_LOST"
    THESIS_INVALIDATED = "THESIS_INVALIDATED"
    THESIS_EXPIRED = "THESIS_EXPIRED"
    THESIS_MISSING = "THESIS_MISSING"
    THESIS_MAXIMUM_AGE = "THESIS_MAXIMUM_AGE"
    OPPOSING_HYPOTHESIS = "OPPOSING_HYPOTHESIS"


def _decimal(value: object, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a finite decimal.") from exc
    if not result.is_finite() or not Decimal(0) <= result <= Decimal(1):
        raise ValueError(f"{field} must be in [0, 1].")
    return result


def _time(value: object, field: str) -> datetime:
    return datetime.fromisoformat(normalized_utc(value, field).replace("Z", "+00:00"))


def _time_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field} must be lowercase SHA-256 text.")
    return value


@dataclass(frozen=True)
class TraderDataQuality:
    """Quality-only L3-B boundary; it carries no prices, flow, or orders."""

    evaluated_at: str
    market_state_hash: str
    trade: DataQuality
    quote: DataQuality
    book: DataQuality
    context: DataQuality

    def __post_init__(self) -> None:
        normalized_utc(self.evaluated_at, "Trader data-quality evaluation time")
        _sha256(self.market_state_hash, "Market-state hash")
        if any(type(value) is not DataQuality for value in (self.trade, self.quote, self.book, self.context)):
            raise ValueError("Trader data quality requires explicit L3-B quality states.")

    @property
    def healthy(self) -> bool:
        return all(value is DataQuality.HEALTHY for value in (self.trade, self.quote, self.book, self.context))

    def payload(self) -> dict[str, str]:
        return {
            "evaluated_at": normalized_utc(self.evaluated_at, "Trader data-quality evaluation time"),
            "market_state_hash": self.market_state_hash,
            "trade": self.trade.value,
            "quote": self.quote.value,
            "book": self.book.value,
            "context": self.context.value,
        }

    @property
    def snapshot_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class TraderV0Config:
    """The immutable and hash-bound Trader V0 policy parameters."""

    version: str = L3D_VERSION
    allowed_hypotheses: tuple[HypothesisIdentity, ...] = (BULLISH_REVERSAL, BEARISH_CONTINUATION)
    entry_relative_support: Decimal = Decimal("0.65")
    entry_dominance_margin: Decimal = Decimal("0.10")
    retention_relative_support: Decimal = Decimal("0.58")
    retention_dominance_margin: Decimal = Decimal("0.03")
    required_entry_families: tuple[EvidenceFamily, ...] = (
        EvidenceFamily.STRUCTURAL_CONTEXT,
        EvidenceFamily.ORDER_FLOW,
        EvidenceFamily.RESTING_LIQUIDITY,
    )
    minimum_entry_family_count: int = 3
    minimum_retention_family_count: int = 2
    hypothesis_freshness_seconds: int = 15
    evidence_freshness_seconds: int = 30
    maximum_entry_hypothesis_age_seconds: int = 60
    maximum_thesis_age_seconds: int = 120
    signal_ttl_seconds: int = 5
    reentry_cooldown_seconds: int = 30
    hypothesis_history_limit: int = 256

    def __post_init__(self) -> None:
        if not isinstance(self.version, str) or not self.version:
            raise ValueError("Trader V0 configuration version is required.")
        if not isinstance(self.allowed_hypotheses, tuple) or not self.allowed_hypotheses:
            raise ValueError("Trader V0 requires an immutable allowed-hypothesis set.")
        if any(type(value) is not HypothesisIdentity for value in self.allowed_hypotheses):
            raise ValueError("Allowed hypotheses must be explicit L3-C identities.")
        if len(self.allowed_hypotheses) != len(set(self.allowed_hypotheses)):
            raise ValueError("Allowed hypotheses may not repeat.")
        for field, value in (
            ("Entry relative support", self.entry_relative_support),
            ("Entry dominance margin", self.entry_dominance_margin),
            ("Retention relative support", self.retention_relative_support),
            ("Retention dominance margin", self.retention_dominance_margin),
        ):
            _decimal(value, field)
        if self.retention_relative_support >= self.entry_relative_support:
            raise ValueError("Retention support must be below entry support to provide hysteresis.")
        if self.retention_dominance_margin >= self.entry_dominance_margin:
            raise ValueError("Retention dominance must be below entry dominance to provide hysteresis.")
        if not isinstance(self.required_entry_families, tuple) or not self.required_entry_families:
            raise ValueError("Required entry families must be an immutable non-empty tuple.")
        if any(type(value) is not EvidenceFamily for value in self.required_entry_families):
            raise ValueError("Required entry families must be explicit evidence families.")
        if len(self.required_entry_families) != len(set(self.required_entry_families)):
            raise ValueError("Required entry families may not repeat.")
        for field, value in (
            ("Minimum entry family count", self.minimum_entry_family_count),
            ("Minimum retention family count", self.minimum_retention_family_count),
            ("Hypothesis freshness seconds", self.hypothesis_freshness_seconds),
            ("Evidence freshness seconds", self.evidence_freshness_seconds),
            ("Maximum entry hypothesis age seconds", self.maximum_entry_hypothesis_age_seconds),
            ("Maximum thesis age seconds", self.maximum_thesis_age_seconds),
            ("Signal TTL seconds", self.signal_ttl_seconds),
            ("Re-entry cooldown seconds", self.reentry_cooldown_seconds),
            ("Hypothesis history limit", self.hypothesis_history_limit),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{field} must be a positive whole-second/count value.")
        if self.minimum_entry_family_count < len(self.required_entry_families):
            raise ValueError("Entry family count may not be smaller than the explicit required set.")
        if self.minimum_retention_family_count >= self.minimum_entry_family_count:
            raise ValueError("Retention breadth must be smaller than entry breadth to provide hysteresis.")

    def payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "allowed_hypotheses": [value.payload() for value in self.allowed_hypotheses],
            "entry_relative_support": str(self.entry_relative_support),
            "entry_dominance_margin": str(self.entry_dominance_margin),
            "retention_relative_support": str(self.retention_relative_support),
            "retention_dominance_margin": str(self.retention_dominance_margin),
            "required_entry_families": [value.value for value in self.required_entry_families],
            "minimum_entry_family_count": self.minimum_entry_family_count,
            "minimum_retention_family_count": self.minimum_retention_family_count,
            "hypothesis_freshness_seconds": self.hypothesis_freshness_seconds,
            "evidence_freshness_seconds": self.evidence_freshness_seconds,
            "maximum_entry_hypothesis_age_seconds": self.maximum_entry_hypothesis_age_seconds,
            "maximum_thesis_age_seconds": self.maximum_thesis_age_seconds,
            "signal_ttl_seconds": self.signal_ttl_seconds,
            "reentry_cooldown_seconds": self.reentry_cooldown_seconds,
            "hypothesis_history_limit": self.hypothesis_history_limit,
            "same_event_reversal": False,
            "confidence_semantics": "RELATIVE_SUPPORT_NOT_PROBABILITY",
            "strategy_owned_position_truth": False,
            "strategy_owned_sizing": False,
        }


COMMISSIONED_TRADER_V0_CONFIG = TraderV0Config()
COMMISSIONED_L3C_CONFIGURATION_HASH = HypothesisEngineConfig().configuration_hash


def strategy_artifact_payload(config: TraderV0Config) -> dict[str, object]:
    if type(config) is not TraderV0Config:
        raise ValueError("Strategy artifact construction requires exact TraderV0Config.")
    return {
        "schema": "lane-iii-phase-d-trader-v0-strategy-artifact-v1",
        "strategy_id": TRADER_V0_STRATEGY_ID,
        "strategy_version": TRADER_V0_STRATEGY_VERSION,
        "instrument": LaneIIIInstrument.MNQ.value,
        "authority": "DIRECTIONAL_SIGNAL_ONLY",
        "expected_l3c_version": L3C_VERSION,
        "expected_l3c_configuration_hash": COMMISSIONED_L3C_CONFIGURATION_HASH,
        "policy": config.payload(),
        "policy_semantics": {
            "candidate_ranking": "RELATIVE_SUPPORT_DESC_THEN_HYPOTHESIS_ID_ASC",
            "competition_scope": "EVERY_CURRENT_L3C_HYPOTHESIS_INCLUDING_UNAUTHORIZED",
            "entry_conjunction": [
                "ALL_DATA_QUALITY_FIELDS_HEALTHY",
                "HYPOTHESIS_ARCHETYPE_ALLOWED",
                "HYPOTHESIS_STATE_ACTIVE",
                "NO_INVALIDATOR",
                "NO_CONTRADICTORY_EVIDENCE_OR_FAMILY_CONTRIBUTION",
                "HYPOTHESIS_AND_CONTRIBUTING_EVIDENCE_FRESH",
                "RELATIVE_SUPPORT_AT_OR_ABOVE_ENTRY_THRESHOLD",
                "LEAD_AT_OR_ABOVE_ENTRY_DOMINANCE_MARGIN",
                "REQUIRED_POSITIVE_FAMILY_BREADTH",
                "HYPOTHESIS_NOT_PREVIOUSLY_SIGNALED",
                "EVENT_TIME_REENTRY_COOLDOWN_COMPLETE",
            ],
            "dependence_control": "CONSUME_EACH_L3C_FAMILY_CONTRIBUTION_ONCE_NEVER_RECOUNT_EVIDENCE",
            "exit_precedence": [
                "THESIS_MISSING",
                "DATA_QUALITY_DEGRADED",
                "THESIS_INVALIDATED",
                "THESIS_EXPIRED",
                "THESIS_MAXIMUM_AGE",
                "DECISIVE_OPPOSING_HYPOTHESIS",
                "BLOCKING_CONTRADICTION",
                "CONFIDENCE_BELOW_RETENTION_THRESHOLD",
                "RETENTION_FAMILY_BREADTH_LOST",
                "RETENTION_DOMINANCE_LOST",
                "HYPOTHESIS_OR_EVIDENCE_STALE",
            ],
            "active_thesis_qualified": "NO_TRADE_ACTIVE_THESIS_RETAINED",
            "same_source_state_repeat": "RETURN_IDENTICAL_DECISION_ID",
            "same_hypothesis_reentry": "PROHIBITED_WITH_BOUNDED_HISTORY",
            "opposing_entry_while_active": "EXIT_ONLY_NO_SAME_EVENT_REVERSAL",
            "active_state_semantics": "STRATEGY_DECISION_STATE_NOT_POSITION_OR_FILL_TRUTH",
            "degraded_quality_exit_semantics": "STRATEGY_SIGNAL_ONLY_NOT_SOVEREIGN_FLATTEN",
            "time_source": "L3C_EVALUATION_EVENT_TIME_ONLY",
        },
        "outputs": [value.value for value in SignalDecisionType],
        "signal_contract_fields": [
            "decision_id",
            "strategy_identity",
            "strategy_artifact_hash",
            "decision",
            "hypothesis_id",
            "related_hypothesis_id",
            "created_at",
            "expires_at",
            "relative_support_snapshot",
            "family_summary",
            "reason_code",
            "l3c_snapshot_hash",
            "data_quality_hash",
            "source_state_hash",
        ],
        "denied_authority": [
            "ACCOUNT_SELECTION",
            "BROKER_CONTACT",
            "COPIER_CONTROL",
            "EXECUTION_INTENT_CONSTRUCTION",
            "HARD_RISK_OVERRIDE",
            "LIVE_CAPITAL",
            "ORDER_CONSTRUCTION",
            "PHASE_E_MODIFICATION",
            "POSITION_RECONCILIATION",
            "POSITION_SIZING",
            "SELF_MODIFICATION",
        ],
    }


TRADER_V0_ARTIFACT_PAYLOAD = strategy_artifact_payload(COMMISSIONED_TRADER_V0_CONFIG)
TRADER_V0_ARTIFACT_HASH = canonical_hash(TRADER_V0_ARTIFACT_PAYLOAD)
TRADER_V0_STRATEGY = LaneIIIStrategyArtifact(
    strategy_id=TRADER_V0_STRATEGY_ID,
    strategy_version=TRADER_V0_STRATEGY_VERSION,
    strategy_artifact_hash=TRADER_V0_ARTIFACT_HASH,
    strategy_instrument=LaneIIIInstrument.MNQ,
)


def build_strategy_artifact(config: TraderV0Config) -> LaneIIIStrategyArtifact:
    """Build an identity for audit; only the exact commissioned one is admitted."""

    return LaneIIIStrategyArtifact(
        strategy_id=TRADER_V0_STRATEGY_ID,
        strategy_version=TRADER_V0_STRATEGY_VERSION,
        strategy_artifact_hash=canonical_hash(strategy_artifact_payload(config)),
        strategy_instrument=LaneIIIInstrument.MNQ,
    )


@dataclass(frozen=True)
class SignalAuthorityRegistration:
    strategy_identity: str
    strategy_artifact_hash: str
    signal_authority: bool

    def __post_init__(self) -> None:
        if self.strategy_identity != TRADER_V0_STRATEGY.strategy_identity:
            raise ValueError("L3-D can register only the exact Trader V0 identity.")
        if self.strategy_artifact_hash != TRADER_V0_ARTIFACT_HASH or self.signal_authority is not True:
            raise ValueError("L3-D registration requires the exact artifact and signal-only grant.")


@dataclass(frozen=True)
class SignalAuthorityRegistry:
    registrations: tuple[SignalAuthorityRegistration, ...]

    def __post_init__(self) -> None:
        if type(self.registrations) is not tuple or len(self.registrations) != 1:
            raise ValueError("L3-D commissions exactly one signal-authority registration.")

    def require(self, artifact: LaneIIIStrategyArtifact) -> SignalAuthorityRegistration:
        if type(artifact) is not LaneIIIStrategyArtifact:
            raise SignalAuthorityRefused("Signal authority requires an exact Lane III strategy artifact.")
        for registration in self.registrations:
            if (
                artifact.strategy_identity == registration.strategy_identity
                and artifact.strategy_artifact_hash == registration.strategy_artifact_hash
                and artifact.strategy_id == TRADER_V0_STRATEGY_ID
                and artifact.strategy_version == TRADER_V0_STRATEGY_VERSION
                and artifact.strategy_instrument is LaneIIIInstrument.MNQ
            ):
                return registration
        if artifact.strategy_id == TRADER_V0_STRATEGY_ID and artifact.strategy_version == TRADER_V0_STRATEGY_VERSION:
            raise SignalAuthorityRefused("Trader V0 strategy artifact hash is not commissioned.")
        raise SignalAuthorityRefused("Strategy identity has no L3-D signal authority.")


TRADER_V0_AUTHORITY_REGISTRY = SignalAuthorityRegistry((SignalAuthorityRegistration(
    TRADER_V0_STRATEGY.strategy_identity, TRADER_V0_ARTIFACT_HASH, True,
),))


@dataclass(frozen=True)
class EvidenceTrace:
    evidence_id: str
    evidence_snapshot_hash: str
    source_observation_id: str
    source_event_ids: tuple[str, ...]
    source_payload_hashes: tuple[str, ...]

    def payload(self) -> dict[str, object]:
        return {
            "evidence_id": self.evidence_id,
            "evidence_snapshot_hash": self.evidence_snapshot_hash,
            "source_observation_id": self.source_observation_id,
            "source_event_ids": list(self.source_event_ids),
            "source_payload_hashes": list(self.source_payload_hashes),
        }


@dataclass(frozen=True)
class FamilySignalSummary:
    family: EvidenceFamily
    strongest_support: Decimal
    strongest_contradiction: Decimal
    balance: Decimal
    supporting_evidence: tuple[EvidenceTrace, ...]
    contradictory_evidence: tuple[EvidenceTrace, ...]

    def __post_init__(self) -> None:
        if type(self.family) is not EvidenceFamily:
            raise ValueError("Signal family summary requires an explicit family.")
        if self.balance != self.strongest_support - self.strongest_contradiction:
            raise ValueError("Signal family summary balance is inconsistent.")

    def payload(self) -> dict[str, object]:
        return {
            "family": self.family.value,
            "strongest_support": str(self.strongest_support),
            "strongest_contradiction": str(self.strongest_contradiction),
            "balance": str(self.balance),
            "supporting_evidence": [value.payload() for value in self.supporting_evidence],
            "contradictory_evidence": [value.payload() for value in self.contradictory_evidence],
        }


@dataclass(frozen=True)
class SignalDecision:
    """A direction-only strategy decision, deliberately not an execution intent."""

    decision_id: str
    strategy_identity: str
    strategy_artifact_hash: str
    decision: SignalDecisionType
    hypothesis_id: str | None
    related_hypothesis_id: str | None
    created_at: str
    expires_at: str
    relative_support_snapshot: Decimal | None
    family_summary: tuple[FamilySignalSummary, ...]
    reason_code: SignalReason
    l3c_snapshot_hash: str
    data_quality_hash: str
    source_state_hash: str

    def __post_init__(self) -> None:
        if self.strategy_identity != TRADER_V0_STRATEGY.strategy_identity or self.strategy_artifact_hash != TRADER_V0_ARTIFACT_HASH:
            raise ValueError("Signal decision must be bound to the commissioned Trader V0 artifact.")
        if type(self.decision) is not SignalDecisionType or type(self.reason_code) is not SignalReason:
            raise ValueError("Signal decision and reason must be explicit enums.")
        if self.decision in {SignalDecisionType.LONG, SignalDecisionType.SHORT, SignalDecisionType.EXIT} and (
            not isinstance(self.hypothesis_id, str) or not self.hypothesis_id.startswith("l3c-h-")
        ):
            raise ValueError("Directional and exit signals require an L3-C hypothesis identity.")
        if self.related_hypothesis_id is not None and (
            self.decision is not SignalDecisionType.EXIT
            or not self.related_hypothesis_id.startswith("l3c-h-")
        ):
            raise ValueError("A related hypothesis is valid only on an EXIT decision.")
        created = _time(self.created_at, "Signal creation time")
        expires = _time(self.expires_at, "Signal expiry time")
        if expires <= created:
            raise ValueError("Signal expiration must follow creation.")
        if self.relative_support_snapshot is not None:
            _decimal(self.relative_support_snapshot, "Signal relative-support snapshot")
        if not isinstance(self.family_summary, tuple) or any(type(value) is not FamilySignalSummary for value in self.family_summary):
            raise ValueError("Signal family summaries must be an immutable tuple.")
        for field, value in (
            ("L3-C snapshot hash", self.l3c_snapshot_hash),
            ("Data-quality hash", self.data_quality_hash),
            ("Source-state hash", self.source_state_hash),
        ):
            _sha256(value, field)
        expected = "l3d-d-" + canonical_hash(self.payload_without_identity())[:32]
        if self.decision_id != expected:
            raise ValueError("Signal decision identity does not match its deterministic payload.")

    @classmethod
    def create(
        cls,
        *,
        decision: SignalDecisionType,
        hypothesis_id: str | None,
        related_hypothesis_id: str | None,
        created_at: str,
        expires_at: str,
        relative_support_snapshot: Decimal | None,
        family_summary: tuple[FamilySignalSummary, ...],
        reason_code: SignalReason,
        l3c_snapshot_hash: str,
        data_quality_hash: str,
        source_state_hash: str,
    ) -> "SignalDecision":
        payload = cls._payload_without_identity_static(
            decision, hypothesis_id, related_hypothesis_id, created_at, expires_at, relative_support_snapshot, family_summary,
            reason_code, l3c_snapshot_hash, data_quality_hash, source_state_hash,
        )
        return cls(
            "l3d-d-" + canonical_hash(payload)[:32], TRADER_V0_STRATEGY.strategy_identity,
            TRADER_V0_ARTIFACT_HASH, decision, hypothesis_id, related_hypothesis_id, created_at, expires_at,
            relative_support_snapshot, family_summary, reason_code, l3c_snapshot_hash,
            data_quality_hash, source_state_hash,
        )

    @staticmethod
    def _payload_without_identity_static(
        decision: SignalDecisionType,
        hypothesis_id: str | None,
        related_hypothesis_id: str | None,
        created_at: str,
        expires_at: str,
        relative_support_snapshot: Decimal | None,
        family_summary: tuple[FamilySignalSummary, ...],
        reason_code: SignalReason,
        l3c_snapshot_hash: str,
        data_quality_hash: str,
        source_state_hash: str,
    ) -> dict[str, object]:
        return {
            "strategy_identity": TRADER_V0_STRATEGY.strategy_identity,
            "strategy_artifact_hash": TRADER_V0_ARTIFACT_HASH,
            "decision": decision.value,
            "hypothesis_id": hypothesis_id,
            "related_hypothesis_id": related_hypothesis_id,
            "created_at": normalized_utc(created_at, "Signal creation time"),
            "expires_at": normalized_utc(expires_at, "Signal expiry time"),
            "relative_support_snapshot": None if relative_support_snapshot is None else str(relative_support_snapshot),
            "family_summary": [value.payload() for value in family_summary],
            "reason_code": reason_code.value,
            "l3c_snapshot_hash": l3c_snapshot_hash,
            "data_quality_hash": data_quality_hash,
            "source_state_hash": source_state_hash,
        }

    def payload_without_identity(self) -> dict[str, object]:
        return self._payload_without_identity_static(
            self.decision, self.hypothesis_id, self.related_hypothesis_id, self.created_at, self.expires_at,
            self.relative_support_snapshot, self.family_summary, self.reason_code,
            self.l3c_snapshot_hash, self.data_quality_hash, self.source_state_hash,
        )

    def payload(self) -> dict[str, object]:
        return {"decision_id": self.decision_id, **self.payload_without_identity()}

    @property
    def decision_hash(self) -> str:
        return canonical_hash(self.payload())


@dataclass(frozen=True)
class TraderV0Metrics:
    evaluations: int
    no_trade_results: int
    long_signals: int
    short_signals: int
    exit_signals: int
    blocked_entries_by_reason: tuple[tuple[str, int], ...]
    duplicate_suppression: int
    hypothesis_expirations_encountered: int
    retained_signaled_hypothesis_ids: int


@dataclass(frozen=True)
class _ActiveThesis:
    hypothesis_id: str
    direction: HypothesisDirection
    entry_decision_id: str
    activated_at: str


class TraderV0:
    """Stateful strategy-decision lifecycle with no brokerage position truth."""

    def __init__(
        self,
        artifact: LaneIIIStrategyArtifact = TRADER_V0_STRATEGY,
        config: TraderV0Config = COMMISSIONED_TRADER_V0_CONFIG,
    ) -> None:
        if type(config) is not TraderV0Config:
            raise SignalAuthorityRefused("Trader V0 requires an exact immutable configuration.")
        expected = build_strategy_artifact(config)
        if artifact.strategy_artifact_hash != expected.strategy_artifact_hash or artifact.strategy_identity != expected.strategy_identity:
            raise SignalAuthorityRefused("Strategy artifact does not match the supplied immutable policy.")
        self.registration = TRADER_V0_AUTHORITY_REGISTRY.require(artifact)
        self.artifact = artifact
        self.config = config
        self._active: _ActiveThesis | None = None
        self._last_evaluated_at: datetime | None = None
        self._last_source_state_hash: str | None = None
        self._last_decision: SignalDecision | None = None
        self._last_exit_at: datetime | None = None
        self._signaled_ids: set[str] = set()
        self._signaled_order: deque[str] = deque()
        self._evaluations = 0
        self._no_trade = 0
        self._long = 0
        self._short = 0
        self._exit = 0
        self._blocked: dict[str, int] = {}
        self._duplicate_suppression = 0
        self._expirations = 0

    @property
    def active_thesis_id(self) -> str | None:
        """Strategy lifecycle state only; it is not actual position/fill truth."""

        return None if self._active is None else self._active.hypothesis_id

    def metrics(self) -> TraderV0Metrics:
        return TraderV0Metrics(
            self._evaluations, self._no_trade, self._long, self._short, self._exit,
            tuple(sorted(self._blocked.items())), self._duplicate_suppression,
            self._expirations, len(self._signaled_ids),
        )

    def evaluate(self, snapshot: HypothesisEngineSnapshot, quality: TraderDataQuality) -> SignalDecision:
        self._validate_input(snapshot, quality)
        now = _time(snapshot.evaluated_at, "Trader evaluation time")
        if self._last_evaluated_at is not None and now < self._last_evaluated_at:
            raise TraderEvaluationRefused("Trader evaluation time may not move backward.")
        source_state_hash = canonical_hash({
            "schema": "lane-iii-phase-d-source-state-v1",
            "l3c_snapshot_hash": snapshot.snapshot_hash,
            "data_quality": quality.payload(),
        })
        if source_state_hash == self._last_source_state_hash:
            self._duplicate_suppression += 1
            assert self._last_decision is not None
            return self._last_decision

        evidence = {item.evidence.evidence_id: item for item in snapshot.evidence}
        current = tuple(
            item for item in snapshot.hypotheses
            if item.state in {
                HypothesisState.FORMING,
                HypothesisState.ACTIVE,
                HypothesisState.CONFLICTED,
                HypothesisState.DECAYING,
            }
        )
        self._evaluations += 1
        if self._active is not None:
            decision = self._evaluate_active(snapshot, quality, current, evidence, now, source_state_hash)
        else:
            decision = self._evaluate_entry(snapshot, quality, current, evidence, now, source_state_hash)
        self._last_evaluated_at = now
        self._last_source_state_hash = source_state_hash
        self._last_decision = decision
        self._record_result(decision)
        return decision

    def _validate_input(self, snapshot: HypothesisEngineSnapshot, quality: TraderDataQuality) -> None:
        if type(snapshot) is not HypothesisEngineSnapshot or type(quality) is not TraderDataQuality:
            raise TraderEvaluationRefused("Trader V0 requires exact L3-C snapshot and quality-boundary types.")
        if snapshot.configuration_hash != COMMISSIONED_L3C_CONFIGURATION_HASH:
            raise TraderEvaluationRefused("L3-C configuration is not the hash-bound commissioned version.")
        if normalized_utc(snapshot.evaluated_at, "L3-C snapshot time") != normalized_utc(quality.evaluated_at, "Quality snapshot time"):
            raise TraderEvaluationRefused("L3-C and quality snapshots must share one evaluation time.")
        if any(type(item) is not HypothesisRecord for item in snapshot.hypotheses):
            raise TraderEvaluationRefused("L3-C hypothesis records must be exact immutable types.")
        if any(type(item) is not EvidenceState for item in snapshot.evidence):
            raise TraderEvaluationRefused("L3-C evidence states must be exact immutable types.")
        hypothesis_ids = [item.hypothesis_id for item in snapshot.hypotheses]
        evidence_ids = [item.evidence.evidence_id for item in snapshot.evidence]
        if len(hypothesis_ids) != len(set(hypothesis_ids)) or len(evidence_ids) != len(set(evidence_ids)):
            raise TraderEvaluationRefused("L3-C snapshot identities may not repeat.")
        if any(item.configuration_hash != snapshot.configuration_hash for item in snapshot.hypotheses):
            raise TraderEvaluationRefused("Hypothesis configuration does not match its L3-C snapshot.")

    def _evaluate_entry(
        self,
        snapshot: HypothesisEngineSnapshot,
        quality: TraderDataQuality,
        current: tuple[HypothesisRecord, ...],
        evidence: dict[str, EvidenceState],
        now: datetime,
        source_state_hash: str,
    ) -> SignalDecision:
        allowed = tuple(item for item in current if item.identity in self.config.allowed_hypotheses)
        if not allowed:
            unauthorized = tuple(item for item in current if item.identity not in self.config.allowed_hypotheses)
            if unauthorized:
                candidate = self._rank(unauthorized)[0]
                return self._decision(snapshot, quality, candidate, SignalDecisionType.NO_TRADE, SignalReason.UNAUTHORIZED_HYPOTHESIS, evidence, now, source_state_hash)
            terminal = tuple(
                item for item in snapshot.hypotheses
                if item.identity in self.config.allowed_hypotheses
                and item.state in {HypothesisState.INVALIDATED, HypothesisState.EXPIRED}
            )
            if terminal:
                candidate = sorted(terminal, key=lambda item: (item.last_updated_at, item.hypothesis_id), reverse=True)[0]
                reason = SignalReason.HYPOTHESIS_INVALIDATED if candidate.state is HypothesisState.INVALIDATED else SignalReason.HYPOTHESIS_EXPIRED
                if reason is SignalReason.HYPOTHESIS_EXPIRED:
                    self._expirations += 1
                return self._decision(snapshot, quality, candidate, SignalDecisionType.NO_TRADE, reason, evidence, now, source_state_hash)
            return self._decision(snapshot, quality, None, SignalDecisionType.NO_TRADE, SignalReason.NO_ELIGIBLE_HYPOTHESIS, evidence, now, source_state_hash)

        candidate = self._rank(allowed)[0]
        reason = self._entry_block(candidate, current, quality, evidence, now, include_lifecycle_history=True)
        if reason is not None:
            return self._decision(snapshot, quality, candidate, SignalDecisionType.NO_TRADE, reason, evidence, now, source_state_hash)
        decision_type = SignalDecisionType.LONG if candidate.identity.direction is HypothesisDirection.BULLISH else SignalDecisionType.SHORT
        reason = SignalReason.ENTRY_BULLISH_REVERSAL if decision_type is SignalDecisionType.LONG else SignalReason.ENTRY_BEARISH_CONTINUATION
        decision = self._decision(snapshot, quality, candidate, decision_type, reason, evidence, now, source_state_hash)
        self._active = _ActiveThesis(candidate.hypothesis_id, candidate.identity.direction, decision.decision_id, _time_text(now))
        self._remember_signaled(candidate.hypothesis_id)
        return decision

    def _evaluate_active(
        self,
        snapshot: HypothesisEngineSnapshot,
        quality: TraderDataQuality,
        current: tuple[HypothesisRecord, ...],
        evidence: dict[str, EvidenceState],
        now: datetime,
        source_state_hash: str,
    ) -> SignalDecision:
        assert self._active is not None
        record = next((item for item in snapshot.hypotheses if item.hypothesis_id == self._active.hypothesis_id), None)
        reason: SignalReason | None = None
        related_hypothesis_id: str | None = None
        if record is None:
            reason = SignalReason.THESIS_MISSING
        elif not quality.healthy:
            reason = SignalReason.DATA_QUALITY_DEGRADED
        elif record.state is HypothesisState.INVALIDATED or record.invalidated_by_evidence_ids:
            reason = SignalReason.THESIS_INVALIDATED
        elif record.state is HypothesisState.EXPIRED or now >= _time(record.expires_at, "Hypothesis expiry time"):
            reason = SignalReason.THESIS_EXPIRED
            self._expirations += 1
        elif now - _time(self._active.activated_at, "Thesis activation time") >= timedelta(seconds=self.config.maximum_thesis_age_seconds):
            reason = SignalReason.THESIS_MAXIMUM_AGE
        else:
            opposing = self._decisive_opposing(record, current, quality, evidence, now)
            if opposing is not None:
                reason = SignalReason.OPPOSING_HYPOTHESIS
                related_hypothesis_id = opposing.hypothesis_id
            elif record.state is HypothesisState.CONFLICTED or record.contradictory_evidence_ids or record.invalidated_by_evidence_ids or any(
                item.strongest_contradiction > 0 for item in record.confidence.family_contributions
            ):
                reason = SignalReason.BLOCKING_CONTRADICTION
            elif record.confidence.relative_support < self.config.retention_relative_support:
                reason = SignalReason.CONFIDENCE_DECAY
            elif not self._retention_breadth(record, evidence, now):
                reason = SignalReason.FAMILY_BREADTH_LOST
            elif self._margin(record, current) < self.config.retention_dominance_margin:
                reason = SignalReason.DOMINANCE_LOST
            elif not self._hypothesis_fresh(record, now, for_entry=False) or not self._evidence_fresh(record, evidence, now):
                reason = SignalReason.HYPOTHESIS_STALE

        if reason is not None:
            decision = self._decision(
                snapshot, quality, record, SignalDecisionType.EXIT, reason, evidence, now, source_state_hash,
                hypothesis_id_override=self._active.hypothesis_id, related_hypothesis_id=related_hypothesis_id,
            )
            self._active = None
            self._last_exit_at = now
            return decision
        return self._decision(snapshot, quality, record, SignalDecisionType.NO_TRADE, SignalReason.ACTIVE_THESIS_RETAINED, evidence, now, source_state_hash)

    def _entry_block(
        self,
        record: HypothesisRecord,
        current: tuple[HypothesisRecord, ...],
        quality: TraderDataQuality,
        evidence: dict[str, EvidenceState],
        now: datetime,
        *,
        include_lifecycle_history: bool,
    ) -> SignalReason | None:
        if not quality.healthy:
            return SignalReason.DATA_QUALITY_DEGRADED
        if record.identity not in self.config.allowed_hypotheses:
            return SignalReason.UNAUTHORIZED_HYPOTHESIS
        if record.state is HypothesisState.INVALIDATED or record.invalidated_by_evidence_ids:
            return SignalReason.HYPOTHESIS_INVALIDATED
        if record.state is HypothesisState.EXPIRED or now >= _time(record.expires_at, "Hypothesis expiry time"):
            return SignalReason.HYPOTHESIS_EXPIRED
        if record.state is HypothesisState.CONFLICTED or record.contradictory_evidence_ids or any(
            item.strongest_contradiction > 0 for item in record.confidence.family_contributions
        ):
            return SignalReason.BLOCKING_CONTRADICTION
        if record.state is not HypothesisState.ACTIVE:
            return SignalReason.HYPOTHESIS_NOT_ACTIVE
        if not self._hypothesis_fresh(record, now, for_entry=True):
            return SignalReason.HYPOTHESIS_STALE
        if not self._evidence_fresh(record, evidence, now):
            return SignalReason.EVIDENCE_STALE
        if record.confidence.relative_support < self.config.entry_relative_support:
            return SignalReason.BELOW_ENTRY_THRESHOLD
        if self._margin(record, current) < self.config.entry_dominance_margin:
            return SignalReason.INSUFFICIENT_DOMINANCE
        positive = self._positive_families(record)
        if len(positive) < self.config.minimum_entry_family_count or not set(self.config.required_entry_families).issubset(positive):
            return SignalReason.INSUFFICIENT_FAMILY_BREADTH
        if include_lifecycle_history and record.hypothesis_id in self._signaled_ids:
            return SignalReason.ALREADY_SIGNALED_HYPOTHESIS
        if include_lifecycle_history and self._last_exit_at is not None and now - self._last_exit_at < timedelta(seconds=self.config.reentry_cooldown_seconds):
            return SignalReason.REENTRY_COOLDOWN
        return None

    def _decisive_opposing(
        self,
        active: HypothesisRecord,
        current: tuple[HypothesisRecord, ...],
        quality: TraderDataQuality,
        evidence: dict[str, EvidenceState],
        now: datetime,
    ) -> HypothesisRecord | None:
        opposing = self._rank(tuple(
            item for item in current
            if item.identity.direction is not active.identity.direction
            and item.identity in self.config.allowed_hypotheses
        ))
        for candidate in opposing:
            if self._entry_block(candidate, current, quality, evidence, now, include_lifecycle_history=False) is None:
                return candidate
        return None

    @staticmethod
    def _rank(records: tuple[HypothesisRecord, ...]) -> tuple[HypothesisRecord, ...]:
        return tuple(sorted(records, key=lambda item: (-item.confidence.relative_support, item.hypothesis_id)))

    @staticmethod
    def _margin(record: HypothesisRecord, current: tuple[HypothesisRecord, ...]) -> Decimal:
        competitor = max(
            (item.confidence.relative_support for item in current if item.hypothesis_id != record.hypothesis_id),
            default=Decimal(0),
        )
        return record.confidence.relative_support - competitor

    def _hypothesis_fresh(self, record: HypothesisRecord, now: datetime, *, for_entry: bool) -> bool:
        created = _time(record.created_at, "Hypothesis creation time")
        updated = _time(record.last_updated_at, "Hypothesis update time")
        evaluated = _time(record.confidence.evaluated_at, "Confidence evaluation time")
        fresh = (
            created <= updated <= now
            and evaluated == now
            and now < _time(record.expires_at, "Hypothesis expiry time")
            and now - updated <= timedelta(seconds=self.config.hypothesis_freshness_seconds)
        )
        return fresh and (
            not for_entry
            or now - created <= timedelta(seconds=self.config.maximum_entry_hypothesis_age_seconds)
        )

    def _evidence_fresh(self, record: HypothesisRecord, evidence: dict[str, EvidenceState], now: datetime) -> bool:
        referenced = {
            identifier
            for contribution in record.confidence.family_contributions
            if contribution.balance > 0
            for identifier in contribution.supporting_evidence_ids
        }
        if not referenced:
            return False
        for identifier in referenced:
            state = evidence.get(identifier)
            if state is None or state.usability is not EvidenceUsability.AUTHORITATIVE:
                return False
            item = state.evidence
            source_end = _time(item.source.window_end, "Evidence source window end")
            if (
                item.source.source_quality is not DataQuality.HEALTHY
                or source_end > now
                or now - source_end > timedelta(seconds=self.config.evidence_freshness_seconds)
                or now >= _time(item.expires_at, "Evidence expiry time")
            ):
                return False
            contribution = next(value for value in record.confidence.family_contributions if identifier in value.supporting_evidence_ids)
            if item.family is not contribution.family or not any(
                impact.hypothesis == record.identity and impact.relation is EvidenceRelation.SUPPORTS
                for impact in item.impacts
            ):
                return False
        return True

    @staticmethod
    def _positive_families(record: HypothesisRecord) -> set[EvidenceFamily]:
        return {
            item.family for item in record.confidence.family_contributions
            if item.balance > 0 and item.strongest_support > 0 and item.supporting_evidence_ids
        }

    def _retention_breadth(self, record: HypothesisRecord, evidence: dict[str, EvidenceState], now: datetime) -> bool:
        positive = self._positive_families(record)
        microstructure = {EvidenceFamily.ORDER_FLOW, EvidenceFamily.RESTING_LIQUIDITY}
        return (
            len(positive) >= self.config.minimum_retention_family_count
            and EvidenceFamily.STRUCTURAL_CONTEXT in positive
            and bool(positive & microstructure)
            and self._evidence_fresh(record, evidence, now)
        )

    def _summaries(self, record: HypothesisRecord | None, evidence: dict[str, EvidenceState]) -> tuple[FamilySignalSummary, ...]:
        if record is None:
            return ()
        result: list[FamilySignalSummary] = []
        for contribution in record.confidence.family_contributions:
            result.append(FamilySignalSummary(
                contribution.family,
                contribution.strongest_support,
                contribution.strongest_contradiction,
                contribution.balance,
                tuple(self._trace(identifier, evidence) for identifier in contribution.supporting_evidence_ids if identifier in evidence),
                tuple(self._trace(identifier, evidence) for identifier in contribution.contradictory_evidence_ids if identifier in evidence),
            ))
        return tuple(result)

    @staticmethod
    def _trace(identifier: str, evidence: dict[str, EvidenceState]) -> EvidenceTrace:
        state = evidence[identifier]
        item = state.evidence
        return EvidenceTrace(
            item.evidence_id, item.snapshot_hash, item.source.source_observation_id,
            item.source.source_event_ids, item.source.source_payload_hashes,
        )

    def _decision(
        self,
        snapshot: HypothesisEngineSnapshot,
        quality: TraderDataQuality,
        record: HypothesisRecord | None,
        decision_type: SignalDecisionType,
        reason: SignalReason,
        evidence: dict[str, EvidenceState],
        now: datetime,
        source_state_hash: str,
        *,
        hypothesis_id_override: str | None = None,
        related_hypothesis_id: str | None = None,
    ) -> SignalDecision:
        return SignalDecision.create(
            decision=decision_type,
            hypothesis_id=hypothesis_id_override if hypothesis_id_override is not None else None if record is None else record.hypothesis_id,
            related_hypothesis_id=related_hypothesis_id,
            created_at=_time_text(now),
            expires_at=_time_text(now + timedelta(seconds=self.config.signal_ttl_seconds)),
            relative_support_snapshot=None if record is None else record.confidence.relative_support,
            family_summary=self._summaries(record, evidence),
            reason_code=reason,
            l3c_snapshot_hash=snapshot.snapshot_hash,
            data_quality_hash=quality.snapshot_hash,
            source_state_hash=source_state_hash,
        )

    def _remember_signaled(self, hypothesis_id: str) -> None:
        if hypothesis_id in self._signaled_ids:
            return
        self._signaled_ids.add(hypothesis_id)
        self._signaled_order.append(hypothesis_id)
        while len(self._signaled_order) > self.config.hypothesis_history_limit:
            self._signaled_ids.remove(self._signaled_order.popleft())

    def _record_result(self, decision: SignalDecision) -> None:
        if decision.decision is SignalDecisionType.NO_TRADE:
            self._no_trade += 1
            self._blocked[decision.reason_code.value] = self._blocked.get(decision.reason_code.value, 0) + 1
        elif decision.decision is SignalDecisionType.LONG:
            self._long += 1
        elif decision.decision is SignalDecisionType.SHORT:
            self._short += 1
        else:
            self._exit += 1


@dataclass(frozen=True)
class TraderReplayReport:
    evaluations: int
    decisions: tuple[SignalDecision, ...]
    decision_sequence_hash: str
    metrics: TraderV0Metrics


class DeterministicTraderReplay:
    """Replay uses the normal Trader V0 evaluation path and supplied event time."""

    def __init__(self, trader: TraderV0) -> None:
        if type(trader) is not TraderV0:
            raise ValueError("Trader replay requires exact TraderV0.")
        self.trader = trader

    def replay(self, inputs: Iterable[tuple[HypothesisEngineSnapshot, TraderDataQuality]]) -> TraderReplayReport:
        decisions = tuple(self.trader.evaluate(snapshot, quality) for snapshot, quality in inputs)
        return TraderReplayReport(
            len(decisions), decisions,
            canonical_hash({"schema": "lane-iii-phase-d-decision-sequence-v1", "decision_hashes": [value.decision_hash for value in decisions]}),
            self.trader.metrics(),
        )
