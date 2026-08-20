"""Fail-closed future-futures admission semantics for Lane III.

The kernel is intentionally not an execution engine.  It validates a bounded
future request against external hard limits and durable safety state, records
the result, and stops there.  A later, separately commissioned futures service
would need to own broker transport, order lifecycle, and reconciliation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from .contracts import (
    ConfidenceLifecycle,
    ExecutionIntent,
    FuturesRiskConstitution,
    IntentPurpose,
    KnowledgeState,
    LaneIIIStrategyArtifact,
    LaneIIIRefused,
    PositionKnowledge,
    SafetySnapshot,
    TargetExposure,
    WorkingOrderKnowledge,
    canonical_hash,
    normalized_utc,
    require_l3a_manifest,
)
from .persistence import LaneIIISafetyStore, PersistedIntent


class AdmissionRefused(LaneIIIRefused):
    """A future request did not pass L3-A's contract/safety review."""


@dataclass(frozen=True)
class StrategyAdmission:
    """An externally commissioned artifact reference, not execution authority."""

    strategy_identity: str
    strategy_artifact_hash: str

    @classmethod
    def from_artifact(cls, artifact: LaneIIIStrategyArtifact) -> "StrategyAdmission":
        if type(artifact) is not LaneIIIStrategyArtifact:
            raise ValueError("Exact LaneIIIStrategyArtifact required.")
        return cls(artifact.strategy_identity, artifact.strategy_artifact_hash)


@dataclass(frozen=True)
class StrategyAdmissionRegistry:
    admissions: tuple[StrategyAdmission, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.admissions, tuple) or any(type(item) is not StrategyAdmission for item in self.admissions):
            raise ValueError("Strategy admissions must be an immutable tuple of exact records.")
        identities = [item.strategy_identity for item in self.admissions]
        if len(identities) != len(set(identities)):
            raise ValueError("Strategy admissions may not duplicate a strategy identity.")

    def find(self, strategy_identity: str) -> StrategyAdmission | None:
        return next((item for item in self.admissions if item.strategy_identity == strategy_identity), None)


@dataclass(frozen=True)
class IntentAdmissionDecision:
    """A durable L3-A decision; `eligible` is never order-submission authority."""

    intent_id: str
    eligible_for_sovereign_review: bool
    reason_code: str
    evaluated_at: str
    replayed: bool = False

    @property
    def execution_authority(self) -> bool:
        return False


class LaneIIIFuturesAdmissionKernel:
    """Narrow hard-risk and operator gate for contract review only."""

    def __init__(
        self,
        store: LaneIIISafetyStore,
        risk_constitution: FuturesRiskConstitution,
        registry: StrategyAdmissionRegistry,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if type(store) is not LaneIIISafetyStore or type(risk_constitution) is not FuturesRiskConstitution:
            raise ValueError("L3-A admission kernel requires exact store and external risk constitution.")
        if type(registry) is not StrategyAdmissionRegistry:
            raise ValueError("L3-A admission kernel requires an immutable strategy-admission registry.")
        self.store = store
        self.risk_constitution = risk_constitution
        self.registry = registry
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.store.initialize()

    def review(self, intent: object, snapshot: object) -> IntentAdmissionDecision:
        """Review exactly one immutable request and record it without transport."""
        now = normalized_utc(self.clock().isoformat(), "Admission evaluation time")
        if type(intent) is not ExecutionIntent:
            raise AdmissionRefused("exact_execution_intent_required")
        if type(snapshot) is not SafetySnapshot:
            raise AdmissionRefused("exact_authoritative_safety_snapshot_required")
        require_l3a_manifest()
        existing = self._existing(intent)
        if existing is not None:
            return IntentAdmissionDecision(
                intent_id=intent.intent_id,
                eligible_for_sovereign_review=existing.disposition == "ELIGIBLE_FOR_SOVEREIGN_REVIEW",
                reason_code=existing.reason_code,
                evaluated_at=existing.recorded_at,
                replayed=True,
            )
        decision = self._evaluate_new(intent, snapshot, now)
        self.store.record_safety_snapshot(snapshot)
        persisted = self.store.record_intent(
            intent,
            disposition="ELIGIBLE_FOR_SOVEREIGN_REVIEW" if decision.eligible_for_sovereign_review else "REFUSED",
            reason_code=decision.reason_code,
            recorded_at=decision.evaluated_at,
        )
        return IntentAdmissionDecision(
            intent_id=persisted.intent_id,
            eligible_for_sovereign_review=persisted.disposition == "ELIGIBLE_FOR_SOVEREIGN_REVIEW",
            reason_code=persisted.reason_code,
            evaluated_at=persisted.recorded_at,
        )

    def _existing(self, intent: ExecutionIntent) -> PersistedIntent | None:
        for item in self.store.unresolved_intents():
            if item.intent_id == intent.intent_id:
                if item.intent_hash != intent.intent_hash:
                    raise AdmissionRefused("duplicate_intent_id_conflicting_payload")
                return item
        return None

    def _evaluate_new(self, intent: ExecutionIntent, snapshot: SafetySnapshot, now: str) -> IntentAdmissionDecision:
        def denied(reason: str) -> IntentAdmissionDecision:
            return IntentAdmissionDecision(intent.intent_id, False, reason, now)

        admission = self.registry.find(intent.strategy_identity)
        if admission is None:
            return denied("unauthorized_strategy_identity")
        if admission.strategy_artifact_hash != intent.strategy_artifact_hash:
            return denied("strategy_artifact_hash_mismatch")
        if intent.instrument != self.risk_constitution.admitted_execution_instrument:
            return denied("execution_expiry_not_admitted")
        if normalized_utc(intent.created_at, "Intent creation time") > now or normalized_utc(intent.expires_at, "Intent expiry time") <= now:
            return denied("intent_expired_or_not_yet_valid")
        operator = self.store.operator_state()
        if operator.flatten_latched:
            return denied("operator_flatten_latched")
        if not operator.armed:
            return denied("operator_disarmed")
        if snapshot.broker_state is not KnowledgeState.FRESH:
            return denied("stale_or_unknown_broker_state")
        if snapshot.market_data is not KnowledgeState.FRESH:
            return denied("stale_or_unknown_market_data")
        if snapshot.position_state is PositionKnowledge.UNKNOWN:
            return denied("unknown_position_not_zero")
        if snapshot.working_orders is WorkingOrderKnowledge.UNKNOWN:
            return denied("unknown_order_not_cancelled")
        if intent.purpose is IntentPurpose.ENTRY:
            if operator.paused_new_entries:
                return denied("operator_pause_new_entries")
            if snapshot.working_orders is not WorkingOrderKnowledge.CLEAR:
                return denied("working_orders_present")
            if snapshot.session_risk.daily_loss >= self.risk_constitution.maximum_daily_loss:
                return denied("maximum_daily_loss_reached")
            if snapshot.session_risk.session_loss >= self.risk_constitution.maximum_session_loss:
                return denied("maximum_session_loss_reached")
            if intent.quantity > self.risk_constitution.maximum_individual_order_quantity:
                return denied("maximum_individual_order_quantity_exceeded")
            if snapshot.position_state is not PositionKnowledge.FLAT:
                return denied("one_position_semantics_existing_position")
            if intent.quantity > self.risk_constitution.maximum_mnq_exposure:
                return denied("maximum_mnq_exposure_exceeded")
            confidence = self.store.confidence_state()
            if confidence is None or confidence.get("snapshot_hash") != intent.confidence_snapshot_hash:
                return denied("confidence_snapshot_unavailable_or_mismatched")
            payload = confidence.get("payload")
            if not isinstance(payload, dict) or payload.get("lifecycle") in {
                ConfidenceLifecycle.CONTRADICTED.value, ConfidenceLifecycle.EXPIRED.value,
            }:
                return denied("confidence_not_eligible_for_future_entry_review")
            hypothesis = self.store.active_hypothesis()
            if hypothesis is None or hypothesis.get("hypothesis_id") != intent.hypothesis_id:
                return denied("active_hypothesis_unavailable_or_mismatched")
            if hypothesis.get("strategy_artifact_hash") != intent.strategy_artifact_hash:
                return denied("hypothesis_artifact_hash_mismatch")
            if canonical_hash(hypothesis) != intent.evidence_state_hash:
                return denied("evidence_state_unavailable_or_mismatched")
        else:
            if snapshot.position_state is PositionKnowledge.FLAT:
                return denied("strategy_exit_requires_known_nonflat_position")
            if intent.quantity > abs(snapshot.position_quantity or 0):
                return denied("strategy_exit_quantity_exceeds_known_position")
        return IntentAdmissionDecision(intent.intent_id, True, "contract_and_safety_review_passed", now)
