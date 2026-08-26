"""Independent Sim101 paper risk authority and reconciliation gates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import threading

from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import (
    ACCOUNT_BINDING,
    POLICY,
    RISK_PROFILE,
    ExecutionAccountBinding,
    PaperDecision,
    PaperDecisionKind,
    PaperDirection,
    PaperExecutionIntent,
    PaperRiskGrant,
    PaperRiskProfile,
    deterministic_id,
    expires_at,
)
from .time_rules import america_new_york
from .sessions import (
    PaperCalendarState,
    PaperSessionKind,
    PaperSessionResolver,
    UNSPECIFIED_OFF_SESSION_CONTEXT,
    context_from_identity,
)


@dataclass(frozen=True)
class PaperRiskSnapshot:
    observed_at: str
    account_name: str = "Sim101"
    account_class: str = "LOCAL_SIMULATION"
    instrument: str = "MNQ SEP26"
    account_match_count: int = 1
    instrument_match_count: int = 1
    tick_size: Decimal = Decimal("0.25")
    canonical_contract: str = "MNQU6"
    current_position: PaperDirection = PaperDirection.FLAT
    current_position_quantity: int = 0
    working_owned_orders: int = 0
    working_entry_orders: int = 0
    foreign_activity: bool = False
    position_snapshot_complete: bool = False
    order_snapshot_complete: bool = False
    reconciliation_current: bool = False
    local_bridge_healthy: bool = False
    market_price_connected: bool = False
    execution_bridge_healthy: bool = False
    evidence_warmed: bool = False
    local_sequence_gap: bool = False
    depth_reset_recovery: bool = True
    quote_observed_at: str | None = None
    classified_trade_observed_at: str | None = None
    depth_mutation_observed_at: str | None = None
    daily_realized_pnl: Decimal = Decimal("0")
    daily_unrealized_pnl: Decimal = Decimal("0")
    account_reported_pnl: Decimal | None = None
    session_entry_count: int = 0
    consecutive_losses: int = 0
    position_opened_at: str | None = None
    protective_stop_state: str = "NONE"
    unresolved_command: bool = False
    unresolved_native_order: bool = False
    unresolved_execution: bool = False
    locked_out: bool = False
    lockout_reason: str | None = None
    session_kind: PaperSessionKind = UNSPECIFIED_OFF_SESSION_CONTEXT.session_kind
    session_id: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_id
    trade_date: str = UNSPECIFIED_OFF_SESSION_CONTEXT.trade_date
    session_profile_hash: str = UNSPECIFIED_OFF_SESSION_CONTEXT.session_profile_hash
    session_generation: int = UNSPECIFIED_OFF_SESSION_CONTEXT.session_generation
    trade_date_entry_count: int = 0

    def __post_init__(self) -> None:
        normalized_utc(self.observed_at, "Paper risk snapshot time")
        for value in (self.account_match_count, self.instrument_match_count, self.current_position_quantity, self.working_owned_orders, self.working_entry_orders, self.session_entry_count, self.consecutive_losses, self.trade_date_entry_count):
            if type(value) is not int or value < 0:
                raise ValueError("Paper risk counters must be non-negative integers.")
        for value in (self.quote_observed_at, self.classified_trade_observed_at, self.depth_mutation_observed_at, self.position_opened_at):
            if value is not None:
                normalized_utc(value, "Paper risk source time")
        context_from_identity(self.session_kind, self.session_id, self.trade_date, self.session_profile_hash, self.session_generation)


class PaperRiskAuthority:
    """The only object permitted to create positive paper risk grants."""

    def __init__(
        self,
        profile: PaperRiskProfile = RISK_PROFILE,
        binding: ExecutionAccountBinding = ACCOUNT_BINDING,
    ) -> None:
        if type(profile) is not PaperRiskProfile or type(binding) is not ExecutionAccountBinding:
            raise ValueError("Paper risk authority requires exact immutable inputs.")
        self.profile = profile
        self.binding = binding
        self._lock = threading.RLock()
        self._locked_out = False
        self._lockout_reason: str | None = None
        self._last_result: PaperRiskGrant | None = None
        self._arm_attempts = 0
        self._arm_denials = 0
        self._risk_grants = 0
        self._risk_denials = 0

    @staticmethod
    def _time(value: str) -> datetime:
        return datetime.fromisoformat(normalized_utc(value, "Paper risk time").replace("Z", "+00:00"))

    @staticmethod
    def _legacy_session_identity(kind: PaperSessionKind, session_id: str) -> bool:
        return kind is PaperSessionKind.OFF_SESSION and session_id == UNSPECIFIED_OFF_SESSION_CONTEXT.session_id

    def _context(self, snapshot: PaperRiskSnapshot, at: str):
        if self._legacy_session_identity(snapshot.session_kind, snapshot.session_id):
            return PaperSessionResolver().resolve(at, generation=snapshot.session_generation).context
        return context_from_identity(
            snapshot.session_kind, snapshot.session_id, snapshot.trade_date,
            snapshot.session_profile_hash, snapshot.session_generation,
        )

    def _inside_entry_session(self, at: datetime, snapshot: PaperRiskSnapshot | None = None) -> bool:
        if snapshot is None:
            return PaperSessionResolver().resolve(at).entry_authorized
        return self._context(snapshot, at.isoformat()).entry_permitted_at(at)

    def hard_flat_due(self, at: str) -> bool:
        resolution = PaperSessionResolver().resolve(at)
        return resolution.context.hard_flat_due_at(self._time(at))

    def maximum_age_due(self, snapshot: PaperRiskSnapshot, at: str) -> bool:
        return snapshot.position_opened_at is not None and self._time(at) - self._time(snapshot.position_opened_at) >= timedelta(seconds=self.profile.maximum_position_age_seconds)

    def _identity_reasons(self, snapshot: PaperRiskSnapshot) -> list[str]:
        reasons: list[str] = []
        if snapshot.account_name != self.binding.account_name or snapshot.account_class != self.binding.account_class or snapshot.account_match_count != 1:
            reasons.append("EXACT_SIM101_BINDING_REQUIRED")
        if snapshot.instrument != self.binding.instrument or snapshot.canonical_contract != self.binding.canonical_contract or snapshot.tick_size != self.profile.tick_size or snapshot.instrument_match_count != 1:
            reasons.append("EXACT_MNQ_SEP26_BINDING_REQUIRED")
        if snapshot.foreign_activity:
            reasons.append("FOREIGN_ACTIVITY_LOCKOUT")
        return reasons

    @staticmethod
    def _age_reason(at: datetime, observed_at: str | None, maximum_seconds: int, reason: str) -> str | None:
        if observed_at is None:
            return reason
        source = PaperRiskAuthority._time(observed_at)
        age = at - source
        return reason if age < timedelta(0) or age > timedelta(seconds=maximum_seconds) else None

    def preflight(self, snapshot: PaperRiskSnapshot, *, at: str) -> tuple[bool, tuple[str, ...]]:
        if type(snapshot) is not PaperRiskSnapshot:
            raise ValueError("Preflight requires an exact paper risk snapshot.")
        with self._lock:
            self._arm_attempts += 1
            moment = self._time(at)
            reasons = self._identity_reasons(snapshot)
            if snapshot.current_position is not PaperDirection.FLAT or snapshot.current_position_quantity != 0:
                reasons.append("SIM101_MNQ_NOT_FLAT")
            if snapshot.working_owned_orders or snapshot.working_entry_orders:
                reasons.append("WORKING_ORDERS_PRESENT")
            if not snapshot.position_snapshot_complete or not snapshot.order_snapshot_complete or not snapshot.reconciliation_current:
                reasons.append("RECONCILIATION_INCOMPLETE")
            if snapshot.unresolved_command or snapshot.unresolved_native_order or snapshot.unresolved_execution:
                reasons.append("UNRESOLVED_EXECUTION_TRUTH")
            if not snapshot.local_bridge_healthy or not snapshot.market_price_connected:
                reasons.append("MARKET_OBSERVER_UNHEALTHY")
            if not snapshot.execution_bridge_healthy:
                reasons.append("EXECUTION_BRIDGE_UNHEALTHY")
            if not snapshot.evidence_warmed:
                reasons.append("PAPER_EVIDENCE_NOT_WARMED")
            if snapshot.local_sequence_gap or snapshot.depth_reset_recovery:
                reasons.append("PAPER_CONTINUITY_UNUSABLE")
            context = self._context(snapshot, at)
            if context.session_kind is PaperSessionKind.OFF_SESSION:
                reasons.append("OFF_SESSION")
            elif context.calendar_state is PaperCalendarState.HOLIDAY_OVERRIDE_REQUIRED:
                reasons.append("HOLIDAY_SESSION_UNVERIFIED")
            elif not self._inside_entry_session(moment, snapshot):
                reasons.append("OUTSIDE_ENTRY_SESSION")
            if self._locked_out or snapshot.locked_out:
                reasons.append(self._lockout_reason or snapshot.lockout_reason or "SESSION_LOCKED_OUT")
            if snapshot.daily_realized_pnl + snapshot.daily_unrealized_pnl <= -self.profile.daily_loss_limit_dollars:
                reasons.append("DAILY_LOSS_LIMIT")
            if snapshot.session_entry_count >= self.profile.maximum_session_entries:
                reasons.append("SESSION_ENTRY_CAP")
            if snapshot.trade_date_entry_count >= self.profile.maximum_session_entries:
                reasons.append("TRADE_DATE_ENTRY_CAP")
            if snapshot.consecutive_losses >= self.profile.maximum_consecutive_losses:
                reasons.append("CONSECUTIVE_LOSS_LOCKOUT")
            if reasons:
                self._arm_denials += 1
            return not reasons, tuple(dict.fromkeys(reasons))

    def make_intent(
        self,
        decision: PaperDecision,
        *,
        reference_bid: Decimal | None,
        reference_ask: Decimal | None,
        reference_last: Decimal | None,
    ) -> PaperExecutionIntent:
        if type(decision) is not PaperDecision or decision.decision is PaperDecisionKind.NO_TRADE:
            raise ValueError("NO_TRADE cannot become an execution intent.")
        target = {
            PaperDecisionKind.LONG: PaperDirection.LONG,
            PaperDecisionKind.SHORT: PaperDirection.SHORT,
            PaperDecisionKind.EXIT: PaperDirection.FLAT,
        }[decision.decision]
        payload = {
            "decision_id": decision.paper_decision_id,
            "target_position": target.value,
            "quantity": 1,
            "instrument": self.binding.instrument,
            "created_at": decision.created_at,
            "expires_at": decision.expires_at,
            "policy_hash": decision.paper_policy_hash,
            "reference_bid": None if reference_bid is None else str(reference_bid),
            "reference_ask": None if reference_ask is None else str(reference_ask),
            "reference_last": None if reference_last is None else str(reference_last),
            "session_kind": decision.session_kind.value,
            "session_id": decision.session_id,
            "trade_date": decision.trade_date,
            "session_profile_hash": decision.session_profile_hash,
            "session_generation": decision.session_generation,
            "commissioning": decision.commissioning,
            "strategy_generated": decision.strategy_generated,
            "scientific_evidence": decision.scientific_evidence,
        }
        return PaperExecutionIntent(
            deterministic_id("l3g-pi-", payload), decision.paper_decision_id, target, 1,
            self.binding.instrument, decision.created_at, decision.expires_at, decision.paper_policy_hash,
            reference_bid, reference_ask, reference_last, decision.session_kind, decision.session_id,
            decision.trade_date, decision.session_profile_hash, decision.session_generation,
            decision.commissioning, decision.strategy_generated, decision.scientific_evidence,
        )

    def evaluate(self, intent: PaperExecutionIntent, snapshot: PaperRiskSnapshot, *, at: str) -> PaperRiskGrant:
        if type(intent) is not PaperExecutionIntent or type(snapshot) is not PaperRiskSnapshot:
            raise ValueError("Risk evaluation requires exact Lane III-G contracts.")
        with self._lock:
            moment = self._time(at)
            reasons = self._identity_reasons(snapshot)
            context = self._context(snapshot, at)
            if intent.instrument != self.binding.instrument or intent.requested_quantity != 1 or intent.policy_hash != POLICY.configuration_hash:
                reasons.append("INTENT_AUTHORITY_MISMATCH")
            if self._time(intent.expires_at) < moment:
                reasons.append("INTENT_EXPIRED")
            if snapshot.unresolved_command or snapshot.unresolved_native_order or snapshot.unresolved_execution:
                reasons.append("UNRESOLVED_EXECUTION_TRUTH")

            if intent.target_position is PaperDirection.FLAT:
                # Exits remain available while data is stale or entries are
                # paused, after a risk lockout, and outside every entry
                # window. Exact account truth and execution connectivity are
                # still mandatory.
                reasons = [reason for reason in reasons if reason != "FOREIGN_ACTIVITY_LOCKOUT"]
                if not snapshot.execution_bridge_healthy:
                    reasons.append("EXECUTION_BRIDGE_UNHEALTHY")
                if not snapshot.reconciliation_current:
                    reasons.append("RECONCILIATION_INCOMPLETE")
            else:
                if self._locked_out or snapshot.locked_out:
                    reasons.append(self._lockout_reason or snapshot.lockout_reason or "SESSION_LOCKED_OUT")
                pnl = snapshot.daily_realized_pnl + snapshot.daily_unrealized_pnl
                if pnl <= -self.profile.daily_loss_limit_dollars:
                    reasons.append("DAILY_LOSS_LIMIT")
                    self.lock_out("DAILY_LOSS_LIMIT")
                if snapshot.session_entry_count >= self.profile.maximum_session_entries:
                    reasons.append("SESSION_ENTRY_CAP")
                if snapshot.trade_date_entry_count >= self.profile.maximum_session_entries:
                    reasons.append("TRADE_DATE_ENTRY_CAP")
                if snapshot.consecutive_losses >= self.profile.maximum_consecutive_losses:
                    reasons.append("CONSECUTIVE_LOSS_LOCKOUT")
                if snapshot.current_position is not PaperDirection.FLAT or snapshot.current_position_quantity != 0:
                    reasons.append("PYRAMIDING_OR_REVERSAL_DENIED")
                if snapshot.working_entry_orders >= self.profile.maximum_pending_entries or snapshot.working_owned_orders:
                    reasons.append("PENDING_ORDER_LOCKOUT")
                intent_legacy = self._legacy_session_identity(intent.session_kind, intent.session_id)
                if not intent_legacy and (
                    intent.session_kind is not context.session_kind or intent.session_id != context.session_id
                    or intent.trade_date != context.trade_date or intent.session_profile_hash != context.session_profile_hash
                    or intent.session_generation != context.session_generation
                ):
                    reasons.append("SESSION_IDENTITY_MISMATCH")
                if context.session_kind is PaperSessionKind.OFF_SESSION:
                    reasons.append("OFF_SESSION")
                elif context.calendar_state is PaperCalendarState.HOLIDAY_OVERRIDE_REQUIRED:
                    reasons.append("HOLIDAY_SESSION_UNVERIFIED")
                elif not self._inside_entry_session(moment, snapshot):
                    reasons.append("OUTSIDE_ENTRY_SESSION")
                if context.hard_flat_due_at(moment):
                    reasons.append("HARD_FLAT_DEADLINE")
                if not snapshot.local_bridge_healthy or not snapshot.market_price_connected:
                    reasons.append("MARKET_BRIDGE_UNHEALTHY")
                if not snapshot.execution_bridge_healthy:
                    reasons.append("EXECUTION_BRIDGE_UNHEALTHY")
                if not snapshot.reconciliation_current or not snapshot.position_snapshot_complete or not snapshot.order_snapshot_complete:
                    reasons.append("RECONCILIATION_INCOMPLETE")
                if snapshot.local_sequence_gap:
                    reasons.append("LOCAL_SEQUENCE_GAP")
                if snapshot.depth_reset_recovery:
                    reasons.append("DEPTH_RESET_RECOVERY")
                if not snapshot.evidence_warmed:
                    reasons.append("PAPER_EVIDENCE_NOT_WARMED")
                freshness = (
                    self._age_reason(moment, snapshot.quote_observed_at, self.profile.quote_maximum_age_seconds, "QUOTE_STALE"),
                    self._age_reason(moment, snapshot.classified_trade_observed_at, self.profile.classified_trade_maximum_age_seconds, "CLASSIFIED_TRADE_STALE"),
                    self._age_reason(moment, snapshot.depth_mutation_observed_at, self.profile.depth_mutation_maximum_age_seconds, "DEPTH_MUTATION_STALE"),
                )
                reasons.extend(reason for reason in freshness if reason is not None)
            granted = not reasons
            reason_codes = ("PAPER_RISK_GRANTED",) if granted else tuple(dict.fromkeys(reasons))
            payload = {
                "intent_id": intent.intent_id,
                "risk_profile_hash": self.profile.configuration_hash,
                "account_binding_hash": self.binding.binding_hash,
                "granted": granted,
                "reason_codes": reason_codes,
                "evaluated_at": normalized_utc(at, "Risk evaluation time"),
                "snapshot_hash": canonical_hash({key: str(value) for key, value in snapshot.__dict__.items()}),
                "session_kind": context.session_kind.value,
                "session_id": context.session_id,
                "trade_date": context.trade_date,
                "session_profile_hash": context.session_profile_hash,
                "session_generation": context.session_generation,
                "commissioning": intent.commissioning,
                "strategy_generated": intent.strategy_generated,
                "scientific_evidence": intent.scientific_evidence,
            }
            grant = PaperRiskGrant(
                deterministic_id("l3g-pg-", payload), intent.intent_id, self.profile.configuration_hash,
                self.binding.binding_hash, granted, reason_codes, str(payload["evaluated_at"]), expires_at(str(payload["evaluated_at"]), 5),
                snapshot.current_position, snapshot.working_owned_orders, snapshot.daily_realized_pnl,
                snapshot.daily_unrealized_pnl, snapshot.session_entry_count, snapshot.consecutive_losses,
                session_kind=context.session_kind, session_id=context.session_id,
                trade_date=context.trade_date, session_profile_hash=context.session_profile_hash,
                session_generation=context.session_generation,
                commissioning=intent.commissioning, strategy_generated=intent.strategy_generated,
                scientific_evidence=intent.scientific_evidence,
            )
            self._last_result = grant
            if granted:
                self._risk_grants += 1
            else:
                self._risk_denials += 1
            return grant

    def protective_stop_price(self, direction: PaperDirection, actual_fill_price: Decimal) -> Decimal:
        if direction not in {PaperDirection.LONG, PaperDirection.SHORT} or actual_fill_price <= 0:
            raise ValueError("Protective stop requires a filled long or short position.")
        raw = actual_fill_price - self.profile.protective_stop_distance_points if direction is PaperDirection.LONG else actual_fill_price + self.profile.protective_stop_distance_points
        ticks = raw / self.profile.tick_size
        return ticks.to_integral_value() * self.profile.tick_size

    def slippage_points(self, direction: PaperDirection, intent: PaperExecutionIntent, actual_fill_price: Decimal) -> Decimal:
        reference = intent.reference_ask if direction is PaperDirection.LONG else intent.reference_bid
        if reference is None:
            return Decimal("Infinity")
        return actual_fill_price - reference if direction is PaperDirection.LONG else reference - actual_fill_price

    def enforce_fill(self, direction: PaperDirection, intent: PaperExecutionIntent, actual_fill_price: Decimal) -> tuple[bool, str]:
        slippage = self.slippage_points(direction, intent, actual_fill_price)
        if not slippage.is_finite() or slippage > self.profile.maximum_entry_slippage_points:
            self.lock_out("ENTRY_SLIPPAGE_LIMIT")
            return False, "ENTRY_SLIPPAGE_LIMIT"
        return True, "PROTECTIVE_STOP_REQUIRED"

    def lock_out(self, reason: str) -> None:
        with self._lock:
            self._locked_out = True
            self._lockout_reason = reason

    def clear_for_new_session(self) -> None:
        with self._lock:
            self._locked_out = False
            self._lockout_reason = None

    def status(self) -> dict[str, object]:
        with self._lock:
            return {
                "schema": "lane-iii-phase-g-paper-risk-status-v1",
                "risk_profile_hash": self.profile.configuration_hash,
                "account_binding_hash": self.binding.binding_hash,
                "paper_only": True,
                "approved_for_live": False,
                "locked_out": self._locked_out,
                "lockout_reason": self._lockout_reason,
                "arm_attempts": self._arm_attempts,
                "arm_denials": self._arm_denials,
                "risk_grants": self._risk_grants,
                "risk_denials": self._risk_denials,
                "last_risk_result": None if self._last_result is None else self._last_result.payload(),
            }
