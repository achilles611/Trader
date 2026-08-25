"""Deterministic provisional policy for experimental Sim101 decisions.

The policy consumes only already-admitted NinjaTrader callbacks.  It uses the
local callback order as a continuity check, never as provider sequencing, and
clears every provisional window at any uncertain boundary.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import threading
from typing import Iterable, Mapping

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.l3f_provider.tradovate_observation import StreamHealth
from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import (
    POLICY,
    BookCompleteness,
    EvidenceFamily,
    HypothesisKind,
    PaperDecision,
    PaperDecisionKind,
    PaperDirection,
    PaperEvidence,
    PaperPolicyArtifact,
    PaperSourceQuality,
    SequenceAuthority,
    deterministic_id,
    expires_at,
)
from .time_rules import america_new_york
from .sessions import (
    PaperSessionContext,
    PaperSessionKind,
    PaperSessionResolver,
    UNSPECIFIED_OFF_SESSION_CONTEXT,
)


MAXIMUM_PROVIDER_FUTURE_SKEW = timedelta(milliseconds=1250)


@dataclass(frozen=True)
class ClassifiedTrade:
    observation_id: str
    local_sequence: int
    payload_hash: str
    observed_at: str
    price: Decimal
    size: int
    side: str | None
    session_id: str


@dataclass(frozen=True)
class QuoteContext:
    observation_id: str
    local_sequence: int
    payload_hash: str
    observed_at: str
    bid: Decimal
    ask: Decimal
    session_id: str


@dataclass(frozen=True)
class DepthMutation:
    observation_id: str
    local_sequence: int
    payload_hash: str
    observed_at: str
    observed_time: datetime
    side: str
    operation: str
    price: Decimal
    volume: int
    prior_volume: int | None
    is_reduction: bool
    is_increase: bool
    session_id: str


class ExperimentalPaperPolicy:
    """One deterministic, synchronous, paper-direction-only consumer."""

    def __init__(self, artifact: PaperPolicyArtifact = POLICY) -> None:
        if type(artifact) is not PaperPolicyArtifact:
            raise ValueError("Paper policy requires the exact immutable artifact type.")
        self.artifact = artifact
        self._policy_hash = artifact.configuration_hash
        self._lock = threading.RLock()
        self._market_session_id: str | None = None
        self._paper_session_context = UNSPECIFIED_OFF_SESSION_CONTEXT
        self._session_resolver = PaperSessionResolver()
        self._last_local_sequence: int | None = None
        self._last_receipt_time: datetime | None = None
        self._transport_state = StreamHealth.UNKNOWN
        self._price_connected = False
        self._depth_recovering = False
        self._quotes: dict[str, QuoteContext] = {}
        self._quote_order: deque[str] = deque(maxlen=32)
        self._trades: deque[ClassifiedTrade] = deque(maxlen=max(64, artifact.structural_window * 4))
        self._classified: deque[ClassifiedTrade] = deque(maxlen=max(64, artifact.classified_flow_window * 4))
        self._depth: deque[DepthMutation] = deque(maxlen=256)
        self._depth_by_price: dict[Decimal, deque[DepthMutation]] = {}
        self._bid_levels: dict[Decimal, int] = {}
        self._bid_depletions: dict[Decimal, int] = {}
        self._bid_replenishment_cycles: dict[Decimal, int] = {}
        self._evidence: dict[tuple[HypothesisKind, EvidenceFamily, str], PaperEvidence] = {}
        self._vwap_notional = Decimal("0")
        self._vwap_volume = 0
        self._vwap_session_date: str | None = None
        self._last_decision: PaperDecision | None = None
        self._last_input_fault: dict[str, object] | None = None
        self._used_hypothesis_instances: set[str] = set()
        self._last_flat_confirmation: datetime | None = None
        self._reset_count = 0
        self._counters: dict[str, int] = {
            "quotes": 0,
            "trades": 0,
            "classified_buy": 0,
            "classified_sell": 0,
            "unknown_aggressor": 0,
            "depth_mutations": 0,
            "resets": 0,
            "local_sequence_gaps": 0,
            "decisions_long": 0,
            "decisions_short": 0,
            "decisions_exit": 0,
            "decisions_no_trade": 0,
        }
        self._suppression_reasons: dict[str, int] = {}

    @staticmethod
    def _time(value: str, name: str = "Paper event time") -> datetime:
        return datetime.fromisoformat(normalized_utc(value, name).replace("Z", "+00:00"))

    @staticmethod
    def _decimal(value: object) -> Decimal | None:
        try:
            result = Decimal(str(value))
        except Exception:
            return None
        return result if result.is_finite() else None

    @staticmethod
    def _payload_hash(observation: NinjaTraderObservation) -> str:
        return canonical_hash(dict(observation.payload))

    @staticmethod
    def _contract_matches(observation: NinjaTraderObservation) -> bool:
        return observation.payload.get("contract_id") == "MNQ SEP26"

    def _clear_provisional(self) -> None:
        self._quotes.clear()
        self._quote_order.clear()
        self._trades.clear()
        self._classified.clear()
        self._depth.clear()
        self._depth_by_price.clear()
        self._bid_levels.clear()
        self._bid_depletions.clear()
        self._bid_replenishment_cycles.clear()
        self._evidence.clear()
        self._vwap_notional = Decimal("0")
        self._vwap_volume = 0
        self._vwap_session_date = None
        self._last_receipt_time = None
        self._depth_recovering = True
        self._reset_count += 1
        self._counters["resets"] += 1

    def reset(self, reason: str) -> None:
        with self._lock:
            self._clear_provisional()
            self._suppress(reason)

    @property
    def session_context(self) -> PaperSessionContext:
        with self._lock:
            return self._paper_session_context

    def _activate_session(self, context: PaperSessionContext) -> bool:
        prior = self._paper_session_context
        changed = (prior.session_id, prior.session_generation) != (context.session_id, context.session_generation)
        if changed:
            if prior.session_id != UNSPECIFIED_OFF_SESSION_CONTEXT.session_id:
                self._clear_provisional()
            self._used_hypothesis_instances.clear()
            self._last_flat_confirmation = None
            self._paper_session_context = context
        return changed

    def on_transport_state(self, state: StreamHealth) -> None:
        if type(state) is not StreamHealth:
            raise ValueError("Paper transport state must be explicit.")
        with self._lock:
            prior = self._transport_state
            self._transport_state = state
            if state is StreamHealth.DISCONNECTED or (prior is StreamHealth.DISCONNECTED and state is StreamHealth.HEALTHY):
                self._clear_provisional()
                self._suppress("LOCAL_BRIDGE_DISCONNECT" if state is StreamHealth.DISCONNECTED else "BRIDGE_RECONNECT")

    def on_rejection(self, _: object) -> None:
        self.reset("MALFORMED_FRAME")

    def on_duplicate(self) -> None:
        with self._lock:
            self._suppress("DUPLICATE_OBSERVATION")

    def mark_entry_used(self, decision: PaperDecision) -> None:
        if type(decision) is not PaperDecision or decision.decision not in {PaperDecisionKind.LONG, PaperDecisionKind.SHORT}:
            raise ValueError("Only an exact entry decision may be marked used.")
        with self._lock:
            if (decision.session_id, decision.session_generation) != (self._paper_session_context.session_id, self._paper_session_context.session_generation):
                raise ValueError("Paper hypothesis cannot cross a session boundary.")
            self._used_hypothesis_instances.add(self._hypothesis_instance(decision))

    def confirm_flat(self, at: str) -> None:
        with self._lock:
            self._last_flat_confirmation = self._time(at, "Flat confirmation time")

    def _suppress(self, reason: str) -> None:
        self._suppression_reasons[reason] = self._suppression_reasons.get(reason, 0) + 1

    def _continuity(self, observation: NinjaTraderObservation) -> str | None:
        # The observer assigns one sequence across every admitted frame, not
        # one sequence per market-data type. Account/health/snapshot frames
        # therefore advance continuity even though they do not create paper
        # evidence.
        if self._market_session_id is None:
            self._market_session_id = observation.session_id
            self._last_local_sequence = observation.local_monotonic_sequence
            return None
        if observation.session_id != self._market_session_id:
            self._market_session_id = observation.session_id
            self._last_local_sequence = observation.local_monotonic_sequence
            self._clear_provisional()
            return "OBSERVATION_SESSION_CHANGED"
        assert self._last_local_sequence is not None
        if observation.local_monotonic_sequence != self._last_local_sequence + 1:
            self._last_local_sequence = observation.local_monotonic_sequence
            self._counters["local_sequence_gaps"] += 1
            self._clear_provisional()
            return "LOCAL_SEQUENCE_GAP"
        self._last_local_sequence = observation.local_monotonic_sequence
        return None

    def _event_time(self, observation: NinjaTraderObservation) -> datetime:
        # Local callback order is this policy's declared temporal authority.
        # Ninja receipt time is created under the same queue lock as the local
        # sequence, so it safely bounds provisional windows without being
        # mislabeled as a provider timestamp.
        return self._time(self._market_event_timestamp(observation), "Paper market event time")

    @staticmethod
    def _market_event_timestamp(observation: NinjaTraderObservation) -> str:
        # Quote, trade, and depth provider clocks are independent and may
        # legitimately cross.  The AddOn emits ninja_receipt_time under the
        # same lock as local_monotonic_sequence, making it the only safe
        # temporal authority for this local-callback policy.  Provider and
        # exchange times remain durable source provenance and are separately
        # checked below for bounded freshness.
        return observation.ninja_receipt_time

    def _validate_time(self, observation: NinjaTraderObservation) -> str | None:
        receipt_time = self._time(self._market_event_timestamp(observation), "Paper market event time")
        if self._last_receipt_time is not None and receipt_time < self._last_receipt_time:
            self._clear_provisional()
            return "TIMESTAMP_MOVED_BACKWARD"
        if observation.provider_timestamp is not None:
            provider_time = self._time(observation.provider_timestamp, "Provider event time")
            receipt = self._time(observation.ninja_receipt_time, "NinjaTrader receipt time")
            if provider_time > receipt + MAXIMUM_PROVIDER_FUTURE_SKEW:
                self._clear_provisional()
                return "FUTURE_EVENT_TIMESTAMP"
            if receipt - provider_time > timedelta(seconds=self.artifact.hypothesis_idle_lifetime_seconds):
                self._clear_provisional()
                return "STALE_EVENT_TIMESTAMP"
        self._last_receipt_time = receipt_time
        return None

    def ingest(
        self,
        observation: NinjaTraderObservation,
        *,
        current_position: PaperDirection = PaperDirection.FLAT,
        pending_order: bool = False,
        session_context: PaperSessionContext | None = None,
    ) -> PaperDecision:
        decision = self._ingest(
            observation,
            current_position=current_position,
            pending_order=pending_order,
            evaluate_passive=True,
            session_context=session_context,
        )
        assert decision is not None
        return decision

    def ingest_runtime(
        self,
        observation: NinjaTraderObservation,
        *,
        current_position: PaperDirection = PaperDirection.FLAT,
        pending_order: bool = False,
        session_context: PaperSessionContext | None = None,
    ) -> PaperDecision | None:
        """Ingest every callback but evaluate only decision-relevant events."""
        return self._ingest(
            observation,
            current_position=current_position,
            pending_order=pending_order,
            evaluate_passive=False,
            session_context=session_context,
        )

    def _ingest(
        self,
        observation: NinjaTraderObservation,
        *,
        current_position: PaperDirection,
        pending_order: bool,
        evaluate_passive: bool,
        session_context: PaperSessionContext | None,
    ) -> PaperDecision | None:
        if type(observation) is not NinjaTraderObservation:
            raise ValueError("Paper policy consumes exact admitted NinjaTrader observations.")
        with self._lock:
            resolution = None if session_context is not None else self._session_resolver.resolve(
                self._market_event_timestamp(observation), generation=self._paper_session_context.session_generation,
            )
            if resolution is not None and resolution.reason_code == "EVENT_TIMESTAMP_MOVED_BACKWARD":
                if observation.observation_type in {"QUOTE", "TRADE", "DEPTH"}:
                    time_fault = self._validate_time(observation)
                    if time_fault is not None:
                        return self._decision(observation, PaperDecisionKind.NO_TRADE, None, time_fault)
                self._clear_provisional()
                return self._decision(observation, PaperDecisionKind.NO_TRADE, None, "TIMESTAMP_MOVED_BACKWARD")
            context = session_context or resolution.context  # type: ignore[union-attr]
            self._activate_session(context)
            fault = self._continuity(observation)
            if fault is not None:
                return self._decision(observation, PaperDecisionKind.NO_TRADE, None, fault)

            if observation.observation_type in {"QUOTE", "TRADE", "DEPTH"}:
                time_fault = self._validate_time(observation)
                if time_fault is not None:
                    return self._decision(observation, PaperDecisionKind.NO_TRADE, None, time_fault)

            if context.session_kind is PaperSessionKind.OFF_SESSION:
                return self._decision(observation, PaperDecisionKind.NO_TRADE, None, "OFF_SESSION")

            if observation.observation_type == "CONNECTION" and observation.payload.get("scope") == "MARKET_DATA":
                state = str(observation.payload.get("price_status", "UNKNOWN")).upper()
                self._price_connected = state == "CONNECTED"
                if not self._price_connected:
                    self._clear_provisional()
                    return self._decision(observation, PaperDecisionKind.NO_TRADE, None, "MARKET_DATA_DISCONNECTED")
                # A connection recovery creates a new provisional evidence domain.
                self._clear_provisional()
                return self._decision(observation, PaperDecisionKind.NO_TRADE, None, "MARKET_DATA_RECONNECTED")

            if observation.observation_type not in {"QUOTE", "TRADE", "DEPTH"}:
                return self._decision(observation, PaperDecisionKind.NO_TRADE, None, "NON_MARKET_OBSERVATION")
            if not self._contract_matches(observation):
                self._clear_provisional()
                return self._decision(observation, PaperDecisionKind.NO_TRADE, None, "CONTRACT_MISMATCH")
            evidence_before = len(self._evidence)
            if observation.observation_type == "QUOTE":
                reason = self._ingest_quote(observation)
            elif observation.observation_type == "TRADE":
                reason = self._ingest_trade(observation)
            else:
                reason = self._ingest_depth(observation)
            if reason is not None:
                return self._decision(observation, PaperDecisionKind.NO_TRADE, None, reason)
            # An admitted callback for the exact bound instrument is itself
            # truthful evidence that the price stream is delivering.  This is
            # needed after a local bridge restart because the NinjaTrader
            # observer's connection transition may predate the new socket.
            self._price_connected = True
            if not evaluate_passive and observation.observation_type == "QUOTE":
                return None
            if not evaluate_passive and observation.observation_type == "DEPTH" and len(self._evidence) == evidence_before:
                return None
            return self.evaluate(observation, current_position=current_position, pending_order=pending_order)

    def _ingest_quote(self, observation: NinjaTraderObservation) -> str | None:
        bid = self._decimal(observation.payload.get("bid"))
        ask = self._decimal(observation.payload.get("ask"))
        bid_size = observation.payload.get("bid_size")
        ask_size = observation.payload.get("ask_size")
        if bid is None or ask is None or bid <= 0 or ask <= 0 or bid >= ask or type(bid_size) is not int or type(ask_size) is not int or bid_size <= 0 or ask_size <= 0:
            self._clear_provisional()
            return "INVALID_OR_CROSSED_QUOTE"
        quote = QuoteContext(
            observation.observation_id, observation.local_monotonic_sequence, self._payload_hash(observation),
            self._market_event_timestamp(observation), bid, ask, self._paper_session_context.session_id,
        )
        self._quotes[quote.observation_id] = quote
        self._quote_order.append(quote.observation_id)
        while len(self._quotes) > self._quote_order.maxlen:
            keep = set(self._quote_order)
            for key in tuple(self._quotes):
                if key not in keep:
                    self._quotes.pop(key, None)
        self._counters["quotes"] += 1
        return None

    def _ingest_trade(self, observation: NinjaTraderObservation) -> str | None:
        price = self._decimal(observation.payload.get("price"))
        size = observation.payload.get("size")
        if price is None or price <= 0 or type(size) is not int or size <= 0:
            self._clear_provisional()
            return "INVALID_TRADE"
        observed_at = self._market_event_timestamp(observation)
        side: str | None = None
        quote_reference = observation.payload.get("derivation_quote_observation_id")
        quote = self._quotes.get(str(quote_reference)) if isinstance(quote_reference, str) else None
        bid_at_trade = self._decimal(observation.payload.get("bid_at_trade"))
        ask_at_trade = self._decimal(observation.payload.get("ask_at_trade"))
        source = observation.payload.get("aggressor_source")
        explicit = str(observation.payload.get("aggressor_side", "UNKNOWN")).upper()
        if explicit in {"BUY", "SELL"} and source == "PROVIDER_NATIVE":
            # Retained for truthful providers; the commissioned NinjaTrader
            # observer does not currently emit this provenance.
            side = explicit
        elif (
            source == "BID_ASK_CLASSIFICATION"
            and quote is not None
            and bid_at_trade == quote.bid
            and ask_at_trade == quote.ask
            and quote.bid < quote.ask
            and self._time(observed_at) >= self._time(quote.observed_at)
        ):
            if price >= quote.ask:
                side = "BUY"
            elif price <= quote.bid:
                side = "SELL"
        trade = ClassifiedTrade(
            observation.observation_id, observation.local_monotonic_sequence, self._payload_hash(observation),
            observed_at, price, size, side, self._paper_session_context.session_id,
        )
        self._trades.append(trade)
        self._counters["trades"] += 1
        if side is None:
            self._counters["unknown_aggressor"] += 1
        else:
            self._classified.append(trade)
            self._counters["classified_buy" if side == "BUY" else "classified_sell"] += 1
        self._update_vwap(trade)
        self._derive_structural(trade)
        self._derive_flow(trade)
        return None

    def _update_vwap(self, trade: ClassifiedTrade) -> None:
        local = america_new_york(self._time(trade.observed_at))
        session_date = local.date().isoformat()
        if self._vwap_session_date is not None and session_date != self._vwap_session_date:
            self._vwap_notional = Decimal("0")
            self._vwap_volume = 0
            # VWAP cannot cross a configured local-session boundary.
            self._evidence = {key: value for key, value in self._evidence.items() if value.family is not EvidenceFamily.STRUCTURAL_CONTEXT}
        self._vwap_session_date = session_date
        self._vwap_notional += trade.price * trade.size
        self._vwap_volume += trade.size

    def _provenance(self, values: Iterable[ClassifiedTrade | DepthMutation]) -> tuple[tuple[str, ...], tuple[int, ...], tuple[str, ...], tuple[str, ...]]:
        items = tuple(values)
        return (
            tuple(item.observation_id for item in items),
            tuple(item.local_sequence for item in items),
            tuple(item.payload_hash for item in items),
            tuple(item.session_id for item in items),
        )

    def _put_evidence(
        self,
        hypothesis: HypothesisKind,
        family: EvidenceFamily,
        label: str,
        strength: Decimal,
        supports: bool,
        observed_at: str,
        lifetime_seconds: int,
        sources: Iterable[ClassifiedTrade | DepthMutation],
        *,
        blocking: bool = False,
    ) -> None:
        ids, sequences, hashes, source_sessions = self._provenance(sources)
        if not source_sessions or set(source_sessions) != {self._paper_session_context.session_id}:
            raise ValueError("CROSS_SESSION_SOURCE_SET")
        identity = {
            "policy_hash": self._policy_hash,
            "hypothesis": hypothesis.value,
            "family": family.value,
            "label": label,
            "strength": str(strength),
            "supports": supports,
            "observed_at": normalized_utc(observed_at, "Evidence observed time"),
            "sources": ids,
            "sequences": sequences,
            "hashes": hashes,
            "session_kind": self._paper_session_context.session_kind.value,
            "session_id": self._paper_session_context.session_id,
            "trade_date": self._paper_session_context.trade_date,
            "session_profile_hash": self._paper_session_context.session_profile_hash,
            "session_generation": self._paper_session_context.session_generation,
        }
        evidence = PaperEvidence(
            deterministic_id("l3g-pe-", identity), hypothesis, family, label, strength, supports,
            normalized_utc(observed_at, "Evidence observed time"), expires_at(observed_at, lifetime_seconds),
            ids, sequences, hashes, blocking=blocking,
            session_kind=self._paper_session_context.session_kind,
            session_id=self._paper_session_context.session_id,
            trade_date=self._paper_session_context.trade_date,
            session_profile_hash=self._paper_session_context.session_profile_hash,
            session_generation=self._paper_session_context.session_generation,
            source_session_ids=source_sessions,
        )
        self._evidence[(hypothesis, family, label)] = evidence
        # A structural range claim is mutually exclusive for the two admitted
        # v0 hypotheses at the same instant. Preserve it as one contradiction
        # in the competing family; do not manufacture extra family votes.
        if family is EvidenceFamily.STRUCTURAL_CONTEXT and supports:
            competitor = HypothesisKind.BEARISH_CONTINUATION if hypothesis is HypothesisKind.BULLISH_REVERSAL else HypothesisKind.BULLISH_REVERSAL
            contrary_identity = {**identity, "hypothesis": competitor.value, "supports": False}
            contrary = PaperEvidence(
                deterministic_id("l3g-pe-", contrary_identity), competitor, family, label, strength, False,
                normalized_utc(observed_at, "Evidence observed time"), expires_at(observed_at, lifetime_seconds),
                ids, sequences, hashes,
                session_kind=self._paper_session_context.session_kind,
                session_id=self._paper_session_context.session_id,
                trade_date=self._paper_session_context.trade_date,
                session_profile_hash=self._paper_session_context.session_profile_hash,
                session_generation=self._paper_session_context.session_generation,
                source_session_ids=source_sessions,
            )
            self._evidence[(competitor, family, "CONTRADICTS_" + label)] = contrary

    def _derive_structural(self, current: ClassifiedTrade) -> None:
        window = tuple(self._trades)[-self.artifact.structural_window:]
        if len(window) < 3:
            return
        if self._time(current.observed_at) - self._time(window[0].observed_at) > timedelta(seconds=self.artifact.structural_evidence_lifetime_seconds):
            return
        prior = window[:-1]
        first = window[0].price
        previous_low = min(item.price for item in prior)
        previous_high = max(item.price for item in prior)
        if previous_low < first and current.price >= first:
            self._put_evidence(HypothesisKind.BULLISH_REVERSAL, EvidenceFamily.STRUCTURAL_CONTEXT, "RANGE_RECLAIM_UP", self.artifact.structural_strength, True, current.observed_at, self.artifact.structural_evidence_lifetime_seconds, window)
        if current.price < previous_low and previous_high >= first:
            self._put_evidence(HypothesisKind.BEARISH_CONTINUATION, EvidenceFamily.STRUCTURAL_CONTEXT, "RANGE_EXPANSION_DOWN", self.artifact.structural_strength, True, current.observed_at, self.artifact.structural_evidence_lifetime_seconds, window)
        if self._vwap_volume > 0 and current.price < self._vwap_notional / self._vwap_volume:
            self._put_evidence(HypothesisKind.BEARISH_CONTINUATION, EvidenceFamily.STRUCTURAL_CONTEXT, "BELOW_PROVISIONAL_SESSION_VWAP", self.artifact.structural_strength, True, current.observed_at, self.artifact.structural_evidence_lifetime_seconds, window)

    def _derive_flow(self, current: ClassifiedTrade) -> None:
        window = tuple(self._classified)[-self.artifact.classified_flow_window:]
        if len(window) < self.artifact.minimum_classified_trades:
            return
        if self._time(current.observed_at) - self._time(window[0].observed_at) > timedelta(seconds=self.artifact.flow_evidence_lifetime_seconds):
            return
        buy = sum(item.size for item in window if item.side == "BUY")
        sell = sum(item.size for item in window if item.side == "SELL")
        total = buy + sell
        strength = Decimal(abs(buy - sell)) / Decimal(total) if total else Decimal("0")
        if sell <= buy or strength <= 0:
            return
        if current.price >= window[0].price:
            self._put_evidence(HypothesisKind.BULLISH_REVERSAL, EvidenceFamily.ORDER_FLOW, "SELLING_WITHOUT_DOWNWARD_PROGRESS", strength, True, current.observed_at, self.artifact.flow_evidence_lifetime_seconds, window)
        self._put_evidence(HypothesisKind.BEARISH_CONTINUATION, EvidenceFamily.ORDER_FLOW, "AGGRESSIVE_SELL_IMBALANCE", strength, True, current.observed_at, self.artifact.flow_evidence_lifetime_seconds, window)

    def _ingest_depth(self, observation: NinjaTraderObservation) -> str | None:
        if observation.payload.get("is_reset") is True:
            self._clear_provisional()
            return "DEPTH_RESET"
        mutation_fields = {"mutation_price", "mutation_volume", "mutation_position", "is_reset"}
        if not any(name in observation.payload for name in mutation_fields):
            self._last_input_fault = {
                "reason": "LEGACY_DEPTH_SNAPSHOT_NO_MUTATION",
                "effect": "PAPER_EVIDENCE_RESET",
            }
            self._clear_provisional()
            return "LEGACY_DEPTH_SNAPSHOT_NO_MUTATION"
        side = str(observation.payload.get("side", "")).upper()
        operation = str(observation.payload.get("operation", "")).upper()
        price = self._decimal(observation.payload.get("mutation_price"))
        volume = observation.payload.get("mutation_volume")
        position = observation.payload.get("mutation_position")
        if side not in {"BID", "ASK"} or operation not in {"ADD", "INSERT", "UPDATE", "REMOVE"} or price is None or price <= 0 or type(volume) is not int or volume < 0 or type(position) is not int or position < 0:
            self._last_input_fault = {
                "reason": "MALFORMED_DEPTH_MUTATION", "side": side, "operation": operation,
                "price_valid": price is not None and price > 0, "volume_type": type(volume).__name__,
                "volume": volume, "position_type": type(position).__name__, "position": position,
            }
            self._clear_provisional()
            return "MALFORMED_DEPTH_MUTATION"
        if not isinstance(observation.payload.get("bids"), list) or not observation.payload.get("bids") or not isinstance(observation.payload.get("asks"), list) or not observation.payload.get("asks"):
            self._last_input_fault = {
                "reason": "PARTIAL_DEPTH", "bids_present": bool(observation.payload.get("bids")),
                "asks_present": bool(observation.payload.get("asks")),
            }
            self._clear_provisional()
            return "PARTIAL_DEPTH"
        prior = self._bid_levels.get(price) if side == "BID" else None
        reduction = side == "BID" and (operation == "REMOVE" or (prior is not None and volume < prior))
        increase = side == "BID" and operation != "REMOVE" and (prior is None or volume > prior)
        if side == "BID":
            if operation == "REMOVE":
                self._bid_levels.pop(price, None)
            else:
                self._bid_levels[price] = volume
        mutation_time = self._event_time(observation)
        mutation = DepthMutation(
            observation.observation_id, observation.local_monotonic_sequence, self._payload_hash(observation),
            self._market_event_timestamp(observation), mutation_time, side, operation, price, volume,
            prior, reduction, increase, self._paper_session_context.session_id,
        )
        self._depth.append(mutation)
        price_history: deque[DepthMutation] | None = None
        if side == "BID":
            price_history = self._depth_by_price.setdefault(price, deque(maxlen=256))
            price_history.append(mutation)
            cutoff = mutation_time - timedelta(seconds=self.artifact.liquidity_evidence_lifetime_seconds)
            while price_history and price_history[0].observed_time < cutoff:
                price_history.popleft()
        self._depth_recovering = False
        self._counters["depth_mutations"] += 1
        if reduction:
            self._bid_depletions[price] = 1
            self._put_evidence(HypothesisKind.BEARISH_CONTINUATION, EvidenceFamily.RESTING_LIQUIDITY, "BID_LIQUIDITY_PULL", self.artifact.structural_strength, True, mutation.observed_at, self.artifact.liquidity_evidence_lifetime_seconds, (mutation,))
        if increase and self._bid_depletions.get(price, 0) > 0:
            # Count a depletion followed by an increase as one replenishment
            # cycle and require two cycles at the same price.
            self._bid_depletions[price] = 0
            cycles = self._bid_replenishment_cycles.get(price, 0) + 1
            self._bid_replenishment_cycles[price] = cycles
            if cycles >= self.artifact.replenishment_count:
                assert price_history is not None
                sources = tuple(item for item in price_history if item.is_increase or item.is_reduction)
                self._put_evidence(HypothesisKind.BULLISH_REVERSAL, EvidenceFamily.RESTING_LIQUIDITY, "BID_REPLENISHMENT", self.artifact.structural_strength, True, mutation.observed_at, self.artifact.liquidity_evidence_lifetime_seconds, sources)
        return None

    def _active_evidence(self, at: datetime, hypothesis: HypothesisKind) -> tuple[PaperEvidence, ...]:
        active = tuple(
            value for value in self._evidence.values()
            if value.hypothesis_kind is hypothesis and self._time(value.expires_at) >= at
            and value.session_id == self._paper_session_context.session_id
            and value.session_generation == self._paper_session_context.session_generation
        )
        return active

    def score(self, at: str, hypothesis: HypothesisKind) -> tuple[Decimal, dict[str, object]]:
        moment = self._time(at, "Paper score time")
        active = self._active_evidence(moment, hypothesis)
        family_summary: dict[str, object] = {}
        balance_total = Decimal("0")
        positive_families = 0
        blocking = False
        for family in EvidenceFamily:
            values = tuple(item for item in active if item.family is family)
            supports = max((item.strength for item in values if item.supports), default=Decimal("0"))
            contradictions = max((item.strength for item in values if not item.supports), default=Decimal("0"))
            if supports > 0:
                positive_families += 1
            blocking = blocking or any(item.blocking and not item.supports for item in values)
            balance_total += supports - contradictions
            family_summary[family.value] = {
                "support": str(supports),
                "contradiction": str(contradictions),
                "labels": sorted(item.label for item in values),
            }
        score = max(Decimal("0"), min(Decimal("1"), Decimal("0.5") + balance_total / self.artifact.score_denominator))
        family_summary["positive_family_count"] = positive_families
        family_summary["blocking_contradiction"] = blocking
        return score, family_summary

    def active_evidence(self, at: str) -> tuple[PaperEvidence, ...]:
        """Expose immutable current paper evidence for durable audit only."""
        moment = self._time(at, "Paper evidence audit time")
        with self._lock:
            return tuple(sorted(
                (value for value in self._evidence.values() if self._time(value.expires_at) >= moment),
                key=lambda value: value.evidence_id,
            ))

    def classified_trade_count(self) -> int:
        """Return the one hot-path counter needed by runtime freshness state."""
        with self._lock:
            return self._counters["classified_buy"] + self._counters["classified_sell"]

    def runtime_gate_state(self) -> tuple[int, bool, bool]:
        """Return minimal preflight state without building public status."""
        with self._lock:
            now = datetime.now(timezone.utc)
            warmed = all(
                any(
                    value.family is family and self._time(value.expires_at) >= now
                    for value in self._evidence.values()
                )
                for family in (
                    EvidenceFamily.STRUCTURAL_CONTEXT,
                    EvidenceFamily.ORDER_FLOW,
                    EvidenceFamily.RESTING_LIQUIDITY,
                )
            )
            classified = self._counters["classified_buy"] + self._counters["classified_sell"]
            return classified, warmed, self._depth_recovering

    def _decision_sources(self, hypothesis: HypothesisKind | None, at: datetime, fallback: NinjaTraderObservation) -> tuple[tuple[str, ...], tuple[int, ...], tuple[str, ...]]:
        if hypothesis is None:
            return ((fallback.observation_id,), (fallback.local_monotonic_sequence,), (self._payload_hash(fallback),))
        evidence = self._active_evidence(at, hypothesis)
        combined = sorted(
            {(identifier, sequence, payload_hash) for item in evidence for identifier, sequence, payload_hash in zip(item.source_observation_ids, item.source_local_sequences, item.source_payload_hashes)},
            key=lambda item: (item[1], item[0]),
        )
        if not combined:
            return ((fallback.observation_id,), (fallback.local_monotonic_sequence,), (self._payload_hash(fallback),))
        return tuple(item[0] for item in combined), tuple(item[1] for item in combined), tuple(item[2] for item in combined)

    def _hypothesis_instance(self, decision: PaperDecision) -> str:
        return canonical_hash({
            "hypothesis": None if decision.hypothesis_kind is None else decision.hypothesis_kind.value,
            "sources": decision.source_observation_ids, "policy": decision.paper_policy_hash,
            "session_id": decision.session_id, "session_generation": decision.session_generation,
        })

    def _decision(
        self,
        observation: NinjaTraderObservation,
        kind: PaperDecisionKind,
        hypothesis: HypothesisKind | None,
        reason: str,
        *,
        score: Decimal = Decimal("0.5"),
        family_summary: Mapping[str, object] | None = None,
    ) -> PaperDecision:
        created = normalized_utc(self._market_event_timestamp(observation), "Paper decision time")
        at = self._time(created)
        source_ids, sequences, hashes = self._decision_sources(hypothesis, at, observation)
        direction = PaperDirection.LONG if kind is PaperDecisionKind.LONG else PaperDirection.SHORT if kind is PaperDecisionKind.SHORT else PaperDirection.FLAT
        payload = {
            "policy_id": self.artifact.policy_id,
            "policy_hash": self._policy_hash,
            "decision": kind.value,
            "created_at": created,
            "expires_at": expires_at(created, self.artifact.decision_ttl_seconds),
            "hypothesis_kind": None if hypothesis is None else hypothesis.value,
            "direction": direction.value,
            "relative_support": str(score),
            "family_summary": dict(family_summary or {}),
            "source_observation_ids": source_ids,
            "source_local_sequences": sequences,
            "source_payload_hashes": hashes,
            "sequence_authority": SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY.value,
            "book_completeness": BookCompleteness.UNVERIFIED.value,
            "scientific_eligibility": False,
            "reason_code": reason,
            "session_kind": self._paper_session_context.session_kind.value,
            "session_id": self._paper_session_context.session_id,
            "trade_date": self._paper_session_context.trade_date,
            "session_profile_hash": self._paper_session_context.session_profile_hash,
            "session_generation": self._paper_session_context.session_generation,
        }
        decision = PaperDecision(
            deterministic_id("l3g-pd-", payload), self.artifact.policy_id, self._policy_hash,
            kind, created, str(payload["expires_at"]), hypothesis, direction, score,
            dict(family_summary or {}), source_ids, sequences, hashes,
            SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY, BookCompleteness.UNVERIFIED, False, reason,
            self._paper_session_context.session_kind, self._paper_session_context.session_id,
            self._paper_session_context.trade_date, self._paper_session_context.session_profile_hash,
            self._paper_session_context.session_generation,
        )
        self._last_decision = decision
        counter = {
            PaperDecisionKind.LONG: "decisions_long",
            PaperDecisionKind.SHORT: "decisions_short",
            PaperDecisionKind.EXIT: "decisions_exit",
            PaperDecisionKind.NO_TRADE: "decisions_no_trade",
        }[kind]
        self._counters[counter] += 1
        if kind is PaperDecisionKind.NO_TRADE:
            self._suppress(reason)
        return decision

    def evaluate(
        self,
        observation: NinjaTraderObservation,
        *,
        current_position: PaperDirection = PaperDirection.FLAT,
        pending_order: bool = False,
    ) -> PaperDecision:
        at_text = normalized_utc(self._market_event_timestamp(observation), "Paper evaluation time")
        at = self._time(at_text)
        if self._transport_state is not StreamHealth.HEALTHY:
            return self._decision(observation, PaperDecisionKind.NO_TRADE, None, "LOCAL_BRIDGE_UNHEALTHY")
        if not self._price_connected:
            return self._decision(observation, PaperDecisionKind.NO_TRADE, None, "MARKET_PRICE_STATE_NOT_CONNECTED")
        if self._depth_recovering:
            return self._decision(observation, PaperDecisionKind.NO_TRADE, None, "DEPTH_RESET_RECOVERY")

        bull_score, bull_families = self.score(at_text, HypothesisKind.BULLISH_REVERSAL)
        bear_score, bear_families = self.score(at_text, HypothesisKind.BEARISH_CONTINUATION)
        scores = {
            HypothesisKind.BULLISH_REVERSAL: (bull_score, bull_families),
            HypothesisKind.BEARISH_CONTINUATION: (bear_score, bear_families),
        }
        winner = max(scores, key=lambda item: (scores[item][0], item.value))
        loser = HypothesisKind.BEARISH_CONTINUATION if winner is HypothesisKind.BULLISH_REVERSAL else HypothesisKind.BULLISH_REVERSAL
        score, families = scores[winner]
        dominance = score - scores[loser][0]
        positive = int(families["positive_family_count"])
        blocking = bool(families["blocking_contradiction"])

        if current_position is not PaperDirection.FLAT:
            owned = HypothesisKind.BULLISH_REVERSAL if current_position is PaperDirection.LONG else HypothesisKind.BEARISH_CONTINUATION
            owned_score, owned_families = scores[owned]
            opposing = HypothesisKind.BEARISH_CONTINUATION if owned is HypothesisKind.BULLISH_REVERSAL else HypothesisKind.BULLISH_REVERSAL
            opposing_score = scores[opposing][0]
            retain = (
                owned_score >= self.artifact.retention_support_threshold
                and int(owned_families["positive_family_count"]) >= self.artifact.retention_family_count
                and owned_score - opposing_score >= self.artifact.retention_dominance_margin
                and not bool(owned_families["blocking_contradiction"])
            )
            if not retain or (winner is opposing and dominance >= self.artifact.entry_dominance_margin):
                return self._decision(observation, PaperDecisionKind.EXIT, opposing if winner is opposing else owned, "OPPOSING_HYPOTHESIS" if winner is opposing else "RETENTION_FAILED", score=opposing_score if winner is opposing else owned_score, family_summary=scores[opposing][1] if winner is opposing else owned_families)
            return self._decision(observation, PaperDecisionKind.NO_TRADE, owned, "POSITION_RETAINED", score=owned_score, family_summary=owned_families)

        if pending_order:
            return self._decision(observation, PaperDecisionKind.NO_TRADE, winner, "PENDING_ORDER", score=score, family_summary=families)
        if positive < self.artifact.entry_family_count:
            return self._decision(observation, PaperDecisionKind.NO_TRADE, winner, "ENTRY_FAMILY_COUNT", score=score, family_summary=families)
        if score < self.artifact.entry_support_threshold:
            return self._decision(observation, PaperDecisionKind.NO_TRADE, winner, "ENTRY_SUPPORT_THRESHOLD", score=score, family_summary=families)
        if dominance < self.artifact.entry_dominance_margin:
            return self._decision(observation, PaperDecisionKind.NO_TRADE, winner, "ENTRY_DOMINANCE_MARGIN", score=score, family_summary=families)
        if blocking:
            return self._decision(observation, PaperDecisionKind.NO_TRADE, winner, "BLOCKING_CONTRADICTION", score=score, family_summary=families)
        entry_kind = PaperDecisionKind.LONG if winner is HypothesisKind.BULLISH_REVERSAL else PaperDecisionKind.SHORT
        candidate = self._decision(observation, entry_kind, winner, "ENTRY_AUTHORIZED_DIRECTION", score=score, family_summary=families)
        instance = self._hypothesis_instance(candidate)
        if instance in self._used_hypothesis_instances:
            return self._decision(observation, PaperDecisionKind.NO_TRADE, winner, "HYPOTHESIS_ALREADY_USED", score=score, family_summary=families)
        if self._last_flat_confirmation is not None and at - self._last_flat_confirmation < timedelta(seconds=self.artifact.reentry_cooldown_seconds):
            return self._decision(observation, PaperDecisionKind.NO_TRADE, winner, "REENTRY_COOLDOWN", score=score, family_summary=families)
        return candidate

    def status(self) -> dict[str, object]:
        with self._lock:
            now = datetime.now(timezone.utc)
            evidence_counts = {
                family.value: sum(1 for value in self._evidence.values() if value.family is family and self._time(value.expires_at) >= now)
                for family in EvidenceFamily
            }
            return {
                "schema": "lane-iii-phase-g-paper-policy-status-v1",
                "paper_policy_id": self.artifact.policy_id,
                "paper_policy_hash": self._policy_hash,
                "authority": self.artifact.authority,
                "quality": PaperSourceQuality.PROVISIONAL_CONTIGUOUS_LOCAL_CALLBACKS.value if self._market_session_id and not self._depth_recovering else PaperSourceQuality.UNUSABLE.value,
                "sequence_authority": SequenceAuthority.LOCAL_CALLBACK_ORDER_ONLY.value,
                "book_completeness": BookCompleteness.UNVERIFIED.value,
                "scientific_eligibility": False,
                "market_session_id": self._market_session_id,
                "current_session": self._paper_session_context.session_kind.value,
                "current_session_id": self._paper_session_context.session_id,
                "trade_date": self._paper_session_context.trade_date,
                "session_profile_hash": self._paper_session_context.session_profile_hash,
                "session_generation": self._paper_session_context.session_generation,
                "last_local_sequence": self._last_local_sequence,
                "market_price_connected": self._price_connected,
                "local_bridge": self._transport_state.value,
                "depth_reset_recovery": self._depth_recovering,
                "provisional_session_vwap": None if self._vwap_volume == 0 else str(self._vwap_notional / self._vwap_volume),
                "vwap_authority": "PROVISIONAL_LOCAL_SESSION_VWAP",
                "paper_evidence_by_family": evidence_counts,
                "counters": dict(self._counters),
                "suppression_reasons": dict(sorted(self._suppression_reasons.items())),
                "last_paper_decision": None if self._last_decision is None else self._last_decision.payload(),
                "last_input_fault": self._last_input_fault,
            }
