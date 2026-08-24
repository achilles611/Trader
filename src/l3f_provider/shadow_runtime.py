"""Bounded, one-way Lane III live-shadow runtime.

This module connects the NinjaTrader observation boundary to the frozen L3-B,
L3-C, and L3-D interfaces.  It is intentionally not an execution runtime:
the only output is an in-memory audit record describing a hypothetical
directional signal or a safety suppression.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
import threading
from typing import Callable

from src.l3f_provider.ninjatrader_observation import (
    L3F2_SCHEMA,
    NinjaTraderContract,
    NinjaTraderMarketDataAdapter,
    NinjaTraderObservation,
    NinjaTraderObservationError,
)
from src.l3f_provider.tradovate_observation import ProviderErrorCode, StreamHealth

from src.lane_iii.contracts import canonical_hash, normalized_utc
from src.lane_iii.hypothesis_engine import HypothesisEngine, HypothesisEngineRefused
from src.lane_iii.market_data import (
    AggressorProvenance,
    BookApplyOutcome,
    DataQuality,
    MarketDataPipeline,
    MarketDataRefused,
    OrderingOutcome,
    RawProviderEvent,
    TradeEvent,
)
from src.lane_iii.trader_v0 import SignalDecision, TraderDataQuality, TraderEvaluationRefused, TraderV0


LANE_III_SHADOW_MODE = "LANE_III_SHADOW"
L3_SHADOW_SCHEMA = "lane-iii-live-shadow-v1"
_MARKET_TYPES = frozenset({"QUOTE", "TRADE", "DEPTH"})
_DEFAULT_NINJATRADER_CONTRACT = NinjaTraderContract(
    "MNQ", "MNQ SEP26", "2026-09", "CME", Decimal("0.25"), "MNQ SEPT26", Decimal("2"),
)


class ShadowExecutionDenied(RuntimeError):
    """There is no execution authority on the live-shadow path."""


@dataclass(frozen=True)
class ShadowRuntimeConfig:
    """Fixed observation identity and bounded operational retention."""

    contract: NinjaTraderContract = _DEFAULT_NINJATRADER_CONTRACT
    audit_limit: int = 512
    duplicate_history_limit: int = 4096

    def __post_init__(self) -> None:
        if type(self.contract) is not NinjaTraderContract:
            raise ValueError("Live shadow requires the exact NinjaTrader MNQ SEP26 contract binding.")
        if type(self.audit_limit) is not int or self.audit_limit <= 0:
            raise ValueError("Live-shadow audit retention must be a positive integer.")
        if type(self.duplicate_history_limit) is not int or self.duplicate_history_limit <= 0:
            raise ValueError("Live-shadow duplicate retention must be a positive integer.")


class ShadowExecutionGuard:
    """The only execution-boundary object reachable from a shadow result."""

    def __init__(self, record_denial: Callable[[str], None]) -> None:
        self._record_denial = record_denial

    def deny(self, candidate: object) -> None:
        """Reject a forced handoff before an execution interface can exist."""
        self._record_denial(type(candidate).__name__)
        raise ShadowExecutionDenied("LANE_III_SHADOW has no execution authority.")


class LaneIIIShadowRuntime:
    """One synchronous, locked consumer of admitted NinjaTrader observations.

    The caller supplies only records already admitted by the read-only
    NinjaTrader listener.  Every market event proceeds through the same frozen
    L3-B → L3-C → L3-D interfaces used by deterministic replay.  Unsafe input
    records produce a suppression audit and cannot be partially retained as
    market state.
    """

    def __init__(self, config: ShadowRuntimeConfig = ShadowRuntimeConfig()) -> None:
        if type(config) is not ShadowRuntimeConfig:
            raise ValueError("Live shadow requires an exact immutable runtime configuration.")
        self.config = config
        self.mode = LANE_III_SHADOW_MODE
        self._lock = threading.RLock()
        self._audit: deque[dict[str, object]] = deque(maxlen=config.audit_limit)
        self._audit_sequence = 0
        self._seen_wire_hashes: set[str] = set()
        self._seen_wire_order: deque[str] = deque()
        self._counters: dict[str, int] = {
            "raw_accepted_observations": 0,
            "raw_rejected_observations": 0,
            "quote_events": 0,
            "trade_events": 0,
            "trade_aggressor_provider_native": 0,
            "trade_aggressor_quote_derived": 0,
            "trade_aggressor_unknown": 0,
            "depth_events": 0,
            "normalized_admitted_market_events": 0,
            "downstream_rejected_market_events": 0,
            "stale_events": 0,
            "malformed_events": 0,
            "duplicate_events": 0,
            "downstream_interpretation_invocations": 0,
            "downstream_interpretation_failures": 0,
            "shadow_decision_evaluations": 0,
            "shadow_decisions_emitted": 0,
            "shadow_directional_actions": 0,
            "decisions_suppressed": 0,
            "execution_attempts": 0,
            "execution_denials": 0,
            "transport_disconnects": 0,
            "state_resets": 0,
        }
        self._transport_state = StreamHealth.UNKNOWN
        self._provider_price_state = "UNKNOWN"
        self._market_session_id: str | None = None
        self._generation = 0
        self._last_event_time: datetime | None = None
        self._last_stream_time: dict[str, datetime] = {}
        self._rebuild_downstream(initial=True)
        self.execution_guard = ShadowExecutionGuard(self._record_execution_denial)

    def _rebuild_downstream(self, *, initial: bool = False) -> None:
        self.adapter = NinjaTraderMarketDataAdapter(self.config.contract)
        self.pipeline = MarketDataPipeline(self.adapter.source, self.config.contract.canonical)
        self.engine = HypothesisEngine(self.adapter.source, self.config.contract.canonical)
        self.trader = TraderV0()
        self._last_event_time = None
        self._last_stream_time.clear()
        if not initial:
            self._generation += 1
            self._counters["state_resets"] += 1

    @staticmethod
    def _time(value: str, field: str) -> datetime:
        return datetime.fromisoformat(normalized_utc(value, field).replace("Z", "+00:00"))

    @staticmethod
    def _wire_payload(observation: NinjaTraderObservation) -> dict[str, object]:
        return {
            "schema": L3F2_SCHEMA,
            "observation_id": observation.observation_id,
            "session_id": observation.session_id,
            "observation_type": observation.observation_type,
            "ninja_receipt_time": observation.ninja_receipt_time,
            "local_monotonic_sequence": observation.local_monotonic_sequence,
            "provider_timestamp": observation.provider_timestamp,
            "provider_sequence": observation.provider_sequence,
            "exchange_timestamp": observation.exchange_timestamp,
            "account": None if observation.account_alias is None else {
                "alias": observation.account_alias,
                "class": observation.account_class.value if observation.account_class is not None else None,
            },
            "payload": dict(observation.payload),
        }

    def _audit_event(
        self,
        kind: str,
        *,
        observation: NinjaTraderObservation | None = None,
        canonical_event_id: str | None = None,
        timestamp: str | None = None,
        **details: object,
    ) -> None:
        self._audit_sequence += 1
        record: dict[str, object] = {
            "schema": L3_SHADOW_SCHEMA,
            "sequence": self._audit_sequence,
            "kind": kind,
            "mode": self.mode,
            "state_generation": self._generation,
            "execution_authority": "DENIED",
            "execution_attempts": 0,
        }
        if observation is not None:
            record.update({
                "observation_id": observation.observation_id,
                "observation_session_id": observation.session_id,
                "observation_type": observation.observation_type,
                "instrument": self.config.contract.canonical.payload(),
            })
        if canonical_event_id is not None:
            record["canonical_event_id"] = canonical_event_id
        if timestamp is not None:
            record["timestamp"] = normalized_utc(timestamp, "Shadow audit timestamp")
        record.update(details)
        self._audit.append(record)

    def _remember_wire_hash(self, wire_hash: str) -> bool:
        if wire_hash in self._seen_wire_hashes:
            return False
        self._seen_wire_hashes.add(wire_hash)
        self._seen_wire_order.append(wire_hash)
        while len(self._seen_wire_order) > self.config.duplicate_history_limit:
            self._seen_wire_hashes.remove(self._seen_wire_order.popleft())
        return True

    def _maximum_age(self, observation_type: str) -> timedelta:
        config = self.engine.config
        return {
            "QUOTE": config.quote_maximum_age,
            "TRADE": config.trade_maximum_age,
            "DEPTH": config.book_maximum_age,
        }[observation_type]

    def _suppress(
        self,
        reason: str,
        *,
        observation: NinjaTraderObservation | None = None,
        timestamp: str | None = None,
        stale: bool = False,
        malformed: bool = False,
        rejected_market_event: bool = False,
        **details: object,
    ) -> None:
        self._counters["decisions_suppressed"] += 1
        if stale:
            self._counters["stale_events"] += 1
        if malformed:
            self._counters["malformed_events"] += 1
        if rejected_market_event:
            self._counters["downstream_rejected_market_events"] += 1
        self._audit_event(
            "SHADOW_DECISION_SUPPRESSED",
            observation=observation,
            timestamp=timestamp,
            reason_code=reason,
            **details,
        )

    def record_raw_rejection(self, error: NinjaTraderObservationError) -> None:
        """Record a receiver-level refusal without manufacturing market state."""
        with self._lock:
            self._counters["raw_rejected_observations"] += 1
            self._suppress(
                error.code.value,
                malformed=error.code is ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD,
                stale=error.code is ProviderErrorCode.STALE_PROVIDER_STATE,
                error_detail=error.detail or "unspecified",
            )

    def record_raw_duplicate(self) -> None:
        with self._lock:
            self._counters["duplicate_events"] += 1
            self._suppress("DUPLICATE_OBSERVATION")

    def on_transport_state(self, state: StreamHealth) -> None:
        """Discard prior market state across a disconnected bridge boundary."""
        if type(state) is not StreamHealth:
            raise ValueError("Shadow transport state must be explicit.")
        with self._lock:
            if state is self._transport_state:
                return
            self._transport_state = state
            self._audit_event("TRANSPORT_STATE", transport_state=state.value)
            if state is StreamHealth.DISCONNECTED:
                self._counters["transport_disconnects"] += 1
                self._rebuild_downstream()
                self._suppress("TRANSPORT_DISCONNECTED", stale=True)

    def _record_execution_denial(self, candidate_type: str) -> None:
        with self._lock:
            # A denial happens before any attempt is constructible.  This is
            # deliberately separate from execution_attempts, which remains 0.
            self._counters["execution_denials"] += 1
            self._audit_event("EXECUTION_HARD_DENIAL", candidate_type=candidate_type)

    def ingest(self, observation: NinjaTraderObservation) -> None:
        """Consume one already-admitted observation, or record why it stopped."""
        if type(observation) is not NinjaTraderObservation:
            raise ValueError("Live shadow requires an exact admitted NinjaTrader observation.")
        with self._lock:
            self._counters["raw_accepted_observations"] += 1
            if not self._remember_wire_hash(observation.wire_hash()):
                self._counters["duplicate_events"] += 1
                self._suppress("DUPLICATE_OBSERVATION", observation=observation)
                return
            if observation.observation_type in _MARKET_TYPES:
                self._counters[{"QUOTE": "quote_events", "TRADE": "trade_events", "DEPTH": "depth_events"}[observation.observation_type]] += 1
                if self._market_session_id is not None and observation.session_id != self._market_session_id:
                    # Session IDs come from the one-way observer and create a
                    # new ordering domain.  No former session state survives.
                    self._rebuild_downstream()
                    self._suppress("MARKET_SESSION_BOUNDARY", observation=observation, stale=True)
                self._market_session_id = observation.session_id
            elif observation.observation_type == "INSTRUMENT":
                supplied = observation.payload.get("contract_id")
                if supplied is not None and supplied != self.config.contract.internal_contract_id:
                    self._suppress("CONTRACT_NOT_FOUND", observation=observation, malformed=True)
                else:
                    self._audit_event("NON_MARKET_OBSERVATION", observation=observation)
                return
            elif observation.observation_type == "CONNECTION" and observation.payload.get("scope") == "MARKET_DATA":
                supplied_state = str(observation.payload.get("price_status", "UNKNOWN")).upper()
                prior_state = self._provider_price_state
                self._provider_price_state = supplied_state
                self._audit_event(
                    "PROVIDER_PRICE_FEED_STATE",
                    observation=observation,
                    provider_price_state=supplied_state,
                    prior_provider_price_state=prior_state,
                )
                if supplied_state != "CONNECTED" and prior_state in {"UNKNOWN", "CONNECTED"}:
                    self._rebuild_downstream()
                    self._suppress("PROVIDER_PRICE_FEED_NOT_CONNECTED", observation=observation, stale=True)
                return
            else:
                self._audit_event("NON_MARKET_OBSERVATION", observation=observation)
                return

            if observation.observation_type == "DEPTH":
                if observation.payload.get("is_reset") is True:
                    # NinjaTrader's public reset flag is authoritative for the
                    # invalidation boundary, even though it does not establish
                    # a synchronized replacement snapshot. Retain no old book,
                    # trade-flow, or hypothesis state across that callback.
                    self._rebuild_downstream()
                    self._suppress("PROVIDER_DEPTH_RESET", observation=observation, stale=True)
                    return
                bids = observation.payload.get("bids")
                asks = observation.payload.get("asks")
                if not isinstance(bids, list) or not bids or not isinstance(asks, list) or not asks:
                    self._suppress("PARTIAL_DEPTH_SNAPSHOT", observation=observation, malformed=True, rejected_market_event=True)
                    return

            event_time_text = observation.exchange_timestamp or observation.provider_timestamp
            if event_time_text is None:
                self._suppress("MISSING_AUTHORITATIVE_EVENT_TIMESTAMP", observation=observation, stale=True, rejected_market_event=True)
                return
            event_time = self._time(event_time_text, "NinjaTrader event timestamp")
            receipt_time = self._time(observation.ninja_receipt_time, "NinjaTrader receipt timestamp")
            maximum_age = self._maximum_age(observation.observation_type)
            if receipt_time - event_time > maximum_age:
                self._suppress(
                    "STALE_PROVIDER_EVENT",
                    observation=observation,
                    timestamp=event_time_text,
                    stale=True,
                    rejected_market_event=True,
                    maximum_age_seconds=int(maximum_age.total_seconds()),
                )
                return
            if self._last_event_time is not None and event_time < self._last_event_time:
                self._suppress("TIMESTAMP_MOVED_BACKWARD", observation=observation, timestamp=event_time_text, stale=True, rejected_market_event=True)
                return
            prior_stream_time = self._last_stream_time.get(observation.observation_type)
            if prior_stream_time is not None and event_time - prior_stream_time > maximum_age:
                self._rebuild_downstream()
                self._suppress(
                    "LARGE_TIMESTAMP_GAP",
                    observation=observation,
                    timestamp=event_time_text,
                    stale=True,
                    rejected_market_event=True,
                    maximum_age_seconds=int(maximum_age.total_seconds()),
                )
                return

            try:
                raw = RawProviderEvent(
                    observation.observation_id,
                    self.adapter.source,
                    observation.ninja_receipt_time,
                    self._wire_payload(observation),
                    observation.observation_id,
                )
                events = self.adapter.normalize(raw)
                if len(events) != 1:
                    raise MarketDataRefused("The NinjaTrader adapter must emit exactly one canonical market event.")
                event = events[0]
                if isinstance(event, TradeEvent):
                    counter = {
                        AggressorProvenance.PROVIDER: "trade_aggressor_provider_native",
                        AggressorProvenance.QUOTE_DERIVED: "trade_aggressor_quote_derived",
                        AggressorProvenance.UNAVAILABLE: "trade_aggressor_unknown",
                    }[event.aggressor_provenance]
                    self._counters[counter] += 1
                result = self.pipeline.apply(event)
                if result.ordering in {OrderingOutcome.DUPLICATE, OrderingOutcome.LATE} or (
                    result.book_application is not None
                    and result.book_application.outcome in {BookApplyOutcome.DUPLICATE, BookApplyOutcome.LATE}
                ):
                    self._counters["duplicate_events"] += 1
                    self._suppress("DUPLICATE_OR_LATE_CANONICAL_EVENT", observation=observation, timestamp=event_time_text)
                    return
                self._counters["normalized_admitted_market_events"] += 1
                self._counters["downstream_interpretation_invocations"] += 1
                snapshot = self.engine.observe(event, result, self.pipeline)
                self._last_event_time = event_time
                self._last_stream_time[observation.observation_type] = event_time
                qualities = self.pipeline.staleness(
                    event.header.timestamps.ordering_time,
                    trade_maximum_age=self.engine.config.trade_maximum_age,
                    quote_maximum_age=self.engine.config.quote_maximum_age,
                    book_maximum_age=self.engine.config.book_maximum_age,
                )
                market_state_hash = self._market_state_hash(qualities)
                quality = TraderDataQuality(
                    snapshot.evaluated_at,
                    market_state_hash,
                    qualities["trade"],
                    qualities["quote"],
                    qualities["book"],
                    qualities["context"],
                )
                self._audit_event(
                    "MARKET_STATE_ADMITTED",
                    observation=observation,
                    canonical_event_id=event.header.event_id,
                    timestamp=event.header.timestamps.ordering_time,
                    market_state_hash=market_state_hash,
                    data_quality=quality.payload(),
                )
                if not quality.healthy:
                    reasons = tuple(sorted(name.upper() for name, value in qualities.items() if value is not DataQuality.HEALTHY))
                    self._suppress(
                        "MARKET_STATE_NOT_HEALTHY:" + ",".join(reasons),
                        observation=observation,
                        timestamp=event.header.timestamps.ordering_time,
                        market_state_hash=market_state_hash,
                    )
                    return
                self._counters["shadow_decision_evaluations"] += 1
                decision = self.trader.evaluate(snapshot, quality)
                self._record_decision(observation, event.header.event_id, snapshot.snapshot_hash, quality, decision)
            except NinjaTraderObservationError as error:
                self.pipeline.note_rejected_provider_event()
                self._suppress(
                    error.code.value,
                    observation=observation,
                    timestamp=event_time_text,
                    stale=error.code is ProviderErrorCode.STALE_PROVIDER_STATE,
                    malformed=error.code is ProviderErrorCode.MALFORMED_PROVIDER_PAYLOAD,
                    rejected_market_event=True,
                    error_detail=error.detail or "unspecified",
                )
            except (MarketDataRefused, HypothesisEngineRefused, TraderEvaluationRefused, ValueError) as error:
                self._counters["downstream_interpretation_failures"] += 1
                self._rebuild_downstream()
                self._suppress(
                    "DOWNSTREAM_REFUSED",
                    observation=observation,
                    timestamp=event_time_text,
                    stale=True,
                    rejected_market_event=True,
                    error_type=type(error).__name__,
                )
            except Exception as error:  # Defensive boundary: retain no possibly-corrupt state.
                self._counters["downstream_interpretation_failures"] += 1
                self._rebuild_downstream()
                self._suppress(
                    "DOWNSTREAM_EXCEPTION",
                    observation=observation,
                    timestamp=event_time_text,
                    stale=True,
                    rejected_market_event=True,
                    error_type=type(error).__name__,
                )

    def _market_state_hash(self, qualities: dict[str, DataQuality]) -> str:
        return canonical_hash({
            "schema": L3_SHADOW_SCHEMA,
            "generation": self._generation,
            "instrument": self.config.contract.canonical.payload(),
            "latest_quote_event_id": None if self.pipeline.latest_quote is None else self.pipeline.latest_quote.header.event_id,
            "latest_trade_event_id": None if self.pipeline.latest_trade is None else self.pipeline.latest_trade.header.event_id,
            "book_state_hash": self.pipeline.book._state().state_hash,
            "qualities": {name: value.value for name, value in sorted(qualities.items())},
        })

    def _record_decision(
        self,
        observation: NinjaTraderObservation,
        canonical_event_id: str,
        snapshot_hash: str,
        quality: TraderDataQuality,
        decision: SignalDecision,
    ) -> None:
        self._counters["shadow_decisions_emitted"] += 1
        if decision.decision.value != "NO_TRADE":
            self._counters["shadow_directional_actions"] += 1
        self._audit_event(
            "SHADOW_DECISION_EMITTED",
            observation=observation,
            canonical_event_id=canonical_event_id,
            timestamp=decision.created_at,
            market_state_hash=quality.market_state_hash,
            l3c_snapshot_hash=snapshot_hash,
            interpreter_identity=self.engine.config.configuration_hash,
            decision_id=decision.decision_id,
            decision_identity=decision.strategy_identity,
            decision_artifact_hash=decision.strategy_artifact_hash,
            hypothetical_action=decision.decision.value,
            decision_result="EMITTED",
            reason_code=decision.reason_code.value,
            execution_authority="DENIED",
        )

    def status(self) -> dict[str, object]:
        """Sanitized operational counters and fixed authority identity."""
        with self._lock:
            pipeline_metrics = self.pipeline.metrics()
            return {
                "schema": L3_SHADOW_SCHEMA,
                "mode": self.mode,
                "contract": {
                    "native_name": self.config.contract.native_full_name,
                    "provider_contract_id": self.config.contract.internal_contract_id,
                    "canonical": self.config.contract.canonical.payload(),
                },
                "transport_state": self._transport_state.value,
                "provider_price_state": self._provider_price_state,
                "state_generation": self._generation,
                "authority": {
                    "observation": "OBSERVE_ONLY",
                    "interpretation": "SHADOW_ONLY",
                    "decision": "SHADOW_ONLY",
                    "execution": "DENIED",
                    "live_capital": "DENIED",
                },
                "counters": dict(sorted(self._counters.items())),
                "pipeline": {
                    "events_processed": pipeline_metrics.events_processed,
                    "events_rejected": pipeline_metrics.events_rejected,
                    "events_duplicated": pipeline_metrics.events_duplicated,
                    "sequence_gaps": pipeline_metrics.sequence_gaps,
                    "latest_book_quality": pipeline_metrics.latest_book_quality.value,
                },
                "audit_records_retained": len(self._audit),
            }

    def audit_records(self, limit: int = 100) -> tuple[dict[str, object], ...]:
        if type(limit) is not int or not 1 <= limit <= self.config.audit_limit:
            raise ValueError("Live-shadow audit limit is outside configured retention.")
        with self._lock:
            return tuple(dict(value) for value in list(self._audit)[-limit:])
