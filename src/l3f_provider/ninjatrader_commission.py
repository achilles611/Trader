"""Beelzebub-owned, read-only NinjaTrader runtime receiver.

The Control Center FastAPI lifespan is the sole production owner. The worker
binds loopback, receives frames, and closes the listener on shutdown. It never
sends a byte to NinjaTrader or exposes a payload, identifier, or secret.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import math
import select
import socket
import threading
import time
from typing import Callable, Iterable, Mapping

from .ninjatrader_observation import (
    AccountClass, LOOPBACK_HOST, LoopbackBridgeConfig, LoopbackNinjaTraderBridge,
    NinjaTraderHealthStream, NinjaTraderHealthTracker, NinjaTraderObservation,
    NinjaTraderObservationError, NinjaTraderProviderHealth,
)
from .tradovate_observation import StreamHealth


# These are display-only account facts from NinjaTrader's read-only
# AccountItemUpdate stream.  They must never become execution or risk inputs.
_ACCOUNT_BALANCE_FIELD_BY_ITEM = {
    "CashValue": "cash_value",
    "NetLiquidation": "net_liquidation",
    "RealizedProfitLoss": "realized_pnl",
    "UnrealizedProfitLoss": "unrealized_pnl",
}
_DISPLAY_ACCOUNT_CLASSES = {
    "Sim101": AccountClass.LOCAL_SIMULATION.value,
    "Lucid25kflex01": AccountClass.PROVIDER_EVALUATION.value,
}

# Fixed receiver-service histogram. It adds constant memory and reports upper
# bounds rather than retaining or logging individual market-event latencies.
_RECEIVER_SERVICE_LATENCY_BUCKETS_MS = (
    0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0,
    10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
)
_NATIVE_TRANSPORT_TELEMETRY_SCHEMA = "native-outbound-aggregate-v1"
_NATIVE_TRANSPORT_INTEGER_FIELDS = frozenset({
    "native_callback_cpu_samples_total",
    "native_callback_cpu_failures_total",
    "native_depth_callback_cpu_cycles_total",
    "native_quote_callback_cpu_cycles_total",
    "native_trade_callback_cpu_cycles_total",
    "native_depth_callback_cpu_cycles_max",
    "native_quote_callback_cpu_cycles_max",
    "native_trade_callback_cpu_cycles_max",
    "native_depth_callbacks_total",
    "native_quote_callbacks_total",
    "native_trade_callbacks_total",
    "native_publication_attempts_total",
    "native_depth_publication_attempts_total",
    "native_quote_publication_attempts_total",
    "native_trade_publication_attempts_total",
    "native_outbound_queue_capacity",
    "native_outbound_queue_current",
    "native_outbound_queue_high_water",
    "native_outbound_queue_enqueued_total",
    "native_outbound_queue_dequeued_total",
    "native_outbound_queue_overflow_total",
    "native_sender_batches_total",
    "native_sender_batch_frames_total",
    "native_sender_batch_bytes_total",
    "native_sender_batch_frames_max",
    "native_sender_batch_bytes_max",
    "native_sender_socket_writes_total",
    "native_sender_reconnects_total",
    "native_sender_transport_failure_events_total",
    "native_sender_inflight_frames",
})
_NATIVE_TRANSPORT_NUMBER_FIELDS = frozenset({
    "native_telemetry_elapsed_seconds",
    "native_depth_callbacks_per_second",
    "native_quote_callbacks_per_second",
    "native_trade_callbacks_per_second",
    "native_depth_callback_wall_ms_total",
    "native_quote_callback_wall_ms_total",
    "native_trade_callback_wall_ms_total",
    "native_depth_callback_wall_ms_max",
    "native_quote_callback_wall_ms_max",
    "native_trade_callback_wall_ms_max",
    "native_publication_attempts_per_second",
    "native_publication_processing_ms_total",
    "native_publication_processing_ms_max",
    "native_outbound_queue_enqueue_per_second",
    "native_outbound_queue_dequeue_per_second",
    "native_outbound_queue_oldest_age_ms",
    "native_sender_batches_per_second",
    "native_sender_records_per_second",
    "native_sender_bytes_per_second",
    "native_sender_frames_per_batch",
    "native_sender_write_ms_total",
    "native_sender_write_ms_max",
    "native_sender_inflight_oldest_age_ms",
})


def _empty_account_balances() -> dict[str, dict[str, object]]:
    """Return the fixed, credential-free account-balance read model."""
    return {
        alias: {
            "alias": alias,
            "account_class": account_class,
            "cash_value": None,
            "cash_value_observed_at": None,
            "net_liquidation": None,
            "net_liquidation_observed_at": None,
            "realized_pnl": None,
            "realized_pnl_observed_at": None,
            "unrealized_pnl": None,
            "unrealized_pnl_observed_at": None,
        }
        for alias, account_class in _DISPLAY_ACCOUNT_CLASSES.items()
    }


@dataclass
class CommissioningSummary:
    listener_ready: bool = False
    listener_port: int = 48135
    accepted: int = 0
    rejected: int = 0
    duplicates: int = 0
    observation_types: dict[str, int] = field(default_factory=dict)
    aliases: dict[str, str] = field(default_factory=dict)
    error_codes: dict[str, int] = field(default_factory=dict)
    error_details: dict[str, int] = field(default_factory=dict)
    snapshot_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    health: dict[str, str] = field(default_factory=dict)
    local_bridge_transitions: list[str] = field(default_factory=list)
    bridge_frames: int = 0
    bridge_total_seconds: float = 0.0
    bridge_maximum_seconds: float = 0.0
    downstream_callbacks: int = 0
    downstream_total_seconds: float = 0.0
    downstream_maximum_seconds: float = 0.0
    first_accepted_monotonic: float | None = None
    last_accepted_monotonic: float | None = None
    controlled_shutdown_partial_frames: int = 0
    controlled_shutdown_partial_bytes: int = 0
    receiver_started_monotonic: float | None = None
    receiver_finished_monotonic: float | None = None
    receiver_recv_calls: int = 0
    receiver_recv_bytes: int = 0
    receiver_frames_extracted: int = 0
    receiver_frame_bytes: int = 0
    framing_buffer_current_bytes: int = 0
    framing_buffer_high_water_bytes: int = 0
    bridge_latency_histogram: list[int] = field(
        default_factory=lambda: [0] * (
            len(_RECEIVER_SERVICE_LATENCY_BUCKETS_MS) + 1
        ),
    )

    def begin_receiver(self) -> None:
        if self.receiver_started_monotonic is None:
            self.receiver_started_monotonic = time.perf_counter()
            self.receiver_finished_monotonic = None

    def finish_receiver(self) -> None:
        if self.receiver_started_monotonic is not None:
            self.receiver_finished_monotonic = time.perf_counter()

    def record_receive(self, byte_count: int) -> None:
        if type(byte_count) is not int or byte_count < 0:
            raise ValueError("Receiver byte count must be a non-negative integer.")
        self.receiver_recv_calls += 1
        self.receiver_recv_bytes += byte_count

    def record_frame_extracted(self, byte_count: int) -> None:
        if type(byte_count) is not int or byte_count < 0:
            raise ValueError("Extracted frame size must be a non-negative integer.")
        self.receiver_frames_extracted += 1
        self.receiver_frame_bytes += byte_count

    def record_framing_buffer(self, byte_count: int) -> None:
        if type(byte_count) is not int or byte_count < 0:
            raise ValueError("Framing buffer size must be a non-negative integer.")
        self.framing_buffer_current_bytes = byte_count
        self.framing_buffer_high_water_bytes = max(
            self.framing_buffer_high_water_bytes,
            byte_count,
        )

    def accept(self, observation: NinjaTraderObservation | None) -> None:
        if observation is None:
            self.duplicates += 1
            return
        self.accepted += 1
        now_monotonic = time.monotonic()
        self.first_accepted_monotonic = (
            now_monotonic if self.first_accepted_monotonic is None else self.first_accepted_monotonic
        )
        self.last_accepted_monotonic = now_monotonic
        self.observation_types[observation.observation_type] = self.observation_types.get(observation.observation_type, 0) + 1
        if observation.account_alias is not None and observation.account_class is not None:
            self.aliases[observation.account_alias] = observation.account_class.value
        if observation.observation_type == "SNAPSHOT_COMPLETE" and observation.account_alias is not None:
            scope = observation.payload.get("scope")
            count_key = "open_count" if scope == "POSITION" else "working_count" if scope == "ORDER" else None
            count = None if count_key is None else observation.payload.get(count_key)
            if type(count) is int and count >= 0:
                self.snapshot_counts.setdefault(observation.account_alias, {})[str(scope)] = count

    def record_bridge(self, elapsed_seconds: float) -> None:
        elapsed = max(0.0, elapsed_seconds)
        self.bridge_frames += 1
        self.bridge_total_seconds += elapsed
        self.bridge_maximum_seconds = max(self.bridge_maximum_seconds, elapsed)
        elapsed_ms = elapsed * 1000.0
        bucket = len(_RECEIVER_SERVICE_LATENCY_BUCKETS_MS)
        for index, upper_bound in enumerate(
            _RECEIVER_SERVICE_LATENCY_BUCKETS_MS,
        ):
            if elapsed_ms <= upper_bound:
                bucket = index
                break
        self.bridge_latency_histogram[bucket] += 1

    def record_downstream(self, elapsed_seconds: float) -> None:
        elapsed = max(0.0, elapsed_seconds)
        self.downstream_callbacks += 1
        self.downstream_total_seconds += elapsed
        self.downstream_maximum_seconds = max(self.downstream_maximum_seconds, elapsed)

    def pipeline_report(self) -> dict[str, object]:
        span = 0.0
        if self.first_accepted_monotonic is not None and self.last_accepted_monotonic is not None:
            span = max(0.0, self.last_accepted_monotonic - self.first_accepted_monotonic)
        receiver_span = 0.0
        if self.receiver_started_monotonic is not None:
            receiver_end = (
                self.receiver_finished_monotonic
                if self.receiver_finished_monotonic is not None
                else time.perf_counter()
            )
            receiver_span = max(
                0.0, receiver_end - self.receiver_started_monotonic,
            )

        def percentile_upper_bound(percentile: float) -> float:
            if self.bridge_frames <= 0:
                return 0.0
            rank = max(1, math.ceil(self.bridge_frames * percentile))
            cumulative = 0
            for index, count in enumerate(self.bridge_latency_histogram):
                cumulative += count
                if cumulative >= rank:
                    if index < len(_RECEIVER_SERVICE_LATENCY_BUCKETS_MS):
                        return _RECEIVER_SERVICE_LATENCY_BUCKETS_MS[index]
                    return round(self.bridge_maximum_seconds * 1000.0, 3)
            return round(self.bridge_maximum_seconds * 1000.0, 3)

        strict_decoded = self.accepted + self.duplicates

        def rate(count: int) -> float | None:
            return (
                round(count / receiver_span, 3)
                if receiver_span > 0
                else None
            )
        return {
            "receiver_transport": {
                "elapsed_seconds": round(receiver_span, 3),
                "recv_calls": self.receiver_recv_calls,
                "recv_bytes": self.receiver_recv_bytes,
                "recv_calls_per_second": rate(self.receiver_recv_calls),
                "recv_bytes_per_second": rate(self.receiver_recv_bytes),
                "frames_extracted": self.receiver_frames_extracted,
                "frame_bytes": self.receiver_frame_bytes,
                "frames_extracted_per_second": rate(
                    self.receiver_frames_extracted,
                ),
                "framing_buffer_occupancy": {
                    "current_bytes": self.framing_buffer_current_bytes,
                    "high_water_bytes": self.framing_buffer_high_water_bytes,
                    "is_queue": False,
                },
            },
            "strict_decode_and_admission": {
                "attempts": self.bridge_frames,
                "strict_decoded": strict_decoded,
                "strict_decode_rate_per_second": rate(strict_decoded),
                "accepted": self.accepted,
                "accepted_rate_per_second": rate(self.accepted),
                "rejected": self.rejected,
                "duplicates": self.duplicates,
                "service_latency_milliseconds": {
                    "quantile_method": "FIXED_HISTOGRAM_UPPER_BOUND",
                    "p50": percentile_upper_bound(0.50),
                    "p95": percentile_upper_bound(0.95),
                    "p99": percentile_upper_bound(0.99),
                    "maximum": round(
                        self.bridge_maximum_seconds * 1000.0, 3,
                    ),
                    "histogram_upper_bounds": [
                        *list(_RECEIVER_SERVICE_LATENCY_BUCKETS_MS),
                        "overflow",
                    ],
                    "histogram_counts": list(
                        self.bridge_latency_histogram,
                    ),
                },
            },
            "bridge_admission": {
                "frames": self.bridge_frames,
                "total_milliseconds": round(self.bridge_total_seconds * 1000.0, 3),
                "mean_milliseconds": round(
                    self.bridge_total_seconds * 1000.0 / self.bridge_frames, 3,
                ) if self.bridge_frames else 0.0,
                "maximum_milliseconds": round(self.bridge_maximum_seconds * 1000.0, 3),
            },
            "downstream_composite": {
                "callbacks": self.downstream_callbacks,
                "total_milliseconds": round(self.downstream_total_seconds * 1000.0, 3),
                "mean_milliseconds": round(
                    self.downstream_total_seconds * 1000.0 / self.downstream_callbacks, 3,
                ) if self.downstream_callbacks else 0.0,
                "maximum_milliseconds": round(self.downstream_maximum_seconds * 1000.0, 3),
            },
            "accepted_span_seconds": round(span, 3),
            "accepted_rate_per_second": round(self.accepted / span, 3) if span > 0 else None,
        }

    def reject(self, error: NinjaTraderObservationError) -> None:
        self.rejected += 1
        self.error_codes[error.code.value] = self.error_codes.get(error.code.value, 0) + 1
        detail = error.detail or "unspecified"
        self.error_details[detail] = self.error_details.get(detail, 0) + 1

    def set_health(self, tracker: NinjaTraderHealthTracker) -> None:
        self.set_health_snapshot(tracker.snapshot())

    def set_health_snapshot(self, snapshot: NinjaTraderProviderHealth) -> None:
        self.health = {
            stream.value: snapshot.streams[stream].value
            for stream in NinjaTraderHealthStream
        }
        local_bridge = self.health[NinjaTraderHealthStream.LOCAL_BRIDGE.value]
        if not self.local_bridge_transitions or self.local_bridge_transitions[-1] != local_bridge:
            self.local_bridge_transitions.append(local_bridge)

    def safe_report(self) -> dict[str, object]:
        types = set(self.observation_types)
        lucid = self.aliases.get("Lucid25kflex01") == AccountClass.PROVIDER_EVALUATION.value
        sim = self.aliases.get("Sim101") == AccountClass.LOCAL_SIMULATION.value
        lucid_snapshots = self.snapshot_counts.get("Lucid25kflex01", {})
        position_count = lucid_snapshots.get("POSITION")
        order_count = lucid_snapshots.get("ORDER")
        return {
            "schema": "lane-iii-phase-f3-ninjatrader-commissioning-v1",
            "listener": {"host": LOOPBACK_HOST, "port": self.listener_port, "ready": self.listener_ready},
            "accepted_observations": self.accepted,
            "rejected_observations": self.rejected,
            "duplicate_observations": self.duplicates,
            "observation_types": dict(sorted(self.observation_types.items())),
            "account_binding": {"lucid_alias_identified": lucid, "sim101_identified_separately": sim},
            "market_data": {"quotes": "QUOTE" in types, "trades": "TRADE" in types, "depth": "DEPTH" in types},
            "state": {
                "account": "ACCOUNT" in types,
                "position": "POSITION" in types,
                "orders": "ORDER" in types,
                "executions": "EXECUTION" in types,
                "lucid_position_truth": "UNAVAILABLE" if position_count is None else "FLAT_CONFIRMED" if position_count == 0 else "OPEN_POSITION_OBSERVED",
                "lucid_working_order_truth": "UNAVAILABLE" if order_count is None else "NONE_WORKING_CONFIRMED" if order_count == 0 else "WORKING_ORDER_OBSERVED",
            },
            # This is provider transport health, never strategy or position
            # state. In particular, healthy quotes cannot make account state
            # healthy, and a closed local socket cannot imply FLAT.
            "provider_health": {
                "streams": dict(sorted(self.health.items())),
                "local_bridge_transitions": list(self.local_bridge_transitions),
            },
            "errors": dict(sorted(self.error_codes.items())),
            "error_details": dict(sorted(self.error_details.items())),
            "pipeline_metrics": self.pipeline_report(),
            "controlled_shutdown": {
                "partial_frames_discarded": self.controlled_shutdown_partial_frames,
                "partial_bytes_discarded": self.controlled_shutdown_partial_bytes,
            },
            "authority": "OBSERVE_ONLY",
        }


@dataclass(frozen=True)
class NinjaTraderListenerRuntimeStatus:
    """Sanitized status for the GUI-owned observation worker."""

    state: str
    host: str
    port: int
    error: str | None
    start_attempts: int
    accepted_observations: int
    last_observation_at: str | None
    observation_types: Mapping[str, int]
    last_level_one_at: str | None
    last_depth_at: str | None
    observer_attachment: Mapping[str, object]
    account_balances: Mapping[str, Mapping[str, object]]
    pipeline_metrics: Mapping[str, object]
    native_transport_metrics: Mapping[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "host": self.host,
            "port": self.port,
            "error": self.error,
            "start_attempts": self.start_attempts,
            "accepted_observations": self.accepted_observations,
            "last_observation_at": self.last_observation_at,
            "observation_types": dict(sorted(self.observation_types.items())),
            "market_observer_state": "ACTIVE" if self.last_level_one_at is not None else "NOT_ACTIVE",
            "market_observer_active": self.last_level_one_at is not None,
            "market_observer_level_one_received": self.last_level_one_at is not None,
            "market_observer_depth_received": self.last_depth_at is not None,
            "last_level_one_at": self.last_level_one_at,
            "last_depth_at": self.last_depth_at,
            "observer_attachment": dict(self.observer_attachment),
            "account_balances": {
                alias: dict(values) for alias, values in self.account_balances.items()
            },
            "pipeline_metrics": dict(self.pipeline_metrics),
            "native_transport_metrics": dict(self.native_transport_metrics),
            "authority": "OBSERVE_ONLY",
        }


class NinjaTraderCommissioningHarness:
    def __init__(
        self,
        config: LoopbackBridgeConfig = LoopbackBridgeConfig(),
        *,
        attestation_key: bytes | None = None,
        require_attestation: bool = False,
        expected_source_fingerprint: str | None = None,
        on_listener_started: Callable[[str, int], None] | None = None,
        on_observation: Callable[[NinjaTraderObservation], None] | None = None,
        on_local_bridge_state: Callable[[StreamHealth], None] | None = None,
        on_rejection: Callable[[NinjaTraderObservationError], None] | None = None,
        on_duplicate: Callable[[], None] | None = None,
        fail_on_observation_error: bool = False,
    ) -> None:
        self.bridge = LoopbackNinjaTraderBridge(
            config,
            attestation_key=attestation_key,
            require_attestation=require_attestation,
            expected_source_fingerprint=expected_source_fingerprint,
        )
        self.summary = CommissioningSummary()
        self.summary.listener_port = config.port
        self.health = NinjaTraderHealthTracker()
        self._on_listener_started = on_listener_started
        # These are one-way, best-effort notifications to an external
        # composite consumer. The listener never receives a response and a
        # consumer failure can never stop the observation boundary.
        self._on_observation = on_observation
        self._on_local_bridge_state = on_local_bridge_state
        self._on_rejection = on_rejection
        self._on_duplicate = on_duplicate
        self._fail_on_observation_error = fail_on_observation_error
        self._transport_owner: object | None = None
        self._market_transport_seen = False
        self.summary.set_health(self.health)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _mark(self, stream: NinjaTraderHealthStream, state: StreamHealth, at: str | None = None) -> None:
        prior = self.health.state(stream)
        snapshot = self.health.mark(stream, state, at or self._now())
        self.summary.set_health_snapshot(snapshot)
        if (
            stream is NinjaTraderHealthStream.LOCAL_BRIDGE
            and prior is not state
            and self._on_local_bridge_state is not None
        ):
            try:
                self._on_local_bridge_state(state)
            except Exception:
                logging.getLogger(__name__).exception(
                    "NINJATRADER_OBSERVER downstream transport notification refused"
                )

    def _record_observation_health(self, observation: NinjaTraderObservation) -> None:
        at = observation.ninja_receipt_time
        # A decoded observation proves only the stream that carried it. These
        # independent marks deliberately do not infer account truth from MNQ.
        updates = [
            (NinjaTraderHealthStream.NINJATRADER_PROCESS, StreamHealth.HEALTHY),
        ]
        if observation.observation_type in {"TRADE", "QUOTE"}:
            updates.append((NinjaTraderHealthStream.MARKET_DATA_STREAM, StreamHealth.HEALTHY))
        elif observation.observation_type == "DEPTH":
            updates.append((NinjaTraderHealthStream.DEPTH_STREAM, StreamHealth.HEALTHY))
        elif observation.observation_type == "ACCOUNT":
            updates.append((NinjaTraderHealthStream.ACCOUNT_STREAM, StreamHealth.HEALTHY))
        elif observation.observation_type == "POSITION":
            updates.append((NinjaTraderHealthStream.POSITION_STREAM, StreamHealth.HEALTHY))
        elif observation.observation_type == "ORDER":
            updates.append((NinjaTraderHealthStream.ORDER_STREAM, StreamHealth.HEALTHY))
        elif observation.observation_type == "SNAPSHOT_COMPLETE":
            scope = observation.payload.get("scope")
            if scope == "POSITION":
                updates.append((NinjaTraderHealthStream.POSITION_STREAM, StreamHealth.HEALTHY))
            elif scope == "ORDER":
                updates.append((NinjaTraderHealthStream.ORDER_STREAM, StreamHealth.HEALTHY))
        elif (
            observation.observation_type == "CONNECTION"
            and observation.account_alias == "Lucid25kflex01"
            and observation.payload.get("state") == "ACCOUNT_BOUND"
        ):
            updates.append((NinjaTraderHealthStream.LUCID_CONNECTION, StreamHealth.HEALTHY))
        snapshot = self.health.mark_many(updates, at)
        self.summary.set_health_snapshot(snapshot)

    def ingest(self, frames: Iterable[bytes], *, source_connection: object | None = None) -> CommissioningSummary:
        self.summary.listener_ready = True
        for frame in frames:
            try:
                bridge_started = time.perf_counter()
                observation = self.bridge.accept_observation(frame)
                self.summary.record_bridge(time.perf_counter() - bridge_started)
                self.summary.accept(observation)
                if observation is None:
                    if self._on_duplicate is not None:
                        try:
                            self._on_duplicate()
                        except Exception:
                            logging.getLogger(__name__).exception(
                                "NINJATRADER_OBSERVER downstream duplicate notification refused"
                            )
                    continue
                if source_connection is not None:
                    market = observation.observation_type in {"TRADE", "QUOTE", "DEPTH"}
                    if market or not self._market_transport_seen:
                        self._transport_owner = source_connection
                        self._market_transport_seen = self._market_transport_seen or market
                        if self.summary.health.get("LOCAL_BRIDGE") != StreamHealth.HEALTHY.value:
                            self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.HEALTHY)
                self._record_observation_health(observation)
                if self._on_observation is not None:
                    try:
                        downstream_started = time.perf_counter()
                        self._on_observation(observation)
                        self.summary.record_downstream(time.perf_counter() - downstream_started)
                    except Exception as error:
                        if self._fail_on_observation_error:
                            raise RuntimeError(
                                "NINJATRADER_REQUIRED_OBSERVATION_SINK_FAILED"
                            ) from error
                        logging.getLogger(__name__).exception(
                            "NINJATRADER_OBSERVER downstream observation notification refused"
                        )
            except NinjaTraderObservationError as error:
                if "bridge_started" in locals():
                    self.summary.record_bridge(time.perf_counter() - bridge_started)
                self.summary.reject(error)
                if self._on_rejection is not None:
                    try:
                        self._on_rejection(error)
                    except Exception:
                        logging.getLogger(__name__).exception(
                            "NINJATRADER_OBSERVER downstream rejection notification refused"
                        )
                if self._fail_on_observation_error:
                    # The qualifying receiver owns a required, authenticated
                    # observation boundary. Persist the first rejection above,
                    # then fail the listener closed instead of accepting an
                    # unbounded stream of identically invalid frames.
                    raise RuntimeError(
                        "NINJATRADER_REQUIRED_OBSERVATION_REJECTED:"
                        f"{error.code.value}:{error.detail}"
                    ) from error
        return self.summary

    def run(self, duration_seconds: float, *, stop_event: threading.Event | None = None) -> CommissioningSummary:
        if duration_seconds <= 0:
            raise ValueError("Commissioning duration must be positive.")
        listener = self.bridge.open_listener()
        connections: dict[socket.socket, bytearray] = {}
        framing_buffer_bytes = 0

        def close_connection(connection: socket.socket, *, admit_partial: bool = True) -> None:
            nonlocal framing_buffer_bytes
            data = connections.pop(connection, bytearray())
            framing_buffer_bytes -= len(data)
            self.summary.record_framing_buffer(framing_buffer_bytes)
            if data:
                if admit_partial:
                    self.summary.record_frame_extracted(len(data))
                    self.ingest((bytes(data),), source_connection=connection)
                else:
                    # Local controlled shutdown can interrupt the sender in
                    # the middle of a newline-delimited frame. It is not a
                    # provider payload and must not be relabelled as one.
                    self.summary.controlled_shutdown_partial_frames += 1
                    self.summary.controlled_shutdown_partial_bytes += len(data)
            try:
                connection.close()
            except OSError:
                # A reset peer may already have invalidated its local socket.
                # Cleanup and provider-health transitions must still complete.
                pass
            finally:
                # A peer disconnect is an explicit transport condition. It
                # does not mutate account/position/order reconciliation.
                # A retired/unverified peer cannot disconnect the current
                # authenticated market owner. Account/control sockets do not
                # keep market transport healthy after its owner disconnects.
                if connection is self._transport_owner or not connections:
                    if connection is self._transport_owner:
                        self._transport_owner = None
                    self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.DISCONNECTED)

        try:
            self.summary.begin_receiver()
            listener.setblocking(False)
            self.summary.listener_ready = True
            if self._on_listener_started is not None:
                self._on_listener_started(LOOPBACK_HOST, self.bridge.config.port)
            self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.CONNECTING)
            deadline = time.monotonic() + duration_seconds
            while time.monotonic() < deadline and (stop_event is None or not stop_event.is_set()):
                readable = [listener, *connections]
                ready, _, _ = select.select(readable, [], [], min(0.25, deadline - time.monotonic()))
                if not ready:
                    continue
                for source in ready:
                    if source is listener:
                        while True:
                            try:
                                connection, remote = listener.accept()
                            except BlockingIOError:
                                # A local client may disconnect between select()
                                # and accept(). Keep the listener available.
                                break
                            if remote[0] != LOOPBACK_HOST:
                                connection.close()
                                continue
                            connection.setblocking(False)
                            connections[connection] = bytearray()
                            # Socket acceptance alone is not authenticated
                            # producer evidence. Admission owns this transition.
                        continue
                    try:
                        buffered = len(connections[source])
                        # Read at most the configured frame allowance plus one
                        # byte. This bounds even newline-free hostile input.
                        chunk = source.recv(min(4096, self.bridge.config.maximum_frame_bytes + 1 - buffered))
                    except BlockingIOError:
                        self.summary.record_receive(0)
                        continue
                    except OSError:
                        self.summary.record_receive(0)
                        # NinjaTrader may terminate or reset a client socket
                        # without a graceful FIN. That is a client transport
                        # transition, not a failure of the listener owner.
                        close_connection(source)
                        continue
                    self.summary.record_receive(len(chunk))
                    if not chunk:
                        close_connection(source)
                        continue
                    data = connections[source]
                    data.extend(chunk)
                    framing_buffer_bytes += len(chunk)
                    self.summary.record_framing_buffer(framing_buffer_bytes)
                    while b"\n" in data:
                        frame, _, remainder = data.partition(b"\n")
                        connections[source] = data = bytearray(remainder)
                        framing_buffer_bytes -= len(frame) + 1
                        self.summary.record_frame_extracted(len(frame))
                        self.summary.record_framing_buffer(
                            framing_buffer_bytes,
                        )
                        self.ingest((bytes(frame),), source_connection=source)
                    if len(data) > self.bridge.config.maximum_frame_bytes:
                        self.summary.record_frame_extracted(len(data))
                        self.ingest((bytes(data),), source_connection=source)
                        framing_buffer_bytes -= len(data)
                        connections[source] = bytearray()
                        self.summary.record_framing_buffer(
                            framing_buffer_bytes,
                        )
                        close_connection(source)
        finally:
            for connection in tuple(connections):
                close_connection(connection, admit_partial=False)
            listener.close()
            # Stopping the listener ends the observation transport even when
            # no client ever connected. It must not remain CONNECTING in a
            # persisted commissioning report.
            self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.DISCONNECTED)
            self.summary.finish_receiver()
        return self.summary

    def run_until_stopped(self, stop_event: threading.Event) -> CommissioningSummary:
        """Run the exact receiver loop used for commissioning until stopped."""
        return self.run(365.0 * 24.0 * 60.0 * 60.0, stop_event=stop_event)


class NinjaTraderListenerWorker:
    """One managed, read-only GUI/runtime owner for the existing receiver loop."""

    _STARTUP_TIMEOUT_SECONDS = 5.0
    _SHUTDOWN_TIMEOUT_SECONDS = 5.0

    def __init__(
        self,
        config: LoopbackBridgeConfig = LoopbackBridgeConfig(),
        *,
        logger: logging.Logger | None = None,
        attestation_key: bytes | None = None,
        require_attestation: bool = False,
        expected_source_fingerprint: str | None = None,
        on_observation: Callable[[NinjaTraderObservation], None] | None = None,
        on_local_bridge_state: Callable[[StreamHealth], None] | None = None,
        on_rejection: Callable[[NinjaTraderObservationError], None] | None = None,
        on_duplicate: Callable[[], None] | None = None,
    ) -> None:
        self.config = config
        self._logger = logger or logging.getLogger(__name__)
        if require_attestation and attestation_key is None:
            raise ValueError("Required NinjaTrader attestation needs a local verification key.")
        self._attestation_key = attestation_key
        self._require_attestation = require_attestation
        self._expected_source_fingerprint = expected_source_fingerprint
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._finished_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._harness: NinjaTraderCommissioningHarness | None = None
        self._on_observation = on_observation
        self._on_local_bridge_state = on_local_bridge_state
        self._on_rejection = on_rejection
        self._on_duplicate = on_duplicate
        self._state = "NEW"
        self._error: str | None = None
        self._start_attempts = 0
        self._accepted_observations = 0
        self._last_observation_at: str | None = None
        self._observation_types: dict[str, int] = {}
        self._last_level_one_at: str | None = None
        self._last_depth_at: str | None = None
        self._observer_attachment: dict[str, object] = {
            "state": "UNKNOWN",
            "configured_instrument": None,
            "instrument": None,
            "chart_found": False,
            "observer_attached": False,
            "subscription_mode": None,
            "observed_at": None,
        }
        self._account_balances = _empty_account_balances()
        self._native_transport_metrics: dict[str, object] = {}

    def status(self) -> NinjaTraderListenerRuntimeStatus:
        with self._lock:
            return NinjaTraderListenerRuntimeStatus(
                state=self._state,
                host=self.config.host,
                port=self.config.port,
                error=self._error,
                start_attempts=self._start_attempts,
                accepted_observations=self._accepted_observations,
                last_observation_at=self._last_observation_at,
                observation_types=dict(self._observation_types),
                last_level_one_at=self._last_level_one_at,
                last_depth_at=self._last_depth_at,
                observer_attachment=dict(self._observer_attachment),
                account_balances={alias: dict(values) for alias, values in self._account_balances.items()},
                pipeline_metrics=(
                    self._harness.summary.pipeline_report()
                    if self._harness is not None
                    else {}
                ),
                native_transport_metrics=dict(
                    self._native_transport_metrics,
                ),
            )

    def _record_native_transport_metrics(
        self,
        observation: NinjaTraderObservation,
    ) -> None:
        """Retain only the fixed aggregate native transport telemetry model."""
        if observation.observation_type != "HEALTH":
            return
        payload = observation.payload
        if (
            payload.get("native_transport_telemetry_schema")
            != _NATIVE_TRANSPORT_TELEMETRY_SCHEMA
        ):
            return
        started_at = payload.get("native_telemetry_started_utc")
        cpu_supported = payload.get("native_callback_cpu_cycles_supported")
        if not isinstance(started_at, str) or type(cpu_supported) is not bool:
            return
        sanitized: dict[str, object] = {
            "native_transport_telemetry_schema": (
                _NATIVE_TRANSPORT_TELEMETRY_SCHEMA
            ),
            "native_telemetry_started_utc": started_at,
            "native_callback_cpu_cycles_supported": cpu_supported,
            "observed_at": observation.ninja_receipt_time,
        }
        for key in _NATIVE_TRANSPORT_INTEGER_FIELDS:
            value = payload.get(key)
            if type(value) is int and value >= 0:
                sanitized[key] = value
        for key in _NATIVE_TRANSPORT_NUMBER_FIELDS:
            value = payload.get(key)
            if (
                not isinstance(value, bool)
                and isinstance(value, (int, float))
                and math.isfinite(float(value))
                and float(value) >= 0.0
            ):
                sanitized[key] = float(value)
        self._native_transport_metrics = sanitized

    def _record_observer_attachment(self, observation: NinjaTraderObservation) -> None:
        """Retain only the fixed, observation-only chart attachment model."""
        if observation.observation_type != "HEALTH":
            return
        payload = observation.payload
        if payload.get("component") != "MARKET_OBSERVER_ATTACHMENT":
            return
        state = payload.get("state")
        configured = payload.get("configured_instrument")
        instrument = payload.get("instrument")
        chart_found = payload.get("chart_found")
        observer_attached = payload.get("observer_attached")
        subscription_mode = payload.get("subscription_mode")
        if (
            state not in {
                "CONFIGURED_INSTRUMENT_UNRESOLVED", "CHART_NOT_FOUND",
                "WRONG_CHART_INSTRUMENT", "OBSERVER_MISSING", "OBSERVER_ATTACHED",
                "OBSERVER_TERMINATED", "NATIVE_ADDON_OBSERVER_ACTIVE",
                "NATIVE_ADDON_OBSERVER_FAILED",
            }
            or (configured is not None and not isinstance(configured, str))
            or (instrument is not None and not isinstance(instrument, str))
            or type(chart_found) is not bool
            or type(observer_attached) is not bool
            or subscription_mode not in {None, "CHART_INDICATOR", "NATIVE_ADDON"}
        ):
            return
        self._observer_attachment = {
            "state": state,
            "configured_instrument": configured,
            "instrument": instrument,
            "chart_found": chart_found,
            "observer_attached": observer_attached,
            "subscription_mode": subscription_mode,
            "observed_at": observation.ninja_receipt_time,
        }

    def _record_account_balance(self, observation: NinjaTraderObservation) -> None:
        """Keep only finite, read-only account values from the exact two aliases."""
        if observation.observation_type != "ACCOUNT" or observation.account_alias not in _DISPLAY_ACCOUNT_CLASSES:
            return
        item = observation.payload.get("item")
        field = _ACCOUNT_BALANCE_FIELD_BY_ITEM.get(item) if isinstance(item, str) else None
        value = observation.payload.get("value")
        if field is None or isinstance(value, bool) or not isinstance(value, (int, float)):
            return
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            return
        account = self._account_balances[observation.account_alias]
        account[field] = numeric_value
        account[f"{field}_observed_at"] = observation.ninja_receipt_time

    def _record_and_forward_observation(self, observation: NinjaTraderObservation) -> None:
        with self._lock:
            self._accepted_observations += 1
            self._last_observation_at = observation.ninja_receipt_time
            kind = observation.observation_type
            self._observation_types[kind] = self._observation_types.get(kind, 0) + 1
            if kind in {"QUOTE", "TRADE"}:
                self._last_level_one_at = observation.ninja_receipt_time
            elif kind == "DEPTH":
                self._last_depth_at = observation.ninja_receipt_time
            self._record_observer_attachment(observation)
            self._record_account_balance(observation)
            self._record_native_transport_metrics(observation)
            callback = self._on_observation
        if callback is not None:
            callback(observation)

    def set_observation_sinks(
        self,
        *,
        on_observation: Callable[[NinjaTraderObservation], None],
        on_local_bridge_state: Callable[[StreamHealth], None],
        on_rejection: Callable[[NinjaTraderObservationError], None],
        on_duplicate: Callable[[], None],
        fail_on_observation_error: bool = False,
    ) -> None:
        """Attach the sole downstream composite consumer before startup.

        This is intentionally unavailable after startup so UI reconnects and
        runtime churn cannot replace or duplicate a consumer mid-stream.
        """
        if not all(callable(value) for value in (on_observation, on_local_bridge_state, on_rejection, on_duplicate)):
            raise ValueError("Shadow sinks must be callable.")
        with self._lock:
            if self._state not in {"NEW", "STOPPED"} or (self._thread is not None and self._thread.is_alive()):
                raise RuntimeError("NINJATRADER_OBSERVER sinks may be attached only before startup")
            self._on_observation = on_observation
            self._on_local_bridge_state = on_local_bridge_state
            self._on_rejection = on_rejection
            self._on_duplicate = on_duplicate
            self._fail_on_observation_error = fail_on_observation_error

    def set_shadow_sinks(
        self,
        *,
        on_observation: Callable[[NinjaTraderObservation], None],
        on_local_bridge_state: Callable[[StreamHealth], None],
        on_rejection: Callable[[NinjaTraderObservationError], None],
        on_duplicate: Callable[[], None],
    ) -> None:
        """Backward-compatible name for the pre-L3G single-shadow wiring."""
        self.set_observation_sinks(
            on_observation=on_observation,
            on_local_bridge_state=on_local_bridge_state,
            on_rejection=on_rejection,
            on_duplicate=on_duplicate,
        )

    def start(self, *, timeout_seconds: float = _STARTUP_TIMEOUT_SECONDS) -> NinjaTraderListenerRuntimeStatus:
        """Start once, wait for a bind result, and never select an alternate port."""
        if timeout_seconds <= 0:
            raise ValueError("Listener startup timeout must be positive.")
        with self._lock:
            if self._state in {"STARTING", "LISTENING"}:
                return self.status()
            if self._thread is not None and self._thread.is_alive():
                return self.status()
            self._stop_event = threading.Event()
            self._ready_event = threading.Event()
            self._finished_event = threading.Event()
            self._error = None
            self._state = "STARTING"
            self._start_attempts += 1
            self._accepted_observations = 0
            self._last_observation_at = None
            self._observation_types = {}
            self._last_level_one_at = None
            self._last_depth_at = None
            self._observer_attachment = {
                "state": "UNKNOWN",
                "configured_instrument": None,
                "instrument": None,
                "chart_found": False,
                "observer_attached": False,
                "subscription_mode": None,
                "observed_at": None,
            }
            self._account_balances = _empty_account_balances()
            self._native_transport_metrics = {}
            self._harness = NinjaTraderCommissioningHarness(
                self.config,
                attestation_key=self._attestation_key,
                require_attestation=self._require_attestation,
                expected_source_fingerprint=self._expected_source_fingerprint,
                on_listener_started=self._listener_started,
                on_observation=self._record_and_forward_observation,
                on_local_bridge_state=self._on_local_bridge_state,
                on_rejection=self._on_rejection,
                on_duplicate=self._on_duplicate,
                fail_on_observation_error=getattr(self, "_fail_on_observation_error", False),
            )
            self._thread = threading.Thread(
                target=self._run,
                name="NINJATRADER_OBSERVER",
                daemon=False,
            )
            self._thread.start()
        if not self._ready_event.wait(timeout_seconds):
            timed_out = False
            with self._lock:
                # The bind callback may have won the race immediately after
                # wait() expired. Never overwrite a real LISTENING result.
                if self._state == "STARTING":
                    self._state = "FAILED"
                    self._error = "listener_start_timeout"
                    timed_out = True
            if timed_out:
                self._logger.error(
                    "NINJATRADER_OBSERVER FAILED %s:%s listener_start_timeout",
                    self.config.host,
                    self.config.port,
                )
                self.stop(timeout_seconds=self._SHUTDOWN_TIMEOUT_SECONDS)
        return self.status()

    def stop(self, *, timeout_seconds: float = _SHUTDOWN_TIMEOUT_SECONDS) -> NinjaTraderListenerRuntimeStatus:
        """Request a clean receiver-loop exit and wait for its socket to close."""
        if timeout_seconds <= 0:
            raise ValueError("Listener shutdown timeout must be positive.")
        with self._lock:
            if self._state == "NEW":
                self._state = "STOPPED"
                return self.status()
            self._stop_event.set()
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout_seconds)
        with self._lock:
            if thread is not None and thread.is_alive():
                self._state = "FAILED"
                self._error = "listener_shutdown_timeout"
                self._logger.error(
                    "NINJATRADER_OBSERVER FAILED %s:%s listener_shutdown_timeout",
                    self.config.host,
                    self.config.port,
                )
            elif self._state != "FAILED":
                self._state = "STOPPED"
            return self.status()

    def _listener_started(self, host: str, port: int) -> None:
        with self._lock:
            if self._state != "STARTING" or self._stop_event.is_set():
                return
            self._state = "LISTENING"
            self._error = None
            self._ready_event.set()
        self._logger.info("NINJATRADER_OBSERVER LISTENING %s:%s", host, port)

    def _run(self) -> None:
        harness = self._harness
        assert harness is not None
        try:
            harness.run_until_stopped(self._stop_event)
        except Exception as exc:
            detail = f"{type(exc).__name__}: {exc}"
            with self._lock:
                self._state = "FAILED"
                self._error = detail
                self._ready_event.set()
            self._logger.error(
                "NINJATRADER_OBSERVER FAILED %s:%s %s",
                self.config.host,
                self.config.port,
                detail,
            )
        finally:
            with self._lock:
                if self._state not in {"FAILED", "STOPPED"}:
                    self._state = "STOPPED"
                self._finished_event.set()
