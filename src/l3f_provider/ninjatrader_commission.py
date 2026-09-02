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
    NinjaTraderObservationError,
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

    def accept(self, observation: NinjaTraderObservation | None) -> None:
        if observation is None:
            self.duplicates += 1
            return
        self.accepted += 1
        self.observation_types[observation.observation_type] = self.observation_types.get(observation.observation_type, 0) + 1
        if observation.account_alias is not None and observation.account_class is not None:
            self.aliases[observation.account_alias] = observation.account_class.value
        if observation.observation_type == "SNAPSHOT_COMPLETE" and observation.account_alias is not None:
            scope = observation.payload.get("scope")
            count_key = "open_count" if scope == "POSITION" else "working_count" if scope == "ORDER" else None
            count = None if count_key is None else observation.payload.get(count_key)
            if type(count) is int and count >= 0:
                self.snapshot_counts.setdefault(observation.account_alias, {})[str(scope)] = count

    def reject(self, error: NinjaTraderObservationError) -> None:
        self.rejected += 1
        self.error_codes[error.code.value] = self.error_codes.get(error.code.value, 0) + 1
        detail = error.detail or "unspecified"
        self.error_details[detail] = self.error_details.get(detail, 0) + 1

    def set_health(self, tracker: NinjaTraderHealthTracker) -> None:
        snapshot = tracker.snapshot()
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
            "authority": "OBSERVE_ONLY",
        }


class NinjaTraderCommissioningHarness:
    def __init__(
        self,
        config: LoopbackBridgeConfig = LoopbackBridgeConfig(),
        *,
        on_listener_started: Callable[[str, int], None] | None = None,
        on_observation: Callable[[NinjaTraderObservation], None] | None = None,
        on_local_bridge_state: Callable[[StreamHealth], None] | None = None,
        on_rejection: Callable[[NinjaTraderObservationError], None] | None = None,
        on_duplicate: Callable[[], None] | None = None,
    ) -> None:
        self.bridge = LoopbackNinjaTraderBridge(config)
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
        self.summary.set_health(self.health)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _mark(self, stream: NinjaTraderHealthStream, state: StreamHealth, at: str | None = None) -> None:
        prior = self.health.snapshot().streams[stream]
        self.health.mark(stream, state, at or self._now())
        self.summary.set_health(self.health)
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
        self._mark(NinjaTraderHealthStream.NINJATRADER_PROCESS, StreamHealth.HEALTHY, at)
        if observation.observation_type in {"TRADE", "QUOTE"}:
            self._mark(NinjaTraderHealthStream.MARKET_DATA_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "DEPTH":
            self._mark(NinjaTraderHealthStream.DEPTH_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "ACCOUNT":
            self._mark(NinjaTraderHealthStream.ACCOUNT_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "POSITION":
            self._mark(NinjaTraderHealthStream.POSITION_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "ORDER":
            self._mark(NinjaTraderHealthStream.ORDER_STREAM, StreamHealth.HEALTHY, at)
        elif observation.observation_type == "SNAPSHOT_COMPLETE":
            scope = observation.payload.get("scope")
            if scope == "POSITION":
                self._mark(NinjaTraderHealthStream.POSITION_STREAM, StreamHealth.HEALTHY, at)
            elif scope == "ORDER":
                self._mark(NinjaTraderHealthStream.ORDER_STREAM, StreamHealth.HEALTHY, at)
        elif (
            observation.observation_type == "CONNECTION"
            and observation.account_alias == "Lucid25kflex01"
            and observation.payload.get("state") == "ACCOUNT_BOUND"
        ):
            self._mark(NinjaTraderHealthStream.LUCID_CONNECTION, StreamHealth.HEALTHY, at)

    def ingest(self, frames: Iterable[bytes]) -> CommissioningSummary:
        self.summary.listener_ready = True
        for frame in frames:
            try:
                observation = self.bridge.accept_observation(frame)
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
                self._record_observation_health(observation)
                if self._on_observation is not None:
                    try:
                        self._on_observation(observation)
                    except Exception:
                        logging.getLogger(__name__).exception(
                            "NINJATRADER_OBSERVER downstream observation notification refused"
                        )
            except NinjaTraderObservationError as error:
                self.summary.reject(error)
                if self._on_rejection is not None:
                    try:
                        self._on_rejection(error)
                    except Exception:
                        logging.getLogger(__name__).exception(
                            "NINJATRADER_OBSERVER downstream rejection notification refused"
                        )
        return self.summary

    def run(self, duration_seconds: float, *, stop_event: threading.Event | None = None) -> CommissioningSummary:
        if duration_seconds <= 0:
            raise ValueError("Commissioning duration must be positive.")
        listener = self.bridge.open_listener()
        connections: dict[socket.socket, bytearray] = {}

        def close_connection(connection: socket.socket) -> None:
            data = connections.pop(connection, bytearray())
            if data:
                self.ingest((bytes(data),))
            try:
                connection.close()
            except OSError:
                # A reset peer may already have invalidated its local socket.
                # Cleanup and provider-health transitions must still complete.
                pass
            finally:
                # A peer disconnect is an explicit transport condition. It
                # does not mutate account/position/order reconciliation.
                self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.DISCONNECTED)

        try:
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
                            self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.HEALTHY)
                        continue
                    try:
                        buffered = len(connections[source])
                        # Read at most the configured frame allowance plus one
                        # byte. This bounds even newline-free hostile input.
                        chunk = source.recv(min(4096, self.bridge.config.maximum_frame_bytes + 1 - buffered))
                    except BlockingIOError:
                        continue
                    except OSError:
                        # NinjaTrader may terminate or reset a client socket
                        # without a graceful FIN. That is a client transport
                        # transition, not a failure of the listener owner.
                        close_connection(source)
                        continue
                    if not chunk:
                        close_connection(source)
                        continue
                    data = connections[source]
                    data.extend(chunk)
                    while b"\n" in data:
                        frame, _, remainder = data.partition(b"\n")
                        connections[source] = data = bytearray(remainder)
                        self.ingest((bytes(frame),))
                    if len(data) > self.bridge.config.maximum_frame_bytes:
                        self.ingest((bytes(data),))
                        connections[source] = bytearray()
                        close_connection(source)
        finally:
            for connection in tuple(connections):
                close_connection(connection)
            listener.close()
            # Stopping the listener ends the observation transport even when
            # no client ever connected. It must not remain CONNECTING in a
            # persisted commissioning report.
            self._mark(NinjaTraderHealthStream.LOCAL_BRIDGE, StreamHealth.DISCONNECTED)
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
        on_observation: Callable[[NinjaTraderObservation], None] | None = None,
        on_local_bridge_state: Callable[[StreamHealth], None] | None = None,
        on_rejection: Callable[[NinjaTraderObservationError], None] | None = None,
        on_duplicate: Callable[[], None] | None = None,
    ) -> None:
        self.config = config
        self._logger = logger or logging.getLogger(__name__)
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
            )

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
            self._harness = NinjaTraderCommissioningHarness(
                self.config,
                on_listener_started=self._listener_started,
                on_observation=self._record_and_forward_observation,
                on_local_bridge_state=self._on_local_bridge_state,
                on_rejection=self._on_rejection,
                on_duplicate=self._on_duplicate,
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
