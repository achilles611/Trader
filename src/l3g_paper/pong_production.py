"""Lifespan-owned production host for the bounded Sim101 Pong experiment.

The pure Pong, Discernment, DailyBias, and controller modules intentionally own
no transport.  This module is the one production composition root: it admits
authenticated native inputs, evaluates the frozen adapters, preserves a
portable qualifying-hour certificate, binds one durable runtime bridge, and
hosts the independently persisted one-hour controller.  It never grants live
authority and never treats accelerated fixtures as market history.
"""

from __future__ import annotations

from collections import deque
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import threading
import time
from typing import Callable, Mapping
from uuid import uuid4

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.lane_iii.contracts import canonical_hash, normalized_utc

from .contracts import PONG_ENTRY_PROFILE_VERSION
from .daily_bias import (
    BiasUpdateReason,
    ClassifiedSide,
    ClassifiedTradeInput,
    CompletedHourlyOrderFlow,
    DailyBiasEngine,
    DailyBiasResult,
    HourlyBiasFrame,
    HourlyOrderFlowEvaluator,
    default_unconfigured_bias,
)
from .discernment import DiscernmentEngine
from .pong import PongActionKind, PongStateMachine
from .pong_controller import (
    PONG_ACTIVATION_DATE_LOCAL,
    PongBoundedController,
    PongControllerCommand,
    PongControllerObservation,
    PongControllerPhase,
)
from .pong_experiment import (
    CompletedPongHour,
    hourly_structure_components,
    unavailable_nonvoting_options,
)
from .pong_inputs import PongCausalInputAdapter, PongNativeInputRefused, PongWarmStart
from .pong_microstructure import (
    AggressorProvenance,
    AggressorSide,
    BookSide,
    CausalEventRef,
    ClassifiedFlowAdapter,
    ClassifiedTrade,
    DepthEventKind,
    DepthObservation,
    LiquidityClusterAdapter,
    MicrostructureEvidenceMapper,
    PriceResponse,
    VolumeRegimeAdapter,
    VolumeWindow,
    VolumeWindowKind,
)
from .pong_quantity import PongQuantityPolicy, PongReconciliationProof
from .pong_runtime import (
    PongBridgeUpdate,
    PongLoadedRuntimeIdentity,
    PongNativeDirective,
    PongNativeDirectiveKind,
    PongNativeEventProof,
    PongRuntimeBridge,
)


PONG_PRODUCTION_HOST_SCHEMA = "beelzebub-pong-production-host-v1"
PONG_QUALIFYING_HOUR_SCHEMA = "beelzebub-pong-qualified-hour-certificate-v1"
PONG_PRODUCTION_EXPERIMENT_ID = "pong-ny-open-20260911"
_RUNNING_CONTROLLER_PHASES = {
    PongControllerPhase.RUNNING_PRE_BLACKOUT,
    PongControllerPhase.RUNNING_POST_BLACKOUT,
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _time(value: object) -> datetime:
    return datetime.fromisoformat(
        normalized_utc(str(value), "Pong production time").replace("Z", "+00:00")
    )


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


def _atomic_write(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
    raw = _canonical(value)
    try:
        with temporary.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


class PongProductionHost:
    """One process owner for native Pong inputs, bridge, and controller."""

    def __init__(
        self,
        state_root: str | Path,
        *,
        selected_profile: str,
        expected_market_source_fingerprint: str,
        runtime_snapshot: Callable[[], Mapping[str, object]],
        collector_status: Callable[[], Mapping[str, object]],
        controller_dispatch: Callable[[str, str], bool],
        native_dispatch: Callable[
            [PongNativeDirective, Mapping[str, object]], Mapping[str, object]
        ],
        activation_authorized: bool = False,
    ) -> None:
        if not callable(native_dispatch):
            raise TypeError("Pong production host requires one native dispatcher.")
        self.state_root = Path(state_root).resolve()
        self.selected_profile = selected_profile
        self.expected_market_source_fingerprint = expected_market_source_fingerprint
        self._runtime_snapshot = runtime_snapshot
        self._collector_status = collector_status
        self._controller_dispatch = controller_dispatch
        self._native_dispatch = native_dispatch
        self.activation_authorized = activation_authorized
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._process_instance_id = "pong-controller-process-" + uuid4().hex
        self._controller: PongBoundedController | None = None
        self._controller_error: str | None = None
        self._bridge: PongRuntimeBridge | None = None
        self._bridge_error: str | None = None
        self._pending_native: deque[
            tuple[PongNativeDirective, dict[str, object]]
        ] = deque()
        self._directive_command_ids: dict[str, str] = {}
        self._command_directive_ids: dict[str, str] = {}
        self._active_directive_ids: dict[PongNativeDirectiveKind, str] = {}
        self._runtime: dict[str, object] = {}
        self._collection: dict[str, object] = {}
        self._warm_start: PongWarmStart | None = None
        self._retained_hour_seed = False
        self._input_adapter: PongCausalInputAdapter | None = None
        self._input_error: str | None = None
        self._producer: tuple[str, str, str] | None = None
        self._stream_sequences: dict[str, int] = {}
        self._stream_counts = {name: 0 for name in ("DEPTH", "QUOTE", "TRADE")}
        self._stream_missing = {name: 0 for name in ("DEPTH", "QUOTE", "TRADE")}
        self._stream_out_of_order = {name: 0 for name in ("DEPTH", "QUOTE", "TRADE")}
        self._classified_hour: list[ClassifiedTradeInput] = []
        self._hour_pair: tuple[CompletedPongHour, CompletedPongHour] | None = None
        self._daily_bias: DailyBiasResult = default_unconfigured_bias()
        self._certificate: dict[str, object] | None = None
        self._certificate_error: str | None = None
        self._trades: deque[ClassifiedTrade] = deque(maxlen=8192)
        self._depth = {
            BookSide.BID: deque(maxlen=4096),
            BookSide.ASK: deque(maxlen=4096),
        }
        self._volume_buckets: dict[str, Decimal] = {}
        self._volume_windows: dict[str, VolumeWindow] = {}
        self._last_microstructure_at: datetime | None = None
        self._microstructure_blockers: tuple[str, ...] = (
            "PONG_MICROSTRUCTURE_WAITING_FOR_NATIVE_INPUT",
        )
        self._mapper = MicrostructureEvidenceMapper()
        self._discernment = DiscernmentEngine(self._mapper.policy.discernment_policy())
        self._flow = ClassifiedFlowAdapter()
        self._liquidity = LiquidityClusterAdapter()
        self._volume = VolumeRegimeAdapter()
        self._load_certificate()

    @property
    def certificate_path(self) -> Path:
        return self.state_root / "qualified-hour.json"

    @property
    def controller_path(self) -> Path:
        return self.state_root / "controller-state.json"

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="PONG_PRODUCTION_CONTROLLER_HOST",
                daemon=True,
            )
            self._thread.start()

    def stop(self, timeout_seconds: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout_seconds)

    def _load_certificate(self) -> None:
        path = self.certificate_path
        if not path.exists():
            return
        try:
            raw = path.read_bytes()
            value = json.loads(raw)
            if type(value) is not dict or raw != _canonical(value):
                raise ValueError("canonical certificate")
            digest = value.get("certificate_hash")
            body = {key: item for key, item in value.items() if key != "certificate_hash"}
            if (
                value.get("schema") != PONG_QUALIFYING_HOUR_SCHEMA
                or digest != canonical_hash(body)
                or value.get("test_only") is not False
                or value.get("eligible") is not True
            ):
                raise ValueError("certificate identity")
            journal = Path(str(value.get("journal_path"))).resolve()
            if not journal.is_file():
                raise ValueError("certificate journal missing")
            self._certificate = value
            self._daily_bias = self._bias_from_certificate(value)
            self._retained_hour_seed = True
        except Exception as error:
            self._certificate_error = (
                "PONG_QUALIFYING_HOUR_CERTIFICATE_INVALID:" + type(error).__name__
            )

    def on_observation(self, record: NinjaTraderObservation) -> None:
        """Admit one already authenticated listener observation, without I/O."""
        if type(record) is not NinjaTraderObservation or record.signature_verified is not True:
            with self._lock:
                self._input_error = "PONG_NATIVE_INPUT_ATTESTATION_UNVERIFIED"
            return
        producer = (
            record.session_id,
            record.producer.source_fingerprint,
            record.producer.build_fingerprint,
        )
        with self._lock:
            if record.producer.source_fingerprint != self.expected_market_source_fingerprint:
                self._input_error = "PONG_NATIVE_INPUT_SOURCE_FINGERPRINT_MISMATCH"
                return
            if self._producer is None:
                self._producer = producer
            elif self._producer != producer:
                self._input_error = "PONG_NATIVE_INPUT_PRODUCER_CHANGED"
                return
            try:
                if record.observation_type == "HISTORICAL_WARM_START":
                    warm = PongWarmStart.from_observation(record)
                    self._warm_start = warm
                    self._input_adapter = PongCausalInputAdapter(warm)
                    return
                if record.observation_type == "HOURLY_BAR":
                    if self._input_adapter is None:
                        if self._retained_hour_seed:
                            # The hash-bound retained certificate already owns
                            # the two completed hours required for this bounded
                            # run.  A later live bar is never backfilled into it.
                            return
                        raise PongNativeInputRefused("PONG_WARM_START_MISSING")
                    self._input_adapter.admit_live_hour(record)
                    self._hour_pair = self._input_adapter.latest_hour_pair()
                    return
                if record.observation_type not in self._stream_counts:
                    return
                sequence = int(record.payload["source_event_sequence"])
                prior = self._stream_sequences.get(record.observation_type)
                if prior is not None:
                    if sequence <= prior:
                        self._stream_out_of_order[record.observation_type] += 1
                        self._input_error = "PONG_NATIVE_INPUT_DUPLICATE_OR_OUT_OF_ORDER"
                        return
                    if sequence != prior + 1:
                        self._stream_missing[record.observation_type] += sequence - prior - 1
                        self._input_error = "PONG_NATIVE_INPUT_SOURCE_SEQUENCE_GAP"
                        return
                self._stream_sequences[record.observation_type] = sequence
                self._stream_counts[record.observation_type] += 1
                if record.observation_type == "TRADE":
                    self._admit_trade(record)
                elif record.observation_type == "DEPTH":
                    self._admit_depth(record)
            except (KeyError, TypeError, ValueError, PongNativeInputRefused) as error:
                self._input_error = str(error) or type(error).__name__

    @staticmethod
    def _event_ref(record: NinjaTraderObservation) -> CausalEventRef:
        received = record.ninja_receipt_time
        event_at = record.provider_timestamp or record.exchange_timestamp or received
        if _time(event_at) > _time(received):
            event_at = received
        producer = record.producer
        return CausalEventRef(
            record.observation_id,
            record.payload_hash,
            event_at,
            received,
            received,
            record.local_monotonic_sequence,
            "LOCAL_CALLBACK_ORDER_ONLY",
            "NINJATRADER_NATIVE:"
            + record.session_id + ":" + producer.source_fingerprint + ":"
            + producer.build_fingerprint,
        )

    def _admit_trade(self, record: NinjaTraderObservation) -> None:
        payload = record.payload
        side_text = str(payload.get("aggressor_side") or "UNKNOWN").upper()
        side = AggressorSide(side_text if side_text in {"BUY", "SELL"} else "UNKNOWN")
        provenance = (
            AggressorProvenance.AUTHENTICATED_PROVIDER
            if side is not AggressorSide.UNKNOWN
            else AggressorProvenance.UNAVAILABLE
        )
        trade = ClassifiedTrade(
            self._event_ref(record), "MNQ SEP26", Decimal(str(payload["price"])),
            Decimal(str(payload["size"])), side, provenance,
        )
        self._trades.append(trade)
        event = _time(trade.event.event_at)
        bucket = event.replace(minute=(event.minute // 5) * 5, second=0, microsecond=0)
        key = bucket.isoformat().replace("+00:00", "Z")
        self._volume_buckets[key] = self._volume_buckets.get(key, Decimal("0")) + trade.volume
        collection = self._collection.get("required_interval")
        if isinstance(collection, Mapping) and side in {AggressorSide.BUY, AggressorSide.SELL}:
            start_raw, end_raw = collection.get("start_utc"), collection.get("end_utc")
            if isinstance(start_raw, str) and isinstance(end_raw, str) and (
                _time(start_raw) <= event < _time(end_raw)
            ):
                self._classified_hour.append(ClassifiedTradeInput(
                    event_id=record.observation_id,
                    payload_hash=record.payload_hash,
                    source="NINJATRADER_NATIVE_AGGRESSOR:"
                    + str(payload.get("aggressor_source") or "UNKNOWN"),
                    instrument="MNQ SEP26",
                    side=ClassifiedSide(side.value),
                    size=trade.volume,
                    event_at=trade.event.event_at,
                    observed_at=record.ninja_receipt_time,
                    available_at=record.ninja_receipt_time,
                    authenticated=True,
                ))

    def _admit_depth(self, record: NinjaTraderObservation) -> None:
        payload = record.payload
        side = BookSide.BID if str(payload.get("side")).upper() == "BID" else BookSide.ASK
        levels = payload.get("bids" if side is BookSide.BID else "asks")
        if not isinstance(levels, list) or not levels:
            return
        touch = Decimal(str(levels[0]["price"]))
        price = Decimal(str(payload["mutation_price"]))
        if (side is BookSide.BID and price > touch) or (side is BookSide.ASK and price < touch):
            return
        kind = DepthEventKind.UPDATE
        if payload.get("is_reset") is True:
            kind = DepthEventKind.RECONNECT_RESET
        nearby = sum((Decimal(str(item["size"])) for item in levels), Decimal("0"))
        self._depth[side].append(DepthObservation(
            self._event_ref(record), "MNQ SEP26", side, price,
            Decimal(str(payload.get("mutation_volume") or 0)), nearby, touch, kind,
            Decimal("0"), Decimal("0"), False, PriceResponse.UNAVAILABLE,
        ))

    def _evaluate_microstructure(self, at: datetime) -> None:
        if self._last_microstructure_at is not None and (
            at - self._last_microstructure_at < timedelta(seconds=1)
        ):
            return
        self._last_microstructure_at = at
        cutoff = at - timedelta(seconds=5)
        trades = tuple(item for item in self._trades if _time(item.event.available_at) >= cutoff)
        bids = tuple(item for item in self._depth[BookSide.BID] if _time(item.event.available_at) >= cutoff)
        asks = tuple(item for item in self._depth[BookSide.ASK] if _time(item.event.available_at) >= cutoff)
        if not trades or not bids or not asks:
            self._microstructure_blockers = ("PONG_MICROSTRUCTURE_NATIVE_WINDOW_INCOMPLETE",)
            return
        as_of = at.isoformat().replace("+00:00", "Z")
        try:
            flow = self._flow.evaluate(
                trades, window_start=cutoff.isoformat().replace("+00:00", "Z"),
                as_of=as_of, feed_continuity_valid=self._input_error is None,
            )
            bid_price = bids[-1].price
            ask_price = asks[-1].price
            bid = self._liquidity.evaluate(
                tuple(item for item in bids if item.price == bid_price),
                as_of=as_of, feed_continuity_valid=self._input_error is None,
            )
            ask = self._liquidity.evaluate(
                tuple(item for item in asks if item.price == ask_price),
                as_of=as_of, feed_continuity_valid=self._input_error is None,
            )
            bucket = at.replace(minute=(at.minute // 5) * 5, second=0, microsecond=0)
            key = bucket.isoformat().replace("+00:00", "Z")
            window = VolumeWindow(
                window_id="pong-volume-" + canonical_hash({"start": key, "at": as_of})[:32],
                payload_hash=canonical_hash({"start": key, "at": as_of, "volume": str(self._volume_buckets.get(key, 0))}),
                instrument="MNQ SEP26",
                normalization_key=f"UTC-{bucket.hour:02d}:{bucket.minute:02d}-5M",
                window_start=key,
                window_end=as_of,
                available_at=as_of,
                volume=self._volume_buckets.get(key, Decimal("0")),
                kind=VolumeWindowKind.AS_OF,
                continuity_valid=self._input_error is None,
                feed_fresh=True,
            )
            prior = tuple(
                item for item in self._volume_windows.values()
                if item.normalization_key == window.normalization_key
                and _time(item.window_end) <= bucket
            )
            regime = self._volume.evaluate(window, prior, as_of=as_of)
            mapped = self._mapper.map_frame(
                flow, bid, ask, regime,
                first_evidence_revision=self._discernment.evidence_revision + 1,
            )
            self._microstructure_blockers = mapped.blockers
            if not mapped.blockers:
                for evidence in mapped.evidence:
                    update = self._discernment.on_evidence(
                        evidence, volume_regime=mapped.volume_regime,
                    )
                    self._apply_discernment_update(update)
            # Keep completed buckets as prior-only candidates. A current partial
            # window never becomes its own historical reference.
            previous_start = bucket - timedelta(minutes=5)
            previous_key = previous_start.isoformat().replace("+00:00", "Z")
            if previous_key in self._volume_buckets and previous_key not in self._volume_windows:
                previous_end = bucket.isoformat().replace("+00:00", "Z")
                previous_volume = self._volume_buckets[previous_key]
                self._volume_windows[previous_key] = VolumeWindow(
                    "pong-volume-complete-" + canonical_hash({"start": previous_key})[:32],
                    canonical_hash({"start": previous_key, "end": previous_end, "volume": str(previous_volume)}),
                    "MNQ SEP26", f"UTC-{previous_start.hour:02d}:{previous_start.minute:02d}-5M",
                    previous_key, previous_end, as_of, previous_volume,
                    VolumeWindowKind.COMPLETED, self._input_error is None, True,
                )
        except Exception as error:
            self._microstructure_blockers = (
                "PONG_MICROSTRUCTURE_EVALUATION_BLOCKED:" + type(error).__name__,
            )

    def _apply_discernment_update(self, update: object) -> None:
        bridge = self._bridge
        if bridge is None:
            return
        snapshot = getattr(update, "snapshot", None)
        switch = getattr(update, "switch", None)
        if snapshot is None:
            return
        if bridge.machine.shadow_direction is None and self._discernment.latched_direction is not None:
            applied = bridge.apply_transition(bridge.machine.observe_startup_seed(
                self._discernment.latched_direction, snapshot,
            ))
        elif switch is None:
            applied = bridge.apply_transition(bridge.machine.on_snapshot_update(snapshot))
        else:
            controller = self._controller
            allowed = False
            if controller is not None:
                allowed = controller.assess_entry(
                    source_switch_id=switch.event_id,
                    source_switch_sequence=switch.sequence,
                    source_event_at=switch.available_at,
                    at=_now(),
                ).allowed
            candidate = bridge.machine.on_switch(
                switch, self._daily_bias, entry_gates_pass=allowed,
            )
            entry_actions = tuple(
                action for action in candidate.actions
                if action.kind in {
                    PongActionKind.REQUEST_ENTRY_LONG,
                    PongActionKind.REQUEST_ENTRY_SHORT,
                }
            )
            if entry_actions:
                action = entry_actions[0]
                now = _now()
                source_fresh = (
                    _time(now) - _time(switch.available_at)
                    <= timedelta(seconds=5)
                )
                admission = bridge.machine.on_entry_admission(
                    action_id=action.action_id,
                    bias=self._daily_bias,
                    at=now,
                    current_snapshot=switch.snapshot,
                    current_switch_cursor=switch,
                    source_fresh=source_fresh,
                    entry_gates_pass=(allowed and not self._blockers_locked()),
                    reconciliation=self._reconciliation(self._runtime),
                    available_loss_allowance_dollars=Decimal(str(
                        self._runtime.get("available_loss_allowance_dollars") or "0"
                    )),
                )
                applied = bridge.apply_transition(admission)
            else:
                applied = bridge.apply_transition(candidate)
        self._queue_bridge_update(applied, snapshot=snapshot, switch=switch)

    def _queue_bridge_update(
        self,
        update: PongBridgeUpdate,
        *,
        snapshot: object | None = None,
        switch: object | None = None,
    ) -> None:
        for directive in update.directives:
            action = None
            if self._bridge is not None:
                action = self._bridge.machine.pending_action
                if action is not None and action.action_id != directive.action_id:
                    action = None
            evidence: dict[str, object] = {
                "source_available_at": getattr(snapshot, "available_at", _now()),
                "snapshot_hash": getattr(snapshot, "result_hash", None),
                "daily_bias_result_hash": self._daily_bias.result_hash,
                "position_side_support": (
                    None if action is None else action.position_side_support
                ),
                "source_switch_id": directive.source_switch_id,
                "source_switch_sequence": directive.source_switch_sequence,
            }
            # Non-entry directives do not consume directional support, but a
            # fixed neutral value keeps the dispatch envelope canonical.
            if evidence["position_side_support"] is None:
                evidence["position_side_support"] = "0.5"
            self._pending_native.append((directive, evidence))

    def _dispatch_native(
        self,
        directive: PongNativeDirective,
        evidence: Mapping[str, object],
    ) -> None:
        with self._lock:
            bridge = self._bridge
            if bridge is None:
                return
            # Persist DISPATCHED before crossing into the runtime.  A crash
            # after this point restores as reconciliation-required instead of
            # replaying an ambiguously sent command.
            bridge.mark_dispatched(directive.directive_id)
            self._active_directive_ids[directive.kind] = directive.directive_id
        result = self._native_dispatch(directive, evidence)
        if result.get("accepted") is not True:
            raise RuntimeError("PONG_NATIVE_RUNTIME_COMMAND_REFUSED")
        command_id = result.get("command_id")
        if isinstance(command_id, str) and command_id:
            with self._lock:
                self._directive_command_ids[directive.directive_id] = command_id
                self._command_directive_ids[command_id] = directive.directive_id

    def _event_proof_locked(self, message: Mapping[str, object]) -> PongNativeEventProof:
        runtime = self._runtime
        observed_at = str(
            message.get("timestamp")
            or message.get("observed_at")
            or runtime.get("observed_at")
            or _now()
        )
        receipt_id = str(message.get("receipt_id") or "").strip()
        if not receipt_id:
            receipt_id = "pong-native-receipt-" + canonical_hash(dict(message))
        return PongNativeEventProof(
            receipt_id=receipt_id,
            observed_at=observed_at,
            source_fingerprint=str(runtime.get("addon_source_fingerprint") or ""),
            build_fingerprint=str(runtime.get("addon_build_fingerprint") or ""),
            execution_session_id=str(runtime.get("execution_session_id") or ""),
            signature_verified=True,
        )

    def _directive_for_message_locked(
        self,
        message: Mapping[str, object],
    ) -> str | None:
        command_id = message.get("command_id")
        if isinstance(command_id, str) and command_id in self._command_directive_ids:
            return self._command_directive_ids[command_id]
        role = str(message.get("order_role") or "").upper()
        state = str(message.get("order_state") or "").upper()
        candidates: tuple[PongNativeDirectiveKind, ...]
        if role == "ENTRY":
            candidates = (
                PongNativeDirectiveKind.ENTER_LONG,
                PongNativeDirectiveKind.ENTER_SHORT,
                PongNativeDirectiveKind.CANCEL_PENDING_ENTRY,
            )
        elif role == "EXIT":
            candidates = (
                PongNativeDirectiveKind.EXIT,
                PongNativeDirectiveKind.EMERGENCY_FLATTEN,
            )
        elif role == "PROTECTIVE" and state in {"CANCELLED", "CANCELED"}:
            candidates = (PongNativeDirectiveKind.CANCEL_PROTECTIVE_STOP,)
        elif role == "PROTECTIVE":
            candidates = (PongNativeDirectiveKind.VERIFY_NATIVE_PROTECTION,)
        else:
            candidates = ()
        return next(
            (
                self._active_directive_ids[kind]
                for kind in candidates
                if kind in self._active_directive_ids
            ),
            None,
        )

    def on_execution_message(self, message: Mapping[str, object]) -> None:
        """Advance the durable bridge from runtime-accepted native evidence."""
        if not isinstance(message, Mapping):
            return
        with self._lock:
            bridge = self._bridge
            if bridge is None:
                return
            try:
                message_type = str(message.get("message_type") or "").upper()
                role = str(message.get("order_role") or "").upper()
                state = str(message.get("order_state") or "").upper()
                directive_id = self._directive_for_message_locked(message)
                proof = self._event_proof_locked(message)
                update: PongBridgeUpdate | None = None
                if directive_id is not None and message_type == "COMMAND_ACK":
                    update = bridge.on_command_ack(directive_id, proof)
                elif directive_id is not None and message_type == "COMMAND_REJECTED":
                    update = bridge.on_command_rejected(
                        directive_id, proof,
                        reason=str(message.get("reason_code") or "UNKNOWN"),
                    )
                elif directive_id is not None and message_type == "EXECUTION_EVENT":
                    quantity = abs(int(message.get("quantity") or 0))
                    fill_id = str(
                        message.get("execution_id")
                        or message.get("fill_id")
                        or message.get("receipt_id")
                        or ""
                    )
                    if role == "ENTRY":
                        update = bridge.on_entry_fill(
                            directive_id, proof, quantity=quantity, fill_id=fill_id,
                        )
                    elif role in {"EXIT", "PROTECTIVE"}:
                        update = bridge.on_reducing_fill(
                            directive_id, proof, quantity=quantity, fill_id=fill_id,
                        )
                elif (
                    directive_id is not None
                    and message_type == "ORDER_EVENT"
                    and role == "PROTECTIVE"
                    and state in {"ACCEPTED", "WORKING"}
                ):
                    update = bridge.on_protection_working(
                        directive_id, proof,
                        quantity=abs(int(message.get("quantity") or 1)),
                    )
                elif (
                    directive_id is not None
                    and message_type == "ORDER_EVENT"
                    and role == "PROTECTIVE"
                    and state in {"CANCELLED", "CANCELED"}
                ):
                    update = bridge.on_protective_cancelled(directive_id, proof)
                elif (
                    message_type == "RECONCILIATION"
                    and int(message.get("position_quantity") or 0) == 0
                    and int(message.get("working_order_count") or 0) == 0
                    and bridge.machine.state.value in {"EXIT_PENDING", "SAFETY_BLOCKED"}
                ):
                    runtime = dict(self._runtime_snapshot())
                    self._runtime = runtime
                    update = bridge.on_signed_flat_reconciliation(
                        self._reconciliation(runtime), proof,
                        at=str(runtime["observed_at"]),
                    )
                if update is not None:
                    self._queue_bridge_update(update)
            except Exception as error:
                self._bridge_error = (
                    "PONG_NATIVE_CALLBACK_BRIDGE_FAILED:" + type(error).__name__
                )

    @staticmethod
    def _bias_from_inputs(
        previous: CompletedPongHour,
        current: CompletedPongHour,
        trades: tuple[ClassifiedTradeInput, ...],
        *,
        published_at: str,
    ) -> DailyBiasResult:
        structural = hourly_structure_components(
            previous, current, available_at=published_at,
        )
        flow = HourlyOrderFlowEvaluator().evaluate(CompletedHourlyOrderFlow(
            current.start, current.end, published_at, current.start, current.end,
            True, trades,
        ))
        options = unavailable_nonvoting_options(
            start=current.start, end=current.end, available_at=published_at,
        )
        frame = HourlyBiasFrame(
            "MNQ SEP26", current.start, current.end, published_at,
            (*structural, flow), options, True, None,
        )
        return DailyBiasEngine().update(
            frame, BiasUpdateReason.COMPLETED_HOURLY_BOUNDARY,
        ).result

    def _bias_from_certificate(self, value: Mapping[str, object]) -> DailyBiasResult:
        raw_hours = value.get("hour_pair")
        raw_trades = value.get("classified_trades")
        if not isinstance(raw_hours, list) or len(raw_hours) != 2 or not isinstance(raw_trades, list):
            raise ValueError("certificate bias inputs")
        hours = tuple(CompletedPongHour(
            start=str(item["start"]), end=str(item["end"]),
            available_at=str(item["available_at"]), open=Decimal(str(item["open"])),
            high=Decimal(str(item["high"])), low=Decimal(str(item["low"])),
            close=Decimal(str(item["close"])), source=str(item["source"]),
            continuity_domain=str(item["continuity_domain"]),
            source_ids=tuple(str(x) for x in item["source_ids"]),
            source_hashes=tuple(str(x) for x in item["source_hashes"]),
            complete=bool(item["complete"]), instrument=str(item["instrument"]),
        ) for item in raw_hours)
        trades = tuple(ClassifiedTradeInput(
            event_id=str(item["event_id"]), payload_hash=str(item["payload_hash"]),
            source=str(item["source"]), instrument=str(item["instrument"]),
            side=ClassifiedSide(str(item["side"])), size=Decimal(str(item["size"])),
            event_at=str(item["event_at"]), observed_at=str(item["observed_at"]),
            available_at=str(item["available_at"]), authenticated=bool(item["authenticated"]),
        ) for item in raw_trades)
        result = self._bias_from_inputs(
            hours[0], hours[1], trades, published_at=str(value["published_at"]),
        )
        if result.result_hash != value.get("daily_bias_result_hash"):
            raise ValueError("certificate daily bias hash")
        return result

    def _maybe_certify_hour(self) -> None:
        if self._certificate is not None or self._certificate_error is not None:
            return
        required = self._collection.get("required_interval")
        durability = self._collection.get("durability")
        if not isinstance(required, Mapping) or required.get("eligible") is not True:
            return
        if (
            self._input_error is not None
            or self._hour_pair is None
            or not isinstance(durability, Mapping)
            or required.get("blockers") not in ([], ())
        ):
            return
        published_at = self._hour_pair[1].available_at
        try:
            bias = self._bias_from_inputs(
                self._hour_pair[0], self._hour_pair[1],
                tuple(self._classified_hour), published_at=published_at,
            )
            if not bias.configured:
                return
            body: dict[str, object] = {
                "schema": PONG_QUALIFYING_HOUR_SCHEMA,
                "created_at": _now(),
                "test_only": False,
                "eligible": True,
                "collector_id": (self._collection.get("collector") or {}).get("collector_id"),
                "producer": self._collection.get("producer"),
                "required_interval": dict(required),
                "journal_path": durability.get("journal_path"),
                "journal_tip_sha256": durability.get("journal_tip_sha256"),
                "hour_pair": [item.payload() for item in self._hour_pair],
                "classified_trades": [
                    {
                        **asdict(item), "side": item.side.value,
                        "size": str(item.size),
                    }
                    for item in self._classified_hour
                ],
                "published_at": published_at,
                "daily_bias": bias.payload(),
                "daily_bias_result_hash": bias.result_hash,
            }
            body["certificate_hash"] = canonical_hash(body)
            _atomic_write(self.certificate_path, body)
            self._certificate = body
            self._daily_bias = bias
        except Exception as error:
            self._certificate_error = "PONG_QUALIFYING_HOUR_CERTIFICATION_FAILED:" + type(error).__name__

    def _reconciliation(self, runtime: Mapping[str, object]) -> PongReconciliationProof:
        observed_at = str(runtime["observed_at"])
        return PongReconciliationProof.create(
            source_id="l3g-paper-addon-provenance-v1",
            source_fingerprint=str(runtime["addon_source_fingerprint"]),
            build_fingerprint=str(runtime["addon_build_fingerprint"]),
            source_hash=canonical_hash(dict(runtime)),
            observed_at=observed_at,
            available_at=observed_at,
            position_quantity=int(runtime["position_quantity"]),
            working_owned_orders=int(runtime["working_owned_orders"]),
            unresolved_execution=bool(runtime["unresolved_execution"]),
        )

    def _maybe_create_bridge(self) -> None:
        if self._bridge is not None or self._bridge_error is not None:
            return
        runtime = self._runtime
        if (
            self.selected_profile != PONG_ENTRY_PROFILE_VERSION
            or self._certificate is None
            or not self._native_input_ready_locked()
            or runtime.get("addon_provenance_valid") is not True
            or not isinstance(runtime.get("execution_session_id"), str)
        ):
            return
        try:
            identity = PongLoadedRuntimeIdentity(
                execution_session_id=str(runtime["execution_session_id"]),
                addon_protocol_version=str(runtime["addon_protocol_version"]),
                addon_source_fingerprint=str(runtime["addon_source_fingerprint"]),
                addon_build_fingerprint=str(runtime["addon_build_fingerprint"]),
                verified_at=str(runtime["observed_at"]),
                attestation_verified=True,
            )
            policy = PongQuantityPolicy(
                native_maximum_quantity=1,
                native_capability_status="CURRENT_AUTHENTICATED_ONE_MNQ_CAPABILITY",
                reconciliation_source_fingerprint=identity.addon_source_fingerprint,
                reconciliation_build_fingerprint=identity.addon_build_fingerprint,
            )
            machine = PongStateMachine(
                quantity_policy=policy,
                discernment_policy=self._discernment.policy,
            )
            machine.on_bias_update(self._daily_bias)
            bridge_path = self.state_root / "bridges" / (
                identity.execution_session_id + ".json"
            )
            if bridge_path.exists():
                self._bridge = PongRuntimeBridge.restore(
                    bridge_path,
                    current_snapshot=None,
                    current_bias=self._daily_bias,
                    reconciliation=self._reconciliation(runtime),
                    current_runtime_identity=identity,
                    quantity_policy=policy,
                    at=str(runtime["observed_at"]),
                )
            else:
                self._bridge = PongRuntimeBridge.create(
                    machine, bridge_path, runtime_identity=identity,
                )
        except Exception as error:
            self._bridge_error = "PONG_RUNTIME_BRIDGE_START_FAILED:" + type(error).__name__

    def _native_input_ready_locked(self) -> bool:
        if (
            self._certificate is None
            or (self._warm_start is None and not self._retained_hour_seed)
            or self._input_error is not None
        ):
            return False
        producer = self._certificate.get("producer")
        if not isinstance(producer, Mapping) or self._producer is None:
            return False
        # A supported observer replacement creates a new producer session and
        # loaded assembly identity.  The retained hour remains historical
        # evidence; current microstructure starts a fresh continuity domain.
        # Source semantics must remain exact, while current market/execution
        # build congruency is checked separately below.
        return self._producer[1] == str(
            producer.get("source_fingerprint"),
        )

    def _blockers_locked(self) -> list[str]:
        blockers: list[str] = []
        runtime = self._runtime
        if self.selected_profile != PONG_ENTRY_PROFILE_VERSION:
            blockers.append("PONG_PROFILE_NOT_SELECTED")
        if not self.activation_authorized:
            blockers.append("PONG_BOUNDED_ACTIVATION_NOT_AUTHORIZED")
        if self._certificate is None:
            blockers.append(self._certificate_error or "PONG_AUTHENTIC_HOUR_NOT_QUALIFIED")
        if not self._native_input_ready_locked():
            blockers.append(self._input_error or "PONG_NATIVE_INPUT_CONTRACT_NOT_READY")
        if self._bridge is None or self._bridge_error is not None:
            blockers.append(self._bridge_error or "PONG_NATIVE_RUNTIME_BRIDGE_NOT_READY")
        if runtime.get("runtime_active") is not True:
            blockers.append("PONG_RUNTIME_NOT_ACTIVE")
        if runtime.get("addon_provenance_valid") is not True:
            blockers.append("PONG_EXECUTION_ADDON_PROVENANCE_INVALID")
        if (
            self._producer is not None
            and runtime.get("addon_build_fingerprint") != self._producer[2]
        ):
            blockers.append("PONG_MARKET_EXECUTION_BUILD_MISMATCH")
        if runtime.get("reconciliation_current") is not True:
            blockers.append("PONG_RECONCILIATION_NOT_CURRENT")
        if runtime.get("unresolved_execution") is True:
            blockers.append("PONG_UNRESOLVED_EXECUTION")
        if runtime.get("safety_lockout") is True:
            blockers.append("PONG_SAFETY_LOCKOUT_RETAINED")
        if runtime.get("entries_paused") is True:
            blockers.append("PONG_RUNTIME_ENTRIES_PAUSED")
        if int(runtime.get("maximum_quantity") or 0) != 1:
            blockers.append("PONG_NATIVE_QUANTITY_CAPABILITY_INVALID")
        return list(dict.fromkeys(blockers))

    def authority_status(self) -> dict[str, object]:
        with self._lock:
            blockers = self._blockers_locked()
            activation_blockers = [
                value for value in blockers
                if value not in {
                    "PONG_BOUNDED_STOP_UNVERIFIED",
                    "PONG_CONTROLLER_ENTRY_FENCE_ACTIVE",
                }
            ]
            phase = None if self._controller is None else self._controller.state.phase
            candidate = not activation_blockers
            ready = candidate and phase in _RUNNING_CONTROLLER_PHASES
            start_blockers = [
                value for value in blockers
                if value != "PONG_RUNTIME_NOT_ACTIVE"
            ]
            return {
                "schema": PONG_PRODUCTION_HOST_SCHEMA,
                "native_input_contract": "READY" if self._native_input_ready_locked() else "BLOCKED",
                "native_runtime_bridge": (
                    "READY"
                    if self._bridge is not None and self._bridge_error is None
                    else "BLOCKED"
                ),
                "bounded_controller": "READY" if phase in _RUNNING_CONTROLLER_PHASES else "BLOCKED",
                "activation_candidate": candidate,
                "operational_start_ready": (
                    not start_blockers
                    and phase is PongControllerPhase.PREPARED
                ),
                "entry_authority_ready": ready,
                "blocking_reasons": blockers,
            }

    def _controller_observation_locked(self) -> PongControllerObservation:
        runtime = self._runtime
        authority = self.authority_status()
        return PongControllerObservation(
            observed_at=str(runtime.get("observed_at") or _now()),
            entry_fence_active=bool(runtime.get("entry_fence_active", True)),
            position_quantity=int(runtime.get("position_quantity") or 0),
            working_owned_orders=int(runtime.get("working_owned_orders") or 0),
            working_entry_orders=int(runtime.get("working_entry_orders") or 0),
            unresolved_execution=bool(runtime.get("unresolved_execution", True)),
            reconciliation_current=bool(runtime.get("reconciliation_current", False)),
            protection_current=bool(runtime.get("protection_current", False)),
            entry_gates_healthy=bool(authority["activation_candidate"]),
            safety_lockout=bool(runtime.get("safety_lockout", True)),
            runtime_active=bool(runtime.get("runtime_active", False)),
            evidence_sealed=bool(runtime.get("evidence_sealed", False)),
            signature_verified=bool(runtime.get("addon_provenance_valid", False)),
        )

    def _open_controller(self) -> None:
        if self._controller is not None:
            return
        try:
            self._controller = PongBoundedController.open(
                self.controller_path,
                experiment_id=PONG_PRODUCTION_EXPERIMENT_ID,
                at=_now(),
                process_instance_id=self._process_instance_id,
            )
            self._controller_error = None
        except Exception as error:
            self._controller_error = "PONG_CONTROLLER_HOST_START_FAILED:" + type(error).__name__

    def _dispatch_controller(self, command: PongControllerCommand) -> None:
        controller = self._controller
        if controller is None:
            return
        controller.mark_dispatched(command.command_id, at=_now())
        accepted = False
        reason = ""
        try:
            accepted = self._controller_dispatch(command.kind.value, command.reason)
            if not accepted:
                reason = "PONG_CONTROLLER_RUNTIME_COMMAND_REFUSED"
        except Exception as error:
            reason = "PONG_CONTROLLER_DISPATCH_FAILED:" + type(error).__name__
        controller.acknowledge_command(
            command.command_id, at=_now(), accepted=accepted, reason=reason,
        )

    def _run(self) -> None:
        while not self._stop.wait(0.5):
            try:
                runtime = dict(self._runtime_snapshot())
                collection = dict(self._collector_status())
                prepare_activation = False
                command: PongControllerCommand | None = None
                with self._lock:
                    self._runtime = runtime
                    self._collection = collection
                    self._open_controller()
                    self._maybe_certify_hour()
                    self._maybe_create_bridge()
                    self._evaluate_microstructure(_time(runtime.get("observed_at") or _now()))
                    controller = self._controller
                    if controller is None:
                        continue
                    observation = self._controller_observation_locked()
                    authority = self.authority_status()
                    if (
                        controller.state.phase is PongControllerPhase.PREPARED
                        and self.activation_authorized
                        and authority["activation_candidate"] is True
                    ):
                        prepare_activation = True
                    else:
                        command = controller.tick(
                            observation, at=observation.observed_at,
                        )
                if prepare_activation:
                    if not self._controller_dispatch(
                        "PREPARE_ACTIVATION", "PONG_CONTROLLER_ACTIVATION",
                    ):
                        raise RuntimeError("PONG_CONTROLLER_ACTIVATION_PREPARATION_REFUSED")
                    refreshed = dict(self._runtime_snapshot())
                    with self._lock:
                        self._runtime = refreshed
                        controller = self._controller
                        if controller is None:
                            continue
                        observation = self._controller_observation_locked()
                        controller.activate(observation, at=observation.observed_at)
                        command = controller.tick(
                            observation, at=observation.observed_at,
                        )
                while True:
                    with self._lock:
                        if not self._pending_native:
                            break
                        directive, evidence = self._pending_native.popleft()
                    self._dispatch_native(directive, evidence)
                if command is not None:
                    self._dispatch_controller(command)
            except Exception as error:
                with self._lock:
                    self._controller_error = "PONG_PRODUCTION_HOST_LOOP_FAILED:" + type(error).__name__

    def status(self) -> dict[str, object]:
        with self._lock:
            authority = self.authority_status()
            controller = self._controller
            bridge = self._bridge
            return {
                **authority,
                "state": "RUNNING" if self._thread is not None and self._thread.is_alive() else "STOPPED",
                "selected_profile": self.selected_profile,
                "registration": "PRODUCTION_REGISTERED",
                "activation_authorized": self.activation_authorized,
                "activation_date_local": PONG_ACTIVATION_DATE_LOCAL.isoformat(),
                "account": "Sim101",
                "account_class": "LOCAL_SIMULATION",
                "instrument": "MNQ SEP26",
                "canonical_contract": "MNQU6",
                "maximum_quantity": 1,
                "live_authority": "DISARMED",
                "live_capital": "DENIED",
                "source_identity": None if self._producer is None else {
                    "session_id": self._producer[0],
                    "source_fingerprint": self._producer[1],
                    "build_fingerprint": self._producer[2],
                },
                "stream_counts": dict(self._stream_counts),
                "missing_events": dict(self._stream_missing),
                "out_of_order_events": dict(self._stream_out_of_order),
                "input_error": self._input_error,
                "warm_start_complete": self._warm_start is not None,
                "hour_seed": (
                    "AUTHENTICATED_WARM_START"
                    if self._warm_start is not None
                    else "QUALIFIED_RETAINED_HOUR_CERTIFICATE"
                    if self._retained_hour_seed
                    else "UNAVAILABLE"
                ),
                "qualified_hour_certificate": None if self._certificate is None else {
                    "path": str(self.certificate_path),
                    "certificate_hash": self._certificate["certificate_hash"],
                    "interval": self._certificate["required_interval"],
                    "daily_bias_result_hash": self._certificate["daily_bias_result_hash"],
                },
                "daily_bias": self._daily_bias.payload(),
                "microstructure_pipeline": {
                    "state": "HOSTED",
                    "mapping_hash": self._mapper.policy.mapping_hash,
                    "discernment_policy_hash": self._discernment.policy.policy_hash,
                    "blockers": list(self._microstructure_blockers),
                },
                "runtime_bridge": None if bridge is None else bridge.status(),
                "controller": None if controller is None else {
                    "state_path": str(controller.path),
                    "controller_id": controller.state.controller_id,
                    "phase": controller.state.phase.value,
                    "activated_at_utc": controller.state.activated_at_utc,
                    "execution_deadline_utc": controller.state.execution_deadline_utc,
                    "heartbeat_valid_until": controller.state.heartbeat_valid_until,
                    "pending_command": (
                        None if controller.state.pending_command is None
                        else asdict(controller.state.pending_command)
                    ),
                    "state_hash": controller.state.state_hash,
                },
                "controller_error": self._controller_error,
                "bridge_error": self._bridge_error,
                "test_only": False,
            }
