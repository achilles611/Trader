"""L3H canary state machine with write-ahead and unknown-state quarantine."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Mapping
from uuid import uuid4

from .authority import LiveReadiness, derive_terminal_status
from .contracts import LiveCapability, canonical_hash, utc_now
from .event_store import LiveEventStore
from .gateway import GatewayDispatchError, LiveGateway, NoDispatchLiveGateway
from .lifecycle import ExecutionLifecycle, OrderLifecycleState
from .live_authorization import AuthorizationFacts, LiveAuthorizationBoundary, LiveEntryRequest
from .reconciliation import BrokerSnapshot, reconcile
from .risk import LiveRiskAuthority


class LiveRuntimeState(StrEnum):
    BLOCKED = "BLOCKED"
    READY_DISARMED = "READY_DISARMED"
    ACTIVATION_PENDING = "ACTIVATION_PENDING"
    ARMED_FLAT = "ARMED_FLAT"
    COMMAND_SEALED = "COMMAND_SEALED"
    QUARANTINED = "QUARANTINED"
    LIVE_CANARY_COMPLETE = "LIVE_CANARY_COMPLETE"


@dataclass(frozen=True)
class OperatorActivation:
    request_id: str
    nonce: str
    hold_confirmed: bool
    requested_at: str

    def __post_init__(self) -> None:
        if len(self.request_id) < 8 or not self.nonce or not self.hold_confirmed:
            raise ValueError("Activation requires a held, idempotent operator request.")


class LiveRuntime:
    """Does not auto-start, auto-arm, or retry a potentially transmitted order."""

    def __init__(
        self, store: LiveEventStore, *, capability: LiveCapability | None = None,
        risk: LiveRiskAuthority | None = None, gateway: LiveGateway | None = None,
        authorization_boundary: LiveAuthorizationBoundary | None = None,
    ) -> None:
        self.store = store
        self.capability = capability
        self.risk = risk or LiveRiskAuthority()
        self.gateway = gateway or NoDispatchLiveGateway()
        self.authorization_boundary = authorization_boundary
        self.state = LiveRuntimeState.BLOCKED
        self.terminal_status = "BLOCKED_CAPABILITY_MISSING"
        self._snapshot: BrokerSnapshot | None = None
        self._activation: OperatorActivation | None = None
        self._active_command_id: str | None = None
        self._active_client_order_id: str | None = None

    def preflight(self, readiness: LiveReadiness, snapshot: BrokerSnapshot | None) -> str:
        self._snapshot = snapshot
        if self.capability is not None and snapshot is not None:
            broker = reconcile(self.capability, snapshot)
            if broker.state != "FLAT":
                readiness = LiveReadiness(
                    gate_passes=readiness.gate_passes, blockers={**readiness.blockers}, broker_position=broker.state,
                    owned_working_orders=snapshot.owned_working_orders, foreign_or_unknown_orders=snapshot.foreign_or_unknown_orders,
                    reconciliation_fresh=False,
                )
        self.terminal_status = derive_terminal_status(self.capability, readiness)
        self.state = LiveRuntimeState.READY_DISARMED if self.terminal_status.endswith("READY_DISARMED") else LiveRuntimeState.BLOCKED
        self.store.append("runtime", "PREFLIGHT_EVALUATED", {"terminal_status": self.terminal_status, "state": self.state.value})
        return self.terminal_status

    def activate(self, activation: OperatorActivation) -> str:
        del activation
        # L3H.3 removed generic activation as a live-capital authority source.
        # Callers must complete the exact preflight/challenge ceremony through
        # LiveAuthorizationBoundary and present its one-shot capability.
        raise ValueError("L3H3_EXACT_AUTHORIZATION_CEREMONY_REQUIRED")

    def seal_entry(
        self, *, expected_trade_risk: Decimal, side: str = "LONG", session_valid: bool = False,
        daily_loss_clear: bool = False, authorization_id: str | None = None,
        authorization_facts: AuthorizationFacts | None = None,
        request_id: str | None = None, strategy_signal_id: str | None = None,
    ) -> Mapping[str, object]:
        if self.capability is None or self.authorization_boundary is None or authorization_id is None or authorization_facts is None:
            raise ValueError("L3H3_EXACT_AUTHORIZATION_CEREMONY_REQUIRED")
        admitted, reason = self.risk.admit_entry(
            position_quantity=authorization_facts.quantity or 0,
            pending_entries=authorization_facts.owned_working_entry_orders or 0,
            expected_trade_risk=expected_trade_risk,
        )
        if not admitted:
            raise ValueError(reason)
        if side not in {"LONG", "SHORT"}:
            raise ValueError("ENTRY_SIDE_INVALID")
        command_id = "l3h-cmd-" + uuid4().hex
        intent_id = "l3h-intent-" + uuid4().hex
        client_order_id = "BZ-L3H-" + uuid4().hex[:20].upper()
        exact_request_id = request_id or "l3h3-request-" + uuid4().hex
        exact_strategy_signal_id = strategy_signal_id or "l3h3-signal-" + uuid4().hex
        request = LiveEntryRequest(
            request_id=exact_request_id, strategy_signal_id=exact_strategy_signal_id,
            action="ENTER_" + side,
            account_fingerprint=authorization_facts.account.account_fingerprint,
            account_class=authorization_facts.account.account_class.value,
            native_instrument=authorization_facts.native_instrument,
            canonical_contract=authorization_facts.canonical_contract,
            quantity=1, resulting_position_quantity=1 if side == "LONG" else -1,
        )
        envelope = self.authorization_boundary.atomic_admit(
            authorization_id, request, authorization_facts, command_id=command_id,
        )
        lifecycle = ExecutionLifecycle(self.store, client_order_id)
        lifecycle.transition(OrderLifecycleState.INTENT_CREATED, evidence={
            "authorization_id_hash": canonical_hash(authorization_id), "request_id": exact_request_id,
        })
        lifecycle.transition(OrderLifecycleState.ADMITTED, evidence={"risk_hash": self.risk.profile.configuration_hash})
        command = {
            "command_id": command_id, "request_id": exact_request_id, "intent_id": intent_id,
            "client_order_id": client_order_id, "action": "ENTER_" + side, "side": side,
            "order_type": "MARKET", "limit_price": None, "stop_price": None,
            "strategy_run_id": exact_strategy_signal_id,
            "prior_causal_event": "ONE_SHOT_CAPABILITY_ATOMICALLY_CONSUMED", "account_alias": self.capability.account_alias,
            "account_binding_hash": self.capability.account_binding_hash, "native_instrument": self.capability.native_instrument,
            "canonical_contract": self.capability.canonical_contract, "quantity": 1, "commissioning_epoch": self.capability.commissioning_epoch,
            "capability_generation": self.capability.capability_id, "created_at": utc_now(),
            "capability_hash": self.capability.capability_hash, "risk_hash": self.risk.profile.configuration_hash,
            "session_valid": session_valid, "daily_loss_clear": daily_loss_clear,
            "account_class": "LIVE_CAPITAL", "live_capital": True,
            "account_fingerprint": authorization_facts.account.account_fingerprint,
            "provider_identity_hash": authorization_facts.account.provider_identity_hash,
            "connection_identity_hash": authorization_facts.account.connection_identity_hash,
            "addon_provenance": authorization_facts.addon_provenance,
            "live_authorization": envelope.as_mapping(),
            "idempotency_fingerprint": canonical_hash({"authorization": authorization_id, "command_id": command_id}),
        }
        command["canonical_payload_hash"] = canonical_hash(command)
        seal, replayed = self.store.seal_command(request_id=exact_request_id, command=command)
        self._active_command_id = command_id
        self._active_client_order_id = client_order_id
        self.state = LiveRuntimeState.COMMAND_SEALED
        return {"command": command, "sealed_event_id": seal.event_id, "idempotent_replay": replayed}

    def dispatch_sealed(self) -> Mapping[str, object]:
        """Dispatch exactly once; a lost acknowledgement is terminally UNKNOWN."""

        if self.state is not LiveRuntimeState.COMMAND_SEALED or self._active_command_id is None:
            raise ValueError("NO_SEALED_COMMAND")
        record = self.store.command(self._active_command_id)
        if record is None or record["state"] != "SEALED":
            raise ValueError("COMMAND_NOT_DISPATCHABLE")
        self.store.mark_command(self._active_command_id, state="DISPATCHING")
        if self._active_client_order_id is not None:
            ExecutionLifecycle(self.store, self._active_client_order_id).transition(
                OrderLifecycleState.SUBMITTING, evidence={"sealed_event_id": record["sealed_event_id"]},
            )
        try:
            acknowledgement = dict(self.gateway.dispatch(record["command"]))  # type: ignore[arg-type]
        except GatewayDispatchError as exc:
            self.store.mark_command(self._active_command_id, state="UNKNOWN", acknowledgement={"reason": str(exc)})
            self.state = LiveRuntimeState.QUARANTINED
            raise
        except Exception as exc:
            self.store.mark_command(self._active_command_id, state="UNKNOWN", acknowledgement={"reason": type(exc).__name__})
            self.state = LiveRuntimeState.QUARANTINED
            raise GatewayDispatchError("DISPATCH_ACKNOWLEDGEMENT_UNKNOWN") from exc
        if acknowledgement.get("outcome") == "NACK":
            self.store.mark_command(self._active_command_id, state="REFUSED", acknowledgement=acknowledgement)
            if self._active_client_order_id is not None:
                ExecutionLifecycle(self.store, self._active_client_order_id).transition(
                    OrderLifecycleState.REJECTED, evidence=acknowledgement,
                )
            self.risk.lock("NATIVE_COMMAND_REJECTED")
            self.state = LiveRuntimeState.QUARANTINED
            raise GatewayDispatchError("NATIVE_COMMAND_REJECTED")
        if acknowledgement.get("outcome") != "ACK":
            self.store.mark_command(self._active_command_id, state="UNKNOWN", acknowledgement={"reason": "ACK_OUTCOME_INVALID"})
            self.state = LiveRuntimeState.QUARANTINED
            raise GatewayDispatchError("DISPATCH_ACKNOWLEDGEMENT_UNKNOWN")
        self.store.mark_command(self._active_command_id, state="ACKNOWLEDGED", acknowledgement=acknowledgement)
        if self._active_client_order_id is not None:
            ExecutionLifecycle(self.store, self._active_client_order_id).transition(
                OrderLifecycleState.BROKER_ACKNOWLEDGED, evidence=acknowledgement,
            )
        return acknowledgement

    def broker_lifecycle(self, state: OrderLifecycleState, evidence: Mapping[str, object]) -> str:
        """Record a native event; unknown events permanently remove authority."""

        if self._active_client_order_id is None:
            raise ValueError("NO_ACTIVE_CLIENT_ORDER")
        lifecycle = ExecutionLifecycle(self.store, self._active_client_order_id)
        lifecycle.transition(state, evidence=evidence)
        if state in {OrderLifecycleState.UNKNOWN, OrderLifecycleState.QUARANTINED}:
            self.risk.lock("BROKER_ORDER_UNRESOLVED")
            self.state = LiveRuntimeState.QUARANTINED
        return state.value

    def status(self) -> Mapping[str, object]:
        result = {
            "mode": "L3H_LIVE_CAPITAL", "state": self.state.value, "terminal_status": self.terminal_status,
            "capability_loaded": self.capability is not None, "account_class": None if self.capability is None else self.capability.account_class.value,
            "live_capital": "DENIED" if self.capability is None or not self.capability.live_capital else "CAPABILITY_BOUND",
            "active_command_id": self._active_command_id, "unknown_never_flat": True,
            "active_client_order_id": self._active_client_order_id,
        }
        if self.authorization_boundary is not None:
            result["live_authorization_boundary"] = self.authorization_boundary.status()
        return result
