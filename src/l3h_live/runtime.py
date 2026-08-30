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
    ) -> None:
        self.store = store
        self.capability = capability
        self.risk = risk or LiveRiskAuthority()
        self.gateway = gateway or NoDispatchLiveGateway()
        self.state = LiveRuntimeState.BLOCKED
        self.terminal_status = "BLOCKED_CAPABILITY_MISSING"
        self._snapshot: BrokerSnapshot | None = None
        self._activation: OperatorActivation | None = None
        self._active_command_id: str | None = None

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
        if self.state is not LiveRuntimeState.READY_DISARMED or self.capability is None:
            raise ValueError("LIVE_PREFLIGHT_NOT_READY")
        if self.terminal_status != "LIVE_READY_DISARMED":
            raise ValueError("LIVE_CAPITAL_NOT_AUTHORIZED")
        if not activation.nonce.startswith(self.capability.activation_nonce_family + "-"):
            raise ValueError("ACTIVATION_NONCE_FAMILY_MISMATCH")
        self._activation = activation
        self.state = LiveRuntimeState.ARMED_FLAT
        self.store.append("activation:" + activation.request_id, "ACTIVATION_ARMED", {
            "request_id": activation.request_id, "nonce_family": self.capability.activation_nonce_family,
            "commissioning_epoch": self.capability.commissioning_epoch,
        })
        return self.state.value

    def seal_entry(self, *, expected_trade_risk: Decimal) -> Mapping[str, object]:
        if self.state is not LiveRuntimeState.ARMED_FLAT or self.capability is None or self._activation is None or self._snapshot is None:
            raise ValueError("LIVE_RUNTIME_NOT_ARMED_FLAT")
        broker = reconcile(self.capability, self._snapshot)
        if broker.state != "FLAT":
            self.state = LiveRuntimeState.QUARANTINED
            raise ValueError("BROKER_STATE_NOT_PROVEN_FLAT")
        admitted, reason = self.risk.admit_entry(position_quantity=self._snapshot.quantity or 0, pending_entries=self._snapshot.owned_working_orders or 0, expected_trade_risk=expected_trade_risk)
        if not admitted:
            raise ValueError(reason)
        command_id = "l3h-cmd-" + uuid4().hex
        command = {
            "command_id": command_id, "action": "ENTER_NATURAL_SIGNAL_ONLY", "account_alias": self.capability.account_alias,
            "account_binding_hash": self.capability.account_binding_hash, "native_instrument": self.capability.native_instrument,
            "canonical_contract": self.capability.canonical_contract, "quantity": 1, "commissioning_epoch": self.capability.commissioning_epoch,
            "created_at": utc_now(), "capability_hash": self.capability.capability_hash, "risk_hash": self.risk.profile.configuration_hash,
            "idempotency_fingerprint": canonical_hash({"activation": self._activation.request_id, "command_id": command_id}),
        }
        seal, replayed = self.store.seal_command(request_id=self._activation.request_id, command=command)
        self._active_command_id = command_id
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
        self.store.mark_command(self._active_command_id, state="ACKNOWLEDGED", acknowledgement=acknowledgement)
        return acknowledgement

    def status(self) -> Mapping[str, object]:
        return {
            "mode": "L3H_LIVE_CAPITAL", "state": self.state.value, "terminal_status": self.terminal_status,
            "capability_loaded": self.capability is not None, "account_class": None if self.capability is None else self.capability.account_class.value,
            "live_capital": "DENIED" if self.capability is None or not self.capability.live_capital else "CAPABILITY_BOUND",
            "active_command_id": self._active_command_id, "unknown_never_flat": True,
        }
