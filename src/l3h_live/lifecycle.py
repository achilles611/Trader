"""Evidence-backed L3H order and protection lifecycle state machines."""

from __future__ import annotations

from enum import StrEnum
from typing import Mapping

from .event_store import LiveEventStore


class OrderLifecycleState(StrEnum):
    INTENT_CREATED = "INTENT_CREATED"
    ADMITTED = "ADMITTED"
    SUBMITTING = "SUBMITTING"
    BROKER_ACKNOWLEDGED = "BROKER_ACKNOWLEDGED"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"
    RECONCILED = "RECONCILED"
    QUARANTINED = "QUARANTINED"


_ALLOWED: Mapping[OrderLifecycleState | None, frozenset[OrderLifecycleState]] = {
    None: frozenset({OrderLifecycleState.INTENT_CREATED}),
    OrderLifecycleState.INTENT_CREATED: frozenset({OrderLifecycleState.ADMITTED, OrderLifecycleState.REJECTED, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.ADMITTED: frozenset({OrderLifecycleState.SUBMITTING, OrderLifecycleState.REJECTED, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.SUBMITTING: frozenset({OrderLifecycleState.BROKER_ACKNOWLEDGED, OrderLifecycleState.UNKNOWN, OrderLifecycleState.REJECTED, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.BROKER_ACKNOWLEDGED: frozenset({OrderLifecycleState.WORKING, OrderLifecycleState.PARTIALLY_FILLED, OrderLifecycleState.FILLED, OrderLifecycleState.REJECTED, OrderLifecycleState.UNKNOWN, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.WORKING: frozenset({OrderLifecycleState.PARTIALLY_FILLED, OrderLifecycleState.FILLED, OrderLifecycleState.CANCEL_PENDING, OrderLifecycleState.REJECTED, OrderLifecycleState.UNKNOWN, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.PARTIALLY_FILLED: frozenset({OrderLifecycleState.FILLED, OrderLifecycleState.CANCEL_PENDING, OrderLifecycleState.UNKNOWN, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.CANCEL_PENDING: frozenset({OrderLifecycleState.CANCELLED, OrderLifecycleState.PARTIALLY_FILLED, OrderLifecycleState.FILLED, OrderLifecycleState.UNKNOWN, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.UNKNOWN: frozenset({OrderLifecycleState.RECONCILED, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.RECONCILED: frozenset({OrderLifecycleState.WORKING, OrderLifecycleState.PARTIALLY_FILLED, OrderLifecycleState.FILLED, OrderLifecycleState.CANCELLED, OrderLifecycleState.REJECTED, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.FILLED: frozenset({OrderLifecycleState.RECONCILED, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.CANCELLED: frozenset({OrderLifecycleState.RECONCILED, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.REJECTED: frozenset({OrderLifecycleState.RECONCILED, OrderLifecycleState.QUARANTINED}),
    OrderLifecycleState.QUARANTINED: frozenset(),
}


class ExecutionLifecycle:
    """Persists legal transitions; it never infers a broker state."""

    def __init__(self, store: LiveEventStore, client_order_id: str) -> None:
        if not client_order_id.startswith("BZ-L3H-"):
            raise ValueError("L3H client order IDs must use the dedicated BZ-L3H prefix.")
        self.store = store
        self.client_order_id = client_order_id

    def current_state(self) -> OrderLifecycleState | None:
        events = self.store.stream("order:" + self.client_order_id)
        if not events:
            return None
        value = events[-1].payload.get("to_state")
        return OrderLifecycleState(str(value))

    def transition(self, state: OrderLifecycleState, *, evidence: Mapping[str, object]) -> OrderLifecycleState:
        previous = self.current_state()
        if state not in _ALLOWED[previous]:
            raise ValueError(f"INVALID_ORDER_TRANSITION_{previous or 'NONE'}_TO_{state}")
        self.store.append("order:" + self.client_order_id, "ORDER_LIFECYCLE", {
            "client_order_id": self.client_order_id, "from_state": None if previous is None else previous.value,
            "to_state": state.value, "evidence": dict(evidence),
        })
        return state


class ProtectionState(StrEnum):
    ENTRY_PENDING = "ENTRY_PENDING"
    ENTRY_WORKING = "ENTRY_WORKING"
    ENTRY_PARTIAL = "ENTRY_PARTIAL"
    PROTECTION_PENDING = "PROTECTION_PENDING"
    PROTECTION_WORKING = "PROTECTION_WORKING"
    PROTECTED = "PROTECTED"
    PROTECTION_UNKNOWN = "PROTECTION_UNKNOWN"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"


class ProtectionLifecycle:
    """Fails closed if a fill lacks confirmed native protection."""

    def __init__(self, store: LiveEventStore, client_order_id: str) -> None:
        self.store = store
        self.client_order_id = client_order_id
        self.state = ProtectionState.ENTRY_PENDING

    def transition(self, state: ProtectionState, *, evidence: Mapping[str, object]) -> ProtectionState:
        if self.state in {ProtectionState.PROTECTION_UNKNOWN, ProtectionState.EMERGENCY_EXIT} and state is not ProtectionState.EMERGENCY_EXIT:
            raise ValueError("PROTECTION_TERMINALLY_UNRESOLVED")
        self.store.append("protection:" + self.client_order_id, "PROTECTION_LIFECYCLE", {
            "client_order_id": self.client_order_id, "from_state": self.state.value, "to_state": state.value,
            "evidence": dict(evidence),
        })
        self.state = state
        return state

    def fail_closed(self, reason: str) -> ProtectionState:
        self.transition(ProtectionState.PROTECTION_UNKNOWN, evidence={"reason": reason})
        return self.transition(ProtectionState.EMERGENCY_EXIT, evidence={"reason": reason, "action": "NATIVE_EMERGENCY_FLATTEN"})
