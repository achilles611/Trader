"""Broker truth is explicit: incomplete or stale facts are UNKNOWN, not FLAT."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Mapping

from .contracts import AccountClass, LiveCapability, NATIVE_INSTRUMENT, parse_utc, utc_now
from .event_store import LiveEventStore


@dataclass(frozen=True)
class BrokerSnapshot:
    observed_at: str
    account_alias: str | None
    account_class: AccountClass
    account_binding_hash: str | None
    native_instrument: str | None
    position: str = "UNKNOWN"
    quantity: int | None = None
    owned_working_orders: int | None = None
    foreign_or_unknown_orders: int | None = None
    position_snapshot_complete: bool = False
    order_snapshot_complete: bool = False
    connection_healthy: bool = False
    source: str = "UNKNOWN"

    def __post_init__(self) -> None:
        parse_utc(self.observed_at, "Broker snapshot time")
        if self.position not in {"FLAT", "LONG", "SHORT", "UNKNOWN"}:
            raise ValueError("Unsupported broker position state.")
        if self.quantity is not None and abs(self.quantity) > 1:
            raise ValueError("L3H broker snapshot exceeds the one-MNQ boundary.")

    def is_proven_flat(self) -> bool:
        return (
            self.connection_healthy
            and self.position_snapshot_complete
            and self.order_snapshot_complete
            and self.position == "FLAT"
            and self.quantity == 0
            and self.owned_working_orders == 0
            and self.foreign_or_unknown_orders == 0
        )

    def is_fresh(self, *, maximum_age_seconds: int = 15, now: str | None = None) -> bool:
        age = parse_utc(now or utc_now(), "Broker freshness time") - parse_utc(self.observed_at, "Broker snapshot time")
        return timedelta(0) <= age <= timedelta(seconds=maximum_age_seconds)


@dataclass(frozen=True)
class ReconciliationResult:
    state: str
    reason: str
    snapshot: BrokerSnapshot


class ExecutionSupervisor:
    """Startup and continuous truth supervisor for the L3H boundary.

    It consumes a complete native snapshot supplied by the authenticated
    AddOn.  It does not invent a flat position from local projections and it
    emits a durable quarantine record before reporting an ambiguous state.
    """

    def __init__(self, capability: LiveCapability, store: LiveEventStore) -> None:
        self.capability = capability
        self.store = store
        self.last_result: ReconciliationResult | None = None
        self.quarantined_reason: str | None = None

    def reconcile_startup(self, snapshot: BrokerSnapshot) -> ReconciliationResult:
        result = reconcile(self.capability, snapshot)
        self.last_result = result
        self.store.append("reconciliation", "BROKER_RECONCILIATION", {
            "state": result.state, "reason": result.reason, "account_alias": snapshot.account_alias,
            "native_instrument": snapshot.native_instrument, "observed_at": snapshot.observed_at,
            "position": snapshot.position, "quantity": snapshot.quantity,
            "owned_working_orders": snapshot.owned_working_orders,
            "foreign_or_unknown_orders": snapshot.foreign_or_unknown_orders,
        })
        if result.state == "UNKNOWN":
            self.quarantine(result.reason)
        return result

    def quarantine(self, reason: str) -> None:
        if self.quarantined_reason is None:
            self.store.append("reconciliation", "QUARANTINED", {"reason": reason})
        self.quarantined_reason = reason

    @property
    def ready_disarmed(self) -> bool:
        return self.quarantined_reason is None and self.last_result is not None and self.last_result.state == "FLAT"


def reconcile(capability: LiveCapability, snapshot: BrokerSnapshot) -> ReconciliationResult:
    if not snapshot.connection_healthy:
        return ReconciliationResult("UNKNOWN", "BROKER_CONNECTION_UNHEALTHY", snapshot)
    if not snapshot.is_fresh():
        return ReconciliationResult("UNKNOWN", "BROKER_SNAPSHOT_STALE", snapshot)
    if not snapshot.position_snapshot_complete or not snapshot.order_snapshot_complete:
        return ReconciliationResult("UNKNOWN", "BROKER_SNAPSHOT_INCOMPLETE", snapshot)
    if (
        snapshot.account_alias != capability.account_alias
        or snapshot.account_class is not capability.account_class
        or snapshot.account_binding_hash != capability.account_binding_hash
    ):
        return ReconciliationResult("UNKNOWN", "ACCOUNT_BINDING_MISMATCH", snapshot)
    if snapshot.native_instrument != NATIVE_INSTRUMENT:
        return ReconciliationResult("UNKNOWN", "INSTRUMENT_MISMATCH", snapshot)
    if snapshot.foreign_or_unknown_orders != 0:
        return ReconciliationResult("UNKNOWN", "FOREIGN_OR_UNKNOWN_ACTIVITY", snapshot)
    if snapshot.position == "UNKNOWN" or snapshot.quantity is None:
        return ReconciliationResult("UNKNOWN", "POSITION_UNKNOWN", snapshot)
    if snapshot.is_proven_flat():
        return ReconciliationResult("FLAT", "CLEAN_FLAT", snapshot)
    return ReconciliationResult("EXPOSED", "POSITION_OR_WORKING_ORDER_PRESENT", snapshot)
