"""Broker truth is explicit: incomplete or stale facts are UNKNOWN, not FLAT."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .contracts import AccountClass, LiveCapability, NATIVE_INSTRUMENT, parse_utc


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


@dataclass(frozen=True)
class ReconciliationResult:
    state: str
    reason: str
    snapshot: BrokerSnapshot


def reconcile(capability: LiveCapability, snapshot: BrokerSnapshot) -> ReconciliationResult:
    if not snapshot.connection_healthy:
        return ReconciliationResult("UNKNOWN", "BROKER_CONNECTION_UNHEALTHY", snapshot)
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
