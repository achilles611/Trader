"""Fail-closed readiness classification for the isolated L3H runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Mapping

from .contracts import AccountClass, LiveCapability


class ReadinessGate(StrEnum):
    SOURCE_INSTALL = "SOURCE_INSTALL"
    ACCOUNT_RULES = "ACCOUNT_RULES"
    MARKET_DATA = "MARKET_DATA"
    BROKER_TRUTH = "BROKER_TRUTH"
    RISK_PROTECTION = "RISK_PROTECTION"
    EVIDENCE_STORAGE = "EVIDENCE_STORAGE"
    OPERATOR_UI = "OPERATOR_UI"


_ORDERED_GATES = tuple(ReadinessGate)


@dataclass(frozen=True)
class LiveReadiness:
    """A gate report where omission is a failure, never an implied pass."""

    gate_passes: Mapping[ReadinessGate, bool] = field(default_factory=dict)
    blockers: Mapping[ReadinessGate, tuple[str, ...]] = field(default_factory=dict)
    broker_position: str = "UNKNOWN"
    owned_working_orders: int | None = None
    foreign_or_unknown_orders: int | None = None
    reconciliation_fresh: bool = False

    def failed_gates(self) -> tuple[str, ...]:
        result: list[str] = []
        for gate in _ORDERED_GATES:
            if self.gate_passes.get(gate) is not True:
                reasons = self.blockers.get(gate) or ("GATE_NOT_PASSED",)
                result.extend(f"{gate.value}_{reason}" for reason in reasons)
        if self.broker_position != "FLAT":
            result.append("BROKER_POSITION_NOT_PROVEN_FLAT")
        if self.owned_working_orders != 0:
            result.append("OWNED_WORKING_ORDERS_NOT_PROVEN_ZERO")
        if self.foreign_or_unknown_orders != 0:
            result.append("FOREIGN_OR_UNKNOWN_ORDERS_NOT_PROVEN_ZERO")
        if not self.reconciliation_fresh:
            result.append("RECONCILIATION_NOT_FRESH")
        return tuple(dict.fromkeys(result))


def derive_terminal_status(capability: LiveCapability | None, readiness: LiveReadiness) -> str:
    """Return only a truthful disarmed terminal state or a precise blocker."""

    if capability is None:
        return "BLOCKED_CAPABILITY_MISSING"
    failures = readiness.failed_gates()
    if failures:
        return "BLOCKED_" + failures[0]
    if capability.account_class is AccountClass.PROVIDER_EVALUATION:
        return "PROVIDER_EVALUATION_READY_DISARMED"
    if capability.account_class in {AccountClass.PROVIDER_FUNDED, AccountClass.BROKERAGE_LIVE} and capability.live_capital:
        return "LIVE_READY_DISARMED"
    return "BLOCKED_LIVE_CAPITAL_CLASSIFICATION_DENIED"
