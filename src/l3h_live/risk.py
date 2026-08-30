"""Independent L3H canary risk authority.

The profile is intentionally no broader than the frozen L3G experimental
policy.  It evaluates admissions only; it neither contacts NinjaTrader nor
changes broker state.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal

from .contracts import canonical_hash


@dataclass(frozen=True)
class LiveCanaryRiskProfile:
    schema: str = "lane-iii-phase-h-live-risk-canary-v1"
    profile_id: str = "l3h-live-risk-canary-v0"
    maximum_absolute_position: int = 1
    maximum_entry_quantity: int = 1
    maximum_pending_entries: int = 1
    maximum_canary_round_trips: int = 1
    pyramiding: bool = False
    averaging: bool = False
    same_event_reversal: bool = False
    protective_stop_distance_points: Decimal = Decimal("25.00")
    maximum_trade_risk_dollars: Decimal = Decimal("50.00")
    daily_loss_limit_dollars: Decimal = Decimal("200.00")
    maximum_entry_slippage_points: Decimal = Decimal("2.00")
    hard_flat_deadline: str = "15:58"
    session_timezone: str = "America/New_York"

    def __post_init__(self) -> None:
        if (self.schema, self.profile_id) != ("lane-iii-phase-h-live-risk-canary-v1", "l3h-live-risk-canary-v0"):
            raise ValueError("Unsupported L3H risk identity.")
        if (self.maximum_absolute_position, self.maximum_entry_quantity, self.maximum_pending_entries, self.maximum_canary_round_trips) != (1, 1, 1, 1):
            raise ValueError("L3H v0 permits one MNQ canary only.")
        if self.pyramiding or self.averaging or self.same_event_reversal:
            raise ValueError("Pyramiding, averaging, and same-event reversal are prohibited.")
        if self.protective_stop_distance_points > Decimal("25.00") or self.maximum_trade_risk_dollars > Decimal("50.00"):
            raise ValueError("L3H canary risk cannot be broader than frozen L3G risk.")
        if self.daily_loss_limit_dollars > Decimal("200.00") or self.maximum_entry_slippage_points > Decimal("2.00"):
            raise ValueError("L3H canary loss or slippage limit is too broad.")

    @property
    def configuration_hash(self) -> str:
        return canonical_hash(asdict(self))


class LiveRiskAuthority:
    def __init__(self, profile: LiveCanaryRiskProfile | None = None) -> None:
        self.profile = profile or LiveCanaryRiskProfile()
        self._canary_round_trips = 0
        self._locked_reason: str | None = None

    @property
    def locked_reason(self) -> str | None:
        return self._locked_reason

    def lock(self, reason: str) -> None:
        self._locked_reason = reason or "RISK_LOCKED"

    def admit_entry(self, *, position_quantity: int, pending_entries: int, expected_trade_risk: Decimal) -> tuple[bool, str]:
        if self._locked_reason:
            return False, self._locked_reason
        if abs(position_quantity) > 0:
            return False, "POSITION_NOT_FLAT"
        if pending_entries != 0:
            return False, "PENDING_ENTRY_EXISTS"
        if self._canary_round_trips >= self.profile.maximum_canary_round_trips:
            return False, "CANARY_EPOCH_CONSUMED"
        if expected_trade_risk > self.profile.maximum_trade_risk_dollars:
            return False, "TRADE_RISK_EXCEEDED"
        return True, "ADMITTED"

    def mark_round_trip_complete(self) -> None:
        self._canary_round_trips += 1
