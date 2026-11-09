from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .models import VirtualTargetPosition


@dataclass(frozen=True)
class NetExecutionIntent:
    """Future venue-facing net order while keeping constituent sleeves auditable."""

    symbol: str
    signed_quantity: float
    sleeve_ids: tuple[str, ...]
    target_wallets: tuple[str, ...]


class ExecutionAggregator:
    """Converts sleeves into inert planning data; this module sends no orders.

    Phase D must remain an aggregation seam until it has durable exchange
    reconciliation, idempotent order state, partial-fill attribution,
    reduce-only/precision/minimum-notional controls, signing isolation, and a
    separately authorized control plane.  Do not add transport or credentials
    here as a shortcut around those requirements.
    """

    @staticmethod
    def net_open_sleeves(sleeves: Iterable[VirtualTargetPosition]) -> list[NetExecutionIntent]:
        grouped: dict[str, list[VirtualTargetPosition]] = {}
        for sleeve in sleeves:
            if sleeve.is_open:
                grouped.setdefault(sleeve.symbol, []).append(sleeve)
        intents: list[NetExecutionIntent] = []
        for symbol, constituents in sorted(grouped.items()):
            signed = sum(sleeve.quantity if sleeve.direction == "long" else -sleeve.quantity for sleeve in constituents)
            if abs(signed) > 1e-12:
                intents.append(NetExecutionIntent(
                    symbol=symbol, signed_quantity=signed,
                    sleeve_ids=tuple(sleeve.sleeve_id for sleeve in constituents),
                    target_wallets=tuple(sleeve.target_wallet for sleeve in constituents),
                ))
        return intents
