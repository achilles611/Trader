from __future__ import annotations

import unittest

from src.copytrade.aggregation import ExecutionAggregator, NetExecutionIntent
from src.copytrade.models import VirtualTargetPosition


def sleeve(sleeve_id: str, *, direction: str = "long", quantity: float = 1.0, closed: bool = False) -> VirtualTargetPosition:
    from src.copytrade.models import utc_now
    now = utc_now()
    return VirtualTargetPosition(
        sleeve_id=sleeve_id, target_wallet="0x1111111111111111111111111111111111111111", campaign_id=None,
        symbol="BTC", direction=direction, quantity=quantity, entry_price=100.0, allocated_capital=100.0,
        remaining_capital=100.0, entry_fee=0.0, opened_at=now, updated_at=now, closed_at=now if closed else None,
    )


class PhaseDInertAggregationTests(unittest.TestCase):
    def test_aggregation_returns_plain_intents_and_exposes_no_submission_path(self) -> None:
        intents = ExecutionAggregator.net_open_sleeves((
            sleeve("long"), sleeve("short", direction="short", quantity=0.4), sleeve("closed", closed=True),
        ))
        self.assertEqual(intents, [NetExecutionIntent(
            symbol="BTC", signed_quantity=0.6, sleeve_ids=("long", "short"),
            target_wallets=("0x1111111111111111111111111111111111111111",) * 2,
        )])
        self.assertFalse(any(name in ExecutionAggregator.__dict__ for name in ("submit", "place_order", "send_order", "sign")))
        self.assertFalse(any(name in NetExecutionIntent.__dict__ for name in ("api_key", "private_key", "signature", "order_id")))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
