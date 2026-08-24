from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.l3f_provider.tradovate_observation import StreamHealth
from src.l3g_paper.contracts import PaperDecisionKind
from src.l3g_paper.policy import ExperimentalPaperPolicy


class ObservationFactory:
    def __init__(self, *, session: str = "paper-market-session", start: datetime | None = None) -> None:
        self.session = session
        self.start = start or datetime(2026, 8, 24, 14, 0, tzinfo=timezone.utc)
        self.sequence = 0

    def make(self, kind: str, payload: dict[str, object]) -> NinjaTraderObservation:
        self.sequence += 1
        timestamp = (self.start + timedelta(milliseconds=self.sequence * 100)).isoformat().replace("+00:00", "Z")
        return NinjaTraderObservation(
            f"paper-observation-{self.sequence}", self.session, kind, timestamp,
            self.sequence, payload, provider_timestamp=timestamp,
        )

    def quote(self, bid: Decimal | int) -> NinjaTraderObservation:
        price = Decimal(str(bid))
        return self.make("QUOTE", {"contract_id": "MNQ SEP26", "bid": str(price), "ask": str(price + Decimal("0.25")), "bid_size": 10, "ask_size": 10})

    def trade(self, quote: NinjaTraderObservation, price: Decimal | int, *, size: int = 2, source: str = "BID_ASK_CLASSIFICATION") -> NinjaTraderObservation:
        value = Decimal(str(price))
        return self.make("TRADE", {
            "contract_id": "MNQ SEP26", "price": str(value), "size": size,
            "aggressor_side": "UNKNOWN", "aggressor_source": source,
            "bid_at_trade": quote.payload["bid"] if source == "BID_ASK_CLASSIFICATION" else None,
            "ask_at_trade": quote.payload["ask"] if source == "BID_ASK_CLASSIFICATION" else None,
            "derivation_quote_observation_id": quote.observation_id if source == "BID_ASK_CLASSIFICATION" else None,
        })

    def depth(self, operation: str, volume: int, *, price: Decimal | int = 98) -> NinjaTraderObservation:
        value = Decimal(str(price))
        return self.make("DEPTH", {
            "contract_id": "MNQ SEP26", "bids": [{"price": str(value), "size": volume}],
            "asks": [{"price": str(value + 3), "size": 10}], "operation": operation,
            "side": "Bid", "mutation_price": str(value), "mutation_volume": volume,
            "mutation_position": 0, "is_reset": False,
        })


def warmed_bullish_policy() -> tuple[ExperimentalPaperPolicy, ObservationFactory, object]:
    policy = ExperimentalPaperPolicy()
    policy.on_transport_state(StreamHealth.HEALTHY)
    factory = ObservationFactory()
    policy.ingest(factory.make("CONNECTION", {"scope": "MARKET_DATA", "price_status": "Connected"}))
    last = None
    for price in (100, 99, 100):
        quote = factory.quote(price)
        policy.ingest(quote)
        last = policy.ingest(factory.trade(quote, price))
    for operation, volume in (("ADD", 10), ("UPDATE", 5), ("UPDATE", 10), ("UPDATE", 5), ("UPDATE", 11)):
        last = policy.ingest(factory.depth(operation, volume))
    assert last is not None and last.decision is PaperDecisionKind.LONG
    return policy, factory, last
