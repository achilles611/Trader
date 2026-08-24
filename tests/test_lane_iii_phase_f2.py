from __future__ import annotations

import ast
import json
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from src.lane_iii.market_data import (
    AggressorProvenance, AggressorSide, MarketDataSource, RawProviderEvent,
)
from src.l3f_provider.ninjatrader_observation import (
    AccountClass, LoopbackBridgeConfig, LoopbackNinjaTraderBridge,
    NinjaTraderAccountIsolation, NinjaTraderContract, NinjaTraderMarketDataAdapter,
    NinjaTraderHealthStream, NinjaTraderHealthTracker, NinjaTraderObservation,
    NinjaTraderObservationError, NinjaTraderSessionLedger,
)
from src.l3f_provider.tradovate_observation import ProviderErrorCode, StreamHealth


TIME = "2026-08-20T15:00:00Z"


def contract() -> NinjaTraderContract:
    return NinjaTraderContract("MNQ", "MNQ SEPT26", "2026-09", "CME", Decimal("0.25"), "MNQ SEPT26", Decimal("2"))


def wire(kind: str, sequence: int = 1, *, session: str = "nt-session-one", payload: dict[str, object] | None = None, account: dict[str, str] | None = None, provider_time: str | None = TIME) -> dict[str, object]:
    return {"schema": "lane-iii-phase-f2-ninjatrader-observation-v1", "observation_id": f"nt-{session}-{sequence}", "session_id": session, "observation_type": kind, "ninja_receipt_time": TIME, "local_monotonic_sequence": sequence, "provider_timestamp": provider_time, "provider_sequence": None, "exchange_timestamp": None, "account": account, "payload": payload or {"contract_id": "MNQ SEPT26"}}


class LaneIIIPhaseF2Tests(unittest.TestCase):
    def observation(self, kind="QUOTE", **changes) -> NinjaTraderObservation:
        value = wire(kind, **changes)
        return NinjaTraderObservation.from_wire(json.dumps(value))

    def raw(self, event: NinjaTraderObservation) -> RawProviderEvent:
        return RawProviderEvent(event.observation_id, MarketDataSource("NINJATRADER", "LUCID_CQG"), TIME, json.loads(json.dumps(wire(event.observation_type, event.local_monotonic_sequence, session=event.session_id, payload=dict(event.payload), provider_time=event.provider_timestamp))), None)

    def test_loopback_configuration_rejects_lan_and_bridge_has_no_send_surface(self):
        with self.assertRaises(NinjaTraderObservationError):
            LoopbackBridgeConfig(host="0.0.0.0")
        bridge = LoopbackNinjaTraderBridge()
        for write_verb in ("send", "dispatch", "CreateOrder", "Submit", "Change", "Cancel", "Flatten", "Liquidate", "Reverse"):
            self.assertFalse(hasattr(bridge, write_verb), write_verb)

    def test_wire_schema_never_accepts_inbound_command_types_or_account_ids(self):
        command = wire("COMMAND")
        with self.assertRaises(NinjaTraderObservationError):
            NinjaTraderObservation.from_wire(json.dumps(command))
        raw = wire("ACCOUNT", payload={"account_id": "must-not-be-present"})
        with self.assertRaisesRegex(NinjaTraderObservationError, "MALFORMED_PROVIDER_PAYLOAD"):
            NinjaTraderObservation.from_wire(json.dumps(raw))

    def test_lucid_and_sim101_are_explicitly_isolated(self):
        isolation = NinjaTraderAccountIsolation()
        lucid = self.observation("ACCOUNT", account={"alias": "Lucid25kflex01", "class": "PROVIDER_EVALUATION"})
        sim = self.observation("ACCOUNT", sequence=2, account={"alias": "Sim101", "class": "LOCAL_SIMULATION"})
        isolation.record(lucid, "lucid-state")
        isolation.record(sim, "sim-state")
        self.assertEqual(isolation.authoritative(), "lucid-state")
        self.assertEqual(isolation.local_simulation(), "sim-state")
        only_sim = NinjaTraderAccountIsolation()
        only_sim.record(sim, "sim-state")
        with self.assertRaisesRegex(NinjaTraderObservationError, "ACCOUNT_NOT_FOUND"):
            only_sim.authoritative()

    def test_account_class_confusion_or_missing_binding_fails_closed(self):
        isolation = NinjaTraderAccountIsolation()
        bad = self.observation("ACCOUNT", account={"alias": "Lucid25kflex01", "class": "LOCAL_SIMULATION"})
        with self.assertRaisesRegex(NinjaTraderObservationError, "ENVIRONMENT_MISMATCH"):
            isolation.record(bad, object())
        with self.assertRaisesRegex(NinjaTraderObservationError, "ACCOUNT_NOT_FOUND"):
            isolation.authoritative()

    def test_contract_must_be_native_sep26_mnq_cme_with_mnq_tick(self):
        with self.assertRaisesRegex(NinjaTraderObservationError, "CONTRACT_NOT_FOUND"):
            NinjaTraderContract("MNQ", "MNQ DEC26", "2026-12", "CME", Decimal("0.25"), "x")
        with self.assertRaisesRegex(NinjaTraderObservationError, "CONTRACT_NOT_FOUND"):
            NinjaTraderContract("MNQ", "MNQ SEPT26", "2026-09", "CME", Decimal("0.5"), "x")

    def test_quotes_trades_and_aggregated_depth_convert_to_frozen_l3b(self):
        adapter = NinjaTraderMarketDataAdapter(contract())
        quote = self.observation("QUOTE", payload={"contract_id": "MNQ SEPT26", "bid": "20000.00", "ask": "20000.25", "bid_size": 2, "ask_size": 3})
        trade = self.observation("TRADE", sequence=2, payload={"contract_id": "MNQ SEPT26", "price": "20000.25", "size": 1, "aggressor_side": "UNKNOWN"})
        depth = self.observation("DEPTH", sequence=3, payload={"contract_id": "MNQ SEPT26", "bids": [{"price": "20000.00", "size": 2}], "asks": [{"price": "20000.25", "size": 3}]})
        self.assertEqual(adapter.normalize(self.raw(quote))[0].header.stream.value, "QUOTE")
        self.assertEqual(adapter.normalize(self.raw(trade))[0].header.stream.value, "TRADE")
        book = adapter.normalize(self.raw(depth))[0]
        self.assertEqual(book.bids[0].price, Decimal("20000.00"))
        self.assertIsNone(book.header.provider_sequence)

    def test_native_aggressor_requires_explicit_native_provenance(self):
        adapter = NinjaTraderMarketDataAdapter(contract())
        native = self.observation("TRADE", payload={
            "contract_id": "MNQ SEPT26", "price": "20000.25", "size": 1,
            "aggressor_side": "BUY", "aggressor_source": "PROVIDER_NATIVE",
        })
        event = adapter.normalize(self.raw(native))[0]
        self.assertIs(event.aggressor_side, AggressorSide.BUY)
        self.assertIs(event.aggressor_provenance, AggressorProvenance.PROVIDER)

        unproven = self.observation("TRADE", sequence=2, payload={
            "contract_id": "MNQ SEPT26", "price": "20000.25", "size": 1,
            "aggressor_side": "BUY",
        })
        event = adapter.normalize(self.raw(unproven))[0]
        self.assertIs(event.aggressor_side, AggressorSide.UNKNOWN)
        self.assertIs(event.aggressor_provenance, AggressorProvenance.UNAVAILABLE)

    def test_quote_derived_aggressor_classification_is_fail_closed_and_no_lookahead(self):
        def classify(
            price: str, bid: object = "20000.00", ask: object = "20000.25", *,
            quote_first: bool = True, quote_time: str = TIME, trade_time: str = TIME,
            quote_reference: str | None = None, quote_ask: str = "20000.25",
        ):
            adapter = NinjaTraderMarketDataAdapter(contract())
            quote = self.observation("QUOTE", provider_time=quote_time, payload={
                "contract_id": "MNQ SEPT26", "bid": "20000.00", "ask": quote_ask,
                "bid_size": 2, "ask_size": 3,
            })
            if quote_first:
                adapter.normalize(self.raw(quote))
            trade = self.observation("TRADE", sequence=2, provider_time=trade_time, payload={
                "contract_id": "MNQ SEPT26", "price": price, "size": 1,
                "aggressor_side": "UNKNOWN", "aggressor_source": "BID_ASK_CLASSIFICATION",
                "bid_at_trade": bid, "ask_at_trade": ask,
                "derivation_quote_observation_id": quote.observation_id if quote_reference is None else quote_reference,
            })
            result = adapter.normalize(self.raw(trade))[0]
            if not quote_first:
                adapter.normalize(self.raw(quote))
            return result

        cases = (
            ("at_ask", "20000.25", "20000.00", "20000.25", AggressorSide.BUY, {}),
            ("above_ask", "20000.50", "20000.00", "20000.25", AggressorSide.BUY, {}),
            ("at_bid", "20000.00", "20000.00", "20000.25", AggressorSide.SELL, {}),
            ("below_bid", "19999.75", "20000.00", "20000.25", AggressorSide.SELL, {}),
            ("inside_spread", "20000.25", "20000.00", "20000.50", AggressorSide.UNKNOWN, {"quote_ask": "20000.50"}),
            ("no_bid", "20000.25", None, "20000.25", AggressorSide.UNKNOWN, {}),
            ("no_ask", "20000.00", "20000.00", None, AggressorSide.UNKNOWN, {}),
            ("locked", "20000.00", "20000.00", "20000.00", AggressorSide.UNKNOWN, {}),
            ("crossed", "20000.25", "20000.25", "20000.00", AggressorSide.UNKNOWN, {}),
        )
        for name, price, bid, ask, expected, options in cases:
            with self.subTest(name=name):
                event = classify(price, bid, ask, **options)
                self.assertIs(event.aggressor_side, expected)
                self.assertIs(
                    event.aggressor_provenance,
                    AggressorProvenance.QUOTE_DERIVED if expected is not AggressorSide.UNKNOWN else AggressorProvenance.UNAVAILABLE,
                )

        stale = classify("20000.25", quote_time="2026-08-20T14:59:59Z")
        self.assertIs(stale.aggressor_side, AggressorSide.UNKNOWN)
        wrong_reference = classify("20000.25", quote_reference="future-or-unrelated-quote")
        self.assertIs(wrong_reference.aggressor_side, AggressorSide.UNKNOWN)
        quote_after_trade = classify("20000.25", quote_first=False)
        self.assertIs(quote_after_trade.aggressor_side, AggressorSide.UNKNOWN)

    def test_depth_mutation_provenance_is_validated_but_remains_unsequenced_snapshot(self):
        base = {
            "contract_id": "MNQ SEPT26",
            "bids": [{"price": "20000.00", "size": 2}],
            "asks": [{"price": "20000.25", "size": 3}],
            "operation": "Add", "side": "Bid", "mutation_price": "20000.00",
            "mutation_volume": 2, "mutation_position": 0, "is_reset": False,
        }
        for index, changes in enumerate((
            {"operation": "Add", "side": "Bid"},
            {"operation": "Update", "side": "Ask", "mutation_price": "20000.25", "mutation_volume": 3},
            {"operation": "Remove", "side": "Bid", "mutation_volume": 0, "bids": [{"price": "19999.75", "size": 1}]},
            {"operation": "Add", "side": "Ask", "mutation_price": "20000.25", "mutation_volume": 3, "is_reset": True},
        ), start=1):
            with self.subTest(changes=changes):
                payload = base | changes
                adapter = NinjaTraderMarketDataAdapter(contract())
                event = adapter.normalize(self.raw(self.observation("DEPTH", sequence=index, payload=payload)))[0]
                self.assertEqual(type(event).__name__, "BookSnapshotEvent")
                self.assertIsNone(event.header.provider_sequence)

        invalid = (
            {"operation": "Replace"},
            {"side": "Both"},
            {"mutation_price": "20000.10"},
            {"mutation_volume": -1},
            {"mutation_position": -1},
            {"mutation_position": 2},
            {"is_reset": "false"},
        )
        for index, changes in enumerate(invalid, start=10):
            with self.subTest(invalid=changes):
                payload = base | changes
                adapter = NinjaTraderMarketDataAdapter(contract())
                with self.assertRaisesRegex(NinjaTraderObservationError, "MALFORMED_PROVIDER_PAYLOAD"):
                    adapter.normalize(self.raw(self.observation("DEPTH", sequence=index, payload=payload)))
        for index, missing_field in enumerate(("mutation_price", "mutation_volume"), start=20):
            with self.subTest(missing_field=missing_field):
                incomplete = dict(base)
                del incomplete[missing_field]
                with self.assertRaisesRegex(NinjaTraderObservationError, "MALFORMED_PROVIDER_PAYLOAD"):
                    NinjaTraderMarketDataAdapter(contract()).normalize(
                        self.raw(self.observation("DEPTH", sequence=index, payload=incomplete))
                    )

    def test_wrong_contract_malformed_value_and_missing_timestamp_fail_closed(self):
        adapter = NinjaTraderMarketDataAdapter(contract())
        wrong = self.observation("QUOTE", payload={"contract_id": "MNQ DEC26", "bid": "20000", "ask": "20000.25", "bid_size": 1, "ask_size": 1})
        with self.assertRaisesRegex(NinjaTraderObservationError, "CONTRACT_NOT_FOUND"):
            adapter.normalize(self.raw(wrong))
        malformed = self.observation("QUOTE", payload={"contract_id": "MNQ SEPT26", "bid": "20000.1", "ask": "20000.25", "bid_size": 1, "ask_size": 1})
        with self.assertRaises(NinjaTraderObservationError):
            adapter.normalize(self.raw(malformed))
        no_provider_time = self.observation("TRADE", provider_time=None, payload={"contract_id": "MNQ SEPT26", "price": "20000.25", "size": 1})
        event = adapter.normalize(self.raw(no_provider_time))[0]
        self.assertIsNone(event.header.timestamps.provider_time)

    def test_duplicate_late_and_prior_session_callbacks_are_safe(self):
        ledger = NinjaTraderSessionLedger()
        first = self.observation(sequence=1)
        self.assertTrue(ledger.admit(first))
        self.assertFalse(ledger.admit(first))
        late = self.observation(sequence=0)
        with self.assertRaisesRegex(NinjaTraderObservationError, "STALE_PROVIDER_STATE"):
            ledger.admit(late)
        restarted = self.observation(sequence=1, session="nt-session-two")
        self.assertTrue(ledger.admit(restarted))
        old = self.observation(sequence=2, session="nt-session-one")
        with self.assertRaisesRegex(NinjaTraderObservationError, "STALE_PROVIDER_STATE"):
            ledger.admit(old)

    def test_market_and_account_channels_keep_independent_sessions(self):
        ledger = NinjaTraderSessionLedger()
        market = self.observation("QUOTE", sequence=1, session="market-session")
        account = self.observation("ACCOUNT", sequence=1, session="account-session", account={"alias": "Lucid25kflex01", "class": "PROVIDER_EVALUATION"})
        self.assertTrue(ledger.admit(market))
        self.assertTrue(ledger.admit(account))
        self.assertTrue(ledger.admit(self.observation("TRADE", sequence=2, session="market-session")))
        self.assertTrue(ledger.admit(self.observation("POSITION", sequence=2, session="account-session", account={"alias": "Lucid25kflex01", "class": "PROVIDER_EVALUATION"})))
        self.assertTrue(ledger.admit(self.observation("QUOTE", sequence=1, session="market-session-two")))
        with self.assertRaisesRegex(NinjaTraderObservationError, "STALE_PROVIDER_STATE"):
            ledger.admit(self.observation("TRADE", sequence=3, session="market-session"))

    def test_bridge_frame_limits_encoding_and_idempotency(self):
        bridge = LoopbackNinjaTraderBridge(LoopbackBridgeConfig(port=48137, maximum_frame_bytes=1024))
        accepted = bridge.accept_observation(json.dumps(wire("HEALTH")).encode())
        self.assertIsNotNone(accepted)
        self.assertIsNone(bridge.accept_observation(json.dumps(wire("HEALTH")).encode()))
        with self.assertRaises(NinjaTraderObservationError):
            bridge.decode_frame(b"{" * 1025)

    def test_ninjascript_sources_have_no_execution_api_or_inbound_command_deserializer(self):
        root = Path(__file__).parents[1] / "ninjatrader" / "NinjaScript"
        source = "\n".join(path.read_text(encoding="utf-8") for path in root.rglob("*.cs"))
        forbidden = (".Submit(", ".Change(", ".Cancel(", ".Flatten(", ".Liquidate(", ".Reverse(", ".CreateOrder(", "AtmStrategy", "Stream.Read", "NetworkStream.Read", "CommandType", "DeserializeCommand")
        self.assertFalse([token for token in forbidden if token in source])
        self.assertIn("IPAddress.Loopback", source)
        self.assertNotIn("IPAddress.Any", source)
        self.assertNotIn("TcpListener", source)

    def test_ninjascript_trade_provenance_uses_same_callback_quote_and_no_future_data(self):
        root = Path(__file__).parents[1] / "ninjatrader" / "NinjaScript"
        observer = (root / "Indicators" / "BeelzebubReadOnlyMarketObserver.cs").read_text(encoding="utf-8")
        outbound = (root / "AddOns" / "BeelzebubReadOnlyAddOn.cs").read_text(encoding="utf-8")
        self.assertIn("double bidAtTrade = e.Bid", observer)
        self.assertIn("double askAtTrade = e.Ask", observer)
        self.assertIn("BID_ASK_CLASSIFICATION", observer)
        self.assertIn("derivation_quote_observation_id", observer)
        self.assertLess(observer.index('Publish("QUOTE"'), observer.index('Publish("TRADE"'))
        self.assertNotIn("OnBarUpdate", observer)
        self.assertNotIn("Close[", observer)
        self.assertNotIn("GetCurrentBid", observer)
        self.assertIn("public static string Publish", outbound)
        self.assertIn("OnConnectionStatusUpdate", observer)
        self.assertIn("ClearMarketState();", observer)
        self.assertIn("if (e.IsReset)", observer)
        self.assertIn("e.Time - bestBidTime <= TimeSpan.FromSeconds(10)", observer)
        self.assertIn("e.Time - bestAskTime <= TimeSpan.FromSeconds(10)", observer)
        self.assertIn("bid_source_time", observer)
        self.assertIn("ask_source_time", observer)
        for field in ("mutation_price", "mutation_volume", "mutation_position", "is_reset"):
            self.assertIn(field, observer)

    def test_no_frozen_lane_iii_source_is_changed_or_networked(self):
        root = Path(__file__).parents[1] / "src" / "lane_iii"
        for path in root.glob("*.py"):
            self.assertNotIn("ninjatrader", path.read_text(encoding="utf-8").lower())

    def test_independent_health_streams_and_stale_account_truth_fail_closed(self):
        tracker = NinjaTraderHealthTracker()
        for stream in NinjaTraderHealthStream:
            tracker.mark(stream, StreamHealth.HEALTHY, TIME)
        self.assertTrue(tracker.snapshot().authoritative)
        result = tracker.assess_stale("2026-08-20T15:00:31Z", timedelta(seconds=30))
        self.assertFalse(result.authoritative)
        self.assertIs(result.streams[NinjaTraderHealthStream.ACCOUNT_STREAM], StreamHealth.STALE)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
