from __future__ import annotations

import ast
import json
import unittest
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from src.lane_iii.market_data import MarketDataSource, RawProviderEvent
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
