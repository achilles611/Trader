import hashlib
import hmac
import json
from pathlib import Path
from decimal import Decimal
import unittest

from src.lane_iii.contracts import canonical_hash
from src.lane_iii.market_data import MarketDataSource, RawProviderEvent
from src.l3f_provider.ninjatrader_observation import (
    L3F2_SCHEMA,
    L3F3_SCHEMA,
    NINJATRADER_ATTESTATION_KEY_CONTEXT,
    NINJATRADER_ATTESTATION_PROTOCOL,
    LoopbackNinjaTraderBridge,
    NinjaTraderContract,
    NinjaTraderMarketDataAdapter,
    NinjaTraderObservationError,
    expected_observer_source_fingerprint,
)


ROOT_KEY = b"observation-test-root-key-material-32bytes!!"
SOURCE_HASH = expected_observer_source_fingerprint()


def signed_record(kind="TRADE", sequence=1, payload=None):
    payload = payload or {
        "contract_id": "MNQ SEP26", "price": 20000, "size": 2,
        "aggressor_side": "UNKNOWN", "aggressor_source": "UNKNOWN",
        "source_event_sequence": sequence,
        "publication_policy": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
    }
    streams = {"TRADE": "MARKET_DATA_LAST", "QUOTE": "MARKET_DATA_BID_ASK", "DEPTH": "MARKET_DEPTH"}
    value = {
        "schema": L3F3_SCHEMA, "observation_id": f"nt-session-{sequence}",
        "session_id": "session", "observation_type": kind,
        "ninja_receipt_time": "2026-09-11T12:30:00Z",
        "local_monotonic_sequence": sequence, "provider_timestamp": "2026-09-11T12:29:59Z",
        "provider_sequence": None, "exchange_timestamp": None, "account": None,
        "payload": payload, "payload_hash": canonical_hash(payload),
        "producer": {
            "protocol_version": NINJATRADER_ATTESTATION_PROTOCOL,
            "source_fingerprint": SOURCE_HASH, "build_fingerprint": "a" * 64,
            "build_timestamp": "2026-09-11T12:00:00Z", "source_stream": streams[kind],
            "publication_policy": payload["publication_policy"],
        },
    }
    derived = hmac.new(ROOT_KEY, NINJATRADER_ATTESTATION_KEY_CONTEXT, hashlib.sha256).digest()
    value["signature"] = hmac.new(
        derived, json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(), hashlib.sha256,
    ).hexdigest()
    return value


class ObservationAttestationTests(unittest.TestCase):
    def test_bridge_verifies_source_build_payload_and_signature(self):
        bridge = LoopbackNinjaTraderBridge(attestation_key=ROOT_KEY, require_attestation=True)
        admitted = bridge.accept_observation((json.dumps(signed_record()) + "\n").encode())
        self.assertTrue(admitted.signature_verified)
        self.assertEqual(admitted.producer.source_fingerprint, SOURCE_HASH)
        self.assertEqual(admitted.producer.build_fingerprint, "a" * 64)
        unsigned = admitted.unsigned_wire_bytes()
        wire_hash = admitted.wire_hash()
        self.assertIs(admitted.unsigned_wire_bytes(), unsigned)
        self.assertIs(admitted.wire_hash(), wire_hash)
        self.assertEqual(
            wire_hash,
            canonical_hash({
                **admitted.unsigned_wire_payload(),
                "signature": admitted.signature,
            }),
        )

    def test_tamper_and_unsigned_legacy_are_refused_when_required(self):
        bridge = LoopbackNinjaTraderBridge(attestation_key=ROOT_KEY, require_attestation=True)
        tampered = signed_record()
        tampered["payload"]["size"] = 3
        with self.assertRaises(NinjaTraderObservationError):
            bridge.accept_observation(json.dumps(tampered).encode())
        legacy = {key: value for key, value in signed_record().items() if key not in {"payload_hash", "producer", "signature"}}
        legacy["schema"] = L3F2_SCHEMA
        with self.assertRaises(NinjaTraderObservationError):
            bridge.accept_observation(json.dumps(legacy).encode())

    def test_depth_order_and_cross_are_not_hidden_by_sorting(self):
        adapter = NinjaTraderMarketDataAdapter(
            NinjaTraderContract("MNQ", "MNQ SEP26", "2026-09", "CME", Decimal("0.25"), "MNQ SEP26"),
            attestation_key=ROOT_KEY, require_attestation=True,
        )
        for bids, asks in (
            ([{"price": 100, "size": 1}, {"price": 101, "size": 1}], [{"price": 102, "size": 1}]),
            ([{"price": 103, "size": 1}], [{"price": 102, "size": 1}]),
        ):
            payload = {
                "contract_id": "MNQ SEP26", "bids": bids, "asks": asks,
                "operation": "Update", "side": "Bid", "mutation_price": bids[0]["price"],
                "mutation_volume": 1, "mutation_position": 0, "is_reset": False,
                "source_event_sequence": 1,
                "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
            }
            value = signed_record("DEPTH", payload=payload)
            raw = RawProviderEvent("raw", MarketDataSource("NINJATRADER", "LUCID_CQG"),
                                   "2026-09-11T12:30:00Z", value)
            with self.assertRaises(NinjaTraderObservationError):
                adapter.normalize(raw)

    def test_canonical_native_snapshot_retains_original_mutation_position(self):
        adapter = NinjaTraderMarketDataAdapter(
            NinjaTraderContract("MNQ", "MNQ SEP26", "2026-09", "CME", Decimal("0.25"), "MNQ SEP26"),
            attestation_key=ROOT_KEY, require_attestation=True,
        )
        payload = {
            "contract_id": "MNQ SEP26",
            "bids": [{"price": 100, "size": 3}, {"price": 99.75, "size": 2}],
            "asks": [{"price": 100.25, "size": 4}, {"price": 100.5, "size": 1}],
            "operation": "Update", "side": "Bid", "mutation_price": 100,
            "mutation_volume": 3, "mutation_position": 7, "is_reset": False,
            "snapshot_ordering": "PRICE_CANONICAL_BEST_TO_WORST_V1",
            "source_event_sequence": 1,
            "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
        }
        value = signed_record("DEPTH", payload=payload)
        raw = RawProviderEvent(
            "raw", MarketDataSource("NINJATRADER", "LUCID_CQG"),
            "2026-09-11T12:30:00Z", value,
        )
        normalized = adapter.normalize(raw)
        self.assertEqual([level.price for level in normalized[0].bids], [Decimal("100"), Decimal("99.75")])
        self.assertEqual([level.price for level in normalized[0].asks], [Decimal("100.25"), Decimal("100.5")])

    def test_checked_in_native_sources_are_event_driven_and_positional(self):
        root = Path(__file__).resolve().parents[1]
        addon = (root / "ninjatrader/NinjaScript/AddOns/BeelzebubReadOnlyAddOn.cs").read_text()
        indicator = (root / "ninjatrader/NinjaScript/Indicators/BeelzebubReadOnlyMarketObserver.cs").read_text()
        for text in (addon, indicator):
            self.assertIn("EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1", text)
            self.assertIn("EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1", text)
            self.assertIn("ShiftKnownPositions(side, update.Position, true)", text)
            self.assertIn("side[update.Position]", text)
            self.assertIn("side.Remove(update.Position)", text)
            self.assertIn("ShiftKnownPositions(side, update.Position, false)", text)
            self.assertIn("DepthPrefix", text)

    def test_bounded_native_positional_snapshot_allows_a_deeper_mutation(self):
        adapter = NinjaTraderMarketDataAdapter(
            NinjaTraderContract("MNQ", "MNQ SEP26", "2026-09", "CME", Decimal("0.25"), "MNQ SEP26"),
            attestation_key=ROOT_KEY, require_attestation=True,
        )
        payload = {
            "contract_id": "MNQ SEP26",
            "bids": [{"price": 100, "size": 3}, {"price": 99.75, "size": 2}],
            "asks": [{"price": 100.25, "size": 4}, {"price": 100.5, "size": 1}],
            "operation": "Update", "side": "Bid", "mutation_price": 98,
            "mutation_volume": 7, "mutation_position": 10, "is_reset": False,
            "snapshot_ordering": "NATIVE_POSITIONAL_BEST_TO_WORST_V1",
            "source_event_sequence": 1,
            "publication_policy": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
        }
        value = signed_record("DEPTH", payload=payload)
        raw = RawProviderEvent(
            "raw", MarketDataSource("NINJATRADER", "LUCID_CQG"),
            "2026-09-11T12:30:00Z", value,
        )
        self.assertEqual(len(adapter.normalize(raw)), 1)

        self.assertEqual(SOURCE_HASH, "1825e9890fe54a71f9b495e778e07c9e523ca1b36f66569b21f9ca4549102b73")


if __name__ == "__main__":
    unittest.main()
