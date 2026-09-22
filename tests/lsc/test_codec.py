from __future__ import annotations

from collections import OrderedDict
import hashlib
import json
from pathlib import Path
import struct
import unittest

from src.l3f_provider.ninjatrader_observation import NinjaTraderObservation
from src.lane_iii.contracts import canonical_hash
from src.lsc.codec import (
    FORMAT_MAGIC,
    FORMAT_VERSION,
    LSCCodecError,
    LSCContext,
    MAX_ENCODED_CONTEXT_BYTES,
    context_digest,
    decode_context,
    decode_observation,
    encode_context,
    encode_observation,
)


SESSION = "0123456789abcdef0123456789abcdef"


def context(**changes: object) -> LSCContext:
    values: dict[str, object] = {
        "source": "NINJATRADER",
        "provider": "LUCID_CQG",
        "environment": "PROP_SIM",
        "source_identity": "source-" + "1" * 64,
        "build_identity": "build-" + "2" * 64,
        "contract_identity": "MNQZ6-CME-2026-12",
        "contract_id": "MNQ DEC26-MNQZ6",
        "tick_size": "0.25",
        "session_id": SESSION,
        "generation": 7,
        "timestamp_origin": "2026-09-23T00:00:00.0000000Z",
    }
    values.update(changes)
    return LSCContext(**values)  # type: ignore[arg-type]


def observation(
    kind: str,
    sequence: int,
    payload: dict[str, object],
    **changes: object,
) -> NinjaTraderObservation:
    values: dict[str, object] = {
        "observation_id": f"nt-{SESSION}-{sequence}",
        "session_id": SESSION,
        "observation_type": kind,
        "ninja_receipt_time": f"2026-09-23T00:00:00.{sequence:07d}Z",
        "local_monotonic_sequence": sequence,
        "payload": payload,
        "provider_timestamp": "2026-09-22T18:00:00.1234567-06:00",
        "provider_sequence": sequence + 100,
        "exchange_timestamp": None,
    }
    values.update(changes)
    return NinjaTraderObservation(**values)  # type: ignore[arg-type]


def quote() -> NinjaTraderObservation:
    return observation(
        "QUOTE",
        1,
        {
            "contract_id": "MNQ DEC26-MNQZ6",
            "bid": 20000.0,
            "ask": "20000.25",
            "bid_size": 2,
            "ask_size": 3,
            "publication_policy": "BOUNDED_LATEST_STATE_2HZ",
            "unknown_extension": {
                "unicode": "snowman \u2603",
                "negative_zero": -0.0,
                "flags": [True, False, None],
            },
        },
    )


def trade() -> NinjaTraderObservation:
    return observation(
        "TRADE",
        2,
        {
            "contract_id": "MNQ DEC26-MNQZ6",
            "price": "20000.10",  # Deliberate lossless tick escape.
            "size": 1,
            "aggressor_side": "UNKNOWN",
            "aggressor_source": "BID_ASK_CLASSIFICATION",
            "bid_at_trade": "20000.00",
            "ask_at_trade": None,
            "derivation_quote_observation_id": f"nt-{SESSION}-1",
        },
        observation_id="provider-trade-id-2",
        ninja_receipt_time="2026-09-23T00:00:00.1234567Z",
    )


def depth() -> NinjaTraderObservation:
    return observation(
        "DEPTH",
        3,
        {
            "contract_id": "MNQ DEC26-MNQZ6",
            "bids": [
                {"price": 20000, "size": 5},
                {"price": "19999.75", "size": 4},
            ],
            "asks": [
                {"price": 20000.25, "size": 6},
                {"price": "20000.50", "size": 7},
            ],
            "operation": "Update",
            "side": "Bid",
            "mutation_price": 20000.0,
            "mutation_volume": 5,
            "mutation_position": 0,
            "is_reset": False,
            "publication_policy": "BOUNDED_LATEST_STATE_2HZ",
        },
        ninja_receipt_time="2026-09-23T00:00:01.0000001Z",
        provider_timestamp=None,
        exchange_timestamp="2026-09-23T00:00:00.999999999Z",
    )


class LSCCodecTests(unittest.TestCase):
    def assert_exact_observation(
        self,
        expected: NinjaTraderObservation,
        actual: NinjaTraderObservation,
    ) -> None:
        self.assertEqual(actual.observation_id, expected.observation_id)
        self.assertEqual(actual.session_id, expected.session_id)
        self.assertEqual(actual.observation_type, expected.observation_type)
        self.assertEqual(actual.ninja_receipt_time, expected.ninja_receipt_time)
        self.assertEqual(actual.local_monotonic_sequence, expected.local_monotonic_sequence)
        self.assertEqual(actual.provider_timestamp, expected.provider_timestamp)
        self.assertEqual(actual.provider_sequence, expected.provider_sequence)
        self.assertEqual(actual.exchange_timestamp, expected.exchange_timestamp)
        self.assertEqual(actual.account_alias, expected.account_alias)
        self.assertEqual(actual.account_class, expected.account_class)
        self.assertEqual(dict(actual.payload), dict(expected.payload))
        self.assertEqual(actual.wire_hash(), expected.wire_hash())
        self.assertEqual(
            struct.pack("<d", actual.payload.get("unknown_extension", {}).get("negative_zero", 0.0)),
            struct.pack("<d", expected.payload.get("unknown_extension", {}).get("negative_zero", 0.0)),
        )

    def test_context_is_canonical_bounded_and_digest_verified(self):
        value = context()
        encoded = encode_context(value)
        self.assertLess(len(encoded), MAX_ENCODED_CONTEXT_BYTES)
        self.assertEqual(decode_context(encoded), value)
        self.assertEqual(context_digest(encoded), encoded[-32:].hex())
        tampered = encoded[:-1] + bytes((encoded[-1] ^ 1,))
        with self.assertRaisesRegex(LSCCodecError, "context digest mismatch"):
            decode_context(tampered)
        with self.assertRaisesRegex(LSCCodecError, "byte limit"):
            context(source_identity="x" * 4_097)

    def test_quote_trade_and_depth_round_trip_exactly(self):
        frozen = encode_context(context())
        for event in (quote(), trade(), depth()):
            with self.subTest(kind=event.observation_type):
                encoded = encode_observation(event, frozen)
                self.assertEqual(encoded.observation_type, event.observation_type)
                self.assertEqual(encoded.payload_hash, canonical_hash(dict(event.payload)))
                self.assertEqual(encoded.wire_hash, event.wire_hash())
                self.assertEqual(encoded.context_digest, context_digest(frozen))
                self.assertEqual(encoded.event_digest, encoded.record_bytes[-32:].hex())
                self.assert_exact_observation(event, decode_observation(encoded.record_bytes, frozen))

    def test_timestamp_precision_is_not_truncated(self):
        event = depth()
        decoded = decode_observation(
            encode_observation(event, context()).record_bytes,
            context(),
        )
        self.assertEqual(decoded.ninja_receipt_time, "2026-09-23T00:00:01.0000001Z")
        self.assertEqual(decoded.exchange_timestamp, "2026-09-23T00:00:00.999999999Z")
        self.assertEqual(decoded.provider_timestamp, None)

        fallback = quote()
        self.assertEqual(fallback.provider_timestamp, "2026-09-22T18:00:00.1234567-06:00")
        decoded_fallback = decode_observation(
            encode_observation(fallback, context()).record_bytes,
            context(),
        )
        self.assertEqual(decoded_fallback.provider_timestamp, fallback.provider_timestamp)

    def test_key_order_does_not_change_canonical_record(self):
        first = quote()
        reordered = OrderedDict(reversed(list(dict(first.payload).items())))
        second = observation("QUOTE", 1, reordered)
        self.assertEqual(first.wire_hash(), second.wire_hash())
        self.assertEqual(
            encode_observation(first, context()).record_bytes,
            encode_observation(second, context()).record_bytes,
        )

    def test_price_tick_projection_and_escape_both_preserve_scalar_type(self):
        value = trade()
        decoded = decode_observation(
            encode_observation(value, context()).record_bytes,
            context(),
        )
        self.assertIs(type(decoded.payload["price"]), str)
        self.assertEqual(decoded.payload["price"], "20000.10")
        self.assertIs(type(decoded.payload["bid_at_trade"]), str)
        self.assertEqual(decoded.payload["bid_at_trade"], "20000.00")
        quote_decoded = decode_observation(
            encode_observation(quote(), context()).record_bytes,
            context(),
        )
        self.assertIs(type(quote_decoded.payload["bid"]), float)
        self.assertIs(type(quote_decoded.payload["ask"]), str)

    def test_context_substitution_is_refused(self):
        encoded = encode_observation(quote(), context())
        variants = (
            context(build_identity="build-" + "9" * 64),
            context(contract_identity="different-contract"),
            context(generation=8),
            context(source_identity="different-source"),
        )
        for replacement in variants:
            with self.subTest(replacement=replacement):
                with self.assertRaisesRegex(LSCCodecError, "context digest"):
                    decode_observation(encoded.record_bytes, replacement)
        with self.assertRaisesRegex(LSCCodecError, "session"):
            encode_observation(quote(), context(session_id="different-session"))

    def test_non_passive_and_wrong_contract_are_refused(self):
        non_passive = observation(
            "HEALTH", 4, {"contract_id": "MNQ DEC26-MNQZ6"},
        )
        with self.assertRaisesRegex(LSCCodecError, "only QUOTE, TRADE, and DEPTH"):
            encode_observation(non_passive, context())
        wrong = observation(
            "QUOTE", 5,
            {"contract_id": "MNQ SEP26", "bid": 1, "ask": 2},
        )
        with self.assertRaisesRegex(LSCCodecError, "contract"):
            encode_observation(wrong, context())

    def test_bit_flip_truncation_unknown_version_and_trailing_data_are_refused(self):
        encoded = encode_observation(quote(), context()).record_bytes
        tampered = bytearray(encoded)
        tampered[len(tampered) // 2] ^= 1
        with self.assertRaisesRegex(LSCCodecError, "event digest mismatch"):
            decode_observation(bytes(tampered), context())
        with self.assertRaises(LSCCodecError):
            decode_observation(encoded[:-1], context())

        content = bytearray(encoded[:-32])
        content[len(FORMAT_MAGIC)] = FORMAT_VERSION + 1
        digest = hashlib.sha256(b"BEELZEBUB_LSC_V1_EVENT\0" + content).digest()
        with self.assertRaisesRegex(LSCCodecError, "unknown LSC record version"):
            decode_observation(bytes(content) + digest, context())

        content = bytearray(encoded[:-32]) + b"\x00"
        digest = hashlib.sha256(b"BEELZEBUB_LSC_V1_EVENT\0" + content).digest()
        with self.assertRaises(LSCCodecError):
            decode_observation(bytes(content) + digest, context())

    def test_payload_hash_and_wire_hash_are_independently_verified(self):
        encoded = encode_observation(quote(), context()).record_bytes
        content = bytearray(encoded[:-32])
        # The final 64 content bytes are payload hash then wire hash.
        content[-64] ^= 1
        digest = hashlib.sha256(b"BEELZEBUB_LSC_V1_EVENT\0" + content).digest()
        with self.assertRaisesRegex(LSCCodecError, "canonical payload hash mismatch"):
            decode_observation(bytes(content) + digest, context())

        content = bytearray(encoded[:-32])
        content[-32] ^= 1
        digest = hashlib.sha256(b"BEELZEBUB_LSC_V1_EVENT\0" + content).digest()
        with self.assertRaisesRegex(LSCCodecError, "wire hash mismatch"):
            decode_observation(bytes(content) + digest, context())

    def test_nesting_and_integer_limits_are_enforced(self):
        nested: object = "leaf"
        for _ in range(34):
            nested = [nested]
        too_deep = observation(
            "QUOTE", 6,
            {"contract_id": "MNQ DEC26-MNQZ6", "bid": 1, "ask": 2, "nested": nested},
        )
        with self.assertRaisesRegex(LSCCodecError, "nesting"):
            encode_observation(too_deep, context())

        huge = observation(
            "TRADE", 7,
            {"contract_id": "MNQ DEC26-MNQZ6", "price": 1, "size": 1 << 4_097},
        )
        with self.assertRaisesRegex(LSCCodecError, "integer"):
            encode_observation(huge, context())

        non_json_type = observation(
            "QUOTE", 8,
            {
                "contract_id": "MNQ DEC26-MNQZ6",
                "bid": 1,
                "ask": 2,
                "tuple_is_not_json": (1, 2),
            },
        )
        with self.assertRaisesRegex(LSCCodecError, "unsupported type tuple"):
            encode_observation(non_json_type, context())

    def test_immutable_golden_vectors(self):
        fixture = json.loads(
            (Path(__file__).with_name("golden_vectors.json")).read_text(encoding="utf-8")
        )
        frozen = encode_context(context())
        self.assertEqual(frozen.hex(), fixture["context_hex"])
        self.assertEqual(context_digest(frozen), fixture["context_digest"])
        for name, event in (("quote", quote()), ("trade", trade()), ("depth", depth())):
            with self.subTest(name=name):
                encoded = encode_observation(event, frozen)
                expected = fixture["records"][name]
                self.assertEqual(encoded.record_bytes.hex(), expected["record_hex"])
                self.assertEqual(encoded.payload_hash, expected["payload_hash"])
                self.assertEqual(encoded.wire_hash, expected["wire_hash"])
                self.assertEqual(encoded.event_digest, expected["event_digest"])


if __name__ == "__main__":
    unittest.main()
