from __future__ import annotations

from dataclasses import replace
import hashlib
import hmac
import json
from pathlib import Path
import unittest

from src.l3f_provider.ninjatrader_observation import (
    L3F2_SCHEMA,
    L3F3_SCHEMA,
    NINJATRADER_ATTESTATION_KEY_CONTEXT,
    NINJATRADER_ATTESTATION_PROTOCOL,
    NinjaTraderObservation,
    NinjaTraderProducerAttestation,
    expected_observer_source_fingerprint,
)
from src.lane_iii.contracts import canonical_hash
from src.lsc.codec import (
    LSCCodecError,
    LSCContext,
    context_digest,
    decode_context,
    decode_observation,
    encode_context,
    encode_observation,
)


SESSION = "0123456789abcdef0123456789abcdef"
ROOT_KEY = b"lsc-current-observation-root-key-material!!"
SOURCE_HASH = expected_observer_source_fingerprint()
BUILD_HASH = "2" * 64
NATIVE_CONTRACT = "MNQ DEC26"
CANONICAL_CONTRACT = "MNQZ6"
STREAMS = {
    "QUOTE": "MARKET_DATA_BID_ASK",
    "TRADE": "MARKET_DATA_LAST",
    "DEPTH": "MARKET_DEPTH",
}
POLICIES = {
    "QUOTE": "EVENT_DRIVEN_SAME_CALLBACK_QUOTE_V1",
    "TRADE": "EVENT_DRIVEN_EVERY_LAST_CALLBACK_V1",
    "DEPTH": "EVENT_DRIVEN_EVERY_DEPTH_MUTATION_V1",
}


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("utf-8")


def _payload(kind: str, sequence: int) -> dict[str, object]:
    common: dict[str, object] = {
        "contract_id": NATIVE_CONTRACT,
        "source_generation": 7,
        "source_event_sequence": sequence + 1000,
        "observer_queue_overflow_frames": 0,
        "observer_transport_failed_frames": 0,
        "publication_policy": POLICIES[kind],
    }
    if kind == "QUOTE":
        common.update({
            "bid": 20000.0,
            "ask": "20000.25",
            "bid_size": 2,
            "ask_size": 3,
            "unknown_extension": {
                "unicode": "snowman \u2603",
                "negative_zero": -0.0,
                "flags": [True, False, None],
            },
        })
    elif kind == "TRADE":
        common.update({
            "price": "20000.10",
            "size": 1,
            "aggressor_side": "UNKNOWN",
            "aggressor_source": "PROVIDER_NATIVE",
            "derivation_quote_observation_id": f"nt-{SESSION}-1",
        })
    elif kind == "DEPTH":
        common.update({
            "operation": "Update",
            "side": "Bid",
            "mutation_price": 20000.0,
            "mutation_volume": 5,
            "mutation_position": 0,
            "is_reset": False,
            "mutation_resolved_price": 20000.0,
            "snapshot_ordering": "NATIVE_POSITIONAL_BEST_TO_WORST_V1",
            "position_bids": [
                {"position": 0, "price": 20000.0, "size": 5},
                {"position": 1, "price": "19999.75", "size": 3},
            ],
            "position_asks": [
                {"position": 0, "price": "20000.25", "size": 2},
                {"position": 1, "price": 20000.5, "size": 6},
            ],
            "bids": [
                {"price": 20000.0, "size": 5},
                {"price": "19999.75", "size": 3},
            ],
            "asks": [
                {"price": "20000.25", "size": 2},
                {"price": 20000.5, "size": 6},
            ],
        })
    else:  # pragma: no cover - helper guard
        raise AssertionError(kind)
    return common


def observation(
    kind: str,
    sequence: int,
    *,
    payload: dict[str, object] | None = None,
) -> NinjaTraderObservation:
    native_payload = payload if payload is not None else _payload(kind, sequence)
    producer = NinjaTraderProducerAttestation(
        protocol_version=NINJATRADER_ATTESTATION_PROTOCOL,
        source_fingerprint=SOURCE_HASH,
        build_fingerprint=BUILD_HASH,
        build_timestamp="2026-09-22T23:59:59.1234567Z",
        source_stream=STREAMS[kind],
        publication_policy=POLICIES[kind],
    )
    unsigned = {
        "schema": L3F3_SCHEMA,
        "observation_id": f"nt-{SESSION}-{sequence}",
        "session_id": SESSION,
        "observation_type": kind,
        "ninja_receipt_time": f"2026-09-23T00:00:00.{sequence:07d}Z",
        "local_monotonic_sequence": sequence,
        "provider_timestamp": "2026-09-22T18:00:00.1234567-06:00",
        "provider_sequence": sequence + 100,
        "exchange_timestamp": None,
        "account": None,
        "payload": native_payload,
        "payload_hash": canonical_hash(native_payload),
        "producer": producer.payload(),
    }
    signing_key = hmac.new(
        ROOT_KEY, NINJATRADER_ATTESTATION_KEY_CONTEXT, hashlib.sha256,
    ).digest()
    signature = hmac.new(signing_key, _canonical_bytes(unsigned), hashlib.sha256).hexdigest()
    wire = _canonical_bytes({**unsigned, "signature": signature}).decode("utf-8")
    return NinjaTraderObservation.from_wire(wire).verify_attestation(
        ROOT_KEY, expected_source_fingerprint=SOURCE_HASH,
    )


def context(event: NinjaTraderObservation, **changes: object) -> LSCContext:
    assert event.producer is not None
    values: dict[str, object] = {
        "observation_schema": event.schema,
        "source": event.source,
        "provider": event.provider,
        "environment": event.environment,
        "producer_protocol_version": event.producer.protocol_version,
        "producer_source_fingerprint": event.producer.source_fingerprint,
        "producer_build_fingerprint": event.producer.build_fingerprint,
        "producer_build_timestamp": event.producer.build_timestamp,
        "native_contract_id": NATIVE_CONTRACT,
        "canonical_contract_id": CANONICAL_CONTRACT,
        "tick_size": "0.25",
        "session_id": SESSION,
        "generation": 7,
        "timestamp_origin": "2026-09-23T00:00:00.0000000Z",
        "legacy_mode": False,
    }
    values.update(changes)
    return LSCContext(**values)  # type: ignore[arg-type]


def legacy_observation() -> NinjaTraderObservation:
    return NinjaTraderObservation(
        observation_id=f"nt-{SESSION}-90",
        session_id=SESSION,
        observation_type="QUOTE",
        ninja_receipt_time="2026-09-23T00:00:00.0000090Z",
        local_monotonic_sequence=90,
        payload=_payload("QUOTE", 90),
        provider_timestamp="2026-09-22T18:00:00.1234567-06:00",
        provider_sequence=190,
        schema=L3F2_SCHEMA,
    )


def legacy_context() -> LSCContext:
    return LSCContext(
        observation_schema=L3F2_SCHEMA,
        source="NINJATRADER",
        provider="LUCID_CQG",
        environment="PROP_SIM",
        producer_protocol_version=None,
        producer_source_fingerprint=None,
        producer_build_fingerprint=None,
        producer_build_timestamp=None,
        native_contract_id=NATIVE_CONTRACT,
        canonical_contract_id=CANONICAL_CONTRACT,
        tick_size="0.25",
        session_id=SESSION,
        generation=7,
        timestamp_origin="2026-09-23T00:00:00.0000000Z",
        legacy_mode=True,
    )


class LSCCodecTests(unittest.TestCase):
    def test_current_authenticated_quote_trade_depth_round_trip_exactly(self) -> None:
        shared_context = context(observation("QUOTE", 1))
        for sequence, kind in enumerate(("QUOTE", "TRADE", "DEPTH"), 1):
            with self.subTest(kind=kind):
                source = observation(kind, sequence)
                encoded = encode_observation(source, shared_context)
                decoded = decode_observation(encoded.record_bytes, encode_context(shared_context))
                self.assertEqual(decoded, source)
                self.assertIs(decoded.signature_verified, True)
                self.assertEqual(decoded.payload_hash, source.payload_hash)
                self.assertEqual(decoded.producer, source.producer)
                self.assertEqual(decoded.signature, source.signature)
                self.assertEqual(decoded.signed_wire_bytes(), source.signed_wire_bytes())
                self.assertEqual(decoded.wire_hash(), source.wire_hash())
                self.assertEqual(encoded.source_sequence, sequence + 1000)
                self.assertEqual(encoded.source_generation, 7)
                self.assertEqual(encoded.schema, L3F3_SCHEMA)

    def test_context_binds_all_current_identities(self) -> None:
        source = observation("QUOTE", 1)
        base = context(source)
        encoded = encode_observation(source, base).record_bytes
        substitutions = {
            "producer_source_fingerprint": "3" * 64,
            "producer_build_fingerprint": "4" * 64,
            "producer_build_timestamp": "2026-09-22T23:59:58.1234567Z",
            "native_contract_id": "MNQ SEP26",
            "canonical_contract_id": "MNQU6",
            "generation": 8,
        }
        for field, value in substitutions.items():
            with self.subTest(field=field):
                changed = replace(base, **{field: value})
                with self.assertRaisesRegex(LSCCodecError, "context"):
                    decode_observation(encoded, encode_context(changed))
        with self.assertRaises(ValueError):
            replace(base, producer_protocol_version="unsupported")

    def test_encode_refuses_unverified_or_inconsistent_attestation(self) -> None:
        verified = observation("QUOTE", 1)
        ctx = context(verified)
        with self.assertRaisesRegex(LSCCodecError, "verified"):
            encode_observation(replace(verified, signature_verified=None), ctx)
        with self.assertRaisesRegex(LSCCodecError, "payload hash"):
            encode_observation(replace(verified, payload_hash="0" * 64), ctx)
        assert verified.producer is not None
        other = replace(verified.producer, source_fingerprint="3" * 64)
        with self.assertRaisesRegex(LSCCodecError, "producer"):
            encode_observation(replace(verified, producer=other), ctx)

    def test_stream_and_publication_policy_vary_per_record_in_one_context(self) -> None:
        shared_context = context(observation("QUOTE", 1))
        streams = []
        policies = []
        for sequence, kind in enumerate(("QUOTE", "TRADE", "DEPTH"), 1):
            source = observation(kind, sequence)
            decoded = decode_observation(
                encode_observation(source, shared_context).record_bytes,
                encode_context(shared_context),
            )
            assert decoded.producer is not None
            streams.append(decoded.producer.source_stream)
            policies.append(decoded.producer.publication_policy)
        self.assertEqual(streams, [STREAMS[kind] for kind in ("QUOTE", "TRADE", "DEPTH")])
        self.assertEqual(policies, [POLICIES[kind] for kind in ("QUOTE", "TRADE", "DEPTH")])

    def test_source_sequence_and_generation_are_required_and_bound(self) -> None:
        for field in ("source_event_sequence", "source_generation"):
            payload = _payload("QUOTE", 1)
            del payload[field]
            source = observation("QUOTE", 1, payload=payload)
            with self.assertRaisesRegex(LSCCodecError, field):
                encode_observation(source, context(source))
        payload = _payload("QUOTE", 1)
        payload["source_generation"] = 8
        source = observation("QUOTE", 1, payload=payload)
        with self.assertRaisesRegex(LSCCodecError, "generation"):
            encode_observation(source, context(source))

    def test_timestamp_precision_types_and_tick_escape_are_lossless(self) -> None:
        source = observation("TRADE", 2)
        ctx = context(source)
        decoded = decode_observation(
            encode_observation(source, ctx).record_bytes, encode_context(ctx),
        )
        self.assertEqual(decoded.ninja_receipt_time, "2026-09-23T00:00:00.0000002Z")
        self.assertEqual(decoded.provider_timestamp, "2026-09-22T18:00:00.1234567-06:00")
        self.assertEqual(decoded.payload["price"], "20000.10")
        self.assertIs(type(decoded.payload["price"]), str)
        self.assertEqual(decoded.signed_wire_bytes(), source.signed_wire_bytes())

    def test_native_depth_object_levels_and_mutation_ticks_round_trip(self) -> None:
        source = observation("DEPTH", 3)
        shared_context = context(observation("QUOTE", 1))
        decoded = decode_observation(
            encode_observation(source, shared_context).record_bytes,
            encode_context(shared_context),
        )
        self.assertEqual(decoded, source)
        self.assertEqual(decoded.payload["mutation_position"], 0)
        self.assertEqual(decoded.payload["snapshot_ordering"], "NATIVE_POSITIONAL_BEST_TO_WORST_V1")
        bids = decoded.payload["bids"]
        self.assertIsInstance(bids, list)
        self.assertEqual(bids[0], {"price": 20000.0, "size": 5})
        self.assertEqual(bids[1], {"price": "19999.75", "size": 3})
        self.assertIs(type(bids[0]["price"]), float)
        self.assertIs(type(bids[1]["price"]), str)

    def test_payload_key_order_does_not_change_canonical_record(self) -> None:
        source = observation("QUOTE", 1)
        reversed_payload = dict(reversed(list(dict(source.payload).items())))
        reordered = observation("QUOTE", 1, payload=reversed_payload)
        self.assertEqual(source.signed_wire_bytes(), reordered.signed_wire_bytes())
        self.assertEqual(
            encode_observation(source, context(source)).record_bytes,
            encode_observation(reordered, context(reordered)).record_bytes,
        )

    def test_legacy_support_is_explicit_and_round_trips(self) -> None:
        source = legacy_observation()
        ctx = legacy_context()
        decoded = decode_observation(
            encode_observation(source, ctx).record_bytes, encode_context(ctx),
        )
        self.assertEqual(decoded, source)
        self.assertIsNone(decoded.signature)
        with self.assertRaises(ValueError):
            replace(ctx, legacy_mode=False)
        with self.assertRaises(LSCCodecError):
            encode_observation(source, context(observation("QUOTE", 1)))

    def test_context_and_record_corruption_fail_closed(self) -> None:
        source = observation("DEPTH", 3)
        context_bytes = encode_context(context(source))
        self.assertEqual(decode_context(context_bytes), context(source))
        self.assertEqual(context_digest(context_bytes), context_bytes[-32:].hex())
        damaged_context = bytearray(context_bytes)
        damaged_context[-1] ^= 1
        with self.assertRaises(LSCCodecError):
            decode_context(bytes(damaged_context))
        record = bytearray(encode_observation(source, context(source)).record_bytes)
        record[-1] ^= 1
        with self.assertRaises(LSCCodecError):
            decode_observation(bytes(record), context_bytes)

    def test_non_passive_observation_is_refused(self) -> None:
        source = observation("QUOTE", 1)
        unsafe = replace(source, observation_type="HEALTH")
        with self.assertRaisesRegex(LSCCodecError, "QUOTE, TRADE, and DEPTH"):
            encode_observation(unsafe, context(source))

    def test_immutable_current_golden_vectors(self) -> None:
        fixture = json.loads(
            (Path(__file__).with_name("golden_vectors.json")).read_text(encoding="utf-8")
        )
        for sequence, kind in enumerate(("QUOTE", "TRADE", "DEPTH"), 1):
            with self.subTest(kind=kind):
                source = observation(kind, sequence)
                ctx = context(observation("QUOTE", 1))
                encoded_context = encode_context(ctx)
                encoded = encode_observation(source, ctx)
                context_vector = fixture["context"]
                record_vector = fixture["records"][kind.lower()]
                self.assertEqual(encoded_context.hex(), context_vector["context_hex"])
                self.assertEqual(context_digest(encoded_context), context_vector["context_digest"])
                self.assertEqual(encoded.record_bytes.hex(), record_vector["record_hex"])
                self.assertEqual(encoded.payload_hash, record_vector["payload_hash"])
                self.assertEqual(encoded.wire_hash, record_vector["wire_hash"])
                self.assertEqual(encoded.event_digest, record_vector["event_digest"])
                self.assertEqual(encoded.source_sequence, record_vector["source_sequence"])
                self.assertEqual(encoded.source_generation, record_vector["source_generation"])
                self.assertEqual(encoded.schema, record_vector["schema"])


if __name__ == "__main__":
    unittest.main()
